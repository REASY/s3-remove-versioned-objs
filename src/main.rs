mod errors;
mod object_collector;

use crate::errors::AppError::CsvError;
use crate::errors::Result;
use crate::object_collector::ObjectCollector;
use aws_config::retry::RetryConfig;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3 as s3;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
use aws_sdk_s3::operation::{RequestId, RequestIdExt};
use aws_sdk_s3::types::{Delete, ObjectIdentifier, ObjectVersion};
use byte_unit::{Byte, UnitType};
use clap::Parser;
use crossbeam::queue::ArrayQueue;
use csv::{QuoteStyle, StringRecord, WriterBuilder};
use log::{info, warn};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
// The request contains a list of up to 1000 keys that you want to delete
static MAX_OBJECTS_PER_DELETE_OBJECTS_REQUEST: usize = 1000;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to AWS S3 bucket
    #[clap(long)]
    bucket: String,

    /// Path to CSV file with the list of keys in S3 to be removed
    #[clap(long)]
    key_list_csv_path: String,

    /// Path to CSV file with the list of prefixes to not remove. It has to have column `key_prefix`
    #[clap(long)]
    prefix_exclude_list_csv_path: Option<String>,

    /// The name of the column in CSV file that holds keys to be deleted
    #[clap(long, default_value_t = String::from("s3_key"))]
    key_column_name: String,

    /// Number of workers
    #[clap(long, default_value_t = 2_u8)]
    workers: u8,

    /// Displays the operations that would be performed using the specified command without actually running them
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    dryrun: bool,
}

#[derive(Debug, Serialize)]
struct RemoveObjectRecord<'a> {
    key: Option<&'a str>,
    version_id: Option<&'a str>,
    last_modified: Option<String>,
    storage_class: Option<String>,
    size: Option<i64>,
    e_tag: Option<&'a str>,
    is_deleted: bool,
    delete_marker: Option<bool>,
    delete_marker_version_id: Option<&'a str>,
    is_dryrun: bool,
    has_error: bool,
    error_code: Option<&'a str>,
    error_message: Option<&'a str>,
    request_id: Option<&'a str>,
    extended_request_id: Option<&'a str>,
}

#[tokio::main]
async fn main() -> Result<()> {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    let args = Args::parse();
    info!("Received args: {:?}", args);
    assert!(
        args.workers >= 1 && args.workers <= 16,
        "should be within [1, 16]"
    );

    let keys = load_object_keys(
        args.key_list_csv_path.as_str(),
        args.key_column_name.as_str(),
    )?;
    info!(
        "Read {} keys from {} at column {}",
        keys.len(),
        args.key_list_csv_path,
        args.key_column_name
    );

    let prefix_exclude_list: Vec<String> = match args.prefix_exclude_list_csv_path.as_ref() {
        None => Result::Ok(Vec::new())?,
        Some(p) => load_object_keys(p.as_str(), "key_prefix")?,
    };
    info!(
        "prefix_exclude_list has {} prefixes",
        prefix_exclude_list.len()
    );

    let config: SdkConfig = aws_config::defaults(BehaviorVersion::v2023_11_09())
        .retry_config(RetryConfig::standard().with_max_attempts(30))
        .load()
        .await;

    let found_objects: Vec<ObjectVersion> =
        collect_objects_in_parallel(config.clone(), args.clone(), keys).await?;
    let to_remove_objs: Vec<ObjectVersion> = found_objects
        .iter()
        .filter(|&obj| {
            // FIXME: If this is too slow, improve it later
            // Note: Iterator.filter returns iterator that will yield only the elements for which the closure returns true.
            let should_exclude_from_removal = prefix_exclude_list
                .iter()
                .any(|exclude_prefix| obj.key().unwrap_or_default().contains(exclude_prefix));
            // if should_exclude_from_removal == true, we should return false
            !should_exclude_from_removal
        })
        .cloned()
        .collect();

    let total_size: u128 = to_remove_objs
        .iter()
        .map(|x| u128::try_from(x.size().unwrap()).unwrap())
        .sum();
    info!(
        "Found {} objects after filtering got {} objects with total size {} to be removed",
        found_objects.len(),
        to_remove_objs.len(),
        to_adjusted_byte_unit(total_size)
    );
    let mut id_to_obj: HashMap<(Option<&str>, Option<&str>), ObjectVersion> = HashMap::new();
    for obj in &to_remove_objs {
        let key: (Option<&str>, Option<&str>) = (obj.key(), obj.version_id());
        id_to_obj.insert(key, obj.clone());
    }
    if !to_remove_objs.is_empty() {
        let removed_objs = remove_objects(args.clone(), config, to_remove_objs.as_slice()).await?;
        let report_records: Vec<RemoveObjectRecord> = if removed_objs.is_empty() {
            to_remove_objs
                .iter()
                .map(|x| RemoveObjectRecord {
                    key: x.key(),
                    version_id: x.version_id(),
                    last_modified: x.last_modified.map(|d| format!("{}", d)),
                    storage_class: x.storage_class().map(|x| x.as_str().to_string()),
                    size: x.size,
                    e_tag: x.e_tag(),
                    is_deleted: false,
                    delete_marker: None,
                    delete_marker_version_id: None,
                    is_dryrun: args.dryrun,
                    has_error: false,
                    error_code: None,
                    error_message: None,
                    request_id: None,
                    extended_request_id: None,
                })
                .collect()
        } else {
            let mut res: Vec<RemoveObjectRecord> = Vec::new();
            removed_objs.iter().for_each(|r| {
                let request_id = r.request_id();
                let extended_request_id = r.extended_request_id();
                // Create records for delete objects
                r.deleted().iter().for_each(|d| {
                    let key = (d.key(), d.version_id());
                    let x = id_to_obj.get(&key).unwrap();
                    let obj = RemoveObjectRecord {
                        key: x.key(),
                        version_id: x.version_id(),
                        last_modified: x.last_modified.map(|d| format!("{}", d)),
                        storage_class: x.storage_class().map(|x| x.as_str().to_string()),
                        size: x.size,
                        e_tag: x.e_tag(),
                        is_deleted: true,
                        delete_marker: d.delete_marker,
                        delete_marker_version_id: d.delete_marker_version_id(),
                        is_dryrun: args.dryrun,
                        has_error: false,
                        error_code: None,
                        error_message: None,
                        request_id,
                        extended_request_id,
                    };
                    res.push(obj);
                });
                // Create records for failed objects
                r.errors().iter().for_each(|d| {
                    let key = (d.key(), d.version_id());
                    let x = id_to_obj.get(&key).unwrap();
                    let obj = RemoveObjectRecord {
                        key: x.key(),
                        version_id: x.version_id(),
                        last_modified: x.last_modified.map(|d| format!("{}", d)),
                        storage_class: x.storage_class().map(|x| x.as_str().to_string()),
                        size: x.size,
                        e_tag: x.e_tag(),
                        is_deleted: false,
                        delete_marker: None,
                        delete_marker_version_id: None,
                        is_dryrun: args.dryrun,
                        has_error: true,
                        error_code: d.code(),
                        error_message: d.message(),
                        request_id,
                        extended_request_id,
                    };
                    res.push(obj);
                });
            });
            // Create record for objects that could not delete
            res
        };

        {
            let mut wrt = WriterBuilder::new()
                .quote_style(QuoteStyle::NonNumeric)
                .from_path("result.csv")
                .unwrap();
            for r in report_records {
                wrt.serialize(r)?;
            }
            wrt.flush().unwrap();
        }

        let total_removed: usize = removed_objs.iter().map(|r| r.deleted().len()).sum();
        let total_removed_size: u128 = removed_objs
            .iter()
            .map(|r| {
                let sz: i64 = r
                    .deleted()
                    .iter()
                    .map(|x| {
                        id_to_obj
                            .get(&(x.key(), x.version_id()))
                            .unwrap()
                            .size()
                            .unwrap()
                    })
                    .sum();
                sz as u128
            })
            .sum();
        let total_errors: usize = removed_objs.iter().map(|r| r.errors().len()).sum();
        info!(
            "Removed {} objects with total size {} and could not remove {} objects",
            total_removed,
            to_adjusted_byte_unit(total_removed_size),
            total_errors
        );
    }
    Ok(())
}

async fn collect_objects_in_parallel(
    config: SdkConfig,
    args: Args,
    keys: Vec<String>,
) -> Result<Vec<ObjectVersion>> {
    let queue = ArrayQueue::<String>::new(keys.len());
    for key in keys {
        queue.push(key).unwrap()
    }
    let arc_queue = Arc::new(queue);

    let mut threads: Vec<tokio::task::JoinHandle<Result<Vec<ObjectVersion>>>> = Vec::new();
    for _ in 0..args.workers {
        let bucket = args.bucket.clone();
        let temp_arc_q = arc_queue.clone();
        let cfg_copy = config.clone();
        let t = tokio::spawn(async move {
            let client: s3::Client = s3::Client::new(&cfg_copy);
            let mut result: Vec<ObjectVersion> = Vec::new();
            loop {
                match temp_arc_q.pop() {
                    None => {
                        break;
                    }
                    Some(key) => {
                        let obj_collector = ObjectCollector::new(&client, bucket.clone(), key);
                        let r = obj_collector.collect().await?;
                        result.extend(r);
                    }
                }
            }
            Ok(result)
        });
        threads.push(t);
    }
    let mut all_obj_to_remove: Vec<ObjectVersion> = Vec::new();
    for t in threads {
        let r = t.await??;
        all_obj_to_remove.extend(r);
    }
    all_obj_to_remove.dedup_by(|a, b| {
        let key_a = (a.key(), a.version_id());
        let key_b = (b.key(), b.version_id());
        key_a == key_b
    });
    // Sort them to have stable output
    all_obj_to_remove.sort_by(|a, b| {
        let key_a = (a.key(), a.version_id());
        let key_b = (b.key(), b.version_id());
        key_a.cmp(&key_b)
    });
    Ok(all_obj_to_remove)
}

fn load_object_keys(csv_path: &str, column_name: &str) -> Result<Vec<String>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)?;

    let headers = rdr.headers()?;
    let mut maybe_column_pos: Option<usize> = None;
    for (pos, header) in headers.iter().enumerate() {
        if header == column_name {
            maybe_column_pos = Some(pos);
            break;
        }
    }

    match maybe_column_pos {
        None => Err(CsvError(From::from(format!(
            "expected to find a column with name '{}' but could not find it",
            column_name
        )))),
        Some(col_pos) => {
            let mut row: usize = 1;
            let mut record = StringRecord::new();
            if !rdr.read_record(&mut record)? {
                return Err(CsvError(From::from(
                    "expected at least one record but got none",
                )));
            }

            let mut objs = Vec::new();
            match record.get(col_pos) {
                None => {}
                Some(key) => {
                    if key.is_empty() {
                        return Err(CsvError(From::from(
                            format!("a key at row {} is empty, this is too broad and dangerous. You will end-up scanning the whole bucket!", row),
                        )));
                    }
                    objs.push(String::from(key))
                }
            }
            row += 1;

            for maybe_record in rdr.records() {
                let r = maybe_record?;
                match r.get(col_pos) {
                    None => {}
                    Some(key) => {
                        if key.is_empty() {
                            return Err(CsvError(From::from(
                                format!("a key at row {} is empty, this is too broad and dangerous. You will end-up scanning the whole bucket!", row),
                            )));
                        }
                        objs.push(String::from(key))
                    }
                }
                row += 1;
            }

            Ok(objs)
        }
    }
}

fn to_adjusted_byte_unit(bytes: u128) -> String {
    Byte::from_u128(bytes)
        .unwrap()
        .get_appropriate_unit(UnitType::Binary)
        .to_string()
}

async fn remove_objects(
    args: Args,
    config: SdkConfig,
    objs: &[ObjectVersion],
) -> Result<Vec<DeleteObjectsOutput>> {
    let chunks: Vec<Vec<ObjectIdentifier>> = objs
        .chunks(MAX_OBJECTS_PER_DELETE_OBJECTS_REQUEST)
        .map(|chunk| {
            let to_delete = chunk
                .iter()
                .map(|obj| {
                    assert!(obj.key().is_some());
                    assert!(obj.version_id().is_some());

                    ObjectIdentifier::builder()
                        .key(obj.key().unwrap())
                        .version_id(obj.version_id().unwrap())
                        .build()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            to_delete
        })
        .collect::<Vec<_>>();
    let queue = ArrayQueue::<Vec<ObjectIdentifier>>::new(chunks.len());
    for v in chunks {
        queue.push(v).unwrap();
    }

    let arc_queue = Arc::new(queue);

    let mut threads: Vec<tokio::task::JoinHandle<Result<Vec<DeleteObjectsOutput>>>> = Vec::new();
    for _ in 0..args.workers {
        let is_dryrun = args.dryrun;
        let bucket = args.bucket.clone();
        let temp_arc_q = arc_queue.clone();
        let cfg_copy = config.clone();
        let t = tokio::spawn(async move {
            let client: s3::Client = s3::Client::new(&cfg_copy);
            let mut result: Vec<DeleteObjectsOutput> = Vec::new();
            loop {
                match temp_arc_q.pop() {
                    None => {
                        break;
                    }
                    Some(delete_objects) => {
                        if is_dryrun {
                            info!("{} objects from bucket {} would have been removed, but aren't because of dryrun mode!", delete_objects.len(), bucket);
                            delete_objects.iter().for_each(|c| {
                                info!(
                                    "Object {} with version {}",
                                    c.key(),
                                    c.version_id().unwrap_or_default()
                                )
                            });
                        } else {
                            let resp: DeleteObjectsOutput = client
                                .delete_objects()
                                .bucket(bucket.clone())
                                .delete(
                                    Delete::builder()
                                        .set_objects(Some(delete_objects))
                                        .build()
                                        .unwrap(),
                                )
                                .send()
                                .await?;
                            let n_errors = resp.errors().len();
                            if n_errors > 0 {
                                warn!("Bucket {}: {} objects have been removed. Could not remove {} objects!", bucket, resp.deleted().len(), n_errors);
                                resp.errors().iter().for_each(|err| {
                                    warn!("Could not remove {} with version {}. Error message: {}, code: {}. Request was {}/{}", err.key().unwrap_or_default(),
                                        err.version_id().unwrap_or_default(),
                                        err.message().unwrap_or_default(),
                                        err.code().unwrap_or_default(),
                                        resp.request_id().unwrap_or_default(),
                                        resp.extended_request_id().unwrap_or_default());
                                });
                            } else {
                                info!(
                                    "Bucket {}: {} objects have been removed",
                                    bucket,
                                    resp.deleted().len()
                                );
                                resp.deleted().iter().for_each(|del_obj| {
                                    info!("Delete object {} with version {}, is_delete_marker: {:?}, delete_marker_version_id: {}", del_obj.key().unwrap_or_default(),
                                        del_obj.version_id().unwrap_or_default(),
                                        del_obj.delete_marker(),
                                        del_obj.delete_marker_version_id().unwrap_or_default());
                                });
                            }
                            result.push(resp);
                        }
                    }
                }
            }
            Ok(result)
        });
        threads.push(t);
    }
    let mut result: Vec<DeleteObjectsOutput> = Vec::new();
    for t in threads {
        let r = t.await??;
        result.extend(r);
    }

    Ok(result)
}
