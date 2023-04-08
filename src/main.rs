mod errors;
mod object_collector;

use crate::errors::AppError::CsvError;
use crate::errors::Result;
use crate::object_collector::ObjectCollector;
use aws_config::retry::RetryConfig;
use aws_config::SdkConfig;
use aws_sdk_s3 as s3;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
use aws_sdk_s3::operation::{RequestId, RequestIdExt};
use aws_sdk_s3::types::{Delete, ObjectIdentifier, ObjectVersion};
use byte_unit::Byte;
use clap::Parser;
use crossbeam::queue::ArrayQueue;
use csv::StringRecord;
use log::{info, warn};
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

    let config: SdkConfig = aws_config::from_env()
        .retry_config(RetryConfig::standard().with_max_attempts(30))
        .load()
        .await;

    let found_objects = collect_objects_in_parallel(config.clone(), args.clone(), keys).await?;
    let total_size: u128 = found_objects
        .iter()
        .map(|x| u128::try_from(x.size()).unwrap())
        .sum();
    info!(
        "Found {} objects with total size {} to be removed",
        found_objects.len(),
        to_adjusted_byte_unit(total_size)
    );
    let mut id_to_obj_size: HashMap<(Option<&str>, Option<&str>), i64> = HashMap::new();
    for obj in &found_objects {
        let key: (Option<&str>, Option<&str>) = (obj.key(), obj.version_id());
        id_to_obj_size.insert(key, obj.size());
    }
    if !found_objects.is_empty() {
        let removed_objs = remove_objects(args.clone(), config, found_objects.as_slice()).await?;
        let total_removed: usize = removed_objs
            .iter()
            .map(|r| r.deleted().unwrap_or_default().len())
            .sum();
        let total_removed_size: u128 = removed_objs
            .iter()
            .map(|r| {
                let sz: i64 = r
                    .deleted()
                    .unwrap_or_default()
                    .iter()
                    .map(|x| id_to_obj_size.get(&(x.key(), x.version_id())).unwrap())
                    .sum();
                sz as u128
            })
            .sum();
        let total_errors: usize = removed_objs
            .iter()
            .map(|r| r.errors().unwrap_or_default().len())
            .sum();
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
    // Sort them to have stable output
    all_obj_to_remove.sort_by(|a, b| {
        let key_a = (a.key(), b.version_id());
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
    Byte::from_bytes(bytes)
        .get_appropriate_unit(true)
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
                            info!("{} objects from bucket {} would have been removed, but aren't because of dryrun mode!", delete_objects.len(), bucket)
                        } else {
                            let resp: DeleteObjectsOutput = client
                                .delete_objects()
                                .bucket(bucket.clone())
                                .delete(Delete::builder().set_objects(Some(delete_objects)).build())
                                .send()
                                .await?;
                            let n_errors = resp.errors().unwrap_or_default().len();
                            if n_errors > 0 {
                                warn!("Bucket {}: {} objects have been removed. Could not remove {} objects!", bucket, resp.deleted().unwrap_or_default().len(), n_errors);
                                resp.errors().unwrap_or_default().iter().for_each(|err| {
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
                                    resp.deleted().unwrap_or_default().len()
                                );
                                resp.deleted().unwrap_or_default().iter().for_each(|del_obj| {
                                    info!("Delete object {} with version {}, is_delete_marker: {}, delete_marker_version_id: {}", del_obj.key().unwrap_or_default(),
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
