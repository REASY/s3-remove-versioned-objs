mod errors;
mod object_collector;

use crate::errors::AppError::CsvError;
use crate::errors::Result;
use crate::object_collector::ObjectCollector;
use aws_config::retry::RetryConfig;
use aws_sdk_s3 as s3;
use aws_sdk_s3::types::ObjectVersion;
use byte_unit::Byte;
use clap::Parser;
use crossbeam::queue::ArrayQueue;
use csv::StringRecord;
use log::info;
use std::sync::Arc;

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
    #[clap(long, default_value_t = true)]
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

    let found_objects = collect_objects_in_parallel(args, keys).await?;
    let total_size: u128 = found_objects
        .iter()
        .map(|x| u128::try_from(x.size()).unwrap())
        .sum();
    info!(
        "Found {} objects with total size {} to be removed",
        found_objects.len(),
        to_adjusted_byte_unit(total_size)
    );

    for version in found_objects {
        println!("{}", version.key().unwrap_or_default());
        println!("  version ID: {}", version.version_id().unwrap_or_default());
        println!(
            "  size: {}",
            to_adjusted_byte_unit(u128::try_from(version.size()).unwrap())
        );
        println!();
    }
    Ok(())
}

async fn collect_objects_in_parallel(args: Args, keys: Vec<String>) -> Result<Vec<ObjectVersion>> {
    let queue = ArrayQueue::<String>::new(keys.len());
    for key in keys {
        queue.push(key).unwrap()
    }
    let arc_queue = Arc::new(queue);

    let mut threads: Vec<tokio::task::JoinHandle<Result<Vec<ObjectVersion>>>> = Vec::new();
    let config = aws_config::from_env()
        .retry_config(RetryConfig::standard().with_max_attempts(30))
        .load()
        .await;

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
