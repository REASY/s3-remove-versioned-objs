use aws_sdk_s3 as s3;
use log::{debug, info};

pub struct ObjectCollector<'a> {
    s3_client: &'a s3::Client,
    bucket: String,
    key: String,
}

impl<'a> ObjectCollector<'a> {
    pub fn new(s3_client: &'a s3::Client, bucket: String, key: String) -> ObjectCollector<'a> {
        ObjectCollector {
            s3_client,
            bucket,
            key,
        }
    }

    pub async fn collect(&self) -> Result<Vec<s3::types::ObjectVersion>, s3::Error> {
        let start = std::time::Instant::now();
        let result = self.collect0().await;
        info!(
            "collect of bucket '{}' with key '{}' took {} ms",
            self.bucket,
            self.key,
            start.elapsed().as_millis()
        );
        result
    }

    async fn collect0(&self) -> Result<Vec<s3::types::ObjectVersion>, s3::Error> {
        let mut result: Vec<s3::types::ObjectVersion> = Vec::new();
        let mut marker = self.key.clone();
        let mut n: usize = 0;
        loop {
            debug!(
                "Listing object versions with next_key_marker = {}. n = {}, result size = {}",
                &marker,
                n,
                result.len()
            );
            let resp = self
                .s3_client
                .list_object_versions()
                .bucket(self.bucket.clone())
                .key_marker(marker.clone())
                .prefix(self.key.clone())
                .send()
                .await?;
            debug!(
                "Listed object versions for next_key_marker = {}. n = {}, response has {} items",
                &marker,
                n,
                resp.versions().len(),
            );
            match &resp.versions {
                None => {
                    break;
                }
                Some(versions) => {
                    versions.iter().for_each(|v| result.push(v.clone()));
                    match resp.next_key_marker() {
                        None => {
                            break;
                        }
                        Some(next_marker) => {
                            marker = String::from(next_marker);
                        }
                    }
                }
            }
            n += 1;
        }
        Ok(result)
    }
}
