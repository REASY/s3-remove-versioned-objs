use async_recursion::async_recursion;
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
        let result = self.collect0(0, self.key.clone(), Vec::new()).await;
        info!(
            "collect of bucket '{}' with key '{}' took {} ms",
            self.bucket,
            self.key,
            start.elapsed().as_millis()
        );
        result
    }

    #[async_recursion]
    async fn collect0(
        &self,
        n: usize,
        next_key_marker: String,
        mut result: Vec<s3::types::ObjectVersion>,
    ) -> Result<Vec<s3::types::ObjectVersion>, s3::Error> {
        debug!(
            "Listing object versions with next_key_marker = {}. n = {}, result size = {}",
            &next_key_marker,
            n,
            result.len()
        );
        let resp = self
            .s3_client
            .list_object_versions()
            .bucket(self.bucket.clone())
            .key_marker(next_key_marker.clone())
            .prefix(self.key.clone())
            .send()
            .await?;
        debug!(
            "Listed object versions for next_key_marker = {}. n = {}, response has {} items",
            &next_key_marker,
            n,
            resp.versions().unwrap_or_default().len(),
        );
        match resp.versions() {
            None => Ok(result),
            Some(versions) => {
                versions.iter().for_each(|v| result.push(v.clone()));
                match resp.next_key_marker() {
                    None => Ok(result),
                    Some(next_marker) => {
                        self.collect0(n + 1, String::from(next_marker), result)
                            .await
                    }
                }
            }
        }
    }
}
