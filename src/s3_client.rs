//! S3 client wrapper.

use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::Client as AwsS3Client;
use tracing::{debug, info};

/// Thin wrapper around [`aws_sdk_s3::Client`] 
pub struct S3Client {
    inner: AwsS3Client,
}

impl S3Client {
    /// Build an `S3Client` from explicit endpoint and static credentials.
    pub fn new(endpoint: &str, region: &str, access_key: &str, secret_key: &str) -> Self {
        let creds = Credentials::new(access_key, secret_key, None, None, "crdt-static");
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .endpoint_url(endpoint)
            .credentials_provider(creds)
            .force_path_style(true)
            .build();
        Self {
            inner: AwsS3Client::from_conf(config),
        }
    }

    /// Create the bucket if it does not already exist.
    pub async fn ensure_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        match self.inner.head_bucket().bucket(bucket).send().await {
            Ok(_) => {
                debug!(bucket, "bucket already exists");
                Ok(())
            }
            Err(_) => {
                info!(bucket, "creating discovery bucket");
                self.inner.create_bucket().bucket(bucket).send().await?;
                Ok(())
            }
        }
    }

    /// Upload `body` to `key` in `bucket` with the given `content_type`.
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> anyhow::Result<()> {
        self.inner
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .content_type(content_type)
            .send()
            .await?;
        Ok(())
    }

    /// Download and return the body of `key` from `bucket`.
    pub async fn get_object(&self, bucket: &str, key: &str) -> anyhow::Result<Vec<u8>> {
        let resp = self.inner.get_object().bucket(bucket).key(key).send().await?;
        let body = resp.body.collect().await?;
        Ok(body.into_bytes().to_vec())
    }

    /// Delete `key` from `bucket`.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> anyhow::Result<()> {
        self.inner
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    /// Return all object keys under `prefix` in `bucket`, handling S3
    /// pagination transparently.
    pub async fn list_object_keys(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self.inner.list_objects_v2().bucket(bucket).prefix(prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await?;

            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    keys.push(k.to_string());
                }
            }

            match resp.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(keys)
    }
}
