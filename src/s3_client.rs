//! S3 client wrapper.

use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::Client as AwsS3Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::{debug, info};

/// Thin wrapper around [`aws_sdk_s3::Client`] 
#[derive(Clone)]
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

    /// Upload `body` to `key` only when it does not already exist.
    ///
    /// Returns:
    /// - `Ok(true)` if the object was created
    /// - `Ok(false)` if it already existed
    pub async fn put_object_if_absent(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> anyhow::Result<bool> {
        let res = self
            .inner
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .content_type(content_type)
            .if_none_match("*")
            .send()
            .await;

        match res {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(err)) => {
                let code = err.err().meta().code().unwrap_or_default();
                if code == "PreconditionFailed" {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!("put_object_if_absent failed for key {key}: {code}"))
                }
            }
            Err(e) => Err(anyhow::anyhow!("put_object_if_absent failed for key {key}: {e}")),
        }
    }

    /// Download and return the body of `key` from `bucket`.
    pub async fn get_object(&self, bucket: &str, key: &str) -> anyhow::Result<Vec<u8>> {
        let resp = self.inner.get_object().bucket(bucket).key(key).send().await?;
        let body = resp.body.collect().await?;
        Ok(body.into_bytes().to_vec())
    }

    /// Download and return the body of `key` from `bucket`, if present.
    pub async fn get_object_optional(
        &self,
        bucket: &str,
        key: &str,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let res = self.inner.get_object().bucket(bucket).key(key).send().await;
        match res {
            Ok(resp) => {
                let body = resp.body.collect().await?;
                Ok(Some(body.into_bytes().to_vec()))
            }
            Err(SdkError::ServiceError(err)) => {
                let code = err.err().meta().code().unwrap_or_default();
                if code == "NoSuchKey" {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!(
                        "get_object_optional failed for key {key}: {code}"
                    ))
                }
            }
            Err(e) => Err(anyhow::anyhow!("get_object_optional failed for key {key}: {e}")),
        }
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

    /// Serialize `value` as JSON and upload it to `key` in `bucket`.
    pub async fn put_json<T: Serialize>(
        &self,
        bucket: &str,
        key: &str,
        value: &T,
    ) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.put_object(bucket, key, bytes, "application/json").await
    }

    /// Serialize `value` as JSON and upload it to `key` only if absent.
    ///
    /// Returns:
    /// - `Ok(true)` if the object was created
    /// - `Ok(false)` if the object already existed
    pub async fn put_json_if_absent<T: Serialize>(
        &self,
        bucket: &str,
        key: &str,
        value: &T,
    ) -> anyhow::Result<bool> {
        let bytes = serde_json::to_vec(value)?;
        self.put_object_if_absent(bucket, key, bytes, "application/json")
            .await
    }

    /// Download `key` from `bucket` and deserialize JSON into `T`.
    pub async fn get_json<T: DeserializeOwned>(&self, bucket: &str, key: &str) -> anyhow::Result<T> {
        let bytes = self.get_object(bucket, key).await?;
        let value = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow::anyhow!("failed to decode json for key {key}: {e}"))?;
        Ok(value)
    }

    /// Download `key` from `bucket` and deserialize JSON into `T`, if present.
    ///
    /// Returns `Ok(None)` when the object does not exist.
    pub async fn get_json_optional<T: DeserializeOwned>(
        &self,
        bucket: &str,
        key: &str,
    ) -> anyhow::Result<Option<T>> {
        match self.get_object_optional(bucket, key).await? {
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes)
                    .map_err(|e| anyhow::anyhow!("failed to decode json for key {key}: {e}"))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}
