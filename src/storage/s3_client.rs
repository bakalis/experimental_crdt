#![allow(dead_code)]
//! S3 client wrapper.

use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::Client as AwsS3Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use core::convert::Into;
use std::sync::Arc;
use tracing::{debug, info};

use crate::metric;

/// Thin wrapper around [`aws_sdk_s3::Client`]
#[derive(Clone)]
pub struct S3Client {
    inner: Arc<AwsS3Client>,
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
            inner: Arc::new(AwsS3Client::from_conf(config)),
        }
    }

    /// Create the bucket if it does not already exist.
    pub async fn ensure_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        let start_millis = std::time::Instant::now();
        let result = match self.inner.head_bucket().bucket(bucket).send().await {
            Ok(_) => {
                debug!(bucket, "bucket already exists");
                Ok(())
            }
            Err(_) => {
                info!(bucket, "creating discovery bucket");
                self.inner.create_bucket().bucket(bucket).send().await?;
                Ok(())
            }
        };
        metric!(event = "s3_ensure_bucket",
            duration_millis = start_millis.elapsed().as_millis() as u64);
        result
    }

    /// Upload `body` to `key` in `bucket` with the given `content_type`.
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> anyhow::Result<()> {
        let start_millis = std::time::Instant::now();
        let size = body.len();
        self.inner
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .content_type(content_type)
            .send()
            .await?;
        metric!(event = "s3_put_object",
            size_bytes = size,
            duration_millis = start_millis.elapsed().as_millis() as u64);
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
        let start_millis = std::time::Instant::now();
        let size = body.len();
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

        let result = match res {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(err)) => {
                let code = err.err().meta().code().unwrap_or_default();
                if code == "PreconditionFailed" {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!(
                        "put_object_if_absent failed for key {key}: {code}"
                    ))
                }
            }
            Err(e) => Err(anyhow::anyhow!(
                "put_object_if_absent failed for key {key}: {e}"
            )),
        };
        metric!(event = "s3_put_object_if_absent",
            size_bytes = size,
            duration_millis = start_millis.elapsed().as_millis() as u64);
        result
    }

    /// Download and return the body of `key` from `bucket`.
    pub async fn get_object(&self, bucket: &str, key: &str) -> anyhow::Result<Vec<u8>> {
        let start_millis = std::time::Instant::now();
        let resp = self
            .inner
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        let body = resp.body.collect().await?.into_bytes();
        let size = body.len();
        metric!(event = "s3_get_object",
            size_bytes = size,
            duration_millis = start_millis.elapsed().as_millis() as u64);
        Ok(body.to_vec())
    }

    /// Download and return the body of `key` from `bucket`, if present.
    pub async fn get_object_optional(
        &self,
        bucket: &str,
        key: &str,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let start_millis = std::time::Instant::now();
        let res = self.inner.get_object().bucket(bucket).key(key).send().await;
        let result = match res {
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
            Err(e) => Err(anyhow::anyhow!(
                "get_object_optional failed for key {key}: {e}"
            )),
        };
        let size = match &result {
            Ok(Some(bytes)) => bytes.len() as u64,
            _ => 0,
        };
        metric!(event = "s3_get_object_optional",
            size_bytes = size,
            duration_millis = start_millis.elapsed().as_millis() as u64);
        result
    }

    /// Delete `key` from `bucket`.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> anyhow::Result<()> {
        let start_millis = std::time::Instant::now();
        self.inner
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        metric!(event = "s3_delete_object", duration_millis = start_millis.elapsed().as_millis() as u64);
        Ok(())
    }

    /// Return all object keys under `prefix` in `bucket`, handling S3
    /// pagination transparently.
    pub async fn list_object_keys(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> anyhow::Result<Vec<String>> {
        let start_millis = std::time::Instant::now();
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
                Option::None => break,
            }
        }
        metric!(event = "s3_list_object_keys", duration_millis = start_millis.elapsed().as_millis() as u64);
        Ok(keys)
    }

    /// Serialize `value` as JSON and upload it to `key` in `bucket`.
    pub async fn put_json<T: Serialize>(
        &self,
        bucket: &str,
        key: &str,
        value: &T,
    ) -> anyhow::Result<()> {
        let start_millis = std::time::Instant::now();
        let bytes = serde_json::to_vec(value)?;
        let size = bytes.len() as u64;
        let result = self.put_object(bucket, key, bytes, "application/json")
            .await;
        metric!(event = "s3_put_json",
            size_bytes = size,
            duration_millis = start_millis.elapsed().as_millis() as u64);
        result
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
        let start_millis = std::time::Instant::now();
        let bytes = serde_json::to_vec(value)?;
        let size = bytes.len() as u64;
        let result = self.put_object_if_absent(bucket, key, bytes, "application/json")
            .await;
        metric!(event = "s3_put_json_if_absent",
            size_bytes = size,
            duration_millis = start_millis.elapsed().as_millis() as u64);
        result
    }

    /// Download `key` from `bucket` and deserialize JSON into `T`.
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        bucket: &str,
        key: &str,
    ) -> anyhow::Result<T> {
        let start_millis = std::time::Instant::now();
        let bytes = self.get_object(bucket, key).await?;
        let size = bytes.len() as u64;
        let value = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow::anyhow!("failed to decode json for key {key}: {e}"))?;
        metric!(event = "s3_get_json", size_bytes = size,
            duration_millis = start_millis.elapsed().as_millis() as u64);
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
        let start_millis = std::time::Instant::now();
        let result: anyhow::Result<(u64, Option<T>)> = match self.get_object_optional(bucket, key).await? {
            Some(bytes) => {
                let size = bytes.len() as u64;
                let value = serde_json::from_slice(&bytes)
                    .map_err(|e| anyhow::anyhow!("failed to decode json for key {key}: {e}"))?;
                Ok((size, Some(value)))
            }
            Option::None => Ok((0, None)),
        };
        let (size, deserialized): (u64, Option<T>) = result?;
        metric!(event = "s3_get_json_optional",
            size_bytes = size,
            duration_millis = start_millis.elapsed().as_millis() as u64);
        Ok(deserialized)
    }
}
