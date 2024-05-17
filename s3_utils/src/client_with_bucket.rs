use crate::{Client, Error, ObjectInfo, S3Object};
use aws_sdk_s3::{
    operation::{
        delete_object::DeleteObjectOutput, delete_objects::DeleteObjectsOutput,
        put_object::PutObjectOutput,
    },
    primitives::ByteStream,
};
use futures_util::TryStream;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClientWithBucket {
    client: Client,
    bucket: String,
}

impl Client {
    pub fn with_bucket(self, bucket: String) -> ClientWithBucket {
        ClientWithBucket {
            client: self,
            bucket,
        }
    }
}

impl ClientWithBucket {
    pub fn ls_inner<'a, T>(
        &'a self,
        prefix: impl Into<String>,
        convert: impl FnMut(aws_sdk_s3::types::Object) -> Option<Result<T, Error>> + Clone + 'a,
    ) -> impl TryStream<Ok = T, Error = Error> + 'a {
        self.client.ls_inner(&self.bucket, prefix, convert)
    }

    pub fn ls(&self, prefix: impl Into<String>) -> impl TryStream<Ok = ObjectInfo, Error = Error> {
        self.client.ls(self.bucket.clone(), prefix)
    }

    pub async fn list_path(&self, prefix: impl Into<String>) -> Result<Vec<String>, Error> {
        self.client.list_path(self.bucket.clone(), prefix).await
    }

    pub async fn get_object(&self, key: impl Into<String>) -> Result<S3Object, Error> {
        self.client.get_object(self.bucket.clone(), key).await
    }

    pub async fn put_object(
        &self,
        content_type: impl Into<String>,
        content_disposition: impl Into<String>,
        key: impl Into<String>,
        body: impl Into<ByteStream>,
    ) -> Result<PutObjectOutput, Error> {
        self.client
            .put_object(&self.bucket, content_type, content_disposition, key, body)
            .await
    }

    pub async fn get_presigned(
        &self,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<String, Error> {
        self.client.get_presigned(&self.bucket, key, expire).await
    }

    pub async fn put_presigned(
        &self,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<String, Error> {
        self.client.put_presigned(&self.bucket, key, expire).await
    }

    pub async fn delete(&self, key: impl Into<String>) -> Result<DeleteObjectOutput, Error> {
        self.client.delete(&self.bucket, key).await
    }

    pub async fn delete_by_prefix(
        &self,
        prefix: impl Into<String>,
    ) -> Result<Option<DeleteObjectsOutput>, Error> {
        self.client.delete_by_prefix(&self.bucket, prefix).await
    }
}
