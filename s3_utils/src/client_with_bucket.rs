use crate::{Client, Error, ObjectInfo, S3Object};
use aws_sdk_s3::{
    operation::{
        delete_object::DeleteObjectOutput, delete_objects::DeleteObjectsOutput,
        get_object::GetObjectOutput, put_object::PutObjectOutput,
    },
    presigning::PresignedRequest,
    primitives::ByteStream,
};
use futures_util::TryStream;
use std::{path::Path, time::Duration};

/// バケットを固定した状態で使う`Client`.
#[derive(Debug, Clone)]
pub struct ClientWithBucket {
    client: Client,
    bucket: String,
}

impl ClientWithBucket {
    /// bucket無しのclientを取得します
    pub fn no_bucket_client(&self) -> &Client {
        &self.client
    }

    pub fn get_bucket_name(&self) -> &str {
        &self.bucket
    }
}

impl Client {
    /// bucketを指定します。APIのbucket名を固定して運用できます。
    pub fn with_bucket(self, bucket: String) -> ClientWithBucket {
        ClientWithBucket {
            client: self,
            bucket,
        }
    }
}

impl ClientWithBucket {
    /// s3のファイルの一覧を取得します。
    ///
    /// [`aws_sdk_s3::types::Object`]を使いたいとき以外は、[`ls`](`Self::ls`)を
    /// 使うことをお勧めします。
    pub fn ls_raw<'a, T>(
        &'a self,
        prefix: impl Into<String>,
        convert: impl FnMut(aws_sdk_s3::types::Object) -> Option<Result<T, Error>> + Clone + 'a,
    ) -> impl TryStream<Ok = T, Error = Error> + 'a {
        self.client.ls_raw(&self.bucket, prefix, convert)
    }

    /// s3のファイルの一覧を[`TryStream`]で取得します。
    ///
    /// pathが欲しかったら[`list_path`](`Self::list_path`),
    /// file名が欲しかったら[`list_file_name`](`Self::list_file_name`)を使ってください。
    pub fn ls(&self, prefix: impl Into<String>) -> impl TryStream<Ok = ObjectInfo, Error = Error> {
        self.client.ls(self.bucket.clone(), prefix)
    }

    /// S3のファイルのパス一覧を取得します。
    pub async fn list_path(&self, prefix: impl Into<String>) -> Result<Vec<String>, Error> {
        self.client.list_path(self.bucket.clone(), prefix).await
    }

    /// S3のファイルのパス一覧を取得します。
    ///
    /// prefixの分は取り除かれます。
    pub async fn list_file_name(&self, prefix: impl Into<String>) -> Result<Vec<String>, Error> {
        self.client.list_file_name(&self.bucket, prefix).await
    }

    /// S3からファイルを取得します。
    ///
    /// [`aws_sdk_s3::operation::get_object::GetObjectOutput`]が使いたいとき以外は、
    /// [`get_object`](`Self::get_object`)を使うことをお勧めします。
    /// このメソッドを使うと、Streamで値を取得できます。
    pub async fn get_object_raw(&self, key: impl Into<String>) -> Result<GetObjectOutput, Error> {
        self.client.get_object_raw(&self.bucket, key).await
    }

    /// S3のファイルのパス一覧を取得します。
    ///
    /// prefixの分は取り除かれています。
    pub async fn get_object(&self, key: impl Into<String>) -> Result<S3Object, Error> {
        self.client.get_object(self.bucket.clone(), key).await
    }

    /// S3へファイルを保存します
    ///
    /// `body`へは[`Vec<u8>`]など[`ByteStream`]に変換できるものを入れれます。
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

    /// ローカルファイルをストリームとして読み込み、S3にアップロードします。
    pub async fn put_object_from_file(
        &self,
        content_type: impl Into<String>,
        content_disposition: impl Into<String>,
        key: impl Into<String>,
        file_path: impl AsRef<Path>,
    ) -> Result<PutObjectOutput, Error> {
        self.client
            .put_object_from_file(
                &self.bucket,
                content_type,
                content_disposition,
                key,
                file_path,
            )
            .await
    }

    /// S3のファイルへのGETのpresigend requestのURLなどを取得します.
    ///
    /// URLだけほしい場合は、[`Self::get_presigned_url`]をお勧めします。
    pub async fn get_presigned(
        &self,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<PresignedRequest, Error> {
        self.client.get_presigned(&self.bucket, key, expire).await
    }

    /// S3のファイルへのGETのpresigend requestのURLを取得します.
    ///
    /// Headerなどがほしい場合は、[`Self::get_presigned`]をお勧めします。
    pub async fn get_presigned_url(
        &self,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<String, Error> {
        self.client
            .get_presigned_url(&self.bucket, key, expire)
            .await
    }

    /// S3のファイルへのPUTのpresigend requestのURLなどを取得します.
    ///
    /// URLだけほしい場合は、[`Self::put_presigned_url`]をお勧めします。
    pub async fn put_presigned(
        &self,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<PresignedRequest, Error> {
        self.client.put_presigned(&self.bucket, key, expire).await
    }

    /// S3のファイルへのPUTのpresigend requestのURLを取得します.
    ///
    ///  Headerなどがほしい場合は、[`Self::put_presigned`]をお勧めします。
    pub async fn put_presigned_url(
        &self,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<String, Error> {
        self.client
            .put_presigned_url(&self.bucket, key, expire)
            .await
    }

    /// S3のファイルを削除します
    pub async fn delete(&self, key: impl Into<String>) -> Result<DeleteObjectOutput, Error> {
        self.client.delete(&self.bucket, key).await
    }

    /// prefix以下の全てのファイルを削除します。
    pub async fn delete_by_prefix(
        &self,
        prefix: impl Into<String>,
    ) -> Result<Option<DeleteObjectsOutput>, Error> {
        self.client.delete_by_prefix(&self.bucket, prefix).await
    }

    /// prefix以下の全てのファイルを別のバケットにコピーします。
    pub fn copy_objects_to_by_prefix<'a>(
        &'a self,
        prefix: &'a str,
        to_bucket: &'a str,
        to_prifix: &'a str,
    ) -> impl TryStream<Ok = aws_sdk_s3::operation::copy_object::CopyObjectOutput, Error = Error> + 'a
    {
        self.client
            .copy_objects_by_prefix(&self.bucket, prefix, to_bucket, to_prifix)
    }
}
