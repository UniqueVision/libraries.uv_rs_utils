use crate::{from_aws_sdk_s3_error, Error};
use aws_config::Region;
use aws_sdk_s3::{
    config::Credentials,
    operation::{
        delete_object::DeleteObjectOutput, delete_objects::DeleteObjectsOutput,
        get_object::GetObjectOutput, put_object::PutObjectOutput,
    },
    presigning::{PresignedRequest, PresigningConfig},
    primitives::{ByteStream, DateTime},
    types::{Delete, ObjectIdentifier},
};
use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures_util::{FutureExt, TryStream, TryStreamExt};
use serde::de::DeserializeOwned;
use std::{mem::swap, path::Path, time::Duration};
use tokio::io::{AsyncReadExt, BufReader};

/// awsのS3の高レベルなClient.
/// 低レベルな操作は[`raw_client`](`Client::raw_client`)を使って取得したものを使ってください
#[derive(Debug, Clone)]
pub struct Client {
    s3: aws_sdk_s3::Client,
}

impl Client {
    /// [`aws_sdk_s3::Client`]から[`Client`]を作ります
    pub fn from_s3_client(s3: aws_sdk_s3::Client) -> Self {
        Self { s3 }
    }

    /// 環境変数から作ります
    pub async fn from_env() -> Self {
        let config = aws_config::from_env().load().await;
        Client::from_conf(&config)
    }

    /// コンフィグから作ります
    pub fn from_conf<C: Into<aws_sdk_s3::Config>>(conf: C) -> Self {
        Self::from_s3_client(aws_sdk_s3::Client::from_conf(conf.into()))
    }

    /// ローカルでminioにアクセス
    /// user : minio
    /// pass : pass
    /// url : http://minio:9000
    pub fn minio(user: &str, pass: &str, url: &str) -> Self {
        let credentials_provider = Credentials::new(user, pass, None, None, "example");
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .credentials_provider(credentials_provider)
            .region(Region::new("ap-northeast-1"))
            .force_path_style(true)
            .endpoint_url(url)
            .build();
        Self::from_conf(config)
    }
}

impl AsRef<aws_sdk_s3::Client> for Client {
    fn as_ref(&self) -> &aws_sdk_s3::Client {
        &self.s3
    }
}

impl Client {
    /// 内側のclientを取得する
    pub fn raw_client(&self) -> &aws_sdk_s3::Client {
        &self.s3
    }

    /// s3のファイルの一覧を取得します。
    ///
    /// [`aws_sdk_s3::types::Object`]を使いたいとき以外は、[`ls`](`Self::ls`)を
    /// 使うことをお勧めします。
    ///
    /// ```no_run
    /// # use s3_utils::*;
    /// # tokio_test::block_on(async {
    /// use futures_util::TryStreamExt;
    /// let client = s3_utils::Client::from_env().await;
    /// client.ls_raw("sample_bucket", "folder1/", |obj| {Some(Ok(ObjectInfo {
    ///     key: obj.key?,
    ///     // 最終更新日が時刻以降のものを取得する
    ///     last_modified: Some(obj.last_modified.filter(|last| last.secs() > 1716877593)?),
    ///     e_tag: obj.e_tag,
    ///     size: obj.size,
    /// }))}).try_collect::<Vec<_>>().await;
    /// # })
    /// ```
    pub fn ls_raw<'a, T>(
        &self,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        convert: impl FnMut(aws_sdk_s3::types::Object) -> Option<Result<T, Error>> + Clone + 'a,
    ) -> impl TryStream<Ok = T, Error = Error> + 'a {
        self.as_ref()
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .into_paginator()
            .send()
            .into_stream_03x()
            .map_err(from_aws_sdk_s3_error)
            .map_ok(move |s| {
                futures_util::stream::iter(
                    s.contents
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(convert.clone()),
                )
            })
            .try_flatten()
    }

    /// s3のファイルの一覧を[`TryStream`]で取得します。
    ///
    /// pathが欲しかったら[`list_path`](`Self::list_path`),
    /// file名が欲しかったら[`list_file_name`](`Self::list_file_name`)を使ってください。
    ///
    /// ```no_run
    /// # use s3_utils::*;
    /// # tokio_test::block_on(async {
    /// use futures_util::{StreamExt, TryStreamExt};
    /// let client = s3_utils::Client::from_env().await;
    /// client.ls("sample_bucket", "folder1/").count();
    /// # })
    /// ```
    pub fn ls(
        &self,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> impl TryStream<Ok = ObjectInfo, Error = Error> {
        self.ls_raw::<ObjectInfo>(bucket, prefix, |obj| {
            Some(Ok(ObjectInfo {
                key: obj.key?,
                last_modified: obj.last_modified,
                e_tag: obj.e_tag,
                size: obj.size,
            }))
        })
    }

    /// S3のファイルのパス一覧を取得します。
    /// ```no_run
    /// # use s3_utils::*;
    /// # tokio_test::block_on(async {
    /// use futures_util::{StreamExt, TryStreamExt};
    /// let client = s3_utils::Client::from_env().await;
    /// let list = client.list_path("sample_bucket", "folder1/");
    /// # })
    /// ```
    pub async fn list_path(
        &self,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Vec<String>, Error> {
        self.ls_raw::<String>(bucket, prefix, |obj| obj.key.map(Ok))
            .try_collect()
            .await
    }

    /// S3のファイルのパス一覧を取得します。
    ///
    /// prefixの分は取り除かれています。
    /// ```no_run
    /// # use s3_utils::*;
    /// # tokio_test::block_on(async {
    /// use futures_util::{StreamExt, TryStreamExt};
    /// let client = s3_utils::Client::from_env().await;
    /// let list = client.list_file_name("sample_bucket", "folder1/");
    /// # })
    /// ```
    pub async fn list_file_name(
        &self,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Vec<String>, Error> {
        let pre: String = prefix.into();
        let pre2 = &*pre.clone();
        self.ls_raw::<String>(bucket, pre, |obj| {
            obj.key.and_then(|x| Some(Ok(x.strip_prefix(pre2)?.into())))
        })
        .try_collect()
        .await
    }

    /// S3からファイルを取得します。
    ///
    /// [`aws_sdk_s3::operation::get_object::GetObjectOutput`]が使いたいとき以外は、
    /// [`get_object`](`Self::get_object`)を使うことをお勧めします。
    /// このメソッドを使うと、Streamで値を取得できます。
    /// ```no_run
    /// # use s3_utils::*;
    /// # tokio_test::block_on(async {
    /// use futures_util::{StreamExt, TryStreamExt};
    /// let client = s3_utils::Client::from_env().await;
    /// let obj = client.get_object_raw("sample_bucket", "folder1/aaa.png").await;
    /// let disposition = obj.ok().and_then(|obj| obj.content_disposition);
    /// # })
    /// ```
    pub async fn get_object_raw(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<GetObjectOutput, Error> {
        self.as_ref()
            .get_object()
            .set_bucket(Some(bucket.into()))
            .set_key(Some(key.into()))
            .send()
            .await
            .map_err(from_aws_sdk_s3_error)
    }

    /// S3からファイルを取得します。
    /// ```no_run
    /// # use s3_utils::*;
    /// # tokio_test::block_on(async {
    /// use futures_util::{StreamExt, TryStreamExt};
    /// let client = s3_utils::Client::from_env().await;
    /// let obj = client.get_object("sample_bucket", "folder1/abc.json").await;
    /// # })
    /// ```
    pub async fn get_object(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<S3Object, Error> {
        let res = self.get_object_raw(bucket, key).await?;
        let content_type = res.content_type().unwrap_or_default().to_owned();

        let mut buf_reader = BufReader::new(res.body.into_async_read());
        let mut buf = vec![];
        buf_reader.read_to_end(&mut buf).await?;

        Ok(S3Object { content_type, buf })
    }

    /// S3へファイルを保存します
    ///
    /// `body`へは[`Vec<u8>`]など[`ByteStream`]に変換できるものを入れれます。
    pub async fn put_object(
        &self,
        bucket: impl Into<String>,
        content_type: impl Into<String>,
        content_disposition: impl Into<String>,
        key: impl Into<String>,
        body: impl Into<ByteStream>,
    ) -> Result<PutObjectOutput, Error> {
        let res = self
            .as_ref()
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type(content_type.into())
            .content_disposition(content_disposition.into())
            .body(body.into())
            .send()
            .await
            .map_err(from_aws_sdk_s3_error)?;

        Ok(res)
    }

    /// ローカルファイルをストリームとして読み込み、S3にアップロードします。
    pub async fn put_object_from_file(
        &self,
        bucket: impl Into<String>,
        content_type: impl Into<String>,
        content_disposition: impl Into<String>,
        key: impl Into<String>,
        file_path: impl AsRef<Path>,
    ) -> Result<PutObjectOutput, Error> {
        let byte_stream = ByteStream::from_path(file_path).await?;

        self.put_object(bucket, content_type, content_disposition, key, byte_stream)
            .await
    }

    /// S3のファイルへのGETのpresigend requestのURLなどを取得します.
    ///
    /// URLだけほしい場合は、[`Self::get_presigned_url`]をお勧めします。
    pub async fn get_presigned(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<PresignedRequest, Error> {
        self.as_ref()
            .get_object()
            .bucket(bucket)
            .key(key)
            .presigned(PresigningConfig::builder().expires_in(expire).build()?)
            .await
            .map_err(from_aws_sdk_s3_error)
    }

    /// S3のファイルへのGETのpresigend requestのURLを取得します.
    ///
    /// Headerなどがほしい場合は、[`Self::get_presigned`]をお勧めします。
    pub async fn get_presigned_url(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<String, Error> {
        Ok(self
            .get_presigned(bucket, key, expire)
            .await?
            .uri()
            .to_owned())
    }

    /// S3のファイルへのPUTのpresigend requestのURLなどを取得します.
    ///
    /// URLだけほしい場合は、[`Self::put_presigned_url`]をお勧めします。
    pub async fn put_presigned(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<PresignedRequest, Error> {
        self.as_ref()
            .put_object()
            .bucket(bucket)
            .key(key)
            .presigned(PresigningConfig::builder().expires_in(expire).build()?)
            .await
            .map_err(from_aws_sdk_s3_error)
    }

    /// S3のファイルへのPUTのpresigend requestのURLを取得します.
    ///
    ///  Headerなどがほしい場合は、[`Self::put_presigned`]をお勧めします。
    pub async fn put_presigned_url(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
        expire: Duration,
    ) -> Result<String, Error> {
        Ok(self
            .put_presigned(bucket, key, expire)
            .await?
            .uri()
            .to_owned())
    }

    /// S3のファイルを削除します
    pub async fn delete(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<DeleteObjectOutput, Error> {
        self.as_ref()
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(from_aws_sdk_s3_error)
    }

    /// prefix以下の全てのファイルを削除します。
    pub async fn delete_by_prefix(
        &self,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Option<DeleteObjectsOutput>, Error> {
        let bucket = bucket.into();
        let mut page = self
            .as_ref()
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        let mut res = None::<DeleteObjectsOutput>;
        while let Some(next) = page.try_next().await.map_err(from_aws_sdk_s3_error)? {
            let Some(contents) = next.contents else {
                continue;
            };
            let map = contents
                .into_iter()
                .map(|content| ObjectIdentifier::builder().set_key(content.key).build())
                .collect::<Result<Vec<_>, _>>()
                .map_err(from_aws_sdk_s3_error)?;
            let mut output = self
                .as_ref()
                .delete_objects()
                .bucket(&bucket)
                .delete(
                    Delete::builder()
                        .set_objects(Some(map))
                        .build()
                        .map_err(from_aws_sdk_s3_error)?,
                )
                .send()
                .await
                .map_err(from_aws_sdk_s3_error)?;
            if let Some(ref mut prev) = res {
                merge(&mut prev.deleted, &mut output.deleted);
                merge(&mut prev.errors, &mut output.errors);
            } else {
                res = Some(output);
            };
        }
        Ok(res)
    }

    /// S3のファイルをコピーします.
    /// ```no_run
    /// # use s3_utils::*;
    /// # tokio_test::block_on(async {
    /// use futures_util::{StreamExt, TryStreamExt};
    /// let client = s3_utils::Client::from_env().await;
    /// client.copy_object("source_bucket", "source_key", "dest_bucket", "dest_key").await;
    /// # })
    /// ```
    pub async fn copy_object(
        &self,
        source_bucket: impl AsRef<[u8]>,
        source_key: impl AsRef<[u8]>,
        dst_bucket: impl Into<String>,
        dst_key: impl Into<String>,
    ) -> Result<aws_sdk_s3::operation::copy_object::CopyObjectOutput, Error> {
        let source = format!(
            "{}/{}",
            urlencoding::Encoded(source_bucket),
            urlencoding::Encoded(source_key.as_ref())
        );
        self.as_ref()
            .copy_object()
            .bucket(dst_bucket)
            .key(dst_key)
            .copy_source(source)
            .send()
            .await
            .map_err(from_aws_sdk_s3_error)
    }

    /// S3のオブジェクトを、prefix以下のものをまとめてcopyします.
    ///
    /// ```no_run
    /// # use s3_utils::*;
    /// # tokio_test::block_on(async {
    /// use futures_util::{StreamExt, TryStreamExt};
    /// let client = s3_utils::Client::from_env().await;
    /// client.copy_objects_by_prefix("source_bucket", "source_prefix", "dest_bucket", "dst_prefix").try_collect::<Vec<_>>().await;
    /// # })
    /// ```
    pub fn copy_objects_by_prefix<'a>(
        &'a self,
        source_bucket: &'a str,
        source_prefix: &'a str,
        dst_bucket: &'a str,
        dst_prefix: &'a str,
    ) -> impl TryStream<Ok = aws_sdk_s3::operation::copy_object::CopyObjectOutput, Error = Error> + 'a
    {
        self.ls_raw(source_bucket, source_prefix, |obj| Ok(obj.key).transpose())
            .and_then(move |key| {
                let dst_key = match key.strip_prefix(source_prefix) {
                    None => {
                        return futures_util::future::err(Error::UnexpectedNoPrefixKey)
                            .left_future()
                    }
                    Some(key) => format!("{}/{}", dst_prefix, key),
                };
                self.copy_object(source_bucket, key, dst_bucket, dst_key)
                    .right_future()
            })
    }
}

fn merge<T>(mut first: &mut Option<Vec<T>>, mut second: &mut Option<Vec<T>>) {
    match (&mut first, &mut second) {
        (None, None) => {}
        (None, Some(_)) => swap(first, second),
        (Some(_), None) => {}
        (Some(l), Some(r)) => l.append(r),
    }
}

#[derive(Debug)]
pub struct S3Object {
    content_type: String,
    buf: Vec<u8>,
}

impl S3Object {
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
    pub fn into_bytes(self) -> (String, Vec<u8>) {
        (self.content_type, self.buf)
    }

    pub fn into_string(self) -> Result<(String, String), std::string::FromUtf8Error> {
        Ok((self.content_type, String::from_utf8(self.buf)?))
    }

    pub fn into_base64_string(self) -> Result<(String, String), base64::EncodeSliceError> {
        use base64::Engine;
        Ok((
            self.content_type,
            base64::engine::general_purpose::STANDARD.encode(self.buf),
        ))
    }

    pub fn deserialize_json<T: DeserializeOwned>(self) -> Result<(String, T), serde_json::Error> {
        Ok((self.content_type, serde_json::from_slice(&self.buf)?))
    }
}

pub struct ObjectInfo {
    pub key: String,
    pub last_modified: Option<DateTime>,
    pub e_tag: Option<String>,
    pub size: Option<i64>,
}

#[cfg(feature = "chrono")]
impl ObjectInfo {
    pub fn last_modified_chrono(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        use aws_smithy_types_convert::date_time::DateTimeExt;
        self.last_modified.and_then(|lm| lm.to_chrono_utc().ok())
    }
}
