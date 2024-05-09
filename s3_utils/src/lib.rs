use aws_sdk_s3::primitives::DateTime;
use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures_util::{TryStream, TryStreamExt};

#[derive(Debug, Clone)]
pub struct Client {
    s3: aws_sdk_s3::Client,
}

impl Client {
    pub fn from_s3_client(s3: aws_sdk_s3::Client) -> Self {
        Self { s3 }
    }

    pub async fn from_env() -> Self {
        let config = aws_config::from_env().load().await;
        Client::from_conf(&config)
    }

    pub fn from_conf<C: Into<aws_sdk_s3::Config>>(conf: C) -> Self {
        Self::from_s3_client(aws_sdk_s3::Client::from_conf(conf.into()))
    }
}

impl std::ops::Deref for Client {
    type Target = aws_sdk_s3::Client;

    fn deref(&self) -> &Self::Target {
        &self.s3
    }
}

impl Client {
    pub fn ls<T>(
        &self,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> impl TryStream<Ok = T, Error = aws_sdk_s3::Error>
    where
        T: TryFrom<aws_sdk_s3::types::Object>,
    {
        self.list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .into_paginator()
            .send()
            .into_stream_03x()
            .map_ok(|s| {
                futures_util::stream::iter(
                    s.contents
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|x| T::try_from(x).ok().map(|x| Ok(x))),
                )
            })
            .try_flatten()
    }

    pub async fn list_file_name(
        &self,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Vec<String>, aws_sdk_s3::Error> {
        self.ls::<KeyData>(bucket, prefix)
            .map_ok(|x| x.0)
            .try_collect()
            .await
    }
}

struct KeyData(String);
impl TryFrom<aws_sdk_s3::types::Object> for KeyData {
    type Error = ();

    fn try_from(value: aws_sdk_s3::types::Object) -> Result<Self, Self::Error> {
        Ok(Self(value.key.ok_or(())?))
    }
}

pub struct LsData {
    pub key: String,
    pub last_modified: Option<DateTime>,
    pub e_tag: Option<String>,
}

impl TryFrom<aws_sdk_s3::types::Object> for LsData {
    type Error = ();

    fn try_from(value: aws_sdk_s3::types::Object) -> Result<Self, Self::Error> {
        Ok(Self {
            key: value.key.ok_or(())?,
            last_modified: value.last_modified,
            e_tag: value.e_tag,
        })
    }
}
