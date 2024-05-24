use std::collections::HashMap;

use aws_sdk_dynamodb::{
    operation::{
        delete_item::DeleteItemOutput, get_item::GetItemOutput, put_item::PutItemOutput,
        update_item::UpdateItemOutput,
    },
    primitives::Blob,
    types::{AttributeValue, AttributeValueUpdate},
};
use serde::{Deserialize, Serialize};

/// awsのS3の高レベルなClient.
/// 低レベルな操作は[`as_ref`](`AsRef::as_ref`)を使って取得したものを使ってください
#[derive(Debug, Clone)]
pub struct Client {
    dynamodb: aws_sdk_dynamodb::Client,
}

impl Client {
    /// [`aws_sdk_s3::Client`]から[`Client`]を作ります
    pub fn from_s3_client(dynamo: aws_sdk_dynamodb::Client) -> Self {
        Self { dynamodb: dynamo }
    }

    /// 環境変数から作ります
    pub async fn from_env() -> Self {
        let config = aws_config::from_env().load().await;
        Client::from_conf(&config)
    }

    /// コンフィグから作ります
    pub fn from_conf<C: Into<aws_sdk_dynamodb::Config>>(conf: C) -> Self {
        Self::from_s3_client(aws_sdk_dynamodb::Client::from_conf(conf.into()))
    }
}

impl Client {
    pub async fn get_item_raw(
        &self,
        table_name: impl Into<String>,
        key_name: impl Into<String>,
        key_value: impl IntoValue,
    ) -> Result<GetItemOutput, Error> {
        self.dynamodb
            .get_item()
            .table_name(table_name)
            .key(key_name, key_value.into_value())
            .send()
            .await
            .map_err(from_aws_sdk_dynamodb_error)
    }

    pub async fn get_item<T>(
        &self,
        table_name: impl Into<String>,
        key_name: impl Into<String>,
        key_value: impl IntoValue,
    ) -> Result<T, Error>
    where
        for<'de> T: Deserialize<'de>,
    {
        self.get_item_raw(table_name, key_name, key_value)
            .await
            .and_then(|value| {
                serde_dynamo::aws_sdk_dynamodb_1::from_item(value.item.ok_or(Error::NotFound)?)
                    .map_err(Into::into)
            })
    }

    pub async fn put_item_raw(
        &self,
        table_name: impl Into<String>,
        item: HashMap<String, AttributeValue>,
    ) -> Result<PutItemOutput, Error> {
        self.dynamodb
            .put_item()
            .table_name(table_name)
            .set_item(Some(item))
            .send()
            .await
            .map_err(from_aws_sdk_dynamodb_error)
    }

    pub async fn put_item<T: Serialize>(
        &self,
        table_name: impl Into<String>,
        data: T,
    ) -> Result<PutItemOutput, Error> {
        self.put_item_raw(table_name, serde_dynamo::aws_sdk_dynamodb_1::to_item(data)?)
            .await
    }

    pub async fn delete_item(
        &self,
        table_name: impl Into<String>,
        key_name: impl Into<String>,
        key_value: impl IntoValue,
    ) -> Result<DeleteItemOutput, Error> {
        self.dynamodb
            .delete_item()
            .table_name(table_name)
            .key(key_name, key_value.into_value())
            .send()
            .await
            .map_err(from_aws_sdk_dynamodb_error)
    }

    // pub async fn add_value(
    //     &self,
    //     table_name: impl Into<String>,
    //     key_name: impl Into<String>,
    //     key_value: impl IntoValue,
    // ) -> Result<UpdateItemOutput, Error> {
    //     self.dynamodb
    //         .update_item()
    //         .table_name(table_name)
    //         .key(key_name, key_value.into_value())
    //         .update_expression(input)
    //         .send()
    //         .await
    //         .map_err(from_aws_sdk_dynamodb_error)
    // }
}

pub trait IntoValue {
    fn into_value(self) -> AttributeValue;
}

pub trait Number: IntoValue {}

impl IntoValue for String {
    fn into_value(self) -> AttributeValue {
        AttributeValue::S(self)
    }
}

impl IntoValue for &str {
    fn into_value(self) -> AttributeValue {
        AttributeValue::S(self.into())
    }
}

macro_rules! num_into_value {
    ($($t: ty),*) => {
        $(
            impl IntoValue for $t {
                fn into_value(self) -> AttributeValue {
                    AttributeValue::N(self.to_string())
                }
            }

            impl Number for $t {}
        )*
    };
}

num_into_value!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, f32, f64);

impl IntoValue for Vec<u8> {
    fn into_value(self) -> AttributeValue {
        AttributeValue::B(Blob::new(self))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    DynamoDb(#[from] aws_sdk_dynamodb::Error),
    #[error(transparent)]
    Serde(#[from] serde_dynamo::Error),
    #[error("No Item")]
    NotFound,
}

pub(crate) fn from_aws_sdk_dynamodb_error(e: impl Into<aws_sdk_dynamodb::Error>) -> Error {
    Error::DynamoDb(e.into())
}
