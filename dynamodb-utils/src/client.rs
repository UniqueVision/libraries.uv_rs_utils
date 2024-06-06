use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

use aws_sdk_dynamodb::{
    operation::{
        delete_item::DeleteItemOutput, get_item::GetItemOutput, put_item::PutItemOutput,
        update_item::UpdateItemOutput, update_table::UpdateTableOutput,
    },
    types::{AttributeValue, ProvisionedThroughput},
};

use crate::{into_values::Number, IntoValue};

/// awsのS3の高レベルなClient.
/// 低レベルな操作は[`as_ref`](`AsRef::as_ref`)を使って取得したものを使ってください
#[derive(Debug, Clone)]
pub struct Client<A = ()> {
    dynamodb: aws_sdk_dynamodb::Client,
    #[allow(dead_code)] // Todo: 後でautoscale対応を足す
    autoscale: A,
}

impl Client {
    /// [`aws_sdk_s3::Client`]から[`Client`]を作ります
    pub fn from_s3_client(dynamo: aws_sdk_dynamodb::Client) -> Self {
        Self {
            dynamodb: dynamo,
            autoscale: (),
        }
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

impl<A> Client<A> {
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

    pub async fn set_value(
        &self,
        table_name: impl Into<String>,
        key_name: impl Into<String>,
        key_value: impl IntoValue,
        update_target: impl Display,
        value: impl IntoValue,
    ) -> Result<UpdateItemOutput, Error> {
        self.dynamodb
            .update_item()
            .table_name(table_name)
            .key(key_name, key_value.into_value())
            .update_expression(format!("SET {update_target} = :val"))
            .expression_attribute_values("val", value.into_value())
            .send()
            .await
            .map_err(from_aws_sdk_dynamodb_error)
    }

    pub async fn add_value(
        &self,
        table_name: impl Into<String>,
        key_name: impl Into<String>,
        key_value: impl IntoValue,
        update_target: impl Display,
        value: impl Number,
    ) -> Result<UpdateItemOutput, Error> {
        self.dynamodb
            .update_item()
            .table_name(table_name)
            .key(key_name, key_value.into_value())
            .update_expression(format!("ADD {update_target} :val"))
            .expression_attribute_values("val", value.into_value())
            .send()
            .await
            .map_err(from_aws_sdk_dynamodb_error)
    }

    pub async fn update_provisioned_throughput(
        &self,
        table_name: impl Into<String>,
        read_capacity: i64,
        write_capacity: i64,
    ) -> Result<UpdateTableOutput, Error> {
        self.dynamodb
            .update_table()
            .table_name(table_name)
            .provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(read_capacity)
                    .write_capacity_units(write_capacity)
                    .build()
                    .map_err(from_aws_sdk_dynamodb_error)?,
            )
            .send()
            .await
            .map_err(from_aws_sdk_dynamodb_error)
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
