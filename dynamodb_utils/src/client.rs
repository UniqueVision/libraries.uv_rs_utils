use crate::{
    into_values::Number,
    sdk::{
        operation::{
            delete_item::DeleteItemOutput, delete_table::DeleteTableOutput,
            get_item::GetItemOutput, put_item::PutItemOutput, update_item::UpdateItemOutput,
            update_table::UpdateTableOutput,
        },
        types::{AttributeValue, ProvisionedThroughput},
        PaginationStreamExt,
    },
    utils::deserialize_stream,
    IntoValue,
};
use aws_sdk_dynamodb::{operation::create_table::CreateTableOutput, types::{AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType}};
use futures_util::{TryStream, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};

pub enum TableType {
    OnDemand,
    Provisioned(i64, i64),
}

/// awsのDynamoDbの高レベルなClient.
/// 低レベルな操作は[`raw_client`](`Client::raw_client`)を使って取得したものを使ってください
#[derive(Debug, Clone)]
pub struct Client<A = ()> {
    dynamodb: aws_sdk_dynamodb::Client,
    #[allow(dead_code)] // Todo: 後でautoscale対応を足す
    autoscale: A,
}

impl Client {
    /// [`aws_sdk_dynamodb::Client`]から[`Client`]を作ります
    pub fn from_dynamodb_client(dynamo: aws_sdk_dynamodb::Client) -> Self {
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
        Self::from_dynamodb_client(aws_sdk_dynamodb::Client::from_conf(conf.into()))
    }
}

impl<A> Client<A> {
    /// 内側のclientを取得する
    pub fn raw_client(&self) -> &aws_sdk_dynamodb::Client {
        &self.dynamodb
    }

    /// itemを取得します
    ///
    /// 生の値を取得します
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

    /// itemを取得して、デシリアライズされた形にします
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
                crate::serde_dynamo::aws_sdk_dynamodb_1::from_item(
                    value.item.ok_or(Error::NotFound)?,
                )
                .map_err(Into::into)
            })
    }

    /// itemを登録します
    /// 生のitemを登録します。
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

    /// itemを登録します
    /// シリアライズされます。
    pub async fn put_item<T: Serialize>(
        &self,
        table_name: impl Into<String>,
        data: T,
    ) -> Result<PutItemOutput, Error> {
        self.put_item_raw(
            table_name,
            crate::serde_dynamo::aws_sdk_dynamodb_1::to_item(data)?,
        )
        .await
    }

    /// itemを削除します。
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

    /// 特定のアイテムの特定の項目の値を登録、更新します
    /// この操作はatomicであることが保証されています。
    ///
    /// - `key_name` 更新対象のitemの、keyの項目名
    /// - `key_value` 更新対象のitemの、keyの値
    /// - `update_target` 更新対象の値の項目名
    /// - `value` 更新対象の値
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

    /// 特定のアイテムの特定の項目の数値を加算します。
    /// この操作はatomicであることが保証されています。
    ///
    /// - `key_name` 更新対象のitemの、keyの項目名
    /// - `key_value` 更新対象のitemの、keyの値
    /// - `update_target` 更新対象の値の項目名
    /// - `value` 更新対象の加算値
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

    /// scanを掛けます
    /// 具体的な型で受けたいなら[`scan_item`](`Self::scan_item`)があります。
    pub fn scan_item_raw(
        &self,
        table_name: impl Into<String>,
    ) -> impl TryStream<Ok = HashMap<String, AttributeValue>, Error = Error> {
        self.dynamodb
            .scan()
            .table_name(table_name)
            .into_paginator()
            .items()
            .send()
            .into_stream_03x()
            .map_err(from_aws_sdk_dynamodb_error)
    }

    /// scanを掛けます
    pub fn scan_item<T>(
        &self,
        table_name: impl Into<String>,
    ) -> impl TryStream<Ok = T, Error = Error>
    where
        for<'de> T: Deserialize<'de>,
    {
        deserialize_stream(self.scan_item_raw(table_name))
    }

    /// テーブルのスループット値を更新します
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

    // テーブルを削除します
    pub async fn delete_table(
        &self,
        table_name: impl Into<String>,
    ) -> Result<DeleteTableOutput, Error> {
        self.dynamodb
            .delete_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(from_aws_sdk_dynamodb_error)
    }

    pub async fn create_table(
        &self,
        table_name: impl Into<String>,
        key: impl Into<String>,
        sort_key: Option<impl Into<String>>,
        table_type: TableType,
    ) -> Result<CreateTableOutput, Error> {
        let key = key.into();
        let ad = AttributeDefinition::builder()
            .attribute_name(&key)
            .attribute_type(ScalarAttributeType::S)
            .build()?;

        let ks = KeySchemaElement::builder()
            .attribute_name(key)
            .key_type(KeyType::Hash)
            .build()?;

        let (ads, kss) = if let Some(sort_key) = sort_key {
            let sort_key = sort_key.into();
            let pair1= {
                let sort_key = AttributeDefinition::builder()
                    .attribute_name(&sort_key)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?;
                vec![ad, sort_key]
            };
            let pair2 = {
                let sort_key = KeySchemaElement::builder()
                .attribute_name(&sort_key)
                .key_type(KeyType::Range)
                .build()?;
                vec![ks, sort_key]
            };
            (pair1, pair2)
        } else {
            (vec![ad], vec![ks])
        };

        let table_builder = self
            .dynamodb
            .create_table()
            .table_name(table_name)
            .set_key_schema(Some(kss))
            .set_attribute_definitions(Some(ads));

        match table_type {
            TableType::OnDemand => {
                table_builder
                    .billing_mode(BillingMode::PayPerRequest)
                    .send()
                    .await
            }
            TableType::Provisioned(read_capacity, write_capacity) => {
                let pt = ProvisionedThroughput::builder()
                    .read_capacity_units(read_capacity)
                    .write_capacity_units(write_capacity)
                    .build()?;
                table_builder.provisioned_throughput(pt).send().await
            }
        }.map_err(|e| e.into())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    DynamoDb(Box<aws_sdk_dynamodb::Error>),
    #[error(transparent)]
    Serde(Box<crate::serde_dynamo::Error>),
    #[error("BuildError {0}")]
    BuildError(#[from] aws_sdk_dynamodb::error::BuildError),
    #[error("No Item")]
    NotFound,
    #[error("CreateTableError {0}")]
    CreateTableError(#[from] aws_sdk_dynamodb::error::SdkError<aws_sdk_dynamodb::operation::create_table::CreateTableError>),
}

pub(crate) fn from_aws_sdk_dynamodb_error(e: impl Into<aws_sdk_dynamodb::Error>) -> Error {
    Error::DynamoDb(Box::new(e.into()))
}

impl From<aws_sdk_dynamodb::Error> for Error {
    fn from(value: aws_sdk_dynamodb::Error) -> Self {
        from_aws_sdk_dynamodb_error(value)
    }
}

impl From<crate::serde_dynamo::Error> for Error {
    fn from(value: crate::serde_dynamo::Error) -> Self {
        Self::Serde(Box::new(value.into()))
    }
}
