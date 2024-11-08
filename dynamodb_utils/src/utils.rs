use crate::{sdk::types::AttributeValue, Error};
use futures_util::{TryStream, TryStreamExt};
use serde::Deserialize;
use std::{collections::HashMap, future::ready};

pub fn deserialize_stream<T>(
    raw_stream: impl TryStream<Ok = HashMap<String, AttributeValue>, Error = Error>,
) -> impl TryStream<Ok = T, Error = Error>
where
    for<'de> T: Deserialize<'de>,
{
    raw_stream.and_then(|item| {
        ready(crate::serde_dynamo::aws_sdk_dynamodb_1::from_item(item).map_err(Into::into))
    })
}
