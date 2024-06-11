pub use client::{Client, Error};
pub use into_values::IntoValue;

mod client;
mod into_values;

pub mod sdk {
    pub use aws_sdk_dynamodb::*;
    pub use aws_smithy_types_convert::stream::*;
}

pub mod sdk_config {
    pub use aws_config::*;
}

pub mod serde_dynamo {
    pub use serde_dynamo::*;
}
