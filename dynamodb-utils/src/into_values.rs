use std::fmt::Display;

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};

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
