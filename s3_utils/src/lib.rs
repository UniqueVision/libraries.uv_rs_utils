mod client;
mod client_with_bucket;
mod error;

pub mod sdk {
    pub mod config {
        pub use aws_config::*;
    }
    pub use aws_sdk_s3::*;
}

pub use client::*;
pub use client_with_bucket::*;
pub use error::*;
