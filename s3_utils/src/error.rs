#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    S3(Box<aws_sdk_s3::Error>),
    #[error(transparent)]
    PresigningConfig(#[from] aws_sdk_s3::presigning::PresigningConfigError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("No prefix in key")]
    UnexpectedNoPrefixKey,
}

pub(crate) fn from_aws_sdk_s3_error(e: impl Into<aws_sdk_s3::Error>) -> Error {
    Error::S3(Box::new(e.into()))
}

impl From<aws_sdk_s3::Error> for Error {
    fn from(value: aws_sdk_s3::Error) -> Self {
        Self::S3(Box::new(value))
    }
}
