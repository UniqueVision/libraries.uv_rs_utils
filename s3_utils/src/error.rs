#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    S3(#[from] aws_sdk_s3::Error),
    #[error(transparent)]
    PresigningConfig(#[from] aws_sdk_s3::presigning::PresigningConfigError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub(crate) fn from_aws_sdk_s3_error(e: impl Into<aws_sdk_s3::Error>) -> Error {
    Error::S3(e.into())
}
