use aws_sdk_s3 as s3;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum AppError {
    S3Error(Box<s3::Error>),
    CsvError(Box<dyn std::error::Error + Send + Sync>),
    AsyncError(Box<dyn std::error::Error + Send + Sync>),
}

impl From<::aws_sdk_s3::Error> for AppError {
    fn from(error: s3::Error) -> Self {
        AppError::S3Error(Box::new(error))
    }
}

impl From<::csv::Error> for AppError {
    fn from(error: csv::Error) -> Self {
        AppError::CsvError(Box::new(error))
    }
}

impl From<::tokio::task::JoinError> for AppError {
    fn from(error: tokio::task::JoinError) -> Self {
        AppError::AsyncError(Box::new(error))
    }
}

impl From<::aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_objects::DeleteObjectsError>>
    for AppError
{
    fn from(
        error: aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::delete_objects::DeleteObjectsError,
        >,
    ) -> Self {
        AppError::S3Error(Box::new(error.into()))
    }
}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::S3Error(source) => {
                write!(f, "S3Error: {}", &source)
            }
            AppError::CsvError(source) => {
                write!(f, "CsvError: {}", &source)
            }
            AppError::AsyncError(source) => {
                write!(f, "AsyncError: {}", &source)
            }
        }
    }
}

impl std::error::Error for AppError {}

pub type Result<T> = std::result::Result<T, AppError>;
