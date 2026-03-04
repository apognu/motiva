#[derive(Debug, thiserror::Error)]
pub enum MotivaError {
  #[error("invalid configuration: {0}")]
  ConfigError(String),
  #[error("missing index: {0}, make sure you ran the indexer")]
  MissingIndex(String),
  #[error("resource not found")]
  ResourceNotFound,
  #[error("invalid schema: {0}")]
  InvalidSchema(String),
  #[error(transparent)]
  IndexError(#[from] elasticsearch::Error),
  #[error(transparent)]
  OtherError(#[from] anyhow::Error),
}
