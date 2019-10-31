use raft::Error as RaftError;
use std::fmt;
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    Raft(RaftError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raft(e) => write!(f, "{}", e),
        }
    }
}
impl std::error::Error for Error {}
