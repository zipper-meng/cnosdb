use std::time::Duration;

use tokio::task::JoinHandle;
use trace::warn;

use crate::{Error, Result};

#[derive(Debug)]
pub struct BackgroundTask<T: Default> {
    name: String,
    join_handle: JoinHandle<T>,
    wait_close: bool,
}

impl<T: Default> BackgroundTask<T> {
    pub fn new(name: String, join_handle: JoinHandle<T>, wait_close: bool) -> Self {
        Self {
            name,
            join_handle,
            wait_close,
        }
    }

    pub async fn until_close(self) -> Result<T> {
        if self.wait_close {
            let mut ticker = tokio::time::interval(Duration::from_secs(10));
            ticker.tick().await;
            tokio::select! {
                 _ = ticker.tick() => {
                    warn!("Job '{}' has been waiting to close for {} seconds.", self.name, ticker.period().as_secs());
                 }
                ret = self.join_handle => {
                    return ret.map_err(|e| Error::CommonError {
                        reason: e.to_string(),
                    });
                }
            }
        }
        Ok(T::default())
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}
