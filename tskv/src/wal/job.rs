use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use trace::{error, info, warn};

use crate::background_task::BackgroundTask;
use crate::error::Result;
use crate::wal::{WalEntryType, WalManager, WalTask};
use crate::TseriesFamilyId;

pub fn run_wal_job(
    runtime: Arc<Runtime>,
    cancellation_token: CancellationToken,
    mut wal_manager: WalManager,
    mut receiver: Receiver<WalTask>,
) -> BackgroundTask<()> {
    info!("Job 'WAL' starting.");
    let join_handle = runtime.spawn(async move {
        info!("Job 'WAL' started.");

        let sync_interval = wal_manager.sync_interval();
        if sync_interval == Duration::ZERO {
            loop {
                tokio::select! {
                    wal_task = receiver.recv() => {
                        match wal_task {
                            Some(WalTask::Write { id, points, tenant, cb }) => on_write(&mut wal_manager, points, cb, id, tenant).await,
                            _ => break
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        on_cancel(wal_manager).await;
                        break;
                    }
                }
            }
        } else {
            let mut ticker = tokio::time::interval(sync_interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    wal_task = receiver.recv() => {
                        match wal_task {
                            Some(WalTask::Write { id, points, tenant, cb }) => on_write(&mut wal_manager, points, cb, id, tenant).await,
                            _ => break
                        }
                    }
                    _ = ticker.tick() => {
                        on_tick(&wal_manager).await;
                    }
                    _ = cancellation_token.cancelled() => {
                        on_cancel(wal_manager).await;
                        break;
                    }
                }
            }
        }
    });

    BackgroundTask::new("WAL".to_string(), join_handle, true)
}

async fn on_write(
    wal_manager: &mut WalManager,
    points: Arc<Vec<u8>>,
    cb: oneshot::Sender<Result<(u64, usize)>>,
    id: TseriesFamilyId,
    tenant: Arc<Vec<u8>>,
) {
    let ret = wal_manager
        .write(WalEntryType::Write, points, id, tenant)
        .await;
    if let Err(e) = cb.send(ret) {
        warn!("send WAL write result failed: {:?}", e);
    }
}

async fn on_tick(wal_manager: &WalManager) {
    if let Err(e) = wal_manager.sync().await {
        error!("Failed flushing WAL file: {:?}", e);
    }
}

async fn on_cancel(wal_manager: WalManager) {
    if let Err(e) = wal_manager.close().await {
        error!("Failed to close job 'WAL': {:?}", e);
    }
}
