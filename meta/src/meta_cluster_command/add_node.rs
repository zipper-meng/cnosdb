use openraft::error::{ClientWriteError, RaftError};
use openraft::raft::ClientWriteResponse;
use openraft::RaftMetrics;
use replication::{RaftNodeId, RaftNodeInfo, TypeConfig};
use tokio::time::{sleep, Duration};

use super::meta_http_client::HttpClient;
pub async fn add_node(bind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = HttpClient::new();

    let cluster_metrics_url = format!("http://{bind}/metrics");
    let cluster_metrics_resp: RaftMetrics<RaftNodeId, RaftNodeInfo> =
        http_client.get(&cluster_metrics_url).await?;
    let cluster_node_ids = cluster_metrics_resp
        .membership_config
        .membership()
        .nodes()
        .map(|(&id, _)| id)
        .collect::<Vec<_>>();

    let add_node_metrics_url = format!("http://{addr}/metrics");
    let add_node_metrics_resp: RaftMetrics<RaftNodeId, RaftNodeInfo> =
        http_client.get(&add_node_metrics_url).await?;
    let add_node_id = add_node_metrics_resp.id;

    let add_learner_url = format!("http://{bind}/add-learner");
    let add_learner_req = serde_json::json!([add_node_id, addr]).to_string();
    let add_learner_resp: Result<
        ClientWriteResponse<TypeConfig>,
        RaftError<u64, ClientWriteError<u64, RaftNodeInfo>>,
    > = http_client.post(&add_learner_url, &add_learner_req).await?;
    if let Err(err) = add_learner_resp {
        return Err(format!("Error adding node {addr} to meta service at {bind}: {err}",).into());
    }

    let change_membership_url = format!("http://{bind}/change-membership");
    let change_membership_req = {
        let mut node_ids = cluster_node_ids;
        node_ids.push(add_node_id);
        serde_json::to_string(&node_ids)?
    };
    let mut attempts = 0;
    let max_attempts = 3;
    while attempts < max_attempts {
        let change_membership_resp: Result<
            ClientWriteResponse<TypeConfig>,
            RaftError<u64, ClientWriteError<u64, RaftNodeInfo>>,
        > = http_client
            .post(&change_membership_url, &change_membership_req)
            .await?;
        if let Ok(_response) = change_membership_resp {
            println!("Node {addr} added and cluster membership updated successfully at {bind}.",);
            return Ok(());
        } else if let Err(err) = change_membership_resp {
            return Err(
                format!("Error adding node {addr} to meta service at {bind}: {err}",).into(),
            );
        }

        attempts += 1;
        if attempts == max_attempts {
            return Err(format!(
                "Error updating cluster membership at {bind}: {change_membership_resp:?}",
            )
            .into());
        }

        sleep(Duration::from_secs(10)).await;
    }

    Ok(())
}
