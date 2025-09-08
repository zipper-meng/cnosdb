use openraft::RaftMetrics;
use replication::{RaftNodeId, RaftNodeInfo};

use super::meta_http_client::HttpClient;

pub async fn remove_node(bind: &str, addr: &str) -> Result<(), String> {
    let http_client = HttpClient::new();

    let cluster_metrics_url = format!("http://{bind}/metrics");
    let cluster_metrics_resp: RaftMetrics<RaftNodeId, RaftNodeInfo> =
        http_client.get(&cluster_metrics_url).await?;

    let leader_id = cluster_metrics_resp.vote.leader_id.node_id;
    let nodes = cluster_metrics_resp
        .membership_config
        .membership()
        .nodes()
        .collect::<Vec<_>>();
    let node_id_to_remove = nodes
        .iter()
        .find(|(_, v)| v.address == addr)
        .map(|(k, _)| **k)
        .ok_or_else(|| format!("Node with address {addr} not found in the cluster"))?;

    let change_membership_url = format!("http://{bind}/change-membership");
    let mut nodes_map = nodes;
    nodes_map.retain(|(id, _)| **id != node_id_to_remove);
    let change_membership_req = {
        let node_ids: Vec<u64> = nodes_map.iter().map(|(id, _)| **id).collect();
        serde_json::json!(node_ids).to_string()
    };
    let change_membership_resp = http_client
        .post_text(&change_membership_url, &change_membership_req)
        .await?;

    if !change_membership_resp.starts_with(r#"{"Ok":"#) {
        return Err(format!(
            "Error removing node {addr} from meta service at {bind}: {change_membership_resp}",
        )
        .into());
    }
    println!("Node {addr} removed successfully");

    if node_id_to_remove == leader_id {
        if let Some(new_leader_candidate) = nodes_map.iter().find_map(|(_, node_info)| {
            let new_addr = &node_info.address;
            if new_addr != addr {
                Some(new_addr.to_string())
            } else {
                None
            }
        }) {
            for _ in 0..100 {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let url = format!("http://{new_leader_candidate}/metrics");
                let resp: RaftMetrics<RaftNodeId, RaftNodeInfo> = http_client.get(&url).await?;
                let new_leader_id = resp.vote.leader_id.node_id;
                if let Some(new_leader_info) =
                    resp.membership_config.membership().get_node(&new_leader_id)
                {
                    let new_leader_addr = &new_leader_info.address;
                    if new_leader_id != node_id_to_remove {
                        println!("New leader address: {new_leader_addr}");
                        return Ok(());
                    }
                }
            }
        } else {
            eprintln!("No other nodes available to query for new leader.");
        }
    }
    Ok(())
}
