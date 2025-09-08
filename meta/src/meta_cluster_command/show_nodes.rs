use openraft::RaftMetrics;
use replication::{RaftNodeId, RaftNodeInfo};

use super::meta_http_client::HttpClient;

pub async fn show_nodes(bind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = HttpClient::new();
    let url = format!("http://{bind}/metrics");
    let resp: RaftMetrics<RaftNodeId, RaftNodeInfo> = http_client.get(&url).await?;

    let nodes = resp
        .membership_config
        .membership()
        .nodes()
        .collect::<Vec<_>>();
    let term = resp.current_term;
    let last_log_index = resp.last_log_index.unwrap_or(0);
    let last_applied = resp.last_applied.map(|log_id| log_id.index).unwrap_or(0);
    let leader = resp.current_leader.unwrap_or(0);
    let members = resp.membership_config.membership().get_joint_config();

    println!(
        "Node ID  Address         State     Term  Last_Log_index  Last_Applied  Leader  Members"
    );

    let members_str = members
        .iter()
        .map(|set| {
            let ids = set
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{ids}]")
        })
        .collect::<Vec<_>>()
        .join(", ");

    for (node_id, node_info) in nodes {
        let address = &node_info.address;
        let state = if Some(*node_id) == resp.current_leader {
            "Leader"
        } else {
            "Follower"
        };
        println!(
            "{node_id:<8} {address:<15} {state:<9} {term:<6} {last_log_index:<16} {last_applied:<12} {leader:<7} {members_str}"
        );
    }
    Ok(())
}
