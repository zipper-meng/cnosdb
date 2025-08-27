use openraft::RaftMetrics;
use replication::{RaftNodeId, RaftNodeInfo};

use super::meta_http_client::HttpClient;

pub async fn show_nodes(bind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let http_client = HttpClient::new();
    let url = format!("http://{bind}/metrics");
    let body: RaftMetrics<RaftNodeId, RaftNodeInfo> =
        http_client.http_request_method("GET", &url, "").await?;

    let nodes = body
        .membership_config
        .membership()
        .nodes()
        .collect::<Vec<_>>();
    let term = body.current_term;
    let last_log_index = body.last_log_index.unwrap_or(0);
    let last_applied = body.last_applied.map(|log_id| log_id.index).unwrap_or(0);
    let leader = body.current_leader.unwrap_or(0);
    let members = body.membership_config.membership().get_joint_config();

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
        let state = if Some(*node_id) == body.current_leader {
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
