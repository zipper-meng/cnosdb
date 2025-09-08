use super::meta_http_client::HttpClient;

pub async fn meta_init(bind: &str) -> Result<(), String> {
    let http_client = HttpClient::new();

    let is_initialized_url = format!("http://{bind}/is_initialized");
    let is_initialized_resp = http_client.get_text(&is_initialized_url).await?;

    if is_initialized_resp.starts_with(r#"{"initialized": true"#) {
        println!("Cluster is already initialized at {bind}");
        return Ok(());
    }

    if is_initialized_resp.starts_with(r#"{"initialized": false"#) {
        let init_url = format!("http://{bind}/init");
        let init_resp = http_client.post_text(&init_url, "{}").await?;

        if !init_resp.starts_with(r#"{"Ok":"#) {
            return Err(format!("Error initializing cluster at {bind}: {init_resp}"));
        }

        println!("Cluster initialized successfully at {bind}");
        return Ok(());
    }

    Err(format!(
        "Internal error: unexpected response of url: '{is_initialized_url}', body: '{is_initialized_resp}'"
    ))
}
