use super::utils::http_save_to_file;

pub async fn dump(bind: &str, file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://{bind}/dump");
    http_save_to_file(&url, file).await
}
