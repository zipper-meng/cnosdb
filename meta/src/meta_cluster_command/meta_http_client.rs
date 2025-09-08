use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::{Client, ClientBuilder, RequestBuilder};
use serde::de::DeserializeOwned;
pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    pub fn new() -> Self {
        let mut default_headers = HeaderMap::new();
        default_headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        HttpClient {
            client: ClientBuilder::new()
                .default_headers(default_headers)
                .build()
                .expect("build meta HTTP client"),
        }
    }

    pub async fn get_text(&self, url: &str) -> Result<String, String> {
        Self::fetch_response_text(self.client.get(url))
            .await
            .map_err(|e| http_error(url, &e))
    }

    pub async fn post_text(&self, url: &str, data: &str) -> Result<String, String> {
        Self::fetch_response_text(self.client.post(url).body(data.to_string()))
            .await
            .map_err(|e| http_error(url, &e))
    }

    pub async fn get<T>(&self, url: &str) -> Result<T, String>
    where
        T: DeserializeOwned,
    {
        Self::fetch_response(self.client.get(url))
            .await
            .map_err(|e| http_error(url, &e))
    }

    pub async fn post<T>(&self, url: &str, data: &str) -> Result<T, String>
    where
        T: DeserializeOwned,
    {
        Self::fetch_response(self.client.post(url).body(data.to_string()))
            .await
            .map_err(|e| http_error(url, &e))
    }

    async fn fetch_response_text(request: RequestBuilder) -> Result<String, String> {
        let resp = request
            .send()
            .await
            .map_err(|e| format!("send request: {e}"))?;

        let resp_status = resp.status();
        let resp_str = resp
            .text()
            .await
            .map_err(|e| format!("read response: {e}"))?;

        // println!("Response body: {resp_str}");

        if !resp_status.is_success() {
            return Err(format!(
                "Request failed, status: {resp_status}, response body: '{resp_str}'"
            ));
        }
        Ok(resp_str)
    }

    async fn fetch_response<T>(request: RequestBuilder) -> Result<T, String>
    where
        T: DeserializeOwned,
    {
        let resp_str = Self::fetch_response_text(request).await?;
        let result = serde_json::from_str::<T>(&resp_str)
            .map_err(|e| format!("deserialize response body: '{resp_str}', error: {e}"))?;
        Ok(result)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

fn http_error(url: &str, error: &str) -> String {
    format!("request {url} failed: {error}")
}
