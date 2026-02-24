use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    gitaly_rs::run_from_env().await
}
