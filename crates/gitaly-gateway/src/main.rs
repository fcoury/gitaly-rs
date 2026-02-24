use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    gitaly_gateway::run_from_env().await
}
