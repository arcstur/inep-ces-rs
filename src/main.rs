mod files;

use files::Ces;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    Ces::ensure_all_data().await?;

    Ok(())
}
