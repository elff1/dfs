use std::sync::mpsc::channel;

mod app;
mod file_processor;
mod file_store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let (term_tx, term_rx) = channel();
    ctrlc::set_handler(move || {
        let _ = term_tx.send(());
    })?;

    let server = app::Server::new();
    server.start().await?;

    term_rx.recv()?;

    server.stop().await?;

    Ok(())
}
