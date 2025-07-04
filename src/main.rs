use std::sync::mpsc::channel;

mod app;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (term_tx, term_rx) = channel();
    ctrlc::set_handler(move || {
        let _ = term_tx.send(());
    })?;

    let server = app::server::Server::new();
    server.start().await?;

    term_rx.recv()?;

    server.stop().await?;

    Ok(())
}
