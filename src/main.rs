use std::sync::mpsc::channel;

use clap::Parser;

mod app;
mod cli;
mod file_store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = cli::Cli::parse();
    match cli.commands {
        cli::Commands::Start => {
            let (term_tx, term_rx) = channel();
            ctrlc::set_handler(move || {
                let _ = term_tx.send(());
            })?;

            let server = app::Server::new(cli);
            server.start().await?;

            term_rx.recv()?;

            server.stop().await?;
        }
    }

    Ok(())
}
