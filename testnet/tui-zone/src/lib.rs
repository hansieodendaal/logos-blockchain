use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use lb_common_http_client::BasicAuthCredentials;
use lb_core::mantle::ops::channel::ChannelId;
use lb_key_management_system_service::keys::{ED25519_SECRET_KEY_SIZE, Ed25519Key};
use lb_zone_sdk::sequencer::{Error, InscriptionId, SequencerCheckpoint, ZoneSequencer};
use reqwest::Url;
use tracing_subscriber::{
    Layer as _, filter::LevelFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

const STATUS_CHECK_TIMEOUT: Duration = Duration::from_secs(1200); // 20 minutes

#[derive(Parser, Debug)]
#[command(about = "Terminal UI zone sequencer - publish text inscriptions")]
pub struct InscribeArgs {
    /// Logos blockchain node HTTP endpoint
    #[arg(long, default_value = "http://localhost:8080", env = "NODE_URL")]
    node_url: String,

    /// Path to the signing key file (created if it doesn't exist)
    #[arg(long, default_value = "sequencer.key", env = "KEY_PATH")]
    key_path: String,

    /// Path to the checkpoint file for crash recovery
    #[arg(long, default_value = "sequencer.checkpoint", env = "CHECKPOINT_PATH")]
    checkpoint_path: String,

    #[arg(long, env = "USERNAME")]
    username: Option<String>,

    #[arg(long, env = "PASSWORD")]
    password: Option<String>,
}

fn save_checkpoint(path: &Path, checkpoint: &SequencerCheckpoint) {
    let data = serde_json::to_vec(checkpoint).expect("failed to serialize checkpoint");
    fs::write(path, data).expect("failed to write checkpoint file");
}

fn load_checkpoint(path: &Path) -> Option<SequencerCheckpoint> {
    if !path.exists() {
        return None;
    }
    let data = fs::read(path).expect("failed to read checkpoint file");
    Some(serde_json::from_slice(&data).expect("failed to deserialize checkpoint"))
}

fn load_or_create_signing_key(path: &Path) -> Ed25519Key {
    if path.exists() {
        let key_bytes = fs::read(path).expect("failed to read key file");
        assert_eq!(
            key_bytes.len(),
            ED25519_SECRET_KEY_SIZE,
            "invalid key file: expected {} bytes, got {}",
            ED25519_SECRET_KEY_SIZE,
            key_bytes.len()
        );
        let key_array: [u8; ED25519_SECRET_KEY_SIZE] =
            key_bytes.try_into().expect("length already checked");
        Ed25519Key::from_bytes(&key_array)
    } else {
        let mut key_bytes = [0u8; ED25519_SECRET_KEY_SIZE];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut key_bytes);
        fs::write(path, key_bytes).expect("failed to write key file");
        Ed25519Key::from_bytes(&key_bytes)
    }
}

async fn wait_for_on_chain_status(
    sequencer: Arc<ZoneSequencer>,
    inscription_id: InscriptionId,
) -> Result<bool, Error> {
    match tokio::time::timeout(
        STATUS_CHECK_TIMEOUT,
        sequencer.wait_for_inclusion(inscription_id),
    )
    .await
    {
        Ok(Ok(())) => Ok(true),
        Ok(Err(e)) => Err(e),
        Err(_) => Ok(false),
    }
}

async fn persist_checkpoint_when_on_chain(
    msg: &str,
    sequencer: Arc<ZoneSequencer>,
    checkpoint_path: PathBuf,
    inscription_id: InscriptionId,
) {
    let start = tokio::time::Instant::now();
    let tx_hash = hex::encode(<[u8; 32]>::from(inscription_id));
    let msg = capped_msg(msg, 20);
    match wait_for_on_chain_status(Arc::<ZoneSequencer>::clone(&sequencer), inscription_id).await {
        Ok(true) => {
            println!(
                "\n    mined tx: {tx_hash}/'{msg}' after {:.2?}\n> ",
                start.elapsed()
            );
            match sequencer.checkpoint().await {
                Ok(checkpoint) => {
                    save_checkpoint(&checkpoint_path, &checkpoint);
                }
                Err(e) => {
                    println!(
                        "\n    error tx: {tx_hash}/'{msg}' failed to fetch & save checkpoint: {e}"
                    );
                }
            }
        }
        Ok(false) => {
            println!(
                "\n    warning tx: {tx_hash}/'{msg}' not on-chain after {:?}\n> ",
                start.elapsed()
            );
        }
        Err(e) => {
            println!(
                "\n    error tx: {tx_hash}/'{msg}' inclusion wait failed after {:?}: {e}\n> ",
                start.elapsed()
            );
        }
    }
}

pub async fn run(args: InscribeArgs) {
    let file_appender = tracing_appender::rolling::daily("logs", "tui-zone.log");
    let (log_writer, _log_guard) = tracing_appender::non_blocking(file_appender);

    let console_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_filter(LevelFilter::WARN);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(log_writer)
        .with_ansi(false)
        .with_filter(LevelFilter::DEBUG);

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();

    let node_url: Url = args.node_url.parse().expect("invalid node URL");
    let signing_key = load_or_create_signing_key(Path::new(&args.key_path));
    let channel_id = ChannelId::from(signing_key.public_key().to_bytes());

    println!("TUI Zone Sequencer");
    println!("  Node:       {node_url}");
    println!("  Key:        {}", args.key_path);
    println!("  Channel ID: {}", hex::encode(channel_id.as_ref()));
    println!();

    let checkpoint_path = Path::new(&args.checkpoint_path);
    let checkpoint = load_checkpoint(checkpoint_path);
    if checkpoint.is_some() {
        println!("  Restored checkpoint from {}", args.checkpoint_path);
    }

    let sequencer = Arc::new(ZoneSequencer::init(
        channel_id,
        signing_key,
        node_url,
        args.username
            .map(|username| BasicAuthCredentials::new(username, args.password.clone())),
        checkpoint,
    ));

    println!();
    println!("Type a message and press Enter to publish it as a zone block.");
    println!("Press Ctrl-D or type an empty line to exit.");
    println!();

    let stdin = std::io::stdin();
    let mut line = String::new();

    loop {
        print!("> ");
        std::io::stdout().flush().expect("failed to flush stdout");

        line.clear();
        let bytes_read = stdin.read_line(&mut line).expect("failed to read line");

        if bytes_read == 0 {
            println!();
            break;
        }

        let msg = line.trim_end().to_owned();
        if msg.is_empty() {
            break;
        }

        match sequencer.publish(msg.as_bytes().to_vec()).await {
            Ok(result) => {
                let tx_hash = hex::encode(<[u8; 32]>::from(result.inscription_id));
                println!("    published: {tx_hash}/'{}'", capped_msg(&msg, 20));

                let sequencer = Arc::clone(&sequencer);
                let checkpoint_path = checkpoint_path.to_path_buf();
                let inscription_id = result.inscription_id;
                let msg_for_task = msg.clone();

                tokio::spawn(async move {
                    persist_checkpoint_when_on_chain(
                        &msg_for_task,
                        sequencer,
                        checkpoint_path,
                        inscription_id,
                    )
                    .await;
                });
            }
            Err(e) => {
                println!("  error: {e}");
            }
        }
    }

    println!("Goodbye!");
}

fn capped_msg(msg: &str, len: usize) -> String {
    if msg.len() > len {
        format!("{}...", &msg.get(0..20).unwrap_or(msg))
    } else {
        msg.to_owned()
    }
}
