use tracing::{info, trace};
use tracing_futures::Instrument;

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let args = nuker::parse_args();
    let config = nuker::parse_config_file(&args.config);

    setup_tracing(args.verbose);

    info!(
        message = "Nuker is starting",
        version = args.version.as_str()
    );
    trace!("{:?}", config);

    let mut nuker = nuker::Nuker::new(config, args);

    nuker
        .run()
        .instrument(tracing::trace_span!("nuker"))
        .await?;

    Ok(())
}

fn setup_tracing(verbose: u64) {
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    let level = match verbose {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
}
