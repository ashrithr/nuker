use tracing::{info, trace};

#[tokio::main]
async fn main() -> Result<(), failure::Error> {
    let args = nuker::parse_args();
    let config = nuker::parse_config_file(&args.config);

    setup_tracing(args.verbose);

    info!(
        message = "Nuker is starting",
        version = args.version.as_str()
    );
    trace!("{:#?}", config);

    let mut nuker = nuker::Nuker::new(config, args);

    nuker.run().await?;

    Ok(())
}

fn setup_tracing(verbose: u64) {
    use tracing::Level;
    use tracing_subscriber::{fmt::time::ChronoUtc, EnvFilter, FmtSubscriber};

    let level = match verbose {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let env_filter = EnvFilter::new(format!("nuker={}", level.to_string().to_lowercase()))
        .add_directive("hyper=error".parse().unwrap());

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_timer(ChronoUtc::with_format("%s".into()))
        .with_target(true)
        .with_env_filter(env_filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
}
