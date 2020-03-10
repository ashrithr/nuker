use nuker::config;
use nuker::nuke;
use tracing::{error, trace};
use tracing_futures::Instrument;

#[tokio::main]
async fn main() {
    let args = config::parse_args();
    let config = config::parse_config_file(&args.config);

    setup_tracing(args.verbose);

    trace!("{:?}", config);

    let nuker = nuke::Nuker::new(config, args);

    match tokio::try_join!(nuker.run().instrument(tracing::trace_span!("nuke"))) {
        Ok(_) => {}
        Err(err) => error!("Encountered error: {:?}", err),
    }
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
