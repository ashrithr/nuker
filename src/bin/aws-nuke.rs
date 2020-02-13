use aws_nuke::{config, nuke};
use {chrono::Utc, fern, log::debug};
fn main() {
    let (verbose, config_path) = config::parse_args();

    let level = match verbose {
        0 => log::LevelFilter::Error,
        1 => log::LevelFilter::Warn,
        2 => log::LevelFilter::Info,
        3 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}][{}] {}",
                record.module_path().unwrap(),
                record.line().unwrap(),
                Utc::now().to_rfc3339(),
                record.level(),
                message
            ))
        })
        .level(level)
        .level_for("rustls", log::LevelFilter::Info)
        .level_for("tokio_reactor", log::LevelFilter::Info)
        .level_for("hyper", log::LevelFilter::Info)
        .level_for("rusoto_core", log::LevelFilter::Info)
        .level_for("tokio_threadpool", log::LevelFilter::Info)
        .level_for("mio", log::LevelFilter::Info)
        .level_for("want", log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()
        .expect("could not set up logging");

    let config = config::parse_config_file(&config_path);

    debug!("{:?}", config);

    nuke::Nuke::new(config).run().unwrap()
}