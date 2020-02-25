use aws_nuke::{config, nuke};
use fern;
use fern::colors::{Color, ColoredLevelConfig};
use log::debug;
fn main() {
    let args = config::parse_args();
    let config = config::parse_config_file(&args.config);

    setup_logging(args.verbose);

    debug!("{:?}", config);

    nuke::Nuke::new(config, args).run().unwrap()
}

fn setup_logging(verbose: u64) {
    let level = match verbose {
        0 => log::LevelFilter::Error,
        1 => log::LevelFilter::Warn,
        2 => log::LevelFilter::Info,
        3 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    let colors_line = ColoredLevelConfig::new()
        .error(Color::Red)
        .warn(Color::Yellow)
        .debug(Color::BrightBlack)
        .trace(Color::BrightBlack);

    let colors_level = colors_line.clone().info(Color::Blue);

    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{color_line}[{date}][{target}][{level}{color_line}] {message}\x1B[0m",
                color_line = format_args!(
                    "\x1B[{}m",
                    colors_line.get_color(&record.level()).to_fg_str()
                ),
                date = chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                target = record.target(),
                level = colors_level.color(record.level()),
                message = message,
            ))
        })
        .level(level)
        .level_for("rustls", log::LevelFilter::Info)
        .level_for("tokio_reactor", log::LevelFilter::Info)
        .level_for("hyper", log::LevelFilter::Info)
        .level_for("rusoto_core", log::LevelFilter::Info)
        .level_for("rusoto_signature::signature", log::LevelFilter::Info)
        .level_for("tokio_threadpool", log::LevelFilter::Info)
        .level_for("mio", log::LevelFilter::Info)
        .level_for("want", log::LevelFilter::Info)
        .chain(std::io::stdout())
        .apply()
        .expect("could not set up logging");

    debug!("finished setting up logging!");
}
