use bustubx::database::Database;
use std::io::Write;
use tracing::info;
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() {
    println!(":) Welcome to the bustubx, please input sql.");

    let fmt_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false);
    let (chrome_layer, _guard) = ChromeLayerBuilder::new().build();
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(chrome_layer)
        .init();

    let mut db = Database::new_temp();
    info!("database created");
    loop {
        print!(">");
        std::io::stdout().flush().unwrap();
        let mut input = String::new();
        match std::io::stdin().read_line(&mut input) {
            Ok(_) => {
                if input.trim() == "exit" {
                    break;
                }
                println!("output: {:?}", db.run(&input));
            }
            Err(_) => {
                println!("Error reading from stdin");
                continue;
            }
        }
    }
}
