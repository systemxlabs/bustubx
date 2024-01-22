use std::io::{self, Write};

use tracing::{debug, info, Level};
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{
    fmt, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
};

use crate::database::Database;

mod buffer;
mod catalog;
mod common;
mod database;
mod dbtype;
mod execution;
mod optimizer;
mod parser;
mod planner;
mod storage;

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

    let mut db = Database::new_on_disk("test.db");
    info!("database created");
    loop {
        print!(">");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                if input.trim() == "exit" {
                    break;
                }
                db.run(&input);
            }
            Err(_) => {
                println!("Error reading from stdin");
                continue;
            }
        }
    }
}
