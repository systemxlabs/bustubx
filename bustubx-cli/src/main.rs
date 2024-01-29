use bustubx::database::Database;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use tracing::info;
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() -> Result<()> {
    println!(":) Welcome to the bustubx, please input sql.");
    /*
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
    */
    let mut db = Database::new_temp();
    info!("database created");
    let mut rl = DefaultEditor::new()?;
    loop {
        let readline = rl.readline("bustubx=#");
        match readline {
            Ok(line) => {
                let _ = rl.add_history_entry(line.as_str());
                if line == "exit" || line == "\\q" {
                    println!("bye!");
                    break;
                }
                let _ = db.run(&line);
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}
