use bustubx::{pretty_format_tuples, Database};
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};

fn main() -> Result<()> {
    env_logger::init();

    let mut db = Database::new_temp().unwrap();

    println!(":) Welcome to the bustubx, please input sql.");
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
                let result = db.run(&line);
                match result {
                    Ok(tuples) => {
                        if !tuples.is_empty() {
                            println!("{}", pretty_format_tuples(&tuples))
                        }
                    }
                    Err(e) => println!("{}", e),
                }
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
