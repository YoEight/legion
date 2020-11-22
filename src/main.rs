mod input;

use eventstore::{Client, ClientSettings, ClientSettingsParseError};
use input::Input;
use std::io;
use std::io::Write;
use structopt::StructOpt;
use termion::cursor::DetectCursorPos;
use termion::raw::IntoRawMode;

#[derive(StructOpt, Debug)]
struct Params {
    #[structopt(short = "c",  long = "connection-string", default_value = "esdb://localhost:2113", parse(try_from_str = parse_connection_string))]
    conn_setts: ClientSettings,
}

fn parse_connection_string(input: &str) -> Result<ClientSettings, ClientSettingsParseError> {
    ClientSettings::parse_str(input)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let params = Params::from_args();
    let mut inputs = crate::input::Inputs::new();
    // let _client = Client::create(params.conn_setts).await?;
    let mut stdout = io::stdout().into_raw_mode().unwrap();

    loop {
        let input = inputs.await_input(&mut stdout)?;
        let (_, y) = stdout.cursor_pos()?;

        match input {
            Input::Exit => {
                write!(stdout, "\n{}", termion::cursor::Goto(1, y + 1))?;
                break;
            }

            Input::String(line) => {
                write!(stdout, "\n{}>>> {}", termion::cursor::Goto(1, y + 1), line)?;
            }

            _ => {}
        }
    }

    Ok(())
}
