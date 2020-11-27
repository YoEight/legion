mod conversion;
mod input;

use crate::conversion::deserialize_repl_value;
use eventstore::{Client, ClientSettings, ClientSettingsParseError};
use futures::TryStreamExt;
use input::Input;
use rlua::Lua;
use std::io;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct Params {
    #[structopt(short = "c",  long = "connection-string", default_value = "esdb://localhost:2113", parse(try_from_str = parse_connection_string))]
    conn_setts: ClientSettings,
}

fn parse_connection_string(input: &str) -> Result<ClientSettings, ClientSettingsParseError> {
    ClientSettings::parse_str(input)
}

async fn list_streams_impl(client: &eventstore::Client) -> rlua::Result<Vec<String>> {
    let result = client
        .read_stream("$streams")
        .start_from_beginning()
        .resolve_link_tos(eventstore::LinkTos::NoResolution)
        .read_through()
        .await;

    match result {
        Ok(result) => match result {
            eventstore::ReadResult::Ok(result) => result
                .map_ok(|event| {
                    let payload = event.get_original_event().data.clone();
                    let value = std::str::from_utf8(payload.as_ref()).unwrap().to_owned();
                    let value = value
                        .as_str()
                        .split("@")
                        .collect::<Vec<&str>>()
                        .last()
                        .unwrap()
                        .to_string();

                    value
                })
                .try_collect::<Vec<String>>()
                .await
                .map_err(|e| rlua::Error::RuntimeError(e.to_string())),

            eventstore::ReadResult::StreamNotFound(_) => Err(rlua::Error::RuntimeError(
                "$streams stream not found".to_string(),
            )),
        },

        Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let params = Params::from_args();
    let mut inputs = crate::input::Inputs::new();
    let mut runtime = tokio::runtime::Runtime::new()?;
    let client = runtime.block_on(Client::create(params.conn_setts))?;
    let mut stdout = io::stdout();
    let lua = Lua::new();

    let lended = client.clone();
    lua.context::<_, rlua::Result<()>>(move |context| {
        let streams_fn = context.create_function_mut(move |ctx, _: ()| {
            match runtime.block_on(list_streams_impl(&lended)) {
                Ok(stream_names) => rlua_serde::to_value(ctx, stream_names),
                Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
            }
        })?;

        context.globals().set("streams", streams_fn)?;

        Ok(())
    })?;

    loop {
        let input = inputs.await_input(&mut stdout)?;

        match input {
            Input::Exit => {
                println!();
                break;
            }

            Input::String(line) => {
                let result = lua.context::<_, Result<serde_json::Value, rlua_serde::error::Error>>(
                    move |context| {
                        let value = context.load(line.as_str()).eval()?;
                        deserialize_repl_value(value)
                    },
                );

                match result {
                    Ok(value) => {
                        println!("\n>>>\n{}", serde_json::to_string_pretty(&value)?);
                    }

                    Err(e) => {
                        println!("\n{}", e);
                    }
                }
            }

            _ => {}
        }
    }

    Ok(())
}
