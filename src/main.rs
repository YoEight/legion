mod conversion;
mod history;
mod input;
mod lua_impl;

use crate::conversion::deserialize_repl_value;
use eventstore::{Client, ClientSettings, ClientSettingsParseError};
use input::Input;
use rlua::Lua;
use std::io;
use structopt::StructOpt;
use tokio::task::block_in_place;

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
    let client = Client::create(params.conn_setts).await?;
    let mut stdout = io::stdout();
    let lua = Lua::new();

    lua.context::<_, rlua::Result<()>>(move |context| {
        let inner_client = client.clone();
        let streams_fn = context.create_function_mut(move |ctx, _: ()| {
            block_in_place(|| {
                match futures::executor::block_on(lua_impl::list_streams_impl(&inner_client)) {
                    Ok(stream_names) => rlua_serde::to_value(ctx, stream_names),
                    Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
                }
            })
        })?;

        let inner_client = client.clone();
        let stream_events = context.create_function_mut(move |ctx, stream_name| {
            block_in_place(|| {
                match futures::executor::block_on(lua_impl::list_stream_events_impl(
                    &inner_client.clone(),
                    stream_name,
                )) {
                    Ok(events) => rlua_serde::to_value(ctx, events),
                    Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
                }
            })
        })?;

        context.globals().set("streams", streams_fn)?;
        context.globals().set("stream_events", stream_events)?;

        Ok(())
    })?;

    loop {
        let input = block_in_place(|| inputs.await_input(&mut stdout))?;

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
