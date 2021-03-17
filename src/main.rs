mod conversion;
mod history;
mod input;
mod lua_impl;
mod sql;

use crate::conversion::deserialize_repl_value;
use eventstore::{Client, ClientSettings, ClientSettingsParseError};
use input::Input;
use rlua::Lua;
use std::io;
use structopt::StructOpt;
use tokio::task::block_in_place;

type Result<A> = std::result::Result<A, Box<dyn std::error::Error>>;

#[derive(StructOpt, Debug)]
struct Params {
    #[structopt(short = "c",  long = "connection-string", default_value = "esdb://localhost:2113", parse(try_from_str = parse_connection_string))]
    conn_setts: ClientSettings,
}

fn parse_connection_string(
    input: &str,
) -> std::result::Result<ClientSettings, ClientSettingsParseError> {
    ClientSettings::parse_str(input)
}

static WELCOME: &'static str = "
 | |              (_)
 | |     ___  __ _ _  ___  _ __
 | |    / _ \\/ _` | |/ _ \\| '_ \\
 | |___|  __/ (_| | | (_) | | | |
 |______\\___|\\__, |_|\\___/|_| |_|
              __/ |
             |___/
";

#[tokio::main]
async fn main() -> crate::Result<()> {
    pretty_env_logger::init();
    let params = Params::from_args();
    let mut inputs = crate::input::Inputs::new();
    let client = Client::create(params.conn_setts.clone()).await?;
    let mut stdout = io::stdout();
    let lua = Lua::new();
    println!("{}", WELCOME);
    println!("Version: {}", clap::crate_version!());

    let es_client = client.clone();
    lua.context::<_, rlua::Result<()>>(move |context| {
        let http_client = reqwest::Client::new();
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
                    &inner_client,
                    stream_name,
                )) {
                    Ok(events) => rlua_serde::to_value(ctx, events),
                    Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
                }
            })
        })?;

        let server_version = context.create_function_mut(move |ctx, _: ()| {
            block_in_place(|| {
                match futures::executor::block_on(lua_impl::server_version_impl(
                    &http_client,
                    &params.conn_setts,
                )) {
                    Ok(version) => rlua_serde::to_value(ctx, version),
                    Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
                }
            })
        })?;

        let inner_client = client.clone();
        let emit = context.create_function_mut(
            move |_, (stream_name, event_type, payload): (String, String, rlua::Value)| {
                block_in_place(|| {
                    let value = rlua_serde::from_value(payload)?;
                    match futures::executor::block_on(lua_impl::emit_impl(
                        &inner_client,
                        stream_name,
                        event_type,
                        value,
                    )) {
                        Ok(result) => {
                            if let Some(e) = result {
                                return Err(rlua::Error::RuntimeError(e.to_string()));
                            }

                            Ok(())
                        }

                        Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
                    }
                })
            },
        )?;

        let inner_client = client.clone();
        let link = context.create_function_mut(
            move |_, (stream_name, payload): (String, rlua::Value)| {
                block_in_place(|| {
                    let link = rlua_serde::from_value(payload)?;
                    match futures::executor::block_on(lua_impl::link_impl(
                        &inner_client,
                        stream_name,
                        link,
                    )) {
                        Ok(result) => {
                            if let Some(e) = result {
                                return Err(rlua::Error::RuntimeError(e.to_string()));
                            }

                            Ok(())
                        }

                        Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
                    }
                })
            },
        )?;

        context.globals().set("streams", streams_fn)?;
        context.globals().set("stream_events", stream_events)?;
        context.globals().set("server_version", server_version)?;
        context.globals().set("emit", emit)?;
        context.globals().set("link", link)?;

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
                let result = lua
                    .context::<_, std::result::Result<String, rlua_serde::error::Error>>(
                        move |context| {
                            let value = context.load(line.as_str()).eval()?;
                            deserialize_repl_value(value)
                        },
                    );

                match result {
                    Ok(s) => {
                        println!("\n>>>\n{}", s);
                    }

                    Err(e) => {
                        println!("\nERR: {}", e);
                    }
                }
            }

            Input::Command(cmd) => match cmd {
                crate::input::Command::Load(path) => {
                    let content = tokio::fs::read(path).await?;
                    match std::str::from_utf8(content.as_slice()) {
                        Ok(source_code) => {
                            let result =
                                lua.context::<_, std::result::Result<(), rlua::Error>>(|context| {
                                    context.load(source_code).exec()?;

                                    Ok(())
                                });

                            if let Err(e) = result {
                                println!("\nERR: {}", e);
                            }
                        }

                        Err(e) => {
                            println!("\nERR: {}", e);
                        }
                    }
                }

                crate::input::Command::SqlQuery(stmts) => {
                    let plan = sql::build_plan(stmts);
                    if let Some(plan) = plan {
                        let result = sql::execute_plan(&es_client, plan).await?;
                        println!("\n>>>\n{}", serde_json::to_string_pretty(&result).unwrap());
                    }
                }
            },

            Input::Error(e) => {
                println!("\nERR: {}", e);
            }
        }
    }

    Ok(())
}
