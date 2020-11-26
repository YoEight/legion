mod input;

use eventstore::{Client, ClientSettings, ClientSettingsParseError};
use input::Input;
use rlua::{prelude::LuaValue, Lua, TablePairs};
use std::collections::HashMap;
use std::io;
use std::io::Write;
use structopt::StructOpt;
use termion::cursor::DetectCursorPos;
use termion::raw::IntoRawMode;
use futures::{ TryStreamExt, StreamExt };

#[derive(StructOpt, Debug)]
struct Params {
    #[structopt(short = "c",  long = "connection-string", default_value = "esdb://localhost:2113", parse(try_from_str = parse_connection_string))]
    conn_setts: ClientSettings,
}

fn parse_connection_string(input: &str) -> Result<ClientSettings, ClientSettingsParseError> {
    ClientSettings::parse_str(input)
}

fn collect_expr_value(
    mut value: LuaValue,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let mut stack = Vec::<(
        HashMap<String, serde_json::Value>,
        String,
        TablePairs<String, LuaValue>,
    )>::new();

    loop {
        if let Some((mut obj, key, mut pairs)) = stack.pop() {
            match value {
                LuaValue::Boolean(bool) => {
                    let child_value = serde_json::to_value(bool)?;

                    obj.insert(key, child_value);
                }

                LuaValue::Integer(i) => {
                    let child_value = serde_json::to_value(i)?;

                    obj.insert(key, child_value);
                }

                LuaValue::Number(n) => {
                    let child_value = serde_json::to_value(n)?;

                    obj.insert(key, child_value);
                }

                LuaValue::String(s) => {
                    let child_value = serde_json::to_value(s.to_str()?)?;

                    obj.insert(key, child_value);
                }

                LuaValue::Error(e) => {
                    return Err(e.into());
                }

                LuaValue::Table(table) => {
                    let child = std::collections::HashMap::<String, serde_json::Value>::new();
                    let mut child_pairs = table.pairs::<String, LuaValue>();

                    if let Some((child_key, next)) = child_pairs.next().transpose()? {
                        value = next;
                        stack.push((obj, key, pairs));
                        stack.push((child, child_key, child_pairs));
                        continue;
                    }

                    let child_value = serde_json::to_value(child)?;

                    obj.insert(key, child_value);
                }

                _ => {}
            }

            if let Some((key, next)) = pairs.next().transpose()? {
                value = next;
                stack.push((obj, key, pairs));
                continue;
            }

            // let mut has_remaining_keys = false;
            // TODO - This needs to be done recursively.
            // The use case being when dealing with multiple level of inner objects.
            if let Some((mut parent, parent_key, mut parent_pairs)) = stack.pop() {
                let parent_value = serde_json::to_value(obj)?;
                parent.insert(parent_key, parent_value);

                if let Some((key, next)) = parent_pairs.next().transpose()? {
                    // has_remaining_keys = true;
                    value = next;
                    stack.push((parent, key, parent_pairs));
                    continue;
                }

                obj = parent;
            }

            /* if has_remaining_keys { */
            // continue;
            /* } */

            let value = serde_json::to_value(obj)?;

            return Ok(Some(value));
        } else {
            match value {
                LuaValue::Boolean(bool) => {
                    let value = serde_json::to_value(bool)?;

                    return Ok(Some(value));
                }

                LuaValue::Integer(i) => {
                    let value = serde_json::to_value(i)?;

                    return Ok(Some(value));
                }

                LuaValue::Number(n) => {
                    let value = serde_json::to_value(n)?;

                    return Ok(Some(value));
                }

                LuaValue::String(s) => {
                    let value = serde_json::to_value(s.to_str()?)?;

                    return Ok(Some(value));
                }

                LuaValue::Error(e) => {
                    return Err(e.into());
                }

                LuaValue::Table(table) => {
                    let obj = std::collections::HashMap::<String, serde_json::Value>::new();
                    let mut pairs = table.pairs::<String, LuaValue>();

                    if let Some((key, next)) = pairs.next().transpose()? {
                        value = next;
                        stack.push((obj, key, pairs));
                        continue;
                    }

                    let value = serde_json::to_value(obj)?;

                    return Ok(Some(value));
                }

                _ => break,
            }
        }
    }

    Ok(None)
}

async fn list_streams_impl(client: &eventstore::Client) -> rlua::Result<Vec<String>> {
    let result = client.read_stream("$streams")
        .start_from_beginning()
        .resolve_link_tos(eventstore::LinkTos::NoResolution)
        .read_through()
        .await;

    match result {
        Ok(result) => {
            match result {
                eventstore::ReadResult::Ok(result) => {
                    result
                        .map_ok(|event| {
                            let payload = event.get_original_event().data.clone();
                            let value = std::str::from_utf8(payload.as_ref()).unwrap().to_owned();
                 
                            value
                        }).try_collect::<Vec<String>>().await.map_err(|e| rlua::Error::RuntimeError(e.to_string()))
                }

                eventstore::ReadResult::StreamNotFound(_) => Err(rlua::Error::RuntimeError("$streams stream not found".to_string())),
            }
        }

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

    lua.context::<_, rlua::Result<()>>(move |context| {
        let streams_fn = context.create_function_mut(move |_, _: ()| {
            match runtime.block_on(list_streams_impl(&client)) {
                Ok(stream_names) => Ok(stream_names),
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
                let result = lua
                    .context::<_, Result<Option<serde_json::Value>, Box<dyn std::error::Error>>>(
                        move |context| {
                            let value = context.load(line.as_str()).eval()?;
                            collect_expr_value(value)
                        },
                    );

                match result {
                    Ok(result) => {
                        if let Some(result) = result.as_ref() {
                            println!("\n>>>\n{}", serde_json::to_string_pretty(result)?);
                        }
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
