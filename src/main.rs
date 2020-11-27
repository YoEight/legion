mod input;

use eventstore::{Client, ClientSettings, ClientSettingsParseError};
use futures::TryStreamExt;
use input::Input;
use rlua::{prelude::LuaValue, Lua, TablePairs};
use serde::{
    de::{SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::io;
use structopt::StructOpt;

/* macro_rules! tri { */
// ($e:expr) => {
//     match $e {
//         serde_json::error::Result::Ok(val) => val,
//         serde_json::error::Result::Err(err) => return serde_json::error::Result::Err(err),
//     }
// };
// ($e:expr,) => {
//     tri!($e)
/* }; */
// }

#[derive(StructOpt, Debug)]
struct Params {
    #[structopt(short = "c",  long = "connection-string", default_value = "esdb://localhost:2113", parse(try_from_str = parse_connection_string))]
    conn_setts: ClientSettings,
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    #[inline]
    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    #[inline]
    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    #[inline]
    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    #[inline]
    fn visit_f64<E>(self, value: f64) -> Result<Value, E> {
        Ok(serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number))
    }

    #[cfg(any(feature = "std", feature = "alloc"))]
    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(String::from(value))
    }

    #[cfg(any(feature = "std", feature = "alloc"))]
    #[inline]
    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    #[inline]
    fn visit_some<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    #[inline]
    fn visit_seq<V>(self, mut visitor: V) -> Result<Value, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut vec = Vec::new();

        while let Some(elem) = visitor.next_element()? {
            vec.push(elem);
        }

        Ok(Value::Array(vec))
    }

    #[cfg(any(feature = "std", feature = "alloc"))]
    fn visit_map<V>(self, mut visitor: V) -> Result<Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        match visitor.next_key_seed(KeyClassifier)? {
            #[cfg(feature = "arbitrary_precision")]
            Some(KeyClass::Number) => {
                let number: NumberFromString = visitor.next_value()?;
                Ok(Value::Number(number.value))
            }
            #[cfg(feature = "raw_value")]
            Some(KeyClass::RawValue) => {
                let value = visitor.next_value_seed(crate::raw::BoxedFromString)?;
                crate::from_str(value.get()).map_err(de::Error::custom)
            }
            Some(KeyClass::Map(first_key)) => {
                let mut values = Map::new();

                values.insert(first_key, tri!(visitor.next_value()));
                while let Some((key, value)) = tri!(visitor.next_entry()) {
                    values.insert(key, value);
                }

                Ok(Value::Object(values))
            }
            None => Ok(Value::Object(Map::new())),
        }
    }
}

struct REPLValue {
    value: serde_json::Value,
}

impl<'de> Deserialize<'de> for REPLValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = deserializer.deserialize_seq(ValueVisitor)?;

        Ok(REPLValue { value })
    }
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
                let result =
                    lua.context::<_, Result<REPLValue, rlua_serde::error::Error>>(move |context| {
                        let value = context.load(line.as_str()).eval()?;
                        REPLValue::deserialize(rlua_serde::de::Deserializer { value })
                    });

                match result {
                    Ok(result) => {
                        println!("\n>>>\n{}", serde_json::to_string_pretty(&result.value)?);
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
