use futures::TryStreamExt;
use serde::Deserialize;

pub async fn list_streams_impl(client: &eventstore::Client) -> rlua::Result<Vec<String>> {
    let options = eventstore::ReadStreamOptions::default()
        .position(eventstore::StreamPosition::Start)
        .forwards();

    let result = client.read_stream("$streams", &options).await;

    match result {
        Ok(stream) => stream
            .into_stream()
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

        Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
    }
}

pub async fn list_stream_events_impl(
    client: &eventstore::Client,
    stream_name: String,
) -> rlua::Result<Vec<serde_json::Value>> {
    let options = eventstore::ReadStreamOptions::default()
        .position(eventstore::StreamPosition::Start)
        .forwards()
        .resolve_link_tos();

    let result = client.read_stream(stream_name, &options).await;

    match result {
        Ok(stream) => stream
            .into_stream()
            .map_ok(|event| {
                let event = event.get_original_event();
                let position = serde_json::json!({
                    "commit": event.position.commit,
                    "prepare": event.position.prepare,
                });

                let mut data = None;

                let raw = if event.is_json {
                    std::str::from_utf8(event.data.as_ref()).unwrap().to_owned()
                } else {
                    base64::encode(event.data.as_ref())
                };

                if event.is_json {
                    if let Some(value) =
                        serde_json::from_slice::<serde_json::Value>(event.data.as_ref()).ok()
                    {
                        data = Some(value);
                    }
                }

                let custom_metadata;

                if let Some(value) =
                    serde_json::from_slice::<serde_json::Value>(event.custom_metadata.as_ref()).ok()
                {
                    custom_metadata = value;
                } else {
                    custom_metadata =
                        serde_json::Value::String(base64::encode(event.custom_metadata.as_ref()));
                }

                let value = serde_json::json!({
                    "stream_id": event.stream_id,
                    "id": event.id,
                    "revision": event.revision,
                    "event_type": event.event_type,
                    "is_json": event.is_json,
                    "raw": raw,
                    "data": data,
                    "metadata": event.metadata,
                    "custom_metadata": custom_metadata,
                    "position": position,
                });

                value
            })
            .try_collect::<Vec<serde_json::Value>>()
            .await
            .map_err(|e| rlua::Error::RuntimeError(e.to_string())),

        Err(e) => Err(rlua::Error::RuntimeError(e.to_string())),
    }
}

#[derive(Deserialize, Debug)]
struct NodeInfo {
    #[serde(rename = "esVersion")]
    version: String,
}

fn create_url(setts: &eventstore::ClientSettings) -> crate::Result<reqwest::Url> {
    let scheme = if setts.is_secure_mode_enabled() {
        "https"
    } else {
        "http"
    };

    let endpoint = setts
        .hosts()
        .get(0)
        .expect("eventstore lib already checked it!");

    let url = reqwest::Url::parse(
        format!("{}://{}:{}/info", scheme, endpoint.host, endpoint.port).as_str(),
    )?;

    Ok(url)
}

pub async fn server_version_impl(
    client: &reqwest::Client,
    setts: &eventstore::ClientSettings,
) -> crate::Result<String> {
    let node_info = client
        .get(create_url(&setts)?)
        .header("content-type", "application/json")
        .send()
        .await?
        .json::<NodeInfo>()
        .await?;

    Ok(node_info.version)
}

pub async fn emit_impl(
    client: &eventstore::Client,
    stream_name: String,
    event_type: String,
    payload: serde_json::Value,
) -> crate::Result<Option<eventstore::WrongExpectedVersion>> {
    let event = eventstore::EventData::json(event_type, payload)?;
    client
        .append_to_stream(stream_name, &Default::default(), event)
        .await?;

    Ok(None)
}

#[derive(Deserialize, Debug)]
pub struct Link {
    stream_id: String,
    revision: u64,
}

pub async fn link_impl(
    client: &eventstore::Client,
    stream_name: String,
    link: Link,
) -> crate::Result<Option<eventstore::WrongExpectedVersion>> {
    let event =
        eventstore::EventData::binary("$>", format!("{}@{}", link.revision, link.stream_id).into());

    client
        .append_to_stream(stream_name, &Default::default(), event)
        .await?;

    Ok(None)
}
