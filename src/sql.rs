use futures::TryStreamExt;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Plan {
    stream_name: String,
    count: usize,
    fields: Vec<String>,
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            stream_name: "".to_string(),
            count: usize::MAX,
            fields: Vec::new(),
        }
    }
}

pub fn build_plan(mut stmts: Vec<sqlparser::ast::Statement>) -> Option<Plan> {
    let stmt = stmts.pop()?;
    let mut plan = Plan::default();

    if let sqlparser::ast::Statement::Query(query) = stmt {
        if let sqlparser::ast::SetExpr::Select(mut select) = query.body {
            for item in select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr) = item {
                    if let sqlparser::ast::Expr::Identifier(ident) = expr {
                        plan.fields.push(ident.value);
                    }
                }
            }

            let from = select.from.pop()?;

            if let sqlparser::ast::TableFactor::Table { mut name, ..} = from.relation {
                let name = name.0.pop()?;

                plan.stream_name = name.value;
            }

            return Some(plan)
        }
    }

    None
}

pub async fn execute_plan(client: &eventstore::Client, plan: Plan) -> eventstore::Result<Vec<serde_json::Value>> {
    let result = client.read_stream(plan.stream_name)
        .start_from_beginning()
        .read_through()
        .await?;

    if let Some(mut stream) = result.ok() {
        let mut events = vec![];

        while let Some(event) = stream.try_next().await? {
            let event = event.get_original_event();
            
            let mut json_payload = serde_json::from_slice::<HashMap<String, serde_json::Value>>(event.data.as_ref()).unwrap();
            let mut output = HashMap::new();

            if plan.fields.is_empty() {
                output = json_payload;
            } else {
                for field in plan.fields.iter() {
                    if let Some(value) = json_payload.remove(field) {
                        output.insert(field.clone(), value);
                    } else {
                        output.insert(field.clone(), serde_json::Value::Null);
                    }
                }
            }

            events.push(serde_json::to_value(output).expect("is valid json"));
        }

        return Ok(events);
    }

    Ok(vec![])
}
