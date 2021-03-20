use futures::TryStreamExt;
use std::collections::HashMap;
use sqlparser::ast::{ BinaryOperator, Expr};

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

type Env = HashMap<String, serde_json::Value>;

#[derive(Debug)]
pub struct ExecutionError(String);

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ExecutionError {}

enum SqlValue {
    Ident(String),
    Bool(bool),
    Number(i64),
    Float(f64),
    String(String),
    Null,
}

impl std::fmt::Display for SqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SqlValue::Ident(ref ident) => write!(f, "{}", ident),
            SqlValue::Bool(ref boolean) => write!(f, "{}", if *boolean { "TRUE" } else { "FALSE" }),
            SqlValue::Number(ref num) => write!(f, "{}", num),
            SqlValue::Float(ref num) => write!(f, "{}", num),
            SqlValue::String(ref value) => write!(f, "\"{}\"", value),
            SqlValue::Null => write!(f, "NULL"),
        }
    }
}

fn execute_predicate(env: &Env, predicate_expr: &Expr) -> Result<bool, ExecutionError> {
    match predicate_expr {
        Expr::BinaryOp { ref left, ref op, ref right } => execute_binary_predicate(env, left, op, right),
        _ => Err(ExecutionError("Invalid predicate expression".to_string())),
    }
}

fn execute_binary_predicate(env: &Env, mut left: &Expr, op: &BinaryOperator, mut right: &Expr) -> Result<bool, ExecutionError> {
    let mut left = collect_sql_value(left)?;
    let mut right = collect_sql_value(right)?;

    if let SqlValue::Ident(ident) = left {
        left = resolve_name(env, ident)?;
    }

    if let SqlValue::Ident(ident) = right {
        right = resolve_name(env, ident)?;
    }
    
    match (left, right) {
        (SqlValue::Number(left), SqlValue::Number(right)) => binary_op_number(left, op, right),
        (SqlValue::Float(left), SqlValue::Float(right)) => binary_op_float(left, op, right),
        (SqlValue::String(left), SqlValue::String(right)) => binary_op_string(left, op, right),
        (SqlValue::Bool(left), SqlValue::Bool(right)) => binary_op_bool(left, op, right),
        (SqlValue::Null, SqlValue::Null) => binary_op_null(op),
        (SqlValue::Null, _) => Ok(false),
        (_, SqlValue::Null) => Ok(false),
        (left, right) => Err(ExecutionError(format!("{} {} {}", left, op, right))),
    }
}

fn resolve_name(env: &Env, name: String) -> Result<SqlValue, ExecutionError> {
    if let Some(value) = env.get(&name) {
        return collect_json_literal(value);
    }
    
    Err(ExecutionError(format!("Unresolved symbol: {}", name)))
}

fn binary_op_number(left: i64, op: &BinaryOperator, right: i64) -> Result<bool, ExecutionError> {
    match op {
        &BinaryOperator::Eq => Ok(left == right),
        &BinaryOperator::NotEq => Ok(left != right),
        &BinaryOperator::Gt => Ok(left > right),
        &BinaryOperator::Lt => Ok(left < right),
        &BinaryOperator::GtEq => Ok(left >= right),
        &BinaryOperator::LtEq => Ok(left <= right),
        unsupported => Err(ExecutionError(format!("Unsupported binary operation on numbers: {}", unsupported))),
    }
}

fn binary_op_float(left: f64, op: &BinaryOperator, right: f64) -> Result<bool, ExecutionError> {
    match op {
        &BinaryOperator::Eq => Ok(left == right),
        &BinaryOperator::NotEq => Ok(left != right),
        &BinaryOperator::Gt => Ok(left > right),
        &BinaryOperator::Lt => Ok(left < right),
        &BinaryOperator::GtEq => Ok(left >= right),
        &BinaryOperator::LtEq => Ok(left <= right),
        unsupported => Err(ExecutionError(format!("Unsupported binary operation on floats: {}", unsupported))),
    }
}

fn binary_op_string(left: String, op: &BinaryOperator, right: String) -> Result<bool, ExecutionError> {
    match op {
        &BinaryOperator::Eq => Ok(left == right),
        &BinaryOperator::NotEq => Ok(left != right),
        &BinaryOperator::Gt => Ok(left > right),
        &BinaryOperator::Lt => Ok(left < right),
        &BinaryOperator::GtEq => Ok(left >= right),
        &BinaryOperator::LtEq => Ok(left <= right),
        &BinaryOperator::Like => Err(ExecutionError("Unsuppoted like/not like operations on strings".to_string())),
        &BinaryOperator::NotLike => Err(ExecutionError("Unsuppoted like/not like operations on strings".to_string())),
        unsupported => Err(ExecutionError(format!("Unsupported binary operation on strings: {}", unsupported))),
    }
}

fn binary_op_bool(left: bool, op: &BinaryOperator, right: bool) -> Result<bool, ExecutionError> {
    match op {
        &BinaryOperator::Eq => Ok(left == right),
        &BinaryOperator::NotEq => Ok(left != right),
        &BinaryOperator::And => Ok(left && right),
        &BinaryOperator::Or => Ok(left || right),
        unsupported => Err(ExecutionError(format!("Unsupported binary operation on booleans: {}", unsupported))),
    }
}

fn binary_op_null(op: &BinaryOperator) -> Result<bool, ExecutionError> {
    match op {
        &BinaryOperator::Eq => Ok(true),
        &BinaryOperator::NotEq => Ok(false),
        unsupported => Err(ExecutionError(format!("Unsupported binary operation on NULL: {}", unsupported))),
    }
}

fn collect_json_literal(value: &serde_json::Value) -> Result<SqlValue, ExecutionError> {
    match value {
        serde_json::Value::Null => Ok(SqlValue::Null),
        serde_json::Value::Bool(ref value) => Ok(SqlValue::Bool(*value)),
        serde_json::Value::String(ref value) => Ok(SqlValue::String(value.clone())),
        serde_json::Value::Number(ref value) => {
            if let Some(value) = value.as_i64() {
                return Ok(SqlValue::Number(value));
            }

            if let Some(value) = value.as_u64() {
                return Ok(SqlValue::Number(value as i64));
            }

            Ok(SqlValue::Float(value.as_f64().expect("to be f64")))
        }

        unsupported => Err(ExecutionError(format!("Unsupported JSON to SQL value conversion: {:?}", unsupported))),
    }
}

fn collect_sql_value(expr: &Expr) -> Result<SqlValue, ExecutionError> {
    match expr {
        Expr::Identifier(ref ident) => Ok(SqlValue::Ident(ident.value.clone())),
        Expr::Value(ref value) => collect_value(value),
        wrong => Err(ExecutionError(format!("Expected SQL value got: {}", wrong))),
    }
}

fn collect_value(value: &sqlparser::ast::Value) -> Result<SqlValue, ExecutionError> {
    match value {
        sqlparser::ast::Value::Number(ref value, ref is_long) => {
            if *is_long {
                return match value.parse::<i64>() {
                    Ok(value) => Ok(SqlValue::Number(value)),
                    Err(e) => Err(ExecutionError(format!("Invalid number value format: {}", e)))
                }
            }

            match value.parse::<f64>() {
                Ok(value) => Ok(SqlValue::Float(value)),
                Err(e) => Err(ExecutionError(format!("Invalid number value format: {}", e)))
            }
        }

        sqlparser::ast::Value::SingleQuotedString(ref value) => Ok(SqlValue::String(value.clone())),
        sqlparser::ast::Value::DoubleQuotedString(ref value) => Ok(SqlValue::String(value.clone())),
        sqlparser::ast::Value::Boolean(ref value) => Ok(SqlValue::Bool(*value)),
        sqlparser::ast::Value::Null => Ok(SqlValue::Null),
        wrong => Err(ExecutionError(format!("Expected SQL value got: {}", wrong))),
    }
}
