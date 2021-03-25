use core::pin::Pin;
use futures::stream::BoxStream;
use futures::{Future, TryStreamExt};
use nom::character::complete::{anychar, char, satisfy};
use nom::combinator::{flat_map, opt, success};
use nom::multi::fold_many1;
use nom::{AsChar, IResult, InputIter, Slice};
use sqlparser::ast::{BinaryOperator, Expr, Ident, Query, UnaryOperator};
use std::collections::HashMap;
use std::ops::RangeFrom;

#[derive(Debug)]
pub struct Plan {
    stream_name: String,
    count: usize,
    fields: Vec<String>,
    predicate: Option<Expr>,
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            stream_name: "".to_string(),
            count: usize::MAX,
            fields: Vec::new(),
            predicate: None,
        }
    }
}

pub fn build_plan(mut stmts: Vec<sqlparser::ast::Statement>) -> Option<Plan> {
    let stmt = stmts.pop()?;

    if let sqlparser::ast::Statement::Query(query) = stmt {
        return build_plan_from_query(*query);
    }

    None
}

pub fn build_plan_from_query(query: Query) -> Option<Plan> {
    let mut plan = Plan::default();
    if let sqlparser::ast::SetExpr::Select(mut select) = query.body {
        for item in select.projection {
            if let sqlparser::ast::SelectItem::UnnamedExpr(expr) = item {
                if let sqlparser::ast::Expr::Identifier(ident) = expr {
                    plan.fields.push(ident.value);
                }
            }
        }

        let from = select.from.pop()?;

        if let sqlparser::ast::TableFactor::Table { mut name, .. } = from.relation {
            let name = name.0.pop()?;

            plan.stream_name = name.value;
        }

        plan.predicate = select.selection;

        return Some(plan);
    }

    None
}

pub async fn execute_plan<'a>(
    client: &'a eventstore::Client,
    plan: Plan,
) -> Result<BoxStream<'a, Result<serde_json::Value, ExecutionError>>, Box<dyn std::error::Error>> {
    let result = client
        .read_stream(plan.stream_name.as_str())
        .start_from_beginning()
        .read_through()
        .await?;

    if let Some(mut stream) = result.ok() {
        let output = async_stream::try_stream! {
            while let Ok(Some(event)) = stream.try_next().await {
                let event = event.get_original_event();

                let mut json_payload = serde_json::from_slice::<HashMap<String, serde_json::Value>>(event.data.as_ref()).unwrap();
                let mut line = HashMap::new();

                json_payload.insert("es_type".to_string(), serde_json::to_value(event.event_type.as_str()).expect("valid json"));
                json_payload.insert("es_stream_id".to_string(), serde_json::to_value(event.stream_id.as_str()).expect("valid json"));
                json_payload.insert("es_event_id".to_string(), serde_json::to_value(event.id.to_string()).expect("valid json"));
                json_payload.insert("es_revision".to_string(), serde_json::to_value(&event.revision).expect("valid json"));
                json_payload.insert("es_commit".to_string(), serde_json::to_value(&event.position.commit).expect("valid json"));
                json_payload.insert("es_prepare".to_string(), serde_json::to_value(&event.position.prepare).expect("valid json"));

                if let Some(expr) = plan.predicate.as_ref() {
                    let passed = execute_predicate(client, &json_payload, expr).await?;

                    if !passed {
                        continue;
                    }
                }

                if plan.fields.is_empty() {
                    line = json_payload;
                } else {
                    for field in plan.fields.iter() {
                        if let Some(value) = json_payload.remove(field) {
                            line.insert(field.clone(), value);
                        } else {
                            line.insert(field.clone(), serde_json::Value::Null);
                        }
                    }
                }

                yield serde_json::to_value(line).expect("valid json");
            }
        };

        let output: BoxStream<'a, Result<serde_json::Value, ExecutionError>> = Box::pin(output);

        return Ok(output);
    }

    Err(ExecutionError(format!("Stream {} doesn't exist.", plan.stream_name)).into())
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

#[derive(Debug)]
enum SqlValue {
    Ident(String),
    Bool(bool),
    Number(i64),
    Float(f64),
    String(String),
    Null,
}

impl SqlValue {
    fn into_expr(self) -> Expr {
        match self {
            SqlValue::Ident(s) => Expr::Identifier(Ident {
                value: s,
                quote_style: None,
            }),
            SqlValue::Bool(b) => Expr::Value(sqlparser::ast::Value::Boolean(b)),
            SqlValue::Number(n) => Expr::Value(sqlparser::ast::Value::Number(n.to_string(), true)),
            SqlValue::Float(n) => Expr::Value(sqlparser::ast::Value::Number(n.to_string(), false)),
            SqlValue::String(s) => Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)),
            SqlValue::Null => Expr::Value(sqlparser::ast::Value::Null),
        }
    }

    fn is_same_type(&self, expr: &SqlValue) -> bool {
        match (self, expr) {
            (SqlValue::Number(_), SqlValue::Number(_)) => true,
            (SqlValue::Float(_), SqlValue::Float(_)) => true,
            (SqlValue::String(_), SqlValue::String(_)) => true,
            (SqlValue::Bool(_), SqlValue::Bool(_)) => true,
            _ => false,
        }
    }

    fn is_null(&self) -> bool {
        match &self {
            SqlValue::Null => true,
            _ => false,
        }
    }

    fn from_expr(expr: Expr) -> Result<SqlValue, ExecutionError> {
        if let Expr::Value(value) = expr {
            return match value {
                sqlparser::ast::Value::Number(value, is_long) => {
                    if is_long {
                        return match value.parse::<i64>() {
                            Ok(value) => Ok(SqlValue::Number(value)),
                            Err(e) => Err(ExecutionError(format!(
                                "Invalid number value format: {}",
                                e
                            ))),
                        };
                    }

                    match value.parse::<f64>() {
                        Ok(value) => Ok(SqlValue::Float(value)),
                        Err(e) => Err(ExecutionError(format!(
                            "Invalid number value format: {}",
                            e
                        ))),
                    }
                }

                sqlparser::ast::Value::SingleQuotedString(value) => Ok(SqlValue::String(value)),
                sqlparser::ast::Value::DoubleQuotedString(value) => Ok(SqlValue::String(value)),
                sqlparser::ast::Value::Boolean(value) => Ok(SqlValue::Bool(value)),
                sqlparser::ast::Value::Null => Ok(SqlValue::Null),

                unsupported => Err(ExecutionError(format!(
                    "Unsuppored SQL literal: {}",
                    unsupported
                ))),
            };
        }

        Err(ExecutionError(format!(
            "Expected SQL literal but got: {}",
            expr
        )))
    }
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

struct Stack(Vec<StackElem>);

impl Stack {
    fn new() -> Self {
        Stack(Vec::new())
    }

    fn push(&mut self, expr: StackElem) {
        self.0.push(expr);
    }

    fn pop(&mut self) -> Result<StackElem, ExecutionError> {
        if let Some(expr) = self.0.pop() {
            return Ok(expr);
        }

        Err(ExecutionError("Unexpected end of stack".to_string()))
    }
}

enum StackElem {
    BinaryOp(BinaryOperator),
    UnaryOp(UnaryOperator),
    IsNull(bool),
    InList(bool),
    InSubquery(bool),
    Between(bool),
    Value(SqlValue),
    Expr(Expr),
    Return,
}

async fn simplify_expr(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    expr: Expr,
) -> Result<Expr, ExecutionError> {
    let mut params = Vec::<SqlValue>::new();

    // Initiate the execution loop.
    stack.push(StackElem::Return);
    stack.push(StackElem::Expr(expr));

    loop {
        match stack.pop()? {
            StackElem::Return => return Ok(params.pop().unwrap().into_expr()),
            StackElem::Value(value) => params.push(value),

            StackElem::Expr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let value = resolve_name(env, ident.value)?;

                    stack.push(StackElem::Value(value));
                }

                Expr::Value(value) => {
                    stack.push(StackElem::Value(collect_value(&value)?));
                }

                Expr::BinaryOp { left, op, right } => {
                    stack.push(StackElem::BinaryOp(op));
                    stack.push(StackElem::Expr(*left));
                    stack.push(StackElem::Expr(*right));
                }

                Expr::UnaryOp { op, expr } => {
                    stack.push(StackElem::UnaryOp(op));
                    stack.push(StackElem::Expr(*expr));
                }

                Expr::IsNull(expr) => {
                    stack.push(StackElem::IsNull(false));
                    stack.push(StackElem::Expr(*expr));
                }

                Expr::IsNotNull(expr) => {
                    stack.push(StackElem::IsNull(true));
                    stack.push(StackElem::Expr(*expr));
                }

                Expr::Between {
                    expr,
                    negated,
                    low,
                    high,
                } => {
                    stack.push(StackElem::Between(negated));
                    stack.push(StackElem::Expr(*expr));
                    stack.push(StackElem::Expr(*low));
                    stack.push(StackElem::Expr(*high));
                }

                Expr::InList {
                    expr,
                    list,
                    negated,
                } => {
                    stack.push(StackElem::InList(negated));
                    stack.push(StackElem::Expr(*expr));

                    for elem in list {
                        stack.push(StackElem::Expr(elem));
                    }
                }

                Expr::Nested(expr) => {
                    stack.push(StackElem::Expr(*expr));
                }

                Expr::InSubquery { .. } => {
                    unimplemented!()
                }

                expr => return Err(ExecutionError(format!("Unsupported expression: {}", expr))),
            },

            StackElem::IsNull(negated) => {
                let result = if let SqlValue::Null = params.pop().unwrap() {
                    true
                } else {
                    false
                };

                stack.push(StackElem::Value(SqlValue::Bool(!negated && result)));
            }

            StackElem::InList(negated) => {
                let mut result = false;
                let expr = params.pop().unwrap();

                while let Some(elem) = params.pop() {
                    if !expr.is_same_type(&elem) {
                        return Err(ExecutionError("IN LIST operation contains elements that have a different time than target expression".to_string()));
                    }

                    match (&expr, elem) {
                        (SqlValue::Number(ref x), SqlValue::Number(y)) => {
                            if *x == y {
                                result = true;
                                break;
                            }
                        }

                        (SqlValue::Float(ref x), SqlValue::Float(y)) => {
                            // We all know doing equality checks over floats is stupid but it is what it is.
                            if *x == y {
                                result = true;
                                break;
                            }
                        }

                        (SqlValue::String(ref x), SqlValue::String(ref y)) => {
                            if x == y {
                                result = true;
                                break;
                            }
                        }

                        (SqlValue::Bool(ref x), SqlValue::Bool(y)) => {
                            if *x == y {
                                result = true;
                                break;
                            }
                        }

                        _ => unreachable!(),
                    }
                }

                params.clear();
                stack.push(StackElem::Value(SqlValue::Bool(!negated && result)));
            }

            StackElem::Between(negated) => {
                let expr = params.pop().unwrap();
                let low = params.pop().unwrap();
                let high = params.pop().unwrap();

                let result = match (expr, low, high) {
                    (SqlValue::Number(value), SqlValue::Number(low), SqlValue::Number(high)) => {
                        Ok(value >= low && value <= high)
                    }
                    (SqlValue::Float(value), SqlValue::Float(low), SqlValue::Float(high)) => {
                        Ok(value >= low && value <= high)
                    }
                    (SqlValue::String(value), SqlValue::String(low), SqlValue::String(high)) => {
                        Ok(value >= low && value <= high)
                    }

                    (expr, low, high) => {
                        let negate_str = if negated { "NOT" } else { "" };

                        Err(ExecutionError(format!(
                            "Invalid between arguments: {} {} BETWEEN {} AND  {}",
                            expr, negate_str, low, high
                        )))
                    }
                }?;

                stack.push(StackElem::Value(SqlValue::Bool(!negated && result)));
            }

            StackElem::BinaryOp(op) => {
                let left = params.pop().unwrap();
                let right = params.pop().unwrap();

                if left.is_null() && right.is_null() && op == BinaryOperator::Spaceship {
                    stack.push(StackElem::Value(SqlValue::Bool(true)));

                    continue;
                }

                if (left.is_null() || right.is_null()) && op == BinaryOperator::Spaceship {
                    stack.push(StackElem::Value(SqlValue::Bool(false)));

                    continue;
                }
                
                if left.is_null() || right.is_null() {
                    stack.push(StackElem::Value(SqlValue::Null));
                    
                    continue;
                }

                let result = match (left, right, op) {
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Plus) => Ok(SqlValue::Number(left + right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Minus) => Ok(SqlValue::Number(left - right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Multiply) => Ok(SqlValue::Number(left * right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Divide) => Ok(SqlValue::Number(left / right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Modulus) => Ok(SqlValue::Number(left % right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::BitwiseOr) => Ok(SqlValue::Number(left | right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::BitwiseAnd) => Ok(SqlValue::Number(left & right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::BitwiseXor) => Ok(SqlValue::Number(left ^ right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Eq) => Ok(SqlValue::Bool(left == right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::NotEq) => Ok(SqlValue::Bool(left != right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Gt) => Ok(SqlValue::Bool(left > right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::GtEq) => Ok(SqlValue::Bool(left >= right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Lt) => Ok(SqlValue::Bool(left < right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::LtEq) => Ok(SqlValue::Bool(left <= right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Spaceship) => Ok(SqlValue::Bool(left == right)),

                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Plus) => Ok(SqlValue::Float(left + right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Minus) => Ok(SqlValue::Float(left - right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Multiply) => Ok(SqlValue::Float(left * right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Divide) => Ok(SqlValue::Float(left / right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Eq) => Ok(SqlValue::Bool(left == right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::NotEq) => Ok(SqlValue::Bool(left != right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Gt) => Ok(SqlValue::Bool(left > right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::GtEq) => Ok(SqlValue::Bool(left >= right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Lt) => Ok(SqlValue::Bool(left < right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::LtEq) => Ok(SqlValue::Bool(left <= right)),
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Spaceship) => Ok(SqlValue::Bool(left == right)),

                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::StringConcat) => Ok(SqlValue::String(format!("{}{}", left, right))),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Eq) => Ok(SqlValue::Bool(left == right)),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::NotEq) => Ok(SqlValue::Bool(left != right)),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Gt) => Ok(SqlValue::Bool(left > right)),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::GtEq) => Ok(SqlValue::Bool(left >= right)),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Lt) => Ok(SqlValue::Bool(left < right)),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::LtEq) => Ok(SqlValue::Bool(left <= right)),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Spaceship) => Ok(SqlValue::Bool(left == right)),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Like) => simplify_like(left, right, false),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::NotLike) => simplify_like(left, right, true),

                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::And) => Ok(SqlValue::Bool(left && right)),
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Or) => Ok(SqlValue::Bool(left || right)),
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Eq) => Ok(SqlValue::Bool(left == right)),
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::NotEq) => Ok(SqlValue::Bool(left != right)),
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Gt) => Ok(SqlValue::Bool(left > right)),
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::GtEq) => Ok(SqlValue::Bool(left >= right)),
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Lt) => Ok(SqlValue::Bool(left < right)),
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::LtEq) => Ok(SqlValue::Bool(left <= right)),
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Spaceship) => Ok(SqlValue::Bool(left == right)),

                    (left, right, op) => Err(ExecutionError(format!("Unsupported binary operation: {} {} {}", left, op, right))),
                }?;

                stack.push(StackElem::Value(result));
            }

            StackElem::UnaryOp(op) => {
                let expr = params.pop().unwrap();

                let result = match (expr, op) {
                    (SqlValue::Number(expr), UnaryOperator::Plus) => Ok(SqlValue::Number(expr)),
                    (SqlValue::Number(expr), UnaryOperator::Minus) => Ok(SqlValue::Number(-expr)),

                    (SqlValue::Float(expr), UnaryOperator::Plus) => Ok(SqlValue::Float(expr)),
                    (SqlValue::Float(expr), UnaryOperator::Minus) => Ok(SqlValue::Float(-expr)),

                    (SqlValue::Bool(expr), UnaryOperator::Not) => Ok(SqlValue::Bool(!expr)),

                    (expr, op) => Err(ExecutionError(format!("Unsupported unary operation: {} {}", op, expr))),
                }?;

                stack.push(StackElem::Value(result));
            }

            StackElem::InSubquery(_) => unimplemented!(),
        }
    }
}

fn is_comparison_operator(op: &BinaryOperator) -> bool {
    match op {
        BinaryOperator::Eq => true,
        BinaryOperator::NotEq => true,
        BinaryOperator::Gt => true,
        BinaryOperator::Lt => true,
        BinaryOperator::GtEq => true,
        BinaryOperator::LtEq => true,
        _ => false,
    }
}

fn is_algebraic_operator(op: &BinaryOperator) -> bool {
    match op {
        BinaryOperator::Plus => true,
        BinaryOperator::Minus => true,
        BinaryOperator::Multiply => true,
        BinaryOperator::Divide => true,
        _ => false,
    }
}

async fn simplify_binary_op(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    left: Expr,
    op: BinaryOperator,
    right: Expr,
) -> Result<Expr, ExecutionError> {
    let left = simplify_expr(stack, client, env, left).await?;
    let right = simplify_expr(stack, client, env, right).await?;
    let left = collect_sql_value(&left)?;
    let right = collect_sql_value(&right)?;

    //println!("DEBUG: {:?} {:?} {:?}", left, op, right);
    //println!("DEBUG: {} {} {}", left, op, right);

    match (left, op, right) {
        (SqlValue::Number(left), BinaryOperator::Plus, SqlValue::Number(right)) => {
            Ok(SqlValue::Number(left + right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Plus, SqlValue::Float(right)) => {
            Ok(SqlValue::Float(left + right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::Plus, SqlValue::Float(right)) => {
            Ok(SqlValue::Float(left as f64 + right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Plus, SqlValue::Number(right)) => {
            Ok(SqlValue::Float(left + right as f64).into_expr())
        }

        (SqlValue::Number(left), BinaryOperator::Minus, SqlValue::Number(right)) => {
            Ok(SqlValue::Number(left - right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Minus, SqlValue::Float(right)) => {
            Ok(SqlValue::Float(left - right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::Minus, SqlValue::Float(right)) => {
            Ok(SqlValue::Float(left as f64 - right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Minus, SqlValue::Number(right)) => {
            Ok(SqlValue::Float(left - right as f64).into_expr())
        }

        (SqlValue::Number(left), BinaryOperator::Multiply, SqlValue::Number(right)) => {
            Ok(SqlValue::Number(left * right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Multiply, SqlValue::Float(right)) => {
            Ok(SqlValue::Float(left * right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::Multiply, SqlValue::Float(right)) => {
            Ok(SqlValue::Float(left as f64 * right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Multiply, SqlValue::Number(right)) => {
            Ok(SqlValue::Float(left * right as f64).into_expr())
        }

        (SqlValue::Number(_), BinaryOperator::Divide, SqlValue::Number(right)) if right == 0 => {
            Err(ExecutionError("Divide by 0 error.".to_string()))
        }
        (SqlValue::Float(_), BinaryOperator::Divide, SqlValue::Number(right)) if right == 0 => {
            Err(ExecutionError("Divide by 0 error.".to_string()))
        }
        (SqlValue::Number(_), BinaryOperator::Divide, SqlValue::Float(right)) if right == 0f64 => {
            Err(ExecutionError("Divide by 0 error.".to_string()))
        }
        (SqlValue::Float(_), BinaryOperator::Divide, SqlValue::Float(right)) if right == 0f64 => {
            Err(ExecutionError("Divide by 0 error.".to_string()))
        }
        (SqlValue::Number(left), BinaryOperator::Divide, SqlValue::Number(right)) => {
            Ok(SqlValue::Number(left / right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Divide, SqlValue::Float(right)) => {
            Ok(SqlValue::Float(left / right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::Divide, SqlValue::Float(right)) => {
            Ok(SqlValue::Float(left as f64 / right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Divide, SqlValue::Number(right)) => {
            Ok(SqlValue::Float(left / right as f64).into_expr())
        }

        (SqlValue::Number(left), BinaryOperator::Modulus, SqlValue::Number(right)) => {
            Ok(SqlValue::Number(left % right).into_expr())
        }

        (SqlValue::String(left), BinaryOperator::StringConcat, SqlValue::String(right)) => {
            Ok(SqlValue::String(format!("{}{}", left, right)).into_expr())
        }

        (SqlValue::Number(left), BinaryOperator::Gt, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left > right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Gt, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left > right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::Gt, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left as f64 > right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Gt, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left > right as f64).into_expr())
        }
        (SqlValue::String(left), BinaryOperator::Gt, SqlValue::String(right)) => {
            Ok(SqlValue::Bool(left > right).into_expr())
        }
        (SqlValue::Bool(left), BinaryOperator::Gt, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left > right).into_expr())
        }
        (SqlValue::Null, BinaryOperator::Gt, SqlValue::Null) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::Number(_), BinaryOperator::Gt, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Float(_), BinaryOperator::Gt, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::String(_), BinaryOperator::Gt, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Bool(_), BinaryOperator::Gt, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Gt, SqlValue::Number(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Gt, SqlValue::Float(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Gt, SqlValue::String(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Gt, SqlValue::Bool(_)) => Ok(SqlValue::Null.into_expr()),

        (SqlValue::Number(left), BinaryOperator::Lt, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left < right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Lt, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left < right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::Lt, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool((left as f64) < right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Lt, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left < right as f64).into_expr())
        }
        (SqlValue::String(left), BinaryOperator::Lt, SqlValue::String(right)) => {
            Ok(SqlValue::Bool(left < right).into_expr())
        }
        (SqlValue::Bool(left), BinaryOperator::Lt, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left < right).into_expr())
        }
        (SqlValue::Null, BinaryOperator::Lt, SqlValue::Null) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::Number(_), BinaryOperator::Lt, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Float(_), BinaryOperator::Lt, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::String(_), BinaryOperator::Lt, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Bool(_), BinaryOperator::Lt, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Lt, SqlValue::Number(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Lt, SqlValue::Float(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Lt, SqlValue::String(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Lt, SqlValue::Bool(_)) => Ok(SqlValue::Null.into_expr()),

        (SqlValue::Number(left), BinaryOperator::GtEq, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left >= right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::GtEq, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left >= right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::GtEq, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left as f64 >= right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::GtEq, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left >= right as f64).into_expr())
        }
        (SqlValue::String(left), BinaryOperator::GtEq, SqlValue::String(right)) => {
            Ok(SqlValue::Bool(left >= right).into_expr())
        }
        (SqlValue::Bool(left), BinaryOperator::GtEq, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left >= right).into_expr())
        }
        (SqlValue::Null, BinaryOperator::GtEq, SqlValue::Null) => {
            Ok(SqlValue::Bool(true).into_expr())
        }
        (SqlValue::Number(_), BinaryOperator::GtEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Float(_), BinaryOperator::GtEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::String(_), BinaryOperator::GtEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Bool(_), BinaryOperator::GtEq, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::GtEq, SqlValue::Number(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::GtEq, SqlValue::Float(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::GtEq, SqlValue::String(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::GtEq, SqlValue::Bool(_)) => Ok(SqlValue::Null.into_expr()),

        (SqlValue::Number(left), BinaryOperator::LtEq, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left <= right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::LtEq, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left <= right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::LtEq, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left as f64 <= right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::LtEq, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left <= right as f64).into_expr())
        }
        (SqlValue::String(left), BinaryOperator::LtEq, SqlValue::String(right)) => {
            Ok(SqlValue::Bool(left <= right).into_expr())
        }
        (SqlValue::Bool(left), BinaryOperator::LtEq, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left <= right).into_expr())
        }
        (SqlValue::Null, BinaryOperator::LtEq, SqlValue::Null) => {
            Ok(SqlValue::Bool(true).into_expr())
        }
        (SqlValue::Number(_), BinaryOperator::LtEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Float(_), BinaryOperator::LtEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::String(_), BinaryOperator::LtEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Bool(_), BinaryOperator::LtEq, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::LtEq, SqlValue::Number(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::LtEq, SqlValue::Float(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::LtEq, SqlValue::String(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::LtEq, SqlValue::Bool(_)) => Ok(SqlValue::Null.into_expr()),

        (SqlValue::Number(left), BinaryOperator::Eq, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Eq, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::String(left), BinaryOperator::Eq, SqlValue::String(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Bool(left), BinaryOperator::Eq, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::Eq, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left as f64 == right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Eq, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left == right as f64).into_expr())
        }
        (SqlValue::Number(_), BinaryOperator::Eq, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Float(_), BinaryOperator::Eq, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::String(_), BinaryOperator::Eq, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Bool(_), BinaryOperator::Eq, SqlValue::Null) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Eq, SqlValue::Number(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Eq, SqlValue::Float(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Eq, SqlValue::String(_)) => Ok(SqlValue::Null.into_expr()),
        (SqlValue::Null, BinaryOperator::Eq, SqlValue::Bool(_)) => Ok(SqlValue::Null.into_expr()),

        (SqlValue::Number(left), BinaryOperator::Spaceship, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Spaceship, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::String(left), BinaryOperator::Spaceship, SqlValue::String(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Bool(left), BinaryOperator::Spaceship, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::Spaceship, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left as f64 == right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::Spaceship, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left == right as f64).into_expr())
        }
        (SqlValue::Null, BinaryOperator::Spaceship, SqlValue::Null) => {
            Ok(SqlValue::Bool(true).into_expr())
        }
        (SqlValue::Number(_), BinaryOperator::Spaceship, SqlValue::Null) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::Float(_), BinaryOperator::Spaceship, SqlValue::Null) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::String(_), BinaryOperator::Spaceship, SqlValue::Null) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::Bool(_), BinaryOperator::Spaceship, SqlValue::Null) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::Null, BinaryOperator::Spaceship, SqlValue::Number(_)) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::Null, BinaryOperator::Spaceship, SqlValue::Float(_)) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::Null, BinaryOperator::Spaceship, SqlValue::String(_)) => {
            Ok(SqlValue::Bool(false).into_expr())
        }
        (SqlValue::Null, BinaryOperator::Spaceship, SqlValue::Bool(_)) => {
            Ok(SqlValue::Bool(false).into_expr())
        }

        (SqlValue::Number(left), BinaryOperator::NotEq, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::NotEq, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::String(left), BinaryOperator::NotEq, SqlValue::String(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Bool(left), BinaryOperator::NotEq, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left == right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::NotEq, SqlValue::Float(right)) => {
            Ok(SqlValue::Bool(left as f64 == right).into_expr())
        }
        (SqlValue::Float(left), BinaryOperator::NotEq, SqlValue::Number(right)) => {
            Ok(SqlValue::Bool(left == right as f64).into_expr())
        }
        (SqlValue::Number(_), BinaryOperator::NotEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Float(_), BinaryOperator::NotEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::String(_), BinaryOperator::NotEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Bool(_), BinaryOperator::NotEq, SqlValue::Null) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::NotEq, SqlValue::Number(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::NotEq, SqlValue::Float(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::NotEq, SqlValue::String(_)) => {
            Ok(SqlValue::Null.into_expr())
        }
        (SqlValue::Null, BinaryOperator::NotEq, SqlValue::Bool(_)) => {
            Ok(SqlValue::Null.into_expr())
        }

        (SqlValue::Bool(left), BinaryOperator::And, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left && right).into_expr())
        }

        (SqlValue::Bool(left), BinaryOperator::Or, SqlValue::Bool(right)) => {
            Ok(SqlValue::Bool(left || right).into_expr())
        }

        (SqlValue::String(left), BinaryOperator::Like, SqlValue::String(right)) => {
            simplify_like(left, right, false).map(|c| c.into_expr())
        }
        (SqlValue::Null, BinaryOperator::Like, SqlValue::String(_)) => {
            Ok(SqlValue::Bool(false).into_expr())
        }

        (SqlValue::String(left), BinaryOperator::NotLike, SqlValue::String(right)) => {
            simplify_like(left, right, false).map(|s| s.into_expr())
        }
        (SqlValue::Null, BinaryOperator::NotLike, SqlValue::String(_)) => {
            Ok(SqlValue::Bool(false).into_expr())
        }

        (SqlValue::Number(left), BinaryOperator::BitwiseOr, SqlValue::Number(right)) => {
            Ok(SqlValue::Number(left | right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::BitwiseAnd, SqlValue::Number(right)) => {
            Ok(SqlValue::Number(left & right).into_expr())
        }
        (SqlValue::Number(left), BinaryOperator::BitwiseXor, SqlValue::Number(right)) => {
            Ok(SqlValue::Number(left ^ right).into_expr())
        }

        (left, op, right) => Err(ExecutionError(format!(
            "Unsupported binary operation: {} {} {}",
            left, op, right
        ))),
    }
}

// Very simplistic implementation.
fn simplify_like(left: String, right: String, negated: bool) -> Result<SqlValue, ExecutionError> {
    let (tpe, content) = parse_like_expr(right)?;

    match tpe {
        Like::StartWith => {
            Ok(SqlValue::Bool(!negated && left.starts_with(content.as_str())))
        }
        Like::EndWith => {
            Ok(SqlValue::Bool(!negated && left.ends_with(content.as_str())))
        }
        Like::Contains => {
            Ok(SqlValue::Bool(!negated && left.contains(content.as_str())))
        }
    }
}

#[derive(Clone)]
enum Like {
    StartWith,
    Contains,
    EndWith,
}

fn parse_like_expr(input: String) -> Result<(Like, String), ExecutionError> {
    let mut start = false;
    let mut end = false;
    let mut content = String::new();

    for (idx, c) in input.char_indices() {
        if idx == 0 && c == '%' {
            start = true;
            continue;
        }

        if idx == input.len() - 1 && c == '%' {
            end = true;
            continue;
        }

        content.push(c);
    }

    if start && end {
        return Ok((Like::Contains, content));
    }

    if start {
        return Ok((Like::EndWith, content));
    }

    if end {
        return Ok((Like::StartWith, content));
    }

    Err(ExecutionError(format!(
        "Malformed (NOT) LIKE expression: {}",
        input
    )))
}

async fn simplify_unary_op(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    op: UnaryOperator,
    expr: Expr,
) -> Result<Expr, ExecutionError> {
    let expr = simplify_expr(stack, client, env, expr).await?;
    let expr = collect_sql_value(&expr)?;

    match (op, expr) {
        (UnaryOperator::Plus, SqlValue::Number(expr)) => Ok(SqlValue::Number(expr).into_expr()),
        (UnaryOperator::Plus, SqlValue::Float(expr)) => Ok(SqlValue::Float(expr).into_expr()),

        (UnaryOperator::Minus, SqlValue::Number(expr)) => Ok(SqlValue::Number(-expr).into_expr()),
        (UnaryOperator::Minus, SqlValue::Float(expr)) => Ok(SqlValue::Float(-expr).into_expr()),

        (UnaryOperator::Not, SqlValue::Bool(boolean)) => Ok(SqlValue::Bool(!boolean).into_expr()),

        (op, expr) => Err(ExecutionError(format!(
            "Unsupported unary operation: {} {}",
            op, expr
        ))),
    }
}

async fn simplify_is_null(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    expr: Expr,
) -> Result<Expr, ExecutionError> {
    let expr = simplify_expr(stack, client, env, expr).await?;
    let expr = collect_sql_value(&expr)?;

    match expr {
        SqlValue::Null => Ok(SqlValue::Bool(true).into_expr()),
        _ => Ok(SqlValue::Bool(false).into_expr()),
    }
}

async fn simplify_is_not_null(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    expr: Expr,
) -> Result<Expr, ExecutionError> {
    let expr = simplify_expr(stack, client, env, expr).await?;
    let expr = collect_sql_value(&expr)?;

    match expr {
        SqlValue::Null => Ok(SqlValue::Bool(false).into_expr()),
        _ => Ok(SqlValue::Bool(true).into_expr()),
    }
}

async fn simplify_between(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    expr: Expr,
    negated: bool,
    low: Expr,
    high: Expr,
) -> Result<Expr, ExecutionError> {
    let expr = simplify_expr(stack, client, env, expr).await?;
    let low = simplify_expr(stack, client, env, low).await?;
    let high = simplify_expr(stack, client, env, high).await?;
    let expr = collect_sql_value(&expr)?;
    let low = collect_sql_value(&low)?;
    let high = collect_sql_value(&high)?;

    let result = match (expr, low, high) {
        (SqlValue::Number(value), SqlValue::Number(low), SqlValue::Number(high)) => {
            Ok(value >= low && value <= high)
        }
        (SqlValue::Float(value), SqlValue::Float(low), SqlValue::Float(high)) => {
            Ok(value >= low && value <= high)
        }
        (SqlValue::String(value), SqlValue::String(low), SqlValue::String(high)) => {
            Ok(value >= low && value <= high)
        }

        (expr, low, high) => {
            let negate_str = if negated { "NOT" } else { "" };

            Err(ExecutionError(format!(
                "Invalid between arguments: {} {} BETWEEN {} AND  {}",
                expr, negate_str, low, high
            )))
        }
    }?;

    let result = if negated { !result } else { result };

    Ok(SqlValue::Bool(result).into_expr())
}

async fn simplify_in_list(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    expr: Expr,
    list: Vec<Expr>,
    negated: bool,
) -> Result<Expr, ExecutionError> {
    let expr = simplify_expr(stack, client, env, expr).await?;
    let expr = collect_sql_value(&expr)?;
    let mut result = false;

    for elem_expr in list {
        let elem_expr = simplify_expr(stack, client, env, elem_expr).await?;
        let elem_expr = collect_sql_value(&elem_expr)?;

        if !expr.is_same_type(&elem_expr) {
            return Err(ExecutionError("IN LIST operation contains elements that have a different time than target expression".to_string()));
        }

        match (&expr, elem_expr) {
            (SqlValue::Number(ref x), SqlValue::Number(y)) => {
                if *x == y {
                    result = true;
                    break;
                }
            }

            (SqlValue::Float(ref x), SqlValue::Float(y)) => {
                // We all know doing equality checks over floats is stupid but it is what it is.
                if *x == y {
                    result = true;
                    break;
                }
            }

            (SqlValue::String(ref x), SqlValue::String(ref y)) => {
                if x == y {
                    result = true;
                    break;
                }
            }

            (SqlValue::Bool(ref x), SqlValue::Bool(y)) => {
                if *x == y {
                    result = true;
                    break;
                }
            }

            _ => unreachable!(),
        }
    }

    result = if negated { !result } else { result };

    Ok(SqlValue::Bool(result).into_expr())
}

async fn simplify_nested(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    expr: Expr,
) -> Result<Expr, ExecutionError> {
    Ok(simplify_expr(stack, client, env, expr).await?)
}

async fn simplify_subquery(
    stack: &mut Stack,
    client: &eventstore::Client,
    env: &Env,
    expr: Expr,
    subquery: Query,
    negated: bool,
) -> Result<Expr, ExecutionError> {
    let expr = simplify_expr(stack, client, env, expr).await?;
    let expr = collect_sql_value(&expr)?;
    let plan = if let Some(p) = build_plan_from_query(subquery) {
        Ok(p)
    } else {
        Err(ExecutionError(
            "Unable to build a successful build plan out of the subquery".to_string(),
        ))
    }?;

    let mut stream = match execute_plan(client, plan).await {
        Ok(s) => Ok(s),
        Err(e) => Err(ExecutionError(format!(
            "Error when executing sub query: {}",
            e
        ))),
    }?;

    let mut result = false;

    loop {
        match stream.try_next().await {
            Err(e) => {
                return Err(ExecutionError(format!(
                    "Error when consuming sub query stream: {}",
                    e
                )));
            }

            Ok(value) => {
                if let Some(value) = value {
                    let sql_value = collect_json_literal(&value)?;

                    if !expr.is_same_type(&sql_value) {
                        return Err(ExecutionError(format!(
                            "Unexpected type in subquery: {} (NOT) IN {}",
                            value, sql_value
                        )));
                    }

                    match (&expr, sql_value) {
                        (SqlValue::String(ref left), SqlValue::String(right))
                            if left.as_str() == right.as_str() =>
                        {
                            result = true;
                            break;
                        }

                        (SqlValue::Number(ref left), SqlValue::Number(right)) if *left == right => {
                            result = true;
                            break;
                        }

                        (SqlValue::Float(ref left), SqlValue::Float(right)) if *left == right => {
                            result = true;
                            break;
                        }

                        _ => continue,
                    }
                }

                break;
            }
        }
    }

    Ok(SqlValue::Bool(!negated && result).into_expr())
}

async fn execute_predicate(
    client: &eventstore::Client,
    env: &Env,
    predicate_expr: &Expr,
) -> Result<bool, ExecutionError> {
    let mut stack = Stack::new();
    let expr = simplify_expr(&mut stack, client, env, predicate_expr.clone()).await?;
    let expr = collect_sql_value(&expr)?;

    match expr {
        SqlValue::Bool(value) => Ok(value),
        SqlValue::Null => Ok(false),
        expr => Err(ExecutionError(format!(
            "Where predicate was not a boolean, got: {}",
            expr
        ))),
    }
}

fn resolve_name(env: &Env, name: String) -> Result<SqlValue, ExecutionError> {
    if let Some(value) = env.get(&name) {
        return collect_json_literal(value);
    }

    Ok(SqlValue::Null)
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

            if let Some(mut value) = value.as_u64() {
                // Please don't judge me!
                if value == u64::MAX {
                    value = i64::MAX as u64;
                }
                return Ok(SqlValue::Number(value as i64));
            }

            Ok(SqlValue::Float(value.as_f64().expect("to be f64")))
        }

        unsupported => Err(ExecutionError(format!(
            "Unsupported JSON to SQL value conversion: {:?}",
            unsupported
        ))),
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
                    Err(e) => Err(ExecutionError(format!(
                        "Invalid number value format: {}",
                        e
                    ))),
                };
            }

            match value.parse::<f64>() {
                Ok(value) => Ok(SqlValue::Float(value)),
                Err(e) => Err(ExecutionError(format!(
                    "Invalid number value format: {}",
                    e
                ))),
            }
        }

        sqlparser::ast::Value::SingleQuotedString(ref value) => Ok(SqlValue::String(value.clone())),
        sqlparser::ast::Value::DoubleQuotedString(ref value) => Ok(SqlValue::String(value.clone())),
        sqlparser::ast::Value::Boolean(ref value) => Ok(SqlValue::Bool(*value)),
        sqlparser::ast::Value::Null => Ok(SqlValue::Null),
        wrong => Err(ExecutionError(format!("Expected SQL value got: {}", wrong))),
    }
}
