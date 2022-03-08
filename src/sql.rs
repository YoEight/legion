use futures::stream::BoxStream;
use futures::TryStreamExt;
use sqlparser::ast::{BinaryOperator, Expr, Ident, Query, UnaryOperator};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Plan {
    stream_name: String,
    alias: Option<String>,
    fields: Vec<String>,
    predicate: Option<Expr>,
    joins: Vec<Join>,
}

#[derive(Debug, Copy, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct Join {
    name: String,
    alias: Option<String>,
    join_type: JoinType,
    join_expr: Expr,
}

impl Join {
    fn prefix(&self) -> Option<&String> {
        self.alias.as_ref()
    }
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            stream_name: "".to_string(),
            alias: None,
            fields: Vec::new(),
            predicate: None,
            joins: Vec::new(),
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

fn create_env(plan: &Plan, event: &eventstore::RecordedEvent) -> Result<Env, ExecutionError> {
    create_env_with_alias(plan.alias.as_ref(), event)
}

fn create_env_with_alias(
    alias: Option<&String>,
    event: &eventstore::RecordedEvent,
) -> Result<Env, ExecutionError> {
    match serde_json::from_slice::<Env>(event.data.as_ref()) {
        Err(e) => Err(ExecutionError(format!(
            "Error when parsing event payload to JSON: {}",
            e
        ))),
        Ok(mut env) => {
            let mut prefix = String::new();

            if let Some(alias) = alias {
                let mut tmp = HashMap::new();
                for (key, value) in env.drain() {
                    tmp.insert(format!("{}.{}", alias, key), value);
                }

                env = tmp;
                prefix = format!("{}.", alias);
            }

            env.insert(
                format!("{}es_type", prefix),
                serde_json::to_value(event.event_type.as_str()).expect("valid json"),
            );
            env.insert(
                format!("{}es_stream_id", prefix),
                serde_json::to_value(event.stream_id.as_str()).expect("valid json"),
            );
            env.insert(
                format!("{}es_event_id", prefix),
                serde_json::to_value(event.id.to_string()).expect("valid json"),
            );
            env.insert(
                format!("{}es_revision", prefix),
                serde_json::to_value(&event.revision).expect("valid json"),
            );
            env.insert(
                format!("{}es_commit", prefix),
                serde_json::to_value(&event.position.commit).expect("valid json"),
            );
            env.insert(
                format!("{}es_prepare", prefix),
                serde_json::to_value(&event.position.prepare).expect("valid json"),
            );

            Ok(env)
        }
    }
}

fn project_env(plan: &Plan, env: &Env) -> serde_json::Value {
    if plan.fields.len() == 1 {
        let field = plan.fields.first().unwrap();

        return serde_json::to_value(env.get(field)).unwrap();
    }

    if plan.fields.is_empty() {
        return serde_json::to_value(env).unwrap();
    }

    let mut line = HashMap::new();

    for key in plan.fields.iter() {
        line.insert(key.clone(), serde_json::to_value(env.get(key)).unwrap());
    }

    serde_json::to_value(line).unwrap()
}

pub fn build_plan_from_query(query: Query) -> Option<Plan> {
    let mut plan = Plan::default();
    if let sqlparser::ast::SetExpr::Select(mut select) = query.body {
        for item in select.projection {
            if let sqlparser::ast::SelectItem::UnnamedExpr(expr) = item {
                match expr {
                    Expr::Identifier(ident) => {
                        plan.fields.push(ident.value);
                    }

                    Expr::CompoundIdentifier(idents) => {
                        let mut ident = String::new();

                        for elem in idents {
                            if ident.is_empty() {
                                ident.push_str(elem.value.as_str());
                            } else {
                                ident.push('.');
                                ident.push_str(elem.value.as_str());
                            }
                        }

                        plan.fields.push(ident);
                    }

                    _ => {}
                }
            }
        }

        let from = select.from.pop()?;

        if let sqlparser::ast::TableFactor::Table {
            mut name, alias, ..
        } = from.relation
        {
            let name = name.0.pop()?;

            plan.stream_name = name.value;
            plan.alias = alias.map(|a| a.name.value);
        }

        let mut joins = Vec::new();
        for join in from.joins {
            if let sqlparser::ast::TableFactor::Table {
                mut name, alias, ..
            } = join.relation
            {
                let name = name.0.pop().expect("non empty ident list").value;
                let alias = alias.map(|a| a.name.value);

                let (join_type, expr) = match join.join_operator {
                    sqlparser::ast::JoinOperator::Inner(expr) => {
                        (JoinType::Inner, join_operator_expr(expr)?)
                    }
                    sqlparser::ast::JoinOperator::LeftOuter(expr) => {
                        (JoinType::Left, join_operator_expr(expr)?)
                    }
                    sqlparser::ast::JoinOperator::RightOuter(expr) => {
                        (JoinType::Right, join_operator_expr(expr)?)
                    }
                    sqlparser::ast::JoinOperator::FullOuter(expr) => {
                        (JoinType::Full, join_operator_expr(expr)?)
                    }
                    _ => return None,
                };

                let join = Join {
                    name,
                    alias,
                    join_type,
                    join_expr: expr,
                };

                joins.push(join);
            }
        }

        plan.predicate = select.selection;
        plan.joins = joins;

        return Some(plan);
    }

    None
}

fn join_operator_expr(constraint: sqlparser::ast::JoinConstraint) -> Option<Expr> {
    if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
        Some(expr)
    } else {
        None
    }
}

pub async fn execute_plan<'a>(
    client: &'a eventstore::Client,
    plan: Plan,
) -> Result<BoxStream<'a, Result<serde_json::Value, ExecutionError>>, Box<dyn std::error::Error>> {
    debug!("Build plan: {:?}", plan);

    let mut stream = client
        .read_stream(plan.stream_name.as_str(), &Default::default())
        .await?;

    let mut join_streams = Vec::new();

    for join in plan.joins.iter().cloned() {
        let join_stream = client
            .read_stream(join.name.as_str(), &Default::default())
            .await?;

        // Because of what joins are, we have to load everything in memory.
        join_streams.push((
            join,
            join_stream
                .into_stream()
                .try_collect::<Vec<eventstore::ResolvedEvent>>()
                .await?,
        ));
    }

    let output = async_stream::try_stream! {
        while let Some(event) = stream.next().await? {
            let event = event.get_original_event();
            let mut json_payload = create_env(&plan, event)?;

            let mut skipped = false;
            for (join, join_stream) in join_streams.iter_mut() {
                let mut line = None;
                let mut found = false;
                for join_elem in join_stream.iter() {
                    let prefix = join.prefix();
                    let join_elem = create_env_with_alias(prefix, join_elem.get_original_event())?;
                    let mut tmp = json_payload.clone();

                    tmp.extend(join_elem.clone());

                    let passed = execute_predicate(client, tmp.clone(), join.join_expr.clone()).await?;

                    line = Some(join_elem);
                    if passed {
                        found = true;
                        break;
                    }
                }

                match join.join_type {
                    JoinType::Inner => {
                        if found {
                            json_payload.extend(line.expect("not empty"));
                        } else {
                            skipped = true;
                            break;
                        }
                    },

                    JoinType::Left => {
                        if found {
                            json_payload.extend(line.expect("not empty"));
                        }
                    }

                    JoinType::Right => {
                        if found {
                            json_payload.extend(line.expect("not empty"));
                        } else {
                            json_payload = line.expect("not empty");
                        }
                    }

                    JoinType::Full => {
                        json_payload.extend(line.expect("not empty"));
                    }
                }
            }

            if skipped {
                continue;
            }

            if let Some(expr) = plan.predicate.as_ref() {
                let passed = execute_predicate(client, json_payload.clone(), expr.clone()).await?;

                if !passed {
                    continue;
                }
            }

            yield project_env(&plan, &json_payload);
        }
    };

    let output: BoxStream<'a, Result<serde_json::Value, ExecutionError>> = Box::pin(output);

    return Ok(output);
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

impl From<eventstore::Error> for ExecutionError {
    fn from(e: eventstore::Error) -> Self {
        ExecutionError(e.to_string())
    }
}

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

    fn as_bool(self) -> Result<bool, ExecutionError> {
        match self {
            SqlValue::Bool(b) => Ok(b),
            other => Err(ExecutionError(format!(
                "Expecting boolean but got: {:?}",
                other
            ))),
        }
    }

    fn is_null(&self) -> bool {
        match &self {
            SqlValue::Null => true,
            _ => false,
        }
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

#[derive(Debug)]
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
    InSubquery {
        plan: Plan,
        event: Env,
        negated: bool,
        stream: eventstore::ReadStream,
    },
    Between(bool),
    Value(SqlValue),
    Expr(Expr),
    Return,
}

impl std::fmt::Debug for StackElem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StackElem::BinaryOp(ref op) => write!(f, "BinaryOp({:?})", op),
            StackElem::UnaryOp(ref op) => write!(f, "UnaryOp({:?})", op),
            StackElem::IsNull(ref op) => write!(f, "IsNull({:?})", op),
            StackElem::InList(ref op) => write!(f, "IsList({:?})", op),
            StackElem::InSubquery {
                ref plan,
                ref event,
                ref negated,
                ..
            } => write!(
                f,
                "InSubquery{{ plan: {:?}, event: {:?}, negated: {:?}, *stream* }})",
                plan, event, negated
            ),
            StackElem::Between(ref op) => write!(f, "Between({:?})", op),
            StackElem::Value(ref op) => write!(f, "Value({:?})", op),
            StackElem::Expr(ref op) => write!(f, "Expr({:?})", op),
            StackElem::Return => write!(f, "Return"),
        }
    }
}

async fn simplify_expr(
    stack: &mut Stack,
    client: &eventstore::Client,
    mut env: Env,
    expr: Expr,
) -> Result<Expr, ExecutionError> {
    let mut params = Vec::<SqlValue>::new();
    let mut scopes = Vec::<Env>::new();

    // Initiate the execution loop.
    stack.push(StackElem::Return);
    stack.push(StackElem::Expr(expr));

    loop {
        debug!("Stack: {:?}", stack);
        match stack.pop()? {
            StackElem::Return => {
                if let Some(prev_env) = scopes.pop() {
                    env = prev_env;
                    stack.push(StackElem::Value(params.pop().unwrap()));

                    continue;
                }

                return Ok(params.pop().unwrap().into_expr());
            }

            StackElem::Value(value) => params.push(value),

            StackElem::Expr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let value = resolve_name(&env, ident.value)?;

                    stack.push(StackElem::Value(value));
                }

                Expr::CompoundIdentifier(idents) => {
                    let mut ident = String::new();

                    for elem in idents {
                        if ident.is_empty() {
                            ident.push_str(elem.value.as_str());
                        } else {
                            ident.push('.');
                            ident.push_str(elem.value.as_str());
                        }
                    }

                    let value = resolve_name(&env, ident)?;

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

                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => {
                    let plan = if let Some(p) = build_plan_from_query(*subquery) {
                        Ok(p)
                    } else {
                        Err(ExecutionError(
                            "Unable to make a build plan out of the subquery".to_string(),
                        ))
                    }?;

                    let stream = client
                        .read_stream(plan.stream_name.as_str(), &Default::default())
                        .await;

                    match stream {
                        Err(e) => {
                            return Err(ExecutionError(format!(
                                "Error during subquery execution: {}",
                                e
                            )));
                        }

                        Ok(mut stream) => {
                            match stream.next().await {
                                Err(e) => {
                                    return Err(ExecutionError(format!(
                                        "Error when consuming subquery: {:?}",
                                        e
                                    )))
                                }
                                Ok(elem) => {
                                    let expr = *expr;

                                    if let Some(elem) = elem {
                                        let mut event =
                                            create_env(&plan, elem.get_original_event())?;
                                        let pred_expr = plan.predicate.as_ref().map(|p| p.clone());

                                        stack.push(StackElem::InSubquery {
                                            plan,
                                            event: event.clone(),
                                            negated,
                                            stream,
                                        });

                                        stack.push(StackElem::Expr(expr));

                                        if let Some(pred_expr) = pred_expr {
                                            stack.push(StackElem::Return);
                                            stack.push(StackElem::Expr(pred_expr));

                                            // We store the previous environment.
                                            scopes.push(env.clone());

                                            // We create a new environment.
                                            event.extend(env);
                                            env = event;
                                        } else {
                                            stack.push(StackElem::Value(SqlValue::Bool(true)));
                                        }
                                    } else {
                                        stack.push(StackElem::InSubquery {
                                            plan,
                                            event: Default::default(),
                                            negated,
                                            stream,
                                        });

                                        // No need to evaluate the left-side operand because
                                        // the right side is NULL.
                                        stack.push(StackElem::Value(SqlValue::Null));
                                        stack.push(StackElem::Value(SqlValue::Null));
                                    }

                                    continue;
                                }
                            }
                        }
                    }
                }

                expr => return Err(ExecutionError(format!("Unsupported expression: {}", expr))),
            },

            StackElem::IsNull(negated) => {
                debug!("IsNull Params: {:?}", params);
                let mut result = if let SqlValue::Null = params.pop().unwrap() {
                    true
                } else {
                    false
                };

                if negated {
                    result = !result;
                }

                stack.push(StackElem::Value(SqlValue::Bool(result)));
            }

            StackElem::InList(negated) => {
                debug!("IsList Params: {:?}", params);
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

                if negated {
                    result = !result;
                }

                params.clear();
                stack.push(StackElem::Value(SqlValue::Bool(result)));
            }

            StackElem::Between(negated) => {
                debug!("Between Params: {:?}", params);
                let expr = params.pop().unwrap();
                let low = params.pop().unwrap();
                let high = params.pop().unwrap();

                let mut result = match (expr, low, high) {
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

                if negated {
                    result = !result;
                }

                stack.push(StackElem::Value(SqlValue::Bool(result)));
            }

            StackElem::BinaryOp(op) => {
                debug!("Binary {:?} Params: {:?}", op, params);
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
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Plus) => {
                        Ok(SqlValue::Number(left + right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Minus) => {
                        Ok(SqlValue::Number(left - right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Multiply) => {
                        Ok(SqlValue::Number(left * right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Divide) => {
                        Ok(SqlValue::Number(left / right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Modulus) => {
                        Ok(SqlValue::Number(left % right))
                    }
                    (
                        SqlValue::Number(left),
                        SqlValue::Number(right),
                        BinaryOperator::BitwiseOr,
                    ) => Ok(SqlValue::Number(left | right)),
                    (
                        SqlValue::Number(left),
                        SqlValue::Number(right),
                        BinaryOperator::BitwiseAnd,
                    ) => Ok(SqlValue::Number(left & right)),
                    (
                        SqlValue::Number(left),
                        SqlValue::Number(right),
                        BinaryOperator::BitwiseXor,
                    ) => Ok(SqlValue::Number(left ^ right)),
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Eq) => {
                        Ok(SqlValue::Bool(left == right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::NotEq) => {
                        Ok(SqlValue::Bool(left != right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Gt) => {
                        Ok(SqlValue::Bool(left > right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::GtEq) => {
                        Ok(SqlValue::Bool(left >= right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::Lt) => {
                        Ok(SqlValue::Bool(left < right))
                    }
                    (SqlValue::Number(left), SqlValue::Number(right), BinaryOperator::LtEq) => {
                        Ok(SqlValue::Bool(left <= right))
                    }
                    (
                        SqlValue::Number(left),
                        SqlValue::Number(right),
                        BinaryOperator::Spaceship,
                    ) => Ok(SqlValue::Bool(left == right)),

                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Plus) => {
                        Ok(SqlValue::Float(left + right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Minus) => {
                        Ok(SqlValue::Float(left - right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Multiply) => {
                        Ok(SqlValue::Float(left * right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Divide) => {
                        Ok(SqlValue::Float(left / right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Eq) => {
                        Ok(SqlValue::Bool(left == right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::NotEq) => {
                        Ok(SqlValue::Bool(left != right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Gt) => {
                        Ok(SqlValue::Bool(left > right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::GtEq) => {
                        Ok(SqlValue::Bool(left >= right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Lt) => {
                        Ok(SqlValue::Bool(left < right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::LtEq) => {
                        Ok(SqlValue::Bool(left <= right))
                    }
                    (SqlValue::Float(left), SqlValue::Float(right), BinaryOperator::Spaceship) => {
                        Ok(SqlValue::Bool(left == right))
                    }

                    (
                        SqlValue::String(left),
                        SqlValue::String(right),
                        BinaryOperator::StringConcat,
                    ) => Ok(SqlValue::String(format!("{}{}", left, right))),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Eq) => {
                        Ok(SqlValue::Bool(left == right))
                    }
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::NotEq) => {
                        Ok(SqlValue::Bool(left != right))
                    }
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Gt) => {
                        Ok(SqlValue::Bool(left > right))
                    }
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::GtEq) => {
                        Ok(SqlValue::Bool(left >= right))
                    }
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Lt) => {
                        Ok(SqlValue::Bool(left < right))
                    }
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::LtEq) => {
                        Ok(SqlValue::Bool(left <= right))
                    }
                    (
                        SqlValue::String(left),
                        SqlValue::String(right),
                        BinaryOperator::Spaceship,
                    ) => Ok(SqlValue::Bool(left == right)),
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::Like) => {
                        simplify_like(left, right, false)
                    }
                    (SqlValue::String(left), SqlValue::String(right), BinaryOperator::NotLike) => {
                        simplify_like(left, right, true)
                    }

                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::And) => {
                        Ok(SqlValue::Bool(left && right))
                    }
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Or) => {
                        Ok(SqlValue::Bool(left || right))
                    }
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Eq) => {
                        Ok(SqlValue::Bool(left == right))
                    }
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::NotEq) => {
                        Ok(SqlValue::Bool(left != right))
                    }
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Gt) => {
                        Ok(SqlValue::Bool(left > right))
                    }
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::GtEq) => {
                        Ok(SqlValue::Bool(left >= right))
                    }
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Lt) => {
                        Ok(SqlValue::Bool(left < right))
                    }
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::LtEq) => {
                        Ok(SqlValue::Bool(left <= right))
                    }
                    (SqlValue::Bool(left), SqlValue::Bool(right), BinaryOperator::Spaceship) => {
                        Ok(SqlValue::Bool(left == right))
                    }

                    (left, right, op) => Err(ExecutionError(format!(
                        "Unsupported binary operation: {} {} {}",
                        left, op, right
                    ))),
                }?;

                stack.push(StackElem::Value(result));
            }

            StackElem::UnaryOp(op) => {
                debug!("Unary {:?} Params: {:?}", op, params);
                let expr = params.pop().unwrap();

                let result = match (expr, op) {
                    (SqlValue::Number(expr), UnaryOperator::Plus) => Ok(SqlValue::Number(expr)),
                    (SqlValue::Number(expr), UnaryOperator::Minus) => Ok(SqlValue::Number(-expr)),

                    (SqlValue::Float(expr), UnaryOperator::Plus) => Ok(SqlValue::Float(expr)),
                    (SqlValue::Float(expr), UnaryOperator::Minus) => Ok(SqlValue::Float(-expr)),

                    (SqlValue::Bool(expr), UnaryOperator::Not) => Ok(SqlValue::Bool(!expr)),

                    (expr, op) => Err(ExecutionError(format!(
                        "Unsupported unary operation: {} {}",
                        op, expr
                    ))),
                }?;

                stack.push(StackElem::Value(result));
            }

            StackElem::InSubquery {
                plan,
                event,
                negated,
                mut stream,
            } => {
                let expr = params.pop().unwrap();
                let elem = params.pop().unwrap();

                if expr.is_null() {
                    stack.push(StackElem::Value(expr));

                    continue;
                }

                let skip = elem.is_null() || !elem.as_bool()?;

                if !skip {
                    let field = collect_json_literal(&project_env(&plan, &event))?;

                    if !field.is_null() {
                        if !expr.is_same_type(&field) {
                            return Err(ExecutionError(format!(
                                "Different types used when consuming subquery {:?} and {:?}",
                                expr, field
                            )));
                        }

                        match (&expr, field) {
                            (SqlValue::Number(ref expr), SqlValue::Number(field))
                                if *expr == field =>
                            {
                                stack.push(StackElem::Value(SqlValue::Bool(!negated)));
                                continue;
                            }

                            (SqlValue::Float(ref expr), SqlValue::Float(field))
                                if *expr == field =>
                            {
                                stack.push(StackElem::Value(SqlValue::Bool(!negated)));
                                continue;
                            }

                            (SqlValue::String(ref expr), SqlValue::String(field))
                                if expr.as_str() == field.as_str() =>
                            {
                                stack.push(StackElem::Value(SqlValue::Bool(!negated)));
                                continue;
                            }

                            (SqlValue::Bool(ref expr), SqlValue::Bool(field)) if *expr == field => {
                                stack.push(StackElem::Value(SqlValue::Bool(!negated)));
                                continue;
                            }

                            _ => {}
                        }
                    }
                }

                match stream.next().await {
                    Err(e) => {
                        return Err(ExecutionError(format!(
                            "Error when consuming subquery: {:?}",
                            e
                        )));
                    }

                    Ok(elem) => {
                        if let Some(elem) = elem {
                            let mut event = create_env(&plan, elem.get_original_event())?;
                            let pred_expr = plan.predicate.as_ref().map(|p| p.clone());

                            stack.push(StackElem::InSubquery {
                                plan,
                                event: event.clone(),
                                negated,
                                stream,
                            });

                            // We push back onto the stack the left-side expression
                            // we already computed.
                            stack.push(StackElem::Value(expr));

                            if let Some(pred_expr) = pred_expr {
                                stack.push(StackElem::Return);
                                stack.push(StackElem::Expr(pred_expr));

                                // We store the previous environment.
                                scopes.push(env.clone());

                                // We create a new environment.
                                event.extend(env);
                                env = event;
                            } else {
                                stack.push(StackElem::Value(SqlValue::Bool(true)));
                            }
                            continue;
                        }

                        stack.push(StackElem::Value(SqlValue::Bool(negated)));
                    }
                }
            }
        }
    }
}

// Very simplistic implementation.
fn simplify_like(left: String, right: String, negated: bool) -> Result<SqlValue, ExecutionError> {
    let (tpe, content) = parse_like_expr(right)?;

    let mut result = match tpe {
        Like::StartWith => left.starts_with(content.as_str()),
        Like::EndWith => left.ends_with(content.as_str()),
        Like::Contains => left.contains(content.as_str()),
    };

    if negated {
        result = !result;
    }

    Ok(SqlValue::Bool(result))
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

async fn execute_predicate(
    client: &eventstore::Client,
    env: Env,
    predicate_expr: Expr,
) -> Result<bool, ExecutionError> {
    let mut stack = Stack::new();
    let expr = simplify_expr(&mut stack, client, env, predicate_expr).await?;
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
        sqlparser::ast::Value::Number(ref value, _) => {
            if !value.contains('.') {
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
