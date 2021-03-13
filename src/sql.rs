#[derive(Debug)]
pub struct Plan {
    stream_name: Option<String>,
    count: usize,
    fields: Vec<String>,
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            stream_name: None,
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

                plan.stream_name = Some(name.value);
            }

            return Some(plan)
        }
    }

    None
}