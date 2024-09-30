use serde_json::de;
use sqlparser::ast::Expr::{self, BinaryOp, CompoundIdentifier, Identifier, Value};
use sqlparser::ast::SetExpr::Select;
use sqlparser::ast::{JoinOperator, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone)]
pub struct Query {
    pub tables: Vec<String>,
    pub rows: Vec<RowProperty>,
    pub condition: Option<WhereCondition>,
    pub joins: Vec<JoinCondition>,
}

#[derive(Debug, Clone)]

pub struct JoinCondition {
    pub left: RowProperty,
    pub operator: String,
    pub right: RowProperty,
}

#[derive(Debug, Clone)]

pub struct RowProperty {
    pub table: String,
    pub row: String,
}

impl RowProperty {
    pub fn to_string(&self) -> String {
        if self.table.is_empty() {
            return self.row.to_string();
        } else {
            return format!("{}.{}", self.table, self.row);
        }
    }
}

#[derive(Debug, Clone)]

pub struct WhereCondition {
    pub left: RowProperty,
    pub op: String,
    pub right: String,
}

pub fn parse_query(sql: &str) -> Result<Query, String> {
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let mut tables: Vec<String> = vec![];
    let statement = &ast[0];
    match statement {
        Statement::Query(ref query) => {
            let body = if let Select(select) = *query.body.clone() {
                select
            } else {
                return Err("Unsupported query type".to_string());
            };

            let rows = parse_projection(body.projection);
            tables = parse_from(body.from.clone());
            let joins = parse_joins(body.from);
            let condition = parse_condition(body.selection);

            let query = Query {
                tables,
                rows,
                joins,
                condition,
            };
            Ok(query)
        }
        _ => {
            return Err("Unsupported query type".to_string());
        }
    }
}

fn parse_projection(select: Vec<sqlparser::ast::SelectItem>) -> Vec<RowProperty> {
    let mut rows: Vec<RowProperty> = vec![];
    select
        .iter()
        .for_each(|p: &sqlparser::ast::SelectItem| match p {
            sqlparser::ast::SelectItem::UnnamedExpr(ref expr) => match expr {
                Identifier(ident) => {
                    let row = RowProperty {
                        table: "".to_string(),
                        row: ident.value.clone(),
                    };
                    rows.push(row);
                }
                CompoundIdentifier(expr) => {
                    let row = RowProperty {
                        table: expr[0].value.clone(),
                        row: expr[1].value.clone(),
                    };
                    rows.push(row);
                }
                _ => {}
            },
            _ => {}
        });
    rows
}

fn parse_from(from: Vec<TableWithJoins>) -> Vec<String> {
    let mut tables: Vec<String> = vec![];
    from.iter().for_each(|from| {
        match from.relation.clone() {
            TableFactor::Table { name, alias, .. } => {
                tables.push(name.to_string().trim_matches('"').to_string());
            }
            _ => {}
        }

        from.joins
            .iter()
            .for_each(|join| match join.relation.clone() {
                TableFactor::Table { name, alias, .. } => {
                    tables.push(name.to_string().trim_matches('"').to_string());
                }
                _ => {}
            });
    });
    tables
}

fn parse_joins(from: Vec<sqlparser::ast::TableWithJoins>) -> Vec<JoinCondition> {
    let mut joins: Vec<JoinCondition> = Vec::new();
    from.iter().for_each(|from: &TableWithJoins| {
        if !from.joins.is_empty() {
            from.joins.iter().for_each(|join: &sqlparser::ast::Join| {
                let join_constraint = match join.join_operator.clone() {
                    JoinOperator::Inner(constraint) => (constraint),
                    _ => return,
                };

                let on = match join_constraint {
                    sqlparser::ast::JoinConstraint::On(expr) => expr,
                    _ => return,
                };

                match on {
                    BinaryOp { left, op, right } => {
                        let left = match *left {
                            Identifier(ident) => ["".to_string(), ident.value.to_string()],
                            CompoundIdentifier(expr) => {
                                [expr[0].value.clone(), expr[1].value.clone()]
                            }
                            _ => ["".to_string(), "".to_string()],
                        };
                        let right = match *right {
                            Identifier(ident) => ["".to_string(), ident.value.to_string()],
                            CompoundIdentifier(expr) => {
                                [expr[0].value.clone(), expr[1].value.clone()]
                            }
                            _ => ["".to_string(), "".to_string()],
                        };
                        let join = JoinCondition {
                            left: RowProperty {
                                table: left[0].clone(),
                                row: left[1].clone(),
                            },
                            operator: op.to_string(),
                            right: RowProperty {
                                table: right[0].clone(),
                                row: right[1].clone(),
                            },
                        };
                        joins.push(join);
                    }
                    _ => {}
                }
            });
        }
    });
    joins
}

fn parse_condition(selection: Option<Expr>) -> Option<WhereCondition> {
    if selection.is_none() {
        return None;
    }
    match selection.unwrap() {
        BinaryOp { left, op, right } => {
            let left = match *left {
                Identifier(ident) => ["".to_string(), ident.value.to_string()],
                CompoundIdentifier(expr) => [expr[0].value.clone(), expr[1].value.clone()],
                _ => ["".to_string(), "".to_string()],
            };
            let right = match *right {
                Value(val) => match val {
                    sqlparser::ast::Value::Number(num, _) => num.to_string(),
                    sqlparser::ast::Value::Boolean(b) => b.to_string(),
                    _ => "".to_string(),
                },
                _ => "".to_string(),
            };
            return Some(WhereCondition {
                left: RowProperty {
                    table: left[0].clone(),
                    row: left[1].clone(),
                },
                op: op.to_string(),
                right,
            });
        }
        _ => return None,
    };
}
