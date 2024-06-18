use sqlparser::ast::Expr::{BinaryOp, CompoundIdentifier};
use sqlparser::ast::{Select, Statement, TableWithJoins};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub struct Query {
    pub tables: Vec<String>,
    pub rows: Vec<RowProperty>,
    pub condition: Option<WhereCondition>,
    pub join: Option<JoinCondition>,
}

pub struct JoinCondition {
    pub left: RowProperty,
    pub operator: String,
    pub right: RowProperty,
}

pub struct RowProperty {
    pub table: String,
    pub row: String,
}

pub struct WhereCondition {
    pub left: RowProperty,
    pub op: String,
    pub right: String,
}

pub fn parse_query(sql: &str) -> Result<Query, String> {
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let mut tables: Vec<String> = vec![];
    let mut rows: Vec<RowProperty> = vec![];

    let body = &ast[0].query().unwrap().body as Select;

    body.projection.iter().for_each(|p| match p {
        CompoundIdentifier(ref rowCompound) => {
            let row = RowProperty {
                table: rowCompound[0].value,
                row: rowCompound[1].value,
            };
            rows.push(row);
        }
        _ => {}
    });

    let from = body.from[0] as TableWithJoins;
    tables.push(from.relation.name.value);
    tables.push(from.joins[0].relation.name.value);
    let join = if from.joins[0].join_operator.is_some() {
        Some(JoinCondition {
            left: RowProperty {
                table: from.relation.name.value,
                row: from.joins[0].constraint.unwrap().left.to_string(),
            },
            operator: from.joins[0].join_operator.to_string(),
            right: RowProperty {
                table: from.joins[0].relation.name.value,
                row: from.joins[0].constraint.unwrap().right.to_string(),
            },
        })
    } else {
        None
    };
    let condition = if body.selection.is_some() {
        Some(WhereCondition {
            left: RowProperty {
                table: body.selection.left[0].name.value,
                row: body.selection.left[1].name.value,
            },
            op: body.selection.op.to_string(),
            right: body.selection.right.to_string(),
        })
    } else {
        None
    };

    let query = Query {
        tables,
        rows,
        join,
        condition,
    };

    Ok(query)
}

fn parse_statement(sql: &str) -> Result<Vec<Statement>, String> {
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    match ast[0] {
        Statement::Insert(ref insert) => {
            println!("Insert: {:?}", insert);
        }
        Statement::Update(ref update) => {
            println!("Update: {:?}", update);
        }
        Statement::Delete(ref delete) => {
            println!("Delete: {:?}", delete);
        }
        _ => {}
    }

    println!("AST: {:?}", ast);

    Ok(ast)
}

fn captureChanges() {}
