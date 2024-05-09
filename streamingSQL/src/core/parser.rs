use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;


pub struct Parser {
    sql: String,
    tokens: Vec<Statement>,
    index: usize,
}

impl Parser {
    pub fn new(sql: String) -> Self {
        Parser {
            sql,
            tokens: vec![],
            index: 0,
        }
    }

    fn parse_statement(&mut self) -> Result<Vec<Statement>, String> {
        let mut involved_tables: Vec<String> = vec![];
        let mut involved_rows = vec![];
        let mut conditions = vec![];
        let sql = "SELECT * FROM weather JOIN cities ON city = name";
    
        let ast = sqlparser::parser::Parser::parse_sql(&PostgreSqlDialect, &self.sql).unwrap();
        match ast[0] {
            Statement::Query(ref query) => {
                match body.query {
                    sqlparser::ast::SetExpr::Select(ref select) => {
                        select.projection.iter().for_each(|p| {
                            involved_rows.push(p.to_string());
                        });
                    }
                    _ => {}
                }
                match body.from {
                    Some(ref from) => {
                        from.iter().for_each(|f| {
                            involved_tables.push(f.to_string());
                        });
                    }
                    None => {}
                }
                match body.selection {
                    Some(ref selection) => {
                        conditions.push(selection.to_string());
                    }
                    None => {}
                }
            }
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
}


fn captureChanges() {}

let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

[
    Query(Query { 
        ctes: [], 
        body: Select(Select { 
            distinct: false, 
            projection: [
                UnnamedExpr(Identifier("a")), 
                UnnamedExpr(Identifier("b")), 
                UnnamedExpr(Value(Long(123))), 
                UnnamedExpr(Function(Function 
                    { 
                        name: ObjectName(["myfunc"]), 
                        args: [Identifier("b")], 
                        filter: None, 
                        over: None, 
                        distinct: false
                    }
                ))
            ],
            from: [
                TableWithJoins { 
                    relation: Table { 
                        name: ObjectName(["table_1"]), 
                        alias: None, 
                        args: [], 
                        with_hints: [] 
                    }, 
                    joins: [] 
                }
            ], 
            selection: Some(BinaryOp { 
                left: BinaryOp { 
                    left: Identifier("a"), 
                    op: Gt, 
                    right: Identifier("b") 
                }, 
                op: And, 
                right: BinaryOp { 
                    left: Identifier("b"), 
                    op: Lt, 
                    right: Value(Long(100)) 
                } 
            }), 
            group_by: [], having: None }), 
            order_by: [
                OrderByExpr { expr: Identifier("a"), asc: Some(false) }, 
                OrderByExpr { expr: Identifier("b"), asc: None }
            ], 
            limit: None, 
            offset: None, 
            fetch: None 
    })
]