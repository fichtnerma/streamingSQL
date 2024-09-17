use std::collections::HashMap;
use std::convert::TryFrom;
use super::replication::{decoderbufs::{col_message::Data, Op}, Transaction};

pub enum ChangeData {
    Insert(Insert),
    Update(Update),
    Delete(Delete),
}

pub struct ChangeEvent {
    pub table: String,
    pub commit_time: u64,
    pub transaction_id: u32,
    pub data: ChangeData,
}

pub struct Insert {
    pub values: HashMap<String, Data>,
}

pub struct Update {
    pub values: HashMap<String, Data>,
    pub old_values: HashMap<String, Data>,
}

pub struct Delete {
    pub values: HashMap<String, Data>,
}

pub async fn subscribe(
    tx: tokio::sync::broadcast::Sender<Transaction>,
    mut done: tokio::sync::mpsc::Receiver<()>,
) -> Result<(), tokio_postgres::Error> {
    let mut rx = tx.subscribe();

    loop {
        tokio::select! {
            _ = done.recv() => {
                break
            }
            Ok(transaction) = rx.recv() => {
                transaction.events.iter().for_each(|event| {
                    match event {
                        super::replication::decoderbufs::RowMessage {table,op,new_tuple,old_tuple,new_typeinfo, transaction_id, commit_time } => {
                            match Op::try_from(op.unwrap()).unwrap() {
                                Op::Insert => {
                                    println!("event: {:?}", event);
                                    let inserts = new_tuple
                                        .iter()
                                        .map(|data| (data.column_name().to_string(), data.data.clone().unwrap()))
                                        .collect::<HashMap<_, _>>();

                                    // println!("INSERT {:?}", inserts);
                                    ChangeEvent{
                                        table: table.clone().expect("No table name"), 
                                        commit_time: commit_time.unwrap(), 
                                        transaction_id: transaction_id.unwrap(), 
                                        data: ChangeData::Insert(Insert{values: inserts})
                                    };
                                }
                                Op::Update => {
                                    let updates = new_tuple
                                        .iter()
                                        .map(|data| (data.column_name().to_string(), data.data.clone().unwrap()))
                                        .collect::<HashMap<_, _>>();

                                    let old_values = old_tuple
                                        .iter()
                                        .map(|data| (data.column_name().to_string(), data.data.clone().unwrap()))
                                        .collect::<HashMap<_, _>>();
                                    
                                    ChangeEvent {
                                        table: table.clone().expect("No table name"), 
                                        commit_time: commit_time.unwrap(), 
                                        transaction_id: transaction_id.unwrap(), 
                                        data: ChangeData::Update(Update{values: updates, old_values})
                                    };
                                }
                                Op::Delete => {
                                    let deletes = old_tuple
                                        .iter()
                                        .map(|data| (data.column_name().to_string(), data.data.clone().unwrap()))
                                        .collect::<HashMap<_, _>>();
                                    ChangeEvent {
                                        table: table.clone().expect("No table name"), 
                                        commit_time: commit_time.unwrap(), 
                                        transaction_id: transaction_id.unwrap(), 
                                        data: ChangeData::Delete(Delete{values: deletes})
                                    };
                                }
                                Op::Begin => unreachable!(),
                                Op::Commit => unreachable!(),
                                Op::Unknown => unreachable!(),
                            }
                        }
                    };
                });
            }
        }
    }

    Ok(())
}

// Usssage { 
//         column_name: Some("id"), 
//         column_type: Some(23), 
//         datum: Some(DatumInt32(1274)) 
//     }, 
//     DatumMessage { 
//         column_name: Some("age"), 
//         column_type: Some(23), datum: Some(DatumInt32(52)) 
//     }, 
//     DatumMessage { 
//         column_name: Some("name"), 
//         column_type: Some(25), 
//         datum: Some(DatumString("User")) 
//     }
// ]

// Order
// [
//     DatumMessage { 
//         column_name: Some("id"), 
//         column_type: Some(23), 
//         datum: Some(DatumInt32(309)) 
//     }, 
//     DatumMessage { 
//         column_name: Some("total"), 
//         column_type: Some(701), datum: Some(DatumDouble(125.0)) 
//     }, 
//     DatumMessage { 
//         column_name: Some("\"nbrOfItems\""), 
//         column_type: Some(23), 
//         datum: Some(DatumInt32(6)) 
//     }, 
//     DatumMessage { 
//         column_name: Some("\"buyerId\""), 
//         column_type: Some(23), 
//         datum: Some(DatumInt32(1246)) 
//     }
// ]