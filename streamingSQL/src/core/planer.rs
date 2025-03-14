use crate::{
    core::{
        parser::RowProperty,
        sink::Sink,
        types::{
            buffer::Buffer,
            dataflow_types::{DBRecord, DataflowData, DataflowInput, Keys, RecordType},
            inputs::InputSessions,
            source::Source,
        },
    },
    pg_client::schema::{Key, KeyType},
};
use std::{collections::HashMap, sync::Arc};
use std::{sync::Mutex, thread};

use super::parser::Query;
extern crate differential_dataflow;
extern crate timely;
use crate::core::planer::differential_dataflow::operators::Consolidate;
use crate::core::planer::differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::{arrange::Arrange, Join, Threshold};
use differential_dataflow::Collection;
use differential_dataflow::{
    operators::arrange::{ArrangeByKey, ArrangeBySelf},
    trace::TraceReader,
};
use timely::{communication::Allocator, dataflow::scopes::Child};
use timely::{dataflow::operators::Probe, worker::Worker as TimelyWorker};
use tracing::{debug, info, warn};

pub struct QueryPlaner {
    table_identities: HashMap<String, Vec<Key>>,
}
type DBState = Arc<Mutex<HashMap<usize, (Option<usize>, DBRecord)>>>;

impl QueryPlaner {
    pub fn new(table_identities: HashMap<String, Vec<Key>>) -> Self {
        QueryPlaner {
            table_identities: table_identities,
        }
    }
    pub async fn build_dataflow(&self, query: Query, source: Source) {
        let table_name = query.to_table_string();
        let sink = Sink::new(table_name.clone()).await;
        let table_identities = self.table_identities.clone();
        let left_state: DBState = Arc::new(Mutex::new(HashMap::new()));
        let right_state: DBState = Arc::new(Mutex::new(HashMap::new()));

        // Spawn a new thread and move `source` into it
        let _ = timely::execute_from_args(std::env::args(), move |worker| {
            let mut inputs: InputSessions = InputSessions::new(query.tables.clone());
            let mut local_source = source.clone();
            let table_name = table_name.clone();
            let mut first_entry: Option<DBRecord> = None;
            let left_key = query.joins.clone().get(0).unwrap().left.clone();
            let schema = table_identities.get(&query.tables[1]).unwrap();
            let mut right_column = schema
                .iter()
                .find(|key| match key.key_type {
                    KeyType::PrimaryKey => true,
                    _ => false,
                })
                .unwrap()
                .column_name
                .clone();
            let mut right_key = format!("{}.{}", query.tables[1], right_column);
            // access the state
            let left_state_clone = left_state.clone();
            let right_state_clone = right_state.clone();

            let probe = worker.dataflow(|scope| {
                let l_state = left_state_clone.clone();
                let r_state = right_state_clone.clone();
                let mut sink = sink.clone();
                // Create a new collection from our input.
                let mut collections = HashMap::new();
                for table in &query.tables.clone() {
                    let collection: Collection<_, DataflowData, isize> =
                        inputs.get(table).unwrap().to_collection(scope);
                    collections.insert(table.clone(), collection);
                }

                let join_prepare_left = move |x: DataflowData| {
                    let record = x.1 .1;
                    l_state.lock().unwrap().insert(x.0, (x.1 .0, record));
                    (x.0, x.1 .0)
                };
                let join_prepare_right = move |x: DataflowData| {
                    let foreign_key = x.1 .0;
                    let record = x.1 .1;
                    r_state.lock().unwrap().insert(x.0, (x.1 .0, record));
                    (foreign_key.unwrap_or(0), x.0)
                };

                // let output = {
                // Join the collections
                let left_table = String::from(query.tables.get(0).unwrap());
                let right_table = String::from(query.tables.get(1).unwrap());

                let output = {
                    let left_collection = collections
                        .get(&left_table.clone())
                        .unwrap()
                        .map(join_prepare_left)
                        .consolidate();
                    let right_collection = collections
                        .get(&right_table.clone())
                        .unwrap()
                        .map(join_prepare_right)
                        .consolidate();

                    let output = left_collection.join(&right_collection);
                    let output: Collection<
                        Child<'_, TimelyWorker<Allocator>, usize>,
                        (usize, (usize, DBRecord)),
                    > = output
                        .map(move |x| {
                            let mut left = left_state_clone
                                .lock()
                                .unwrap()
                                .get(&x.0)
                                .unwrap()
                                .1
                                .clone()
                                .prefix_keys(left_table.to_string());
                            let right = right_state_clone
                                .lock()
                                .unwrap()
                                .get(&x.1 .1)
                                .unwrap()
                                .1
                                .clone()
                                .prefix_keys(right_table.to_string());
                            (x.0, (x.1 .1, left.merge(right)))
                        })
                        .inspect(|x| debug!("Mapped: {:?}", x));
                    output
                };

                let output = if query.condition.is_some() {
                    let condition = query.condition.clone().unwrap();
                    let output = output.filter(move |x| {
                        let record = &x.1 .1;

                        let left = record.get(&condition.left.to_string());
                        let right = condition.right.clone();
                        match condition.op.as_str() {
                            "=" => left == right,
                            "!=" => left != right,
                            ">" => left.as_f64().unwrap() > right.parse().unwrap(),
                            "<" => left.as_f64().unwrap() < right.parse().unwrap(),
                            ">=" => left.as_f64().unwrap() >= right.parse().unwrap(),
                            "<=" => left.as_f64().unwrap() <= right.parse().unwrap(),
                            _ => false,
                        }
                    });
                    output
                } else {
                    output
                };

                output.inspect_batch(move |_t, batch| {
                    let first = batch.first().unwrap();
                    if first_entry.is_none() {
                        first_entry = Some(first.0 .1 .1.clone()); // x is a tuple of (data, time, diff)
                        sink.set_schema(first_entry.clone().unwrap().create_sql_schema());
                        sink.set_columns(first_entry.clone().unwrap().get_sql_columns());
                    }

                    let batch = batch.to_vec();

                    let mut values = batch
                        .iter()
                        .map(|((pk, (fk, record)), _, diff)| {
                            let record_type = RecordType::from_value(*diff);
                            record.to_sql_values(
                                record_type.clone(),
                                table_name.clone(),
                                match record_type {
                                    RecordType::Insert => None,
                                    RecordType::Delete => {
                                        let mut keys = Vec::new();
                                        keys.push((left_key.to_string(), pk.clone()));
                                        keys.push((right_key.as_str().to_string(), fk.clone()));
                                        Some(keys)
                                    }
                                },
                            )
                        })
                        .collect::<Vec<String>>();
                    sink.insert(&mut values);
                    sink.execute_transaction();
                });

                output.probe()
            });

            // Process events until the source is done
            inputs.advance_to(0);
            let mut buffer = Buffer::new();
            while !local_source.done() {
                if let Some(events) = local_source.fetch() {
                    for event in events {
                        let join_prop = query.joins.clone().get(0).unwrap().right.clone();
                        let required_key = if event.0 == join_prop.table {
                            Some(join_prop.row.to_string())
                        } else {
                            None
                        };
                        let parsed_event =
                            DataflowInput::from_wal_event(event.1.clone(), required_key);
                        for i in parsed_event.clone() {
                            buffer.insert(event.0.clone(), i.element, i.time, i.change);
                        }
                    }
                } else {
                    worker.step_while(|| probe.less_than(&inputs.time()));
                }
                let popped = buffer.pop();
                if let Some(popped) = popped {
                    for event in popped.clone() {
                        let (table, data, time, change) = event;

                        // Handle deletions with only primary key
                        if change == -1 {
                            // Determine if it's from left or right
                            let state = if table == query.tables[0] {
                                &left_state
                            } else {
                                &right_state
                            };

                            if let Some(full_record) = state.lock().unwrap().remove(&data.0) {
                                // fill in the rest of the record
                                inputs.update_at_for_table(
                                    &table,
                                    DataflowData(data.0, full_record.clone()), // Full key-value pair
                                    time,
                                    change,
                                );
                            } else {
                                info!("Delete event received for non-existent id: {:?}", data);
                            }
                        } else {
                            // For insertions or updates, handle normally
                            inputs.update_at_for_table(&table, data, time, change);
                        }
                    }
                    let time = popped.iter().map(|x| x.2).min().unwrap();
                    let max_time = popped.iter().map(|x| x.2).max().unwrap();
                    buffer.update_watermark(max_time);
                    inputs.advance_to(time);
                    inputs.flush();
                    info!("Advancing to time: {} and Flushed", time);
                }
                if buffer.data.is_empty() && (inputs.time() < buffer.get_watermark()) {
                    inputs.advance_to(buffer.get_watermark());
                    inputs.flush();
                    debug!(
                        "Advancing to time: {} and Flushed from buffer",
                        buffer.get_watermark()
                    );
                }
                worker.step_while(|| probe.less_than(&inputs.time()));
            }
        });
    }
}
