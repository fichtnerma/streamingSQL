use std::collections::HashMap;
use std::future::IntoFuture;

use crate::core::dataflow_types::DataflowInput;
use crate::pg_client::data::WalEvent;

use super::dataflow_types::DataflowData;
use super::parser::Query;
extern crate differential_dataflow;
extern crate timely;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Join;
use differential_dataflow::Collection;
use serde_json::Value;
use timely::communication::Allocate;
use timely::worker::Worker as TimelyWorker;

pub struct ActiveCompute<A: Allocate> {
    worker: TimelyWorker<A>,
    planer: QueryPlaner,
    export: String,
}

pub struct QueryPlaner {}

impl QueryPlaner {
    pub fn new() -> Self {
        QueryPlaner {}
    }

    pub async fn build_dataflow(
        &self,
        query: &mut Query,
        publishers: HashMap<String, tokio::sync::broadcast::Sender<Vec<WalEvent>>>,
    ) {
        let query_clone = query.clone();
        let _ = timely::execute_from_args(std::env::args(), move |worker| {
            let mut inputs: HashMap<String, InputSession<usize, DataflowData, isize>> =
                HashMap::new();

            worker.dataflow(|scope| {
                // create a new collection from our input.
                let mut collections = HashMap::new();
                query_clone.tables.iter().for_each(|table| {
                    let mut input: InputSession<usize, DataflowData, isize> = InputSession::new();
                    let collection: Collection<_, DataflowData, isize> = input.to_collection(scope);
                    collections.insert(table.to_string(), collection);
                    inputs.insert(table.to_string(), input);
                });
                // if (m2, m1) and (m1, p), then output (m1, (m2, p))
                collections
                    .get_mut(&query_clone.tables[0])
                    .unwrap()
                    .inspect(|x| println!("{:?}", x));
            });
            // Publish to the export
            for (table, tx) in publishers.iter() {
                let mut rx = tx.subscribe();
                loop {
                    match rx.try_recv() {
                        Ok(event) => {
                            let parsed_event = DataflowInput::from_wal_event(event);
                            let input = inputs.get_mut(table).unwrap();
                            for i in parsed_event.clone() {
                                input.update_at(i.element, i.time, i.change);
                            }
                            let time = parsed_event.iter().map(|x| x.time).max().unwrap();
                            input.advance_to(time);
                            input.flush();
                            worker.step();
                        }
                        Err(_) => {
                            worker.step_or_park(None);
                        }
                    }
                }
            }
        });
    }
}
