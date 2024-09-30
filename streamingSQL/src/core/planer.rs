use crate::{
    core::types::{
        dataflow_types::{DataflowData, DataflowInput},
        inputs::InputSessions,
        source::Source,
    },
    pg_client::data::WalEvent,
};
use std::collections::HashMap;
use std::future::IntoFuture;
use std::thread;

use super::parser::Query;
extern crate differential_dataflow;
extern crate timely;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::Join;
use differential_dataflow::Collection;
use serde_json::Value;
use timely::communication::Allocate;
use timely::worker::Worker as TimelyWorker;
use tracing::{debug, info};

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

    pub fn build_dataflow(&self, query: Query, source: Source) {
        info!("Building Dataflow for Query: {:?}", query);
        // Spawn a new thread and move `source` into it
        let _ = timely::execute_from_args(std::env::args(), move |worker| {
            let mut inputs: InputSessions = InputSessions::new(query.tables.clone());
            let mut local_source = source.clone();
            info!("Worker started");

            worker.dataflow(|scope| {
                // Create a new collection from our input.
                let mut collections = HashMap::new();
                for table in &query.tables.clone() {
                    let collection: Collection<_, DataflowData, isize> =
                        inputs.get(table).unwrap().to_collection(scope);
                    collections.insert(table.clone(), collection);
                }

                // Inspect the first collection if available
                if let Some(first_table) = query.tables.clone().get(0) {
                    collections
                        .get_mut(first_table)
                        .unwrap()
                        .inspect(|x| info!("Inspect: {:?}", x));
                }
            });

            // Process events until the source is done
            while !local_source.done() {
                if let Some(events) = local_source.fetch() {
                    for event in events {
                        let parsed_event = DataflowInput::from_wal_event(event.1);
                        for i in parsed_event.clone() {
                            inputs.update_at_for_table(&event.0, i.element, i.time, i.change);
                        }
                        let time = parsed_event.iter().map(|x| x.time).max().unwrap();
                        inputs.advance_to(time);
                        inputs.flush();
                        worker.step(); // Advance the worker
                    }
                } else {
                    worker.step(); // No events; step the worker
                }
            }
        });
        loop {
            thread::sleep(std::time::Duration::from_secs(1));
        }
    }
}
