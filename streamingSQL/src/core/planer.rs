use crate::{
    core::types::{
        dataflow_types::{DBRecord, DataflowData, DataflowInput},
        inputs::InputSessions,
        source::Source,
    },
    pg_client::data::WalEvent,
};
use std::hash::Hasher;
use std::thread;
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::Hash,
};

use super::parser::Query;
extern crate differential_dataflow;
extern crate timely;
use differential_dataflow::operators::Join;
use differential_dataflow::Collection;
use serde_json::Value;
use timely::communication::Allocate;
use timely::worker::Worker as TimelyWorker;
use tracing::{debug, info, warn};

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

                // Join the collections
                info!("Joining collections");
                let left_table = query.tables.get(0).unwrap();
                let right_table = query.tables.get(1).unwrap();
                let join_key = query.joins.get(0).unwrap().right.row.as_str();

                let left_collection = collections
                    .get(left_table)
                    .unwrap()
                    .map(|x| (x.0, x.1.as_raw_pointer() as usize));
                let right_collection = collections.get(right_table).unwrap().map(|x| {
                    let key = x.1 .0.get("buyerId").unwrap(); // Use join_key directly
                    let val = match key {
                        Value::Number(num) => usize::try_from(num.as_u64().unwrap()).unwrap(),
                        Value::String(str) => {
                            let mut hasher = DefaultHasher::new();
                            str.hash(&mut hasher);
                            let hash = hasher.finish();
                            usize::try_from(hash).unwrap()
                        }
                        _ => {
                            panic!("Failed to parse key")
                        }
                    };

                    (val, x.1.as_raw_pointer() as usize) // No need for return here
                });
                let joined_collection = left_collection.join(&right_collection);
                joined_collection.inspect(|x| info!("Joined: {:?}", x));
                joined_collection
                    .map(|x| {
                        (
                            x.0,
                            (
                                DBRecord::from_raw_pointer(x.1 .0 as *const DBRecord),
                                DBRecord::from_raw_pointer(x.1 .1 as *const DBRecord),
                            ),
                        )
                    })
                    .inspect(|x| info!("Mapped: {:?}", x));

                // Inspect the first collection if available
                // if let Some(first_table) = query.tables.clone().get(0) {
                //     collections
                //         .get_mut(first_table)
                //         .unwrap()
                //         .inspect(|x| info!("Inspect: {:?}", x));
                // }
            });

            // Process events until the source is done
            while !local_source.done() {
                if let Some(events) = local_source.fetch() {
                    let mut time = 0;
                    for event in events {
                        let parsed_event = DataflowInput::from_wal_event(event.1);
                        for i in parsed_event.clone() {
                            inputs.update_at_for_table(&event.0, i.element, i.time, i.change);
                        }
                        time = parsed_event.iter().map(|x| x.time).max().unwrap();
                    }
                    inputs.advance_to(time);
                    inputs.flush();
                    worker.step(); // Advance the worker
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
