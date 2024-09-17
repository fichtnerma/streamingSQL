
use super::parser::Query;
extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::Join;
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

    pub fn plan(&self, query: Query) -> Result<(), Box<dyn std::error::Error>> {
        let input = "";

        let length = input.len();

        let mut input = InputSession::new();

        // define a new timely dataflow computation.
        timely::execute_from_args(std::env::args(), move |worker| {
            let index = worker.index();
            let peers = worker.peers();

            let worker_input = input
                .split('\n')
                .enumerate()
                .filter(|&(pos, _)| pos % peers == index)
                .map(|(pos, line)| (pos, line.to_string()))
                .collect::<Vec<_>>();

            worker.dataflow::<(), _, _>(|scope| {
                let digits = scope.new_collection_from(worker_input).1;

                // The two parts ask to line up elements with the next one in the sequence (part 1) and
                // the one half way around the sequence (part 2). To find matches, we will shift the
                // associated position field while keeping the part identifier as a field (`1` or `2`
                // respectively). We then restrict both by the original `digits` to find matches.

                let part1 =
                    digits.map(move |(digit, position)| ((digit, (position + 1) % length), 1));
                let part2 = digits
                    .map(move |(digit, position)| ((digit, (position + length / 2) % length), 2));

                part1
                    .concat(&part2) // merge collections.
                    .semijoin(&digits) // restrict to matches.
                    .explode(|((digit, _pos), part)| Some((part, digit as isize))) // `part` with weight `digit`.
                    .consolidate() // consolidate weights by `part`.
                    .inspect(|elt| println!("part {} accumulation: {:?}", elt.0, elt.2));
                // check out answers.
            });
        })
        .unwrap();
        Ok(())
    }

    pub fn build_dataflow(&self, query: &mut Query) {
        timely::execute_from_args(std::env::args(), move |timely_worker| {
            let name = format!("Dataflow: {}", query.tables.join(", "));
            let logging = timely_worker.log_register().new_logger(name.clone());

            let probe = timely_worker.dataflow_core(&name, logging, Box::new(()), |_, scope| {
                let input = Input::new();
                let input = input.to_collection(scope);

                // JOIN
                match query.join {
                    Some(join) => {
                        let input = input.filter(|x| x.0 == join.left.table);
                        let right = input.filter(|x| x.0 == join.right.table);

                        let input = input.join(&right).inspect(|x| println!("{:?}", x));
                    }
                    None => {}
                }
                // WHERE
                match query.condition {
                    Some(condition) => {
                        let input = input.filter(|x| x.0 == condition.left.table);
                    }
                    None => {}
                }

                // SELECT properties from rows
                let input = input.map(|x| x.0 == query.rows[0].table);
                query.rows.iter().for_each(|row| {
                    let input = input.map(|x| x.0 == row.table);
                    input.inspect(|x| println!("{:?}", x));
                });
            });
        })
    }

    fn plan_insert() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn plan_update() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn plan_delete() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
