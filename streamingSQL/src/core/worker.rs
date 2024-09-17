use std::rc::Rc;


pub struct Worker {
}

impl Worker {
    pub fn new() -> Self {
        Worker {
        }
    }

    pub async fn listen_to_changes(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let name = "Ferris";
        let data = None::<&[u8]>;
        Ok(())
    }
}
