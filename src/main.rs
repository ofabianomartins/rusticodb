pub mod storage;
pub mod parser;
pub mod setup;
pub mod machine;
pub mod config;
pub mod utils;
pub mod command_line;

use crate::setup::setup_system;
use crate::machine::machine::Machine;
use crate::machine::context::Context;
use crate::storage::pager::Pager;
use crate::command_line::command_line;

fn main() {
    let pager = Pager::new();
    let context = Context::new();
    let mut machine = Machine::new(pager, context);


    setup_system(&mut machine);
    command_line(&mut machine);
}
