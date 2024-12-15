//! Used by validators to run events on exit.

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        RwLock,
    },
};

#[derive(Default)]
pub struct Exit {
    exited: AtomicBool,
    exits: RwLock<Vec<Box<dyn FnOnce() + Send + Sync>>>,
}

impl Exit {
    pub fn is_exited(&self) -> bool {
        self.exited.load(Ordering::Relaxed)
    }
    pub fn register_exit(&self, exit: Box<dyn FnOnce() + Send + Sync>) {
        if self.is_exited() {
            exit();
        } else {
            let mut w_exits = self.exits.write().unwrap();
            if self.exited.load(Ordering::Relaxed) {
                exit();
            } else {
                w_exits.push(exit);
            }
        }
    }

    pub fn exit(&self) {
        let mut w_exits = self.exits.write().unwrap();
        self.exited.store(true, Ordering::Relaxed);
        for exit in w_exits.drain(..) {
            exit();
        }
    }
}

/*impl fmt::Debug for Exit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} exits", self.exits.read().unwrap().len())
    }
}*/
