use error::NatsError;
use futures::prelude::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Debug, Default)]
pub struct AckTrigger(Arc<AtomicBool>);

impl AckTrigger {
    #[inline(always)]
    fn store(&self, val: bool) {
        self.0.store(val, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn pull_down(&self) {
        self.store(false);
    }

    #[inline(always)]
    pub fn pull_up(&self) {
        self.store(true);
    }

    #[inline(always)]
    pub fn is_up(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn fire(self) -> impl Future<Item = (), Error = NatsError> + Send + Sync {
        self.pull_down();
        self
    }
}

impl From<bool> for AckTrigger {
    fn from(v: bool) -> Self {
        AckTrigger(Arc::new(AtomicBool::from(v)))
    }
}

impl Clone for AckTrigger {
    #[inline(always)]
    fn clone(&self) -> Self {
        AckTrigger(Arc::clone(&self.0))
    }
}

impl Future for AckTrigger {
    type Item = ();
    type Error = NatsError;

    #[inline(always)]
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        Ok(if self.is_up() {
            Async::Ready(())
        } else {
            futures::task::current().notify();
            Async::NotReady
        })
    }
}
