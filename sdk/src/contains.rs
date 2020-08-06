use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

pub trait Contains<T: Eq + Hash> {
    fn contains(&self, key: &T) -> bool;
}

impl<T: Eq + Hash, U> Contains<T> for HashMap<T, U> {
    fn contains(&self, key: &T) -> bool {
        self.contains_key(key)
    }
}
impl<T: Eq + Hash> Contains<T> for HashSet<T> {
    fn contains(&self, key: &T) -> bool {
        self.contains(key)
    }
}
