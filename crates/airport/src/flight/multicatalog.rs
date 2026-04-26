/// Multi-catalog routing support.
/// This module provides utilities for the MultiCatalogServer.
/// See multicatalog.rs at the crate root for the full implementation.

use crate::catalog::Catalog;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// CatalogRegistry stores multiple named catalogs.
#[derive(Default)]
pub struct CatalogRegistry {
    catalogs: RwLock<HashMap<String, Arc<dyn Catalog>>>,
}

impl CatalogRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a catalog to the registry. Returns error if a catalog with the same name exists.
    pub fn add(&self, name: impl Into<String>, catalog: Arc<dyn Catalog>) -> Result<(), String> {
        let name = name.into();
        let mut map = self.catalogs.write().unwrap();
        if map.contains_key(&name) {
            return Err(format!("catalog '{}' already exists", name));
        }
        map.insert(name, catalog);
        Ok(())
    }

    /// Removes a catalog by name.
    pub fn remove(&self, name: &str) -> bool {
        let mut map = self.catalogs.write().unwrap();
        map.remove(name).is_some()
    }

    /// Gets a catalog by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        let map = self.catalogs.read().unwrap();
        map.get(name).cloned()
    }

    /// Returns all catalog names.
    pub fn names(&self) -> Vec<String> {
        let map = self.catalogs.read().unwrap();
        map.keys().cloned().collect()
    }

    pub fn exists(&self, name: &str) -> bool {
        let map = self.catalogs.read().unwrap();
        map.contains_key(name)
    }
}
