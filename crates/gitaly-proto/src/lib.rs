//! Protobuf bindings and RPC metadata registry.

#[cfg(gitaly_proto_codegen)]
pub mod gitaly {
    tonic::include_proto!("gitaly");
}

#[cfg(not(gitaly_proto_codegen))]
pub mod gitaly {}

#[cfg(gitaly_proto_codegen)]
pub mod raftpb {
    tonic::include_proto!("raftpb");
}

#[cfg(not(gitaly_proto_codegen))]
pub mod raftpb {}

pub mod registry {
    use std::collections::{HashMap, HashSet};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Operation {
        Unknown,
        Accessor,
        Mutator,
        Maintenance,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Scope {
        Unknown,
        Repository,
        Storage,
        Partition,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RpcEntry {
        pub full_method: String,
        pub service: String,
        pub method: String,
        pub operation: Operation,
        pub scope: Scope,
    }

    #[derive(Debug, Default)]
    pub struct RpcRegistry {
        entries: HashMap<String, RpcEntry>,
        intercepted_methods: HashSet<String>,
    }

    impl RpcRegistry {
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        #[must_use]
        pub fn from_generated() -> Self {
            let mut registry = Self::new();
            for &(full_method, service, method, operation, scope) in GENERATED_ENTRIES {
                let _ = registry.register(RpcEntry {
                    full_method: full_method.to_string(),
                    service: service.to_string(),
                    method: method.to_string(),
                    operation,
                    scope,
                });
            }

            registry.intercepted_methods.extend(
                GENERATED_INTERCEPTED_METHODS
                    .iter()
                    .map(|method| (*method).to_string()),
            );

            registry
        }

        pub fn register(&mut self, entry: RpcEntry) -> Option<RpcEntry> {
            self.entries.insert(entry.full_method.clone(), entry)
        }

        pub fn register_intercepted(&mut self, full_method: impl Into<String>) {
            self.intercepted_methods.insert(full_method.into());
        }

        #[must_use]
        pub fn lookup(&self, full_method: &str) -> Option<&RpcEntry> {
            self.entries.get(full_method)
        }

        #[must_use]
        pub fn is_intercepted_method(&self, full_method: &str) -> bool {
            self.intercepted_methods.contains(full_method)
        }
    }

    include!(concat!(env!("OUT_DIR"), "/registry_generated.rs"));

    #[cfg(test)]
    mod tests {
        use super::{Operation, RpcEntry, RpcRegistry, Scope};

        #[test]
        fn register_and_lookup_entry() {
            let mut registry = RpcRegistry::new();
            let entry = RpcEntry {
                full_method: "/gitaly.RepositoryService/RepositoryExists".to_string(),
                service: "gitaly.RepositoryService".to_string(),
                method: "RepositoryExists".to_string(),
                operation: Operation::Accessor,
                scope: Scope::Repository,
            };

            registry.register(entry.clone());

            let lookup = registry.lookup(&entry.full_method);
            assert_eq!(lookup, Some(&entry));
        }

        #[test]
        fn lookup_missing_method_returns_none() {
            let registry = RpcRegistry::new();

            assert!(registry
                .lookup("/gitaly.RepositoryService/DoesNotExist")
                .is_none());
        }

        #[test]
        fn generated_registry_contains_known_rpc_metadata() {
            let registry = RpcRegistry::from_generated();
            let method = "/gitaly.RepositoryService/RepositoryExists";
            let entry = registry.lookup(method).expect("entry should exist");

            assert_eq!(entry.operation, Operation::Accessor);
            assert_eq!(entry.scope, Scope::Repository);
        }

        #[test]
        fn generated_registry_tracks_intercepted_methods() {
            let registry = RpcRegistry::from_generated();
            let method = "/gitaly.ServerService/ServerInfo";

            assert!(registry.lookup(method).is_none());
            assert!(registry.is_intercepted_method(method));
        }

        #[test]
        fn generated_registry_contains_storage_and_partition_scopes() {
            let registry = RpcRegistry::from_generated();

            let storage_method = "/gitaly.PartitionService/ListPartitions";
            let storage_entry = registry
                .lookup(storage_method)
                .expect("storage-scoped method should exist");
            assert_eq!(storage_entry.scope, Scope::Storage);

            let partition_method = "/gitaly.PartitionService/BackupPartition";
            let partition_entry = registry
                .lookup(partition_method)
                .expect("partition-scoped method should exist");
            assert_eq!(partition_entry.scope, Scope::Partition);
        }

        #[test]
        fn generated_registry_has_broad_rpc_coverage() {
            let registry = RpcRegistry::from_generated();
            assert!(
                registry.entries.len() > 100,
                "expected broad RPC coverage, got {} entries",
                registry.entries.len()
            );
            assert!(
                registry.intercepted_methods.len() >= 6,
                "expected intercepted methods to be tracked, got {}",
                registry.intercepted_methods.len()
            );
        }
    }
}
