use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use gitaly_proto::gitaly::{
    cluster_statistics, get_partitions_response, ClusterStatistics, GetPartitionsRequest,
    GetPartitionsResponse, JoinClusterRequest, RaftClusterInfoResponse, RaftMessageRequest,
    RaftPartitionKey, ReplicaId,
};
use openraft::{BasicNode, Config};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};

const DEFAULT_CLUSTER_ID: &str = "default";
const UNKNOWN_PARTITION_KEY: &str = "unknown";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub destination: String,
    pub snapshot_size: u64,
}

#[derive(Debug, Clone)]
struct RecordedMessage {
    from: u64,
    to: u64,
    term: u64,
    index: u64,
}

#[derive(Debug, Clone, Default)]
struct SnapshotState {
    destination: String,
    snapshot_size: u64,
    term: u64,
    index: u64,
}

#[derive(Debug, Clone)]
struct ReplicaState {
    replica_id: ReplicaId,
    node: BasicNode,
    last_index: u64,
    match_index: u64,
    state: String,
}

impl ReplicaState {
    fn new(replica_id: ReplicaId) -> Self {
        Self {
            node: basic_node_for_replica(&replica_id),
            replica_id,
            last_index: 0,
            match_index: 0,
            state: String::new(),
        }
    }

    fn update_replica_id(&mut self, replica_id: ReplicaId) {
        self.node = basic_node_for_replica(&replica_id);
        self.replica_id = replica_id;
    }

    fn is_healthy(&self) -> bool {
        !self.node.addr.trim().is_empty()
    }

    fn storage_name(&self) -> &str {
        &self.replica_id.storage_name
    }
}

#[derive(Debug, Clone)]
struct PartitionState {
    partition_key: String,
    replicas: HashMap<u64, ReplicaState>,
    relative_paths: BTreeSet<String>,
    leader_id: u64,
    term: u64,
    index: u64,
    messages: Vec<RecordedMessage>,
    snapshot: Option<SnapshotState>,
}

impl PartitionState {
    fn new(partition_key: String) -> Self {
        Self {
            partition_key,
            replicas: HashMap::new(),
            relative_paths: BTreeSet::new(),
            leader_id: 0,
            term: 0,
            index: 0,
            messages: Vec::new(),
            snapshot: None,
        }
    }

    fn upsert_replica(&mut self, replica_id: ReplicaId) {
        let member_id = replica_id.member_id;
        if member_id == 0 {
            return;
        }

        match self.replicas.get_mut(&member_id) {
            Some(existing) => existing.update_replica_id(replica_id),
            None => {
                self.replicas
                    .insert(member_id, ReplicaState::new(replica_id));
            }
        }
    }

    fn ensure_message_origin_replica(&mut self, member_id: u64) {
        if member_id == 0 {
            return;
        }

        self.replicas.entry(member_id).or_insert_with(|| {
            ReplicaState::new(ReplicaId {
                partition_key: Some(RaftPartitionKey {
                    value: self.partition_key.clone(),
                }),
                member_id,
                storage_name: String::new(),
                metadata: None,
                r#type: 0,
            })
        });
    }

    fn normalize_replica_states(&mut self) {
        for (member_id, replica) in &mut self.replicas {
            if *member_id == self.leader_id {
                replica.state = "StateLeader".to_string();
            } else if replica.state.trim().is_empty() {
                replica.state = "StateFollower".to_string();
            }
        }
    }

    fn record_message(&mut self, request: &RaftMessageRequest) {
        if let Some(replica_id) = request.replica_id.clone() {
            let member_id = replica_id.member_id;
            self.upsert_replica(replica_id);
            if member_id != 0 {
                self.leader_id = member_id;
            }
        }

        if let Some(message) = &request.message {
            let term = message.term.unwrap_or(0);
            let index = message
                .index
                .or(message.commit)
                .or(message.log_term)
                .unwrap_or(0);

            self.term = self.term.max(term);
            self.index = self.index.max(index);
            self.messages.push(RecordedMessage {
                from: message.from.unwrap_or(0),
                to: message.to.unwrap_or(0),
                term,
                index,
            });

            let from_id = message.from.unwrap_or(0);
            if self.leader_id == 0 && from_id != 0 {
                self.leader_id = from_id;
            }

            self.ensure_message_origin_replica(from_id);
            if let Some(replica) = self.replicas.get_mut(&from_id) {
                replica.last_index = replica.last_index.max(index);
                replica.match_index = replica.match_index.max(index);
            }

            if let Some(snapshot_meta) = message
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.metadata.as_ref())
            {
                self.term = self.term.max(snapshot_meta.term.unwrap_or(0));
                self.index = self.index.max(snapshot_meta.index.unwrap_or(0));
            }
        }

        self.normalize_replica_states();
    }

    fn record_snapshot(
        &mut self,
        request: &RaftMessageRequest,
        snapshot_size: u64,
    ) -> SnapshotRecord {
        self.record_message(request);

        let snapshot_term = request
            .message
            .as_ref()
            .and_then(|message| message.snapshot.as_ref())
            .and_then(|snapshot| snapshot.metadata.as_ref())
            .and_then(|metadata| metadata.term)
            .unwrap_or(self.term);
        let snapshot_index = request
            .message
            .as_ref()
            .and_then(|message| message.snapshot.as_ref())
            .and_then(|snapshot| snapshot.metadata.as_ref())
            .and_then(|metadata| metadata.index)
            .unwrap_or(self.index);

        self.term = self.term.max(snapshot_term);
        self.index = self.index.max(snapshot_index);

        let snapshot = SnapshotState {
            destination: "in-memory".to_string(),
            snapshot_size,
            term: self.term,
            index: self.index,
        };
        self.snapshot = Some(snapshot.clone());

        SnapshotRecord {
            destination: snapshot.destination,
            snapshot_size: snapshot.snapshot_size,
        }
    }

    fn matches_filters(&self, request: &GetPartitionsRequest) -> bool {
        if let Some(partition_key) = &request.partition_key {
            if partition_key.value != self.partition_key {
                return false;
            }
        }

        let relative_path = request.relative_path.trim();
        if !relative_path.is_empty() && !self.relative_paths.contains(relative_path) {
            return false;
        }

        let storage_name = request.storage.trim();
        if !storage_name.is_empty()
            && !self
                .replicas
                .values()
                .any(|replica| replica.storage_name() == storage_name)
        {
            return false;
        }

        true
    }

    fn as_response(
        &self,
        cluster_id: &str,
        request: &GetPartitionsRequest,
    ) -> GetPartitionsResponse {
        let mut replicas = Vec::new();
        if request.include_replica_details {
            let mut ordered_replicas = self.replicas.iter().collect::<Vec<_>>();
            ordered_replicas.sort_by_key(|(member_id, _)| **member_id);

            replicas.extend(ordered_replicas.into_iter().map(|(member_id, replica)| {
                let message_bias = self
                    .messages
                    .iter()
                    .filter(|message| message.from == *member_id || message.to == *member_id)
                    .map(|message| (message.index, message.term))
                    .max_by_key(|(index, term)| (*index, *term));
                let (message_index, _message_term) = message_bias.unwrap_or((0, 0));
                let snapshot_index = self
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.index)
                    .unwrap_or(0);
                let snapshot_term = self
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.term)
                    .unwrap_or(0);

                get_partitions_response::ReplicaStatus {
                    replica_id: Some(replica.replica_id.clone()),
                    is_leader: *member_id == self.leader_id,
                    is_healthy: replica.is_healthy(),
                    last_index: replica.last_index.max(message_index),
                    match_index: replica.match_index.max(message_index),
                    state: if *member_id == self.leader_id {
                        if snapshot_term > 0 || snapshot_index > 0 {
                            "StateLeader".to_string()
                        } else {
                            replica.state.clone()
                        }
                    } else {
                        replica.state.clone()
                    },
                }
            }));
        }

        let mut relative_paths = if request.include_relative_paths {
            self.relative_paths.iter().cloned().collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        if request.include_relative_paths
            && relative_paths.is_empty()
            && !request.relative_path.trim().is_empty()
        {
            relative_paths.push(request.relative_path.clone());
        }

        let relative_path = if !request.relative_path.trim().is_empty() {
            request.relative_path.clone()
        } else {
            relative_paths.first().cloned().unwrap_or_default()
        };

        GetPartitionsResponse {
            cluster_id: cluster_id.to_string(),
            partition_key: Some(RaftPartitionKey {
                value: self.partition_key.clone(),
            }),
            replicas,
            leader_id: self.leader_id,
            term: self.term,
            index: self.index,
            relative_path,
            relative_paths,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ClusterState {
    partitions: HashMap<String, PartitionState>,
}

#[derive(Debug, Clone, Default)]
struct StateStore {
    clusters: HashMap<String, ClusterState>,
    partition_to_cluster: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedStateStore {
    schema_version: u32,
    clusters: Vec<PersistedClusterState>,
    partition_to_cluster: Vec<PersistedPartitionToCluster>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedClusterState {
    cluster_id: String,
    partitions: Vec<PersistedPartitionState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPartitionState {
    partition_key: String,
    replicas: Vec<PersistedReplicaState>,
    relative_paths: Vec<String>,
    leader_id: u64,
    term: u64,
    index: u64,
    messages: Vec<PersistedRecordedMessage>,
    snapshot: Option<PersistedSnapshotState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedReplicaState {
    member_id: u64,
    replica_id: Vec<u8>,
    last_index: u64,
    match_index: u64,
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedRecordedMessage {
    from: u64,
    to: u64,
    term: u64,
    index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSnapshotState {
    destination: String,
    snapshot_size: u64,
    term: u64,
    index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPartitionToCluster {
    partition_key: String,
    cluster_id: String,
}

const PERSISTED_STATE_SCHEMA_VERSION: u32 = 1;

impl From<&StateStore> for PersistedStateStore {
    fn from(state: &StateStore) -> Self {
        let mut clusters = state.clusters.iter().collect::<Vec<_>>();
        clusters.sort_by(|(left, _), (right, _)| left.cmp(right));

        let clusters = clusters
            .into_iter()
            .map(|(cluster_id, cluster)| {
                let mut partitions = cluster.partitions.iter().collect::<Vec<_>>();
                partitions.sort_by(|(left, _), (right, _)| left.cmp(right));

                let partitions =
                    partitions
                        .into_iter()
                        .map(|(_, partition)| {
                            let mut replicas = partition.replicas.iter().collect::<Vec<_>>();
                            replicas.sort_by_key(|(member_id, _)| **member_id);

                            let replicas = replicas
                                .into_iter()
                                .filter_map(|(member_id, replica)| {
                                    let mut replica_id = Vec::new();
                                    replica.replica_id.encode(&mut replica_id).ok()?;

                                    Some(PersistedReplicaState {
                                        member_id: *member_id,
                                        replica_id,
                                        last_index: replica.last_index,
                                        match_index: replica.match_index,
                                        state: replica.state.clone(),
                                    })
                                })
                                .collect::<Vec<_>>();

                            let relative_paths = partition.relative_paths.iter().cloned().collect();
                            let messages = partition
                                .messages
                                .iter()
                                .map(|message| PersistedRecordedMessage {
                                    from: message.from,
                                    to: message.to,
                                    term: message.term,
                                    index: message.index,
                                })
                                .collect();
                            let snapshot = partition.snapshot.as_ref().map(|snapshot| {
                                PersistedSnapshotState {
                                    destination: snapshot.destination.clone(),
                                    snapshot_size: snapshot.snapshot_size,
                                    term: snapshot.term,
                                    index: snapshot.index,
                                }
                            });

                            PersistedPartitionState {
                                partition_key: partition.partition_key.clone(),
                                replicas,
                                relative_paths,
                                leader_id: partition.leader_id,
                                term: partition.term,
                                index: partition.index,
                                messages,
                                snapshot,
                            }
                        })
                        .collect::<Vec<_>>();

                PersistedClusterState {
                    cluster_id: cluster_id.clone(),
                    partitions,
                }
            })
            .collect::<Vec<_>>();

        let mut partition_to_cluster = state.partition_to_cluster.iter().collect::<Vec<_>>();
        partition_to_cluster.sort_by(|(left, _), (right, _)| left.cmp(right));
        let partition_to_cluster = partition_to_cluster
            .into_iter()
            .map(|(partition_key, cluster_id)| PersistedPartitionToCluster {
                partition_key: partition_key.clone(),
                cluster_id: cluster_id.clone(),
            })
            .collect::<Vec<_>>();

        Self {
            schema_version: PERSISTED_STATE_SCHEMA_VERSION,
            clusters,
            partition_to_cluster,
        }
    }
}

impl From<PersistedStateStore> for StateStore {
    fn from(persisted: PersistedStateStore) -> Self {
        if persisted.schema_version != PERSISTED_STATE_SCHEMA_VERSION {
            return Self::default();
        }

        let clusters = persisted
            .clusters
            .into_iter()
            .map(|cluster| {
                let partitions = cluster
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        let replicas = partition
                            .replicas
                            .into_iter()
                            .filter_map(|replica| {
                                let replica_id =
                                    ReplicaId::decode(replica.replica_id.as_slice()).ok()?;
                                let member_id = if replica.member_id == 0 {
                                    replica_id.member_id
                                } else {
                                    replica.member_id
                                };
                                if member_id == 0 {
                                    return None;
                                }

                                let mut replica_state = ReplicaState::new(replica_id);
                                replica_state.last_index = replica.last_index;
                                replica_state.match_index = replica.match_index;
                                replica_state.state = replica.state;

                                Some((member_id, replica_state))
                            })
                            .collect::<HashMap<_, _>>();
                        let relative_paths = partition.relative_paths.into_iter().collect();
                        let messages = partition
                            .messages
                            .into_iter()
                            .map(|message| RecordedMessage {
                                from: message.from,
                                to: message.to,
                                term: message.term,
                                index: message.index,
                            })
                            .collect::<Vec<_>>();
                        let snapshot = partition.snapshot.map(|snapshot| SnapshotState {
                            destination: snapshot.destination,
                            snapshot_size: snapshot.snapshot_size,
                            term: snapshot.term,
                            index: snapshot.index,
                        });

                        (
                            partition.partition_key.clone(),
                            PartitionState {
                                partition_key: partition.partition_key,
                                replicas,
                                relative_paths,
                                leader_id: partition.leader_id,
                                term: partition.term,
                                index: partition.index,
                                messages,
                                snapshot,
                            },
                        )
                    })
                    .collect::<HashMap<_, _>>();

                (cluster.cluster_id, ClusterState { partitions })
            })
            .collect::<HashMap<_, _>>();

        let partition_to_cluster = persisted
            .partition_to_cluster
            .into_iter()
            .map(|entry| (entry.partition_key, entry.cluster_id))
            .collect::<HashMap<_, _>>();

        Self {
            clusters,
            partition_to_cluster,
        }
    }
}

#[derive(Debug)]
pub struct ClusterStateManager {
    config: Arc<Config>,
    state: RwLock<StateStore>,
    state_path: Option<PathBuf>,
}

impl Default for ClusterStateManager {
    fn default() -> Self {
        let config = Config {
            cluster_name: "gitaly-cluster".to_string(),
            ..Config::default()
        }
        .validate()
        .expect("default OpenRaft configuration should validate");

        Self {
            config: Arc::new(config),
            state: RwLock::new(StateStore::default()),
            state_path: None,
        }
    }
}

impl ClusterStateManager {
    fn from_state_path(state_path: Option<PathBuf>) -> Self {
        let manager = Self {
            state_path,
            ..Self::default()
        };
        manager.load_state();
        manager
    }

    #[must_use]
    pub fn new() -> Self {
        Self::from_state_path(None)
    }

    #[must_use]
    pub fn with_state_path(state_path: impl Into<PathBuf>) -> Self {
        Self::from_state_path(Some(state_path.into()))
    }

    #[must_use]
    pub fn with_optional_state_path(state_path: Option<PathBuf>) -> Self {
        Self::from_state_path(state_path)
    }

    #[must_use]
    pub fn openraft_config(&self) -> Arc<Config> {
        Arc::clone(&self.config)
    }

    pub fn record_message(&self, request: &RaftMessageRequest) {
        let cluster_id = request.cluster_id.trim();
        if cluster_id.is_empty() {
            return;
        }

        let partition_key = partition_key_from_message(request);

        let mut state = self
            .state
            .write()
            .expect("cluster state lock should not be poisoned");
        state
            .partition_to_cluster
            .insert(partition_key.clone(), cluster_id.to_string());
        let cluster = state.clusters.entry(cluster_id.to_string()).or_default();
        let partition = cluster
            .partitions
            .entry(partition_key.clone())
            .or_insert_with(|| PartitionState::new(partition_key));
        partition.record_message(request);

        let state_snapshot = state.clone();
        drop(state);
        self.persist_state(&state_snapshot);
    }

    #[must_use]
    pub fn record_snapshot(
        &self,
        raft_message: &RaftMessageRequest,
        snapshot_size: u64,
    ) -> SnapshotRecord {
        let cluster_id = raft_message.cluster_id.trim();
        if cluster_id.is_empty() {
            return SnapshotRecord {
                destination: "in-memory".to_string(),
                snapshot_size,
            };
        }

        let partition_key = partition_key_from_message(raft_message);
        let mut state = self
            .state
            .write()
            .expect("cluster state lock should not be poisoned");
        state
            .partition_to_cluster
            .insert(partition_key.clone(), cluster_id.to_string());

        let cluster = state.clusters.entry(cluster_id.to_string()).or_default();
        let partition = cluster
            .partitions
            .entry(partition_key.clone())
            .or_insert_with(|| PartitionState::new(partition_key));
        let snapshot = partition.record_snapshot(raft_message, snapshot_size);

        let state_snapshot = state.clone();
        drop(state);
        self.persist_state(&state_snapshot);

        snapshot
    }

    pub fn process_join_cluster(&self, request: &JoinClusterRequest) {
        let Some(partition_key) = request.partition_key.as_ref().map(|key| key.value.trim()) else {
            return;
        };
        if partition_key.is_empty() {
            return;
        }

        let partition_key = partition_key.to_string();

        let mut state = self
            .state
            .write()
            .expect("cluster state lock should not be poisoned");
        let cluster_id = state
            .partition_to_cluster
            .get(&partition_key)
            .cloned()
            .unwrap_or_else(|| DEFAULT_CLUSTER_ID.to_string());

        state
            .partition_to_cluster
            .insert(partition_key.clone(), cluster_id.clone());

        let cluster = state.clusters.entry(cluster_id).or_default();
        let partition = cluster
            .partitions
            .entry(partition_key.clone())
            .or_insert_with(|| PartitionState::new(partition_key.clone()));

        if request.leader_id != 0 {
            partition.leader_id = request.leader_id;
        }

        let relative_path = request.relative_path.trim();
        if !relative_path.is_empty() {
            partition.relative_paths.insert(relative_path.to_string());
        }

        if request.member_id != 0 {
            partition.upsert_replica(ReplicaId {
                partition_key: Some(RaftPartitionKey {
                    value: partition_key.clone(),
                }),
                member_id: request.member_id,
                storage_name: request.storage_name.clone(),
                metadata: None,
                r#type: 0,
            });
        }

        for replica in &request.replicas {
            partition.upsert_replica(replica.clone());
        }

        partition.normalize_replica_states();

        let state_snapshot = state.clone();
        drop(state);
        self.persist_state(&state_snapshot);
    }

    #[must_use]
    pub fn get_partitions(&self, request: &GetPartitionsRequest) -> Vec<GetPartitionsResponse> {
        let cluster_id = request.cluster_id.trim();
        if cluster_id.is_empty() {
            return Vec::new();
        }

        let state = self
            .state
            .read()
            .expect("cluster state lock should not be poisoned");
        let Some(cluster) = state.clusters.get(cluster_id) else {
            return Vec::new();
        };

        let mut responses = cluster
            .partitions
            .values()
            .filter(|partition| partition.matches_filters(request))
            .map(|partition| partition.as_response(cluster_id, request))
            .collect::<Vec<_>>();
        responses.sort_by(|left, right| {
            let left_key = left
                .partition_key
                .as_ref()
                .map(|partition_key| partition_key.value.as_str())
                .unwrap_or("");
            let right_key = right
                .partition_key
                .as_ref()
                .map(|partition_key| partition_key.value.as_str())
                .unwrap_or("");
            left_key.cmp(right_key)
        });
        responses
    }

    #[must_use]
    pub fn get_cluster_info(&self, cluster_id: &str) -> RaftClusterInfoResponse {
        let state = self
            .state
            .read()
            .expect("cluster state lock should not be poisoned");
        let statistics = state
            .clusters
            .get(cluster_id)
            .map_or_else(ClusterStatistics::default, collect_cluster_statistics);

        RaftClusterInfoResponse {
            cluster_id: cluster_id.to_string(),
            statistics: Some(statistics),
        }
    }

    fn load_state(&self) {
        let Some(state_path) = self.state_path.as_deref() else {
            return;
        };
        let Ok(bytes) = fs::read(state_path) else {
            return;
        };
        let Ok(persisted) = serde_json::from_slice::<PersistedStateStore>(&bytes) else {
            return;
        };

        let loaded_state = StateStore::from(persisted);
        let mut state = self
            .state
            .write()
            .expect("cluster state lock should not be poisoned");
        *state = loaded_state;
    }

    fn persist_state(&self, state: &StateStore) {
        let Some(state_path) = self.state_path.as_deref() else {
            return;
        };

        let _ = persist_state_to_path(state_path, state);
    }
}

fn persist_state_to_path(state_path: &Path, state: &StateStore) -> io::Result<()> {
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let serialized = serde_json::to_vec(&PersistedStateStore::from(state))
        .map_err(|error| io::Error::other(format!("failed to serialize state: {error}")))?;
    let tmp_path = state_path.with_extension("tmp");
    fs::write(&tmp_path, serialized)?;

    if let Err(error) = fs::rename(&tmp_path, state_path) {
        if state_path.exists() {
            fs::remove_file(state_path)?;
            fs::rename(&tmp_path, state_path)
        } else {
            Err(error)
        }
    } else {
        Ok(())
    }
}

fn collect_cluster_statistics(cluster: &ClusterState) -> ClusterStatistics {
    let mut statistics = ClusterStatistics::default();

    statistics.total_partitions = u32::try_from(cluster.partitions.len()).unwrap_or(u32::MAX);

    for partition in cluster.partitions.values() {
        statistics.total_replicas = statistics
            .total_replicas
            .saturating_add(u32::try_from(partition.replicas.len()).unwrap_or(u32::MAX));

        if partition.leader_id != 0 && is_leader_healthy(partition) {
            statistics.healthy_partitions = statistics.healthy_partitions.saturating_add(1);
        }

        for (member_id, replica) in &partition.replicas {
            if replica.is_healthy() {
                statistics.healthy_replicas = statistics.healthy_replicas.saturating_add(1);
            }

            let storage_name = replica.storage_name().trim();
            if storage_name.is_empty() {
                continue;
            }

            let storage_entry = statistics
                .storage_stats
                .entry(storage_name.to_string())
                .or_insert(cluster_statistics::StorageStats {
                    leader_count: 0,
                    replica_count: 0,
                });
            storage_entry.replica_count = storage_entry.replica_count.saturating_add(1);
            if *member_id == partition.leader_id {
                storage_entry.leader_count = storage_entry.leader_count.saturating_add(1);
            }
        }
    }

    statistics
}

fn is_leader_healthy(partition: &PartitionState) -> bool {
    partition
        .replicas
        .get(&partition.leader_id)
        .is_some_and(ReplicaState::is_healthy)
}

fn partition_key_from_message(request: &RaftMessageRequest) -> String {
    request
        .replica_id
        .as_ref()
        .and_then(|replica| replica.partition_key.as_ref())
        .map(|partition_key| partition_key.value.trim())
        .filter(|partition_key| !partition_key.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| UNKNOWN_PARTITION_KEY.to_string())
}

fn basic_node_for_replica(replica_id: &ReplicaId) -> BasicNode {
    let address = replica_id
        .metadata
        .as_ref()
        .map(|metadata| metadata.address.trim())
        .unwrap_or("");
    BasicNode::new(address)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use gitaly_proto::gitaly::{
        cluster_statistics, replica_id, ClusterStatistics, GetPartitionsRequest,
        JoinClusterRequest, RaftMessageRequest, RaftPartitionKey, ReplicaId,
    };
    use gitaly_proto::raftpb::{Message, Snapshot, SnapshotMetadata};

    use crate::ClusterStateManager;

    static TEST_STATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn unique_state_file_path(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after unix epoch")
            .as_nanos();
        let sequence = TEST_STATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "gitaly-cluster-{test_name}-{}-{sequence}-{nanos}.json",
            std::process::id()
        ))
    }

    fn build_message(cluster_id: &str, partition_key: &str, member_id: u64) -> RaftMessageRequest {
        RaftMessageRequest {
            cluster_id: cluster_id.to_string(),
            replica_id: Some(ReplicaId {
                partition_key: Some(RaftPartitionKey {
                    value: partition_key.to_string(),
                }),
                member_id,
                storage_name: "default".to_string(),
                metadata: Some(replica_id::Metadata {
                    address: "127.0.0.1:2301".to_string(),
                }),
                r#type: 1,
            }),
            message: Some(Message {
                from: Some(member_id),
                term: Some(4),
                index: Some(12),
                snapshot: Some(Snapshot {
                    metadata: Some(SnapshotMetadata {
                        term: Some(4),
                        index: Some(12),
                        conf_state: None,
                    }),
                    data: None,
                }),
                ..Message::default()
            }),
        }
    }

    #[test]
    fn openraft_config_is_validated() {
        let manager = ClusterStateManager::new();
        let config = manager.openraft_config();

        assert_eq!(config.cluster_name, "gitaly-cluster");
        assert!(config.election_timeout_min < config.election_timeout_max);
    }

    #[test]
    fn tracks_partition_and_cluster_state_from_message_join_and_snapshot() {
        let manager = ClusterStateManager::new();
        let cluster_id = "cluster-a";
        let partition_key = "partition-a";

        manager.record_message(&build_message(cluster_id, partition_key, 1));
        manager.process_join_cluster(&JoinClusterRequest {
            partition_key: Some(RaftPartitionKey {
                value: partition_key.to_string(),
            }),
            leader_id: 1,
            member_id: 2,
            storage_name: "default".to_string(),
            relative_path: "group/project.git".to_string(),
            replicas: vec![
                ReplicaId {
                    partition_key: Some(RaftPartitionKey {
                        value: partition_key.to_string(),
                    }),
                    member_id: 1,
                    storage_name: "default".to_string(),
                    metadata: Some(replica_id::Metadata {
                        address: "127.0.0.1:2301".to_string(),
                    }),
                    r#type: 1,
                },
                ReplicaId {
                    partition_key: Some(RaftPartitionKey {
                        value: partition_key.to_string(),
                    }),
                    member_id: 2,
                    storage_name: "secondary".to_string(),
                    metadata: Some(replica_id::Metadata {
                        address: "127.0.0.1:2302".to_string(),
                    }),
                    r#type: 2,
                },
            ],
        });
        let snapshot = manager.record_snapshot(&build_message(cluster_id, partition_key, 1), 4096);
        assert_eq!(snapshot.snapshot_size, 4096);

        let partitions = manager.get_partitions(&GetPartitionsRequest {
            cluster_id: cluster_id.to_string(),
            partition_key: Some(RaftPartitionKey {
                value: partition_key.to_string(),
            }),
            relative_path: "group/project.git".to_string(),
            include_replica_details: true,
            include_relative_paths: true,
            ..GetPartitionsRequest::default()
        });
        assert_eq!(partitions.len(), 1);
        let partition = &partitions[0];
        assert_eq!(partition.cluster_id, cluster_id);
        assert_eq!(partition.leader_id, 1);
        assert_eq!(partition.term, 4);
        assert_eq!(partition.index, 12);
        assert_eq!(
            partition.relative_paths,
            vec!["group/project.git".to_string()]
        );
        assert_eq!(partition.replicas.len(), 2);

        let cluster = manager.get_cluster_info(cluster_id);
        let statistics = cluster.statistics.unwrap_or(ClusterStatistics {
            total_partitions: 0,
            healthy_partitions: 0,
            total_replicas: 0,
            healthy_replicas: 0,
            storage_stats: HashMap::new(),
        });
        assert_eq!(statistics.total_partitions, 1);
        assert_eq!(statistics.total_replicas, 2);
        assert_eq!(statistics.healthy_partitions, 1);
        assert_eq!(statistics.healthy_replicas, 2);
        assert_eq!(
            statistics.storage_stats.get("default"),
            Some(&cluster_statistics::StorageStats {
                leader_count: 1,
                replica_count: 1,
            })
        );
    }

    #[test]
    fn persists_state_and_recovers_on_restart() {
        let state_file = unique_state_file_path("restart-recovery");
        let _ = fs::remove_file(&state_file);

        let cluster_id = "cluster-persist";
        let partition_key = "partition-persist";

        let manager = ClusterStateManager::with_state_path(state_file.clone());
        manager.record_message(&build_message(cluster_id, partition_key, 1));
        manager.process_join_cluster(&JoinClusterRequest {
            partition_key: Some(RaftPartitionKey {
                value: partition_key.to_string(),
            }),
            leader_id: 1,
            member_id: 2,
            storage_name: "default".to_string(),
            relative_path: "group/persist.git".to_string(),
            replicas: vec![ReplicaId {
                partition_key: Some(RaftPartitionKey {
                    value: partition_key.to_string(),
                }),
                member_id: 1,
                storage_name: "default".to_string(),
                metadata: Some(replica_id::Metadata {
                    address: "127.0.0.1:2401".to_string(),
                }),
                r#type: 1,
            }],
        });
        let _ = manager.record_snapshot(&build_message(cluster_id, partition_key, 1), 2048);
        drop(manager);

        let restarted = ClusterStateManager::with_state_path(state_file.clone());
        let partitions = restarted.get_partitions(&GetPartitionsRequest {
            cluster_id: cluster_id.to_string(),
            partition_key: Some(RaftPartitionKey {
                value: partition_key.to_string(),
            }),
            relative_path: "group/persist.git".to_string(),
            include_replica_details: true,
            include_relative_paths: true,
            ..GetPartitionsRequest::default()
        });

        assert_eq!(partitions.len(), 1);
        let partition = &partitions[0];
        assert_eq!(partition.cluster_id, cluster_id);
        assert_eq!(partition.leader_id, 1);
        assert_eq!(partition.term, 4);
        assert_eq!(partition.index, 12);
        assert_eq!(
            partition.relative_paths,
            vec!["group/persist.git".to_string()]
        );
        assert_eq!(partition.replicas.len(), 2);

        let cluster = restarted.get_cluster_info(cluster_id);
        let statistics = cluster.statistics.expect("statistics should be present");
        assert_eq!(statistics.total_partitions, 1);
        assert_eq!(statistics.total_replicas, 2);
        assert_eq!(statistics.healthy_partitions, 1);

        let _ = fs::remove_file(state_file);
    }
}
