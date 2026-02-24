# Gitaly Client Reference Documentation

Comprehensive reference for building a Rust gRPC client for Gitaly, GitLab's Git RPC service.

---

## 1. What is Gitaly

Gitaly is GitLab's dedicated Git RPC service that provides high-level remote procedure call (RPC) access to Git repositories. It is present in **every** GitLab installation and coordinates Git repository storage and retrieval.

### Purpose

- **Single point of access**: All Git repository operations in GitLab go through Gitaly. There is no direct disk access to Git repositories from other components.
- **Abstraction layer**: Gitaly wraps Git operations behind gRPC RPCs, enabling network-transparent repository access.
- **Performance**: Uses `git cat-file --batch` process pools, caching, and connection multiplexing for high throughput.

### Architecture

```
  GitLab Rails ──┐
  GitLab Shell ──┤
  GitLab Workhorse──┤── gRPC ──► Gitaly Server ──► Local Filesystem
  Zoekt Indexer ──┤                                  (Git repositories)
  ES Indexer ─────┤
  KAS ────────────┘
```

**Key architectural facts:**
- Gitaly implements a **client-server architecture** over gRPC (HTTP/2).
- A single gRPC connection can multiplex many concurrent RPCs.
- Gitaly manages **only Git repository access**; other GitLab data uses different paths.
- Only local (SSD-backed) storage is supported; NFS/cloud filesystems are not.
- One Gitaly server application implements **all** services in the protocol.

### Components that act as Gitaly clients

| Component | Role |
|---|---|
| GitLab Rails | Primary application server; most repository operations |
| GitLab Shell | SSH Git operations |
| GitLab Workhorse | HTTP Git operations, file uploads/downloads |
| Zoekt Indexer | Code search indexing |
| Elasticsearch Indexer | Legacy code search indexing |
| KAS (Kubernetes Agent) | Agent-based Git operations |

---

## 2. gRPC API: Protobuf Service Definitions

Gitaly's protocol is defined in `.proto` files in the `proto/` directory of the Gitaly repository. The protocol documentation is published at https://gitlab-org.gitlab.io/gitaly/.

### Proto file layout

| File | Service | Purpose |
|---|---|---|
| `blob.proto` | BlobService | Retrieve blob objects |
| `commit.proto` | CommitService | Commit queries, tree entries, blame |
| `ref.proto` | RefService | Branch/tag/ref operations |
| `repository.proto` | RepositoryService | Repository-level operations |
| `diff.proto` | DiffService | Diffs between commits |
| `operations.proto` | OperationService | Mutating operations (merge, rebase, cherry-pick) |
| `server.proto` | ServerService | Server health, info, disk stats |
| `smarthttp.proto` | SmartHTTPService | HTTP Git protocol |
| `ssh.proto` | SSHService | SSH Git protocol |
| `hook.proto` | HookService | Git hook execution |
| `remote.proto` | RemoteService | Remote repository operations |
| `conflicts.proto` | ConflictsService | Merge conflict resolution |
| `cleanup.proto` | CleanupService | Repository cleanup/rewriting |
| `objectpool.proto` | ObjectPoolService | Object pool management |
| `analysis.proto` | AnalysisService | Repository analysis |
| `partition.proto` | PartitionService | Partition management |
| `praefect.proto` | PraefectInfoService | Praefect cluster info |
| `raft.proto` | RaftService | Raft consensus (newer) |
| `transaction.proto` | RefTransaction | Reference transaction voting |
| `shared.proto` | (messages only) | Shared types: Repository, GitCommit, User, etc. |
| `lint.proto` | (options only) | Custom protobuf option definitions |
| `errors.proto` | (messages only) | Structured error types |

### Custom protobuf annotations

Gitaly defines custom protobuf options in `lint.proto`:

```protobuf
// Service-level: marks service as intercepted (handled specially, not routed)
extend google.protobuf.ServiceOptions {
  bool intercepted = 82302;
}

// Method-level: categorizes RPC as accessor/mutator/maintenance
extend google.protobuf.MethodOptions {
  OperationMsg op_type = 82303;
}

// Field-level: marks fields for routing
extend google.protobuf.FieldOptions {
  bool storage = 91233;
  bool repository = 91234;
  bool target_repository = 91235;
  bool additional_repository = 91236;
  bool partition_id = 91237;
}

message OperationMsg {
  enum Operation {
    UNKNOWN = 0;
    MUTATOR = 1;    // Modifies repository state
    ACCESSOR = 2;   // Read-only
    MAINTENANCE = 3; // Housekeeping operations
  }
  enum Scope {
    REPOSITORY = 0;  // Operates on a single repository
    STORAGE = 2;     // Operates on a storage
    PARTITION = 3;   // Operates on a partition
  }
  Operation op = 1;
  Scope scope_level = 2;
}
```

These annotations are critical for:
- **Praefect routing**: MUTATOR RPCs are broadcast to all replicas; ACCESSOR RPCs can be load-balanced.
- **Request routing**: `target_repository` field tells Praefect which repository is being accessed.
- **Intercepted services**: ServerService is intercepted (handled by Praefect itself, not forwarded).

---

## 3. Authentication

Gitaly uses **shared-secret HMAC authentication** transmitted as gRPC metadata.

### V2 Token Format (current, used by official Gitaly)

The bearer token format is:
```
v2.<hex_encoded_hmac_sha256>.<unix_timestamp>
```

**Construction:**
1. Get current Unix timestamp as a string (e.g., `"1708700000"`)
2. Compute HMAC-SHA256 of the timestamp string using the shared secret as key
3. Hex-encode the HMAC result
4. Concatenate: `v2.` + hex_hmac + `.` + timestamp

**Validation (server-side):**
1. Split token on `.` into 3 parts: version, signature, message
2. Verify version is `v2`
3. Decode hex signature
4. Recompute HMAC-SHA256(secret, timestamp_string) and compare (constant-time)
5. Parse timestamp and verify it is within +/- 30 seconds of current time

### How tokens are transmitted

Tokens are sent as gRPC metadata:
```
authorization: Bearer v2.<hmac>.<timestamp>
```

The Ruby client constructs this as:
```ruby
hmac = OpenSSL::HMAC.hexdigest(OpenSSL::Digest.new('SHA256'), token, issued_at)
"v2.#{hmac}.#{issued_at}"
```

### Go client usage

```go
import gitalyauth "gitlab.com/gitlab-org/gitaly/v16/auth"

creds := gitalyauth.RPCCredentialsV2("your-shared-secret")
conn, err := grpc.Dial(
    "gitaly.example.com:9999",
    grpc.WithPerRPCCredentials(creds),
)
```

### Configuration

In Gitaly's `config.toml`:
```toml
[auth]
token = 'abc123secret'
transitioning = false  # Set true to allow unauthenticated during rollout
```

In GitLab's `gitlab.yml`, each storage maps to an address + token:
```yaml
repositories:
  storages:
    default:
      gitaly_address: tcp://gitaly.internal:8075
      gitaly_token: abc123secret
```

### TLS

Gitaly supports TLS on a separate listener:
```toml
tls_listen_addr = "localhost:8888"

[tls]
certificate_path = '/path/to/cert.pem'
key_path = '/path/to/key.pem'
```

Clients use the `tls://` scheme to connect over TLS.

---

## 4. Connection Setup

### Address Schemes

| Scheme | Description |
|---|---|
| `unix:<path>` | Unix domain socket |
| `tcp://<host:port>` | Insecure TCP |
| `tls://<host:port>` | TCP with TLS |
| `dns://<host:port>` | DNS resolution (insecure) |
| `dns+tls://<host:port>` | DNS resolution with TLS |

### Connection configuration

The official Go client configures connections with:
- **Keepalive**: 20-second keepalive intervals (`grpc.keepalive_time_ms: 20000`)
- **Tracing interceptors**: Correlation ID propagation
- **Round-robin load balancing**: `loadBalancingConfig: [{ round_robin: {} }]`
- **Connection pooling**: `Pool` type manages connections keyed by (address, token)

### Metadata headers sent by clients

Every RPC call includes these gRPC metadata headers:

| Header | Description | Example |
|---|---|---|
| `authorization` | Bearer token for auth | `Bearer v2.<hmac>.<ts>` |
| `client_name` | Client identifier | `gitlab-web`, `gitlab-sidekiq` |
| `x-gitlab-correlation-id` | Request tracing ID | UUID string |
| `gitaly-session-id` | Session tracking | UUID string |
| `gitaly-servers` | Remote storage info (base64 JSON) | For inter-Gitaly calls |
| `username` | Current user name | From application context |
| `user_id` | Current user ID | From application context |
| `remote_ip` | Client IP address | From application context |
| `gitaly-route-repository-accessor-policy` | Routing hint | `primary-only` |
| `relative-path-bin` | Quarantine path | For quarantine repos |
| Feature flag headers | `gitaly-feature-*` | `gitaly-feature-some-flag: true` |

### Ruby client call flow

```ruby
# 1. Build metadata
kwargs = request_kwargs(storage, timeout: timeout)
# 2. Get or create gRPC stub for the service
stub = stub(service, storage)
# 3. Invoke RPC
stub.send(rpc_method, request, kwargs)
```

### Recommended Rust client setup (tonic)

```rust
use tonic::transport::Channel;
use tonic::metadata::MetadataValue;

// Create channel
let channel = Channel::from_static("http://127.0.0.1:2305")
    .connect()
    .await?;

// Create service client
let mut client = RepositoryServiceClient::with_interceptor(channel, |mut req: Request<()>| {
    let token = generate_v2_token(&secret);
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {}", token)).unwrap(),
    );
    Ok(req)
});
```

---

## 5. Key RPC Calls by Service

### ServerService (intercepted -- handled by Praefect)

| RPC | Type | Description |
|---|---|---|
| `ServerInfo` | Unary | Server version, Git version, storage statuses |
| `DiskStatistics` | Unary | Disk usage per storage |
| `ReadinessCheck` | Unary | Server readiness probe |
| `ServerSignature` | Unary | Server's public key |

### RepositoryService

| RPC | Type | Description |
|---|---|---|
| `RepositoryExists` | Unary | Check if repository exists |
| `CreateRepository` | Unary | Create empty repository |
| `RemoveRepository` | Unary | Delete repository |
| `RepositorySize` | Unary | Get repository size in KB |
| `RepositoryInfo` | Unary | Detailed repository info |
| `FindMergeBase` | Unary | Find merge base of revisions |
| `FindLicense` | Unary | Detect repository license |
| `CreateFork` | Unary | Fork from source repository |
| `FetchRemote` | Unary | Fetch from remote |
| `GetArchive` | Server stream | Get tar/zip archive |
| `GetSnapshot` | Server stream | Get repository snapshot |
| `CreateBundle` | Server stream | Create Git bundle |
| `OptimizeRepository` | Unary | Run gc/repack/prune |
| `WriteRef` | Unary | Write a reference |
| `ReplicateRepository` | Unary | Replicate from another Gitaly |
| `SearchFilesByContent` | Server stream | Grep repository content |
| `SearchFilesByName` | Server stream | Search files by name pattern |
| `BackupRepository` | Server stream | Backup entire repository |
| `RestoreRepository` | Client stream | Restore from backup |
| `Fsck` | Server stream | Check repository integrity |

### CommitService

| RPC | Type | Description |
|---|---|---|
| `FindCommit` | Unary | Find single commit by revision |
| `ListCommits` | Server stream | List commits with filters (pagination, author, dates, paths) |
| `CommitIsAncestor` | Unary | Check ancestry relationship |
| `CommitStats` | Unary | Additions/deletions/changed files |
| `CountCommits` | Unary | Count matching commits |
| `CountDivergingCommits` | Unary | Count commits diverging between refs |
| `LastCommitForPath` | Unary | Last commit touching a path |
| `GetTreeEntries` | Server stream | List directory contents at revision |
| `TreeEntry` | Server stream | Get single tree entry with data |
| `ListFiles` | Server stream | List all files at revision |
| `FindAllCommits` | Server stream | List all commits |
| `FindCommits` | Server stream | Find commits with criteria |
| `CommitsByMessage` | Server stream | Search by commit message |
| `RawBlame` | Server stream | Git blame output |
| `GetCommitSignatures` | Server stream | GPG/SSH signatures |
| `GetCommitMessages` | Server stream | Full commit messages |
| `ListLastCommitsForTree` | Server stream | Last commit for each tree entry |
| `CheckObjectsExist` | Unary | Check existence of objects/refs |

### RefService

| RPC | Type | Description |
|---|---|---|
| `FindDefaultBranchName` | Unary | Get HEAD target branch |
| `FindBranch` | Unary | Find specific branch |
| `RefExists` | Unary | Check if ref exists |
| `FindAllBranches` | Server stream | List all branches with commits |
| `FindLocalBranches` | Server stream | List local branches |
| `FindAllTags` | Server stream | List all tags |
| `FindTag` | Unary | Find specific tag |
| `ListRefs` | Server stream | List refs with sorting/filtering |
| `DeleteRefs` | Unary | Delete references |
| `UpdateReferences` | Client stream | Batch update references |
| `FindRefsByOID` | Server stream | Find refs pointing at object |
| `ListBranchNamesContainingCommit` | Server stream | Branches containing commit |
| `ListTagNamesContainingCommit` | Server stream | Tags containing commit |
| `GetTagSignatures` | Server stream | Tag signature data |
| `GetTagMessages` | Server stream | Tag annotation messages |

### BlobService

| RPC | Type | Description |
|---|---|---|
| `GetBlob` | Server stream | Get blob by OID |
| `GetBlobs` | Server stream | Get blobs by revision+path pairs |
| `ListBlobs` | Server stream | List blobs reachable from revisions |
| `ListAllBlobs` | Server stream | List all blobs in repository |
| `GetLFSPointers` | Server stream | Get LFS pointers by blob IDs |
| `ListLFSPointers` | Server stream | List LFS pointers from revisions |
| `ListAllLFSPointers` | Server stream | List all LFS pointers |

### DiffService

| RPC | Type | Description |
|---|---|---|
| `CommitDiff` | Server stream | Full diff output between commits |
| `CommitDelta` | Server stream | Change metadata (no content) |
| `DiffStats` | Server stream | Per-file statistics |
| `DiffBlobs` | Server stream | Diff two specific blobs |
| `FindChangedPaths` | Bidi stream | Find paths changed between revisions |
| `GetPatchID` | Unary | Compute patch ID |
| `RawDiff` | Server stream | Raw unified diff |
| `RawPatch` | Server stream | Raw patch format |
| `RangeDiff` | Server stream | Range diff between revision ranges |

### OperationService

| RPC | Type | Description |
|---|---|---|
| `UserCreateBranch` | Unary | Create branch (with access checks) |
| `UserDeleteBranch` | Unary | Delete branch |
| `UserCreateTag` | Unary | Create tag |
| `UserDeleteTag` | Unary | Delete tag |
| `UserMergeBranch` | Bidi stream | Two-phase merge (propose + confirm) |
| `UserMergeToRef` | Unary | Merge to a specific ref |
| `UserFFBranch` | Unary | Fast-forward branch |
| `UserCherryPick` | Unary | Cherry-pick commit |
| `UserRevert` | Unary | Revert commit |
| `UserSquash` | Unary | Squash commit range |
| `UserRebaseConfirmable` | Bidi stream | Two-phase rebase |
| `UserCommitFiles` | Bidi stream | Commit file changes |
| `UserApplyPatch` | Bidi stream | Apply patches |
| `UserUpdateBranch` | Unary | Update branch target |
| `UserUpdateSubmodule` | Unary | Update submodule pointer |

### SmartHTTPService

| RPC | Type | Description |
|---|---|---|
| `InfoRefs` | Server stream | Git info/refs advertisement |
| `PostReceivePack` | Bidi stream | Git push over HTTP |
| `PostUploadPackWithSidechannel` | Unary | Git fetch over HTTP (uses sidechannel) |

### SSHService

| RPC | Type | Description |
|---|---|---|
| `SSHReceivePack` | Bidi stream | Git push over SSH |
| `SSHUploadArchive` | Bidi stream | Git archive over SSH |
| `SSHUploadPackWithSidechannel` | Unary | Git fetch over SSH (uses sidechannel) |

---

## 6. Streaming RPCs

Gitaly makes extensive use of gRPC streaming for large data transfers. Understanding the streaming patterns is essential for a correct client implementation.

### Streaming Types

| Pattern | Client | Server | Use case |
|---|---|---|---|
| Unary | 1 message | 1 message | Simple queries: `FindCommit`, `RepositoryExists` |
| Server streaming | 1 message | N messages | Large data reads: `GetBlob`, `ListCommits`, `CommitDiff` |
| Client streaming | N messages | 1 message | Uploads: `RestoreRepository`, `SetCustomHooks`, `UpdateReferences` |
| Bidirectional | N messages | N messages | Interactive: `UserMergeBranch`, hooks, SSH/HTTP protocol |

### Server Streaming: The Header+Data Pattern

For large binary data (blobs, diffs, archives), Gitaly uses a **header-then-data** pattern:

```protobuf
// First message in stream contains metadata
message GetBlobResponse {
  int64 size = 1;    // Only set in first message
  bytes data = 2;    // Data chunks in all messages
  string oid = 3;    // Only set in first message
}
```

**Client handling:**
1. First response message: read metadata fields (`size`, `oid`, etc.) + optional first data chunk
2. Subsequent messages: concatenate `data` fields
3. Stream ends when server closes

Some newer RPCs use explicit `oneof` for clarity:
```protobuf
message FooResponse {
  message Header { /* metadata */ }
  oneof payload {
    Header header = 1;
    bytes data = 2;
  }
}
```

### Server Streaming: Batched Objects

For listing operations, responses batch multiple objects per message:

```protobuf
message ListCommitsResponse {
  repeated GitCommit commits = 1;        // Multiple commits per message
  PaginationCursor pagination_cursor = 2;
}

message ListBlobsResponse {
  message Blob {
    string oid = 1;
    int64 size = 2;
    bytes data = 3;
    bytes path = 4;
  }
  repeated Blob blobs = 1;  // Multiple blobs per message
}
```

### Client Streaming: Chunked Uploads

For uploading data, the first message typically contains metadata, and subsequent messages contain data chunks:

```protobuf
// RestoreCustomHooksRequest: first message has repository, rest have data
message SetCustomHooksRequest {
  Repository repository = 1;
  bytes data = 2;
}
```

### Bidirectional Streaming: Interactive Protocols

**Two-phase operations** (merge, rebase):
```
Client                    Server
  |--- Request{header} --->|   (propose operation)
  |<-- Response{} ---------|   (operation result preview)
  |--- Request{confirm} -->|   (confirm or abort)
  |<-- Response{result} ---|   (final result)
```

**SSH/HTTP protocol proxying**:
```
Client                    Server
  |--- Request{stdin} ---->|   (git protocol data)
  |<-- Response{stdout} ---|   (git protocol response)
  |--- Request{stdin} ---->|   (more data)
  |<-- Response{stderr} ---|   (progress/errors)
  ...
```

### Chunk Sizes

- The Gitaly `streamio` package uses a configurable write buffer size (default appears to be 128KB).
- Recommended chunk sizes for streaming are in the range of **16-128 KiB**.
- The `GITALY_STREAMIO_WRITE_BUFFER_SIZE` environment variable can override this.

### Sidechannel

For high-throughput operations (git fetch), Gitaly uses a **sidechannel** mechanism:
- A separate data channel is established alongside the gRPC connection
- The sidechannel carries raw Git pack data, avoiding gRPC message framing overhead
- The gRPC RPC itself becomes unary (just metadata), while the bulk data goes through the sidechannel
- This requires special connection setup using `DialSidechannel`

**For a Rust client not implementing sidechannel**: Use the older streaming RPCs (e.g., `PostReceivePack` for push, non-sidechannel variants where available).

---

## 7. Repository Addressing

Every repository-scoped RPC includes a `Repository` message that identifies the target repository.

### Repository Message

```protobuf
message Repository {
  string storage_name = 2;         // Gitaly storage name (e.g., "default")
  string relative_path = 3;        // Path relative to storage root
  string git_object_directory = 4;  // For quarantine environments
  repeated string git_alternate_object_directories = 5;  // Additional object dirs
  string gl_repository = 6;        // GitLab identifier (e.g., "project-123")
  string gl_project_path = 8;      // Human-readable path (e.g., "gitlab-org/gitlab")
}
```

### Key fields

**`storage_name`** (required): Identifies which physical or virtual storage contains the repository. Maps to a `[[storage]]` entry in Gitaly's config.toml. Examples: `"default"`, `"gitaly-1"`.

**`relative_path`** (required): The path to the repository relative to the storage root. In GitLab, this typically uses hashed storage:
```
@hashed/b1/7e/b17ef6d19c7a5b1ee83b907c595526dcb1eb06db8227d650d5dda0a9f4ce8cd9.git
```
Or for legacy layout: `namespace/project.git`

**`gl_repository`**: GitLab's internal identifier (e.g., `"project-123"`). Used in callbacks to GitLab's internal API. May be empty for operations that don't trigger callbacks.

**`gl_project_path`**: Human-readable project path. Used only for logging/debugging.

**`git_object_directory` / `git_alternate_object_directories`**: Used for quarantine environments during Git pushes. These override where Git looks for objects.

### How repository fields are used in routing

Every RPC method with `REPOSITORY` scope must have a field annotated with `[(target_repository) = true]`. This tells Praefect which repository the RPC targets, enabling:
- Routing to the correct Gitaly node
- Replication of writes to other replicas
- Read distribution across replicas

Example from protobuf:
```protobuf
message FindCommitRequest {
  Repository repository = 1 [(target_repository) = true];
  bytes revision = 2;
}
```

Some RPCs have an `additional_repository` for cross-repository operations (e.g., `CreateFork`, `FetchSourceBranch`).

---

## 8. Praefect (Gitaly Cluster)

Praefect is an optional **reverse proxy and transaction manager** for Gitaly that provides high availability.

### Architecture

```
  Clients ──► Praefect ──┬──► Gitaly-1 (primary)
                         ├──► Gitaly-2 (replica)
                         └──► Gitaly-3 (replica)
                         │
                         └──► PostgreSQL (metadata)
```

### How Praefect affects client usage

**Transparent proxying**: Clients connect to Praefect using the same Gitaly gRPC protocol. From the client's perspective, Praefect looks like a Gitaly server.

**Virtual storage**: Praefect presents a **virtual storage name** (e.g., `"default"`) that maps to multiple physical Gitaly nodes. Clients use the virtual storage name in the `Repository.storage_name` field.

**Two-token system**:
- `PRAEFECT_EXTERNAL_TOKEN`: Used by clients connecting to Praefect
- `PRAEFECT_INTERNAL_TOKEN`: Used for Praefect-to-Gitaly internal communication
- Clients must **never** connect directly to internal Gitaly nodes

**Request routing**:
- **Read (ACCESSOR) RPCs**: Distributed across replicas for load balancing
- **Write (MUTATOR) RPCs**: Routed to primary, then replicated to other nodes
- **Intercepted services** (ServerService): Handled by Praefect itself, not forwarded

**Consistency guarantees**:
- Strong consistency for writes (transaction voting)
- Eventual consistency for reads (may read from slightly stale replicas)
- Clients can force primary reads with the `gitaly-route-repository-accessor-policy: primary-only` metadata header

### Impact on client implementation

For a Rust client:
- No code changes needed to work with Praefect; it's protocol-compatible
- Use the Praefect address and external token in connection setup
- Be aware that some operations may be slower due to replication overhead
- The `target_repository` annotation in requests is critical for correct routing

---

## 9. Error Handling

### gRPC Status Codes

Gitaly uses standard gRPC status codes:

| Code | Usage in Gitaly |
|---|---|
| `OK` | Operation succeeded |
| `NOT_FOUND` | Repository or ref doesn't exist |
| `ALREADY_EXISTS` | Resource already exists |
| `INVALID_ARGUMENT` | Bad request parameters |
| `FAILED_PRECONDITION` | State doesn't allow operation |
| `PERMISSION_DENIED` | Authentication failed |
| `UNAUTHENTICATED` | No/invalid auth token |
| `UNAVAILABLE` | Server temporarily unavailable |
| `ABORTED` | Operation aborted (e.g., conflict) |
| `INTERNAL` | Server bug |
| `RESOURCE_EXHAUSTED` | Rate/concurrency limit hit |
| `DEADLINE_EXCEEDED` | Request timed out |
| `CANCELLED` | Client cancelled |

### Structured Error Details

Gitaly attaches structured error details to gRPC status responses using the standard `google.rpc.Status` / `google.rpc.ErrorDetail` pattern. These are protobuf-encoded in the status `details` field.

**Defined error types** (from `errors.proto`):

| Error Type | Description | Key Fields |
|---|---|---|
| `AccessCheckError` | GitLab access check failed | `error_message`, `protocol`, `user_id`, `changes` |
| `CustomHookError` | Custom hook returned non-zero | `stdout`, `stderr`, `hook_type` (PRE_RECEIVE/UPDATE/POST_RECEIVE) |
| `MergeConflictError` | Merge conflict | `conflicting_files`, `conflicting_commit_ids` |
| `ReferenceExistsError` | Ref exists when it shouldn't | `reference_name`, `oid` |
| `ReferenceNotFoundError` | Ref doesn't exist when it should | `reference_name` |
| `ReferenceStateMismatchError` | Ref points to unexpected object | `reference_name`, `expected_object_id`, `actual_object_id` |
| `ReferencesLockedError` | Refs couldn't be locked | `refs` |
| `ReferenceUpdateError` | Ref update failed | `reference_name`, `old_oid`, `new_oid` |
| `NotAncestorError` | Commit isn't ancestor of another | `parent_revision`, `child_revision` |
| `ChangesAlreadyAppliedError` | No-op (already applied) | (no fields) |
| `LimitError` | Rate/concurrency limit | `error_message`, `retry_after` |
| `IndexError` | Index operation conflict | `path`, `error_type` (EMPTY_PATH/INVALID_PATH/FILE_EXISTS/...) |
| `PathError` | Invalid path | `path`, `error_type` (EMPTY_PATH/RELATIVE_ESCAPE/ABSOLUTE/LONG/...) |
| `PathNotFoundError` | Path doesn't exist | `path` |
| `InvalidRefFormatError` | Bad ref format | `refs` |
| `ResolveRevisionError` | Can't resolve revision | `revision` |
| `BadObjectError` | Invalid object ID | `bad_oid` |
| `AmbiguousReferenceError` | Ambiguous ref name | `reference` |
| `RemoteNotFoundError` | Remote URL not found | (no fields) |

**RPC-specific error wrappers** use `oneof` to group relevant errors:
```protobuf
message UpdateReferencesError {
  oneof error {
    InvalidRefFormatError invalid_format = 1;
    ReferencesLockedError references_locked = 2;
    ReferenceStateMismatchError reference_state_mismatch = 3;
  }
}
```

### Handling errors in Rust (tonic)

```rust
use tonic::Status;

match client.find_commit(request).await {
    Ok(response) => { /* handle response */ }
    Err(status) => {
        match status.code() {
            tonic::Code::NotFound => { /* repository doesn't exist */ }
            tonic::Code::Unauthenticated => { /* bad token */ }
            _ => {
                // Check for structured error details
                // tonic doesn't decode details automatically;
                // you need to decode from status.details() bytes
                // using prost to decode google.rpc.Status
            }
        }
    }
}
```

---

## 10. Client Libraries

### Official Libraries

**Go client** (`gitlab.com/gitlab-org/gitaly/client`):
- Full-featured with connection pooling (`Pool`), health checks, sidechannel support
- `Dial` / `DialContext` / `DialSidechannel` for connection establishment
- Built-in SSH operation helpers: `UploadPack`, `ReceivePack`, `UploadArchive`
- Automatic keepalive, tracing, correlation ID propagation

**Ruby client** (`gitaly` gem):
- Auto-generated gRPC stubs from protobuf
- `Gitlab::GitalyClient` wrapper in GitLab Rails
- Connection management, metadata injection, timeout handling
- N+1 detection for preventing chatty RPC patterns

### Building a Rust Client

**Dependencies needed:**
- `tonic` - gRPC framework
- `prost` - Protobuf codec
- `tonic-build` - Proto compilation (build time)
- `hmac`, `sha2` - For V2 auth token generation
- `tokio` - Async runtime

**Key implementation tasks:**
1. Compile `.proto` files with `tonic-build`
2. Implement V2 auth token as a `tonic::service::Interceptor`
3. Handle streaming responses (concatenate chunked data)
4. Parse structured error details from `Status::details()`
5. Implement connection pooling if needed
6. Add metadata headers (client_name, correlation_id, etc.)

---

## 11. Configuration Reference

### Gitaly server configuration (`config.toml`)

```toml
# Socket and network listeners
socket_path = "/tmp/gitaly.socket"         # Unix socket (default method)
listen_addr = "127.0.0.1:8075"            # TCP listener
tls_listen_addr = "127.0.0.1:8076"        # TLS listener
internal_socket_dir = "/tmp/gitaly-internal"

# Authentication
[auth]
token = 'shared-secret-token'
transitioning = false   # Allow unauthenticated during rollout

# TLS
[tls]
certificate_path = '/etc/gitaly/cert.pem'
key_path = '/etc/gitaly/key.pem'

# Storage definitions (one or more)
[[storage]]
name = "default"
path = "/var/opt/gitlab/git-data/repositories"

[[storage]]
name = "secondary"
path = "/mnt/secondary/repositories"

# Git configuration
[git]
bin_path = "/usr/bin/git"
catfile_cache_size = 100

# Per-RPC concurrency limits
[[concurrency]]
rpc = "/gitaly.RepositoryService/GarbageCollect"
max_per_repo = 1
max_queue_wait = "1m"
max_queue_size = 10

# Rate limiting
[[rate_limiting]]
rpc = "/gitaly.SmartHTTPService/PostUploadPackWithSidechannel"
interval = "1m"
burst = 5
```

### GitLab configuration (`gitlab.yml`)

```yaml
repositories:
  storages:
    default:
      gitaly_address: tcp://gitaly-1.internal:8075
      gitaly_token: secret-token-1
    secondary:
      gitaly_address: tcp://gitaly-2.internal:8075
      gitaly_token: secret-token-2
```

This is the **single source of truth** for the Gitaly network topology from the client's perspective.

### Default ports

| Service | Default Port |
|---|---|
| Gitaly (TCP) | 8075 |
| Gitaly (TLS) | 8076 |
| Praefect (TCP) | 2305 |
| Prometheus metrics | 9236 |

---

## Appendix A: Key Shared Message Types

### GitCommit
```protobuf
message GitCommit {
  string id = 1;                    // SHA1/SHA256 hex
  bytes subject = 2;                // First line of message
  bytes body = 3;                   // Full message (may be truncated)
  CommitAuthor author = 4;
  CommitAuthor committer = 5;
  repeated string parent_ids = 6;
  int64 body_size = 7;             // Full body size (body may be truncated)
  SignatureType signature_type = 8;
  string tree_id = 9;
  repeated CommitTrailer trailers = 10;
  CommitStatInfo short_stats = 11;
  repeated bytes referenced_by = 12;
  string encoding = 13;            // Encoding from commit header
}
```

### CommitAuthor
```protobuf
message CommitAuthor {
  bytes name = 1;       // Raw bytes (not necessarily UTF-8)
  bytes email = 2;      // Raw bytes
  google.protobuf.Timestamp date = 3;
  bytes timezone = 4;   // E.g., "+0200"
}
```

### User (for operation RPCs)
```protobuf
message User {
  string gl_id = 1;        // GitLab user ID (e.g., "user-123")
  bytes name = 2;
  bytes email = 3;
  string gl_username = 4;  // GitLab username
  string timezone = 5;
}
```

### PaginationParameter / PaginationCursor
```protobuf
message PaginationParameter {
  int32 limit = 1;       // Max items to return
  string page_token = 2; // Cursor from previous response
}

message PaginationCursor {
  string next_cursor = 1;  // Use as page_token for next request
}
```

### Branch / Tag
```protobuf
message Branch {
  bytes name = 1;
  GitCommit target_commit = 2;
}

message Tag {
  bytes name = 1;
  string id = 2;
  GitCommit target_commit = 3;
  bytes message = 4;
  int64 message_size = 5;
  CommitAuthor tagger = 6;
  SignatureType signature_type = 7;
}
```

### ObjectPool
```protobuf
message ObjectPool {
  Repository repository = 1 [(repository) = true];
}
```

---

## Appendix B: Encoding Considerations

Git does not enforce encodings on most data. As a result:

- **`bytes` fields** are used for data that may not be valid UTF-8: commit subjects, file paths, branch names, blob data, etc.
- **`string` fields** are used for data known to be valid UTF-8: OIDs (hex), storage names, protocol-level identifiers.
- A Rust client should use `Vec<u8>` (mapped from `bytes`) for Git content, not assume `String`/UTF-8.
- Commit messages may have an `encoding` field indicating the character encoding used.

---

## Sources

- [Gitaly Protocol Documentation](https://gitlab-org.gitlab.io/gitaly/)
- [Gitaly Architecture Overview (GitLab Docs)](https://docs.gitlab.com/administration/gitaly/)
- [Configure Gitaly (GitLab Docs)](https://docs.gitlab.com/administration/gitaly/configure_gitaly/)
- [Gitaly Auth Package (Go)](https://pkg.go.dev/gitlab.com/gitlab-org/gitaly/v16/auth)
- [Gitaly Client Package (Go)](https://pkg.go.dev/gitlab.com/gitlab-org/gitaly/v14/client)
- [Gitaly Protobuf Conventions](https://gitlab.com/gitlab-org/gitaly/blob/master/doc/protobuf.md)
- [GitLab Rails Gitaly Client](https://github.com/gitlabhq/gitlabhq/blob/master/lib/gitlab/gitaly_client.rb)
- [Gitaly config.toml Example](https://github.com/tnir/gitaly/blob/master/config.toml.example)
- [Gitaly Proto: shared.proto](https://gitlab.com/gitlab-org/gitaly/blob/master/proto/shared.proto)
- [Gitaly Proto: lint.proto (custom options)](https://gitlab.com/gitlab-org/gitaly/blob/master/proto/lint.proto)
- [Gitaly Proto: errors.proto](https://gitlab.com/gitlab-org/gitaly/blob/master/proto/errors.proto)
- [Gitaly Proto: ref.proto](https://gitlab.com/gitlab-org/gitaly/blob/master/proto/ref.proto)
- [Gitaly Sidechannel/Backchannel MR](https://gitlab.com/gitlab-org/gitaly/-/merge_requests/3768)
- [Gitaly Streaming (streamio package)](https://pkg.go.dev/github.com/stan/gitaly/streamio)
- [Gitaly Ruby Gem](https://rubygems.org/gems/gitaly)
- [Gitaly Development Guidelines](https://docs.gitlab.com/18.6/development/gitaly/)
- [Repository Storage Paths (GitLab Docs)](https://docs.gitlab.com/administration/repository_storage_paths/)
- [Praefect Configuration](https://docs.gitlab.com/administration/gitaly/praefect/configure/)
- [Troubleshooting Gitaly](https://docs.gitlab.com/administration/gitaly/troubleshooting/)
