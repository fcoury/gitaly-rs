use std::env;
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const PROTO_ROOT_ENV: &str = "GITALY_PROTO_ROOT";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Operation {
    Unknown,
    Accessor,
    Mutator,
    Maintenance,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Scope {
    Unknown,
    Repository,
    Storage,
    Partition,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RegistryEntry {
    full_method: String,
    service: String,
    method: String,
    operation: Operation,
    scope: Scope,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(gitaly_proto_codegen)");
    println!("cargo:rerun-if-env-changed={PROTO_ROOT_ENV}");

    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let workspace_proto_root = crate_dir.join("../..").join("proto");
    let proto_root = resolve_proto_root(&crate_dir, &workspace_proto_root)?;
    let local_proto_root = workspace_proto_root
        .exists()
        .then(|| canonical_or_self(&workspace_proto_root))
        .transpose()?;
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    println!("cargo:rerun-if-changed={}", proto_root.display());
    if let Some(local_proto_root) = &local_proto_root {
        println!("cargo:rerun-if-changed={}", local_proto_root.display());
    }

    let mut protos = std::fs::read_dir(&proto_root)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension() == Some(OsStr::new("proto")))
        .collect::<Vec<_>>();
    protos.sort();

    let mut include_paths = Vec::new();
    push_unique_path(&mut include_paths, proto_root.clone());
    if let Some(local_proto_root) = &local_proto_root {
        push_unique_path(&mut include_paths, local_proto_root.clone());
    }

    let has_raft_proto = include_paths
        .iter()
        .any(|include_path| include_path.join("raftpb/raft.proto").exists());

    if !has_raft_proto {
        protos.retain(|path| path.file_name() != Some(OsStr::new("cluster.proto")));
        println!(
            "cargo:warning=Skipping `cluster.proto`: missing dependency `proto/raftpb/raft.proto`"
        );
    }

    if protos.is_empty() {
        println!(
            "cargo:warning=No proto files found in {}",
            proto_root.display()
        );
        generate_registry_source(&out_dir, &[], &[])?;
        return Ok(());
    }

    let (registry_entries, intercepted_methods) = build_registry(&protos)?;
    generate_registry_source(&out_dir, &registry_entries, &intercepted_methods)?;

    match tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &include_paths)
    {
        Ok(()) => {
            println!("cargo:rustc-cfg=gitaly_proto_codegen");
            Ok(())
        }
        Err(err) => {
            let strict = env::var_os("GITALY_PROTO_STRICT_BUILD").is_some();
            let message = format!(
                "Failed to compile protobufs from {}: {err}",
                proto_root.display()
            );

            if strict {
                Err(message.into())
            } else {
                println!("cargo:warning={message}");
                println!("cargo:warning=Continuing without generated proto module");
                Ok(())
            }
        }
    }
}

fn resolve_proto_root(
    crate_dir: &Path,
    workspace_proto_root: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut candidates = Vec::new();
    if let Some(configured_root) = env::var_os(PROTO_ROOT_ENV) {
        candidates.push(PathBuf::from(configured_root));
    }
    candidates.push(workspace_proto_root.to_path_buf());
    candidates.push(crate_dir.join("../../..").join("proto"));

    for candidate in &candidates {
        if candidate.is_dir() && has_top_level_proto_files(candidate) {
            return canonical_or_self(candidate);
        }
    }

    let searched = candidates
        .iter()
        .map(|candidate| candidate.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "Unable to locate proto root. Searched paths: {searched}. Set `{PROTO_ROOT_ENV}` to a valid `proto` directory."
    )
    .into())
}

fn canonical_or_self(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    match path.canonicalize() {
        Ok(canonical) => Ok(canonical),
        Err(_) => Ok(path.to_path_buf()),
    }
}

fn has_top_level_proto_files(path: &Path) -> bool {
    std::fs::read_dir(path).is_ok_and(|entries| {
        entries.filter_map(Result::ok).any(|entry| {
            entry.path().extension() == Some(OsStr::new("proto"))
        })
    })
}

fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.iter().any(|existing| existing == &path) {
        paths.push(path);
    }
}

fn build_registry(
    protos: &[PathBuf],
) -> Result<(Vec<RegistryEntry>, Vec<String>), Box<dyn std::error::Error>> {
    let mut entries = Vec::new();
    let mut intercepted = Vec::new();

    for proto_path in protos {
        let source = std::fs::read_to_string(proto_path)?;
        let source = strip_comments(&source);
        let package = parse_package(&source).unwrap_or_else(|| "gitaly".to_string());

        for (service_name, service_body) in parse_service_blocks(&source) {
            let is_intercepted = contains_intercepted_option(&service_body);
            for (method_name, rpc_body) in parse_rpc_blocks(&service_body) {
                let full_method = format!("/{package}.{service_name}/{method_name}");
                if is_intercepted {
                    intercepted.push(full_method);
                    continue;
                }

                let (operation, scope) = parse_method_metadata(rpc_body.as_deref());
                entries.push(RegistryEntry {
                    full_method,
                    service: service_name.clone(),
                    method: method_name,
                    operation,
                    scope,
                });
            }
        }
    }

    entries.sort();
    entries.dedup_by(|a, b| a.full_method == b.full_method);

    intercepted.sort();
    intercepted.dedup();

    Ok((entries, intercepted))
}

fn generate_registry_source(
    out_dir: &Path,
    entries: &[RegistryEntry],
    intercepted: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = String::new();

    output.push_str("// @generated by build.rs; do not edit.\n");
    output.push_str(
        "pub(crate) const GENERATED_ENTRIES: &[(&str, &str, &str, Operation, Scope)] = &[\n",
    );
    for entry in entries {
        let _ = writeln!(
            output,
            "    ({:?}, {:?}, {:?}, Operation::{}, Scope::{}),",
            entry.full_method,
            entry.service,
            entry.method,
            operation_variant(&entry.operation),
            scope_variant(&entry.scope),
        );
    }
    output.push_str("];\n\n");
    output.push_str("pub(crate) const GENERATED_INTERCEPTED_METHODS: &[&str] = &[\n");
    for full_method in intercepted {
        let _ = writeln!(output, "    {:?},", full_method);
    }
    output.push_str("];\n");

    std::fs::write(out_dir.join("registry_generated.rs"), output)?;
    Ok(())
}

fn operation_variant(operation: &Operation) -> &'static str {
    match operation {
        Operation::Unknown => "Unknown",
        Operation::Accessor => "Accessor",
        Operation::Mutator => "Mutator",
        Operation::Maintenance => "Maintenance",
    }
}

fn scope_variant(scope: &Scope) -> &'static str {
    match scope {
        Scope::Unknown => "Unknown",
        Scope::Repository => "Repository",
        Scope::Storage => "Storage",
        Scope::Partition => "Partition",
    }
}

fn parse_package(source: &str) -> Option<String> {
    source.lines().find_map(|line| {
        let trimmed = line.trim();
        if !trimmed.starts_with("package ") {
            return None;
        }

        let remainder = trimmed.strip_prefix("package ")?;
        let package = remainder.strip_suffix(';')?.trim();
        if package.is_empty() {
            return None;
        }

        Some(package.to_string())
    })
}

fn strip_comments(source: &str) -> String {
    enum State {
        Normal,
        LineComment,
        BlockComment,
        StringLiteral,
    }

    let mut output = String::with_capacity(source.len());
    let chars: Vec<char> = source.chars().collect();
    let mut i = 0usize;
    let mut state = State::Normal;

    while i < chars.len() {
        match state {
            State::Normal => {
                if chars[i] == '"' {
                    output.push(chars[i]);
                    state = State::StringLiteral;
                    i += 1;
                } else if i + 1 < chars.len() && chars[i] == '/' && chars[i + 1] == '/' {
                    state = State::LineComment;
                    i += 2;
                } else if i + 1 < chars.len() && chars[i] == '/' && chars[i + 1] == '*' {
                    state = State::BlockComment;
                    i += 2;
                } else {
                    output.push(chars[i]);
                    i += 1;
                }
            }
            State::LineComment => {
                if chars[i] == '\n' {
                    output.push('\n');
                    state = State::Normal;
                }
                i += 1;
            }
            State::BlockComment => {
                if i + 1 < chars.len() && chars[i] == '*' && chars[i + 1] == '/' {
                    state = State::Normal;
                    i += 2;
                } else {
                    if chars[i] == '\n' {
                        output.push('\n');
                    }
                    i += 1;
                }
            }
            State::StringLiteral => {
                output.push(chars[i]);
                if chars[i] == '\\' && i + 1 < chars.len() {
                    output.push(chars[i + 1]);
                    i += 2;
                } else if chars[i] == '"' {
                    state = State::Normal;
                    i += 1;
                } else {
                    i += 1;
                }
            }
        }
    }

    output
}

fn parse_service_blocks(source: &str) -> Vec<(String, String)> {
    parse_named_blocks(source, "service")
}

fn parse_rpc_blocks(service_body: &str) -> Vec<(String, Option<String>)> {
    let mut rpc_blocks = Vec::new();
    let mut cursor = 0usize;

    while let Some(offset) = service_body[cursor..].find("rpc ") {
        let rpc_start = cursor + offset;
        let name_start = skip_ws(service_body, rpc_start + "rpc ".len());
        let name_end = take_ident_end(service_body, name_start);
        if name_end <= name_start {
            cursor = rpc_start + "rpc ".len();
            continue;
        }

        let method_name = service_body[name_start..name_end].to_string();
        let body_or_end_start = name_end;
        let open_idx = service_body[body_or_end_start..]
            .find('{')
            .map(|v| body_or_end_start + v);
        let semicolon_idx = service_body[body_or_end_start..]
            .find(';')
            .map(|v| body_or_end_start + v);

        let (rpc_body, next_cursor) = match (open_idx, semicolon_idx) {
            (Some(open), Some(semicolon)) if semicolon < open => (None, semicolon + 1),
            (Some(open), _) => {
                if let Some(close) = find_matching_brace(service_body, open) {
                    (Some(service_body[open + 1..close].to_string()), close + 1)
                } else {
                    (None, open + 1)
                }
            }
            (None, Some(semicolon)) => (None, semicolon + 1),
            (None, None) => (None, service_body.len()),
        };

        rpc_blocks.push((method_name, rpc_body));
        cursor = next_cursor;
    }

    rpc_blocks
}

fn parse_method_metadata(rpc_body: Option<&str>) -> (Operation, Scope) {
    let Some(body) = rpc_body else {
        return (Operation::Unknown, Scope::Unknown);
    };

    let Some(op_type_start) = body.find("option (op_type)") else {
        return (Operation::Unknown, Scope::Unknown);
    };
    let Some(open) = body[op_type_start..].find('{').map(|v| op_type_start + v) else {
        return (Operation::Unknown, Scope::Unknown);
    };
    let Some(close) = find_matching_brace(body, open) else {
        return (Operation::Unknown, Scope::Unknown);
    };

    let op_type_body = &body[open + 1..close];
    let operation = parse_scalar_token(op_type_body, "op")
        .map(|token| match token.as_str() {
            "ACCESSOR" => Operation::Accessor,
            "MUTATOR" => Operation::Mutator,
            "MAINTENANCE" => Operation::Maintenance,
            _ => Operation::Unknown,
        })
        .unwrap_or(Operation::Unknown);

    let scope = parse_scalar_token(op_type_body, "scope_level")
        .map(|token| match token.as_str() {
            "REPOSITORY" => Scope::Repository,
            "STORAGE" => Scope::Storage,
            "PARTITION" => Scope::Partition,
            _ => Scope::Unknown,
        })
        .unwrap_or(Scope::Repository);

    (operation, scope)
}

fn parse_scalar_token(body: &str, field_name: &str) -> Option<String> {
    let mut search_start = 0usize;
    while let Some(offset) = body[search_start..].find(field_name) {
        let field_start = search_start + offset;
        let field_end = field_start + field_name.len();

        let prev_ok = field_start == 0 || !is_ident(body[..field_start].chars().next_back()?);
        let next_ok = field_end >= body.len() || !is_ident(body[field_end..].chars().next()?);
        if !prev_ok || !next_ok {
            search_start = field_end;
            continue;
        }

        let colon_index = skip_ws(body, field_end);
        if body[colon_index..].chars().next()? != ':' {
            search_start = field_end;
            continue;
        }

        let value_start = skip_ws(body, colon_index + 1);
        let value_end = body[value_start..]
            .char_indices()
            .find_map(|(i, ch)| (!is_ident(ch)).then_some(value_start + i))
            .unwrap_or(body.len());
        if value_end <= value_start {
            return None;
        }

        return Some(body[value_start..value_end].to_string());
    }

    None
}

fn contains_intercepted_option(service_body: &str) -> bool {
    service_body.contains("option (intercepted) = true")
        || service_body.contains("option (gitaly.intercepted) = true")
}

fn parse_named_blocks(source: &str, keyword: &str) -> Vec<(String, String)> {
    let token = format!("{keyword} ");
    let mut blocks = Vec::new();
    let mut cursor = 0usize;

    while let Some(offset) = source[cursor..].find(&token) {
        let keyword_start = cursor + offset;
        let name_start = skip_ws(source, keyword_start + token.len());
        let name_end = take_ident_end(source, name_start);
        if name_end <= name_start {
            cursor = keyword_start + token.len();
            continue;
        }

        let open = match source[name_end..].find('{').map(|v| name_end + v) {
            Some(value) => value,
            None => break,
        };

        let Some(close) = find_matching_brace(source, open) else {
            break;
        };

        blocks.push((
            source[name_start..name_end].to_string(),
            source[open + 1..close].to_string(),
        ));
        cursor = close + 1;
    }

    blocks
}

fn find_matching_brace(source: &str, open_index: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, ch) in source[open_index..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return Some(open_index + offset);
                }
            }
            _ => {}
        }
    }

    None
}

fn skip_ws(source: &str, start: usize) -> usize {
    source[start..]
        .char_indices()
        .find_map(|(i, ch)| (!ch.is_whitespace()).then_some(start + i))
        .unwrap_or(source.len())
}

fn take_ident_end(source: &str, start: usize) -> usize {
    source[start..]
        .char_indices()
        .find_map(|(i, ch)| (!is_ident(ch)).then_some(start + i))
        .unwrap_or(source.len())
}

fn is_ident(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric() || ch == '.'
}
