use std::env;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() == 1 && (args[0] == "--help" || args[0] == "-h") {
        print_help();
        return ExitCode::SUCCESS;
    }
    if !args.is_empty() {
        eprintln!("gitaly-lfs-smudge: unexpected arguments; try `--help`");
        return ExitCode::from(2);
    }

    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gitaly-lfs-smudge: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> io::Result<()> {
    let mut pointer = Vec::new();
    io::stdin().read_to_end(&mut pointer)?;

    let object_dir = env::var_os("GITALY_LFS_OBJECT_DIR").map(PathBuf::from);
    let oid = extract_oid(&pointer);

    if let (Some(object_dir), Some(oid)) = (object_dir, oid) {
        if let Some(object_path) = find_object_path(&object_dir, &oid) {
            let mut stdout = io::stdout().lock();
            let mut file = File::open(object_path)?;
            io::copy(&mut file, &mut stdout)?;
            stdout.flush()?;
            return Ok(());
        }
    }

    let mut stdout = io::stdout().lock();
    stdout.write_all(&pointer)?;
    stdout.flush()?;
    Ok(())
}

fn extract_oid(pointer: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(pointer).ok()?;
    for line in text.lines() {
        if let Some(oid_value) = line.strip_prefix("oid ") {
            let hash = oid_value.split(':').nth(1).unwrap_or(oid_value).trim();
            if !hash.is_empty() {
                return Some(hash.to_string());
            }
        }
    }
    None
}

fn find_object_path(base_dir: &Path, oid: &str) -> Option<PathBuf> {
    let direct = base_dir.join(oid);
    if direct.is_file() {
        return Some(direct);
    }

    if oid.len() > 2 {
        let split = base_dir.join(&oid[..2]).join(&oid[2..]);
        if split.is_file() {
            return Some(split);
        }
    }

    let nested = base_dir.join("sha256").join(oid);
    if nested.is_file() {
        return Some(nested);
    }

    None
}

fn print_help() {
    println!(
        "Usage: gitaly-lfs-smudge\n\
         \n\
         Read an LFS pointer from stdin. If object data exists under\n\
         `GITALY_LFS_OBJECT_DIR`, write the object bytes; otherwise pass the\n\
         original pointer through."
    );
}
