use gitaly_storage::snapshot;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match parse_command(env::args().skip(1)) {
        Ok(Command::Help) => {
            print_help();
            ExitCode::SUCCESS
        }
        Ok(Command::Backup { source, dest }) => match snapshot::create_snapshot(source, dest) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("gitaly-backup: backup failed: {error}");
                ExitCode::from(1)
            }
        },
        Ok(Command::Restore { snapshot, target }) => {
            match snapshot::restore_snapshot(snapshot, target) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("gitaly-backup: restore failed: {error}");
                    ExitCode::from(1)
                }
            }
        }
        Err(error) => {
            eprintln!("gitaly-backup: {error}");
            eprintln!("gitaly-backup: try `--help` for usage");
            ExitCode::from(2)
        }
    }
}

enum Command {
    Help,
    Backup { source: PathBuf, dest: PathBuf },
    Restore { snapshot: PathBuf, target: PathBuf },
}

fn parse_command(args: impl Iterator<Item = String>) -> Result<Command, String> {
    let mut args = args.peekable();
    match args.next().as_deref() {
        None | Some("--help") | Some("-h") | Some("help") => Ok(Command::Help),
        Some("backup") => parse_backup(args),
        Some("restore") => parse_restore(args),
        Some(other) => Err(format!("unknown subcommand '{other}'")),
    }
}

fn parse_backup(args: impl Iterator<Item = String>) -> Result<Command, String> {
    let mut source: Option<PathBuf> = None;
    let mut dest: Option<PathBuf> = None;
    let mut args = args.peekable();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source" => {
                source =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        String::from("missing value for `--source`")
                    })?));
            }
            "--dest" => {
                dest = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| String::from("missing value for `--dest`"))?,
                ));
            }
            value if value.starts_with("--source=") => {
                source = Some(PathBuf::from(value.trim_start_matches("--source=")));
            }
            value if value.starts_with("--dest=") => {
                dest = Some(PathBuf::from(value.trim_start_matches("--dest=")));
            }
            "--help" | "-h" => return Ok(Command::Help),
            other => return Err(format!("unknown argument for `backup`: '{other}'")),
        }
    }

    let source = source.ok_or_else(|| String::from("`backup` requires `--source <dir>`"))?;
    let dest = dest.ok_or_else(|| String::from("`backup` requires `--dest <dir>`"))?;
    Ok(Command::Backup { source, dest })
}

fn parse_restore(args: impl Iterator<Item = String>) -> Result<Command, String> {
    let mut snapshot_dir: Option<PathBuf> = None;
    let mut target: Option<PathBuf> = None;
    let mut args = args.peekable();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--snapshot" => {
                snapshot_dir =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        String::from("missing value for `--snapshot`")
                    })?));
            }
            "--target" => {
                target =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        String::from("missing value for `--target`")
                    })?));
            }
            value if value.starts_with("--snapshot=") => {
                snapshot_dir = Some(PathBuf::from(value.trim_start_matches("--snapshot=")));
            }
            value if value.starts_with("--target=") => {
                target = Some(PathBuf::from(value.trim_start_matches("--target=")));
            }
            "--help" | "-h" => return Ok(Command::Help),
            other => return Err(format!("unknown argument for `restore`: '{other}'")),
        }
    }

    let snapshot =
        snapshot_dir.ok_or_else(|| String::from("`restore` requires `--snapshot <dir>`"))?;
    let target = target.ok_or_else(|| String::from("`restore` requires `--target <dir>`"))?;
    Ok(Command::Restore { snapshot, target })
}

fn print_help() {
    println!(
        "Usage:\n\
           gitaly-backup backup --source <dir> --dest <dir>\n\
           gitaly-backup restore --snapshot <dir> --target <dir>\n\
         \n\
         Create or restore local directory snapshots."
    );
}
