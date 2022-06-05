use color_eyre::eyre::{eyre, Report, Result, WrapErr};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use ignore::WalkBuilder;
use log::{info, trace, warn};
use regex::Regex;
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, Read};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use structopt::StructOpt;
use walkdir::WalkDir;

fn main() -> Result<(), Report> {
    let opt = Opt::from_args();
    TermLogger::init(
        match opt.verbose {
            0 => log::LevelFilter::Warn,
            1 => log::LevelFilter::Info,
            2 => log::LevelFilter::Debug,
            _ => log::LevelFilter::Trace,
        },
        ConfigBuilder::new()
            .set_time_level(log::LevelFilter::Debug)
            .set_time_format_str("%M:%S.%6f")
            .clear_filter_ignore()
            .build(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )?;

    trace!("{:#?}", opt);

    let mut inventory = Inventory::load(opt.cache_path.clone());

    let cur_key = opt.command_input_key().map_err(|e| warn!("{}", e)).ok();
    trace!("Computed key: {:#?}", cur_key);

    if let (Some(cur_key), Some(inventory)) = (&cur_key, inventory.as_mut()) {
        if inventory.try_restore_from_cache(cur_key, &opt.output_path, opt.dry_run) {
            return Ok(());
        }
    }

    if !opt.dry_run {
        info!("Running command");
        let exit_status = std::process::Command::new(&opt.command[0])
            .args(&opt.command[1..])
            .status()
            .wrap_err("Cannot start command")?;
        if !exit_status.success() {
            trace!("Child failed, returning its exit code");
            std::process::exit(exit_status.code().unwrap_or(0))
        }
        trace!("Command was successful");
    }

    if let (Some(inventory), Some(cur_key)) = (inventory.as_mut(), cur_key) {
        let output_size = inventory.output_size(&opt.output_path)?;
        if !opt.dry_run {
            inventory.discard_until(output_size, opt.max_cache_size)?;
            inventory.write_to_cache(&opt.output_path, &cur_key)?;
        }
    }

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name = "folca", about = "Folder-based command cache")]
struct Opt {
    /// Respect `.ignore` and `.gitignore` files
    #[structopt(long)]
    respect_ignore: bool,

    /// Hash hidden files
    #[structopt(long)]
    include_hidden: bool,

    #[structopt(long, default_value = ".folca_cache")]
    cache_path: PathBuf,

    #[structopt(long, default_value = "10 GB", parse(try_from_str = Self::non_zero_bytes))]
    max_cache_size: u64,

    /// Verbose
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    #[structopt(parse(from_os_str))]
    input_path: PathBuf,

    #[structopt(parse(from_os_str))]
    output_path: PathBuf,

    #[structopt(required = true)]
    command: Vec<String>,

    /// Do not run command or modify cache + log intermediate hashes (has a performance hit)
    #[structopt(long)]
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct Inventory {
    inv: HashMap<CommandInputHashes, LastUsedAndSize>,
    cache_path: PathBuf,
    regex: Regex,
}

impl Inventory {
    fn load_entry(&mut self, path: PathBuf) -> Result<()> {
        let string_path = path.to_string_lossy().to_string();
        let caps = self
            .regex
            .captures(&string_path)
            .ok_or(eyre!(string_path.clone()))?;

        let command_hash = u64::from_str_radix(&caps[1], 16)?;
        let input_hash = u64::from_str_radix(&caps[2], 16)?;

        let metadata = path.metadata()?;

        self.inv.insert(
            CommandInputHashes {
                command_hash,
                input_hash,
            },
            LastUsedAndSize {
                last_used: metadata.accessed()?,
                size: metadata.len(),
            },
        );

        Ok(())
    }

    fn load(path: PathBuf) -> Option<Self> {
        let mut result = Self {
            inv: HashMap::new(),
            cache_path: path,
            regex: Regex::new(r".*/([[:a-z0-9:]]+)/([[:a-z0-9:]]{16}).tar.gz$").unwrap(),
        };

        if !result.cache_path.exists() {
            info!("Cache path does not exist");
            return Some(result);
        }

        for entry in WalkDir::new(&result.cache_path)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(|e| match e {
                Err(err) => {
                    warn!("{}", err);
                    None
                }
                Ok(walkdir_entry) => Some(walkdir_entry.path().to_owned()),
            })
        {
            result
                .load_entry(entry.to_path_buf())
                .wrap_err(format!(
                    "Error while loading cache entry from {}",
                    &entry.to_string_lossy()
                ))
                .unwrap_or_else(|e| warn!("{}", e));
        }
        Some(result)
    }

    fn to_path(&self, key: &CommandInputHashes) -> PathBuf {
        let mut result = self
            .cache_path
            .join(format!("{:x}", &key.command_hash))
            .join(format!("{:x}", &key.input_hash));
        result.set_extension("tar.gz");
        result
    }

    fn try_restore_from_cache(
        &mut self,
        key: &CommandInputHashes,
        output_path: &PathBuf,
        dry_run: bool,
    ) -> bool {
        let cached_path = self.to_path(key);
        let output_dir = {
            if output_path.is_file() {
                output_path.parent().unwrap().to_path_buf()
            } else {
                output_path.clone()
            }
        };

        if let Some(val) = self.inv.get_mut(key) {
            info!(
                "Found cached entry, copying {}",
                cached_path.to_string_lossy()
            );
            if !dry_run {
                let result = File::open(&cached_path)
                    .map(GzDecoder::new)
                    .map(tar::Archive::new)
                    .and_then(|mut archive| archive.unpack(output_dir))
                    .map_err(|e| warn!("{}", e));
                if result.is_ok() {
                    val.last_used = std::time::SystemTime::now();
                }
                return result.is_ok();
            }
        }
        info!("No such cached entry: {}", cached_path.to_string_lossy());
        false
    }

    fn output_size(&self, output_path: &PathBuf) -> Result<u64> {
        let mut sum = 0u64;
        for entry in WalkDir::new(&output_path) {
            let metadata = entry?.metadata()?;
            if metadata.is_file() {
                sum += metadata.len();
            }
        }
        Ok(sum)
    }

    fn write_to_cache(&mut self, output_path: &PathBuf, key: &CommandInputHashes) -> Result<u64> {
        if !output_path.exists() {
            std::fs::create_dir(&output_path)?
        }
        let cached_path = self.to_path(key);
        trace!(
            "Copying result {} to cache {}",
            output_path.to_string_lossy(),
            cached_path.to_string_lossy()
        );

        std::fs::create_dir_all(&cached_path.parent().unwrap())?;

        let mut tar = tar::Builder::new(GzEncoder::new(
            File::create(&cached_path)?,
            Compression::default(),
        ));
        if output_path.is_dir() {
            tar.append_dir_all(".", output_path)?;
        } else {
            tar.append_path_with_name(output_path, output_path.file_name().unwrap())?;
        }
        tar.finish()?;

        self.output_size(output_path)
    }

    fn discard_until(&mut self, output_size: u64, limit: u64) -> Result<()> {
        if output_size >= limit {
            warn!("Output is larger than cache size, will not cache");
            return Ok(());
        }

        trace!("Assuring cache is within limits");
        let mut cache_size = 0u64;
        let mut cache_entries: Vec<(CommandInputHashes, LastUsedAndSize)> = self
            .inv
            .iter()
            .map(|(key, value)| {
                cache_size += value.size;
                (key.clone(), value.clone())
            })
            .collect();
        cache_entries.sort_by(|p1, p2| p2.1.last_used.cmp(&p1.1.last_used));

        while output_size + cache_size >= limit {
            let (key, value) = cache_entries.pop().expect(
                "
                Ran out of cache entries without hitting 0 size.
                This likely means somebody touched the cache entry folder mid-run.
            ",
            );
            trace!(
                "Removing {} with size: {:?}, last_used: {:?}",
                self.to_path(&key).to_string_lossy(),
                &value.size,
                &value
                    .last_used
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or(Duration::new(0, 0)),
            );
            self.inv.remove(&key);
            let path = self.to_path(&key);
            std::fs::remove_file(&path)
                .wrap_err(format!("Folca: cannot remove {}", &path.to_string_lossy()))
                .map_err(|e| {
                    warn!("{}", e);
                    e
                })?;
            let parent = path.parent().ok_or_else(|| eyre!("Can't list empty dir"))?;
            if std::fs::read_dir(parent)?.next().is_none() {
                // empty directory?
                trace!("Directory is empty after cache limiting, cleaning up...");
                std::fs::remove_dir(parent)?;
            }
            cache_size -= value.size;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CommandInputHashes {
    command_hash: u64,
    input_hash: u64,
}

impl Opt {
    fn non_zero_bytes(input: &str) -> Result<u64, &'static str> {
        let parsed = bytefmt::parse(input)?;
        if parsed == 0 {
            Err("Cache size cannot be zero")
        } else {
            Ok(parsed)
        }
    }

    fn command_input_key(&self) -> Result<CommandInputHashes> {
        let command_hash = {
            let mut command_hasher = DefaultHasher::new();
            for command_part in &self.command {
                command_hasher.write(command_part.as_bytes());
            }
            command_hasher.finish()
        };

        let mut hasher = DefaultHasher::new();
        let mut buffer = vec![0u8; 125_000];
        if self.dry_run {
            trace!("initial hash state: {:x}", hasher.finish());
        }

        for entry in WalkBuilder::new(&self.input_path)
            .hidden(self.include_hidden)
            .git_exclude(self.respect_ignore)
            .sort_by_file_path(|p1, p2| p1.cmp(p2))
            .skip_stdout(true)
            .build()
        {
            let dir_entry = entry.map_err(|e| {
                warn!("{}", e);
                e
            })?;
            let path = dir_entry.path();
            hasher.write(path.as_os_str().as_bytes());
            if self.dry_run {
                trace!(
                    "after hashing the path {}: {:x}",
                    path.to_string_lossy(),
                    hasher.finish()
                );
            }

            if path.is_dir() {
                continue;
            }
            if !path.is_file() {
                warn!(
                    "{} is not a file or a directory, skipping.",
                    path.to_string_lossy()
                );
                continue;
            }

            trace!("Hashing content of {}", path.to_string_lossy());
            if let Err(e) = Opt::update_hasher_with_file(&mut buffer, path, &mut hasher) {
                warn!("{}", e);
            }
        }

        Ok(CommandInputHashes {
            input_hash: hasher.finish(),
            command_hash,
        })
    }

    fn update_hasher_with_file(
        buffer: &mut [u8],
        path: &Path,
        hasher: &mut DefaultHasher,
    ) -> Result<()> {
        let mut file_handler = BufReader::new(File::open(path)?);
        loop {
            let bytes_read = file_handler.read(buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.write(&buffer[0..bytes_read]);
        }

        trace!("Hashed content of {}", path.to_string_lossy());
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct LastUsedAndSize {
    last_used: std::time::SystemTime,
    size: u64,
}
