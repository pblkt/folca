use blake3;
use color_eyre::eyre::{Report, Result, WrapErr};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use ignore::WalkBuilder;
use log::{debug, info, trace, warn};
use serde::{Deserialize, Serialize};
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use structopt::StructOpt;
use tar;
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
    if !opt.cache_path.is_dir() {
        debug!("Creating cache directory");
        std::fs::create_dir(&opt.cache_path)
            .wrap_err("Folca: cannot create cache directory:")
            .unwrap_or_else(|e| warn!("{}", e));
    }

    let mut inventory = Inventory::load(opt.cache_path.clone());

    let cur_key = opt.create_key();

    if let (Some(cur_key), Some(inventory)) = (&cur_key, inventory.as_mut()) {
        if inventory.restore(&cur_key, &opt.output_path) {
            return Ok(());
        }
    }

    info!("Running command");
    let exit_status = std::process::Command::new(&opt.command[0])
        .args(&opt.command[1..])
        .status()
        .wrap_err("Cannot start command")?;
    if !exit_status.success() {
        trace!("Child failed, returning its exit code");
        std::process::exit(exit_status.code().unwrap_or(0))
    }
    trace!("Command was succesful");

    if let (Some(inventory), Some(cur_key)) = (inventory.as_mut(), cur_key) {
        if let Some(output_size) = inventory.output_size(&opt.output_path) {
            if inventory.limit_cache(output_size, opt.cache_size).is_ok() {
                inventory.insert(&opt.output_path, cur_key);
                inventory.persist();
            }
        }
    }

    Ok(())
}
#[derive(Debug, StructOpt)]
#[structopt(name = "folca", about = "Folder-based command cache")]
struct Opt {
    /// Do not respect `.ignore` and `.gitignore` files
    #[structopt(long)]
    no_ignore: bool,

    /// Hash hidden files
    #[structopt(long)]
    hidden: bool,

    #[structopt(long, default_value = ".folca_cache")]
    cache_path: PathBuf,

    #[structopt(long, default_value = "10 GB", parse(try_from_str = Self::non_zero_bytes))]
    cache_size: u64,

    /// Verbose
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    #[structopt(parse(from_os_str))]
    input_path: PathBuf,

    #[structopt(parse(from_os_str))]
    output_path: PathBuf,

    #[structopt(required = true)]
    command: Vec<String>,
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
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Inventory {
    inv: HashMap<CacheKey, CacheValue>,
    cache_path: PathBuf,
}

impl Inventory {
    fn load(path: PathBuf) -> Option<Self> {
        let inventory_path = path.join(".inventory.ron");
        if inventory_path.is_file() {
            debug!("Medatadata file found");
            File::open(&inventory_path)
                .map_err(Report::msg)
                .and_then(|file| {
                    let reader = BufReader::new(file);
                    ron::de::from_reader(reader).map_err(Report::msg)
                })
                .wrap_err(format!(
                    "Folca: Cannot parse {}",
                    &inventory_path.to_string_lossy()
                ))
                .map_err(|e| warn!("{}", e))
                .ok()
        } else {
            debug!("Medatadata file not found");
            Some(Self {
                inv: HashMap::new(),
                cache_path: path,
            })
        }
    }

    fn persist(&self) {
        trace!("Serializing inventory");
        //let x: i32 =
        let inventory_path = self.cache_path.join(".inventory.ron");
        std::fs::File::create(inventory_path)
            .map_err(Report::msg)
            .and_then(|file| {
                ron::ser::to_writer_pretty(file, &self, Default::default()).map_err(Report::msg)
            })
            .wrap_err("Folca: cannot serialize inventory")
            .unwrap_or_else(|e| warn!("{}", e));
    }

    fn to_path(&self, key: &CacheKey) -> PathBuf {
        let mut result = self
            .cache_path
            .join(format!("{:x}", &key.command_hash))
            .join(format!("{:x}", simple_hash(&key.input_hash)));
        result.set_extension("tar.gz");
        result
    }

    fn restore(&mut self, key: &CacheKey, output_path: &PathBuf) -> bool {
        let cached_path = self.to_path(&key);
        if let Some(val) = self.inv.get_mut(&key) {
            info!(
                "Found cached entry, copying {}",
                cached_path.to_string_lossy()
            );
            if File::open(&cached_path)
                .map(|file| GzDecoder::new(file))
                .map(|tar| tar::Archive::new(tar))
                .and_then(|mut archive| archive.unpack(output_path))
                .wrap_err("Folca: Cannot extract cached tar")
                .map_err(|e| warn!("{}", e))
                .is_ok()
            {
                val.last_used = std::time::SystemTime::now();
                return true;
            }
        }
        info!("No such cached entry: {}", cached_path.to_string_lossy());
        false
    }

    fn output_size(&self, output_path: &PathBuf) -> Option<u64> {
        WalkDir::new(&output_path)
            .into_iter()
            .try_fold(0u64, |sum, entry| {
                entry.and_then(|entry| entry.metadata()).map(|metadata| {
                    if metadata.is_file() {
                        sum + metadata.len()
                    } else {
                        sum
                    }
                })
            })
            .wrap_err("Folca: cannot calculate output size")
            .ok()
    }

    fn insert(&mut self, output_path: &PathBuf, key: CacheKey) -> () {
        let cached_path = self.to_path(&key);
        trace!(
            "Copying result {} to cache {}",
            output_path.to_string_lossy(),
            cached_path.to_string_lossy()
        );

        std::fs::create_dir_all(&cached_path.parent().unwrap())
            .and_then(|_| {
                File::create(&cached_path)
                    .map(|file| GzEncoder::new(file, Compression::default()))
                    .and_then(|enc| {
                        let mut tar = tar::Builder::new(enc);
                        tar.append_dir_all(".", &output_path)
                            .and_then(|_| tar.finish())
                    })
            })
            .wrap_err(format!(
                "Folca: Cannot write to cache {}",
                &cached_path.to_string_lossy()
            ))
            .unwrap_or_else(|e| warn!("{}", e));

        let output_size = self.output_size(output_path).unwrap();
        self.inv.insert(
            key,
            CacheValue {
                size: output_size,
                last_used: std::time::SystemTime::now(),
            },
        );
    }

    fn limit_cache(&mut self, output_size: u64, limit: u64) -> Result<()> {
        if output_size >= limit {
            warn!("Output is larger than cache size, will not cache");
            return Ok(());
        }

        trace!("Assuring cache is within limits");
        let mut cache_size = 0u64;
        let mut cache_entries: Vec<(CacheKey, CacheValue)> = self
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
            cache_size -= value.size;
        }
        Ok(())
    }
}

fn simple_hash(x: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(x);
    hasher.finish()
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    command_hash: u64,
    input_hash: Vec<u8>,
}

impl Opt {
    fn create_key(&self) -> Option<CacheKey> {
        let command_hash = {
            let mut command_hasher = DefaultHasher::new();
            for command_part in &self.command {
                command_hasher.write(command_part.as_bytes());
            }
            command_hasher.finish()
        };

        let mut hasher = blake3::Hasher::new();
        let mut buffer = Vec::new();

        let update_hash = |entry: Result<ignore::DirEntry, ignore::Error>| -> Result<()> {
            let dir_entry = entry.map_err(|e| {
                warn!("{}", e);
                e
            })?;
            let path = dir_entry.path();
            hasher.update(&path.to_string_lossy().as_bytes().to_vec());

            if !path.is_file() {
                return Ok(());
            }
            File::open(path)
                .map(|file| BufReader::new(file))
                .and_then(|mut reader| {
                    reader.read_to_end(&mut buffer).map(|_| {
                        hasher.update(&buffer);
                        buffer.clear();
                    })
                })
                .wrap_err(format!(
                    "Folca: cannot hash file: {:?}",
                    &path.to_string_lossy()
                ))
        };

        WalkBuilder::new(&self.input_path)
            .hidden(self.hidden)
            .git_exclude(!self.no_ignore)
            .sort_by_file_path(|p1, p2| p1.cmp(p2))
            .skip_stdout(true)
            .filter_entry(|dir_entry| dir_entry.file_type().map_or(false, |x| x.is_file()))
            .build()
            .try_for_each(update_hash)
            .wrap_err("Folca: cannot hash input")
            .map_err(|e| warn!("{}", e))
            .map(|_| CacheKey {
                input_hash: hasher.finalize().as_bytes().to_vec(),
                command_hash,
            })
            .ok()
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
struct CacheValue {
    last_used: std::time::SystemTime,
    size: u64,
}
