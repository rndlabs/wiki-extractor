use std::{
    collections::HashMap,
    error::Error,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf}, borrow::BorrowMut,
    sync::mpsc,
    env,
};

use bee_herder::{HerdFile, HerdStatus};
use clap::ArgMatches;
use indicatif::{ProgressBar, ProgressStyle};
use kuchiki::{parse_html, traits::TendrilSink};
use rayon::prelude::*;
use zim::{Cluster, DirectoryEntry, MimeType, Namespace, Target, Zim};

use flate2::write::GzEncoder;
use flate2::Compression;

#[derive(Debug, Clone)]
pub struct ClusterNotFoundError {
    pub cluster: usize,
}
impl  std::fmt::Display for ClusterNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cluster {} not found", self.cluster)
    }
}
impl Error for ClusterNotFoundError {}

// invalid blob error
#[derive(Debug, Clone)]
pub struct InvalidBlobError {
    pub blob: String,
}
impl  std::fmt::Display for InvalidBlobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid blob: {}", self.blob)
    }
}
impl Error for InvalidBlobError {}

// skip missing target
#[derive(Debug, Clone)]
pub struct SkipMissingTargetError {
    pub target: String,
}
impl  std::fmt::Display for SkipMissingTargetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Skip missing target: {}", self.target)
    }
}
impl Error for SkipMissingTargetError {}

pub struct Config {
    pub input: String,
    pub output: String,
    pub db: String,
}

impl Config {
    pub fn new(matches: ArgMatches) -> Result<Config, &'static str> {

        let input = matches.value_of("input").unwrap().to_string();
        let output = matches.value_of("output").unwrap().to_string();
        // return err if db is not set
        let db = match env::var("BEE_HERDER_DB") {
            Ok(val) => val,
            Err(_) => return Err("Environment variable BEE_HERDER_DB must be set"),
        };
    
        let config = Config {
            input,
            output,
            db,
        };
    
        Ok(config)
    }
}


pub fn run(config: Config) -> Result<(), Box<dyn Error>> {
    // eprintln!("Number of article: {}", zim.article_count());

    eprintln!("Extracting file {} to {}", config.input, config.output);

    let sw = std::time::Instant::now();
    let zim = Zim::new(config.input)?;

    let pb = ProgressBar::new(zim.article_count() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")?
        .progress_chars("#>-"));

    let root_output = Path::new(&config.output);
    let db_output_path = Path::new(&config.output).join("db");

    ensure_dir(root_output);

    // map between cluster and directory entry
    let mut cluster_map = HashMap::new();

    for i in 0..zim.header.cluster_count {
        let cluster = zim.get_cluster(i).expect("Failed to get cluster");
        cluster_map.insert(i, cluster);
    }

    // create a sync channel to send the directory entry to the thread
    let (tx, rx) = mpsc::sync_channel::<HerdFile>(100);

    // create a thread to handle the directory entries and log them to a leveldb database
    let handle = std::thread::spawn(move || {
        let db = sled::open(db_output_path).expect("Failed to open database");
        let mut batch = sled::Batch::default();

        let mut count = 0;
        for entry in rx {
            // let the key be f_ concatenated with entry.path
            let mut key = "f_".as_bytes().to_vec();
            key.extend_from_slice(&entry.path);
            let value = bincode::serialize(&entry).expect("Failed to serialize entry");
            batch.insert(key, value);

            count += 1;
            if count % 1000 == 0 {
                db.apply_batch(batch).expect("Failed to apply batch");
                batch = sled::Batch::default();
            }
        }

        db.apply_batch(batch).expect("Failed to apply batch");
    });

    // parallel processing of the entries
    let entries: Vec<_> = zim.iterate_by_urls().collect();
    pb.set_message("Writing entries to disk");
    entries
        .par_iter()
        .filter(|entry| matches!(entry.target.as_ref(), Some(Target::Cluster(_, _))))
        .for_each(|entry| {
            let path = process_file(root_output, &cluster_map, entry, &pb);

            // make sure the path is valid
            if let Ok(path) = path {
                let path = path.strip_prefix(root_output).unwrap().to_str().unwrap().to_string().as_bytes().to_vec();

                // process the metadata
                let mut metadata: HashMap<String, String> = HashMap::new();

                if let MimeType::Type(t) = &entry.mime_type {
                    metadata.insert("Content-Type".to_string(), t.to_string());
                    metadata.insert("Filename".to_string(), entry.url.to_string());
                }

                // send the entry to the thread
                tx.send(HerdFile {
                    path,
                    prefix: entry.url.as_bytes().to_vec(),
                    status: HerdStatus::Pending,
                    tag: None,
                    reference: None,
                    mantaray_reference: None,
                    metadata,
                }).expect("Failed to send entry");
            } else {
                pb.inc(1);
            }
        });

    // close the channel
    drop(tx);

    // wait for the thread to finish
    handle.join().unwrap();

    pb.finish_with_message(format!("Extraction done in {}s", sw.elapsed().as_secs()));

    Ok(())
}

fn safe_write<T: AsRef<[u8]>>(path: &Path, data: T, count: usize) {
    let display = path.display();
    let contain_path = path.parent().unwrap();

    ensure_dir(contain_path);

    match File::create(&path) {
        Err(why) => {
            if count < 3 {
                safe_write(path, data, count + 1);
            } else {
                eprintln!(
                    "skipping: failed retry: couldn't create {}: {:?}",
                    display, why
                );
            }
        }
        Ok(file) => {
            let mut writer = BufWriter::new(&file);

            if let Err(why) = writer.write_all(data.as_ref()) {
                eprintln!("skipping: couldn't write to {}: {}", display, why);
            }
        }
    }
}

fn enhance_html(dst: &Path, blob: &[u8]) -> Result<(), Box<dyn Error>> {
    // before we do any processing, declare the zlib compressor for streaming
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());

    let contain_path = dst.parent().unwrap();
    ensure_dir(contain_path);

    // strip out some html
    let root = parse_html()
        .one(String::from_utf8(<&[u8]>::clone(&blob).to_vec())?)
        .select_first("body")
        .unwrap();

    // process all `source`, `img`, and `track` src attributes
    root.as_node()
        .select("img,track,source").unwrap()
        .borrow_mut()
        .for_each(|mut e| {
            let rc = &e.borrow_mut().attributes;
            let attributes = &mut (*rc.borrow_mut());

            let mut src = "media/".to_string();
            src.push_str(&attributes.get_mut("src").unwrap()[5..]);

            attributes.insert("src", src);
        });

    // process all `script` src attributes
    root.as_node()
        .select("script").unwrap()
        .borrow_mut()
        .for_each(|mut e| {
            let rc = &e.borrow_mut().attributes;
            let attributes = &mut (*rc.borrow_mut());

            let mut src = "static/".to_string();
            src.push_str(&attributes.get_mut("src").unwrap()[5..]);

            attributes.insert("src", src);
        });


    root.as_node()
        // .serialize_to_file(dst)?;
        .serialize(&mut enc)?;
            
    let output: Vec<u8> = enc.finish()?;

    safe_write(dst, output, 1);

    Ok(())
}

fn process_file<'a>(
    root_output: &Path,
    cluster_map: &'a HashMap<u32, Cluster<'a>>,
    entry: &DirectoryEntry,
    pb: &ProgressBar,
) -> Result<PathBuf, Box<dyn Error>> {
    let dst = make_path(root_output, entry.namespace, &entry.url, &entry.mime_type);
    match entry.target.as_ref() {
        Some(Target::Cluster(cluster_index, blob_idx)) => {
            let cluster = cluster_map.get(cluster_index).ok_or(ClusterNotFoundError{ cluster: *cluster_index as usize })?;

            match cluster.get_blob(*blob_idx) {
                Ok(blob) => {
                    if let MimeType::Type(mime) = &entry.mime_type {
                        if mime == "text/html" {
                            enhance_html(&dst, &blob)?;
                        } else {
                            safe_write(&dst, blob, 1);
                        }
                    } else {
                        safe_write(&dst, blob, 1);
                    }
                }
                Err(err) => return Err(Box::new(err)),
            }
            pb.inc(1);

            Ok(dst)
        }
        Some(_) => unreachable!("filtered out earlier"),
        None => Err(SkipMissingTargetError{ target: entry.url.clone() }.into()),
    }
}

fn ensure_dir(path: &Path) {
    if path.exists() {
        // already done
        return;
    }

    std::fs::create_dir_all(path)
        .unwrap_or_else(|e| ignore_exists_err(e, &format!("create: {}", path.display())));
}

fn ignore_exists_err<T: AsRef<str>>(e: std::io::Error, msg: T) {
    use std::io::ErrorKind::*;

    match e.kind() {
        // do not panic if it already exists, that's fine, we just want to make
        // sure we have it before moving on
        AlreadyExists => {}
        _ => {
            eprintln!("skipping: {}: {}", msg.as_ref(), e);
        }
    }
}

fn make_path(root: &Path, namespace: Namespace, url: &str, mime_type: &MimeType) -> PathBuf {
    // let mut s = String::new();
    // s.push(namespace as u8 as char);
    let s = match namespace {
        Namespace::Layout => "static".to_string(),
        Namespace::Articles => "wiki".to_string(),
        Namespace::ArticleMetaData => todo!(),
        Namespace::ImagesFile => "media".to_string(),
        Namespace::ImagesText => todo!(),
        Namespace::Metadata => "meta".to_string(),
        Namespace::CategoriesText => todo!(),
        Namespace::CategoriesArticleList => todo!(),
        Namespace::CategoriesArticle => todo!(),
        Namespace::FulltextIndex => "search".to_string(),
    };

    let mut path = if url.starts_with('/') {
        // make absolute urls relative to the output folder
        let url = url.replacen('/', "", 1);
        root.join(&s).join(url)
    } else {
        root.join(&s).join(url)
    };

    if let MimeType::Type(typ) = mime_type {
        let extension = match typ.as_str() {
            "text/html" => None,
            "image/jpeg" => Some("jpg"),
            "image/png" => Some("png"),
            "image/gif" => Some("gif"),
            "image/svg+xml" => Some("svg"),
            "application/javascript" => Some("js"),
            "text/css" => Some("css"),
            "text/plain" => Some("txt"),
            _ => None,
        };
        if let Some(extension) = extension {
            if path.extension().is_none()
                || !path
                    .extension()
                    .unwrap()
                    .to_str()
                    .unwrap_or_default()
                    .starts_with(extension)
            {
                path.set_extension(extension);
            }
        }
    }

    path
}
