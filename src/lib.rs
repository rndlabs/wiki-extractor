#![feature(let_chains)]
use std::{
    borrow::BorrowMut,
    collections::{HashMap, BTreeMap},
    env,
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::mpsc,
};

use bee_herder::{HerdFile, HerdStatus};
use clap::ArgMatches;
use indicatif::{ProgressBar, ProgressStyle};
use kuchiki::{parse_html, traits::TendrilSink};
use rayon::prelude::*;
use zim::{MimeType, Namespace, Target, Zim};

use flate2::write::GzEncoder;
use flate2::Compression;

use thiserror::Error;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send>>;

#[derive(Error, Debug, Clone)]
pub enum WikiExtractorError {
    #[error("Missing environment variable: {0}")]
    MissingEnvVarError(String),
    #[error("Cluster {0} not found")]
    ClusterNotFound(usize),
    #[error("Invalid blob: {0}")]
    InvalidBlob(String),
    #[error("Skip missing target: {0}")]
    SkipMissingTargetError(String),
    #[error("zim error: {0}")]
    Zim(String),
    #[error("compress error: {0}")]
    Compress(String),
    #[error("io error: {0}")]
    Io(String),
}

pub struct Config {
    pub input: String,
    pub output: String,
    pub db: String,
}

impl Config {
    pub fn new(matches: ArgMatches) -> Result<Config> {
        let input = matches.value_of("input").unwrap().to_string();
        let output = matches.value_of("output").unwrap().to_string();
        // return err if db is not set
        let db = match env::var("BEE_HERDER_DB") {
            Ok(val) => val,
            Err(_) => {
                return Err(Box::new(WikiExtractorError::MissingEnvVarError(
                    "BEE_HERDER_DB".to_string(),
                )))
            }
        };

        let config = Config { input, output, db };

        Ok(config)
    }
}

pub fn run(config: Config) -> Result<()> {
    // eprintln!("Number of article: {}", zim.article_count());

    eprintln!("Extracting file {} to {}", config.input, config.output);

    let zim = Zim::new(config.input);
    // if an error, get the error and bubble it up
    if zim.is_err() {
        return Err(Box::new(WikiExtractorError::Zim(
            zim.err().unwrap().to_string(),
        )));
    }
    let zim = zim.unwrap();

    // generate an index of clusters to directory entries
    let (num_articles, clusters) = cluster_to_entries_index(&zim);

    let sw = std::time::Instant::now();
    let pb = ProgressBar::new(num_articles);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
        .progress_chars("#>-"));

    let root_output = Path::new(&config.output);
    let db_output_path = Path::new(&config.output).join("db");

    ensure_dir(root_output);

    // create a sync channel to send the directory entry to the thread
    let (tx, rx) = mpsc::sync_channel::<Option<HerdFile>>(100);

    pb.set_message("Writing entries to disk");

    // create a thread to handle the directory entries and log them to a database
    let handle = std::thread::spawn(move || {
        let db = sled::open(db_output_path).expect("Failed to open database");
        let mut batch = sled::Batch::default();

        // read current number of pending entries
        let mut num_pending: u64 = match db
            .get(bincode::serialize(&HerdStatus::Pending).unwrap())
            .unwrap()
        {
            Some(b) => bincode::deserialize(&b).unwrap(),
            None => 0,
        };

        let mut num_failed = 0;
        let mut count = 0;
        for entry in rx {
            match entry {
                Some(entry) => {
                    // let the key be f_ concatenated with entry.path
                    let mut key = "f_".as_bytes().to_vec();
                    key.extend_from_slice(entry.file_path.as_bytes());
                    let value = bincode::serialize(&entry).expect("Failed to serialize entry");
                    batch.insert(key, value);

                    count += 1;
                    if count % 1000 == 0 {
                        batch.insert(
                            bincode::serialize(&HerdStatus::Pending).unwrap(),
                            bincode::serialize(&(num_pending + count)).unwrap(),
                        );
                        db.apply_batch(batch).expect("Failed to apply batch");
                        batch = sled::Batch::default();
                    }
                }
                None => {
                    num_failed += 1;
                }
            }

            pb.inc(1);
        }

        num_pending += count;
        batch.insert(
            bincode::serialize(&HerdStatus::Pending).unwrap(),
            bincode::serialize(&num_pending).unwrap(),
        );
        db.apply_batch(batch).expect("Failed to apply batch");

        pb.finish_with_message(format!(
            "Extraction done in {}s with {} failures",
            sw.elapsed().as_secs(),
            num_failed
        ));
    });

    // parallel processing of the entries

    // unlike the extractor in `zim` crate, we want to extract by cluster
    // we do this because the cluster struct caches the decompressed
    clusters
        .par_iter()
        .filter(|(_, entries)| !entries.is_empty())
        .filter(|(cluster_index, _)| zim.get_cluster(**cluster_index).is_ok())
        .for_each(|cluster| {
            let (cluster, article_blob_map) = cluster;

            // get the cluster
            let cluster_object = zim.get_cluster(*cluster).unwrap();

            // iterate through the entries
            article_blob_map
                .iter()
                .map(|(article_id, blob_id)| {
                    (
                        article_id,
                        zim.get_by_url_index(*article_id).unwrap(),
                        blob_id,
                    )
                })
                .filter(|(_, entry, _)| matches!(entry.mime_type, MimeType::Type(_)))
                .filter(|(_, entry, _)| entry.mime_type != MimeType::Type("application/octet-stream+xapian".to_string()))
                .for_each(|(article_id, entry, blob_id)| {
                    // compute the path as a combination of the root output and the article_id
                    let mut path = root_output.to_path_buf();
                    path.push(article_id.to_string());

                    let written = match cluster_object.get_blob(*blob_id) {
                        Ok(blob) => {
                            if let MimeType::Type(mime) = &entry.mime_type {
                                if mime == "text/html" {
                                    enhance_html(&path, &blob)
                                } else {
                                    safe_write(&path, blob, 1)
                                }
                            } else {
                                safe_write(&path, blob, 1)
                            }
                        }
                        Err(e) => Err(Box::new(WikiExtractorError::Io(e.to_string()))
                            as Box<dyn std::error::Error + Send>),
                    };

                    match written {
                        Ok(_) => {
                            // process the metadata
                            let mut metadata: BTreeMap<String, String> = BTreeMap::new();

                            // if it's text/html, override this as we've gzipped it

                            if let MimeType::Type(mime) = &entry.mime_type {
                                if mime == "text/html" {
                                    metadata.insert(
                                        "Content-Type".to_string(),
                                        "application/octet-stream".to_string(),
                                    );
                                } else {
                                    metadata.insert("Content-Type".to_string(), mime.to_string());
                                }
                            }

                            let url = make_url(entry.namespace, &entry.url);

                            metadata.insert("Filename".to_string(), url.to_string());

                            // send the entry to the thread
                            tx.send(Some(HerdFile {
                                file_path: path.to_str().unwrap().to_owned(),
                                prefix: url,
                                status: HerdStatus::Pending,
                                tag: None,
                                reference: None,
                                mantaray_reference: None,
                                metadata,
                            }))
                            .expect("Failed to send entry");
                        }
                        Err(e) => {
                            // print failure message of cluster, blob and article as well as path
                            eprintln!(
                                "Failed to write {} from cluster {} blob {} article {} error {}",
                                path.display(),
                                cluster,
                                blob_id,
                                entry.url,
                                e
                            );
                            tx.send(None).unwrap();
                        }
                    }
                });
        });

    // close the channel
    drop(tx);

    // wait for the thread to finish
    handle.join().unwrap();

    Ok(())
}

fn safe_write<T: AsRef<[u8]>>(path: &Path, data: T, count: usize) -> Result<()> {
    // let display = path.display();
    let contain_path = path.parent().unwrap();

    ensure_dir(contain_path);

    match File::create(&path) {
        Err(why) => {
            if count < 3 {
                safe_write(path, data, count + 1)
            } else {
                Err(Box::new(why))
            }
        }
        Ok(file) => {
            let mut writer = BufWriter::new(&file);

            let success = writer.write_all(data.as_ref());

            if success.is_err() {
                Err(Box::new(success.err().unwrap()))
            } else {
                Ok(())
            }
        }
    }
}

fn enhance_html(path: &Path, blob: &[u8]) -> Result<()> {
    // before we do any processing, declare the zlib compressor for streaming
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());

    let contain_path = path.parent().unwrap();
    ensure_dir(contain_path);

    // strip out some html
    let root = parse_html()
        .one(String::from_utf8(<&[u8]>::clone(&blob).to_vec()).unwrap())
        .select_first("body")
        .unwrap();

    // process all `source`, `img`, and `track` src attributes
    root.as_node()
        .select("img,track,source")
        .unwrap()
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
        .select("script")
        .unwrap()
        .borrow_mut()
        .for_each(|mut e| {
            let rc = &e.borrow_mut().attributes;
            let attributes = &mut (*rc.borrow_mut());

            let mut src = "static/".to_string();
            src.push_str(&attributes.get_mut("src").unwrap()[5..]);

            attributes.insert("src", src);
        });

    let res = root.as_node().serialize(&mut enc);

    // if res is an error, return the error in a Box
    if let Err(e) = res {
        return Err(Box::new(WikiExtractorError::Compress(e.to_string())));
    }

    let output = enc.finish();

    // if output is an error, return the error in a Box
    if let Err(e) = output {
        return Err(Box::new(WikiExtractorError::Compress(e.to_string())));
    }

    // unwrap the output
    let output = output.unwrap();

    safe_write(path, output, 1)
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

fn make_url(namespace: Namespace, url: &str) -> String {
    // let mut s = String::new();
    // s.push(namespace as u8 as char);
    let mut path = match namespace {
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

    // append a separator and the URL
    path.push('/');
    path.push_str(url);

    path
}

fn cluster_to_entries_index(zim: &Zim) -> (u64, HashMap<u32, HashMap<u32, u32>>) {
    let mut cluster_index = HashMap::new();
    let num_articles = zim.article_count() as u64;
    let mut num_redirects = 0;

    let sw = std::time::Instant::now();

    let pb = ProgressBar::new(zim.article_count() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
        .progress_chars("#>-"));

    pb.set_message("Generating cluster index");

    // monitor performance of the for loop
    let start = std::time::Instant::now();
    for (article_id, entry) in zim.iterate_by_urls().enumerate() {
        if let Some(target) = entry.target {
            match target {
                Target::Cluster(cluster, blob) => {
                    cluster_index
                        .entry(cluster)
                        .and_modify(|v: &mut HashMap<u32, u32>| {
                            v.insert(article_id.try_into().unwrap(), blob);
                        })
                        .or_insert_with(|| {
                            let mut map = HashMap::new();
                            map.insert(article_id as u32, blob);
                            map
                        });
                }
                Target::Redirect(_) => num_redirects += 1,
            }
        }

        pb.inc(1);
    }

    pb.finish_with_message(format!(
        "Extraction done in {}s, finding {} redirects",
        sw.elapsed().as_secs(),
        num_redirects
    ));
    println!(
        "Processed {} articles in {}ms",
        num_articles,
        start.elapsed().as_millis()
    );
    println!("Found {} redirects", num_redirects);

    (num_articles - num_redirects, cluster_index)
}
