use anyhow::{ensure, Context, Result};
use async_zip::ZipEntry;
use aws_sdk_s3::Client;
use bytesize::ByteSize;
use clap::{Parser, Subcommand, ValueEnum};
use cobalt_aws::config;
use cobalt_aws::s3::S3Object;
use cobalt_s3_archiver as s3_archiver;
use futures::prelude::*;
use s3_archiver::{Archiver, Compression, ZipEntries};
use serde::Serialize;
use std::io::{BufRead, BufReader};
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Parser, Debug, PartialEq, Clone)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
enum Command {
    ///Create an ZIP archive in S3 from source files in S3.
    Archive(ArchiveCommand),
    ///Validate a ZIP archive matches the given manifest.
    ValidateArchive(ValidateCommand),
    ///Validate the calculated crc32 of files in the manifest match those recorded the manifest.
    ValidateManifest(ValidateManifestCommand),
    ///Extract compressed files from archive.
    Unarchive(UnarchiveCommand),
    ///List archive files
    List(ListCommand),
}

#[derive(Parser, Debug, PartialEq, Clone)]
struct ArchiveCommand {
    /// S3 output location `s3://{bucket}/{key}`
    output_location: S3Object,
    #[clap(
        value_enum,
        default_value = "stored",
        short = 'c',
        help = "Compression to use"
    )]
    compression: Compression,
    /// Prefix to remove from input keys
    #[clap(short = 'p', long = "prefix-strip")]
    prefix_strip: Option<String>,
    /// Part size to use in multipart upload.
    /// Accepts human readable bytes e.g. K, KiB.
    /// Min 5MiB, Max 5GiB.
    #[clap(short = 's', long = "part-size", default_value = "5MiB")]
    part_size: ByteSize,
    /// How many src object requests to eagerly fetch.
    #[clap(short = 'f', long = "src-fetch-concurrency", default_value_t = 2)]
    src_fetch_concurrency: usize,
    /// The location to write the manifest object. A list of src files and their crc32 values.
    #[clap(short = 'm', long = "manifest-object")]
    manifest_object: Option<S3Object>,
    /// Generate a manifest object at "{output_location}.manifest.jsonl"
    #[clap(
        short = 'g',
        long = "generate-manifest-name",
        conflicts_with = "manifest_object"
    )]
    auto_manifest: bool,
    /// Keep memory usage constant by streaming the input files
    /// but generate data descriptors for each file.
    /// Some tools can not read ZIP files using data descriptors.
    #[clap(short = 'd', long = "data-descriptors")]
    data_descriptors: bool,
}

#[derive(Parser, Debug, PartialEq, Clone)]
struct UnarchiveCommand {
    /// S3 input ZIP object `s3://{bucket}/{key}`
    input_location: S3Object,
    /// S3 output location `s3://{bucket}/{key}`
    output_location: S3Object,
    /// Part size to use in multipart upload.
    /// Accepts human readable bytes e.g. K, KiB.
    /// Min 5MiB, Max 5GiB.
    #[clap(short = 's', long = "part-size", default_value = "5MiB")]
    part_size: ByteSize,
}

#[derive(Parser, Debug, PartialEq, Clone)]
struct ListCommand {
    /// S3 input ZIP object `s3://{bucket}/{key}`
    input_location: S3Object,
    /// Supress printing of table headers
    #[clap(short = 'q')]
    quiet: bool,
    /// Print extra entry information.
    #[clap(short = 'v')]
    verbose: bool,
    #[clap(short = 'j',
            conflicts_with_all = ["verbose", "quiet"]
           )]
    json: bool,
}

#[derive(Parser, Debug, PartialEq, Clone)]
struct ValidateCommand {
    /// S3 manifest location `s3://{bucket}/{key}`
    manifest_file: S3Object,
    /// S3 ZIP file location `s3://{bucket}/{key}`
    zip_file: S3Object,
    #[clap(
        value_enum,
        default_value = "bytes",
        short = 'v',
        help = "Type of validation to apply to the CRC32"
    )]
    crc32_validation_type: CRC32ValidationType,
}

#[derive(Parser, Debug, PartialEq, Clone)]
struct ValidateManifestCommand {
    /// S3 manifest location `s3://{bucket}/{key}`
    #[clap(
        help = "S3 manifest location `s3://{bucket}/{key}`",
        required_unless_present = "stdin"
    )]
    manifest_object: Option<S3Object>,
    #[clap(
        default_value_t = 1,
        conflicts_with = "manifest_object",
        short = 's',
        help = "How many manifests to validate concurrently"
    )]
    manifest_concurrency: usize,
    #[clap(
        default_value_t = 1,
        short = 'f',
        help = "How many concurrent connections to open to S3"
    )]
    fetch_concurrency: usize,
    #[clap(
        default_value_t = 1,
        short = 'v',
        help = "How many concurrent validations to run"
    )]
    validate_concurrency: usize,
    #[clap(
        short = 'c',
        help = "Read a list of manifest files to validate from stdin",
        conflicts_with = "manifest_object"
    )]
    stdin: bool,
}
#[derive(Debug, Clone, ValueEnum, Copy, PartialEq, Eq)]
enum CRC32ValidationType {
    /// Calculate the CRC32 again from the bytes in the ZIP.
    Bytes,
    /// Read teh CRC32 from the ZIP central directory.
    CentralDirectory,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = config::load_from_env().await?;
    let client = Client::new(&config);

    // Start configuring a `fmt` subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .json()
        .init();

    match args.command {
        Command::Archive(cmd) => {
            create_zip_from_read(&client, &mut BufReader::new(std::io::stdin()), &cmd).await
        }
        Command::ValidateArchive(cmd) => match cmd.crc32_validation_type {
            CRC32ValidationType::Bytes => {
                s3_archiver::validate_zip_entry_bytes(&client, &cmd.manifest_file, &cmd.zip_file)
                    .await?;
                println!(
                    "The input archive {:?} bytes matched the manifest {:?}",
                    &cmd.zip_file, &cmd.manifest_file
                );
                Ok(())
            }
            CRC32ValidationType::CentralDirectory => {
                s3_archiver::validate_zip_central_dir(&client, &cmd.manifest_file, &cmd.zip_file)
                    .await?;
                println!(
                    "The input archive {:?} central directory matched the manifest {:?}",
                    &cmd.zip_file, &cmd.manifest_file
                );
                Ok(())
            }
        },
        Command::ValidateManifest(cmd) if cmd.stdin => {
            validate_manifest_files_from_read(&client, &mut BufReader::new(std::io::stdin()), &cmd)
                .await?;
            println!( "All input manfest file crc32 values matched those calculated from the source files");
            Ok(())
        }
        Command::ValidateManifest(cmd) => {
            let manifest_object = cmd
                .manifest_object
                .context("Manifest object requied if not reading from stdin")?;
            s3_archiver::validate_manifest_file(
                &client,
                &manifest_object,
                cmd.fetch_concurrency,
                cmd.validate_concurrency,
            )
            .await?;
            println!(
                    "All manfest entry crc32 values in ${:?} matched those calculated from the source files",
                    &manifest_object
                );
            Ok(())
        }
        Command::Unarchive(cmd) => {
            s3_archiver::unarchive_all(
                &client,
                &cmd.input_location,
                &cmd.output_location,
                usize::try_from(cmd.part_size.as_u64())?,
            )
            .await
        }
        Command::List(cmd) if cmd.json => {
            let entries = s3_archiver::ZipEntries::new(&client, &cmd.input_location).await?;
            print_entries_json(&entries)
        }
        Command::List(cmd) => {
            let entries = s3_archiver::ZipEntries::new(&client, &cmd.input_location).await?;
            if cmd.verbose {
                print_entries_verbose(&Url::try_from(&cmd.input_location)?, &entries, cmd.quiet);
            } else {
                print_entries(&Url::try_from(&cmd.input_location)?, &entries, cmd.quiet);
            }
            Ok(())
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct ListZipEntry {
    compression: String,
    compressed_size: u32,
    uncompressed_size: u32,
    compression_percentage: u8,
    last_modification_date: String,
    last_modification_time: String,
    filename: String,
    crc32: String,
}

impl From<&ZipEntry> for ListZipEntry {
    fn from(entry: &ZipEntry) -> Self {
        let compression_percent = if entry.uncompressed_size() == 0 {
            0
        } else {
            100 - ((entry.uncompressed_size() as f32 / entry.compressed_size() as f32) * 100_f32)
                as u8
        };
        ListZipEntry {
            compression: format!("{:?}", entry.compression()),
            compressed_size: entry.compressed_size(),
            uncompressed_size: entry.uncompressed_size(),
            compression_percentage: compression_percent,
            last_modification_date: entry
                .last_modification_date()
                .format("%Y-%m-%d")
                .to_string(),
            last_modification_time: entry.last_modification_date().format("%H-%M").to_string(),
            crc32: format!("{:08x}", entry.crc32()),
            filename: entry.filename().to_string(),
        }
    }
}

/// Print the files in the archive.
/// Prints out the:
/// * Uncompressed size.
/// * Last modified date.
/// * Last modified time.
/// * File name
///
/// Quiet flag will only print out the entry data without headers.
fn print_entries(src: &Url, entries: &ZipEntries, quiet: bool) {
    if !quiet {
        println!("Archive: {:}", src);
        println!(
            "{:^9} {:^10} {:^5}  {:^4}",
            "Length", "Date", "Time", "Name"
        );
        println!("{:-<9} {:-<10} {:-<5}  {:-<4}", "", "", "", "");
    }
    let mut total_size: u32 = 0;
    let mut file_count: u32 = 0;
    for entry in entries {
        let entry = ListZipEntry::from(entry);
        println!(
            "{:>9} {:>10} {:>5}  {}",
            entry.uncompressed_size,
            entry.last_modification_date,
            entry.last_modification_time,
            entry.filename
        );
        total_size += entry.uncompressed_size;
        file_count += 1;
    }
    if !quiet {
        println!("{:-<9} {:10} {:5}  {:-<7}", "", " ", " ", "");
        println!(
            "{:>9} {:10} {:5}  {:} files",
            total_size, "", "", file_count
        );
    }
}

/// Print the files in the archive.
/// Prints out the same information as `print_entries` and additionally:
/// * Compression method.
/// * Compressed size.
/// * Compression percentage.
/// * CRC-32
///
/// Quiet flag will only print out the entry data without headers.
fn print_entries_verbose(src: &Url, entries: &ZipEntries, quiet: bool) {
    if !quiet {
        println!("Archive: {:}", src);
        println!(
            "{:^9} {:^6} {:^7} {:^4} {:^10} {:^5} {:^8}  {:^4}",
            "Length", "Method", "Size", "Cmpr", "Date", "Time", "CRC-32", "Name"
        );
        println!(
            "{:-<9} {:-<6} {:-<7} {:-<4} {:-<10} {:-<5} {:-<8}  {:-<4}",
            "", "", "", "", "", "", "", ""
        );
    }
    let mut total_size: u32 = 0;
    let mut total_length: u32 = 0;
    let mut file_count: u32 = 0;
    for entry in entries {
        let entry: ListZipEntry = ListZipEntry::from(entry);
        println!(
            "{:>9} {:>6?} {:>7} {:>3}% {:>10} {:>5} {:08} {}",
            entry.uncompressed_size,
            entry.compression,
            entry.compressed_size,
            entry.compression_percentage,
            entry.last_modification_date,
            entry.last_modification_time,
            entry.crc32,
            entry.filename
        );
        total_length += entry.uncompressed_size;
        total_size += entry.compressed_size;
        file_count += 1;
    }
    if !quiet {
        let total_compression = 100 - ((total_size as f32 / total_length as f32) * 100_f32) as u32;
        println!(
            "{:-<9} {:6} {:-<7} {:-<4} {:10} {:5} {:8}  {:-<7}",
            "", "", "", "", "", "", "", ""
        );
        println!(
            "{:>9} {:6} {:>7} {:>3}% {:10} {:5} {:8}  {:} files",
            total_length, "", total_size, total_compression, "", "", "", file_count
        );
    }
}

pub fn print_entries_json(entries: &ZipEntries) -> Result<()> {
    for entry in entries {
        println!("{}", serde_json::to_string(&ListZipEntry::from(entry))?)
    }
    Ok(())
}

async fn validate_manifest_files_from_read(
    client: &Client,
    input: &mut impl BufRead,
    args: &ValidateManifestCommand,
) -> Result<()> {
    let objects = input
        .lines()
        .map(|x| x.map_err(anyhow::Error::from))
        .map(|x| x.and_then(S3Object::try_from));

    futures::stream::iter(objects)
        .map_ok(|m| async move {
            s3_archiver::validate_manifest_file(
                client,
                &m,
                args.fetch_concurrency,
                args.validate_concurrency,
            )
            .await
        })
        .try_buffered(1)
        .try_collect()
        .await
}

async fn create_zip_from_read(
    client: &Client,
    input: &mut impl BufRead,
    args: &ArchiveCommand,
) -> Result<()> {
    ensure!(
        args.manifest_object
            .as_ref()
            .filter(|o| *o == &args.output_location)
            .is_none(),
        "output_location and manifest_object must not be the same"
    );

    let objects = input
        .lines()
        .map(|x| x.map_err(anyhow::Error::from))
        .map(|x| x.and_then(S3Object::try_from));

    let archiver = Archiver::builder()
        .prefix_strip(args.prefix_strip.as_deref())
        .compression(args.compression)
        .part_size(usize::try_from(args.part_size.as_u64())?)
        .src_fetch_buffer(args.src_fetch_concurrency)
        .data_descriptors(args.data_descriptors)
        .build();

    let manifest_file = if args.auto_manifest {
        Some(S3Object::new(
            &args.output_location.bucket,
            args.output_location.key.to_owned() + ".manifest.jsonl",
        ))
    } else {
        args.manifest_object.clone()
    };

    archiver
        .create_zip(
            client,
            objects,
            &args.output_location,
            manifest_file.as_ref(),
        )
        .await
}

#[cfg(test)]
mod test {
    use clap::error::ErrorKind;

    use super::*;
    #[test]
    fn test_arg_parser_happy() {
        let result = Args::try_parse_from(vec!["prog", "archive", "s3://output/zip"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_arg_parser_manifest_and_generate() {
        let result = Args::try_parse_from(vec![
            "prog",
            "archive",
            "s3://output/zip",
            "-m",
            "s3://output/manifest",
            "-g",
        ]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn test_arg_parser_invalid_s3_url() {
        let result = Args::try_parse_from(vec!["prog", "archive", "output/zip"]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::ValueValidation);
    }
}
