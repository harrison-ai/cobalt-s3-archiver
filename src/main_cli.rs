use anyhow::{ensure, Result};
use aws_sdk_s3::Client;
use bytesize::ByteSize;
use clap::Parser;
use cobalt_aws::config;
use s3_archiver::{Compression, S3Object};
use std::io::{BufRead, BufReader};

#[derive(Parser, Debug, PartialEq, Eq)]
struct Args {
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = config::load_from_env().await?;
    let client = Client::new(&config);

    create_zip_from_read(&client, &mut BufReader::new(std::io::stdin()), &args).await
}

async fn create_zip_from_read(
    client: &Client,
    input: &mut impl BufRead,
    args: &Args,
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

    let archiver = s3_archiver::Archiver::builder()
        .prefix_strip(args.prefix_strip.as_deref())
        .compression(args.compression)
        .part_size(usize::try_from(args.part_size.as_u64())?)
        .src_fetch_buffer(args.src_fetch_concurrency)
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
        let result = Args::try_parse_from(vec!["prog", "s3://output/zip"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_arg_parser_manifest_and_generate() {
        let result = Args::try_parse_from(vec![
            "prog",
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
        let result = Args::try_parse_from(vec!["prog", "output/zip"]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::ValueValidation);
    }
}
