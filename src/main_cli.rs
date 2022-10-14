use anyhow::{Result, ensure};
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

    #[clap(short = 'm', long = "manifest_object")]
    manifest_object: Option<S3Object>,
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
    
    ensure!(args.manifest_object.as_ref().filter(|o| *o == &args.output_location).is_none(), 
            "output_location and manifest_object must not be the same");

    let objects = input
        .lines()
        .map(|x| x.map_err(anyhow::Error::from))
        .map(|x| x.and_then(S3Object::try_from));

    s3_archiver::create_zip(
        client,
        objects,
        args.prefix_strip.as_deref(),
        args.compression,
        usize::try_from(args.part_size.as_u64())?,
        args.src_fetch_concurrency,
        &args.output_location.clone().try_into()?,
        args.manifest_object.clone()
    )
    .await
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn write_tests_here() -> Result<()> {
        Ok(())
    }
}
