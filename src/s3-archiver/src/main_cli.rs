use anyhow::Result;
use aws_sdk_s3::Client;
use clap::Parser;
use s3_archiver::{Compression, S3Object};
use std::io::{BufRead, BufReader};
use cobalt_aws::config;

#[derive(Parser)]
struct Args {
    /// S3 output location `s3://{bucket}/{key}`
    output_location: url::Url,
    #[clap(value_enum, 
           default_value="stored",
           short='c', help="Compression to use")]
    compression: Compression,
    /// Prefix to remove from input keys
    #[clap(short='p')]
    prefix_strip: Option<String>,
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
    let objects = input
        .lines()
        .map(|x| x.map_err(anyhow::Error::from))
        .map(|x| x.and_then(S3Object::try_from));

    s3_archiver::create_zip(
        client,
        objects,
        args.prefix_strip.as_deref(),
        args.compression,
        &args.output_location.clone().try_into()?,
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
