use anyhow::Result;
use aws_sdk_s3::Client;
use clap::Parser;
use s3_archiver::{Compression, S3Object};
use std::io::{BufRead, BufReader};

#[derive(Parser)]
struct Args {
    /// S3 output location `s3://{bucket}/{key}`
    output_location: url::Url,
    /// Prefix to remove from input keys
    prefix_strip: Option<String>,
    //Compression to use for the files
    #[clap(value_enum)]
    compression: Compression,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = aws_config::load_from_env().await;
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
        &client,
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
