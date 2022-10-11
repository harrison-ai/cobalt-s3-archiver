pub mod localstack {

    use std::collections::HashMap;
    use testcontainers::{core::WaitFor, Image};

    const NAME: &str = "localstack/localstack";
    const TAG: &str = "latest";

    #[derive(Debug, Default)]
    pub struct LocalStack {
        env_vars: HashMap<String, String>,
    }

    impl LocalStack {
        pub fn new(env_vars: HashMap<String, String>) -> Self {
            LocalStack { env_vars }
        }
    }

    impl Image for LocalStack {
        type Args = ();

        fn name(&self) -> String {
            NAME.to_owned()
        }

        fn tag(&self) -> String {
            TAG.to_owned()
        }

        fn ready_conditions(&self) -> Vec<WaitFor> {
            vec![WaitFor::message_on_stdout("Ready")]
        }

        fn expose_ports(&self) -> Vec<u16> {
            let localstack_gateway = 4566_u16..4567;
            let external_service_ports = 4510_u16..=4559;
            localstack_gateway.chain(external_service_ports).collect()
        }

        fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
            Box::new(self.env_vars.iter())
        }
    }
}

pub mod aws {

    use super::localstack::LocalStack;
    use aws_config::SdkConfig;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::{Credentials, Endpoint};
    #[cfg(feature = "test_containers")]
    use std::collections::HashMap;
    #[cfg(feature = "test_containers")]
    use testcontainers::clients::Cli;
    use testcontainers::Container;

    pub async fn localstack_sdkconfig(host: &str, port: u16) -> SdkConfig {
        aws_config::from_env()
            .endpoint_resolver(Endpoint::immutable(
                format!("http://{host}:{port}")
                    .parse()
                    .expect("Invalid URI"),
            ))
            .region("us-east-1")
            .credentials_provider(Credentials::new("test", "test", None, None, ""))
            .load()
            .await
    }

    pub async fn s3_client(host: &str, endpoint_port: u16) -> Client {
        let config = localstack_sdkconfig(host, endpoint_port).await;
        Client::new(&config)
    }

    pub fn localstack_host() -> String {
        std::env::var("LOCALSTACK_HOSTNAME").unwrap_or_else(|_| "localhost".into())
    }

    pub enum S3TestClient {
        #[cfg(feature = "test_containers")]
        TestContainer(Cli),
        DockerCompose,
    }

    impl S3TestClient {
        pub async fn client(&self) -> (Option<Box<Container<LocalStack>>>, Client) {
            match self {
                #[cfg(feature = "test_containers")]
                Self::TestContainer(cli) => {
                    let stack = Box::new(cli.run(LocalStack::new(HashMap::from([(
                        "SERVICES".into(),
                        "s3".into(),
                    )]))));
                    let endpoint_port = stack.get_host_port_ipv4(4566);
                    let s3_client = s3_client("localhost", endpoint_port).await;

                    (Some(stack), s3_client)
                }
                Self::DockerCompose => {
                    let host = localstack_host();
                    (None, s3_client(&host, 4566).await)
                }
            }
        }
    }

    impl Default for S3TestClient {
        #[cfg(feature = "test_containers")]
        fn default() -> Self {
            S3TestClient::TestContainer(Cli::default())
        }
        #[cfg(not(feature = "test_containers"))]
        fn default() -> Self {
            S3TestClient::DockerCompose
        }
    }
}

pub mod fixtures {

    use anyhow::Result;
    use async_zip::read::mem::ZipFileReader;
    use async_zip::read::ZipEntryReader;
    use aws_sdk_s3::error::HeadObjectError;
    use aws_sdk_s3::error::HeadObjectErrorKind;
    use aws_sdk_s3::types::ByteStream;
    use aws_sdk_s3::types::SdkError;
    use aws_sdk_s3::Client;
    use bytesize::MIB;
    use crc::{Crc, CRC_32_ISCSI};
    use rand::distributions::{Alphanumeric, DistString};
    use rand::Rng;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use s3_archiver::{Compression, S3Object};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

    pub async fn create_bucket(client: &Client, bucket: &str) -> Result<()> {
        client.create_bucket().bucket(bucket).send().await?;
        Ok(())
    }

    pub async fn check_object_exists(client: &Client, obj: &S3Object) -> Result<bool> {
        let result = client
            .head_object()
            .bucket(&obj.bucket)
            .key(&obj.key)
            .send()
            .await
            .map(|_| true);
        match result {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError {
                err:
                    HeadObjectError {
                        kind: HeadObjectErrorKind::NotFound(_),
                        ..
                    },
                ..
            }) => Ok(false),
            err => err.map_err(anyhow::Error::from),
        }
    }

    pub async fn fetch_bytes(client: &Client, obj: &S3Object) -> Result<Vec<u8>> {
        Ok(client
            .get_object()
            .bucket(&obj.bucket)
            .key(&obj.key)
            .send()
            .await
            .expect("Expceted dst key to exist")
            .body
            .collect()
            .await
            .expect("Expected a body")
            .into_bytes()
            .into())
    }

    pub async fn create_random_file(client: &Client, obj: &S3Object, size: usize) -> Result<()> {
        let data: Vec<_> = (0..size).map(|_| rand::random::<u8>()).collect();
        Ok(client
            .put_object()
            .bucket(&obj.bucket)
            .key(&obj.key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map(|_| ())?)
    }

    pub async fn create_random_files<'a, I>(client: &Client, size: usize, keys: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a S3Object> + 'a,
    {
        for obj in keys {
            create_random_file(client, obj, size).await?;
        }
        Ok(())
    }

    pub fn s3_object_from_keys<'a, I>(
        bucket: &'a str,
        keys: I,
    ) -> impl Iterator<Item = S3Object> + 'a
    where
        I: IntoIterator + 'a,
        I::Item: AsRef<str>,
    {
        keys.into_iter().map(move |k| S3Object::new(bucket, k))
    }

    pub async fn validate_zip<'a, I>(
        client: &Client,
        zip_obj: &S3Object,
        prefix_to_strip: Option<&'a str>,
        file_names: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = &'a S3Object> + 'a,
    {
        let bytes = fetch_bytes(client, zip_obj).await?;

        //It was not possible to get this to work using the streaming ZipFileReader
        //This issue show similar issues https://github.com/Majored/rs-async-zip/issues/29
        let zip = &mut ZipFileReader::new(&bytes).await?;
        let num_entries = zip.entries().len();
        for (index, src) in (0..num_entries).zip(file_names) {
            let src_crc = object_crc32(client, src).await?;
            let entry_reader = zip.entry_reader(index).await?;
            let entry_name = entry_reader.entry().filename().to_owned();
            let dst_crc = zip_entry_crc32(entry_reader).await?;
            assert_eq!(src_crc, dst_crc);
            assert_eq!(
                entry_name,
                src.key
                    .trim_start_matches(prefix_to_strip.unwrap_or_default())
            );
        }
        Ok(())
    }

    pub async fn object_crc32(client: &Client, obj: &S3Object) -> Result<u32> {
        Ok(CASTAGNOLI.checksum(&fetch_bytes(client, obj).await?))
    }

    pub async fn zip_entry_crc32<R>(entry_reader: ZipEntryReader<'_, R>) -> Result<u32>
    where
        R: tokio::io::AsyncRead + core::marker::Unpin,
    {
        let file_bytes = entry_reader.read_to_end_crc().await?;
        Ok(CASTAGNOLI.checksum(&file_bytes))
    }

    pub struct CheckZipArgs<'a> {
        pub dst_obj: S3Object,
        pub prefix_to_strip: Option<&'a str>,
        pub src_bucket: String,
        pub src_keys: Vec<String>,
        pub file_size: usize,
        pub compression: Compression,
    }

    impl<'a> CheckZipArgs<'a> {
        pub fn new(
            dst_obj: S3Object,
            prefix_to_strip: Option<&'a str>,
            src_bucket: &str,
            src_keys: Vec<String>,
            file_size: usize,
            compression: Compression,
        ) -> Self {
            CheckZipArgs {
                dst_obj,
                prefix_to_strip,
                src_bucket: src_bucket.into(),
                src_keys,
                file_size,
                compression,
            }
        }

        pub fn src_objs(&'a self) -> impl Iterator<Item = S3Object> + 'a {
            s3_object_from_keys(&self.src_bucket, &self.src_keys)
        }

        pub fn seeded_args<R: Rng>(
            rng: &mut R,
            src_file_count: usize,
            src_dirs: Option<&[&str]>,
        ) -> Self {
            let dst_file = gen_random_file_name(rng);
            let dst_obj = S3Object::new("dst-bucket", dst_file);
            let prefix_to_strip = Option::<&str>::None;
            let src_bucket = "src-bucket";
            let src_keys = match src_dirs {
                Some(dirs) => gen_random_file_names(rng, src_file_count)
                    .into_iter()
                    .zip(dirs.iter().cycle())
                    .map(|(f, d)| format!("{d}/{f}"))
                    .collect(),
                None => gen_random_file_names(rng, src_file_count),
            };
            //gen_random_file_names(rng, src_file_count);
            let file_size = MIB as usize;
            let compression = Compression::Stored;
            CheckZipArgs::new(
                dst_obj,
                prefix_to_strip,
                src_bucket,
                src_keys,
                file_size,
                compression,
            )
        }
    }

    impl<'a> Default for CheckZipArgs<'a> {
        fn default() -> Self {
            let dst_obj = S3Object::new("dst-bucket", "dst_check_file.zip");
            let prefix_to_strip = Option::<&str>::None;
            let src_bucket = "src-bucket";
            let src_files = vec!["src-file_1.txt", "src-file_2.txt"]
                .into_iter()
                .map(String::from)
                .collect();
            let file_size = MIB as usize;
            let compression = Compression::Stored;

            CheckZipArgs::new(
                dst_obj,
                prefix_to_strip,
                src_bucket,
                src_files,
                file_size,
                compression,
            )
        }
    }

    pub async fn create_src_files(
        client: &Client,
        src_bucket: &str,
        src_objs: &Vec<S3Object>,
        file_size: usize,
    ) -> Result<()> {
        create_bucket(client, src_bucket).await?;
        create_random_files(client, file_size, src_objs).await?;
        Ok(())
    }

    pub async fn create_and_validate_zip<'a>(
        client: &Client,
        args: &'a CheckZipArgs<'a>,
    ) -> Result<()> {
        create_src_files(
            client,
            &args.src_bucket,
            &args.src_objs().collect(),
            args.file_size,
        )
        .await?;
        create_bucket(client, &args.dst_obj.bucket).await?;
        s3_archiver::create_zip(
            client,
            args.src_objs().map(Ok),
            args.prefix_to_strip,
            args.compression,
            (5 * bytesize::MIB).try_into()?,
            &args.dst_obj,
        )
        .await?;

        let files_to_validate: Vec<_> = args.src_objs().collect();
        validate_zip(
            client,
            &args.dst_obj,
            args.prefix_to_strip,
            files_to_validate.iter(),
        )
        .await
    }

    pub fn seeded_rng<H: Hash + ?Sized>(seed: &H) -> impl Rng {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        ChaCha8Rng::seed_from_u64(hasher.finish())
    }

    pub fn gen_random_file_name<R: Rng>(rng: &mut R) -> String {
        Alphanumeric.sample_string(rng, 16)
    }

    pub fn gen_random_file_names<R: Rng>(rng: &mut R, count: usize) -> Vec<String> {
        //let mut rng = ChaCha8Rng::seed_from_u64(hasher.finish());
        (0..count).map(|_| gen_random_file_name(rng)).collect()
    }

    pub fn str_vec_to_owned(v: &[&str]) -> Vec<String> {
        v.iter().map(|&s| s.into()).collect()
    }
}
