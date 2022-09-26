pub mod localstack {

    use testcontainers::{core::WaitFor, Image};
    const NAME: &str = "localstack/localstack";
    const TAG: &str = "latest";

    #[derive(Debug, Default)]
    pub struct LocalStack;

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
    }
}

pub mod aws {

    use super::localstack::LocalStack;
    use aws_config::SdkConfig;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::{Credentials, Endpoint};
    use testcontainers::{clients::Cli, Container};

    pub async fn localstack_sdkconfig(port: u16) -> SdkConfig {
        aws_config::from_env()
            .endpoint_resolver(Endpoint::immutable(
                format!("http://localhost:{port}")
                    .parse()
                    .expect("Invalid URI"),
            ))
            .region("us-east-1")
            .credentials_provider(Credentials::new("test", "test", None, None, ""))
            .load()
            .await
    }

    pub async fn s3_client(endpoint_port: u16) -> Client {
        let config = localstack_sdkconfig(endpoint_port).await;
        Client::new(&config)
    }

    pub enum S3TestClient {
        TestContainer(Cli),
        DockerCompose(Client),
    }

    impl S3TestClient {
        pub async fn client(&self) -> (Option<Box<Container<LocalStack>>>, Client) {
            match self {
                Self::TestContainer(cli) => {
                    let stack = Box::new(cli.run(LocalStack::default()));
                    let endpoint_port = stack.get_host_port_ipv4(4566);
                    let s3_client = s3_client(endpoint_port).await;

                    (Some(stack), s3_client)
                }
                Self::DockerCompose(client) => (None, client.clone()),
            }
        }
    }
}

pub mod fixtures {

    use anyhow::Result;
    use aws_sdk_s3::Client;

    pub async fn create_bucket(client: &Client, bucket: &str) -> Result<()> {
        client.create_bucket().bucket(bucket).send().await?;
        Ok(())
    }
}
