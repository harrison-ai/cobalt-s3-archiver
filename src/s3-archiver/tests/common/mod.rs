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
                    let host = std::env::var("LOCALSTACK_HOSTNAME")
                        .expect("LOCALSTACK_HOSTNAME must be set in docker compose");
                    (None, s3_client(&host, 4566).await)
                }
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
