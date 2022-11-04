# `s3-archiver`

`s3-archiver` is a binary to create zip archives from multiple files in S3.

Given an output S3 URL argument and list of line spaced S3 URLs on standard input `s3-archiver` will add each input file into an archive at the output S3 URL.  `s3-archiver` uses S3 multipart uploads in order
to keep the memory usage low.

## Usage

```bash
Usage: s3-archiver-cli <COMMAND>

Commands:
  archive            Create an ZIP archive in S3 from source files in S3
  validate           Validate a ZIP archive matches the given manifest
  validate-manifest  Validate the calculated crc32 of files in the manifest match those recorded the manifest
  help               Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help information
```

Each file will be `stored` in the ZIP with no compression unless a compression argument is provided. Supported compression includes deflate, bzip2, lzma, zstd and xz.  All input files will be compressed with the same compression.

The path of each file in the ZIP will the be the key of the input object unless a `prefix_strip`argument is provided. If a `prefix_strip` argument is provided its value is removed from the input object key before writing it into the zip. For example `my/input/key.txt` with a `prefix_strip` argument of `my/` will be added to the zip as `input/key.txt`.  The `prefix_strip` must not start with `\` but must include a trailing `\`, this is explicitly validated rather than `magically` edited in the code.

## Limitations

The [`async-zip`](https://github.com/Majored/rs-async-zip) crate does not support ZIP64 which introduces the following limitations:

* Number of files inside an Archive 65,535.
* Size of a file in an Archive [bytes] 4,294,967,295.
* Size of an Archive [bytes] 4,294,967,295.
* Central Directory Size [bytes] 4,294,967,295.

The current failure mode of this binary is silent if these limits are reached.

## Testing

The testing of this binary operates in three modes, all of which use [localstack](https://github.com/localstack/localstack):

* Single instance Localstack.
* Localstack with `docker compose`.
* Multiple instances of Localstack using [testcontainers-rs](https://github.com/testcontainers/testcontainers-rs).

### Single Instance Localstack
To run `cargo test` an instance of `localstack` must be running and listening on `127.0.0.1:4566`.  The test code will connect to `localhost:4566` (so `localhost` must resolve to your local machine).  Since the `localstack`
instance is shared between all the tests they must be run in series using 

```bash
cargo test -- --test-threads=1
```

to ensure that the tests do not share resources.

### Localstack with `docker compose`
To run the tests as part of `docker compose` use 

```bash
make test
```

this will start `localstack` as part of `docker compose`.  Note `make test` does not run `docker compose down` so this will leave a running container. Like the single instance method the tests must be run in a single thread or using isolated resources.

### TestContainers
To run the tests using `testcontainers` use the `test_containers` feature flag.

```bash
cargo test --features test_containers 
```

For each test a separate localstack instance is started listening on a separate port allowing the tests to be run in isolation and in parallel.

## Building

The project can be built and run using `cargo`.  To build the project use `cargo build`, to run the project `cargo run` and to install the project use `cargo install`.

## Development

### Basic Work Flow

This repo uses the [3 Musketeers pattern](https://3musketeers.io/) to provide a consistent
dev, test and build environment using `make` and `docker`. To get started:

```
make help
```

The source code for the service components can be found under [`./src/`](./src/).
You can run the tests by doing:

```
make test
```

And you can apply some automated code formatting by doing:

```
make fmt
```

This project is designed to be deploy as a docker image.
You can build all the docker image by doing:

```
make build
```

To publish the docker image to an ECR repository, set the environment variables
`ECR_REPOSITORY` and `ECR_TAG` and then:

```
make publish
```
