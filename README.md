# `s3-archiver`

`s3-archiver` is a service to create zip archives from multiple files in S3.

## Basic Workflow

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

The top-level crate(s) in this project are designed to deploy as docker images.
You can build all of the docker images by doing:

```
make build
```

Or build just a subset of the images by doing:

```
make build IMAGES="crate-one crate-two"
```

To publish the docker images to an ECR repository, set the environment variables
`ECR_REPOSITORY` and `ECR_TAG` and then:

```
make publish
```
