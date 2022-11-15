###############
# Builder image
###############
FROM --platform=$BUILDPLATFORM harrisonai/rust:1.65-1 as builder

ARG BUILD_PROFILE=dev

ENV HOME=/home/root

# Cross-compile the application to run on Graviton2 processors.
# We use a cache mount to help share build artifacts between different
# crates in the repo, and use cargo's JSON output to avoid hard-coding
# the specific paths to the executables being built.
WORKDIR /build
COPY . .

RUN mkdir ./bin

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    # This will build and package all bin targets in the crate.
    # You can add one or more `--bin <name>` arguments to build only a subset of them.
    cargo build --profile ${BUILD_PROFILE} --target aarch64-unknown-linux-musl --message-format=json \
        -p s3-archiver \
        | jq -r ".executable | values" \
        | xargs -I input-file cp input-file ./bin/

###############
# Runtime image
###############
# The AWS SDK expects a certain amount of infrastructure to be present on the system,
# e.g. a home directory in which to look for credentials and some CA certificates with
# which to validate connections. For simplicity as we get started, we deploy based on
# a full-fledged distro rather than building an image from scratch.
FROM --platform=arm64 debian:buster-slim

RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && \
    apt-get install --yes --quiet --no-install-recommends \
        ca-certificates

COPY --from=builder /build/bin/* /bin/
ENTRYPOINT ["/bin/s3-archiver-cli"]
