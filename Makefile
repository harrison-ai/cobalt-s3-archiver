## 
## Project Makefile for cobalt-s3-archiver
## 

.DEFAULT_GOAL := help

## Customizable settings
## 
## BUILD_PROFILE:			build profile for Rust code ("dev" or "release").
BUILD_PROFILE ?= dev

## ECR_REPOSITORY:			repository added to crate name when tagging docker images
ifdef ECR_REPOSITORY
	ECR_PREFIX = $(ECR_REPOSITORY)/
endif

## ECR_NAME_SUFFIX:			suffix added to crate name when tagging docker images
## ECR_TAG:					tag to used when tagging docker images
ifdef ECR_TAG
	ECR_SUFFIX = $(ECR_NAME_SUFFIX):$(ECR_TAG)
else
	ECR_SUFFIX = $(ECR_NAME_SUFFIX)
endif

## IMAGES:					list of crates from which to build docker images
IMAGES ?= $(shell find ./src/ -mindepth 2 -maxdepth 2 -name Dockerfile | cut -d '/' -f 3)

UID = $(shell id -u)
DCRUN = docker-compose run --rm --user $(UID)
DOCKER_BUILD = docker build --platform arm64 --build-arg BUILD_PROFILE=$(BUILD_PROFILE)

export COMPOSE_DOCKER_CLI_BUILD = 1
export DOCKER_BUILDKIT = 1

## 
## Available targets
## 
## test: 				run all the tests
test: cargo-init
	$(DCRUN) cargo test

# These directories need to exist so they can be mounted into the `cargo`
# docker image with the correct owner and permissions.
cargo-init: ~/.cargo/registry/.keep ~/.cargo/git/db/.keep ~/.cargo/advisory-dbs/.keep

%/.keep:
	mkdir -p $*
	touch $@

## fmt: 				format all code using standard conventions
fmt: cargo-init
	$(DCRUN) cargo fmt

## fix:					apply automated code style fixes
fix: cargo-init fmt
	$(DCRUN) cargo fix --all-targets --all-features
	$(DCRUN) cargo clippy --fix --all-targets --all-features --no-deps

## ci-fmt:				check that all code is formatted using standard conventions
ci-fmt: cargo-init
	$(DCRUN) cargo fmt -- --check

## validate: 			perform style and consistency validation checks
validate: cargo-init ci-fmt
	$(DCRUN) cargo clippy --all-targets --all-features --no-deps -- -D warnings
	$(DCRUN) cargo deny check
	$(DCRUN) shellcheck ./scripts/*.sh

## licenses-report: Build license summary file
licenses-report:
	$(DCRUN) cargo about generate --output-file ./licenses/licenses.html about.hbs

## build:				build all the docker images for this service
build:
	$(DOCKER_BUILD) -t $(ECR_PREFIX)s3-archiver$(ECR_SUFFIX) -f ./Dockerfile .

## publish:				publish docker images to ECR
publish: build
	docker push $(ECR_PREFIX)s3-archiver$(ECR_SUFFIX) -f ./Dockerfile .

## pull:				docker-compose pull
pull:
	docker-compose pull

## help:				show this help
help:
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)


