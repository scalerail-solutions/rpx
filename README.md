# rpx

`rpx` is a Rust CLI for managing R package project dependencies.

It uses `DESCRIPTION` as the manifest, keeps a resolved `rpx.lock` in the project root, and manages an isolated package library outside the repository.

`rpx` works with R package projects. A `DESCRIPTION` file is required.

## Usage

Install from GitHub:

```bash
cargo install --git https://github.com/scalerail-solutions/rpx.git
```

Example workflow for an existing package project with a `DESCRIPTION` file and no `rpx.lock` yet:

```bash
rpx lock   # resolve dependencies from DESCRIPTION and write rpx.lock
rpx sync   # install the exact locked package set into the project library
rpx run R  # start an R shell with the project library activated
```

## Notes

- `DESCRIPTION` is required.
- `rpx.lock` records the resolved package set.
- `rpx` manages an isolated library for each project.

## Local Development

Run the test suite with:

```bash
cargo test
```

The test suite depends on Docker and uses `testcontainers`.

Integration tests run against the official `r-base` image and execute realistic package-management workflows inside containers. This keeps the tests close to real usage while avoiding changes to your local R installation or package library.

## Roadmap

- Native integration with `rrepo.org` for private registry support.
