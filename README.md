# rpx

`rpx` is a Rust CLI for managing R package project dependencies.

It uses `DESCRIPTION` as the manifest, keeps a resolved `rpx.lock` in the project root, and manages an isolated package library outside the repository.

`rpx` resolves and installs packages through `https://api.rrepo.org`.

`rpx` works with R package projects. Use `rpx init` to create a `DESCRIPTION` file for a new project.

## Usage

Install from GitHub:

```bash
cargo install --git https://github.com/scalerail-solutions/rpx.git
```

Example workflow for a new project:

```bash
rpx init   # create a DESCRIPTION in the current directory
rpx add digest
```

Example workflow for an existing package project with a `DESCRIPTION` file and no `rpx.lock` yet:

```bash
rpx lock   # resolve dependencies from DESCRIPTION and write rpx.lock
rpx sync   # download and install the exact locked package set into the project library
rpx run R  # start an R shell with the project library activated
```

Add or remove a dependency:

```bash
rpx add digest
rpx remove digest
```

## Notes

- `DESCRIPTION` is required.
- `rpx.lock` records the resolved package set and the registry origin.
- `rpx` manages an isolated library for each project.
- `rpx lock` resolves from `DESCRIPTION` through `api.rrepo.org`; it does not install packages.
- `rpx sync` installs the exact locked package set from downloaded source artifacts.
- Custom repositories are not supported.

## Local Development

Run the test suite with:

```bash
cargo test
```

The test suite depends on Docker and uses `testcontainers`.

Integration tests run against the official `r-base` image and execute realistic package-management workflows inside containers. This keeps the tests close to real usage while avoiding changes to your local R installation or package library.
