# rpx

`rpx` is a package manager for R projects. It resolves dependencies from package metadata, writes a committed `rpx.lock`, and runs R inside an isolated project library.

Use it when you want everyone on a project, including CI, to install the same R package versions without depending on a global user library.

`rpx` uses [`DESCRIPTION`](https://cran.r-project.org/doc/manuals/r-release/R-exts.html#The-DESCRIPTION-file) as the manifest. It resolves packages through the public rrepo registry at `https://upstream.rrepo.dev/cran` by default.

## Documentation

The full user guide lives at [rrepo.org](https://rrepo.org/documentation/overview):

- [Install rpx](https://rrepo.org/documentation/install-rpx)
- [Start a project](https://rrepo.org/documentation/start-a-project)
- [Use an existing project](https://rrepo.org/documentation/use-an-existing-project)
- [Run R](https://rrepo.org/documentation/run-r)
- [Private packages](https://rrepo.org/documentation/private-packages)

## Install

`rpx` requires R to be installed and available on `PATH`.

Install the latest release on macOS or Linux:

```bash
curl -LsSf https://github.com/scalerail-solutions/rpx/releases/latest/download/rpx-installer.sh | sh
```

Install the latest release on Windows:

```powershell
powershell -ExecutionPolicy Bypass -Command "irm https://github.com/scalerail-solutions/rpx/releases/latest/download/rpx-installer.ps1 | iex"
```

Rust users can install from source:

```bash
cargo install --git https://github.com/scalerail-solutions/rpx.git
```

You can also run the Docker image directly:

```bash
docker run --rm ghcr.io/scalerail-solutions/rpx:latest --help
```

The Docker image contains the `rpx` binary but does not include R. For project workflows, copy `rpx` into an image that provides R:

```dockerfile
FROM r-base:latest
COPY --link --from=ghcr.io/scalerail-solutions/rpx:latest /rpx /usr/local/bin/rpx
```

## Start a New Project

Create a `DESCRIPTION` file, add a dependency, and start R through `rpx`:

```bash
rpx init
rpx add digest
rpx run R
```

`rpx add` updates `DESCRIPTION`, resolves a compatible package set, writes `rpx.lock`, and syncs the project library.

## Use an Existing Project

For an R package project that already has a `DESCRIPTION` file, create the lockfile and install the locked package set:

```bash
rpx lock
rpx sync
rpx run R
```

Commit both `DESCRIPTION` and `rpx.lock`. Do not commit the project library or local cache; `rpx sync` recreates local state from the lockfile.

## Daily Workflow

Add or remove direct dependencies with `rpx` so the manifest, lockfile, and project library stay together:

```bash
rpx add jsonlite
rpx remove digest
```

Check the project before committing or in CI:

```bash
rpx status
```

Run R commands through `rpx run` so R sees the project library:

```bash
rpx run R
rpx run Rscript scripts/check.R
```

If local package state becomes confusing, remove the project library and caches:

```bash
rpx clean
```

## Additional Repositories

The public rrepo registry is configured by default. Projects can add private or internal rrepo-compatible repositories:

```bash
rpx repo add https://<org-slug>.rrepo.dev/<repo-slug>
rpx repo list
rpx repo remove https://<org-slug>.rrepo.dev/<repo-slug>
```

Additional repositories must expose the rrepo package metadata API. `rpx repo add` does not support CRAN-like repositories, `PACKAGES` indexes, or CRAN-style Posit Package Manager URLs.

When a repository requires authentication, `rpx` prompts for an API key and stores it in the operating system keyring for that repository URL.

You can override the default public registry root with `RPX_REGISTRY_BASE_URL`:

```bash
RPX_REGISTRY_BASE_URL=https://example.rrepo.dev/cran rpx lock
```

## How It Works

- `DESCRIPTION` is the dependency manifest.
- `rpx.lock` records the resolved package set, package sources, and R runtime metadata.
- `rpx lock` resolves dependencies and writes `rpx.lock`; it does not install packages.
- `rpx sync` installs exactly what `rpx.lock` records into the project library.
- `rpx sync` uses Windows and macOS binary artifacts when available, then falls back to source artifacts.
- `rpx run` sets the R library path for the command it runs.

## Local Development

Run the test suite with:

```bash
cargo test
```

The test suite depends on Docker and uses `testcontainers`.

Integration tests run against the official `r-base` image and execute realistic package-management workflows inside containers. This keeps tests close to real usage while avoiding changes to your local R installation or package library.

Releases are created by pushing a version tag such as `v1.1.0`. The release workflow builds precompiled binaries for Linux, macOS, and Windows, then uploads archives, checksums, and installers to GitHub Releases. The Docker workflow publishes `ghcr.io/scalerail-solutions/rpx` images for `linux/amd64` and `linux/arm64`.
