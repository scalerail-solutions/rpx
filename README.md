# rpx

**Modern package management for R.**

`rpx` brings modern package-management semantics to R projects. Packages declare compatible dependency ranges in [`DESCRIPTION`](https://cran.r-project.org/doc/manuals/r-release/R-exts.html#The-DESCRIPTION-file), `rpx` resolves those constraints before installation, and `rpx.lock` records the exact result.

`rpx` works with `rrepo`, the registry infrastructure that provides the package metadata needed for reliable resolution across public, private, and historical R packages.

The short version:

- `rpx` is the developer workflow.
- `rrepo` is the registry that makes the workflow reliable.
- `DESCRIPTION` declares compatibility.
- `rpx.lock` records the exact solution.

## Why rpx Exists

R has excellent packages, but package management still often depends on installed state, latest package indexes, manual intervention, or after-the-fact compatibility checks.

`rpx` starts earlier:

1. Declare compatibility in `DESCRIPTION`.
2. Resolve the dependency graph before installing packages.
3. Lock the exact package versions that were selected.
4. Sync the local project library from the lockfile.
5. Run R inside the managed project environment.

This gives R developers a workflow closer to modern ecosystems built around explicit constraints, registries, solvers, and lockfiles.

## Compatibility Contracts

`rpx add` turns dependency declarations into compatibility contracts.

When you add a package, `rpx` resolves the current compatible version and records a semver-aware range in `DESCRIPTION`. For example, adding a package resolved at `1.4.2` records a lower bound at the resolved version and an upper bound before the next major version:

```text
Imports:
    examplepkg (>= 1.4.2),
    examplepkg (< 2.0.0)
```

That declaration says more than "this project uses `examplepkg`." It says the project is compatible with `examplepkg` in that major-version range, at or above the version that was selected when the dependency was added.

This matters because the compatibility intent becomes useful to:

- The package author
- Downstream packages
- Applications depending on the package
- The dependency resolver
- The registry

## DESCRIPTION vs rpx.lock

`DESCRIPTION` and `rpx.lock` have different jobs.

`DESCRIPTION` is the compatibility contract. It answers:

> What dependency versions should this package or project be compatible with?

`rpx.lock` is the exact resolved receipt. It answers:

> What exact package versions were selected for this environment?

Commit both files. Do not commit the project library or local cache; `rpx sync` recreates local state from the lockfile.

## Semver Caveat

`rpx` makes a pragmatic semver bet. It cannot force every upstream R package to follow semantic versioning, and it does not guarantee that every upstream major-version boundary is correct.

What it can do is make safer defaults explicit:

- No silent downgrades below the version selected when a dependency was added
- No accidental major-version jumps without changing the declared constraint
- Solver-first installation instead of install-first discovery
- Exact lockfiles for reproducible environments

For `0.x` packages, `rpx` still records a lower bound at the resolved version and an upper bound before `1.0.0`, reflecting the conservative treatment of pre-`1.0` versions.

## Install

`rpx` requires R to be installed and available on `PATH`. Before using `rpx`, confirm that `Rscript` works in your shell.

Install R:

- Windows: https://cran.r-project.org/bin/windows/base/
- macOS: https://cran.r-project.org/bin/macosx/
- CRAN mirrors: https://cran.r-project.org/mirrors.html

Install the latest release on macOS or Linux:

```bash
curl -LsSf https://github.com/scalerail-solutions/rpx/releases/latest/download/rpx-installer.sh | sh
```

Install the latest release on Windows:

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/scalerail-solutions/rpx/releases/latest/download/rpx-installer.ps1 | iex"
```

Windows binary signing is still being worked on. The PowerShell installer is available, but Windows Defender or SmartScreen may warn until the signing flow is finalized.

You can also install `rpx` from Git with Cargo:

```bash
cargo install --git https://github.com/scalerail-solutions/rpx.git
```

If you do not already have Rust and Cargo installed, install them with `rustup`:

- Rust: https://rustup.rs/
- Windows Rust/MSVC prerequisites: https://rust-lang.github.io/rustup/installation/windows-msvc.html

`rpx` prefers binary R package artifacts on Windows and macOS when available, but some packages may still need to be built from source. On Windows, install Rtools if you hit source-build requirements:

- Rtools: https://cran.r-project.org/bin/windows/Rtools/

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

## Repository Management

The package universe is the set of package versions and metadata available to the resolver.

By default, `rpx` uses the public rrepo-backed CRAN universe at `https://upstream.rrepo.dev/cran`. This gives the resolver CRAN package metadata through rrepo APIs, including historical package metadata instead of only today's latest package state.

Projects can add additional repositories. `rpx` supports rrepo API repositories, CRAN repositories, and CRAN-like repositories:

```bash
rpx repo add https://cloud.r-project.org
rpx repo add https://packagemanager.posit.co/cran/latest
rpx repo add https://<org-slug>.rrepo.dev/<repo-slug>
rpx repo list
rpx repo remove https://packagemanager.posit.co/cran/latest
```

The default rrepo-backed CRAN universe remains enabled unless you explicitly disable it. A useful setup is to keep the default universe enabled and add another CRAN or CRAN-like repository as a fallback for binary artifacts:

```bash
rpx repo add https://packagemanager.posit.co/cran/latest
```

During locking, `rpx` merges versions across enabled repositories. Existing locked versions are preferred when they are still available from enabled repositories. If the same version is available from multiple repositories, the earlier repository wins for the locked source URL.

When a repository requires authentication, `rpx` prompts for an API key and stores it in the operating system keyring for that repository URL.

You can override the default public registry root with `RPX_REGISTRY_BASE_URL`:

```bash
RPX_REGISTRY_BASE_URL=https://example.rrepo.dev/cran rpx lock
```

To lock without the default public registry, use `rpx lock --no-default-repo`. The default-repo choice is recorded in `rpx.lock`; use `--default-repo` to enable it again when regenerating the lockfile.

For private package universes, add an rrepo repository for your organization:

```bash
rpx repo add https://<org-slug>.rrepo.dev/<repo-slug>
```

## rrepo

`rrepo` exists because modern dependency resolution needs registry metadata.

A resolver cannot reliably solve public, private, and historical packages if it can only see today's latest package index. The public rrepo registry supports the open R ecosystem and demonstrates the registry-backed model. Commercial rrepo extends the same workflow to private packages, internal package ecosystems, organizations, access control, publishing, and higher usage limits.

## Documentation

The full user guide lives at [rrepo.org](https://rrepo.org/documentation/overview):

- [Install rpx](https://rrepo.org/documentation/install-rpx)
- [Start a project](https://rrepo.org/documentation/start-a-project)
- [Use an existing project](https://rrepo.org/documentation/use-an-existing-project)
- [Run R](https://rrepo.org/documentation/run-r)
- [Private packages](https://rrepo.org/documentation/private-packages)

## How It Works

- `DESCRIPTION` is the dependency manifest and compatibility contract.
- `rpx.lock` records the resolved package set, package sources, and R runtime metadata.
- `rpx lock` resolves dependencies from enabled repositories and writes `rpx.lock`; it does not install packages.
- `rpx` prefers existing locked versions when they are still available from enabled repositories.
- `rpx sync` installs exactly what `rpx.lock` records into the project library.
- `rpx sync` tries Windows and macOS binary artifacts from enabled repositories when available, then falls back to the locked source artifact.
- `rpx run` sets the R library path for the command it runs.

## Local Development

Run the test suite with:

```bash
cargo test
```

The test suite depends on Docker and uses `testcontainers`.

Integration tests run against the official `r-base` image and execute realistic package-management workflows inside containers. This keeps tests close to real usage while avoiding changes to your local R installation or package library.

Releases are created by pushing a version tag such as `v1.1.0`. The release workflow builds precompiled binaries for Linux, macOS, and Windows, then uploads archives, checksums, and installers to GitHub Releases. The Docker workflow publishes `ghcr.io/scalerail-solutions/rpx` images for `linux/amd64` and `linux/arm64`.
