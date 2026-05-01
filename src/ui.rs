use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fmt,
    io::IsTerminal,
};

use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

use crate::registry::ArtifactKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstallKind {
    Binary,
    Source,
}

impl InstallKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::Source => "source",
        }
    }

    fn active_label(self) -> &'static str {
        match self {
            Self::Binary => "installing binary",
            Self::Source => "compiling from source",
        }
    }
}

#[derive(Debug)]
pub(crate) struct SyncUi {
    interactive: bool,
    _multi: Option<MultiProgress>,
    downloads: RefCell<Option<ProgressBar>>,
    binary_installs: RefCell<Option<ProgressBar>>,
    source_builds: RefCell<Option<ProgressBar>>,
    active_downloads: RefCell<BTreeMap<String, ProgressBar>>,
    active_installs: RefCell<BTreeMap<String, ProgressBar>>,
    restored_packages: Cell<u64>,
    downloaded_packages: Cell<u64>,
    downloaded_bytes: Cell<u64>,
    total_download_bytes: Cell<Option<u64>>,
    binary_finished: Cell<u64>,
    source_finished: Cell<u64>,
}

impl SyncUi {
    pub(crate) fn new() -> Self {
        let interactive = std::io::stderr().is_terminal();

        if interactive {
            let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());

            Self {
                interactive,
                _multi: Some(multi),
                downloads: RefCell::new(None),
                binary_installs: RefCell::new(None),
                source_builds: RefCell::new(None),
                active_downloads: RefCell::new(BTreeMap::new()),
                active_installs: RefCell::new(BTreeMap::new()),
                restored_packages: Cell::new(0),
                downloaded_packages: Cell::new(0),
                downloaded_bytes: Cell::new(0),
                total_download_bytes: Cell::new(Some(0)),
                binary_finished: Cell::new(0),
                source_finished: Cell::new(0),
            }
        } else {
            Self {
                interactive,
                _multi: None,
                downloads: RefCell::new(None),
                binary_installs: RefCell::new(None),
                source_builds: RefCell::new(None),
                active_downloads: RefCell::new(BTreeMap::new()),
                active_installs: RefCell::new(BTreeMap::new()),
                restored_packages: Cell::new(0),
                downloaded_packages: Cell::new(0),
                downloaded_bytes: Cell::new(0),
                total_download_bytes: Cell::new(Some(0)),
                binary_finished: Cell::new(0),
                source_finished: Cell::new(0),
            }
        }
    }

    pub(crate) fn start_restores(&self, total: usize) {
        if total == 0 {
            return;
        }

        self.restored_packages.set(0);

        if self.interactive {
            let bar = self
                .create_aggregate_bar("downloads          [{bar:30.cyan/blue}] {pos}/{len} {msg}");
            bar.set_length(total as u64);
            bar.set_position(0);
            bar.set_message("from cache");
            self.downloads.replace(Some(bar));
        } else {
            eprintln!("Restoring {total} cached packages");
        }
    }

    pub(crate) fn finish_restore(&self, name: &str, version: &str) {
        self.restored_packages
            .set(self.restored_packages.get().saturating_add(1));
        if let Some(bar) = self.downloads.borrow().as_ref() {
            bar.inc(1);
        }
        if !self.interactive {
            eprintln!("Restored {name}@{version} from cache");
        }
    }

    pub(crate) fn finish_restores(&self) {
        if self.interactive {
            if let Some(bar) = self.downloads.borrow_mut().take() {
                bar.finish_with_message(format!(
                    "Restored {} {} from cache",
                    self.restored_packages.get(),
                    pluralize(self.restored_packages.get(), "package", "packages")
                ));
            }
        }
    }

    pub(crate) fn start_downloads(&self, total: usize) {
        if total == 0 {
            return;
        }

        self.downloaded_packages.set(0);
        self.downloaded_bytes.set(0);
        self.total_download_bytes.set(Some(0));

        if self.interactive {
            let bar = self
                .create_aggregate_bar("downloads          [{bar:30.cyan/blue}] {pos}/{len} {msg}");
            bar.set_length(total as u64);
            bar.set_position(0);
            self.downloads.replace(Some(bar));
            self.update_download_message();
        } else {
            eprintln!("Downloading {total} packages");
        }
    }

    pub(crate) fn start_download(&self, name: &str, version: &str, kind: ArtifactKind) {
        if let Some(bar) = self.active_downloads.borrow_mut().remove(name) {
            bar.finish_and_clear();
        }

        if self.interactive {
            let bar = self
                ._multi
                .as_ref()
                .map_or_else(ProgressBar::hidden, |multi| {
                    let bar = multi.add(ProgressBar::new(0));
                    bar.set_style(
                        ProgressStyle::with_template(
                            "  {msg:28} [{bar:24.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec}",
                        )
                        .expect("progress template should parse")
                        .progress_chars("##-"),
                    );
                    bar.set_message(format!("{name} {version} {}", artifact_label(kind)));
                    bar
                });
            self.active_downloads
                .borrow_mut()
                .insert(name.to_string(), bar);
        } else {
            eprintln!("Downloading {name}@{version} {}", artifact_label(kind));
        }
    }

    pub(crate) fn set_download_length(&self, name: &str, length: u64) {
        if let Some(bar) = self.active_downloads.borrow().get(name) {
            bar.set_length(length);
        }

        let total = self
            .total_download_bytes
            .get()
            .and_then(|total| total.checked_add(length));
        self.total_download_bytes.set(total);
        self.update_download_message();
    }

    pub(crate) fn advance_download(&self, name: &str, bytes: u64) {
        if let Some(bar) = self.active_downloads.borrow().get(name) {
            bar.inc(bytes);
        }
        self.downloaded_bytes
            .set(self.downloaded_bytes.get().saturating_add(bytes));
        self.update_download_message();
    }

    pub(crate) fn fallback_to_source(&self, name: &str, version: &str) {
        if !self.interactive {
            eprintln!("{name}@{version} binary unavailable; falling back to source");
        }
    }

    pub(crate) fn finish_download(&self, name: &str, version: &str, kind: InstallKind) {
        self.downloaded_packages
            .set(self.downloaded_packages.get().saturating_add(1));
        if let Some(bar) = self.downloads.borrow().as_ref() {
            bar.inc(1);
        }
        if let Some(bar) = self.active_downloads.borrow_mut().remove(name) {
            bar.finish_and_clear();
        }
        self.update_download_message();

        if !self.interactive {
            eprintln!("Downloaded {name}@{version} {}", kind.label());
        }
    }

    pub(crate) fn finish_downloads(&self) {
        if self.interactive {
            for (_, bar) in self.active_downloads.borrow_mut().split_off("") {
                bar.finish_and_clear();
            }
            if let Some(bar) = self.downloads.borrow_mut().take() {
                bar.finish_with_message(format!(
                    "Downloaded {} {} ({})",
                    self.downloaded_packages.get(),
                    pluralize(self.downloaded_packages.get(), "package", "packages"),
                    HumanBytes(self.downloaded_bytes.get())
                ));
            }
        }
    }

    pub(crate) fn start_installs(&self, binary_total: usize, source_total: usize) {
        self.binary_finished.set(0);
        self.source_finished.set(0);

        if self.interactive {
            if binary_total > 0 {
                let bar = self
                    .create_aggregate_bar("installing binaries[{bar:30.green/blue}] {pos}/{len}");
                bar.set_length(binary_total as u64);
                bar.set_position(0);
                self.binary_installs.replace(Some(bar));
            }
            if source_total > 0 {
                let bar = self
                    .create_aggregate_bar("compiling source   [{bar:30.yellow/blue}] {pos}/{len}");
                bar.set_length(source_total as u64);
                bar.set_position(0);
                self.source_builds.replace(Some(bar));
            }
        } else {
            if binary_total > 0 {
                eprintln!("Installing {binary_total} binary packages");
            }
            if source_total > 0 {
                eprintln!("Compiling {source_total} packages from source");
            }
        }
    }

    pub(crate) fn start_install_batch<I>(&self, packages: I)
    where
        I: IntoIterator<Item = (String, String, InstallKind)>,
    {
        for (name, version, kind) in packages {
            if self.interactive {
                let bar = self
                    ._multi
                    .as_ref()
                    .map_or_else(ProgressBar::hidden, |multi| {
                        let bar = multi.add(ProgressBar::new_spinner());
                        bar.set_style(
                            ProgressStyle::with_template("  {spinner} {msg}")
                                .expect("progress template should parse"),
                        );
                        bar.enable_steady_tick(std::time::Duration::from_millis(100));
                        bar.set_message(format!("{name} {version} {}", kind.active_label()));
                        bar
                    });
                self.active_installs.borrow_mut().insert(name.clone(), bar);
            } else {
                eprintln!("{} {name}@{version}", sentence_case(kind.active_label()));
            }
        }
    }

    pub(crate) fn finish_install(&self, name: &str, version: &str, kind: InstallKind) {
        match kind {
            InstallKind::Binary => {
                self.binary_finished
                    .set(self.binary_finished.get().saturating_add(1));
                if let Some(bar) = self.binary_installs.borrow().as_ref() {
                    bar.inc(1);
                }
            }
            InstallKind::Source => {
                self.source_finished
                    .set(self.source_finished.get().saturating_add(1));
                if let Some(bar) = self.source_builds.borrow().as_ref() {
                    bar.inc(1);
                }
            }
        }
        if let Some(bar) = self.active_installs.borrow_mut().remove(name) {
            bar.finish_and_clear();
        }

        if !self.interactive {
            eprintln!("Finished {name}@{version} {}", kind.label());
        }
    }

    pub(crate) fn fail_install(&self, name: &str, _version: &str) {
        if let Some(bar) = self.active_installs.borrow_mut().remove(name) {
            bar.finish_and_clear();
        }
    }

    pub(crate) fn finish_installs(&self) {
        if self.interactive {
            for (_, bar) in self.active_installs.borrow_mut().split_off("") {
                bar.finish_and_clear();
            }
            if let Some(bar) = self.binary_installs.borrow_mut().take() {
                bar.finish_with_message(format!(
                    "Installed {} {}",
                    self.binary_finished.get(),
                    pluralize(self.binary_finished.get(), "binary", "binaries")
                ));
            }
            if let Some(bar) = self.source_builds.borrow_mut().take() {
                bar.finish_with_message(format!(
                    "Compiled {} {} from source",
                    self.source_finished.get(),
                    pluralize(self.source_finished.get(), "package", "packages")
                ));
            }
        }
    }

    pub(crate) fn start_removals(&self, total: usize) {
        if total == 0 {
            return;
        }

        if !self.interactive {
            eprintln!("Removing {total} extra packages");
        }
    }

    pub(crate) fn finish_removals(&self) {}

    pub(crate) fn finish(&self) {}

    fn update_download_message(&self) {
        if !self.interactive {
            return;
        }

        let downloaded = HumanBytes(self.downloaded_bytes.get());
        let message = match self.total_download_bytes.get() {
            Some(0) | None => format!("{downloaded}"),
            Some(total) => format!("{downloaded}/{}", HumanBytes(total)),
        };
        if let Some(bar) = self.downloads.borrow().as_ref() {
            bar.set_message(message);
        }
    }

    fn create_aggregate_bar(&self, template: &str) -> ProgressBar {
        self._multi
            .as_ref()
            .map_or_else(ProgressBar::hidden, |multi| {
                let bar = multi.add(ProgressBar::new(0));
                bar.set_style(
                    ProgressStyle::with_template(template)
                        .expect("progress template should parse")
                        .progress_chars("##-"),
                );
                bar
            })
    }
}

#[derive(Debug)]
pub(crate) struct SystemDepsUi {
    interactive: bool,
    bar: ProgressBar,
}

impl SystemDepsUi {
    pub(crate) fn start() -> Self {
        let interactive = std::io::stderr().is_terminal();
        let bar = if interactive {
            let bar = ProgressBar::new_spinner();
            bar.set_style(
                ProgressStyle::with_template("{spinner} {msg}")
                    .expect("progress template should parse"),
            );
            bar.enable_steady_tick(std::time::Duration::from_millis(100));
            bar.set_message("installing system dependencies");
            bar
        } else {
            eprintln!("Installing system dependencies...");
            ProgressBar::hidden()
        };

        Self { interactive, bar }
    }

    pub(crate) fn finish(self) {
        if self.interactive {
            self.bar
                .finish_with_message("Installed system dependencies".to_string());
        } else {
            eprintln!("Installed system dependencies");
        }
    }

    pub(crate) fn fail(self) {
        if self.interactive {
            self.bar.finish_and_clear();
        }
    }
}

fn artifact_label(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Source => "source",
        ArtifactKind::Binary => "binary",
    }
}

fn sentence_case(value: &str) -> String {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    first.to_uppercase().chain(chars).collect()
}

fn pluralize<'a>(count: u64, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

#[derive(Debug)]
pub(crate) struct ResolutionUi {
    bar: ProgressBar,
    version_loads: Cell<u64>,
    description_loads: Cell<u64>,
    cache_hits: Cell<u64>,
}

impl ResolutionUi {
    pub(crate) fn new() -> Self {
        let bar = if std::io::stderr().is_terminal() {
            let bar = ProgressBar::new_spinner();
            bar.set_style(
                ProgressStyle::with_template("{spinner} {msg}")
                    .expect("resolution spinner template should be valid"),
            );
            bar.enable_steady_tick(std::time::Duration::from_millis(120));
            bar
        } else {
            ProgressBar::hidden()
        };

        let ui = Self {
            bar,
            version_loads: Cell::new(0),
            description_loads: Cell::new(0),
            cache_hits: Cell::new(0),
        };
        ui.update_message("starting resolution");
        ui
    }

    pub(crate) fn on_version_load(&self, package: &str) {
        self.version_loads.set(self.version_loads.get() + 1);
        self.update_message(&format!("loading versions for {package}"));
    }

    pub(crate) fn on_description_load(&self, package: &str, version: impl fmt::Display) {
        self.description_loads.set(self.description_loads.get() + 1);
        self.update_message(&format!("loading DESCRIPTION for {package}@{version}"));
    }

    pub(crate) fn on_cache_hit(&self, detail: &str) {
        self.cache_hits.set(self.cache_hits.get() + 1);
        self.update_message(detail);
    }

    pub(crate) fn finish(&self, resolved_packages: usize) {
        self.bar.finish_with_message(format!(
            "resolved {resolved_packages} packages (version lists: {}, descriptions: {}, cache hits: {})",
            self.version_loads.get(),
            self.description_loads.get(),
            self.cache_hits.get()
        ));
    }

    pub(crate) fn fail(&self) {
        self.bar.finish_with_message(format!(
            "resolution failed (version lists: {}, descriptions: {}, cache hits: {})",
            self.version_loads.get(),
            self.description_loads.get(),
            self.cache_hits.get()
        ));
    }

    fn update_message(&self, detail: &str) {
        self.bar.set_message(format!(
            "resolving packages: {detail} (version lists: {}, descriptions: {}, cache hits: {})",
            self.version_loads.get(),
            self.description_loads.get(),
            self.cache_hits.get()
        ));
    }
}
