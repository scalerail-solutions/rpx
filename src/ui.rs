use std::{cell::Cell, fmt, io::IsTerminal};

use indicatif::{ProgressBar, ProgressStyle};

use crate::output::note;

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
            note("Installing system dependencies...");
            ProgressBar::hidden()
        };

        Self { interactive, bar }
    }

    pub(crate) fn finish(self) {
        if self.interactive {
            self.bar
                .finish_with_message("Installed system dependencies".to_string());
        } else {
            note("Installed system dependencies");
        }
    }

    pub(crate) fn fail(self) {
        if self.interactive {
            self.bar.finish_and_clear();
        }
    }
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
