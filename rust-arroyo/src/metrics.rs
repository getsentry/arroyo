//! Global default tags for Arroyo metrics.
//!
//! Configure once at startup:
//!
//! ```
//! use sentry_arroyo::metrics::configure_scope;
//!
//! configure_scope(|scope| scope.set_tag("application", "my-service")).unwrap();
//! ```

use std::sync::OnceLock;

use ::metrics::Label;

/// Default tags set by [`configure_scope`].
#[derive(Default)]
pub struct Scope {
    labels: Vec<Label>,
}

impl Scope {
    pub fn set_tag(&mut self, key: &'static str, value: impl Into<String>) {
        self.set_label(Label::new(key, value.into()));
    }

    fn set_label(&mut self, label: Label) {
        if let Some(existing) = self.labels.iter_mut().find(|l| l.key() == label.key()) {
            *existing = label;
        } else {
            self.labels.push(label);
        }
    }
}

static GLOBAL_SCOPE: OnceLock<Scope> = OnceLock::new();

/// Arroyo's metric tags have already been configured.
#[derive(Debug, thiserror::Error)]
#[error("Arroyo metric tags have already been configured")]
pub struct ScopeAlreadyConfigured;

/// Sets default tags once, across all threads.
///
/// Call before using Arroyo. Existing metric handles keep their original tags.
/// Returns [`ScopeAlreadyConfigured`] if tags have already been configured.
pub fn configure_scope(f: impl FnOnce(&mut Scope)) -> Result<(), ScopeAlreadyConfigured> {
    if GLOBAL_SCOPE.get().is_some() {
        return Err(ScopeAlreadyConfigured);
    }

    let mut scope = Scope::default();
    f(&mut scope);
    GLOBAL_SCOPE.set(scope).map_err(|_| ScopeAlreadyConfigured)
}

pub(crate) fn labels_with_scope<const N: usize>(labels: [Label; N]) -> Vec<Label> {
    let scope_labels: &'static [Label] = GLOBAL_SCOPE
        .get()
        .map_or(&[], |scope| scope.labels.as_slice());

    let mut merged = Vec::with_capacity(scope_labels.len() + labels.len());
    merged.extend(
        scope_labels
            .iter()
            .filter(|tag| !labels.iter().any(|label| label.key() == tag.key()))
            .map(|tag| Label::new(tag.key(), tag.value())),
    );
    merged.extend(labels);
    merged
}

macro_rules! counter {
    ($name:expr $(, $key:expr => $value:expr)* $(,)?) => {
        ::metrics::counter!($name, $crate::metrics::labels_with_scope([
            $(::metrics::Label::new($key, $value)),*
        ]))
    };
}

macro_rules! gauge {
    ($name:expr $(, $key:expr => $value:expr)* $(,)?) => {
        ::metrics::gauge!($name, $crate::metrics::labels_with_scope([
            $(::metrics::Label::new($key, $value)),*
        ]))
    };
}

macro_rules! histogram {
    ($name:expr $(, $key:expr => $value:expr)* $(,)?) => {
        ::metrics::histogram!($name, $crate::metrics::labels_with_scope([
            $(::metrics::Label::new($key, $value)),*
        ]))
    };
}

pub(crate) use {counter, gauge, histogram};
