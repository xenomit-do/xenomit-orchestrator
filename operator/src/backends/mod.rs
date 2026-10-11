//! `PodBackend` implementations: real Kubernetes pods (`kube`, needs a
//! cluster) and `local` child processes for dev/tests (docs/02 §2.6).

pub mod kube_backend;
pub mod local;

pub use kube_backend::KubePodBackend;
pub use local::LocalBackend;
