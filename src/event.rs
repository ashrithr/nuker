use crate::resource::{Resource, ResourceType};

/// Event is the central data structure that is used to passed around from
/// Resource Scanners
#[derive(Debug, Clone)]
pub enum Event {
    /// Shutdown event marks the location in the queue after which no more
    /// resources will appear from a specific resource scanner.
    Shutdown(ResourceType),
    /// A wrapper around `resource::Resource` which represents that the resource
    /// that is scanned by the resource scanner.
    Resource(Resource),
}
