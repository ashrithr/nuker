//! Graph utility to create DAG for tracking `Resource` dependencies.

use crate::resource::Resource;
use crate::{Error, Result};
use petgraph::{
    algo::{is_cyclic_directed, toposort},
    dot::{Config, Dot},
    stable_graph::NodeIndex,
    EdgeType, Graph,
};
use std::collections::HashMap;
use tracing::trace;

#[derive(Debug, Copy, Clone)]
pub enum Relation {
    Depends,
    Root,
}

pub struct Dag {
    pub graph: Graph<Resource, Relation>,
    pub id_map: HashMap<String, NodeIndex<u32>>,
    pub root_node: NodeIndex<u32>,
}

impl Dag {
    /// Create a Dag with "root" node
    pub fn new() -> Self {
        let mut graph: Graph<Resource, Relation> = Graph::new();
        let id_map: HashMap<String, NodeIndex<u32>> = HashMap::new();

        let root_node = graph.add_node(Resource::default());

        Dag {
            graph,
            id_map,
            root_node,
        }
    }

    /// Builds a DAG from provided resources
    pub fn build_graph(&mut self, resources: &[Resource]) -> Result<()> {
        let root_node = self.graph.add_node(Resource::default());

        for resource in resources {
            let r_index = if self.id_map.contains_key(&resource.id) {
                *self.id_map.get(&resource.id).unwrap()
            } else {
                let rid = self.graph.add_node(resource.clone());
                self.id_map.insert(resource.id.clone(), rid);
                rid
            };
            self.graph.add_edge(root_node, r_index, Relation::Root);

            if let Some(dependencies) = resource.dependencies.as_ref() {
                for dep in dependencies {
                    let dep_index = if self.id_map.contains_key(&dep.id) {
                        *self.id_map.get(&dep.id).unwrap()
                    } else {
                        let rid = self.graph.add_node(dep.clone());
                        self.id_map.insert(dep.id.clone(), rid);
                        self.graph.add_edge(root_node, rid, Relation::Root);
                        rid
                    };

                    self.graph.add_edge(dep_index, r_index, Relation::Depends);
                }
            }
        }

        if !is_dag(&self.graph) {
            return Err(Error::Dag(
                "Failed constructing dependency graph for the resources".to_string(),
            ));
        }

        if self.graph.capacity().0 > 1 {
            trace!(
                "{:?}",
                Dot::with_config(&self.graph, &[Config::EdgeIndexLabel])
            );
        }

        Ok(())
    }

    /// Add a given Resource to the DAG
    pub fn add_node_to_dag(&mut self, mut r: Resource) {
        let resource_id = r.id.clone();
        let resource_deps = std::mem::replace(&mut r.dependencies, None);

        let r_index = if self.id_map.contains_key(&r.id) {
            *self.id_map.get(&r.id).unwrap()
        } else {
            let rid = self.graph.add_node(r);
            self.id_map.insert(resource_id, rid);
            rid
        };

        self.graph.add_edge(self.root_node, r_index, Relation::Root);

        if let Some(deps) = resource_deps {
            for dep in deps {
                let dep_id = dep.id.clone();
                let dep_index = if self.id_map.contains_key(&dep.id) {
                    *self.id_map.get(&dep.id).unwrap()
                } else {
                    let rid = self.graph.add_node(dep);
                    self.id_map.insert(dep_id, rid);
                    self.graph.add_edge(self.root_node, rid, Relation::Root);
                    rid
                };

                self.graph.add_edge(dep_index, r_index, Relation::Depends);
            }
        }
    }

    /// Order the resources based on their dependencies by performing topological
    /// sort of the DAG.
    /// TODO: Return list of lists to parallelize the execution of tasks.
    pub fn order_by_dependencies(&self) -> Result<Vec<Resource>> {
        let mut resources = Vec::new();

        match toposort(&self.graph, None) {
            Ok(order) => {
                for i in order {
                    if let Some(resource) = self.graph.node_weight(i) {
                        if resource.type_.is_root() {
                            continue;
                        }
                        resources.push(resource.clone());
                    }
                }

                Ok(resources)
            }
            Err(err) => {
                let error = self
                    .graph
                    .node_weight(err.node_id())
                    .map(|weight| format!("Error graph has cycle at node: {:?}", weight));

                Err(Error::Dag(error.unwrap_or_default()))
            }
        }
    }
}

/// Checks if provided Graph is a DAG or not
fn is_dag<'a, N: 'a, E: 'a, Ty, Ix>(g: &'a Graph<N, E, Ty, Ix>) -> bool
where
    Ty: EdgeType,
    Ix: petgraph::graph::IndexType,
{
    return g.is_directed() && !is_cyclic_directed(g);
}
