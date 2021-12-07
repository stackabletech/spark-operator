use std::convert::TryInto;
use std::{collections::BTreeSet, num::TryFromIntError};

use crate::error::Error::{
    self, ExpectedPods, FindEndpoints, InvalidNodePort, NoName, NoNamespace, NoNodePort,
    NoServicePort, ObjectMissingMetadataForOwnerRef,
};

use crate::util::version;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service},
    kube::{self, runtime::reflector::ObjectRef, Resource, ResourceExt},
};
use stackable_spark_crd::constants::{APP_NAME, APP_PORT};
use stackable_spark_crd::{SparkCluster, SparkRole};

/// Builds discovery [`ConfigMap`]s for connecting to a [`SparkCluster`] for all expected scenarios
pub async fn build_discovery_configmaps(
    kube: &kube::Client,
    owner: &impl Resource<DynamicType = ()>,
    sc: &SparkCluster,
    svc: &Service,
    chroot: Option<&str>,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner.name();
    Ok(vec![
        build_discovery_configmap(&name, owner, sc, chroot, pod_hosts(sc)?)?,
        build_discovery_configmap(
            &format!("{}-nodeport", name),
            owner,
            sc,
            chroot,
            nodeport_hosts(kube, svc, "sc").await?,
        )?,
    ])
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`SparkCluster`]
///
/// `hosts` will usually come from either [`pod_hosts`] or [`nodeport_hosts`].
fn build_discovery_configmap(
    name: &str,
    owner: &impl Resource<DynamicType = ()>,
    sc: &SparkCluster,
    chroot: Option<&str>,
    hosts: impl IntoIterator<Item = (impl Into<String>, u16)>,
) -> Result<ConfigMap, Error> {
    // Write a connection string of the format that Java ZooKeeper client expects:
    // "{host1}:{port1},{host2:port2},.../{chroot}"
    // See https://zookeeper.apache.org/doc/current/apidocs/zookeeper-server/org/apache/zookeeper/ZooKeeper.html#ZooKeeper-java.lang.String-int-org.apache.zookeeper.Watcher-
    let mut conn_str = hosts
        .into_iter()
        .map(|(host, port)| format!("{}:{}", host.into(), port))
        .collect::<Vec<_>>()
        .join(",");
    // if let Some(chroot) = chroot {
    //     if !chroot.starts_with('/') {
    //         return RelativeChroot { chroot }.fail();
    //     }
    //     conn_str.push_str(chroot);
    // }
    Ok(ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(sc)
                .name(name)
                .ownerreference_from_resource(owner, None, Some(true))
                .with_context(|| ObjectMissingMetadataForOwnerRef {
                    sc: ObjectRef::from_obj(sc),
                })?
                .with_recommended_labels(
                    sc,
                    APP_NAME,
                    version(sc).unwrap(),
                    &SparkRole::Server.to_string(),
                    "discovery",
                )
                .build(),
        )
        .add_data("SPARK", conn_str)
        .build()
        .unwrap())
}

/// Lists all Pods FQDNs expected to host the [`SparkCluster`]
fn pod_hosts(sc: &SparkCluster) -> Result<impl IntoIterator<Item = (String, u16)> + '_, Error> {
    Ok(sc
        .pods()
        .context(ExpectedPods)?
        .into_iter()
        .map(|pod_ref| (pod_ref.fqdn(), APP_PORT)))
}

/// Lists all nodes currently hosting Pods participating in the [`Service`]
async fn nodeport_hosts(
    kube: &kube::Client,
    svc: &Service,
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)>, Error> {
    let ns = svc.metadata.namespace.as_deref().context(NoNamespace)?;
    let endpointses = kube::Api::<Endpoints>::namespaced(kube.clone(), ns);
    let svc_port = svc
        .spec
        .as_ref()
        .and_then(|svc_spec| {
            svc_spec
                .ports
                .as_ref()?
                .iter()
                .find(|port| port.name.as_deref() == Some("sc"))
        })
        .context(NoServicePort {
            port_name: String::from(port_name),
        })?;
    let node_port = svc_port.node_port.context(NoNodePort {
        port_name: String::from(port_name),
    })?;
    let endpoints = endpointses
        .get(svc.metadata.name.as_deref().context(NoName)?)
        .await
        .context(FindEndpoints)?;
    let nodes = endpoints
        .subsets
        .into_iter()
        .flatten()
        .flat_map(|subset| subset.addresses)
        .flatten()
        .flat_map(|addr| addr.node_name);
    let addrs = nodes
        .map(|node| Ok((node, node_port.try_into().context(InvalidNodePort)?)))
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(addrs)
}
