use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::kube;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_spark_crd::RoleGroupRef;
use stackable_spark_crd::{SparkCluster, SparkRole};
use std::num::TryFromIntError;

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", sc))]
    ObjectMissingMetadataForOwnerRef {
        sc: ObjectRef<SparkCluster>,
    },
    #[snafu(display("chroot path {} was relative (must be absolute)", chroot))]
    RelativeChroot {
        chroot: String,
    },
    #[snafu(display("object has no name associated"))]
    NoName,
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("failed to list expected pods"))]
    ExpectedPods {
        source: stackable_spark_crd::NoNamespaceError,
    },
    NoServicePort {
        port_name: String,
    },
    NoNodePort {
        port_name: String,
    },
    FindEndpoints {
        source: kube::Error,
    },
    InvalidNodePort {
        source: TryFromIntError,
    },

    #[snafu(display("object {} has no namespace", obj_ref))]
    ObjectHasNoNamespace {
        obj_ref: ObjectRef<SparkCluster>,
    },
    #[snafu(display("object {} defines no version", obj_ref))]
    ObjectHasNoVersion {
        obj_ref: ObjectRef<SparkCluster>,
    },
    #[snafu(display("{} has no server role", obj_ref))]
    NoServerRole {
        obj_ref: ObjectRef<SparkCluster>,
    },
    #[snafu(display("failed to calculate global service name for {}", obj_ref))]
    GlobalServiceNameNotFound {
        obj_ref: ObjectRef<SparkCluster>,
    },
    #[snafu(display("failed to calculate service name for role {}", rolegroup))]
    RoleGroupServiceNameNotFound {
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to apply global Service for {}", sc))]
    ApplyRoleService {
        source: kube::Error,
        sc: ObjectRef<SparkCluster>,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("invalid product config for {}", sc))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
        sc: ObjectRef<SparkCluster>,
    },
    #[snafu(display("failed to serialize zoo.cfg for {}", rolegroup))]
    SerializeZooCfg {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to build discovery ConfigMap for {}", sc))]
    BuildDiscoveryConfig {
        source: kube::Error,
        sc: ObjectRef<SparkCluster>,
    },
    #[snafu(display("failed to apply discovery ConfigMap for {}", sc))]
    ApplyDiscoveryConfig {
        source: kube::Error,
        sc: ObjectRef<SparkCluster>,
    },
    #[snafu(display("failed to update status of {}", sc))]
    ApplyStatus {
        source: kube::Error,
        sc: ObjectRef<SparkCluster>,
    },
}
