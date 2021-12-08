use crate::error::Error;
use crate::error::Error::ObjectHasNoVersion;
use crate::ObjectRef;
use stackable_operator::k8s_openapi::serde::de::DeserializeOwned;
use stackable_operator::k8s_openapi::serde::Serialize;
use stackable_operator::kube;
use stackable_operator::kube::api::{Patch, PatchParams};
use stackable_operator::kube::Resource;
use stackable_spark_crd::SparkCluster;
use std::fmt::Debug;

pub fn version(sc: &SparkCluster) -> Result<&str, Error> {
    sc.spec.version.as_deref().ok_or(ObjectHasNoVersion {
        obj_ref: ObjectRef::from_obj(sc),
    })
}
