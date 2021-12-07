use crate::error::Error;
use crate::error::Error::ObjectHasNoVersion;
use crate::{spark_controller, ObjectRef};
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

/// Gets a [`kube::Api`] in `obj`'s native scope
fn kube_api_for_obj<K>(kube: &kube::Client, obj: &K) -> kube::Api<K>
where
    K: Resource<DynamicType = ()>,
{
    if let Some(ns) = &obj.meta().namespace {
        kube::Api::<K>::namespaced(kube.clone(), ns)
    } else {
        kube::Api::<K>::all(kube.clone())
    }
}

/// Server-side applies an object that our controller "owns" (is the primary controller for)
///
/// Compared to [`kube::Api::patch`], this automatically reads the kind, namespace, and name from the `object` rather than
/// requiring them to be duplicated manually.
pub async fn apply_owned<K>(kube: &kube::Client, field_manager: &str, obj: &K) -> kube::Result<K>
where
    K: Resource<DynamicType = ()> + Serialize + DeserializeOwned + Clone + Debug,
{
    let api = kube_api_for_obj(kube, obj);
    api.patch(
        // Name is required, but K8s API will fail if this is not provided
        obj.meta().name.as_deref().unwrap_or(""),
        &PatchParams {
            force: true,
            field_manager: Some(field_manager.to_string()),
            ..PatchParams::default()
        },
        &Patch::Apply(obj),
    )
    .await
}

/// Server-side applies the status of our controller object
///
/// Compared to [`kube::Api::patch`], this automatically reads the kind, namespace, and name from the `object` rather than
/// requiring them to be duplicated manually.
pub async fn apply_status<K>(kube: &kube::Client, field_manager: &str, obj: &K) -> kube::Result<K>
where
    K: Resource<DynamicType = ()> + Serialize + DeserializeOwned + Clone + Debug,
{
    let api = kube_api_for_obj(kube, obj);
    api.patch_status(
        // Name is required, but K8s API will fail if this is not provided
        obj.meta().name.as_deref().unwrap_or(""),
        &PatchParams {
            force: true,
            field_manager: Some(field_manager.to_string()),
            ..PatchParams::default()
        },
        &Patch::Apply(obj),
    )
    .await
}
