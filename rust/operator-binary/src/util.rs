use crate::error::Error;
use crate::error::Error::ObjectHasNoVersion;
use crate::ObjectRef;
use stackable_spark_crd::SparkCluster;

pub fn version(sc: &SparkCluster) -> Result<&str, Error> {
    sc.spec.version.as_deref().ok_or(ObjectHasNoVersion {
        obj_ref: ObjectRef::from_obj(sc),
    })
}
