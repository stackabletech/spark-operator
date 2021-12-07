use stackable_operator::crd::CustomResourceExt;
use stackable_spark_crd::SparkCluster;

fn main() -> Result<(), stackable_operator::error::Error> {
    built::write_built_file().expect("Failed to acquire build-time information");

    SparkCluster::write_yaml_schema("../../deploy/crd/sparkcluster.crd.yaml")?;

    Ok(())
}
