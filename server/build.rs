use stackable_operator::crd::CustomResourceExt;
use stackable_spark_crd::commands::{Restart, Start, Stop};
use stackable_spark_crd::SparkCluster;

fn main() -> Result<(), stackable_operator::error::Error> {
    SparkCluster::write_yaml_schema("../deploy/crd/sparkcluster.crd.yaml")?;
    Restart::write_yaml_schema("../deploy/crd/restart.command.crd.yaml")?;
    Start::write_yaml_schema("../deploy/crd/start.command.crd.yaml")?;
    Stop::write_yaml_schema("../deploy/crd/stop.command.crd.yaml")?;

    Ok(())
}
