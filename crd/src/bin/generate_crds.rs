use stackable_operator::crd::CustomResourceExt;
use stackable_spark_crd::commands::{Restart, Start, Stop};
use stackable_spark_crd::SparkCluster;

fn main() {
    let cluster_target_file = "deploy/crd/sparkcluster.crd.yaml";
    SparkCluster::write_yaml_schema(cluster_target_file).unwrap();
    println!("Wrote CRD to [{}]", cluster_target_file);

    let restart_target_file = "deploy/crd/restart.command.crd.yaml";
    Restart::write_yaml_schema(restart_target_file).unwrap();
    println!("Wrote CRD to [{}]", restart_target_file);

    let start_target_file = "deploy/crd/start.command.crd.yaml";
    Start::write_yaml_schema(start_target_file).unwrap();
    println!("Wrote CRD to [{}]", start_target_file);

    let stop_target_file = "deploy/crd/stop.command.crd.yaml";
    Stop::write_yaml_schema(stop_target_file).unwrap();
    println!("Wrote CRD to [{}]", stop_target_file);
}
