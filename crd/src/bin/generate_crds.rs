use stackable_operator::crd::CustomResourceExt;
use stackable_spark_crd::commands::{Restart, Start, Stop};
use stackable_spark_crd::SparkCluster;

fn main() {
    let cluster_target_file = "deploy/crd/sparkcluster.crd.yaml";
    match SparkCluster::write_yaml_schema(cluster_target_file) {
        Ok(_) => println!("Wrote CRD to [{}]", cluster_target_file),
        Err(err) => println!(
            "Could not write CRD to [{}]: {:?}",
            cluster_target_file, err
        ),
    }

    let restart_target_file = "deploy/crd/restart.command.crd.yaml";
    match Restart::write_yaml_schema(restart_target_file) {
        Ok(_) => println!("Wrote CRD to [{}]", restart_target_file),
        Err(err) => println!(
            "Could not write CRD to [{}]: {:?}",
            restart_target_file, err
        ),
    }

    let start_target_file = "deploy/crd/start.command.crd.yaml";
    match Start::write_yaml_schema(start_target_file) {
        Ok(_) => println!("Wrote CRD to [{}]", start_target_file),
        Err(err) => println!("Could not write CRD to [{}]: {:?}", start_target_file, err),
    }

    let stop_target_file = "deploy/crd/stop.command.crd.yaml";
    match Stop::write_yaml_schema(stop_target_file) {
        Ok(_) => println!("Wrote CRD to [{}]", stop_target_file),
        Err(err) => println!("Could not write CRD to [{}]: {:?}", stop_target_file, err),
    }
}
