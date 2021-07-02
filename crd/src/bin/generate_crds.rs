use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::CustomResourceExt;
use stackable_spark_crd::{Restart, SparkCluster, Start, Stop};
use std::error::Error;
use std::fs;

fn main() -> Result<(), Box<dyn Error>> {
    write_crd::<SparkCluster>("deploy/crd/sparkcluster.crd.yaml");
    write_crd::<Restart>("deploy/crd/restart.command.crd.yaml");
    write_crd::<Start>("deploy/crd/start.command.crd.yaml");
    write_crd::<Stop>("deploy/crd/stop.command.crd.yaml");
    Ok(())
}

fn write_crd<T: CustomResourceExt>(file_path: &str) {
    let schema = T::crd();
    let string_schema = match serde_yaml::to_string(&schema) {
        Ok(schema) => schema,
        Err(err) => panic!("Failed to retrieve CRD: [{}]", err),
    };
    match fs::write(file_path, string_schema) {
        Ok(()) => println!("Successfully wrote CRD to file [{}].", file_path),
        Err(err) => println!("Failed to write file: [{}]", err),
    }
}
