use stackable_spark_crd::{Restart, SparkCluster, Start, Stop};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!(
        "SparkCluster CRD:\n{}\n",
        serde_yaml::to_string(&SparkCluster::crd())?
    );
    println!(
        "Restart Command CRD:\n{}\n",
        serde_yaml::to_string(&Restart::crd())?
    );
    println!(
        "Start Command CRD:\n{}\n",
        serde_yaml::to_string(&Start::crd())?
    );
    println!(
        "Stop Command CRD:\n{}\n",
        serde_yaml::to_string(&Stop::crd())?
    );
    Ok(())
}
