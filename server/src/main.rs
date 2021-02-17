use stackable_operator::{client, error};
use stackable_spark_crd::SparkCluster;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::initialize_logging("SPARK_OPERATOR_LOG");
    let client = client::create_client(Some("spark.stackable.tech".to_string())).await?;

    stackable_operator::crd::ensure_crd_created::<SparkCluster>(client.clone()).await?;

    stackable_spark_operator::create_controller(client).await;
    Ok(())
}
