use stackable_operator::error;
use stackable_spark_crd::SparkCluster;
use stackable_spark_operator::create_controller;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::initialize_logging("SPARK_OPERATOR_LOG");
    let client =
        stackable_operator::create_client(Some("spark.stackable.de".to_string())).await?;

    stackable_operator::crd::ensure_crd_created::<SparkCluster>(client.clone()).await?;

    create_controller(client).await;
    Ok(())
}
