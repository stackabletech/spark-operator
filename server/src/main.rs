use stackable_operator::{client, error};
use stackable_spark_crd::SparkCluster;
use stackable_spark_crd::{Restart, Start, Stop};

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::logging::initialize_logging("SPARK_OPERATOR_LOG");
    let client = client::create_client(Some("spark.stackable.tech".to_string())).await?;

    stackable_operator::crd::ensure_crd_created::<SparkCluster>(&client).await?;
    stackable_operator::crd::ensure_crd_created::<Restart>(&client).await?;
    stackable_operator::crd::ensure_crd_created::<Start>(&client).await?;
    stackable_operator::crd::ensure_crd_created::<Stop>(&client).await?;

    tokio::join!(
        stackable_spark_operator::create_controller(client.clone()),
        stackable_operator::command_controller::create_command_controller::<Restart, SparkCluster>(
            client.clone()
        ),
        stackable_operator::command_controller::create_command_controller::<Start, SparkCluster>(
            client.clone()
        ),
        stackable_operator::command_controller::create_command_controller::<Stop, SparkCluster>(
            client.clone()
        )
    );

    Ok(())
}
