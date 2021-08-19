use clap::{crate_version, App, AppSettings, SubCommand};
use stackable_operator::crd::CustomResourceExt;
use stackable_operator::{cli, client};
use stackable_spark_crd::SparkCluster;
use stackable_spark_crd::{Restart, Start, Stop};
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stackable_operator::logging::initialize_logging("SPARK_OPERATOR_LOG");

    info!("Starting Stackable Operator for Apache Spark");

    // Handle CLI arguments
    let matches = App::new("Spark Operator")
        .author("Stackable GmbH - info@stackable.de")
        .about("Stackable Operator for Apache Spark")
        .version(crate_version!())
        .arg(cli::generate_productconfig_arg())
        .subcommand(
            SubCommand::with_name("crd")
                .setting(AppSettings::ArgRequiredElseHelp)
                .subcommand(cli::generate_crd_subcommand::<SparkCluster>())
                .subcommand(cli::generate_crd_subcommand::<Restart>())
                .subcommand(cli::generate_crd_subcommand::<Start>())
                .subcommand(cli::generate_crd_subcommand::<Stop>()),
        )
        .get_matches();

    if let ("crd", Some(subcommand)) = matches.subcommand() {
        if cli::handle_crd_subcommand::<SparkCluster>(subcommand)?
            || cli::handle_crd_subcommand::<Restart>(subcommand)?
            || cli::handle_crd_subcommand::<Start>(subcommand)?
            || cli::handle_crd_subcommand::<Stop>(subcommand)?
        {
            return Ok(());
        }
    }

    let paths = vec![
        "deploy/config-spec/properties.yaml",
        "/etc/stackable/spark-operator/config-spec/properties.yaml",
    ];
    let product_config_path = cli::handle_productconfig_arg(&matches, paths)?;

    let client = client::create_client(Some("spark.stackable.tech".to_string())).await?;

    // This will wait for (but not create) all CRDs we need.
    if let Err(error) = stackable_operator::crd::wait_until_crds_present(
        &client,
        vec![
            &SparkCluster::crd_name(),
            &Restart::crd_name(),
            &Start::crd_name(),
            &Stop::crd_name(),
        ],
        None,
    )
    .await
    {
        error!("Required CRDs missing, aborting: {:?}", error);
        return Err(error.into());
    };

    tokio::try_join!(
        stackable_spark_operator::create_controller(client.clone(), &product_config_path),
        stackable_operator::command_controller::create_command_controller::<Restart, SparkCluster>(
            client.clone()
        ),
        stackable_operator::command_controller::create_command_controller::<Start, SparkCluster>(
            client.clone()
        ),
        stackable_operator::command_controller::create_command_controller::<Stop, SparkCluster>(
            client.clone()
        )
    )?;

    Ok(())
}
