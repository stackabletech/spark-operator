use anyhow::Result;
use clap::{crate_version, App, Arg, ArgMatches, SubCommand};
use stackable_operator::crd::CustomResourceExt;
use stackable_operator::{client, error};
use stackable_spark_crd::SparkCluster;
use stackable_spark_crd::{Restart, Start, Stop};
use tracing::{error, info};

fn create_subcommand<'a, 'b>(name: &'static str) -> App<'a, 'b> {
    SubCommand::with_name(name)
        .about("CRD stuff")
        .arg(Arg::with_name("print").short("p").long("print"))
        .arg(Arg::with_name("save").short("s"))
}

fn handle_subcommand<T>(matches: Option<&ArgMatches>) -> Result<()>
where
    T: CustomResourceExt,
{
    let matches = match matches {
        Some(matches) => matches,
        None => return Ok(()),
    };
    if matches.is_present("print") {
        T::print_yaml_schema()?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::logging::initialize_logging("SPARK_OPERATOR_LOG");

    info!("Starting Stackable Operator for Apache Spark");

    let matches = App::new("Spark Operator")
        .version(crate_version!())
        .subcommand(create_subcommand("crd-sparkcluster"))
        .subcommand(create_subcommand("crd-restart"))
        .subcommand(create_subcommand("crd-start"))
        .subcommand(create_subcommand("crd-stop"))
        .get_matches();

    handle_subcommand::<SparkCluster>(matches.subcommand_matches("crd-sparkcluster"));
    handle_subcommand::<>

    return Ok(());

    let client = client::create_client(Some("spark.stackable.tech".to_string())).await?;

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
        return Err(error);
    };

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
