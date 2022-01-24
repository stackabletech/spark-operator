mod spark_controller;

#[macro_use]
extern crate lazy_static;

use clap::Parser;
use futures::stream::StreamExt;
use stackable_operator::cli::{Command, ProductOperatorRun};
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Service};
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::controller::{Context, Controller};
use stackable_operator::kube::CustomResourceExt;
use stackable_spark_crd::SparkCluster;

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stackable_operator::logging::initialize_logging("SPARK_OPERATOR_LOG");
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => println!("{}", serde_yaml::to_string(&SparkCluster::crd())?,),
        Command::Run(ProductOperatorRun { product_config }) => {
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );

            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/spark-operator/config-spec/properties.yaml",
            ])?;

            let client =
                stackable_operator::client::create_client(Some("spark.stackable.tech".to_string()))
                    .await?;

            Controller::new(client.get_all_api::<SparkCluster>(), ListParams::default())
                .owns(client.get_all_api::<Service>(), ListParams::default())
                .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
                .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
                .shutdown_on_signal()
                .run(
                    spark_controller::reconcile,
                    spark_controller::error_policy,
                    Context::new(spark_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                )
                .for_each(|res| async {
                    match res {
                        Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                        Err(err) => {
                            tracing::error!(
                                error = &err as &dyn std::error::Error,
                                "Failed to reconcile object",
                            )
                        }
                    }
                })
                .await;
        }
    }

    Ok(())
}
