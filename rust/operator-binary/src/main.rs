mod error;
mod spark_controller;
mod util;

use futures::stream::StreamExt;
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service};
use stackable_operator::kube::api::{DynamicObject, ListParams};
use stackable_operator::kube::runtime::controller::{Context, Controller, ReconcilerAction};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::{CustomResourceExt, Resource};
use stackable_operator::product_config::ProductConfigManager;
use stackable_spark_crd::SparkCluster;
use std::str::FromStr;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(about = built_info::PKG_DESCRIPTION, author = "Stackable GmbH - info@stackable.de")]
struct Opts {
    #[structopt(subcommand)]
    cmd: Cmd,
}

#[derive(StructOpt)]
enum Cmd {
    /// Print CRD objects
    Crd,
    /// Run operator
    Run {
        /// Provides the path to a product-config file
        #[structopt(long, short = "p", value_name = "FILE")]
        product_config: Option<String>,
    },
}

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

/// Erases the concrete types of the controller result, so that we can merge the streams of multiple controllers for different resources.
///
/// In particular, we convert `ObjectRef<K>` into `ObjectRef<DynamicObject>` (which carries `K`'s metadata at runtime instead), and
/// `E` into the trait object `anyhow::Error`.
fn erase_controller_result_type<K: Resource, E: std::error::Error + Send + Sync + 'static>(
    res: Result<(ObjectRef<K>, ReconcilerAction), E>,
) -> anyhow::Result<(ObjectRef<DynamicObject>, ReconcilerAction)> {
    let (obj_ref, action) = res?;
    Ok((obj_ref.erase(), action))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stackable_operator::logging::initialize_logging("SPARK_OPERATOR_LOG");
    let opts = Opts::from_args();
    match opts.cmd {
        Cmd::Crd => println!("{}", serde_yaml::to_string(&SparkCluster::crd())?,),
        Cmd::Run { product_config } => {
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = if let Some(product_config_path) = product_config {
                ProductConfigManager::from_yaml_file(&product_config_path)?
            } else {
                ProductConfigManager::from_str(include_str!(
                    "../../../deploy/config-spec/properties.yaml"
                ))?
            };
            let client =
                stackable_operator::client::create_client(Some("spark.stackable.tech".to_string()))
                    .await?;
            let controller_builder =
                Controller::new(client.get_all_api::<SparkCluster>(), ListParams::default());
            let sc_store = controller_builder.store();
            let controller = controller_builder
                .owns(client.get_all_api::<Service>(), ListParams::default())
                .watches(
                    client.get_all_api::<Endpoints>(),
                    ListParams::default(),
                    move |endpoints| {
                        sc_store
                            .state()
                            .into_iter()
                            .filter(move |sc| {
                                let _ = &endpoints;
                                sc.metadata.namespace == endpoints.metadata.namespace
                                    && sc.server_role_service_name() == endpoints.metadata.name
                            })
                            .map(|sc| ObjectRef::from_obj(&sc))
                    },
                )
                .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
                .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
                .run(
                    spark_controller::reconcile,
                    spark_controller::error_policy,
                    Context::new(spark_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                );

            controller
                .map(erase_controller_result_type)
                .for_each(|res| async {
                    match res {
                        Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                        Err(err) => {
                            tracing::error!(
                                error = &*err as &dyn std::error::Error,
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
