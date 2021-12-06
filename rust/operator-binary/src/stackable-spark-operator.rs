use crate::kube::runtime::Controller;
use clap::{crate_version, App, AppSettings, SubCommand};
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service};
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::product_config::ProductConfigManager;
use stackable_operator::{cli, client, error, kube};
use stackable_spark_crd::{Restart, SparkCluster, Start, Stop};
use std::str::FromStr;
use tracing::error;

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::logging::initialize_logging("SPARK_OPERATOR_LOG");
    let opts = Opts::from_args();
    match opts.cmd {
        Cmd::Crd => println!("{}{}", serde_yaml::to_string(&SparkCluster::crd())?,),
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
                ProductConfigManager::from_str(include_str!("deploy/config-spec/properties.yaml"))?
            };
            let kube = kube::Client::try_default().await?;
        }
    }

    let kube = kube::Client::try_default().await?;
    let apiSpark = kube::Api::<SparkCluster>::all(kube.clone());
    // This will wait for (but not create) all CRDs we need.
    if let Err(error) = stackable_operator::crd::wait_until_crds_present(
        &kube,
        vec![SparkCluster::crd_name()],
        None,
    )
    .await
    {
        error!("Required CRDs missing, aborting: {:?}", error);
        return Err(error);
    };

    let controller_builder = Controller::new(zks.clone(), ListParams::default());
    let controller = controller_builder
        .owns(
            kube::Api::<Service>::all(kube.clone()),
            ListParams::default(),
        )
        .watches(
            kube::Api::<Endpoints>::all(kube.clone()),
            ListParams::default(),
            move |endpoints| {
                zk_store
                    .state()
                    .into_iter()
                    .filter(move |zk| {
                        zk.metadata.namespace == endpoints.metadata.namespace
                            && zk.server_role_service_name() == endpoints.metadata.name
                    })
                    .map(|zk| ObjectRef::from_obj(&zk))
            },
        )
        .owns(
            kube::Api::<StatefulSet>::all(kube.clone()),
            ListParams::default(),
        )
        .owns(
            kube::Api::<ConfigMap>::all(kube.clone()),
            ListParams::default(),
        )
        .run(
            zk_controller::reconcile_zk,
            zk_controller::error_policy,
            Context::new(zk_controller::Ctx {
                kube: kube.clone(),
                product_config,
            }),
        );

    /*
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

     */

    Ok(())
}
