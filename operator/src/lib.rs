#![feature(backtrace)]
mod error;

use kube::api::ListParams;
use kube::Api;

use stackable_operator::client::Client;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};

use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use stackable_spark_crd::{SparkCluster, SparkClusterSpec};
use std::pin::Pin;
use tokio::macros::support::Future;

const FINALIZER_NAME: &str = "spark.stackable.de/cleanup";

type SparkReconcileResult = ReconcileResult<error::Error>;

struct SparkState {
    context: ReconciliationContext<SparkCluster>,
    spark_spec: SparkClusterSpec,
}

impl SparkState {
    pub async fn delete_excess_pods(&self) -> SparkReconcileResult {
        Ok(ReconcileFunctionAction::Continue)
    }
}

impl ReconciliationState for SparkState {
    type Error = error::Error;

    fn reconcile_operations(
        &self,
    ) -> Vec<Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + '_>>> {
        let vec: Vec<Pin<Box<dyn Future<Output = SparkReconcileResult> + '_>>> =
            vec![Box::pin(self.delete_excess_pods())];

        vec
    }
}

#[derive(Debug)]
struct SparkStrategy {}

impl SparkStrategy {
    pub fn new() -> SparkStrategy {
        SparkStrategy {}
    }
}

impl ControllerStrategy for SparkStrategy {
    type Item = SparkCluster;
    type State = SparkState;

    fn finalizer_name(&self) -> String {
        return FINALIZER_NAME.to_string();
    }

    fn init_reconcile_state(&self, context: ReconciliationContext<Self::Item>) -> Self::State {
        SparkState {
            spark_spec: context.resource.spec.clone(),
            context,
        }
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let spark_api: Api<SparkCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(spark_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let strategy = SparkStrategy::new();

    controller.run(client, strategy).await;
}
