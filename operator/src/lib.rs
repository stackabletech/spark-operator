#![feature(backtrace)]
mod error;

use crate::error::Error;

use kube::Api;
use tracing::{debug, error, info, trace};

use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::{ListParams, Meta};
use serde_json::json;

use stackable_operator::client::Client;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::{create_config_map, finalizer, metadata, podutils, reconcile};
use stackable_spark_crd::{SparkCluster, SparkClusterSpec, SparkClusterStatus};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

const FINALIZER_NAME: &str = "spark.stackable.de/cleanup";

type SparkReconcileResult = ReconcileResult<error::Error>;

struct SparkState {
    context: ReconciliationContext<SparkCluster>,
    spec: SparkClusterSpec,
    status: Option<SparkClusterStatus>,
}

impl SparkState {
    pub async fn update_cluster_status(&self) -> SparkReconcileResult {
        info!("update_cluster_status: {:?}", &self.status);

        //let cluster_status_version = if self.status.is_some() {
        //    &self.status.as_ref().unwrap().image.version
        //};

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn read_cluster_status(&self) -> SparkReconcileResult {
        info!("read_cluster_status: {:?}", &self.status);
        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn reconcile_cluster(&self) -> SparkReconcileResult {
        info!("reconcile_cluster: {:?}", &self.spec);
        Ok(ReconcileFunctionAction::Continue)
    }

    async fn create_pod(&self, spark_cluster_spec: &SparkClusterSpec) -> Result<Pod, Error> {
        let pod = self.build_pod(spark_cluster_spec)?;
        Ok(self.context.client.create(&pod).await?)
    }

    fn build_pod(&self, spark_cluster_spec: &SparkClusterSpec) -> Result<Pod, Error> {
        let (containers, volumes) = self.build_containers(spark_cluster_spec);

        Ok(Pod {
            metadata: metadata::build_metadata(
                self.get_pod_name(spark_cluster_spec),
                None, //Some(self.build_labels(id)),
                &self.context.resource,
            )?,
            spec: Some(PodSpec {
                //node_name: Some(spark_cluster_spec.node_name.clone()),
                //tolerations: Some(stackable_operator::create_tolerations()),
                containers,
                volumes: Some(volumes),
                ..PodSpec::default()
            }),

            ..Pod::default()
        })
    }

    fn build_containers(
        &self,
        spark_cluster_spec: &SparkClusterSpec,
    ) -> (Vec<Container>, Vec<Volume>) {
        let version = self.context.resource.spec.master.clone();
        let image_name = format!(
            "stackable/spark:{}",
            serde_json::json!(version).as_str().unwrap()
        );

        let containers = vec![Container {
            image: Some(image_name),
            name: "spark".to_string(),
            command: Some(vec![
                "bin/zkServer.sh".to_string(),
                "start-foreground".to_string(),
                // "--config".to_string(), TODO: Version 3.4 does not support --config but later versions do
                "{{ configroot }}/conf/zoo.cfg".to_string(), // TODO: Later versions can probably point to a directory instead, investigate
            ]),
            volume_mounts: Some(vec![
                // One mount for the config directory, this will be relative to the extracted package
                VolumeMount {
                    mount_path: "conf".to_string(),
                    name: "config-volume".to_string(),
                    ..VolumeMount::default()
                },
                // We need a second mount for the data directory
                // because we need to write the myid file into the data directory
                VolumeMount {
                    mount_path: "/tmp/spark-events".to_string(), // TODO: get log dir from crd
                    name: "data-volume".to_string(),
                    ..VolumeMount::default()
                },
            ]),
            ..Container::default()
        }];

        let cm_name_prefix = format!("zk-{}", self.get_pod_name(spark_cluster_spec));
        let volumes = vec![
            Volume {
                name: "config-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", cm_name_prefix)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
            Volume {
                name: "data-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-data", cm_name_prefix)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
        ];

        (containers, volumes)
    }

    /// All pod names follow a simple pattern: <name of ZooKeeperCluster object>-<Node name>
    fn get_pod_name(&self, spark_cluster_spec: &SparkClusterSpec) -> String {
        // TODO: get name
        format!("{}-{}", self.context.name(), "spark-master")
    }
}

impl ReconciliationState for SparkState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        Box::pin(async move {
            self.update_cluster_status()
                .await?
                .then(self.read_cluster_status())
                .await?
                .then(self.reconcile_cluster())
                .await
        })
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
            spec: context.resource.spec.clone(),
            status: context.resource.status.clone(),
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
