//! Ensures that `Pod`s are configured and running for each [`SparkCluster`]

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EnvVar, HTTPGetAction, PersistentVolumeClaim,
                PersistentVolumeClaimSpec, Probe, ResourceRequirements, Service, ServiceSpec,
                Volume,
            },
        },
        apimachinery::pkg::{
            api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
        },
    },
    kube::{
        api::ObjectMeta,
        runtime::controller::{Action, Context},
    },
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::Configuration,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::{Role, RoleGroupRef},
};
use stackable_spark_crd::{constants::*, SparkCluster, SparkRole};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

const FIELD_MANAGER_SCOPE: &str = "sparkcluster";

lazy_static! {
    /// Liveliness probe used by all master, worker and history containers.
    static ref PROBE: Probe = Probe {
        http_get: Some(HTTPGetAction {
            port: IntOrString::String(String::from(PORT_NAME_WEB)),
            ..HTTPGetAction::default()
        }),
        period_seconds: Some(10),
        initial_delay_seconds: Some(10),
        ..Probe::default()
    };
}

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to apply global service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<SparkCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<SparkCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<SparkCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<SparkCluster>,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to serialize spark-defaults.conf for {rolegroup}"))]
    SerializeSparkDefaults {
        rolegroup: RoleGroupRef<SparkCluster>,
    },
    #[snafu(display("failed to serialize spark-env.sh for {rolegroup}"))]
    SerializeSparkEnv {
        rolegroup: RoleGroupRef<SparkCluster>,
    },
    #[snafu(display("failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("internal operator failure"))]
    InternalOperatorFailure { source: stackable_spark_crd::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

/// The main reconcile loop.
///
/// For each rolegroup a [`StatefulSet`] and a `ClusterIP` service is created.
pub async fn reconcile(spark: Arc<SparkCluster>, ctx: Context<Ctx>) -> Result<Action, Error> {
    tracing::info!("Starting reconcile");
    let client = &ctx.get_ref().client;

    let validated_config = validate_all_roles_and_groups_config(
        version(&*spark)?,
        &transform_all_roles_to_config(&*spark, build_spark_role_properties(&*spark))
            .context(ProductConfigTransformSnafu)?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let master_role_service = build_master_role_service(&spark)?;

    client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &master_role_service,
            &master_role_service,
        )
        .await
        .context(ApplyRoleServiceSnafu)?;

    for (role_name, group_config) in validated_config.iter() {
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let role = &SparkRole::from_str(role_name).context(InternalOperatorFailureSnafu)?;

            let rolegroup = role.rolegroup_ref(&spark, rolegroup_name);
            let rg_service = build_rolegroup_service(&spark, role, &rolegroup)?;
            let rg_configmap = build_rolegroup_config_map(&spark, &rolegroup, rolegroup_config)?;
            let rg_statefulset =
                build_rolegroup_statefulset(&spark, role, &rolegroup, rolegroup_config)?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
        }
    }

    Ok(Action::await_change())
}

/// Build the `NodePort` service for clients.
fn build_master_role_service(spark: &SparkCluster) -> Result<Service, Error> {
    let role = SparkRole::Master;
    let role_svc_name = spark
        .master_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark)
            .name(&role_svc_name)
            .ownerreference_from_resource(spark, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                spark,
                APP_NAME,
                version(spark)?,
                &role.to_string(),
                "global",
            )
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(role.service_ports()),
            selector: Some(role_selector_labels(spark, APP_NAME, &role.to_string())),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_rolegroup_config_map(
    sc: &SparkCluster,
    rolegroup: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<ConfigMap, Error> {
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(sc)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(sc, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(
                    sc,
                    APP_NAME,
                    version(sc)?,
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .build(),
        )
        .add_data(
            SPARK_DEFAULTS_CONF,
            rolegroup_config
                .get(&PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()))
                .map(|c| convert_map_to_string(c, " "))
                .with_context(|| SerializeSparkDefaultsSnafu {
                    rolegroup: rolegroup.clone(),
                })?,
        )
        .add_data(
            SPARK_ENV_SH,
            rolegroup_config
                .get(&PropertyNameKind::File(SPARK_ENV_SH.to_string()))
                .map(|c| convert_map_to_string(c, "="))
                .with_context(|| SerializeSparkEnvSnafu {
                    rolegroup: rolegroup.clone(),
                })?,
        )
        .add_data(
            SPARK_METRICS_PROPERTIES,
            rolegroup_config
                .get(&PropertyNameKind::File(
                    SPARK_METRICS_PROPERTIES.to_string(),
                ))
                .map(|c| convert_map_to_string(c, "="))
                .unwrap_or_default(),
        )
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    spark: &SparkCluster,
    role: &SparkRole,
    rolegroup: &RoleGroupRef<SparkCluster>,
) -> Result<Service, Error> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(spark, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                spark,
                APP_NAME,
                version(spark)?,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(role.service_ports()),
            selector: Some(role_group_selector_labels(
                spark,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// Build the [`StatefulSet`]s for the given rolegroup.
///
/// # Arguments
/// * `spark` - The cluster resource object.
/// * `role` - The SparkRole.
/// * `rolegroup` - The rolegroup.
/// * `rolegroup_config` - The validated configuration for the rolegroup.
///
fn build_rolegroup_statefulset(
    spark: &SparkCluster,
    role: &SparkRole,
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    let version = version(spark)?;

    let image = format!(
        "docker.stackable.tech/stackable/spark:{}-stackable0",
        version
    );
    let env = rolegroup_config
        .get(&PropertyNameKind::Env)
        .iter()
        .flat_map(|env_vars| env_vars.iter())
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();

    let container_sc = ContainerBuilder::new("spark")
        .image(image)
        .args(role.command(spark).context(InternalOperatorFailureSnafu)?)
        .readiness_probe(PROBE.clone())
        .liveness_probe(PROBE.clone())
        .add_env_vars(env)
        .add_container_ports(role.container_ports())
        .add_volume_mount("log", LOG_DIR)
        .add_volume_mount("config", CONF_DIR)
        .build();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(spark, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                spark,
                APP_NAME,
                version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if spark.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                role.replicas(spark, &rolegroup_ref.role_group)
                    .map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    spark,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: PodBuilder::new()
                .metadata_builder(|m| {
                    m.with_recommended_labels(
                        spark,
                        APP_NAME,
                        version,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                })
                .add_container(container_sc)
                .add_volume(Volume {
                    name: "config".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(rolegroup_ref.object_name()),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                })
                .build_template(),
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("log".to_string()),
                    ..ObjectMeta::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(ResourceRequirements {
                        requests: Some({
                            let mut map = BTreeMap::new();
                            map.insert("storage".to_string(), Quantity("1Gi".to_string()));
                            map
                        }),
                        ..ResourceRequirements::default()
                    }),
                    ..PersistentVolumeClaimSpec::default()
                }),
                ..PersistentVolumeClaim::default()
            }]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

/// TODO: this is pure boilerplate code that should be part of product-config.
fn build_spark_role_properties(
    resource: &SparkCluster,
) -> HashMap<
    String,
    (
        Vec<PropertyNameKind>,
        Role<impl Configuration<Configurable = SparkCluster>>,
    ),
> {
    let mut result = HashMap::new();
    let pnk: Vec<PropertyNameKind> = vec![
        PropertyNameKind::File(SPARK_ENV_SH.to_string()),
        PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
        PropertyNameKind::File(SPARK_METRICS_PROPERTIES.to_string()),
        PropertyNameKind::Env,
    ];
    if let Some(masters) = &resource.spec.masters {
        result.insert(
            SparkRole::Master.to_string(),
            (pnk.clone(), masters.clone().erase()),
        );
    }
    if let Some(workers) = &resource.spec.workers {
        result.insert(
            SparkRole::Worker.to_string(),
            (pnk.clone(), workers.clone().erase()),
        );
    }
    if let Some(history_servers) = &resource.spec.history_servers {
        result.insert(
            SparkRole::HistoryServer.to_string(),
            (pnk, history_servers.clone().erase()),
        );
    }
    result
}

/// Unroll a map into a String using a given assignment character (for writing config maps)
///
/// # Arguments
/// * `map`        - Map containing option_name:option_value pairs
/// * `assignment` - Used character to assign option_value to option_name (e.g. "=", " ", ":" ...)
///
fn convert_map_to_string(map: &BTreeMap<String, String>, assignment: &str) -> String {
    let mut data = String::new();
    for (key, value) in map {
        data.push_str(format!("{}{}{}\n", key, assignment, value).as_str());
    }
    data
}

fn version(spark: &SparkCluster) -> Result<&str, Error> {
    spark
        .spec
        .version
        .as_deref()
        .context(ObjectHasNoVersionSnafu)
}
