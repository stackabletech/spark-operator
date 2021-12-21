//! Ensures that `Pod`s are configured and running for each [`SparkCluster`]

use crate::error::Error;
use crate::error::Error::*;
use stackable_operator::k8s_openapi::api::core::v1::ContainerPort;
use stackable_operator::product_config_utils::Configuration;
use stackable_operator::role_utils::{Role, RoleGroupRef};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EnvVar, HTTPGetAction, PersistentVolumeClaim,
                PersistentVolumeClaimSpec, Probe, ResourceRequirements, Service, ServicePort,
                ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::{
            api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
        },
    },
    kube::{
        api::ObjectMeta,
        runtime::{
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
        },
    },
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
};
use stackable_spark_crd::constants::*;
use stackable_spark_crd::{SparkCluster, SparkRole};
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

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

/// The main reconcile loop.
///
/// For each rolegroup a [`StatefulSet`] and a [`ClusterIP`] service is created.
pub async fn reconcile(sc: SparkCluster, ctx: Context<Ctx>) -> Result<ReconcilerAction, Error> {
    tracing::info!("Starting reconcile");
    let sc_ref = ObjectRef::from_obj(&sc);
    let client = &ctx.get_ref().client;

    let validated_config = validate_all_roles_and_groups_config(
        version(&sc)?,
        &transform_all_roles_to_config(&sc, build_spark_role_properties(&sc)),
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .map_err(|e| Error::InvalidProductConfig {
        source: e,
        sc: sc_ref.clone(),
    })?;

    // Extract the master default group ports here because they are needed twice:
    // 1. For the NodePort service built by build_master_role_service()
    // 2. For the worker's master URL built with build_worker_stateful_set()
    let default_master_role_ports = validated_config
        .iter()
        .filter(|(role_name, _groups)| SparkRole::Master.to_string().eq(*role_name))
        .flat_map(|(_role_name, groups)| groups)
        .filter(|(group_name, _group_config)| "default".eq(*group_name))
        .take(1)
        .next()
        .map(|(rolegroup, rolegroup_config)| {
            build_ports(
                &sc,
                &sc.server_rolegroup_ref(SparkRole::Master.to_string(), rolegroup),
                rolegroup_config,
            )
        })
        .ok_or(MasterRoleGroupDefaultExpected)??;

    let master_role_service = build_master_role_service(
        &sc,
        default_master_role_ports
            .iter()
            .map(|(name, value)| ServicePort {
                name: Some(name.clone()),
                port: *value,
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            })
            .collect(),
    )?;

    client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &master_role_service,
            &master_role_service,
        )
        .await
        .map_err(|e| ApplyRoleService {
            source: e,
            sc: sc_ref.clone(),
        })?;

    for (role_name, group_config) in validated_config.iter() {
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup = sc.server_rolegroup_ref(role_name, rolegroup_name);
            let rg_service = build_rolegroup_service(&sc, &rolegroup, rolegroup_config)?;
            let rg_configmap = build_rolegroup_config_map(&sc, &rolegroup, rolegroup_config)?;
            let rg_statefulset = build_rolegroup_statefulset(
                &sc,
                &default_master_role_ports,
                &rolegroup,
                rolegroup_config,
            )?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
                .await
                .map_err(|e| ApplyRoleGroupService {
                    source: e,
                    rolegroup: rolegroup.clone(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
                .await
                .map_err(|e| ApplyRoleGroupConfig {
                    source: e,
                    rolegroup: rolegroup.clone(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
                .await
                .map_err(|e| ApplyRoleGroupStatefulSet {
                    source: e,
                    rolegroup: rolegroup.clone(),
                })?;
        }
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// Build the [`NodePort`] service for clients.
fn build_master_role_service(
    sc: &SparkCluster,
    service_ports: Vec<ServicePort>,
) -> Result<Service, Error> {
    let role_name = SparkRole::Master.to_string();
    let role_svc_name = sc
        .server_role_service_name()
        .ok_or(GlobalServiceNameNotFound {
            obj_ref: ObjectRef::from_obj(sc),
        })?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&role_svc_name)
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|e| ObjectMissingMetadataForOwnerRef {
                source: e,
                obj_ref: ObjectRef::from_obj(sc),
            })?
            .with_recommended_labels(sc, APP_NAME, version(sc)?, &role_name, "global")
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(service_ports),
            selector: Some(role_selector_labels(sc, APP_NAME, &role_name)),
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
                .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
                    source: e,
                    obj_ref: ObjectRef::from_obj(sc),
                })?
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
                .ok_or_else(|| SerializeSparkDefaults {
                    rolegroup: rolegroup.clone(),
                })?,
        )
        .add_data(
            SPARK_ENV_SH,
            rolegroup_config
                .get(&PropertyNameKind::File(SPARK_ENV_SH.to_string()))
                .map(|c| convert_map_to_string(c, "="))
                .ok_or_else(|| SerializeSparkEnv {
                    rolegroup: rolegroup.clone(),
                })?,
        )
        .add_data(
            SPARK_METRICS_PROPERTIES,
            rolegroup_config
                .get(&PropertyNameKind::File(
                    SPARK_METRICS_PROPERTIES.to_string(),
                ))
                .and(sc.enable_monitoring())
                .filter(|&monitoring_enabled_flag| monitoring_enabled_flag)
                .map(|_| "\
                            *.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet\n\
                            *.sink.prometheusServlet.path=/metrics\n\
                            *.source.jvm.class=org.apache.spark.metrics.source.JvmSource")
                .unwrap_or_default()
        )
        .build()
        .map_err(|e| Error::BuildRoleGroupConfig {
            source: e,
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    sc: &SparkCluster,
    rolegroup: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Service, Error> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
                source: e,
                obj_ref: ObjectRef::from_obj(sc),
            })?
            .with_recommended_labels(
                sc,
                APP_NAME,
                version(sc)?,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(
                build_ports(sc, rolegroup, rolegroup_config)?
                    .iter()
                    .map(|(name, value)| ServicePort {
                        name: Some(name.clone()),
                        port: *value,
                        protocol: Some("TCP".to_string()),
                        ..ServicePort::default()
                    })
                    .collect(),
            ),
            selector: Some(role_group_selector_labels(
                sc,
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
/// * `sc`                       - The cluster resource object.
/// * `default_master_role_ports` - Master role service (and container ports). Used to build the master URLs needed by the worker pods.
/// * `rolegroup`                 - The rolegroup.
/// * `rolegroup_config`          - The validated configuration for the rolegroup.
///
fn build_rolegroup_statefulset(
    sc: &SparkCluster,
    default_master_role_ports: &[(String, i32)],
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    match serde_yaml::from_str(&rolegroup_ref.role).unwrap() {
        SparkRole::Master => build_master_stateful_set(sc, rolegroup_ref, rolegroup_config),
        SparkRole::Worker => build_worker_stateful_set(
            sc,
            default_master_role_ports,
            rolegroup_ref,
            rolegroup_config,
        ),
        SparkRole::HistoryServer => build_history_stateful_set(sc, rolegroup_ref, rolegroup_config),
    }
}

/// Build the [`StatefulSet`]s for the given [`SparkRole::Worker`] rolegroup.
///
/// # Arguments
/// * `_sc`                       - The cluster resource object.
/// * `default_master_role_ports` - Master role service (and container ports). Used to build the master URLs needed by the worker pods.
/// * `rolegroup`                 - The rolegroup.
/// * `rolegroup_config`          - The validated configuration for the rolegroup.
///
fn build_worker_stateful_set(
    sc: &SparkCluster,
    default_master_role_ports: &[(String, i32)],
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    let rolegroup = sc
        .spec
        .workers
        .as_ref()
        .ok_or(Error::MissingRoleGroup {
            obj_ref: rolegroup_ref.clone(),
        })?
        .role_groups
        .get(&rolegroup_ref.role_group);
    let sc_version = version(sc)?;
    let image = format!(
        "docker.stackable.tech/stackable/spark:{}-stackable0",
        sc_version
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

    let container_sc = ContainerBuilder::new("worker")
        .image(image)
        .args(vec![
            "sbin/start-slave.sh".to_string(),
            build_master_service_url(rolegroup_ref, default_master_role_ports),
        ])
        .readiness_probe(PROBE.clone())
        .liveness_probe(PROBE.clone())
        .add_env_vars(env)
        .add_container_ports(build_container_ports(sc, rolegroup_ref, rolegroup_config)?)
        .add_volume_mount("log", spark_log_dir(rolegroup_config))
        .add_volume_mount("config", spark_conf_dir(rolegroup_config))
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
                source: e,
                obj_ref: ObjectRef::from_obj(sc),
            })?
            .with_recommended_labels(
                sc,
                APP_NAME,
                sc_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if sc.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.and_then(|rg| rg.replicas).map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    sc,
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
                        sc,
                        APP_NAME,
                        sc_version,
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

/// Build the [`StatefulSet`]s for the given [`SparkRole::HistoryServer`] rolegroup.
///
/// # Arguments
/// * `_sc`              - The cluster resource object.
/// * `rolegroup`        - The rolegroup.
/// * `rolegroup_config` - The validated configuration for the rolegroup.
///
fn build_history_stateful_set(
    sc: &SparkCluster,
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    let rolegroup = sc
        .spec
        .history_servers
        .as_ref()
        .ok_or(Error::MissingRoleGroup {
            obj_ref: rolegroup_ref.clone(),
        })?
        .role_groups
        .get(&rolegroup_ref.role_group);
    let sc_version = version(sc)?;
    let image = format!(
        "docker.stackable.tech/stackable/spark:{}-stackable0",
        sc_version
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

    let container_sc = ContainerBuilder::new("history")
        .image(image)
        .args(vec!["sbin/start-history-server.sh".to_string()])
        .add_env_vars(env)
        .add_container_ports(build_container_ports(sc, rolegroup_ref, rolegroup_config)?)
        .liveness_probe(PROBE.clone())
        .readiness_probe(PROBE.clone())
        .add_volume_mount("log", spark_log_dir(rolegroup_config))
        .add_volume_mount("config", spark_conf_dir(rolegroup_config))
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
                source: e,
                obj_ref: ObjectRef::from_obj(sc),
            })?
            .with_recommended_labels(
                sc,
                APP_NAME,
                sc_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if sc.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.and_then(|rg| rg.replicas).map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    sc,
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
                        sc,
                        APP_NAME,
                        sc_version,
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

/// Build the [`StatefulSet`]s for the given [`SparkRole::Master`] rolegroup.
///
/// # Arguments
/// * `_sc`              - The cluster resource object.
/// * `rolegroup`        - The rolegroup.
/// * `rolegroup_config` - The validated configuration for the rolegroup.
///
fn build_master_stateful_set(
    sc: &SparkCluster,
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    let rolegroup = sc
        .spec
        .masters
        .as_ref()
        .ok_or(Error::MissingRoleGroup {
            obj_ref: rolegroup_ref.clone(),
        })?
        .role_groups
        .get(&rolegroup_ref.role_group);
    let sc_version = version(sc)?;
    let image = format!(
        "docker.stackable.tech/stackable/spark:{}-stackable0",
        sc_version
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

    let container_sc = ContainerBuilder::new(&SparkRole::Master.to_string())
        .image(image)
        .args(vec!["sbin/start-master.sh".to_string()])
        .add_env_vars(env)
        .readiness_probe(PROBE.clone())
        .liveness_probe(PROBE.clone())
        .add_container_ports(build_container_ports(sc, rolegroup_ref, rolegroup_config)?)
        .add_volume_mount("log", spark_log_dir(rolegroup_config))
        .add_volume_mount("config", spark_conf_dir(rolegroup_config))
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
                source: e,
                obj_ref: ObjectRef::from_obj(sc),
            })?
            .with_recommended_labels(
                sc,
                APP_NAME,
                sc_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if sc.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.and_then(|rg| rg.replicas).map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    sc,
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
                        sc,
                        APP_NAME,
                        sc_version,
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

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
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
            (
                pnk.clone(),
                masters.clone().erase(),
            ),
        );
    }
    if let Some(workers) = &resource.spec.workers {
        result.insert(
            SparkRole::Worker.to_string(),
            (
                pnk.clone(),
                workers.clone().erase(),
            ),
        );
    }
    if let Some(history_servers) = &resource.spec.history_servers {
        result.insert(
            SparkRole::HistoryServer.to_string(),
            (
                pnk.clone(),
                history_servers.clone().erase(),
            ),
        );
    }
    result
}

/// Build [`ContainerPort`]s for the given rolegroup.
///
/// # Arguments
/// * `_sc`              - The cluster resource object.
/// * `rolegroup`        - The rolegroup for which to extract the pods.
/// * `rolegroup_config` - The validated configuration for the rolegroup.
///
fn build_container_ports(
    _sc: &SparkCluster,
    rolegroup: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Vec<ContainerPort>, Error> {
    Ok(build_ports(_sc, rolegroup, rolegroup_config)?
        .iter()
        .map(|(name, value)| ContainerPort {
            name: Some(name.clone()),
            container_port: *value,
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        })
        .collect())
}

/// Extract all named ports from the given validated configuration.
///
/// # Arguments
/// * `_sc`              - The cluster resource object.
/// * `rolegroup`        - The rolegroup for which to extract the pods.
/// * `rolegroup_config` - The validated configuration for the rolegroup.
///
fn build_ports(
    _sc: &SparkCluster,
    rolegroup: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Vec<(String, i32)>, Error> {
    Ok(match serde_yaml::from_str(&rolegroup.role).unwrap() {
        SparkRole::Master => vec![
            (
                PORT_NAME_SPARK.to_string(),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                    .and_then(|c| c.get(SPARK_ENV_MASTER_PORT))
                    .unwrap_or(&String::from("7077"))
                    .parse::<i32>()
                    .map_err(|e| Error::InvalidPort {
                        source: e,
                        rolegroup_ref: rolegroup.clone(),
                    })?,
            ),
            (
                String::from(PORT_NAME_WEB),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                    .and_then(|c| c.get(SPARK_ENV_MASTER_WEBUI_PORT))
                    .unwrap_or(&String::from("8080"))
                    .parse::<i32>()
                    .map_err(|e| Error::InvalidPort {
                        source: e,
                        rolegroup_ref: rolegroup.clone(),
                    })?,
            ),
        ],
        SparkRole::Worker => vec![
            (
                PORT_NAME_SPARK.to_string(),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                    .and_then(|c| c.get(SPARK_ENV_WORKER_PORT))
                    .unwrap_or(&String::from("7077"))
                    .parse::<i32>()
                    .map_err(|e| Error::InvalidPort {
                        source: e,
                        rolegroup_ref: rolegroup.clone(),
                    })?,
            ),
            (
                String::from(PORT_NAME_WEB),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                    .and_then(|c| c.get(SPARK_ENV_WORKER_WEBUI_PORT))
                    .unwrap_or(&String::from("8080"))
                    .parse::<i32>()
                    .map_err(|e| Error::InvalidPort {
                        source: e,
                        rolegroup_ref: rolegroup.clone(),
                    })?,
            ),
        ],
        SparkRole::HistoryServer => vec![(
            String::from(PORT_NAME_WEB),
            rolegroup_config
                .get(&PropertyNameKind::File(String::from(SPARK_DEFAULTS_CONF)))
                .and_then(|c| c.get(SPARK_DEFAULTS_HISTORY_WEBUI_PORT))
                .unwrap_or(&String::from("8080"))
                .parse::<i32>()
                .map_err(|e| Error::InvalidPort {
                    source: e,
                    rolegroup_ref: rolegroup.clone(),
                })?,
        )],
    })
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

/// Build the connection string for the start-worker.sh script
///
/// # Arguments
/// * `rolegroup_ref`             - The worker's RoleGroupRef.
/// * `default_master_role_ports` - The ports used to create the master's ClusterIP service.
///
fn build_master_service_url(
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    default_master_role_ports: &[(String, i32)],
) -> String {
    let default_master_port = default_master_role_ports
        .iter()
        .filter_map(|(name, value)| match name.as_ref() {
            PORT_NAME_SPARK => Some(*value),
            _ => None,
        })
        .take(1)
        .next()
        .unwrap_or(7077);

    format!(
        "spark://{}:{}",
        RoleGroupRef {
            cluster: rolegroup_ref.cluster.clone(),
            role: SparkRole::Master.to_string(),
            role_group: "default".to_string(),
        }
        .object_name(),
        default_master_port
    )
}

/// Extract the SPARK_CONF_DIR path from the validated rolegroup configuration.
///
/// # Arguments
/// * `rolegroup_config` - Validated config for a rolegroup.
///
fn spark_conf_dir(
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> String {
    rolegroup_config
        .get(&PropertyNameKind::Env)
        .iter()
        .flat_map(|env_vars| env_vars.iter())
        .filter_map(|(k, v)| match k.as_ref() {
            SPARK_CONF_DIR => Some(v.clone()),
            _ => None,
        })
        .take(1)
        .next()
        .unwrap_or_else(|| "/stackable/config".to_string())
}

/// Extract the log path from the validated rolegroup configuration.
///
/// # Arguments
/// * `rolegroup_config` - Validated config for a rolegroup.
///
fn spark_log_dir(rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>) -> String {
    rolegroup_config
        .get(&PropertyNameKind::File(String::from(SPARK_DEFAULTS_CONF)))
        .iter()
        .flat_map(|vars| vars.iter())
        .filter_map(|(k, v)| match k.as_ref() {
            SPARK_DEFAULTS_EVENT_LOG_DIR => Some(v.clone()),
            _ => None,
        })
        .take(1)
        .next()
        .unwrap_or_else(|| DEFAULT_LOG_DIR.to_string())
}

fn version(sc: &SparkCluster) -> Result<&str, Error> {
    sc.spec.version.as_deref().ok_or(ObjectHasNoVersion {
        obj_ref: ObjectRef::from_obj(sc),
    })
}
