//! Ensures that `Pod`s are configured and running for each [`SparkCluster`]

use crate::config::convert_map_to_string;
use crate::error::Error;
use crate::error::Error::{
    ApplyRoleGroupConfig, ApplyRoleGroupService, ApplyRoleGroupStatefulSet, ApplyRoleService,
    ApplyStatus, GlobalServiceNameNotFound, ObjectMissingMetadataForOwnerRef,
    SerializeSparkDefaults, SerializeSparkEnv, SerializeSparkMetrics,
};
use crate::util::version;
use stackable_operator::k8s_openapi::api::core::v1::ContainerPort;
use stackable_operator::product_config_utils::Configuration;
use stackable_operator::role_utils::{Role, RoleGroupRef};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, EnvVar, EnvVarSource, ExecAction,
                ObjectFieldSelector, PersistentVolumeClaim, PersistentVolumeClaimSpec, Probe,
                ResourceRequirements, Service, ServicePort, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
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
use stackable_spark_crd::constants::{
    APP_NAME, APP_PORT, FIELD_MANAGER_SCOPE, SPARK_DEFAULTS_CONF,
    SPARK_DEFAULTS_HISTORY_WEBUI_PORT, SPARK_ENV_MASTER_PORT, SPARK_ENV_MASTER_WEBUI_PORT,
    SPARK_ENV_SH, SPARK_ENV_WORKER_PORT, SPARK_ENV_WORKER_WEBUI_PORT, SPARK_METRICS_PROPERTIES,
};
use stackable_spark_crd::{SparkCluster, SparkClusterSpec, SparkClusterStatus, SparkRole};
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

pub async fn reconcile(sc: SparkCluster, ctx: Context<Ctx>) -> Result<ReconcilerAction, Error> {
    tracing::info!("Starting reconcile");
    let sc_ref = ObjectRef::from_obj(&sc);
    let client = &ctx.get_ref().client;

    let sc_version = sc
        .spec
        .version
        .as_deref()
        .ok_or(Error::ObjectHasNoVersion {
            obj_ref: sc_ref.clone(),
        })?;
    let validated_config = validate_all_roles_and_groups_config(
        sc_version,
        &transform_all_roles_to_config(&sc, build_spark_role_properties(&sc)),
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .map_err(|e| Error::InvalidProductConfig {
        source: e,
        sc: sc_ref.clone(),
    })?;

    let server_role_service = build_server_role_service(&sc)?;
    let server_role_service = client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &server_role_service,
            &server_role_service,
        )
        .await
        .map_err(|e| ApplyRoleService {
            source: e,
            sc: sc_ref.clone(),
        })?;

    for (role_name, group_config) in validated_config.iter() {
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup = sc.server_rolegroup_ref(role_name, rolegroup_name);
            let rg_service = build_server_rolegroup_service(&sc, &rolegroup, rolegroup_config)?;
            let rg_configmap =
                build_server_rolegroup_config_map(&sc, &rolegroup, rolegroup_config)?;
            let rg_statefulset =
                build_rolegroup_statefulset(&sc, &rolegroup, rolegroup_config)?;
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

    // TODO: razvan
    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    // let mut discovery_hash = FnvHasher::with_key(0);
    // for discovery_cm in build_discovery_configmaps(&kube, &sc, &sc, &server_role_service, None)
    //     .await
    //     .with_context(|| BuildDiscoveryConfig { sc: sc_ref.clone() })?
    // {
    //     let discovery_cm = apply_owned(&kube, FIELD_MANAGER, &discovery_cm)
    //         .await
    //         .with_context(|| ApplyDiscoveryConfig { sc: sc_ref.clone() })?;
    //     if let Some(generation) = discovery_cm.metadata.resource_version {
    //         discovery_hash.write(generation.as_bytes())
    //     }
    // }

    let status = SparkClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some("TODO".to_string()),
    };
    let sc_with_status = {
        let mut sc_with_status = SparkCluster::new(&sc_ref.name, SparkClusterSpec::default());
        sc_with_status.metadata.namespace = sc.metadata.namespace.clone();
        sc_with_status.status = Some(status);
        sc_with_status
    };
    client
        .apply_patch_status(FIELD_MANAGER_SCOPE, &sc_with_status, &sc_with_status)
        .await
        .map_err(|e| ApplyStatus {
            source: e,
            sc: sc_ref.clone(),
        })?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
///
/// Note that you should generally *not* hard-code clients to use these services; instead, create a [`ZookeeperZnode`](`stackable_zookeeper_crd::ZookeeperZnode`)
/// and use the connection string that it gives you.
pub fn build_server_role_service(sc: &SparkCluster) -> Result<Service, Error> {
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
            .map_err(|_| ObjectMissingMetadataForOwnerRef {
                sc: ObjectRef::from_obj(sc),
            })?
            .with_recommended_labels(sc, APP_NAME, version(sc)?, &role_name, "global")
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("spark".to_string()),
                port: APP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_selector_labels(sc, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_server_rolegroup_config_map(
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
                .map_err(|_| Error::ObjectMissingMetadataForOwnerRef {
                    sc: ObjectRef::from_obj(sc),
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
                .ok_or_else(|| SerializeSparkMetrics {
                    rolegroup: rolegroup.clone(),
                })?
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
fn build_server_rolegroup_service(
    sc: &SparkCluster,
    rolegroup: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Service, Error> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|_| Error::ObjectMissingMetadataForOwnerRef {
                sc: ObjectRef::from_obj(sc),
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
            ports: Some(build_service_ports(sc, rolegroup, rolegroup_config)),
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

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
fn build_rolegroup_statefulset(
    sc: &SparkCluster,
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    match serde_yaml::from_str(&rolegroup_ref.role).unwrap() {
        SparkRole::Master => build_master_stateful_set(sc, rolegroup_ref, rolegroup_config),
        SparkRole::Worker => build_worker_stateful_set(sc, rolegroup_ref, rolegroup_config),
        SparkRole::HistoryServer => build_history_stateful_set(sc, rolegroup_ref, rolegroup_config),
    }
}

fn build_worker_stateful_set(
    sc: &SparkCluster,
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    let rolegroup = sc
        .spec
        .workers
        .as_ref()
        .ok_or(Error::NoServerRole {
            obj_ref: ObjectRef::from_obj(sc),
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
        .args(vec!["sbin/start-slave.sh".to_string(), build_master_service_url(sc)?])
        .add_env_vars(env)
        .add_container_ports(build_container_ports(sc, rolegroup_ref, rolegroup_config))
        .add_volume_mount("data", "/stackable/data")
        .add_volume_mount("config", "/stackable/config")
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|_| Error::ObjectMissingMetadataForOwnerRef {
                sc: ObjectRef::from_obj(sc),
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
                    name: Some("data".to_string()),
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
fn build_history_stateful_set(
    sc: &SparkCluster,
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    let rolegroup = sc
        .spec
        .history_servers
        .as_ref()
        .ok_or(Error::NoServerRole {
            obj_ref: ObjectRef::from_obj(sc),
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
        .add_container_ports(build_container_ports(sc, rolegroup_ref, rolegroup_config))
        .add_volume_mount("log", "/stackable/data/log")
        .add_volume_mount("config", "/stackable/config")
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|_| Error::ObjectMissingMetadataForOwnerRef {
                sc: ObjectRef::from_obj(sc),
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
fn build_master_stateful_set(
    sc: &SparkCluster,
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet, Error> {
    let rolegroup = sc
        .spec
        .masters
        .as_ref()
        .ok_or(Error::NoServerRole {
            obj_ref: ObjectRef::from_obj(sc),
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

    let container_sc = ContainerBuilder::new("master")
        .image(image)
        .args(vec!["sbin/start-master.sh".to_string()])
        .add_env_vars(env)
        .add_container_ports(build_container_ports(sc, rolegroup_ref, rolegroup_config))
        .add_volume_mount("data", "/stackable/data")
        .add_volume_mount("config", "/stackable/config")
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|_| Error::ObjectMissingMetadataForOwnerRef {
                sc: ObjectRef::from_obj(sc),
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
                    name: Some("data".to_string()),
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
pub fn build_spark_role_properties(
    resource: &SparkCluster,
) -> HashMap<
    String,
    (
        Vec<PropertyNameKind>,
        Role<impl Configuration<Configurable = SparkCluster>>,
    ),
> {
    let mut result = HashMap::new();
    if let Some(masters) = &resource.spec.masters {
        result.insert(
            SparkRole::Master.to_string(),
            (
                vec![
                    PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                    PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                    PropertyNameKind::File(SPARK_METRICS_PROPERTIES.to_string()),
                    PropertyNameKind::Env,
                ],
                masters.clone().erase(),
            ),
        );
    }
    if let Some(workers) = &resource.spec.workers {
        result.insert(
            SparkRole::Worker.to_string(),
            (
                vec![
                    PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                    PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                    PropertyNameKind::File(SPARK_METRICS_PROPERTIES.to_string()),
                    PropertyNameKind::Env,
                ],
                workers.clone().erase(),
            ),
        );
    }
    if let Some(history_servers) = &resource.spec.history_servers {
        result.insert(
            SparkRole::HistoryServer.to_string(),
            (
                vec![
                    PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                    PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                    PropertyNameKind::File(SPARK_METRICS_PROPERTIES.to_string()),
                    PropertyNameKind::Env,
                ],
                history_servers.clone().erase(),
            ),
        );
    }
    result
}

fn build_service_ports<'a>(
    _sc: &SparkCluster,
    rolegroup: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<ServicePort> {
    build_ports(_sc, rolegroup, rolegroup_config)
        .iter()
        .map(|(name, value)| ServicePort {
            name: Some(name.clone()),
            port: *value,
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        })
        .collect()
}

fn build_container_ports<'a>(
    _sc: &SparkCluster,
    rolegroup: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<ContainerPort> {
    build_ports(_sc, rolegroup, rolegroup_config)
        .iter()
        .map(|(name, value)| ContainerPort {
            name: Some(name.clone()),
            container_port: *value,
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        })
        .collect()
}

fn build_ports<'a>(
    _sc: &SparkCluster,
    rolegroup: &RoleGroupRef<SparkCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<(String, i32)> {
    match serde_yaml::from_str(&rolegroup.role).unwrap() {
        SparkRole::Master => vec![
            (
                "master".to_string(),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                    .and_then(|c| c.get(SPARK_ENV_MASTER_PORT))
                    .unwrap_or(&String::from("7077"))
                    .parse()
                    .unwrap(),
            ),
            (
                "web".to_string(),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                    .and_then(|c| c.get(SPARK_ENV_MASTER_WEBUI_PORT))
                    .unwrap_or(&String::from("8080"))
                    .parse()
                    .unwrap(),
            ),
        ],
        SparkRole::Worker => vec![
            (
                "worker".to_string(),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                    .and_then(|c| c.get(SPARK_ENV_WORKER_PORT))
                    .unwrap_or(&String::from("7077"))
                    .parse()
                    .unwrap(),
            ),
            (
                "web".to_string(),
                rolegroup_config
                    .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                    .and_then(|c| c.get(SPARK_ENV_WORKER_WEBUI_PORT))
                    .unwrap_or(&String::from("8080"))
                    .parse()
                    .unwrap(),
            ),
        ],
        SparkRole::HistoryServer => vec![(
            "web".to_string(),
            rolegroup_config
                .get(&PropertyNameKind::File(String::from(SPARK_ENV_SH)))
                .and_then(|c| c.get(SPARK_DEFAULTS_HISTORY_WEBUI_PORT))
                .unwrap_or(&String::from("8080"))
                .parse()
                .unwrap(),
        )],
    }
}
fn build_master_service_url(
    _sc: &SparkCluster,
) -> Result<String, Error> {
    Ok("spark://simple-master-default:7078".to_string())
}
    /*
          // Only allow the global load balancing service to send traffic to pods that are members of the quorum
          // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
          .readiness_probe(Probe {
              exec: Some(ExecAction {
                  command: Some(vec![
                      "bash".to_string(),
                      "-c".to_string(),
                      // We don't have telnet or netcat in the container images, but
                      // we can use Bash's virtual /dev/tcp filesystem to accomplish the same thing
                      format!(
                          "exec 3<>/dev/tcp/localhost/$SPARK_ENV_MASTER_PORT && echo srvr >&3 && grep '^Mode: ' <&3",
                      ),
                  ]),
              }),
              period_seconds: Some(1),
              ..Probe::default()
          })
   */
