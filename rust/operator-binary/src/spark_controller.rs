//! Ensures that `Pod`s are configured and running for each [`SparkCluster`]

use crate::error::Error;
use crate::error::Error::{
    ApplyRoleGroupConfig, ApplyRoleGroupService, ApplyRoleGroupStatefulSet, ApplyRoleService,
    ApplyStatus, GlobalServiceNameNotFound, ObjectMissingMetadataForOwnerRef,
};
use crate::util::version;
use fnv::FnvHasher;
use snafu::Snafu;
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
        self,
        api::ObjectMeta,
        runtime::{
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
        },
    },
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{
        types::PropertyNameKind, writer::to_java_properties_string, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
};
use stackable_spark_crd::constants::{
    APP_NAME, APP_PORT, FIELD_MANAGER_SCOPE, SPARK_DEFAULTS_CONF, SPARK_ENV_SH,
    SPARK_METRICS_PROPERTIES,
};
use stackable_spark_crd::{SparkCluster, SparkClusterSpec, SparkClusterStatus, SparkRole};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    hash::Hasher,
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
            let rg_service = build_server_rolegroup_service(&rolegroup, &sc)?;
            let rg_configmap =
                build_server_rolegroup_config_map(&rolegroup, &sc, rolegroup_config)?;
            let rg_statefulset =
                build_server_rolegroup_statefulset(&rolegroup, &sc, rolegroup_config)?;
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
    rolegroup: &RoleGroupRef<SparkCluster>,
    sc: &SparkCluster,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<ConfigMap, Error> {
    unimplemented!();
    // TODO:
    // let mut zoo_cfg = server_config
    //     .get(&PropertyNameKind::File(PROPERTIES_FILE.to_string()))
    //     .cloned()
    //     .unwrap_or_default();
    // zoo_cfg.extend(sc.pods().into_iter().flatten().map(|pod| {
    //     (
    //         format!("server.{}", pod.zookeeper_myid),
    //         format!("{}:2888:3888;{}", pod.fqdn(), APP_PORT),
    //     )
    // }));
    // let zoo_cfg = zoo_cfg
    //     .into_iter()
    //     .map(|(k, v)| (k, Some(v)))
    //     .collect::<Vec<_>>();
    // ConfigMapBuilder::new()
    //     .metadata(
    //         ObjectMetaBuilder::new()
    //             .name_and_namespace(sc)
    //             .name(rolegroup.object_name())
    //             .ownerreference_from_resource(sc, None, Some(true))
    //             .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
    //                 sc: ObjectRef::from_obj(sc),
    //             })?
    //             .with_recommended_labels(
    //                 sc,
    //                 APP_NAME,
    //                 version(sc)?,
    //                 &rolegroup.role,
    //                 &rolegroup.role_group,
    //             )
    //             .build(),
    //     )
    // .add_data(
    //     "zoo.cfg",
    //     to_java_properties_string(zoo_cfg.iter().map(|(k, v)| (k, v))).with_context(|| {
    //         SerializeZooCfg {
    //             rolegroup: rolegroup.clone(),
    //         }
    //     })?,
    // )
    // .build()
    // .map_err(|e| Error::BuildRoleGroupConfig {
    //     source: e,
    //     rolegroup: rolegroup.clone(),
    // })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_server_rolegroup_service(
    rolegroup: &RoleGroupRef<SparkCluster>,
    sc: &SparkCluster,
) -> Result<Service, Error> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
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
            ports: Some(vec![
                ServicePort {
                    name: Some("sc".to_string()),
                    port: APP_PORT.into(),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some("metrics".to_string()),
                    port: 9505,
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
            ]),
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
fn build_server_rolegroup_statefulset(
    rolegroup_ref: &RoleGroupRef<SparkCluster>,
    sc: &SparkCluster,
    server_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
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
    let env = server_config
        .get(&PropertyNameKind::Env)
        .iter()
        .flat_map(|env_vars| env_vars.iter())
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();
    let container_decide_myid = ContainerBuilder::new("decide-myid")
        .image(&image)
        .args(vec![
            "sh".to_string(),
            "-c".to_string(),
            "expr $MYID_OFFSET + $(echo $POD_NAME | sed 's/.*-//') > /stackable/data/myid"
                .to_string(),
        ])
        .add_env_vars(env.clone())
        .add_env_vars(vec![EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.name".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .add_volume_mount("data", "/stackable/data")
        .build();
    let container_sc = ContainerBuilder::new("spark")
        .image(image)
        .args(vec![
            "bin/scServer.sh".to_string(),
            "start-foreground".to_string(),
            "/stackable/config/zoo.cfg".to_string(),
        ])
        .add_env_vars(env)
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
                        "exec 3<>/dev/tcp/localhost/{} && echo srvr >&3 && grep '^Mode: ' <&3",
                        APP_PORT
                    ),
                ]),
            }),
            period_seconds: Some(1),
            ..Probe::default()
        })
        .add_container_port("sc", APP_PORT.into())
        .add_container_port("sc-leader", 2888)
        .add_container_port("sc-election", 3888)
        .add_container_port("metrics", 9505)
        .add_volume_mount("data", "/stackable/data")
        .add_volume_mount("config", "/stackable/config")
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(sc)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(sc, None, Some(true))
            .map_err(|e| Error::ObjectMissingMetadataForOwnerRef {
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
                .add_init_container(container_decide_myid)
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
