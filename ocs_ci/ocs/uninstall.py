import logging

from ocs_ci.framework import config
from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.exceptions import UnavailableResourceException
from ocs_ci.ocs.machine import get_labeled_nodes
from ocs_ci.ocs.node import get_all_nodes
from ocs_ci.ocs.resources.pod import get_all_pods, get_pvc_name
from ocs_ci.ocs.resources.pvc import get_all_pvcs_in_storageclass
from ocs_ci.ocs.resources.storage_cluster import get_all_storageclass
from ocs_ci.utility.localstorage import check_local_volume

log = logging.getLogger(__name__)


def remove_monitoring_stack_from_ocs():
    """
    Function removes monitoring stack from OCS

    """
    monitoring_obj = ocp.OCP(
        namespace=constants.MONITORING_NAMESPACE, kind='ConfigMap',
    )
    param_cmd = '[{"op": "replace", "path": "/data/config.yaml", "value": ""}]'
    monitoring_obj.patch(
        resource_name='cluster-monitoring-config',
        params=param_cmd,
        format_type='json'
    )


def remove_ocp_registry_from_ocs(platform):
    """
    Function removes OCS registry from OCP cluster

    Args:
        platform (str): the platform the cluster deployed on

    """
    image_registry_obj = ocp.OCP(
        kind=constants.CONFIG, namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
    )
    param_cmd1 = None
    param_cmd2 = None
    if platform.lower() == constants.AWS_PLATFORM:
        param_cmd1 = '[{"op": "remove", "path": "/spec/storage"}]'
        param_cmd2 = '[{"op": "remove", "path": "/status/generations/storage"}]'

    elif platform.lower() == constants.VSPHERE_PLATFORM:
        param_cmd1 = '[{"op": "replace", "path": "/spec/storage", "value": {"emptyDir": "{}"}}]'
        param_cmd2 = '[{"op": "replace", "path": "/status/generations/storage", "value": {"emptyDir": "{}"}}]'

    if param_cmd1:
        image_registry_obj.patch(
            resource_name=constants.IMAGE_REGISTRY_RESOURCE_NAME, params=param_cmd1, format_type='json'
        )
        image_registry_obj.patch(
            resource_name=constants.IMAGE_REGISTRY_RESOURCE_NAME, params=param_cmd2, format_type='json'
        )
    else:
        log.info("platform registry not supported")


def uninstall_lso():
    """
    Function uninstalls local-volume objects from OCS cluster

    """
    ocp_obj = ocp.OCP()
    storage_cluster = (
        ocp.OCP(
            kind=constants.STORAGECLUSTER,
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace='openshift-storage'
        )
    )

    sc_name = (
        storage_cluster.get().get('spec').get('storageDeviceSets')[0].get(
            'dataPVCTemplate').get('spec').get('storageClassName')
    )

    sc_obj = (
        ocp.OCP(
            kind=constants.STORAGECLASS,
            resource_name=sc_name,
            namespace=constants.LOCAL_STORAGE_NAMESPACE
        )
    )

    lv_name = sc_obj.get().get('metadata').get('labels').get('local.storage.openshift.io/owner-name')
    lv_obj = (
        ocp.OCP(
            kind=constants.LOCAL_VOLUME,
            resource_name=lv_name,
            namespace=constants.LOCAL_STORAGE_NAMESPACE
        )
    )

    log.info(f"storage class: {sc_name}  local volume:{lv_name}")

    device_list = lv_obj.get().get('spec').get('storageClassDevices')[0].get('devicePaths')
    storage_node_list = get_labeled_nodes(constants.OPERATOR_NODE_LABEL)

    pv_obj_list = (
        ocp.OCP(
            kind=constants.PV,
            selector=f'storage.openshift.com/local-volume-owner-name={lv_name}',
            namespace=constants.LOCAL_STORAGE_NAMESPACE
        )
    )

    log.info("deleting local volume PVs")
    for pv in pv_obj_list.get().get('items'):
        log.info(f"deleting pv {pv.get('metadata').get('name')}")
        pv_obj_list.delete(resource_name=pv.get('metadata').get('name'))

    log.info("removing local volume from storage nodes")
    for node in storage_node_list:
        log.info(f"removing from node {node}")
        ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=[f"rm -rfv /mnt/local-storage/{sc_name}"])

    disk_list_str = ""
    for device in device_list:
        disk_list_str = disk_list_str + f" {device}"
    disk_list_str = f"DISKS=\"{disk_list_str}\""
    log.info(f"the disk list is {disk_list_str}")

    sgd_command = "for disk in $DISKS; do sgdisk --zap-all $disk;done"
    log.info("wiping disks on storage nodes ")
    for node in storage_node_list:
        log.info(f"wiping on node {node}")
        cmd_list = [disk_list_str, sgd_command]
        ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=cmd_list)

    log.info(f"deleting storage class {sc_name}")
    sc_obj.delete(resource_name=sc_name)

    log.info(f"deleting local volume {lv_name}")
    lv_obj.delete(resource_name=lv_name)


def uninstall_ocs():
    """
    The function uninstalls the OCS operator from a openshift
    cluster and removes all its settings and dependencies

    """
    ocp_obj = ocp.OCP()
    provisioners = constants.OCS_PROVISIONERS

    # List the storage classes
    sc_list = get_all_storageclass()
    sc_name_list = []
    for storage_class in sc_list:
        if storage_class.get('provisioner') not in provisioners:
            sc_list.remove(storage_class)
        else:
            sc_name_list.append(storage_class.get('metadata').get('name'))

    # Query for PVCs and OBCs that are using the storage class provisioners listed in the previous step.
    pvc_to_delete = []
    pvc_name_list = []
    for sc in sc_name_list:
        pvc_to_delete.extend(get_all_pvcs_in_storageclass(sc))

    # ignoring all noobaa pvcs & make name list
    for pvc in pvc_to_delete:
        if "noobaa" in pvc.name:
            pvc_to_delete.remove(pvc)
        else:
            pvc_name_list.append(pvc.name)

    pods_to_delete = []
    all_pods = get_all_pods()  # default openshift-storage namespace
    all_pods.extend(get_all_pods(namespace=constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE))
    all_pods.extend(get_all_pods(namespace=constants.OPENSHIFT_MONITORING_NAMESPACE))

    for pod_obj in all_pods:
        try:
            pvc_name = get_pvc_name(pod_obj)
        except UnavailableResourceException:
            continue
        if pvc_name in pvc_name_list:
            pods_to_delete.append(pod_obj)

    log.info("Removing monitoring stack from OpenShift Container Storage")
    remove_monitoring_stack_from_ocs()

    log.info("Removing OpenShift Container Platform registry from OpenShift Container Storage")
    remove_ocp_registry_from_ocs(config.ENV_DATA['platform'])

    log.info("Removing the cluster logging operator from OpenShift Container Storage")
    csv = ocp.OCP(
        kind=constants.CLUSTER_SERVICE_VERSION,
        namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
    )
    logging_csv = csv.get().get('items')
    if logging_csv:
        clusterlogging_obj = ocp.OCP(
            kind=constants.CLUSTER_LOGGING, namespace=constants.OPENSHIFT_LOGGING_NAMESPACE
        )
        clusterlogging_obj.delete(resource_name='instance')

    log.info("deleting pvcs")
    for pvc in pvc_to_delete:
        log.info(f"deleting pvc: {pvc.name}")
        pvc.delete()

    log.info("deleting pods")
    for pod in pods_to_delete:
        log.info(f"deleting pod {pod.name}")
        pod.delete()

    log.info("checking local storage")  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if check_local_volume():
        "local volume was found. removing from cluster"
        uninstall_lso()

    log.info("deleting storageCluster object")
    storage_cluster = ocp.OCP(
        kind=constants.STORAGECLUSTER,
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace='openshift-storage'
    )
    storage_cluster.delete(resource_name=constants.DEFAULT_CLUSTERNAME)

    log.info("removing rook directory from nodes")
    nodes_list = get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
    for node in nodes_list:
        log.info(f"removing rook from {node}")
        ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=["rm -rf /var/lib/rook"])

    log.info("Delete the storage classes with an openshift-storage provisioner list")
    for storage_class in sc_list:
        log.info(f"deleting storage class {storage_class.get('metadata').get('name')}")
        sc_obj = ocp.OCP(kind=constants.STORAGECLASS)
        sc_obj.delete(resource_name=storage_class.get('metadata').get('name'))

    log.info("unlabaling storage nodes")
    nodes_list = get_all_nodes()
    for node in nodes_list:
        node_obj = ocp.OCP(kind=constants.NODE, resource_name=node)
        node_obj.add_label(resource_name=node, label=constants.OPERATOR_NODE_LABEL[:-3] + '-')
        node_obj.add_label(resource_name=node, label=constants.TOPOLOGY_ROOK_LABEL + '-')

    log.info("removing CRDs")
    crd_list = ['backingstores.noobaa.io', 'bucketclasses.noobaa.io', 'cephblockpools.ceph.rook.io',
                'cephfilesystems.ceph.rook.io', 'cephnfses.ceph.rook.io',
                'cephobjectstores.ceph.rook.io', 'cephobjectstoreusers.ceph.rook.io', 'noobaas.noobaa.io',
                'ocsinitializations.ocs.openshift.io', 'storageclusterinitializations.ocs.openshift.io',
                'storageclusters.ocs.openshift.io', 'cephclusters.ceph.rook.io']
    for crd in crd_list:
        ocp_obj.exec_oc_cmd(f"delete crd {crd} --timeout=300m")

    log.info("deleting openshift-storage namespace")
    ocp_obj.delete_project('openshift-storage')
    ocp_obj.wait_for_delete('openshift-storage')
