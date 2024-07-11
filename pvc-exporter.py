import os
import time
import logging
from kubernetes import client, config
from prometheus_client import start_http_server, Gauge

# 日志初始化
logger = logging.getLogger('pvc-exporter')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# 环境变量加载
EXPORTER_SERVER_PORT = int(os.getenv('EXPORTER_SERVER_PORT', 9100))
SCAN_INTERVAL = float(os.getenv('SCAN_INTERVAL', 60))

# Prometheus度量初始化
metric_pvc_usage = Gauge('pvc_usage_bytes', 'Bytes used by PVC', ['persistentvolumeclaim', 'pvc_namespace', 'pvc_type'])
metric_pvc_mapping = Gauge('pvc_pod_mapping', 'Mapping between PVC and Pods',
                           ['persistentvolumeclaim', 'pvc_namespace', 'mountedby', 'pod_namespace', 'host_ip'])

# Kubernetes API初始化
try:
    logger.info("Loading in-cluster configuration...")
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    logger.info("In-cluster configuration loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load in-cluster configuration: {e}")
    raise


# 主要逻辑函数
def update_metrics():
    # 获取所有PVC和PV信息
    logger.info("Fetching PVCs and PVs information...")
    pvcs = v1.list_persistent_volume_claim_for_all_namespaces().items
    pvs = v1.list_persistent_volume().items
    logger.info(f"Fetched {len(pvcs)} PVCs and {len(pvs)} PVs.")

    # 构建PV到PVC的映射
    pv_to_pvc = {}
    for pvc in pvcs:
        pv_name = pvc.spec.volume_name
        if pv_name:
            pv_to_pvc[pv_name] = {
                'name': pvc.metadata.name,
                'namespace': pvc.metadata.namespace,
                'requested_size': pvc.status.capacity['storage'],
            }
    logger.info(f"Built mapping for {len(pv_to_pvc)} PVs.")

    # 遍历PV，更新PVC使用情况
    for pv in pvs:
        pv_name = pv.metadata.name
        if pv_name in pv_to_pvc and pv.spec.csi:
            pvc_info = pv_to_pvc[pv_name]
            pvc_used, pvc_type = get_pvc_used(pv)
            if pvc_used is not None:
                metric_pvc_usage.labels(persistentvolumeclaim=pvc_info['name'],
                                        pvc_namespace=pvc_info['namespace'],
                                        pvc_type=pvc_type).set(pvc_used)
            logger.info(f"Updated PVC usage for {pvc_info['name']}.")

    # 更新PVC到Pod的映射
    for namespace in v1.list_namespace().items:
        ns_name = namespace.metadata.name
        pods = v1.list_namespaced_pod(ns_name).items
        for pod in pods:
            if pod.status.host_ip == os.getenv('HOST_IP') and pod.status.phase == 'Running':
                try:
                    for volume in pod.spec.volumes:
                        if 'persistentVolumeClaim' in volume:
                            claim_ref = volume.persistentVolumeClaim.claim_name
                            if claim_ref in [pvc['name'] for pvc in pv_to_pvc.values()]:
                                metric_pvc_mapping.labels(persistentvolumeclaim=claim_ref,
                                                          pvc_namespace=ns_name,
                                                          mountedby=pod.metadata.name,
                                                          pod_namespace=ns_name,
                                                          host_ip=pod.status.host_ip).inc()
                                logger.info(f"Updated PVC mapping for {claim_ref} in pod {pod.metadata.name}.")
                except TypeError as e:
                    logger.error(f"TypeError encountered when processing volumes for pod {pod.metadata.name}: {e}")


def get_pvc_used(pv):
    # 获取PVC使用情况
    try:
        with open('/etc/mtab', 'r') as mtab:
            for line in mtab:
                if pv.metadata.name in line:
                    mount_point = line.split()[1]
                    if pv.spec.csi:  # 只处理CSI类型的PV
                        st = os.statvfs(mount_point)
                        pvc_used = (st.f_blocks - st.f_bfree) * st.f_frsize
                        return pvc_used, 'csi'
    except Exception as e:
        logger.error(f"Error getting PVC usage for {pv.metadata.name}: {e}")
    return None, None


# exporter运行函数
def run_exporter():
    start_http_server(EXPORTER_SERVER_PORT)
    while True:
        try:
            update_metrics()
        except Exception as e:
            logger.error(f"Error during metrics update: {e}")
        logger.info(f"Sleeping for {SCAN_INTERVAL} seconds before next update.")
        time.sleep(SCAN_INTERVAL)


if __name__ == '__main__':
    run_exporter()