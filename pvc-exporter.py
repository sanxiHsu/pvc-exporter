import os
import time
import logging
from kubernetes import client, config
from kubernetes.client import V1PersistentVolumeClaimVolumeSource
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
    # 缓存用于存储PVC到PV的映射信息
    pvc_to_pv = {}

    # 获取所有命名空间下的Pods
    pods = v1.list_pod_for_all_namespaces(watch=False).items

    # 更新PVC到Pod的映射、PVC的使用情况和总容量
    for pod in pods:
        if pod.status.host_ip == os.getenv('HOST_IP') and pod.status.phase == 'Running':
            ns_name = pod.metadata.namespace
            for volume in pod.spec.volumes:
                pvc_source = getattr(volume, 'persistent_volume_claim', None)
                if isinstance(pvc_source, V1PersistentVolumeClaimVolumeSource):
                    claim_ref = pvc_source.claim_name
                    if claim_ref in pvc_to_pv:
                        pvc = pvc_to_pv[claim_ref]['pvc']
                        pv = pvc_to_pv[claim_ref]['pv']
                    else:
                        try:
                            pvc = v1.read_namespaced_persistent_volume_claim(claim_ref, ns_name)
                            if pvc.spec.volume_name:
                                pv_name = pvc.spec.volume_name
                                pv = v1.read_persistent_volume(pv_name)
                                pvc_to_pv[claim_ref] = {
                                    'pvc': pvc,
                                    'pv': pv,
                                }
                        except Exception as e:
                            logger.error(f"Failed to read PVC/PV for {claim_ref}: {e}")

                        # 更新PVC的使用情况
                        pvc_used, pvc_type = get_pvc_used(pv)
                        if pvc_used is not None:
                            metric_pvc_usage.labels(
                                persistentvolumeclaim=pvc.metadata.name,
                                pvc_namespace=pvc.metadata.namespace,
                                pvc_type=pvc_type
                            ).set(pvc_used)

                        # 更新PVC到Pod的映射
                        metric_pvc_mapping.labels(
                            persistentvolumeclaim=claim_ref,
                            pvc_namespace=ns_name,
                            mountedby=pod.metadata.name,
                            pod_namespace=ns_name,
                            host_ip=pod.status.host_ip
                        ).inc()
                        logger.info(f"Updated PVC mapping for {claim_ref} in pod {pod.metadata.name}.")


# 辅助函数，用于将大小字符串转换为字节
def parse_size(size_str):
    units = {'Ki': 1024, 'Mi': 1024 ** 2, 'Gi': 1024 ** 3, 'Ti': 1024 ** 4, 'Pi': 1024 ** 5}
    num, unit = size_str[:-2], size_str[-2:]
    return int(float(num)) * units[unit]


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
