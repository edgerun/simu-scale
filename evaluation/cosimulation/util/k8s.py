import logging
from typing import Dict, List

from kubernetes import client

logger = logging.getLogger(__name__)


def get_pods_by_label(label_value: str, v1: client.CoreV1Api()):
    label_selector = "component=" + label_value
    pods = v1.list_pod_for_all_namespaces(label_selector=label_selector)
    return pods


def get_env_var_values(pod, env_vars: List[str]) -> Dict[str, str]:
    res = {}

    for idx, container in enumerate(pod.spec.containers):
        container_env_vars = {env_var.name: env_var.value for env_var in container.env or []}
        for var_name in env_vars:
            res[var_name] = container_env_vars.get(var_name, None)
    return res


def get_all_hpa_component_configurations() -> Dict:
    # TODO parameters
    all_env_vars = {}
    v1 = client.CoreV1Api()

    loadbalancer_pods = get_pods_by_label("loadbalancer", v1)
    load_balancer_env_vars_names = ['ZONE', 'SCHEDULER_NAME', ' RECONCILE_INTERVAL', 'MOBILENET_CPU']
    for load_balancer_pod in loadbalancer_pods.items:
        load_balancer_env_vars = get_env_var_values(load_balancer_pod, load_balancer_env_vars_names)
        all_env_vars[load_balancer_pod.metadata.name] = load_balancer_env_vars

    autoscaler_pods = get_pods_by_label("autoscaler", v1)
    autoscaler_env_vars = ['ZONE', 'FUNCTION', 'SCHEDULER_NAME', 'TARGET_DURATION', 'FUNCTION_MAX_SCALE',
                           'RECONCILE_INTERVAL',
                           'MOBILENET_CPU', 'PERCENTILE_DURATION', 'LOOKBACK', 'THRESHOLD_TOLERANCE']
    for autoscaler_pod in autoscaler_pods.items:
        autoscaler_env_vars = get_env_var_values(autoscaler_pod, autoscaler_env_vars)
        all_env_vars[autoscaler_pod.metadata.name] = autoscaler_env_vars

    global_scheduler_pods = get_pods_by_label("global-scheduler", v1)
    global_scheduler_env_vars = ['SCHEDULER_NAME', 'DELAY', 'STORAGE_LOCAL_SCHEDULERS', 'MOBILENET_CPU',
                                 'FUNCTION_MAX_SCALE']

    for global_scheduler_pod in global_scheduler_pods.items:
        global_scheduler_env_vars = get_env_var_values(global_scheduler_pod, global_scheduler_env_vars)
        all_env_vars[global_scheduler_pod.metadata.name] = global_scheduler_env_vars

    local_scheduler_pods = get_pods_by_label("local-scheduler", v1)
    local_scheduler_env_vars = ['ZONE', 'SCHEDULER_NAME', 'GLOBAL_SCHEDULER_NAME', 'DELAY', 'MOBILENET_CPU']

    for local_scheduler_pod in local_scheduler_pods.items:
        local_scheduler_env_vars = get_env_var_values(local_scheduler_pod, local_scheduler_env_vars)
        all_env_vars[local_scheduler_pod.metadata.name] = local_scheduler_env_vars

    return all_env_vars


def get_hpa_component_configurations() -> Dict:
    all_env_vars = {}
    v1 = client.CoreV1Api()

    autoscaler_pods = get_pods_by_label("autoscaler", v1)
    autoscaler_env_vars = ['ZONE', 'FUNCTION', 'SCHEDULER_NAME', 'TARGET_DURATION', 'FUNCTION_MAX_SCALE',
                           'RECONCILE_INTERVAL',
                           'MOBILENET_CPU', 'PERCENTILE_DURATION', 'LOOKBACK', 'THRESHOLD_TOLERANCE']
    for autoscaler_pod in autoscaler_pods.items:
        autoscaler_env_vars = get_env_var_values(autoscaler_pod, autoscaler_env_vars)
        all_env_vars[autoscaler_pod.metadata.name] = autoscaler_env_vars

    return all_env_vars


def get_pressure_autoscaler_component_configurations() -> Dict:
    all_env_vars = {}
    v1 = client.CoreV1Api()

    autoscaler_pods = get_pods_by_label("autoscaler", v1)
    autoscaler_env_vars = ['ZONE', 'SCHEDULER_NAME', 'MAX_THRESHOLD', 'MIN_THRESHOLD', 'FUNCTION_MAX_SCALE',
                           'FUNCTION_REQUIREMENT', 'TARGET_TIME_MEASURE', 'PRESSURE_NAMES', 'MAX_LATENCY', 'A', 'B',
                           'C', 'D', 'OFFSET', 'RECONCILE_INTERVAL',
                           'MOBILENET_CPU', 'PERCENTILE_DURATION', 'LOOKBACK']
    for autoscaler_pod in autoscaler_pods.items:
        autoscaler_env_vars = get_env_var_values(autoscaler_pod, autoscaler_env_vars)
        all_env_vars[autoscaler_pod.metadata.name] = autoscaler_env_vars

    return all_env_vars
