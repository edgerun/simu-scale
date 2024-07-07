import json
import logging
import random
import time
from datetime import datetime
from typing import Tuple, Dict, List

from ether.blocks.cells import BusinessIsp
from ether.blocks.nodes import create_node
from ether.cell import SharedLinkCell, UpDownLink
from ether.qos import latency
from faas.system import Clock, LoggingLogger
from faas.util.constant import zone_label, worker_role_label, client_role_label, controller_role_label
from galileofaas.connections import RedisClient, KubernetesClient
from galileofaas.context.daemon import GalileoFaasContextDaemon
from galileofaas.context.model import GalileoFaasContext
from galileofaas.context.platform.deployment.factory import create_deployment_service
from galileofaas.context.platform.network.factory import create_network_service
from galileofaas.context.platform.node.factory import create_node_service
from galileofaas.context.platform.pod.factory import PodFactory
from galileofaas.context.platform.replica.factory import KubernetesFunctionReplicaFactory, create_replica_service
from galileofaas.context.platform.telemetry.factory import create_telemetry_service
from galileofaas.context.platform.trace.factory import create_trace_service
from galileofaas.context.platform.zone.factory import create_zone_service
from galileofaas.system.core import GalileoFaasMetrics, KubernetesFunctionReplica
from galileofaas.system.faas import GalileoFaasSystem
from galileofaas.system.metrics import GalileoLogger
from kubernetes import client
from kubernetes.client import V1EnvVar
from sim.topology import Topology
from telemc import TelemetryController

from evaluation.cosimulation.apps.pi import PiProfilingApplication
from evaluation.cosimulation.util.deployments import init_function_deployments

logger = logging.getLogger(__name__)


class TimestampClock(Clock):

    def now(self) -> datetime:
        return time.time()


def setup_metrics(clock: Clock = None, rds: RedisClient = None):
    if rds is not None:
        metric_logger = GalileoLogger(rds, clock)
    else:
        log_fn = lambda x: logger.info(f'[log] {x}')
        metric_logger = LoggingLogger(log_fn, clock)
    return GalileoFaasMetrics(metric_logger)


def setup_galileo_faas(scheduler_name: str) -> Tuple[GalileoFaasContextDaemon, GalileoFaasSystem, GalileoFaasMetrics]:
    rds = RedisClient.from_env()
    metrics = setup_metrics(clock=TimestampClock(), rds=rds)
    daemon = setup_daemon(rds, metrics, scheduler_name)

    faas_system = GalileoFaasSystem(daemon.context, metrics)
    return daemon, faas_system, metrics


class PiPodFactory(PodFactory):

    def __init__(self, scheduler_name: str):
        self.scheduler_name = scheduler_name
        self.pi_app = PiProfilingApplication()

    def create_pod(self, replica: KubernetesFunctionReplica, **kwargs) -> client.V1Pod:
        """
        Creates a Pod from a KubernetesFunctionReplica.
        :param replica:
        :param kwargs: passed on to the spec filed of the PodSpec
        :return:
        """
        pod = client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=client.V1ObjectMeta(name=replica.replica_id, labels=replica.labels,
                                         annotations={'kubectl.kubernetes.io/last-applied-configuration': ''}),
            spec=client.V1PodSpec(
                **kwargs,
                containers=[
                    self.create_container(replica)
                ],
                scheduler_name=self.scheduler_name
            ),
        )
        pod_dict = json.dumps(pod.to_dict())
        pod.metadata.annotations['kubectl.kubernetes.io/last-applied-configuration'] = pod_dict
        return pod

    def create_container(self, replica: KubernetesFunctionReplica) -> client.V1Container:
        container = self.pi_app.pod_factory(replica.pod_name, replica.container.image,
                                            replica.container.get_resource_requirements())
        container.env.append(V1EnvVar('exec_timeout', '100'))
        container.env.append(V1EnvVar('read_timeout', '100'))
        container.env.append(V1EnvVar('write_timeout', '100'))

        return container


def setup_daemon(rds: RedisClient, metrics: GalileoFaasMetrics, scheduler_name: str,
                 init_deployments: bool = True, topology: Topology=None) -> GalileoFaasContextDaemon:
    if init_deployments:
        deployment_service = create_deployment_service(metrics, init_function_deployments(topology))
    else:
        deployment_service = create_deployment_service(metrics, [])
    telemc = TelemetryController(rds.conn())

    node_service = create_node_service(telemc)

    # latency in ms
    min_latency = 1
    max_latency = 1000
    latency_map = {}
    nodes = node_service.get_nodes()
    for node_1 in nodes:
        for node_2 in nodes:
            if node_1 == node_2:
                # same node has 0.5 ms latency
                latency_map[(node_1.name, node_2.name)] = 0.5
            else:
                # else make random for demonstration purposes
                latency_map[(node_1.name, node_2.name)] = random.randint(1, 100)
    network_service = create_network_service(min_latency, max_latency, latency_map)

    pod_factory = PiPodFactory(scheduler_name)
    KubernetesClient.from_env()
    core_v1_api = client.CoreV1Api()
    replica_factory = KubernetesFunctionReplicaFactory()
    replica_service = create_replica_service(node_service, rds, deployment_service, core_v1_api, pod_factory,
                                             replica_factory, metrics)

    # window size to store, in seconds
    window_size = 3 * 60
    telemetry_service = create_telemetry_service(window_size, rds, node_service)

    #
    trace_service = create_trace_service(window_size, rds, replica_service, network_service, node_service)

    zones = node_service.get_zones()
    zone_service = create_zone_service(zones)

    context = GalileoFaasContext(
        deployment_service,
        network_service,
        node_service,
        replica_service,
        telemetry_service,
        trace_service,
        zone_service,
        KubernetesFunctionReplicaFactory(),
        rds,
        telemc
    )
    return GalileoFaasContextDaemon(context)


def create_strong_node(name: str, labels: Dict):
    return create_node(name, 8, '16384M', 'x86', labels)


def create_medium_node(name: str, labels: Dict):
    return create_node(name, 4, '4096M', 'x86', labels)


def create_weak_node(name: str, labels: Dict):
    return create_node(name, 2, '3072M', 'x86', labels)


def create_mixed_labels() -> Dict[str, Dict]:
    labels = {}

    zone_a_worker_nodes = [
        'w10-worker-zone-a-4',
        'w10-worker-zone-a-3',
        'w10-worker-zone-a-2',
        'w10-worker-zone-a-1',
        'w10-worker-zone-a-0',
    ]

    for worker in zone_a_worker_nodes:
        labels[worker] = {zone_label: 'zone-a', worker_role_label: 'true'}

    zone_a_client_nodes = [
        'w20-client-zone-a-0',
    ]

    for worker in zone_a_client_nodes:
        labels[worker] = {zone_label: 'zone-a', client_role_label: 'true'}

    zone_a_controller_node = 'm10-controller-zone-a-0'

    labels[zone_a_controller_node] = {zone_label: 'zone-a', controller_role_label: 'true'}

    zone_b_worker_nodes = [
        'w30-worker-zone-b-0',
        'w30-worker-zone-b-1',
        'w30-worker-zone-b-2',
        'm30-worker-zone-b-0',
        'm30-worker-zone-b-1',
        'm30-worker-zone-b-2',
        'm30-worker-zone-b-3',
        'm30-worker-zone-b-4',
    ]
    for worker in zone_b_worker_nodes:
        labels[worker] = {zone_label: 'zone-b', worker_role_label: 'true'}

    zone_b_client_nodes = [
        'w40-client-zone-b-0'
    ]
    for worker in zone_b_client_nodes:
        labels[worker] = {zone_label: 'zone-b', client_role_label: 'true'}

    zone_b_controller_node = 'm20-controller-zone-b-0'
    labels[zone_b_controller_node] = {zone_label: 'zone-b', controller_role_label: 'true'}

    zone_c_worker_nodes = [
        's30-worker-zone-c-0',
        's30-worker-zone-c-1',
        's30-worker-zone-c-2',
        's30-worker-zone-c-3',
    ]
    for worker in zone_c_worker_nodes:
        labels[worker] = {zone_label: 'zone-c', worker_role_label: 'true'}

    zone_c_controller_node = 's20-controller-zone-c-0'
    labels[zone_c_controller_node] = {zone_label: 'zone-c', controller_role_label: 'true'}

    zone_c_master_node = 'm40-master-zone-c-0'
    labels[zone_c_master_node] = {zone_label: 'zone-c'}

    return labels


import re


def extract_cluster_pattern(input_string, prefix: str = 'zone'):
    pattern = r'zone-\w+'
    results = re.findall(pattern, input_string)
    return results


def create_dece_hc_labels(nodes: List[str]) -> Dict[str, Dict]:
    labels = {}

    for node in nodes:
        cluster = extract_cluster_pattern(node)[0]

        if 'controller' in node:
            labels[node] = {zone_label: cluster, controller_role_label: 'true'}

        if 'worker' in node:
            labels[node] = {zone_label: cluster, worker_role_label: 'true'}

        if 'client' in node:
            labels[node] = {zone_label: cluster, client_role_label: 'true'}

        if 'master' in node:
            labels[node] = {zone_label: cluster}

    return labels


def create_dece_hc_topology() -> Topology:
    node_names = [
        's10-master-zone-c-0',
        's60-controller-zone-a-0',
        's70-controller-zone-b-0',
        's20-controller-zone-c-0',
        's30-worker-zone-c-0',
        's30-worker-zone-c-1',
        's30-worker-zone-c-2',
        's40-worker-zone-a-0',
        'w10-worker-zone-a-0',
        'w10-worker-zone-a-1',
        'w10-worker-zone-a-2',
        'w30-client-zone-a-0',
        'w20-worker-zone-b-0',
        'w20-worker-zone-b-1',
        's50-worker-zone-b-0',
        'w40-client-zone-b-0'
    ]
    return create_topology(node_names)


def create_topology(node_list: List[str]):
    labels = create_dece_hc_labels(node_list)

    node_objects = {}
    for node in node_list:
        if node[0] == 's':
            node_objects[node] = create_strong_node(node, labels[node])
        elif node[0] == 'w':
            node_objects[node] = create_weak_node(node, labels[node])

    # The rest of the code constructing the topology remains unchanged:
    topology = Topology()
    backhaul_zone_edge_2_cloud = EdgeToCloud('internet_vie')
    backhaul_cloud = BusinessIsp('internet_vie')

    cell_zone_c = SharedLinkCell(
        [node_objects[node] for node in node_list if 'zone-c' in node], 1000, backhaul=backhaul_cloud)
    cell_zone_b = SharedLinkCell(
        [node_objects[node] for node in node_list if 'zone-b' in node], 1000, backhaul=backhaul_zone_edge_2_cloud)
    cell_zone_a = SharedLinkCell(
        [node_objects[node] for node in node_list if 'zone-a' in node], 1000, backhaul=backhaul_zone_edge_2_cloud)

    topology.add(cell_zone_c)
    topology.add(cell_zone_b)
    topology.add(cell_zone_a)

    topology.init_docker_registry()
    return topology


def create_mixed_topology(labels: Dict[str, Dict]):
    strong_controller_c = create_strong_node('s20-controller-zone-c-0', labels['s20-controller-zone-c-0'])

    strong_workers_zone_c_0 = create_strong_node('s30-worker-zone-c-0', labels['s30-worker-zone-c-0'])
    strong_workers_zone_c_1 = create_strong_node('s30-worker-zone-c-1', labels['s30-worker-zone-c-1'])
    strong_workers_zone_c_2 = create_strong_node('s30-worker-zone-c-2', labels['s30-worker-zone-c-2'])
    strong_workers_zone_c_3 = create_strong_node('s30-worker-zone-c-3', labels['s30-worker-zone-c-3'])

    medium_controller_zone_a = create_medium_node('m10-controller-zone-a-0', labels['m10-controller-zone-a-0'])
    medium_controller_zone_b = create_medium_node('m20-controller-zone-b-0', labels['m20-controller-zone-b-0'])

    medium_worker_zone_b_0 = create_medium_node('m30-worker-zone-b-0', labels['m30-worker-zone-b-0'])
    medium_worker_zone_b_1 = create_medium_node('m30-worker-zone-b-1', labels['m30-worker-zone-b-1'])
    medium_worker_zone_b_2 = create_medium_node('m30-worker-zone-b-2', labels['m30-worker-zone-b-2'])
    medium_worker_zone_b_3 = create_medium_node('m30-worker-zone-b-3', labels['m30-worker-zone-b-3'])
    medium_worker_zone_b_4 = create_medium_node('m30-worker-zone-b-4', labels['m30-worker-zone-b-4'])

    medium_master_zone_c = create_medium_node('m40-master-zone-c-0', labels['m40-master-zone-c-0'])

    weak_worker_zone_a_0 = create_weak_node('w10-worker-zone-a-0', labels['w10-worker-zone-a-0'])
    weak_worker_zone_a_1 = create_weak_node('w10-worker-zone-a-1', labels['w10-worker-zone-a-1'])
    weak_worker_zone_a_2 = create_weak_node('w10-worker-zone-a-2', labels['w10-worker-zone-a-2'])
    weak_worker_zone_a_3 = create_weak_node('w10-worker-zone-a-3', labels['w10-worker-zone-a-3'])
    weak_worker_zone_a_4 = create_weak_node('w10-worker-zone-a-4', labels['w10-worker-zone-a-4'])

    weak_client_zone_a_0 = create_weak_node('w20-client-zone-a-0', labels['w20-client-zone-a-0'])

    weak_worker_zone_b_0 = create_weak_node('w30-worker-zone-b-0', labels['w30-worker-zone-b-0'])
    weak_worker_zone_b_1 = create_weak_node('w30-worker-zone-b-1', labels['w30-worker-zone-b-1'])
    weak_worker_zone_b_2 = create_weak_node('w30-worker-zone-b-2', labels['w30-worker-zone-b-2'])

    weak_client_zone_b_0 = create_weak_node('w40-client-zone-b-0', labels['w40-client-zone-b-0'])

    topology = Topology()
    backhaul_zone_edge_2_cloud = EdgeToCloud('internet_vie')

    backhaul_cloud = BusinessIsp('internet_vie')

    cell_zone_c = SharedLinkCell(
        [strong_controller_c, strong_workers_zone_c_0, strong_workers_zone_c_1, strong_workers_zone_c_2,
         strong_workers_zone_c_3, medium_master_zone_c], 1000, backhaul=backhaul_cloud)
    cell_zone_b = SharedLinkCell(
        [medium_controller_zone_b, medium_worker_zone_b_0, medium_worker_zone_b_1, medium_worker_zone_b_2,
         medium_worker_zone_b_3, medium_worker_zone_b_4, weak_client_zone_b_0, weak_worker_zone_b_0,
         weak_worker_zone_b_1, weak_worker_zone_b_2], 1000, backhaul=backhaul_zone_edge_2_cloud)
    cell_zone_a = SharedLinkCell(
        [weak_client_zone_a_0, weak_worker_zone_a_0, weak_worker_zone_a_1, weak_worker_zone_a_2, weak_worker_zone_a_3,
         weak_worker_zone_a_4, medium_controller_zone_a], 1000, backhaul=backhaul_zone_edge_2_cloud)

    topology.add(cell_zone_c)
    # for node in topology.get_nodes():
    #     if node.labels.get(zone_label, None) is None:
    #         node.labels[zone_label] = f"zone-c"

    topology.add(cell_zone_b)
    # for node in topology.get_nodes():
    #     if node.labels.get(zone_label, None) is None:
    #         node.labels[zone_label] = f"zone-b"

    topology.add(cell_zone_a)
    # for node in topology.get_nodes():
    #     if node.labels.get(zone_label, None) is None:
    #         node.labels[zone_label] = f"zone-a"

    topology.init_docker_registry()
    return topology


class EdgeToCloud(UpDownLink):

    def __init__(self, backhaul='internet') -> None:
        super().__init__(125, 25, backhaul, latency.mobile_isp)


def main():
    topology = create_dece_hc_topology()
    pass


if __name__ == '__main__':
    main()
