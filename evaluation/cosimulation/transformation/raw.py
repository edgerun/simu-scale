import random
import secrets
from dataclasses import dataclass
from typing import Dict, Optional, List

from faas.context import NodeService, \
    FunctionReplicaService
from faas.util.constant import function_label, zone_label
from faas.util.point import PointWindow
from galileofaas.context.model import GalileoFaasContext
from galileofaas.context.platform.replica.k8s import KubernetesFunctionReplicaService
from galileofaas.context.platform.telemetry.rds import RedisTelemetryService
from galileofaas.context.platform.trace.rds import RedisTraceService
from galileofaas.system.core import KubernetesFunctionReplica, KubernetesFunctionNode
from sim.context.platform.node.model import SimFunctionNode
from sim.context.platform.replica.model import SimFunctionReplica
from sim.resource import NodeResourceWindow, ReplicaResourceWindow
from sim.topology import Topology
from skippy.core.model import Pod, PodSpec, Container, ResourceRequirements

from evaluation.cosimulation.orchestration.model import RawPlatformContext
from evaluation.cosimulation.transformation.deployment import real_world_to_sim_deployment


@dataclass
class InitialSimState:
    topology: Topology


def create_raw_telemetry_windows(window: int, k8s_telemetry_service: RedisTelemetryService,
                                 node_service: NodeService, replica_service: FunctionReplicaService,
                                 container_resource_map: Dict[str, str], node_resource_map: Dict[str, str],
                                 sim_start_time):
    end = sim_start_time + window
    start = sim_start_time
    replica_resource_windows = []
    node_resource_windows = []
    for replica in replica_service.get_function_replicas():
        for k8s_resource, sim_resource in container_resource_map.items():
            if 'cpu' in k8s_resource:
                df = k8s_telemetry_service.get_replica_cpu(replica.replica_id, start, end)
                if df is None:
                    continue

                df['resource'] = sim_resource
                unique_ts = df['ts'].unique()
                for ts in unique_ts:
                    df_ts = df[df['ts'] == ts]
                    sim_ts = abs(start - ts)
                    df_ts['ts'] = sim_ts
                    replica_resource_window = ReplicaResourceWindow(replica, df_ts, sim_ts)
                    replica_resource_windows.append(replica_resource_window)

    for node in node_service.get_nodes():
        node_name = node.name
        for k8s_resource, sim_resource in node_resource_map.items():
            df = k8s_telemetry_service.get_node_resource(node_name, k8s_resource, start, end)
            if df is None:
                continue
            df['resource'] = sim_resource
            unique_ts = df['ts'].unique()
            for ts in unique_ts:
                df_ts = df[df['ts'] == ts]
                sim_ts = abs(start - ts)
                df_ts['ts'] = sim_ts
                node_resource_window = NodeResourceWindow(node_name, df_ts, sim_ts)
                node_resource_windows.append(node_resource_window)
    return replica_resource_windows, node_resource_windows


def galileo_world_to_raw(ctx: GalileoFaasContext, sim_start_time, check_fn_replicas: List[str] = None) -> Optional[
    RawPlatformContext]:
    deployments = create_raw_deployments(ctx)
    if len(deployments) == 0:
        return None

    if check_fn_replicas is not None:
        for check_fn in check_fn_replicas:
            if len(ctx.replica_service.get_function_replicas_of_deployment(check_fn)) == 0:
                return None

    replicas = transform_raw_replica(ctx.replica_service)
    if len(replicas) == 0:
        return None

    for replica in replicas:
        for other_replica in replicas:
            if replica.labels.get(function_label) == 'client' and other_replica.labels.get(
                    function_label) == 'galileo-worker-zone-a':
                if replica.labels.get(zone_label) == 'zone-a':
                    other_replica.replica_id = f'{replica.replica_id}:pythonpi-zone-a:0'
            if replica.labels.get(function_label) == 'client' and other_replica.labels.get(
                    function_label) == 'galileo-worker-zone-b':
                if replica.labels.get(zone_label) == 'zone-b':
                    other_replica.replica_id = f'{replica.replica_id}:pythonpi-zone-b:0'
    container_resource_map = {'kubernetes_cgrp_cpu': 'cpu'}
    node_resource_map = {'cpu': 'cpu'}

    k8s_telemetry_service = ctx.telemetry_service
    telemetry_window = 60
    replica_resource_windows, node_resource_windows = create_raw_telemetry_windows(telemetry_window,
                                                                                   k8s_telemetry_service,
                                                                                   ctx.node_service,
                                                                                   ctx.replica_service,
                                                                                   container_resource_map,
                                                                                   node_resource_map,
                                                                                   sim_start_time)

    responses = create_raw_traces(ctx.node_service, ctx.trace_service, sim_start_time)

    return RawPlatformContext(
        deployments, replicas, replica_resource_windows, node_resource_windows, responses
    )


def create_raw_traces(node_service, k8s_trace_service: RedisTraceService, sim_start_time):
    responses = []
    for node in node_service.get_nodes():
        k8s_inmemory = k8s_trace_service.inmemory_trace_service
        node_lock = k8s_inmemory.locks.get(node.name)
        if node_lock is None:
            continue
        with node_lock.lock.gen_rlock():
            window: PointWindow = k8s_inmemory.requests_per_node.get(node.name, None)
            if window is not None:
                for point in window.value():
                    response = point.val

                    ts_end_sim = abs(sim_start_time - response.ts_end)
                    ts_exec_sim = abs(sim_start_time - response.ts_exec)
                    ts_wait_sim = abs(sim_start_time - response.ts_wait) if response.ts_wait != -1 else -1
                    ts_start_sim = abs(sim_start_time - response.ts_start)
                    response.ts_end = ts_end_sim
                    response.ts_start = ts_start_sim
                    response.ts_exec = ts_exec_sim
                    response.ts_wait = ts_wait_sim
                    response.request.start = abs(sim_start_time - response.request.start)

                    responses.append(response)

    return responses


def convert_k8s_node_to_sim_node(node: KubernetesFunctionNode) -> SimFunctionNode:
    return SimFunctionNode(node)


def convert_k8s_replica_to_sim_replica(k8s_replica: KubernetesFunctionReplica) -> SimFunctionReplica:
    replica = SimFunctionReplica(
        k8s_replica.replica_id,
        k8s_replica.labels,
        k8s_replica.function,
        k8s_replica.container,
        convert_k8s_node_to_sim_node(k8s_replica.node),
        k8s_replica.state
    )

    pod = Pod(k8s_replica.replica_id, 'default',
              PodSpec(containers=[Container(k8s_replica.image,
                                            ResourceRequirements(k8s_replica.container.get_resource_requirements()))]))
    replica.pod = pod
    return replica


def convert_sim_replica_to_k8s_replica(sim_replica: SimFunctionReplica) -> KubernetesFunctionReplica:
    ip = ".".join(str(random.randint(0, 255)) for _ in range(4))
    replica = KubernetesFunctionReplica(
        sim_replica,
        ip=ip,
        port=8080,
        url=f"http://{ip}:8080",
        namespace="default",
        host_ip=ip,
        qos_class='Burstable',
        start_time=None,
        pod_name=sim_replica.replica_id,
        container_id=secrets.token_hex(32)
    )

    return replica


def transform_raw_replica(k8s_replica_service: KubernetesFunctionReplicaService):
    replicas = []
    for k8s_replica in k8s_replica_service.get_function_replicas():
        sim_replica = convert_k8s_replica_to_sim_replica(k8s_replica)
        replicas.append(sim_replica)

    return replicas


def create_raw_deployments(ctx):
    k8s_deployments = ctx.deployment_service.get_deployments()
    sim_deployments = real_world_to_sim_deployment(k8s_deployments)
    return sim_deployments
