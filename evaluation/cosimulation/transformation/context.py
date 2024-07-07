import time
from dataclasses import dataclass
from typing import Dict

from faas.context import InMemoryDeploymentService, InMemoryFunctionReplicaService, NodeService, \
    FunctionReplicaService
from faas.system import RuntimeLogger
from faas.util.point import PointWindow
from galileofaas.context.model import GalileoFaasContext
from galileofaas.context.platform.replica.k8s import KubernetesFunctionReplicaService
from galileofaas.context.platform.telemetry.rds import RedisTelemetryService
from galileofaas.context.platform.trace.rds import RedisTraceService
from galileofaas.system.core import KubernetesFunctionReplica
from sim.context.model import SimPlatformContext
from sim.context.platform.deployment.model import SimFunctionDeployment
from sim.context.platform.deployment.service import SimFunctionDeploymentService
from sim.context.platform.network.service import SimNetworkService
from sim.context.platform.node.factory import create_node_service
from sim.context.platform.replica.factory import SimFunctionReplicaFactory
from sim.context.platform.replica.model import SimFunctionReplica
from sim.context.platform.replica.service import SimFunctionReplicaService
from sim.context.platform.telemetry.service import SimTelemetryService
from sim.context.platform.trace.factory import create_trace_service
from sim.context.platform.zone.factory import create_zone_service
from sim.core import Environment
from sim.logging import SimulatedClock
from sim.metrics import SimMetrics
from sim.resource import NodeResourceWindow, ReplicaResourceWindow
from sim.topology import Topology

from evaluation.cosimulation.setup import create_mixed_labels, create_mixed_topology
from evaluation.cosimulation.transformation.deployment import real_world_to_sim_deployment


@dataclass
class InitialSimState:
    topology: Topology


def create_sim_telemetry_service(window: int, env: Environment, k8s_telemetry_service: RedisTelemetryService,
                                 node_service: NodeService, replica_service: FunctionReplicaService,
                                 container_resource_map: Dict[str, str], node_resource_map: Dict[str, str]):
    end = time.time()
    start = end - window
    sim_telemetry_service = SimTelemetryService(window, env)
    for replica in replica_service.get_function_replicas():
        for k8s_resource, sim_resource in container_resource_map.items():
            if 'cpu' in k8s_resource:
                df = k8s_telemetry_service.get_replica_cpu(replica.replica_id, start, end)
                df['resource'] = sim_resource
                unique_ts = df['ts'].unique()
                for ts in unique_ts:
                    df_ts = df[df['ts'] == ts]
                    sim_ts = end - ts
                    replica_resource_window = ReplicaResourceWindow(replica, df_ts, sim_ts)
                    sim_telemetry_service.put_replica_resource_utilization(replica_resource_window)

    for node in node_service.get_nodes():
        node_name = node.name
        for k8s_resource, sim_resource in node_resource_map.items():
            df = k8s_telemetry_service.get_node_resource(node_name, k8s_resource, start, end)
            df['resource'] = sim_resource
            unique_ts = df['ts'].unique()
            for ts in unique_ts:
                df_ts = df[df['ts'] == ts]
                sim_ts = end - ts
                node_resource_window = NodeResourceWindow(node_name, df_ts, sim_ts)
                sim_telemetry_service.put_node_resource_utilization(node_resource_window)
    return sim_telemetry_service




def galileo_world_to_simulation(ctx: GalileoFaasContext, start_time) -> Environment:
    env = Environment(start_time)
    topology = create_mixed_topology(create_mixed_labels())
    env.topology = topology
    metrics = SimMetrics(env, RuntimeLogger(SimulatedClock(env)))
    env.metrics = metrics

    deployment_service = create_deployment_service(ctx, metrics)
    node_service = create_node_service(env, env.topology)

    replica_service = create_sim_replica_service(ctx.replica_service, deployment_service, env, node_service)
    network_service = SimNetworkService(env.topology)

    container_resource_map = {'kubernetes_cgrp_cpu': 'cpu'}
    node_resource_map = {'cpu': 'cpu'}

    k8s_telemetry_service = ctx.telemetry_service
    telemetry_window = 60
    telemetry_service = create_sim_telemetry_service(telemetry_window, env, k8s_telemetry_service, node_service,
                                                     replica_service, container_resource_map, node_resource_map)

    trace_window = 60
    trace_service = create_sim_trace_service(trace_window, node_service, replica_service, env, ctx.trace_service)
    zone_service = create_zone_service(node_service.get_zones())

    sim_ctx = SimPlatformContext(
        deployment_service,
        network_service,
        node_service,
        replica_service,
        telemetry_service,
        trace_service,
        zone_service
    )
    env.context = sim_ctx
    return env


def create_sim_trace_service(window, node_service, replica_service, env, k8s_trace_service: RedisTraceService):
    trace_service = create_trace_service(window, node_service, replica_service, lambda: env.now)
    for node in node_service.get_nodes():
        k8s_inmemory = k8s_trace_service.inmemory_trace_service
        node_lock = k8s_inmemory.locks.get(node.name)
        if node_lock is None:
            continue
        with node_lock.lock.gen_rlock():
            window: PointWindow = k8s_inmemory.requests_per_node.get(node, None)
            if window is not None:
                for point in window.value():
                    trace_service.add_trace(point.val)

    return trace_service


def convert_k8s_replica_to_sim_replica(k8s_replica: KubernetesFunctionReplica) -> SimFunctionReplica:
    return SimFunctionReplica(
        k8s_replica.replica_id,
        k8s_replica.labels,
        k8s_replica.function,
        k8s_replica.container,
        k8s_replica.node,
        k8s_replica.state
    )


def create_sim_replica_service(k8s_replica_service: KubernetesFunctionReplicaService, deployment_service, env,
                               node_service):
    in_memory_function_service = InMemoryFunctionReplicaService[SimFunctionReplica](node_service, deployment_service,
                                                                                    SimFunctionReplicaFactory())
    for k8s_replica in k8s_replica_service.get_function_replicas():
        sim_replica = convert_k8s_replica_to_sim_replica(k8s_replica)
        in_memory_function_service.add_function_replica(sim_replica)

    sim_function_replica_service = SimFunctionReplicaService(in_memory_function_service, env)

    return sim_function_replica_service


def create_deployment_service(ctx, metrics):
    k8s_deployments = ctx.deployment_service.get_deployments()
    sim_deployments = real_world_to_sim_deployment(k8s_deployments)
    deployment_service = SimFunctionDeploymentService(InMemoryDeploymentService[SimFunctionDeployment](sim_deployments),
                                                      metrics)
    return deployment_service
