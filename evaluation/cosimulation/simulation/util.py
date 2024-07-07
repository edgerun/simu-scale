import abc
import logging
import math
import os
import random
import time
from pathlib import Path
from typing import Callable, List

import pandas as pd
import simpy
import srds
from ether.topology import Topology
from ext.raith21.oracles import Raith21FetOracle, Raith21ResourceOracle
from faas.context import NodeService, InMemoryDeploymentService, InMemoryNodeService, InMemoryFunctionReplicaService, \
    PlatformContext
from faas.system import FunctionContainer, RuntimeLogger, FunctionNode, NodeState, \
    FunctionReplicaState
from faas.system.autoscaler.core import BaseAutoscalerConfiguration
from faas.system.scheduling.decentralized import BaseGlobalSchedulerConfiguration
from faas.util.constant import zone_label, pod_type_label, api_gateway_type_label
from faasopts.autoscalers.base.hpa.decentralized.latency import HorizontalLatencyPodAutoscalerParameters, \
    HorizontalLatencyFunctionParameters
from faasopts.loadbalancers.wrr.wrr import RoundRobinWeightCalculator
from faasopts.schedulers.decentralized.locality.globalscheduler import LocalityGlobalScheduler
from faasopts.schedulers.decentralized.pressure.globalscheduler import PressureGlobalScheduler, \
    PressureGlobalSchedulerConfiguration
from faasopts.utils.pressure.api import PressureAutoscalerParameters, PressureFunctionParameters
from galileofaas.connections import RedisClient
from galileojp.faassiminmemory import InMemoryFaasSimGateway
from sim.context.model import SimPlatformContext
from sim.context.platform.deployment.model import SimFunctionDeployment
from sim.context.platform.deployment.service import SimFunctionDeploymentService
from sim.context.platform.network.service import SimNetworkService
from sim.context.platform.node.factory import _get_bandwidth
from sim.context.platform.node.model import SimFunctionNode
from sim.context.platform.replica.factory import SimFunctionReplicaFactory
from sim.context.platform.replica.model import SimFunctionReplica
from sim.context.platform.replica.service import SimFunctionReplicaService
from sim.context.platform.request.service import SimpleRequestService
from sim.context.platform.telemetry.service import SimTelemetryService
from sim.context.platform.trace.factory import create_trace_service
from sim.context.platform.trace.service import SimTraceService
from sim.context.platform.zone.factory import create_zone_service
from sim.core import Environment
from sim.docker import ImageProperties
from sim.faas import SimulatorFactory, FunctionSimulator
from sim.faas.decentralized import DecentralizedFaasSystem
from sim.faas.loadbalancers import UpdateableLoadBalancerSimulator, \
    WrrLoadBalancer, DefaultWrrProviderFactory, LoadBalancerOptimizerUpdateProcess, ForwardingClientFunctionContainer
from sim.faassim import Simulation
from sim.logging import SimulatedClock
from sim.metrics import SimMetrics
from sim.oracle.oracle import FetOracle, ResourceOracle
from sim.resource import ResourceMonitor
from sim.skippy import to_skippy_node
from sim.util.experiment import extract_dfs
from skippy.core.clustercontext import ClusterContext
from skippy.core.model import SchedulingResult
from skippy.core.utils import normalize_image_name

from evaluation.cosimulation.benchmark.cosim import CoSimBenchmark
from evaluation.cosimulation.orchestration.model import HpaAutoscalerConfiguration, PressureAutoscalerConfiguration, \
    FunctionOptResult, SimInput, SimResults, PRESSURE_MIN_MAX_MAPPING, PRESSURE_WEIGHT_MAPPING
from evaluation.cosimulation.setup import create_mixed_labels, create_dece_hc_topology
from evaluation.cosimulation.simulation.deployment.cosim import get_pi_image_properties
from evaluation.cosimulation.simulation.functionsim.factory import CoSimFunctionSimulatorFactory
from evaluation.cosimulation.simulation.oracles.fet import fet
from evaluation.cosimulation.simulation.oracles.resources import res
from evaluation.cosimulation.simulation.orchestration.autoscaler import setup_dece_hpa_latency_strategies
from evaluation.cosimulation.simulation.orchestration.pressure import SimPressureAutoscalerDaemon, \
    SimPressureGlobalSchedulerDaemon
from evaluation.cosimulation.simulation.orchestration.scheduler import create_scheduler
from evaluation.cosimulation.simulation.scenario.client import prepare_workload_ia_generator
from evaluation.cosimulation.util.constants import HPA_AUTOSCALER, PRESSURE_AUTOSCALER, PRESSURE_GLOBAL_SCHEDULER, \
    LOCALITY_GLOBAL_SCHEDULER
from evaluation.cosimulation.util.k8s import get_hpa_component_configurations, \
    get_pressure_autoscaler_component_configurations
from evaluation.simulation.optimization.loadbalancer.wrr import SimWrrOptimizer

logger = logging.getLogger(__name__)

import copy
import json
from typing import Dict


def get_initial_parameters(autoscaler_config: BaseAutoscalerConfiguration, zones: List[str],
                           fetch_params: bool = False) -> Dict:
    if autoscaler_config.autoscaler_type == HPA_AUTOSCALER:
        return get_initial_hpa_params(autoscaler_config, zones, fetch_params)
    elif autoscaler_config.autoscaler_type == PRESSURE_AUTOSCALER:
        return get_initial_pressure_params(autoscaler_config, zones, fetch_params)


def fetch_initial_parameters(autoscaler_type: str, zones: List[str]) -> BaseAutoscalerConfiguration:
    if autoscaler_type == HPA_AUTOSCALER:
        return fetch_hpa_autoscaler_params()
    elif autoscaler_type == PRESSURE_AUTOSCALER:
        return get_initial_pressure_params(zones, True)


def get_initial_hpa_params(autoscaler_config: HpaAutoscalerConfiguration, zones: List[str], fetch_params: bool):
    if fetch_params:
        all_params = fetch_hpa_autoscaler_params()
        return all_params
    else:
        params = autoscaler_config.zone_parameters

        autoscalers = {}
        for zone in zones:
            initial_parameters = json.loads(HorizontalLatencyPodAutoscalerParameters.to_json(params[zone]))
            initial_parameters['reconcile_interval'] = autoscaler_config.reconcile_interval
            autoscalers[f'autoscaler-{zone}'] = copy.deepcopy(initial_parameters)
        return autoscalers


def fetch_hpa_autoscaler_params() -> HpaAutoscalerConfiguration:
    configs = get_hpa_component_configurations()
    cluster_params = {}
    reconcile_interval = None
    for component, params in configs.items():
        zone = params['zone'.upper()]
        target_duration = float(params['target_duration'.upper()])
        percentile_duration = int(params['percentile_duration'.upper()])
        lookback = int(params['lookback'.upper()])
        function = params['function'.upper()]
        reconcile_interval = int(params['reconcile_interval'.upper()])
        threshold_tolerance = float(params['threshold_tolerance'.upper()])
        target_time_measure = params['target_time_measure'.upper()]

        cluster_params[zone] = HorizontalLatencyPodAutoscalerParameters(
            {function: HorizontalLatencyFunctionParameters(target_time_measure, lookback, threshold_tolerance,
                                                           target_duration, percentile_duration)}
        )
    if reconcile_interval is None:
        raise ValueError("No reconcile interval")
    return HpaAutoscalerConfiguration(HPA_AUTOSCALER, reconcile_interval, cluster_params)


def get_initial_pressure_params(autoscaler_config: PressureAutoscalerConfiguration, clusters: List[str],
                                fetch_params: bool) -> PressureAutoscalerConfiguration:
    if fetch_params:
        all_params = fetch_pressure_autoscaler_params()
        return all_params
    else:
        params = autoscaler_config.zone_parameters

        autoscalers = {}
        for cluster in clusters:
            initial_parameters = json.loads(PressureAutoscalerParameters.to_json(params[cluster]))
            initial_parameters['reconcile_interval'] = autoscaler_config.reconcile_interval
            autoscalers[f'autoscaler-{cluster}'] = copy.deepcopy(initial_parameters)
        return autoscalers


def fetch_pressure_autoscaler_params() -> PressureAutoscalerConfiguration:
    configs = get_pressure_autoscaler_component_configurations()
    zone_params = {}
    reconcile_interval = None
    for component, params in configs.items():
        # TODO this has to be updated because we mount the config as json now
        zone = params['zone'.upper()]
        max_threshold = float(params['max_threshold'.upper()])
        min_threshold = int(params['min_threshold'.upper()])
        a = float(params['a'.upper()])
        b = float(params['b'.upper()])
        c = float(params['c'.upper()])
        d = float(params['d'.upper()])
        offset = float(params['offset'.upper()])
        local_scheduler_name = params['local_scheduler_name']
        lookback = int(params['lookback'.upper()])
        target_time_measure = params['target_time_measure'.upper()]
        function = params['function'.upper()]
        reconcile_interval = int(params['reconcile_interval'.upper()])
        pressure_names = params['pressure_names'.upper()].split(' ')
        percentile_duration = float(params['percentile_duration'.upper()])
        function_requirement = float(params['function_requirement'.upper()])
        zone_params[zone] = PressureAutoscalerParameters({function: PressureFunctionParameters(
            max_threshold,
            min_threshold,
            function_requirement,
            target_time_measure,
            pressure_names,
            a,
            b, c, d, offset, lookback, percentile_duration
        )}, local_scheduler_name=local_scheduler_name)
    if reconcile_interval is None:
        raise ValueError('Reconcile interval is not set')
    return PressureAutoscalerConfiguration(PRESSURE_AUTOSCALER, reconcile_interval, zone_params)


def randomize_hpa_latency_parameter_space(config: HpaAutoscalerConfiguration) -> HpaAutoscalerConfiguration:
    """
    Randomizes the parameters from the given HpaAutoscalerConfiguration object and returns a copy of it..
    :param config: HpaAutoscalerConfiguration object that will be randomized
    """
    modified_cluster_parameters: Dict[str, HorizontalLatencyPodAutoscalerParameters] = {}
    for zone, autoscaler_parameters in config.zone_parameters.items():
        modified_fn_parameters = {}
        for fn, orchestration_parameters in autoscaler_parameters.function_parameters.items():
            orchestration_parameters_copy = orchestration_parameters.copy()
            orchestration_parameters_copy.percentile_duration = randomize_percentage(
                orchestration_parameters.percentile_duration, 15)
            if orchestration_parameters_copy.percentile_duration > 100:
                orchestration_parameters_copy.percentile_duration = 100
            orchestration_parameters_copy.target_duration = randomize_percentage(
                orchestration_parameters.target_duration, 15)
            orchestration_parameters_copy.threshold_tolerance = randomize_percentage(
                orchestration_parameters.threshold_tolerance, 15)
            modified_fn_parameters[fn] = orchestration_parameters_copy
        modified_cluster_parameters[zone] = HorizontalLatencyPodAutoscalerParameters(modified_fn_parameters)

    return HpaAutoscalerConfiguration(config.autoscaler_type, config.reconcile_interval, modified_cluster_parameters)


def randomize_pressure_autoscaler_parameter_space(
        config: PressureAutoscalerConfiguration) -> PressureAutoscalerConfiguration:
    if config.mapping_type == PRESSURE_MIN_MAX_MAPPING:
        copy = config.copy()
        for zone, params in copy.zone_parameters.items():
            for fn, fn_params in params.function_parameters.items():
                copy.zone_parameters[zone].function_parameters[fn].max_threshold = randomize_percentage(
                    fn_params.max_threshold, 15)
                copy.zone_parameters[zone].function_parameters[fn].min_threshold = randomize_percentage(
                    fn_params.min_threshold, 15)
        return copy
    elif config.mapping_type == PRESSURE_WEIGHT_MAPPING:
        copy = config.copy()
        for zone, params in copy.zone_parameters.items():
            for fn, fn_params in params.function_parameters.items():
                for pressure, weight in fn_params.weights.items():
                    copy.zone_parameters[zone].function_parameters[fn].weights[pressure] = randomize_percentage(
                        weight, 15)
        return copy


def add_all_images_to_nodes(node_service: NodeService[SimFunctionNode], images: List[ImageProperties]):
    for node in node_service.get_nodes():
        for image in images:
            if image.arch == node.arch:
                node.docker_images.add(image)


def randomize_percentage(number: float, percentage: float) -> float:
    range_diff = number * (percentage / 100.0)
    lower_bound = max(0, number - range_diff)
    upper_bound = number + range_diff
    return random.uniform(lower_bound, upper_bound)


def publish_updates(rds_updates: Dict[str, str], rds: RedisClient):
    for component, updates in rds_updates.items():
        msg = f'{time.time()} {component} {updates}'
        rds.publish_async('galileo/events', msg)


def extract_zones_from_topology(topology: Topology) -> List[str]:
    zones = set()
    for node in topology.get_nodes():
        zone_label_value = node.labels.get(zone_label)
        if zone_label_value is not None:
            zones.add(zone_label_value)
    return list(zones)


def prepare_results_for_publishing(params: FunctionOptResult) -> Dict[str, str]:
    # { fn: { orchestration_component: json(params) } }
    rds_updates = {}
    v = params.input_params.as_json_update()
    logger.info(v)
    autoscaler_config = json.loads(v['autoscaler_config'])
    for zone, function_parameters in autoscaler_config['zone_parameters'].items():
        rds_updates[f'autoscaler-{zone}-parameters'] = json.dumps(function_parameters)

    return rds_updates


def prepare_total_replica_df(fn: str, dfs: Dict[str, pd.DataFrame], zones: List[str]) -> pd.DataFrame:
    exp_id = dfs['experiment_df']['EXP_ID'].iloc[0]
    # logger.error(f'{dfs}')
    gw = InMemoryFaasSimGateway({exp_id: dfs})
    nodes = gw.get_nodes_by_name(exp_id)
    total_cores = 0
    for node in nodes.values():
        total_cores += node.cpus
    total_cores *= 1000
    cpu_req = 500

    replica_df = gw.get_replica_schedule_statistics(exp_id, fn, zones,
                                                    per_second=True)

    total_usage_replica_df = replica_df.groupby('ts').max()
    total_usage_replica_df['resource_usage'] = (total_usage_replica_df['total'] * cpu_req) / total_cores
    return total_usage_replica_df


def transform_dfs_into_sim_results(dfs: Dict[str, pd.DataFrame], fns: List[str], zones: List[str]) -> Dict[
    str, Dict[str, pd.DataFrame]]:
    """
    Prepares Dict that contains a Dict for each passed Zone.
    Each Dict per Zone contains a traces_df and a total_replica_df.
    The traces_df contains only traces from the specific function and also includes the RTT (i.e., "rtt").
    The total_replica_df contains per second the total number of replicas running
    """
    fn_dfs = {}
    for fn in fns:
        traces_df = prepare_traces_df(fn, dfs)
        replica_df = prepare_total_replica_df(fn, dfs, zones)
        fn_dfs[fn] = {
            'traces_df': traces_df,
            'total_replica_df': replica_df
        }
    return fn_dfs


def prepare_traces_df(fn: str, dfs):
    traces_df = dfs['traces_df']
    traces_df['rtt'] = traces_df['ts_end'] - traces_df['ts_start']
    traces_df = traces_df[traces_df['function_name'] == fn]
    return traces_df


def setup_pressure_strategies(sim_input: SimInput, global_scheduler_q: simpy.Store):
    config: PressureAutoscalerConfiguration = sim_input.orchestration_configuration.autoscaler_config

    daemon = SimPressureAutoscalerDaemon(config.zone_parameters, config.reconcile_interval, global_scheduler_q)
    return [daemon.run]


def create_fet_oracle() -> FetOracle:
    return Raith21FetOracle(fet)


def create_resource_oracle() -> ResourceOracle:
    return Raith21ResourceOracle(res)


def create_function_simulator_factory(
        load_balancer_factory: Callable[[Environment, FunctionContainer], FunctionSimulator],
        application) -> SimulatorFactory:
    return CoSimFunctionSimulatorFactory(load_balancer_factory, create_fet_oracle(), create_resource_oracle(),
                                         application)


def save_results(root_folder: str, dfs: Dict[str, pd.DataFrame], sim: Simulation, expName, application):
    path = f'{root_folder}/{application}/{expName}'
    if not os.path.exists(path):
        Path(path).mkdir(parents=True, exist_ok=False)
        for name, df in dfs.items():
            if '/' in name:
                name = name.replace('/', '-')
            file_name = f'{path}/{name}.csv'
            df.to_csv(file_name)
    else:
        logger.info(f'Path {path} already exists.')
    return path


def create_raw_deployment_service(sim_input: SimInput, metrics):
    deployment_service = SimFunctionDeploymentService(
        InMemoryDeploymentService[SimFunctionDeployment](sim_input.raw_platform_ctx.deployments),
        metrics)
    return deployment_service


def create_sim_node(ether_node, topology):
    skippy_node = to_skippy_node(ether_node)
    labels = ether_node.labels.copy()
    labels.update(skippy_node.labels)
    fn_node = FunctionNode(
        name=ether_node.name,
        arch=ether_node.arch,
        cpus=ether_node.capacity.cpu_millis / 1000,
        ram=ether_node.capacity.memory,
        netspeed=_get_bandwidth(ether_node.name, topology),
        labels=labels,
        allocatable={'cpu': f'{ether_node.capacity.cpu_millis}m',
                     'memory': ether_node.capacity.memory},
        cluster=labels.get(zone_label),
        state=NodeState.READY
    )
    node = SimFunctionNode(fn_node=fn_node)
    node.ether_node = ether_node
    node.skippy_node = skippy_node
    return node


def create_raw_node_service(env: Environment):
    zones = set()
    nodes = []
    topology = env.topology
    for node in topology.get_nodes():
        node_zone = node.labels.get(zone_label)
        if node_zone is not None:
            zones.add(node_zone)
        sim_fn_node = create_sim_node(node, topology)
        if sim_fn_node is not None:
            nodes.append(sim_fn_node)
    zones = list(zones)
    return InMemoryNodeService[SimFunctionNode](zones, nodes)


def create_raw_replica_service(sim_input: SimInput, node_service: NodeService[SimFunctionNode], deployment_service,
                               env):
    in_memory_function_service = InMemoryFunctionReplicaService[SimFunctionReplica](node_service, deployment_service,
                                                                                    SimFunctionReplicaFactory())
    logger.info("Initialize clients with request pattern")
    for sim_replica in sim_input.raw_platform_ctx.replicas:
        name = sim_replica.node.name
        sim_replica.node = node_service.find(name)
        in_memory_function_service.add_function_replica(sim_replica)
        # sim_replica.simulator = env.simulator_factory.create(env, sim_replica.container)
        if 'galileo-worker' in sim_replica.fn_name or 'client' in sim_replica.fn_name:
            for other_sim_replica in sim_input.raw_platform_ctx.replicas:
                if other_sim_replica.labels.get(pod_type_label) == api_gateway_type_label:
                    if other_sim_replica.labels.get(zone_label) == sim_replica.labels.get(zone_label):
                        lb = other_sim_replica
                        zone = other_sim_replica.labels.get(zone_label)

                        for fn_deployment in deployment_service.get_deployments():
                            if fn_deployment.name == sim_replica.labels.get('to_call'):
                                request_files = sim_input.scenario[zone][fn_deployment.get_containers()[0].image]
                                request_file = request_files[0]

                                ia_generator = prepare_workload_ia_generator(sim_input.duration, request_file,
                                                                             sim_input.start_time)

                                # TODO: implement forwarding client container that supports multiple ia_generators
                                client_container = ForwardingClientFunctionContainer(
                                    sim_replica.container,
                                    ia_generator,
                                    200,
                                    fn_deployment,
                                    lb.function,
                                )
                                sim_replica.container = client_container
    sim_function_replica_service = SimFunctionReplicaService(in_memory_function_service, env)

    return sim_function_replica_service


def create_raw_telemetry_service(sim_input: SimInput, window: int, env: Environment) -> SimTelemetryService:
    sim_telemetry_service = SimTelemetryService(window, env)

    for replica_resource_window in sim_input.raw_platform_ctx.replica_resource_windows:
        sim_telemetry_service.put_replica_resource_utilization(replica_resource_window)

    for node_resource_window in sim_input.raw_platform_ctx.node_resource_windows:
        sim_telemetry_service.put_node_resource_utilization(node_resource_window)

    return sim_telemetry_service


def create_raw_trace_service(sim_input: SimInput, env: Environment, node_service,
                             replica_service: SimFunctionReplicaService,
                             window: int) -> SimTraceService:
    trace_service = create_trace_service(window, node_service, replica_service, lambda: env.now)
    for response in sim_input.raw_platform_ctx.responses:
        trace_service.add_trace(response)
        # trace_service.request_cache[response.request_id] = response

    return trace_service


def create_sim_platform_context(sim_input: SimInput, simulation: Simulation) -> SimPlatformContext:
    network_service = SimNetworkService(simulation.env.topology)
    # simulation.env.context.network_service = network_service

    deployment_service = create_raw_deployment_service(sim_input, simulation.env.metrics)
    # simulation.env.context.deployment_service = deployment_service

    node_service = create_raw_node_service(simulation.env)
    # simulation.env.context.node_service = node_service

    replica_service = create_raw_replica_service(sim_input, node_service, deployment_service, simulation.env)
    # simulation.env.context.replica_service =replica_service

    telemetry_window = 60
    telemetry_service = create_raw_telemetry_service(sim_input, telemetry_window, simulation.env)
    # simulation.env.telemetry_service = telemetry_service

    trace_window = 60
    trace_service = create_raw_trace_service(sim_input, simulation.env, node_service, replica_service, trace_window)
    # simulation.env.context.trace_service = trace_service

    zone_service = create_zone_service(node_service.get_zones())
    # simulation.env.context.zone_service = zone_service

    sim_ctx = SimPlatformContext(
        deployment_service=deployment_service,
        network_service=network_service,
        node_service=node_service,
        replica_service=replica_service,
        telemetry_service=telemetry_service,
        trace_service=trace_service,
        zone_service=zone_service
    )

    simulation.env.context = sim_ctx
    simulation.env.context.request_service = SimpleRequestService(lambda: simulation.env.now)

    return simulation.env.context


def topology_factory(sim_input: SimInput):
    topology = sim_input.topology
    if topology == 'dece_hc':
        return create_dece_hc_topology()
    else:
        raise AttributeError("Unknown topology '{}'".format(topology))


def load_balancer_factory(sim_input: SimInput, cluster: str, env: Environment, scaling: float):
    if sim_input.orchestration_configuration.load_balancer_type == 'rr':
        wrr = WrrLoadBalancer(env, cluster, DefaultWrrProviderFactory(scaling))
        rr_calculator = RoundRobinWeightCalculator()
        updater = SimWrrOptimizer(wrr, env.context, cluster, env.metrics, rr_calculator)
        return updater, wrr
    else:
        raise AttributeError(
            "Unknown load balancer type '{}'".format(sim_input.orchestration_configuration.load_balancer_type))


def read_application_image_parameters(application_name: str) -> List[ImageProperties]:
    if application_name == 'pythonpi':
        return get_pi_image_properties()
    else:
        raise ValueError("Unknown application name '{}'".format(application_name))


def execute_benchmark(sim_input: SimInput):
    topology = topology_factory(sim_input)
    application_name = list(sim_input.application_params.keys())[0]
    application = sim_input.application_params[application_name]['name']
    payload_size = sim_input.application_params[application_name]['payload_size']
    app_image_properties = read_application_image_parameters(application_name)
    scenario = sim_input.scenario
    duration = sim_input.duration
    workload_start_time = sim_input.workload_start_time
    start_time = sim_input.start_time
    init_deploy = len(sim_input.raw_platform_ctx.deployments) == 0
    benchmark = CoSimBenchmark(topology, scenario, payload_size, application, sim_input.target_cluster,
                               app_image_properties, duration,
                               workload_start_time,
                               init_deploy, sim_input.clients)

    # prepare simulation with topology and benchmark from basic example
    simulation = Simulation(topology, benchmark, Environment(start_time))
    simulation.env.faas = DecentralizedFaasSystem(simulation.env)

    lb_process = LoadBalancerOptimizerUpdateProcess(reconcile_interval=1)

    simulation.env.topology = topology
    metrics = SimMetrics(simulation.env, RuntimeLogger(SimulatedClock(simulation.env)))
    simulation.env.metrics = metrics
    if not init_deploy:
        for replica in sim_input.raw_platform_ctx.replicas:
            simulation.env.metrics.log_function_replica(replica)
            simulation.env.metrics.log_queue_schedule(replica)
            simulation.env.metrics.log_start_schedule(replica)
            simulation.env.metrics.log_finish_schedule(replica, SchedulingResult(replica.node, 1, []))
            simulation.env.metrics.log_deploy(replica)
            simulation.env.metrics.log_startup(replica)
            simulation.env.metrics.log_setup(replica)
            simulation.env.metrics.log_finish_deploy(replica)

    sim_ctx = create_sim_platform_context(sim_input, simulation)

    def create_load_balancer(env: Environment, fn: FunctionContainer):
        cluster = fn.labels[zone_label]
        scaling = 1.0
        # wrr = WrrLoadBalancer(env, cluster, SmoothWeightedRoundRobinProviderFactory(scaling))
        # rr_calculator = SmoothLrtWeightCalculator(env.context, lambda: env.now, 1, lrt_window=10)
        updater, wrr = load_balancer_factory(sim_input, cluster, env, scaling)
        lb_process.add(updater)
        return UpdateableLoadBalancerSimulator(wrr)

    simulation.env.simulator_factory = create_function_simulator_factory(create_load_balancer, application)

    for sim_replica in sim_ctx.replica_service.get_function_replicas():
        sim_replica.simulator = simulation.env.simulator_factory.create(simulation.env, sim_replica.container)
        if 'python' in sim_replica.fn_name:
            sim_replica.simulator.queue = simpy.Resource(simulation.env, capacity=4)

    simulation.init_environment(simulation.env)
    simulation.env.resource_monitor = ResourceMonitor(simulation.env, 1000000)

    benchmark.setup(simulation.env)

    context = init_pods_on_nodes(benchmark, simulation)
    local_scheduler_queues = {}
    local_schedulers = {}
    storage_local_schedulers = {}
    orchestration_configuration = sim_input.orchestration_configuration
    zones = simulation.env.context.zone_service.get_zones()
    for zone in zones:
        local_scheduler_queue = simpy.Store(simulation.env)
        local_scheduler_name = f'local-scheduler-{zone}'
        local_scheduler = (
            create_scheduler(orchestration_configuration.local_scheduler_configs[zone], zone, context,
                             simulation.env.metrics),
            local_scheduler_queue)
        local_schedulers[local_scheduler_name] = local_scheduler
        storage_local_schedulers[zone] = local_scheduler_name
    aux_global_scheduler_q = simpy.Store(simulation.env)
    global_scheduler = (
        create_global_scheduler(orchestration_configuration.global_scheduler_config, context, simulation.env,
                                storage_local_schedulers, aux_global_scheduler_q),
        simpy.Store(simulation.env))

    if orchestration_configuration.global_scheduler_config.scheduler_type == PRESSURE_GLOBAL_SCHEDULER:
        simulation.env.background_processes.append(global_scheduler[0].run)

    simulation.env.global_scheduler = global_scheduler
    simulation.env.local_schedulers = local_schedulers
    # simulation.env.scheduler = create_scheduler(simulation.env)
    simulation.env.background_processes.append(lb_process.run)

    # initialize autoscaler processes
    autoscaler_processes = create_autoscaler_processes(sim_input, simulation, aux_global_scheduler_q)
    simulation.env.background_processes.extend(autoscaler_processes)
    # run the simulation
    start = time.time()
    simulation.run()
    end = time.time()
    duration = end - start
    return duration, simulation


def create_global_scheduler(config: BaseGlobalSchedulerConfiguration, context: PlatformContext,
                            env: Environment, storage_local_schedulers: Dict[str, str], global_scheduler_q):
    if config.scheduler_type == LOCALITY_GLOBAL_SCHEDULER:
        return LocalityGlobalScheduler(config, storage_local_schedulers, context, env.metrics)
    elif config.scheduler_type == PRESSURE_GLOBAL_SCHEDULER:
        if type(config) is not PressureGlobalSchedulerConfiguration:
            raise ValueError('Invalid scheduler configuration')
        config: PressureGlobalSchedulerConfiguration = config
        default_config = BaseGlobalSchedulerConfiguration(config.scheduler_name, LOCALITY_GLOBAL_SCHEDULER, config.delay)
        default_global_scheduler = LocalityGlobalScheduler(default_config, storage_local_schedulers, context, env.metrics)
        pressure_global_scheduler = PressureGlobalScheduler(config, storage_local_schedulers, context, env.metrics,
                                                            lambda: env.now, SimFunctionReplicaFactory(),
                                                            default_global_scheduler)
        return SimPressureGlobalSchedulerDaemon(pressure_global_scheduler, global_scheduler_q)


def autoscaler_factory(sim_input: SimInput, simulation, global_scheduler_q: simpy.Store) -> List:
    clusters = simulation.env.context.zone_service.get_zones()
    autoscaler_type = sim_input.orchestration_configuration.autoscaler_config.autoscaler_type
    if autoscaler_type == HPA_AUTOSCALER:
        return setup_dece_hpa_latency_strategies(clusters, sim_input)
    elif autoscaler_type == PRESSURE_AUTOSCALER:
        return setup_pressure_strategies(sim_input, global_scheduler_q)


def create_autoscaler_processes(sim_input: SimInput, simulation, global_scheduler_q: simpy.Store):
    autoscaler_processes = autoscaler_factory(sim_input, simulation, global_scheduler_q)
    return autoscaler_processes


def init_pods_on_nodes(benchmark, simulation):
    container_image_names = set()

    cluster_ctx: ClusterContext = simulation.env.cluster
    for replica in simulation.env.context.replica_service.get_function_replicas():
        if replica.state == FunctionReplicaState.RUNNING:
            cluster_ctx.place_pod_on_node(replica.pod, replica.node.skippy_node)
            for container in replica.pod.spec.containers:
                image_name = normalize_image_name(container.image)
                container_image_names.add(image_name)
    for image_name in container_image_names:
        for node in simulation.env.context.node_service.get_nodes():
            if image_name not in cluster_ctx.images_on_nodes[node.name]:
                image_state = cluster_ctx.get_image_state(image_name)

                image_state.num_nodes += 1

                cluster_ctx.images_on_nodes[node.name][image_name] = image_state
    context = simulation.env.context
    add_all_images_to_nodes(context.node_service, benchmark.images)
    return context


def get_labels():
    return create_mixed_labels()


def extract_pressure_dfs(sim: Simulation, sim_input: SimInput) -> Dict[str, pd.DataFrame]:
    config: PressureAutoscalerConfiguration = sim_input.orchestration_configuration.autoscaler_config
    pressures = {}
    for zone in config.zone_parameters.keys():
        key = f'pressure/{zone}'
        pressures[key] = sim.env.metrics.extract_dataframe(key)
    return pressures


def execute_application_simulation(sim_input: SimInput):
    srds.seed(3482592)

    root_folder = 'results'
    application = list(sim_input.application_params.keys())[0]
    logger.info(f'Start {application} scenario {sim_input.scenario_filename}.')
    duration, sim = execute_benchmark(sim_input)
    env = sim.env
    logger.info(f'env.now: {env.now}')
    dfs = extract_dfs(sim)

    logger.info(f'Time passed in simulation: {env.now - sim_input.start_time}, wall time passed: {duration}')
    logger.info(sim_input.orchestration_configuration)
    invocations_df_ = dfs['invocations_df']
    invocations_df_ = invocations_df_[invocations_df_['function_name'] == 'pythonpi']
    mean = invocations_df_['t_exec'].mean()
    logger.info('Mean exec time %f', mean)
    traces_df_ = dfs['traces_df']
    traces_df_ = traces_df_[traces_df_['function_name'] == 'pythonpi']

    logger.info('Mean response time %f', (traces_df_['ts_end'] - traces_df_['ts_start']).mean())
    fets_df_ = dfs['fets_df']
    fets_df_ = fets_df_[fets_df_['function_name'] == 'pythonpi']

    logger.info('Median fets time %f', (fets_df_['ts_fet_end'] - fets_df_['ts_fet_start']).median())
    logger.info(f'Fets invocations: {len(dfs["fets_df"])}')

    logger.info(f'Saving results')

    if sim_input.save:
        if sim_input.orchestration_configuration.autoscaler_config.autoscaler_type == PRESSURE_AUTOSCALER:
            pressure_dfs = extract_pressure_dfs(sim, sim_input)
            dfs.update(pressure_dfs)
        results = save_results(root_folder, dfs, sim, f"{sim_input.name}-{sim_input.scenario_filename}-{time.time()}",
                               application)
        logger.info(f'Results saved under {results}')

    logger.info(f'End scenario {sim_input.scenario_filename}.')
    return dfs


def replicas_slo_violation(df, target_quality) -> float:
    return (df['resource_usage'].mean() / target_quality['resource']['target']) ** 2


def map_quality_results(quality_results: Dict[str, float], sim_input: SimInput) -> SimResults:
    fn_results = FunctionOptResult(sim_input.orchestration_configuration, quality_results)
    final_sim_result = SimResults(fn_results)
    return final_sim_result


def judge_sim_results_quality(fn_dfs: Dict[str, Dict[str, pd.DataFrame]], target_quality: Dict) -> Dict[str, float]:
    all_qualities = {}
    for fn, dfs in fn_dfs.items():
        quality_implementation = target_quality['implementation']
        if quality_implementation == 'performance':
            quality = rtt_slo_violation(dfs['traces_df'], target_quality)
        elif quality_implementation == 'balanced':
            impl_params = target_quality['implementation_params']
            quality = math.sqrt(rtt_slo_violation(dfs['traces_df'], target_quality)) * impl_params[
                'weight_performance'] + math.sqrt(
                replicas_slo_violation(dfs['total_replica_df'], target_quality)) * impl_params['weight_resources']
        elif quality_implementation == 'resource':
            quality = replicas_slo_violation(dfs['total_replica_df'], target_quality)
        else:
            raise ValueError(f'Unknown quality implementation: {quality_implementation}')
        all_qualities[fn] = quality
    return all_qualities


def rtt_slo_violation(df: pd.DataFrame, target_quality):
    percentile = df['rtt'].quantile(target_quality['rtt']['percentile'])
    quality_value = (percentile / target_quality['rtt']['target']) ** 2
    return quality_value


def rtt_percentile(df: pd.DataFrame, target_quality):
    percentile = df['rtt'].quantile(target_quality['rtt']['percentile'])
    return percentile


def judge_sim_results_quality_windowed(sim_results: SimResults, target_quality: Dict, window: str, agg_func):
    # Create windowed time frames using the Grouper
    sim_results.rtt['time'] = sim_results.rtt.index
    grouped = sim_results.rtt.groupby(pd.Grouper(key='time', freq=window))

    quality = {'time': [], 'quality': []}

    for t, group in grouped:
        if not group.empty:
            quality_value = agg_func(group, target_quality)
            quality['time'].append(t)
            quality['quality'].append(quality_value)

    # Here, quality is a list of qualities for each x-second interval
    return pd.DataFrame(data=quality)


class SimExecutor(abc.ABC):
    def execute_simulation(self, sim_input: SimInput) -> SimResults:
        raise NotImplementedError()


class VanillaSimExecutor(SimExecutor):

    def __init__(self, target_quality):
        self.target_quality = target_quality
        self.results = {}

    def execute_simulation(self, sim_input: SimInput) -> SimResults:
        self.randomize_autoscaler_parameters(sim_input)
        dfs = execute_application_simulation(sim_input)
        zones = list(sim_input.orchestration_configuration.local_scheduler_configs.keys())
        sim_results = transform_dfs_into_sim_results(dfs, list(sim_input.application_params.keys()), zones)
        quality_results = judge_sim_results_quality(sim_results, self.target_quality)
        final_sim_result = map_quality_results(quality_results, sim_input)
        return final_sim_result

    def randomize_autoscaler_parameters(self, sim_input: SimInput):
        """
        Modifies the passed SimInput by randomizing the autoscaler config, depending on the autoscaler type.
        """
        if sim_input.orchestration_configuration.autoscaler_config.autoscaler_type == HPA_AUTOSCALER:
            config: HpaAutoscalerConfiguration = sim_input.orchestration_configuration.autoscaler_config
            sim_input.orchestration_configuration.autoscaler_config = randomize_hpa_latency_parameter_space(
                config)
        elif sim_input.orchestration_configuration.autoscaler_config.autoscaler_type == PRESSURE_AUTOSCALER:
            config: PressureAutoscalerConfiguration = sim_input.orchestration_configuration.autoscaler_config
            randomize_config = randomize_pressure_autoscaler_parameter_space(
                config)
            sim_input.orchestration_configuration.autoscaler_config = randomize_config
            global_scheduler: PressureGlobalSchedulerConfiguration = sim_input.orchestration_configuration.global_scheduler_config
            global_scheduler.parameters = randomize_config.zone_parameters
        else:
            raise ValueError(
                f"Unknown autoscaler type {sim_input.orchestration_configuration.autoscaler_config.autoscaler_type}")


def execute_simulations(sim_input: SimInput) -> SimResults:
    dfs = execute_application_simulation(sim_input)
    sim_results = transform_dfs_into_sim_results(dfs)
    return sim_results
