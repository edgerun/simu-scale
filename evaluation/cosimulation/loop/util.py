import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Dict, List

from ether.topology import Topology
from faas.system import FunctionReplicaState
from faas.system.autoscaler.core import BaseAutoscalerConfiguration, ReconciledBaseAutoscalerConfiguration
from faas.system.scheduling.decentralized import BaseGlobalSchedulerConfiguration, BaseLocalSchedulerConfiguration
from faas.util.constant import zone_label, client_role_label, controller_role_label
from faasopts.schedulers.decentralized.pressure.globalscheduler import PressureGlobalSchedulerConfiguration
from galileofaas.connections import RedisClient
from galileofaas.context.model import GalileoFaasContext
from sim.context.platform.replica.model import SimFunctionReplica
from sim.skippy import create_function_pod

from evaluation.cosimulation.simulation.optimization.gradientdescent import GradientDescentExecutor
from evaluation.cosimulation.simulation.util import create_sim_node, \
    VanillaSimExecutor, HpaAutoscalerConfiguration, PressureAutoscalerConfiguration, \
    fetch_hpa_autoscaler_params, fetch_pressure_autoscaler_params
from evaluation.cosimulation.transformation.raw import convert_sim_replica_to_k8s_replica
from evaluation.cosimulation.util.constants import HPA_AUTOSCALER, PRESSURE_AUTOSCALER, PRESSURE_GLOBAL_SCHEDULER, \
    LOCALITY_GLOBAL_SCHEDULER, CPU_LOCAL_SCHEDULER, BALANCED_LOCAL_SCHEDULER

logger = logging.getLogger(__name__)


def parse_cli_arguments():
    if len(sys.argv) == 2:
        return read_co_sim_input_from_json(sys.argv[1])
    else:
        raise ValueError(
            "Usage: python main.py <cosim-config-file-name>")


@dataclass
class CoSimInput:
    # determines if scenarios should be executed in parallel
    # in sim_params, the number of scenarios is defined to generate (all will have the same configuration)
    max_workers: int
    sim_executor_name: str
    sim_executor_params: Dict
    workload_profile: Dict
    target_quality: Dict
    save_simulations: bool
    sleep: int
    sim_executor_params_name: str
    target_quality_file_name: str
    sim_params: Dict
    sim_params_name: str
    workload_profile_name: str
    autoscaler_config_name: str
    autoscaler_config_file_name: str
    topology: str
    global_scheduler_name: str
    application_params: Dict
    application_params_file_name: str
    target_cluster: str
    global_scheduler_config_file_name: str
    global_scheduler_config_name: str
    local_schedulers_config_file_name: str
    local_schedulers_config_name: str
    n_iterations: int


def read_co_sim_input_from_json(json_file_name: str) -> CoSimInput:
    with open(json_file_name, 'r') as f:
        json_obj = json.load(f)
        workload_profile_file_name = json_obj['workload_profile_file_name']
        workload_profile_name = os.path.basename(workload_profile_file_name)
        workload_profile = read_json_file(workload_profile_file_name)

        sim_executor_name = json_obj['sim_executor_name']
        sim_executor_params_file_name = json_obj['sim_executor_params_file_name']
        sim_executor_params_name = os.path.basename(sim_executor_params_file_name)
        sim_executor_params = read_json_file(sim_executor_params_file_name)

        target_quality_file_name = json_obj['target_quality_file_name']
        target_quality = read_json_file(target_quality_file_name)

        max_workers = int(json_obj['max_workers'])
        save_simulations = json_obj['save_simulations'] == 'True'
        sleep = int(json_obj['sleep'])

        application_params_file_name = json_obj['application_params_file_name']
        application_params = read_json_file(application_params_file_name)

        sim_params_file_name = json_obj['sim_params_file_name']
        sim_params = read_json_file(sim_params_file_name)
        sim_params_name = os.path.basename(sim_params_file_name)

        autoscaler_config_file_name = json_obj['autoscaler_config_file_name']
        autoscaler_config_name = os.path.basename(autoscaler_config_file_name)
        global_scheduler_config_file_name = json_obj['global_scheduler_config_file_name']
        global_scheduler_config_name = os.path.basename(global_scheduler_config_file_name)
        local_scheduler_config_file_name = json_obj['local_scheduler_config_file_name']
        local_schedulers_config_name = os.path.basename(local_scheduler_config_file_name)

        topology = json_obj['topology']
        global_scheduler_name = json_obj['global_scheduler_name']
        target_cluster = json_obj['target_cluster']

        n_iterations = int(json_obj['n_iterations'])

        return CoSimInput(max_workers=max_workers,
                          sim_executor_name=sim_executor_name,
                          sim_executor_params=sim_executor_params,
                          workload_profile=workload_profile,
                          target_quality=target_quality,
                          save_simulations=save_simulations,
                          sleep=sleep,
                          sim_executor_params_name=sim_executor_params_name,
                          target_quality_file_name=target_quality_file_name,
                          sim_params=sim_params,
                          sim_params_name=sim_params_name,
                          workload_profile_name=workload_profile_name,
                          autoscaler_config_name=autoscaler_config_name,
                          autoscaler_config_file_name=autoscaler_config_file_name,
                          topology=topology,
                          global_scheduler_name=global_scheduler_name,
                          application_params=application_params,
                          application_params_file_name=application_params_file_name,
                          target_cluster=target_cluster,
                          global_scheduler_config_file_name=global_scheduler_config_file_name,
                          global_scheduler_config_name=global_scheduler_config_name,
                          local_schedulers_config_file_name=local_scheduler_config_file_name,
                          local_schedulers_config_name=local_schedulers_config_name,
                          n_iterations=n_iterations)


def get_autoscaler_configuration(autoscaler_config_file_name: str,
                                 fetch_params: bool) -> ReconciledBaseAutoscalerConfiguration:
    config = read_json_file(autoscaler_config_file_name)
    if len(config) == 0:
        raise ValueError(f'No autoscaler configuration found at {autoscaler_config_file_name}')
    if config['autoscaler_type'] == HPA_AUTOSCALER:
        if fetch_params:
            return fetch_hpa_autoscaler_params()
        else:
            return HpaAutoscalerConfiguration.from_json(json.dumps(config))
    elif config['autoscaler_type'] == PRESSURE_AUTOSCALER:
        if fetch_params:
            return fetch_pressure_autoscaler_params()
        else:
            return PressureAutoscalerConfiguration.from_json(json.dumps(config))
    else:
        raise ValueError(f'Unknown autoscaler type: {config["autoscaler_type"]}')


def get_global_scheduler_config(global_scheduler_config_file_name: str,
                                autoscaler_config: BaseAutoscalerConfiguration = None) -> BaseGlobalSchedulerConfiguration:
    config = read_json_file(global_scheduler_config_file_name)
    if len(config) == 0:
        raise ValueError(f'No global scheduler configuration found at {global_scheduler_config_file_name}')
    if config['scheduler_type'] == PRESSURE_GLOBAL_SCHEDULER:
        # TODO the global scheduler needs the same parameters as the autoscaler
        autoscaler_config: PressureAutoscalerConfiguration = autoscaler_config
        parameters = autoscaler_config.zone_parameters
        return PressureGlobalSchedulerConfiguration(config['scheduler_name'],
                                                    config['scheduler_type'],
                                                    float(config['delay']), parameters)
    elif config['scheduler_type'] == LOCALITY_GLOBAL_SCHEDULER:
        return BaseGlobalSchedulerConfiguration.from_json(json.dumps(config))
    else:
        raise ValueError(f'Unknown global scheduler type: {config["scheduler_type"]}')


def wait_for_exp_start():
    rds = RedisClient.from_env()
    try:
        for event in rds.sub('experiment'):
            if event['data'] == 'START':
                return
    finally:
        if rds is not None:
            rds.close()


def add_load_balancers_and_workers(ctx: GalileoFaasContext, topology: Topology):
    # DEV STUFF BEGIN
    nodes = topology.get_nodes()

    for node in nodes:
        cluster = node.labels.get(zone_label)

        if node.labels.get(client_role_label):
            fn_name = f'galileo-worker-{node.labels[zone_label]}'
            galileo_worker_deployment = ctx.deployment_service.get_by_name(fn_name)
            galileo_worker_container = galileo_worker_deployment.get_containers()[0]
            galileo_worker_ether_node = node
            galileo_worker_node = create_sim_node(galileo_worker_ether_node, topology)

            galileo_worker_replica = SimFunctionReplica(fn_name, {zone_label: cluster}, galileo_worker_deployment,
                                                        galileo_worker_container, galileo_worker_node,
                                                        FunctionReplicaState.RUNNING)

            galileo_worker_pod = create_function_pod(galileo_worker_deployment, galileo_worker_container)
            galileo_worker_replica.pod = galileo_worker_pod
            galileo_worker_k8s_replica = convert_sim_replica_to_k8s_replica(galileo_worker_replica)
            ctx.replica_service.add_function_replica(galileo_worker_k8s_replica)

        if node.labels.get(controller_role_label):
            fn_name = f'load-balancer-{node.name}'
            api_gateway_deployment = ctx.deployment_service.get_by_name(fn_name)
            api_gateway_container = api_gateway_deployment.get_containers()[0]
            api_gateway_ether_node = node
            api_gateway_node = create_sim_node(api_gateway_ether_node, topology)

            api_gateway_replica = SimFunctionReplica(fn_name, {zone_label: cluster}, api_gateway_deployment,
                                                     api_gateway_container, api_gateway_node,
                                                     FunctionReplicaState.RUNNING)
            api_gateway_pod = create_function_pod(api_gateway_deployment, api_gateway_container)
            api_gateway_replica.pod = api_gateway_pod
            api_gateway_k8s_replica = convert_sim_replica_to_k8s_replica(api_gateway_replica)
            ctx.replica_service.add_function_replica(api_gateway_k8s_replica)


def create_sim_executor(sim_executor_name, target_quality, params: Dict):
    if sim_executor_name == "GD":
        return GradientDescentExecutor(target_quality, params)
    if sim_executor_name == "Vanilla":
        return VanillaSimExecutor(target_quality)
    else:
        raise ValueError(f"Unknown simulation executor: {sim_executor_name}")


def read_json_file(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    else:
        return {}


def get_local_scheduler_configs(local_scheduler_config_file_name: str, zones: List[str],
                                global_scheduler_name: str) -> Dict[str, BaseLocalSchedulerConfiguration]:
    config = read_json_file(local_scheduler_config_file_name)
    configs = {}
    for zone, scheduler_config in config.items():
        if len(config) == 0:
            raise ValueError(f'No local scheduler configuration found at {local_scheduler_config_file_name}')
        elif scheduler_config['scheduler_type'] == BALANCED_LOCAL_SCHEDULER:
            base_config = BaseLocalSchedulerConfiguration(
                scheduler_name=f'local-scheduler-{zone}',
                scheduler_type=scheduler_config['scheduler_type'],
                global_scheduler_name=global_scheduler_name,
                delay=float(scheduler_config['delay']),
                zone=zone
            )
            configs[zone] = base_config
        elif scheduler_config['scheduler_type'] == CPU_LOCAL_SCHEDULER:
            base_config = BaseLocalSchedulerConfiguration(
                scheduler_name=f'local-scheduler-{zone}',
                scheduler_type=scheduler_config['scheduler_type'],
                global_scheduler_name=global_scheduler_name,
                delay=float(scheduler_config['delay']),
                zone=zone
            )
            configs[zone] = base_config
        else:
            raise ValueError(f'Unknown local scheduler type: {config["scheduler_type"]}')
    return configs
