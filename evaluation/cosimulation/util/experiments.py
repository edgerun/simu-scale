import logging
import pickle
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List

from evaluation.cosimulation.orchestration.model import RawPlatformContext, OrchestrationConfiguration

logger = logging.getLogger(__name__)

from evaluation.cosimulation.simulation.util import SimInput, SimExecutor, SimResults


def create_empty_raw_platform_ctx() -> RawPlatformContext:
    deployments = []
    replicas = []
    replica_resource_windows = []
    node_resource_windows = []
    responses = []
    raw_platform_ctx = RawPlatformContext(deployments, replicas, replica_resource_windows, node_resource_windows,
                                          responses)
    return raw_platform_ctx


def create_pickle_safe_copy(sim_input: SimInput) -> SimInput:
    return sim_input.copy_pickle_safe()


def run_simulations(sim_inputs, in_parallel, max_workers, sim_executor: SimExecutor) -> Dict[str, SimResults]:
    sim_results = {}
    if in_parallel:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            sim_results_futures = {executor.submit(sim_executor.execute_simulation, sim_input): sim_input
                                   for sim_input in sim_inputs.values()}
            for future in as_completed(sim_results_futures):
                sim_input = sim_results_futures[future]
                try:
                    result = future.result()
                    sim_results[sim_input.name] = result
                except Exception as exc:
                    logger.exception(exc)

    else:
        for sim_input in sim_inputs.values():
            interim_result = sim_executor.execute_simulation(sim_input)
            sim_results[sim_input.name] = interim_result
            with open(sim_input.name + '.pkl', 'wb+') as outfile:
                safe_copy = create_pickle_safe_copy(sim_input)
                outdata = {
                    'results': sim_executor.results,
                    'final_result': interim_result,
                    'final_params': safe_copy,
                    # 'topology': create_dece_hc_topology()
                }
                pickle.dump(outdata, outfile)
                sim_executor.results = {'plus': [], 'minus': [], 'grad': [], 'quality_plus': [], 'quality_minus': []}

    return sim_results


def judge_simulation_quality(sim_results: Dict[str, SimResults]):
    quality_results = defaultdict(int)
    for sim_input_name, results in sim_results.items():
        for fn, quality in results.result.quality.items():
            quality_results[sim_input_name] += quality
    return quality_results


def prepare_simulations(application_params, scenario_length, scenario, scenario_filename, start_ts, duration,
                        orchestration_configuration: OrchestrationConfiguration,
                        target_cluster: str, topology: str,
                        clients: List[str] = None,
                        n_scenarios: int = 1, save_simulations: bool = False, raw_ctx: RawPlatformContext = None):
    """
    Prepares simulation inputs for running simulations. Basically creates <n_scenarios> copies of the configurations
    to execute them afterward in parallel separate processes
    :rtype: Dict {str: SimInput}
    """
    if clients is None:
        clients = []

    if raw_ctx is None:
        raw_ctx = create_empty_raw_platform_ctx()

    start_time = time.time() - start_ts
    if scenario_length < start_time + duration:
        duration = scenario_length - start_time
    sim_inputs = {}
    workload_start_time = start_time
    for i in range(n_scenarios):
        sim_input = SimInput(str(i), start_time, duration, application_params, scenario, save=save_simulations,
                             raw_platform_ctx=raw_ctx,
                             workload_start_time=workload_start_time, clients=clients,
                             scenario_filename=scenario_filename,
                             orchestration_configuration=orchestration_configuration, target_cluster=target_cluster,
                             topology=topology)
        sim_inputs[str(i)] = sim_input

    return sim_inputs
