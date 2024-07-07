import json
import logging
import multiprocessing
import os
import pickle
import time
from typing import Dict

import kubernetes.config
import srds
from faas.system import NullLogger
from galileofaas.connections import RedisClient
from galileofaas.context.daemon import GalileoFaasContextDaemon
from galileofaas.system.core import GalileoFaasMetrics

from evaluation.cosimulation.loop.util import parse_cli_arguments, CoSimInput, read_json_file, \
    get_autoscaler_configuration, get_global_scheduler_config, get_local_scheduler_configs, create_sim_executor, \
    add_load_balancers_and_workers
from evaluation.cosimulation.orchestration.model import OrchestrationConfiguration
from evaluation.cosimulation.setup import create_dece_hc_topology, setup_daemon
from evaluation.cosimulation.simulation.util import prepare_results_for_publishing, publish_updates, \
    extract_zones_from_topology, SimInput
from evaluation.cosimulation.transformation.raw import galileo_world_to_raw
from evaluation.cosimulation.util.experiments import prepare_simulations, run_simulations, judge_simulation_quality
from evaluation.cosimulation.util.k8s import get_hpa_component_configurations

logger = logging.getLogger(__name__)

srds.seed(3482592)



def set_function_max_scale():
    if os.environ.get('FUNCTION_MAX_SCALE', '') == '':
        logger.info('Fetching configurations and setting FUNCTION_MAX_SCALE')
        configs = get_hpa_component_configurations()
        os.environ['FUNCTION_MAX_SCALE'] = configs['autoscaler-zone-a']['FUNCTION_MAX_SCALE']


def main():
    stop_queue: multiprocessing.Queue = multiprocessing.Queue()
    try:
        kubernetes.config.load_config()
        co_sim_input = parse_cli_arguments()
        co_sim_loop = CoSimLoop(co_sim_input, stop_queue)
        co_sim_loop.start()
        stop_queue.get(block=True)
    finally:
        logger.info('Closing co_sim_loop')
        stop_queue.put(False)

    # with open('raw_ctx.pkl', 'rb') as fd:
    #     obj = pickle.load(fd)
    #     pass

    # run_cosim_loop(ctx_daemon, max_workers, sim_executor_name, sim_executor_params, workload_profile)





class CoSimLoop:

    def __init__(self, co_sim_input: CoSimInput, stop_queue: multiprocessing.Queue):
        self.max_workers = co_sim_input.max_workers
        self.sim_executor_name = co_sim_input.sim_executor_name
        self.sim_executor_params = co_sim_input.sim_executor_params
        self.workload_profile = co_sim_input.workload_profile
        self.workload_profile_name = co_sim_input.workload_profile_name
        self.target_quality = co_sim_input.target_quality
        self.stop_queue = stop_queue
        self.save_simulations = co_sim_input.save_simulations
        self.sleep_time = co_sim_input.sleep
        self.sim_executor_params_name = co_sim_input.sim_executor_params_name
        self.target_quality_file_name = co_sim_input.target_quality_file_name
        self.sim_params = co_sim_input.sim_params
        self.sim_params_name = co_sim_input.sim_params_name
        self.autoscaler_config_file_name = co_sim_input.autoscaler_config_file_name
        self.autoscaler_config_name = co_sim_input.autoscaler_config_name
        self.topology_name = co_sim_input.topology
        self.global_scheduler_name = co_sim_input.global_scheduler_name
        self.application_params = co_sim_input.application_params
        self.application_param_file_name = co_sim_input.application_params_file_name
        self.target_cluster = co_sim_input.target_cluster
        self.global_scheduler_config_file_name = co_sim_input.global_scheduler_config_file_name
        self.global_scheduler_config_name = co_sim_input.global_scheduler_config_name
        self.local_schedulers_config_file_name = co_sim_input.global_scheduler_config_file_name
        self.local_schedulers_config_name = co_sim_input.global_scheduler_config_name

    def start(self):
        t = multiprocessing.Process(target=self.run_cosim_loop)
        t.start()
        return t

    def is_running(self) -> bool:
        try:
            value = self.stop_queue.get_nowait()
            return value
        except Exception:
            return True

    def run_cosim_loop(self):
        kubernetes.config.load_config()
        workload_profile = self.workload_profile
        max_workers = self.max_workers
        sim_executor_params = self.sim_executor_params
        sim_executor_name = self.sim_executor_name
        target_quality = self.target_quality
        ctx_daemon: GalileoFaasContextDaemon = None
        try:
            logger.info("Starting CoSim loop")
            start_ts = time.time()
            set_function_max_scale()
            logger.info("Fetched Function Max Scale")

            # galileo_faas_redis_host
            # galileo_faas_redis_password
            # galileo_faas_redis_port
            rds = RedisClient.from_env()
            galileo_logger = NullLogger()
            galileo_metrics = GalileoFaasMetrics(galileo_logger)
            topology = self.create_topology()
            ctx_daemon = setup_daemon(rds, galileo_metrics, self.global_scheduler_name, True, topology)

            add_load_balancers_and_workers(ctx_daemon.context, topology)
            ctx_daemon.start()

            scenario = workload_profile['profiles']

            scenario_length = int(self.workload_profile['scenario_length_seconds'])

            sim_params = {
                'target_quality': target_quality,
                'scenario': scenario,
                'scenario_name': self.workload_profile_name,
                'executor': sim_executor_name,
                'executor_params': sim_executor_params,
                'max_workers': max_workers,
                'target_quality_filename': self.target_quality_file_name,
                'sim_executor_params_name': self.sim_executor_params_name,
                'sim_params': self.sim_params,
                'sim_params_name': self.sim_params_name,
                'topology': self.topology_name,
                'autoscaler_config_name': self.autoscaler_config_name,
                'autoscaler_config_file_name': self.autoscaler_config_file_name,
                'autoscaler_config': json.dumps(read_json_file(self.autoscaler_config_file_name)),
                'application_param_file_name': self.application_param_file_name,
                'application_params': self.application_params,
                'target_cluster': self.target_cluster,
                'global_scheduler_config_name': self.global_scheduler_config_name,
                'global_scheduler_config_file_name': self.global_scheduler_config_file_name,
                'local_scheduler_config_name': self.global_scheduler_config_name,
                'local_scheduler_config_file_name': self.global_scheduler_config_file_name,

            }

            zones = extract_zones_from_topology(topology)
            autoscaler_config = get_autoscaler_configuration(self.autoscaler_config_file_name, fetch_params=True)
            global_scheduler_config = get_global_scheduler_config(self.global_scheduler_config_file_name,
                                                                  autoscaler_config)
            local_scheduler_configs = get_local_scheduler_configs(self.local_schedulers_config_file_name, zones,
                                                                  self.global_scheduler_name)
            orchestration_configuration = OrchestrationConfiguration(autoscaler_config=autoscaler_config,
                                                                     global_scheduler_config=global_scheduler_config,
                                                                     local_scheduler_configs=local_scheduler_configs)

            while self.is_running():
                time.sleep(self.sleep_time)
                if not self.is_running():
                    logger.info('Break out loop')
                    break
                now = time.time()
                duration = int(self.sim_params['sim_duration'])
                n_scenarios = int(sim_executor_params['n_scenarios'])

                sim_start_time = now - 60
                raw_ctx = galileo_world_to_raw(ctx_daemon.context, sim_start_time, list(self.application_params.keys()))
                if raw_ctx is None or (len(raw_ctx.deployments) == 0 or len(raw_ctx.replicas) == 0):
                    logger.info("No deployments")
                    continue

                sim_params_msg = f'{time.time()} exp-params {json.dumps(sim_params, indent=4)}'
                rds.publish_async('galileo/events', sim_params_msg)
                with open(f'data/raw_ctx/raw_ctx_{time.time()}.pkl', 'wb+') as fd:
                    pickle.dump(raw_ctx, fd)

                clients = ['galileo-worker-zone-a', 'galileo-worker-zone-b']
                sim_inputs: Dict[str, SimInput] = prepare_simulations(self.application_params, scenario_length,
                                                                      scenario,
                                                                      self.workload_profile_name, start_ts, duration,
                                                                      clients=clients,
                                                                      n_scenarios=n_scenarios,
                                                                      save_simulations=self.save_simulations,
                                                                      raw_ctx=raw_ctx,
                                                                      topology=self.topology_name,
                                                                      target_cluster=self.target_cluster,
                                                                      orchestration_configuration=orchestration_configuration)
                sim_executor = create_sim_executor(sim_executor_name, target_quality, sim_executor_params)
                sim_results = run_simulations(sim_inputs, max_workers > 1, max_workers, sim_executor)
                quality_results = judge_simulation_quality(sim_results)
                logger.info(quality_results)
                best_input = min(quality_results, key=quality_results.get)
                best_input = sim_results[best_input].result

                # Process and publish the result of the best input
                # first dict level contains functions, and value is dict of updates (key -> cluster, value -> params)
                rds_updates: Dict[str, str] = prepare_results_for_publishing(best_input)

                # set parameters to the ones just found to be best for the next iteration
                autoscaler_parameters_as_dict = best_input
                logger.info("Chosen parameters to publish via redis:")
                logger.info(rds_updates)
                publish_updates(rds_updates, rds)
        except Exception as e:
            logger.exception(e)
        finally:
            if ctx_daemon is not None:
                logger.info("Stopping ctx_daemon")
                ctx_daemon.stop(5, False)

    def create_topology(self):
        if self.topology_name == 'dece_hc':
            return create_dece_hc_topology()
        else:
            raise ValueError(f'Unknown topology: {self.topology_name}')


if __name__ == '__main__':
    formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
                " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
    logging.basicConfig(level=logging.ERROR, format=formatter)
    logger.info("Starting cosimulation")
    main()
