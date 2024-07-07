import logging
import logging
import time
from typing import Dict

from evaluation.cosimulation.loop.util import CoSimInput, parse_cli_arguments, get_global_scheduler_config, \
    get_autoscaler_configuration, get_local_scheduler_configs, create_sim_executor
from evaluation.cosimulation.orchestration.model import OrchestrationConfiguration
from evaluation.cosimulation.setup import create_dece_hc_topology
from evaluation.cosimulation.simulation.util import prepare_results_for_publishing, extract_zones_from_topology, \
    SimInput
from evaluation.cosimulation.util.experiments import prepare_simulations, run_simulations, judge_simulation_quality

logger = logging.getLogger(__name__)


def main():
    co_sim_input = parse_cli_arguments()
    co_sim_loop = CoSimLoop(co_sim_input)
    co_sim_loop.run_cosim_loop()

    # with open('raw_ctx.pkl', 'rb') as fd:
    #     obj = pickle.load(fd)
    #     pass

    # run_cosim_loop(ctx_daemon, max_workers, sim_executor_name, sim_executor_params, workload_profile)


class CoSimLoop:

    def __init__(self, co_sim_input: CoSimInput):
        self.max_workers = co_sim_input.max_workers
        self.sim_executor_name = co_sim_input.sim_executor_name
        self.sim_executor_params = co_sim_input.sim_executor_params
        self.workload_profile = co_sim_input.workload_profile
        self.workload_profile_name = co_sim_input.workload_profile_name
        self.target_quality = co_sim_input.target_quality
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
        self.local_scheduler_config_file_name = co_sim_input.local_schedulers_config_file_name
        self.local_scheduler_config_name = co_sim_input.local_schedulers_config_name
        self.n_iterations = co_sim_input.n_iterations

    def run_cosim_loop(self):
        """
        param: n_iterations: number of iterations to execute the optimization process, each consecutive iteration takes
        the orchestration configuration of the previous iteration.
        """
        workload_profile = self.workload_profile
        sim_executor_params = self.sim_executor_params
        sim_executor_name = self.sim_executor_name
        target_quality = self.target_quality
        start_start_ts = time.time()
        orchestration_configuration: OrchestrationConfiguration = None
        n_iterations = self.n_iterations

        for i in range(n_iterations):
            try:
                logger.info("Starting CoSim loop")
                start_ts = time.time()

                topology = self.create_topology()

                max_workers = 1
                n_scenarios = sim_executor_params.get('n_scenarios')
                n_scenarios = 1 if not n_scenarios else int(n_scenarios)
                scenario = workload_profile['profiles']
                scenario_length = int(self.workload_profile['scenario_length_seconds'])

                if orchestration_configuration is None:
                    zones = extract_zones_from_topology(topology)
                    autoscaler_config = get_autoscaler_configuration(self.autoscaler_config_file_name,
                                                                     fetch_params=False)
                    global_scheduler_config = get_global_scheduler_config(self.global_scheduler_config_file_name,
                                                                          autoscaler_config)
                    local_scheduler_configs = get_local_scheduler_configs(self.local_scheduler_config_file_name, zones,
                                                                          self.global_scheduler_name)
                    orchestration_configuration = OrchestrationConfiguration(autoscaler_config=autoscaler_config,
                                                                             global_scheduler_config=global_scheduler_config,
                                                                             local_scheduler_configs=local_scheduler_configs)
                else:
                    logger.info(f"Continuing with parameters from previous iteration: {orchestration_configuration}")

                duration = int(self.sim_params['sim_duration'])

                raw_ctx = None
                # raw_ctx_file = 'data/raw_ctx/raw_ctx_1710802198.6811175.pkl'
                # with open(raw_ctx_file, 'rb') as fd:
                #     raw_ctx = pickle.load(fd)

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
                orchestration_configuration = best_input.input_params
            except Exception as e:
                logger.exception(e)

        print(f'{time.time() - start_start_ts}')

    def create_topology(self):
        if self.topology_name == 'dece_hc':
            return create_dece_hc_topology()
        else:
            raise ValueError(f'Unknown topology: {self.topology_name}')


if __name__ == '__main__':
    formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
                " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
    logging.basicConfig(level=logging.INFO, format=formatter)
    logger.info("Starting cosimulation")
    main()
