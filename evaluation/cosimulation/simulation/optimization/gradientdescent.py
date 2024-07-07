import logging
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed, Future
from typing import Tuple, List

import numpy as np
from faasopts.utils.pressure.calculation import \
    PressureNetworkLatencyFulfillmentFunction, PressureNormalizedNetworkLatencyFunction, \
    PressureNetworkLatencyRequirementLogFunction, PressureRTTLogFunction, PressureCpuUsageFunction, \
    PressureRequestFunction

from evaluation.cosimulation.orchestration.model import PressureAutoscalerConfiguration, HpaAutoscalerConfiguration, \
    OrchestrationConfiguration, PRESSURE_MIN_MAX_MAPPING, PRESSURE_WEIGHT_MAPPING, SimInput, SimResults
from evaluation.cosimulation.simulation.util import SimExecutor, execute_application_simulation, \
    judge_sim_results_quality, \
    transform_dfs_into_sim_results, map_quality_results
from evaluation.cosimulation.util.constants import PRESSURE_AUTOSCALER, HPA_AUTOSCALER

HPA_TARGET_DURATION_PARAMETER_KEY = 'target_duration'
PRESSURE_MAX_THRESHOLD_PARAMETER_KEY = 'max_threshold'
PRESSURE_MIN_THRESHOLD_PARAMETER_KEY = 'min_threshold'
PRESSURE_CPU_USAGE_PARAMETER_KEY = PressureCpuUsageFunction.name
PRESSURE_RTT_LOG_PARAMETER_KEY = PressureRTTLogFunction.name
PRESSURE_NETWORK_LATENCY_REQUIREMENT_LOG_PARAMETER_KEY = PressureNetworkLatencyRequirementLogFunction.name
PRESSURE_NETWORK_LATENCY_FULFILLMENT_PARAMETER_KEY = PressureNetworkLatencyFulfillmentFunction.name
PRESSURE_NORMALIZED_NETWORK_LATENCY_PARAMETER_KEY = PressureNormalizedNetworkLatencyFunction.name
PRESSURE_REQUEST_PARAMETER_KEY = PressureRequestFunction.name
logger = logging.getLogger(__name__)


def flatten_pressure_min_max_autoscaler_parameters(config: PressureAutoscalerConfiguration) -> Tuple[
    np.ndarray, List[Tuple[str, str, str]]]:
    """
    Returns a tuple consisting of an numpy array that contains the parameters of all autoscalers, and a list of tuples.
    The list of tuples specifies each parameter in the numpy array.
    In example: (zone, function, parameter name).
    The list of tuples allows us to map the flattened parameters back to a PressureAutoscalerConfiguration.
    """
    flattened_params = []
    metadata = []
    for zone, fn_params in config.zone_parameters.items():
        for fn, params in fn_params.function_parameters.items():
            flattened_params.append(params.max_threshold)
            metadata.append((zone, fn, PRESSURE_MAX_THRESHOLD_PARAMETER_KEY))

            flattened_params.append(params.min_threshold)
            metadata.append((zone, fn, PRESSURE_MIN_THRESHOLD_PARAMETER_KEY))

    return np.asarray(flattened_params), metadata


def flatten_pressure_weight_autoscaler_parameters(config: PressureAutoscalerConfiguration) -> Tuple[
    np.ndarray, List[Tuple[str, str, str]]]:
    """
    Returns a tuple consisting of an numpy array that contains the parameters of all autoscalers, and a list of tuples.
    The list of tuples specifies each parameter in the numpy array.
    In example: (zone, function, parameter name).
    The list of tuples allows us to map the flattened parameters back to a PressureAutoscalerConfiguration.
    """
    flattened_params = []
    metadata = []
    for zone, fn_params in config.zone_parameters.items():
        for fn, params in fn_params.function_parameters.items():
            for pressure_key, weight in params.pressure_weights.items():
                flattened_params.append(weight)
                metadata.append((pressure_key, zone, fn))

    return np.asarray(flattened_params), metadata


def flatten_hpa_autoscaler_parameters(config: HpaAutoscalerConfiguration) -> Tuple[
    np.ndarray, List[Tuple[str, str, str]]]:
    """
    Returns a tuple consisting of an numpy array that contains the parameters of all autoscalers, and a list of tuples.
    The list of tuples specifies each parameter in the numpy array.
    In example: (zone, function, parameter name).
    The list of tuples allows us to map the flattened parameters back to a HpaAutoscalerConfiguration.
    """
    flattened_params = []
    metadata = []
    for zone, fn_params in config.zone_parameters.items():
        for fn, params in fn_params.function_parameters.items():
            flattened_params.append(params.target_duration)
            metadata.append((zone, fn, HPA_TARGET_DURATION_PARAMETER_KEY))
    return np.asarray(flattened_params), metadata


def flatten_autoscaler_parameters(orchestration_parameters: OrchestrationConfiguration) -> Tuple[
    np.ndarray, List[Tuple[str, str, str]]]:
    config = orchestration_parameters.autoscaler_config
    if config.autoscaler_type == PRESSURE_AUTOSCALER:
        config: PressureAutoscalerConfiguration = config
        if config.mapping_type == PRESSURE_MIN_MAX_MAPPING:
            return flatten_pressure_min_max_autoscaler_parameters(config)
        elif config.mapping_type == PRESSURE_WEIGHT_MAPPING:
            return flatten_pressure_weight_autoscaler_parameters(config)
    elif config.autoscaler_type == HPA_AUTOSCALER:
        config: HpaAutoscalerConfiguration = config
        return flatten_hpa_autoscaler_parameters(config)
    else:
        raise ValueError(f'Cannot flatten parameters: unknown autoscaler {config.autoscaler_type}')




def copy_sim_input_with_new_params(x: SimInput, flattened_params: np.ndarray, metadata: List[Tuple[str, str, str]],
                                   postfix: str = None, i: int = None,
                                   j: int = None) -> SimInput:
    copied_sim_input = x.copy()

    def setter_pressure_min_threshold(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].min_threshold = flattened_params[value_idx]

    def setter_pressure_max_threshold(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].max_threshold = flattened_params[value_idx]

    def set_pressure_cpu_usage_weight(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].pressure_weights[PRESSURE_CPU_USAGE_PARAMETER_KEY] = flattened_params[value_idx]

    def set_pressure_rtt_log_weight(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].pressure_weights[PRESSURE_RTT_LOG_PARAMETER_KEY] = flattened_params[value_idx]

    def set_pressure_network_latency_req_log_weight(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].pressure_weights[PRESSURE_NETWORK_LATENCY_REQUIREMENT_LOG_PARAMETER_KEY] = flattened_params[value_idx]

    def set_pressure_network_latency_fulfillment_weight(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].pressure_weights[PRESSURE_NETWORK_LATENCY_FULFILLMENT_PARAMETER_KEY] = flattened_params[value_idx]

    def set_pressure_normalized_network_latency_weight(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].pressure_weights[PRESSURE_NORMALIZED_NETWORK_LATENCY_PARAMETER_KEY] = flattened_params[value_idx]

    def set_pressure_request_weight(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].pressure_weights[PRESSURE_REQUEST_PARAMETER_KEY] = flattened_params[value_idx]

    def setter_hpa_target_duration(value_idx):
        copied_sim_input.orchestration_configuration.autoscaler_config.zone_parameters[zone].function_parameters[
            fn].target_duration = flattened_params[value_idx]

    setter_dict = {
        PRESSURE_MIN_THRESHOLD_PARAMETER_KEY: setter_pressure_min_threshold,
        PRESSURE_MAX_THRESHOLD_PARAMETER_KEY: setter_pressure_max_threshold,
        PRESSURE_CPU_USAGE_PARAMETER_KEY: set_pressure_cpu_usage_weight,
        HPA_TARGET_DURATION_PARAMETER_KEY: setter_hpa_target_duration,
        PRESSURE_RTT_LOG_PARAMETER_KEY: set_pressure_rtt_log_weight,
        PRESSURE_NETWORK_LATENCY_REQUIREMENT_LOG_PARAMETER_KEY: set_pressure_network_latency_req_log_weight,
        PRESSURE_NETWORK_LATENCY_FULFILLMENT_PARAMETER_KEY: set_pressure_network_latency_fulfillment_weight,
        PRESSURE_NORMALIZED_NETWORK_LATENCY_PARAMETER_KEY: set_pressure_normalized_network_latency_weight,
        PRESSURE_REQUEST_PARAMETER_KEY: set_pressure_request_weight
    }

    for idx, (zone, fn, key) in enumerate(metadata):
        setter_dict[key](idx)

    prepared_name = f'{x.name}-{postfix}-{j}-{i}' if postfix else x.name
    copied_sim_input.name = prepared_name
    return copied_sim_input


class GradientDescentExecutor(SimExecutor):

    def __init__(self, target_quality, params=None):
        self.target_quality = target_quality
        self.learning_rate = 0.1 if params is None else params['learning_rate']
        self.max_iterations = 5 if params is None else params['max_iterations']
        self.tolerance = 1e-4 if params is None else params['tolerance']
        self.epsilon = 0.2 if params is None else params['epsilon']
        self.results = {'plus': [], 'minus': [], 'grad': [], 'quality_plus': defaultdict(list),
                        'quality_minus': defaultdict(list)}

    def execute_simulation(self, sim_input: SimInput) -> SimResults:
        return self.gradient_descent(sim_input, self.learning_rate, self.max_iterations, self.tolerance, self.epsilon)

    def simulation(self, x: SimInput) -> SimResults:
        dfs = execute_application_simulation(x)
        zones = list(x.orchestration_configuration.local_scheduler_configs.keys())
        sim_results = transform_dfs_into_sim_results(dfs, list(x.application_params.keys()), zones)
        quality_results = judge_sim_results_quality(sim_results, self.target_quality)
        for fn, quality in quality_results.items():
            if 'plus' in x.name:
                self.results['quality_plus'][fn].append(quality)
            else:
                self.results['quality_minus'][fn].append(quality)
        sim_results = map_quality_results(quality_results, x)
        return sim_results

    def approximate_gradient(self, x: SimInput, j, epsilon=0.2):
        # Approximates the gradient of the simulation function at x
        flattened_params, metadata = flatten_autoscaler_parameters(x.orchestration_configuration)
        grad = np.zeros_like(flattened_params)
        futures = []
        with ProcessPoolExecutor(max_workers=len(flattened_params)) as executor:
            for i in range(len(flattened_params)):
                futures.append(
                    executor.submit(self.execute_simulation_param, epsilon, flattened_params, grad, i, j, x, metadata))

        for future in as_completed(futures):
            i, grad_val = future.result()
            logger.error(f'Finished: {grad_val}, {i}')
            grad[i] = grad_val
        return grad

    def execute_simulation_param(self, epsilon: float, flattened_params: np.ndarray, grad: np.ndarray, i: int, j: int,
                                 x: SimInput, metadata: List[Tuple[str, str, str]]):
        flattened_params_plus = np.array(flattened_params)
        flattened_params_minus = np.array(flattened_params)
        flattened_params_plus[i] += epsilon
        flattened_params_minus[i] -= epsilon
        if flattened_params_minus[i] <= 0:
            flattened_params_minus[i] = 0.1
        if flattened_params_plus[i] >= 100:
            flattened_params_plus[i] = 99
        if flattened_params_minus[i] >= 100:
            flattened_params_minus[i] = 99
        sim_input_plus = copy_sim_input_with_new_params(x, flattened_params_plus, metadata, 'plus', i, j)
        sim_input_minus = copy_sim_input_with_new_params(x, flattened_params_minus, metadata, 'minus', i, j)
        self.results['plus'].append(flattened_params_plus)
        self.results['minus'].append(flattened_params_minus)
        with ProcessPoolExecutor(max_workers=2) as executor:
            future_plus: Future = executor.submit(self.simulation, sim_input_plus)
            future_minus = executor.submit(self.simulation, sim_input_minus)
            simulation_plus_result = future_plus.result()
            simulation_minus_result = future_minus.result()
            grad_val = (simulation_plus_result - simulation_minus_result) / (2 * epsilon)
            grad[i] = grad_val
            self.results['grad'].append(grad)
            return i, grad_val

    def gradient_descent(self, initial_x: SimInput, learning_rate=0.1, max_iterations=1, tolerance=1e-4,
                         epsilon=0.2) -> SimResults:
        x = initial_x
        flattened_params, metadata = flatten_autoscaler_parameters(x.orchestration_configuration)
        for j in range(max_iterations):
            logger.error(f'Start Iteration {j}')
            x = copy_sim_input_with_new_params(x, flattened_params, metadata)
            grad = self.approximate_gradient(x, j, epsilon)
            logger.error(f'Finished Iteration {j}')

            if np.linalg.norm(grad) < tolerance:
                break
            flattened_params -= learning_rate * grad
            for i in range(len(flattened_params)):
                if flattened_params[i] <= 0:
                    flattened_params[i] = 0.1
                if flattened_params[i] >= 100:
                    flattened_params[i] = 99
            pass
        logger.error(f'Finished gradient descent')
        x = copy_sim_input_with_new_params(x, flattened_params, metadata)
        sim_result = self.simulation(x)
        return sim_result
