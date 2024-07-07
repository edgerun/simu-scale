import logging
from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json
from faas.system import FunctionResponse
from faas.system.autoscaler.core import ReconciledBaseAutoscalerConfiguration
from faas.system.scheduling.decentralized import BaseGlobalSchedulerConfiguration, BaseLocalSchedulerConfiguration
from faasopts.autoscalers.base.hpa.decentralized.latency import HorizontalLatencyPodAutoscalerParameters
from faasopts.utils.pressure.api import PressureAutoscalerParameters
from sim.context.platform.deployment.model import SimFunctionDeployment
from sim.context.platform.replica.model import SimFunctionReplica
from sim.resource import ReplicaResourceWindow, NodeResourceWindow

logger = logging.getLogger(__name__)

import copy
from typing import Dict

PRESSURE_MIN_MAX_MAPPING = 'min_max'
PRESSURE_WEIGHT_MAPPING = 'weights'


@dataclass
class RawPlatformContext:
    deployments: List[SimFunctionDeployment]
    replicas: List[SimFunctionReplica]
    replica_resource_windows: List[ReplicaResourceWindow]
    node_resource_windows: List[NodeResourceWindow]
    responses: List[FunctionResponse]


@dataclass
class OrchestrationConfiguration:
    autoscaler_config: ReconciledBaseAutoscalerConfiguration
    global_scheduler_config: BaseGlobalSchedulerConfiguration
    local_scheduler_configs: Dict[str, BaseLocalSchedulerConfiguration]
    load_balancer_type: str = 'rr'

    def copy(self):
        copied_local_scheduler_configs = {}
        for zone, local_scheduler_config in self.local_scheduler_configs.items():
            copied_local_scheduler_configs[zone] = local_scheduler_config.copy()
        return OrchestrationConfiguration(self.autoscaler_config.copy(), self.global_scheduler_config.copy(),
                                          copied_local_scheduler_configs)

    def as_json_update(self) -> Dict[str, str]:
        """
        Returns: JSON representation of the configuration
        { <orchestration_component>:  { <zone>: { <function>: <configuration> } } }
        except for load balancer: just str
        """
        return {

            "autoscaler_config": self.autoscaler_config.to_json(),
            "global_scheduler_config": self.global_scheduler_config.to_json(),
            "local_scheduler_configs": {zone: config.to_json() for zone, config in
                                        self.local_scheduler_configs.items()},
            "load_balancer_type": self.load_balancer_type
        }


@dataclass_json
@dataclass
class PressureAutoscalerConfiguration(ReconciledBaseAutoscalerConfiguration):
    # parameters per cluster (key: cluster, value: function parameters)
    zone_parameters: Dict[str, PressureAutoscalerParameters]
    # this is used to determine which parameters are flattened (i.e., used for optimization)
    # in case of min max, we only tune the min and maximum thresholds of the pressure function
    # in case of weight, we only tune the weights of the individual pressure functions
    mapping_type: str = PRESSURE_MIN_MAX_MAPPING

    def copy(self):
        copied_zone_parameters = {}
        for zone, parameters in self.zone_parameters.items():
            copied_zone_parameters[zone] = parameters.copy()
        return PressureAutoscalerConfiguration(self.autoscaler_type, self.reconcile_interval, copied_zone_parameters,
                                               self.mapping_type)


@dataclass_json
@dataclass
class HpaAutoscalerConfiguration(ReconciledBaseAutoscalerConfiguration):
    # parameters per cluster (key: cluster, value: function parameters)
    zone_parameters: Dict[str, HorizontalLatencyPodAutoscalerParameters]

    def copy(self):
        copied_zone_parameters = {}
        for zone, parameters in self.zone_parameters.items():
            copied_zone_parameters[zone] = parameters.copy()
        return HpaAutoscalerConfiguration(self.autoscaler_type, self.reconcile_interval, copied_zone_parameters)


@dataclass
class FunctionOptResult:
    input_params: OrchestrationConfiguration
    # { function: quality }
    quality: Dict[str, float]


@dataclass
class SimResults:
    # result contains parameters of each orchestration per zone and per function + the quality of each function
    result: FunctionOptResult


def copy_raw_platform_ctx(raw_platform_ctx: RawPlatformContext) -> RawPlatformContext:
    return RawPlatformContext(
        deployments=list(raw_platform_ctx.deployments),
        replicas=list(raw_platform_ctx.replicas),
        replica_resource_windows=list(raw_platform_ctx.replica_resource_windows),
        node_resource_windows=list(raw_platform_ctx.node_resource_windows),
        responses=list(raw_platform_ctx.responses)
    )


@dataclass
class SimInput:
    name: str
    start_time: float
    duration: int
    application_params: Dict
    scenario: Dict
    save: bool
    raw_platform_ctx: RawPlatformContext
    workload_start_time: float
    clients: List[str]
    scenario_filename: str
    orchestration_configuration: OrchestrationConfiguration
    # "central" cluster, used by local schedulers
    target_cluster: str
    # topology to create
    topology: str

    def copy(self):
        return SimInput(
            self.name,
            self.start_time,
            self.duration,
            copy.deepcopy(self.application_params),
            copy.deepcopy(self.scenario),
            self.save,
            copy_raw_platform_ctx(self.raw_platform_ctx),
            self.workload_start_time,
            copy.deepcopy(self.clients),
            self.scenario_filename,
            self.orchestration_configuration.copy(),
            self.target_cluster,
            self.topology
        )

    def copy_pickle_safe(self):
        return SimInput(
            self.name,
            self.start_time,
            self.duration,
            copy.deepcopy(self.application_params),
            copy.deepcopy(self.scenario),
            self.save,
            None,
            self.workload_start_time,
            copy.deepcopy(self.clients),
            self.scenario_filename,
            self.orchestration_configuration.copy(),
            self.target_cluster,
            self.topology
        )
