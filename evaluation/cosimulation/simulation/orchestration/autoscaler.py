from typing import List

from faasopts.autoscalers.base.hpa.decentralized.latency import HorizontalLatencyPodAutoscalerParameters
from faasopts.autoscalers.k8s.hpa.decentralized.latency import DecentralizedHorizontalLatencyPodAutoscaler
from faasopts.autoscalers.simulation.hpa.decentralized.latency import \
    SimulationDecentralizedHorizontalLatencyPodAutoscaler
from sim.core import Environment

from evaluation.cosimulation.simulation.util import SimInput, HpaAutoscalerConfiguration


class DecentralizedAutoscalerDaemon:

    def __init__(self, cluster: str, reconcile_interval: float, scheduler_name: str,
                 params: HorizontalLatencyPodAutoscalerParameters):
        self.cluster = cluster
        self.reconcile_interval = reconcile_interval
        self.scheduler_name = scheduler_name
        self.params = params
        self.stopped = False

    def run(self, env):
        autoscaler = self.setup_autoscaler(env)
        metrics = env.metrics
        print(f'Start autoscaling for {self.cluster}, and {self.scheduler_name}')
        while not self.stopped:
            yield env.timeout(self.reconcile_interval)

            start_ts = env.now
            yield from autoscaler.run()
            end_ts = env.now
            metrics.log('scaling-duration', end_ts - start_ts)

    def setup_autoscaler(self, env: Environment) -> DecentralizedHorizontalLatencyPodAutoscaler:
        ctx = env.context
        metrics = env.metrics
        faas_system = env.faas

        return SimulationDecentralizedHorizontalLatencyPodAutoscaler(self.params, ctx, faas_system, metrics,
                                                                     lambda: env.now,
                                                                     cluster=self.cluster,
                                                                     replica_factory=ctx.replica_service.replica_service.replica_factory)


def setup_dece_hpa_latency_strategies(clusters: List[str], sim_input):
    background_processes = []
    config = sim_input.orchestration_configuration.autoscaler_config
    parameters = config.zone_parameters
    for cluster in clusters:
        autoscaler_parameters: HorizontalLatencyPodAutoscalerParameters = parameters[cluster]
        autoscaler = DecentralizedAutoscalerDaemon(cluster, config.reconcile_interval,
                                                   sim_input.orchestration_configuration.global_scheduler_config.scheduler_name,
                                                   autoscaler_parameters)
        background_processes.append(autoscaler.run)

    return background_processes
