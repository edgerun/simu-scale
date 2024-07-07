from collections import defaultdict
from typing import List, Dict, Tuple

import pandas as pd
import simpy
from faas.context import PlatformContext
from faas.system import FunctionReplica, FaasSystem
from faasopts.autoscalers.simulation.pressure.autoscaler import SimPressureAutoscaler
from faasopts.schedulers.decentralized.pressure.globalscheduler import PressureGlobalScheduler
from faasopts.utils.pressure.api import PressureScaleScheduleEvent, PressureAutoscalerParameters
from faasopts.utils.pressure.service import PressureService
from sim.core import Environment


class SimPressureAutoscalerDaemon:

    def __init__(self, parameters: Dict[str, PressureAutoscalerParameters], reconcile_interval: float,
                 global_scheduler_q: simpy.Store):
        """
        :param parameters: dict of {cluster name: PressureAutoscalerParameters}
        :param reconcile_interval: in seconds
        """
        self.parameters = parameters
        self.reconcile_interval = reconcile_interval
        self.pressure_autoscalers = []
        self.global_scheduler_q = global_scheduler_q

    def run(self, env: Environment):
        self.pressure_autoscalers = self.setup_autoscalers(env)
        while True:
            yield env.timeout(self.reconcile_interval)
            all_pressure_values = []
            ps = []
            for pressure_autoscaler, _ in self.pressure_autoscalers:
                ps.append(env.process(pressure_autoscaler.run()))

            for p in ps:
                yield p

            for _, store in self.pressure_autoscalers:
                pressure_values = yield store.get()
                if pressure_values is not None:
                    all_pressure_values.append(pressure_values)

            if len(all_pressure_values) > 0:
                pressure = pd.concat(all_pressure_values)
                yield self.global_scheduler_q.put(pressure)

    def setup_autoscalers(self, env: Environment) -> List[Tuple[SimPressureAutoscaler, simpy.Store]]:
        """
        The return is a list of tuples of (SimPressureAutoscaler, Store).
        The store is used to send back the used pressure values.
        """
        ctx: PlatformContext = env.context
        metrics = env.metrics
        scalers = []
        all_scaler_parameters = self.parameters
        for zone, scaler_parameters in all_scaler_parameters.items():
            result_q = simpy.Store(env)
            pressure_service = PressureService(env.metrics)
            scalers.append((SimPressureAutoscaler(ctx, scaler_parameters,
                                                 zone, ctx.replica_service.replica_service.replica_factory,
                                                 lambda: env.now,
                                                  metrics, result_q, env.faas, env, pressure_service), result_q))
        return scalers


class SimPressureGlobalSchedulerDaemon:

    def __init__(self, global_scheduler: PressureGlobalScheduler, global_scheduler_q: simpy.Store):
        self.global_scheduler = global_scheduler
        self.global_scheduler_q = global_scheduler_q

    def run(self, env: Environment):
        while True:
            pressure_values = yield self.global_scheduler_q.get()
            scale_schedule_events: List[
                PressureScaleScheduleEvent] = self.global_scheduler.find_clusters_for_autoscaler_decisions(
                pressure_values)
            scale_ups: Dict[str, List[FunctionReplica]] = defaultdict(list)
            scale_downs: Dict[str, List[FunctionReplica]] = defaultdict(list)
            for scale in scale_schedule_events:
                if scale.delete:
                    scale_downs[scale.fn].extend(scale.replicas)
                else:
                    scale_ups[scale.fn].extend(scale.replicas)
            faas: FaasSystem = env.faas
            for fn, replicas in scale_ups.items():
                env.process(faas.scale_up(fn, replicas))
            for fn, replicas in scale_downs.items():
                env.process(faas.scale_down(fn, replicas))

    def find_cluster(self, replica: FunctionReplica) -> Tuple[str, str]:
        return self.global_scheduler.find_cluster(replica)
