from typing import Dict, List

from faas.context import PlatformContext
from faas.system import Metrics, FunctionReplica
from faasopts.loadbalancers.wrr.wrr import WrrOptimizer, WeightCalculator
from sim.faas.loadbalancers import UpdateableLoadBalancer


class SimWrrOptimizer(WrrOptimizer):

    def __init__(self, lb: UpdateableLoadBalancer, context: PlatformContext, cluster: str, metrics: Metrics,
                 weight_calculator: WeightCalculator) -> None:
        super(SimWrrOptimizer, self).__init__(context, cluster, metrics, weight_calculator)
        self.lb = lb

    def set_weights(self, weights: Dict[str, Dict[str, float]]):
        self.lb.update(weights)

    def add_replica(self, replica: FunctionReplica):
        super().add_replica(replica)
        functions = self._get_function(replica)
        for function, actual_replica in functions:
            self.lb.add_replica(function, actual_replica)

    def remove_replica(self, replica: FunctionReplica):
        super().remove_replica(replica)

    def add_replicas(self, replicas: List[FunctionReplica]):
        super().add_replicas(replicas)
        for replica in replicas:
            self.add_replica(replica)

    def remove_replicas(self, replicas: List[FunctionReplica]):
        super().remove_replicas(replicas)

    def update(self):
        super().update()

