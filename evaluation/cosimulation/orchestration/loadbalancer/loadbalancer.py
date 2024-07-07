import json
import logging
from collections import defaultdict
from typing import Dict

from faas.context import PlatformContext
from faas.system import FunctionReplicaState, Metrics
from faasopts.loadbalancers.wrr.wrr import WrrOptimizer, GlobalWrrOptimizer, WeightCalculator
from galileofaas.connections import EtcdClient
from galileofaas.context.platform.replica.k8s import KubernetesFunctionReplicaService

logger = logging.getLogger(__name__)


class K8sLoadBalancer(WrrOptimizer):

    def set_weights(self, weights: Dict[str, Dict[str, float]]):
        # the weights dictionary has the replica ids as keys - not the pod ips!

        replica_service: KubernetesFunctionReplicaService = self.context.replica_service
        ip_weights = defaultdict(dict)
        logger.info(weights)
        for fn, fn_weights in weights.items():
            for replica_id, weight in fn_weights.items():
                replica = replica_service.get_function_replica_by_id(replica_id)
                if replica.state != FunctionReplicaState.RUNNING:
                    # it can happen that replicas are shutdown in the meantime
                    continue
                else:
                    ip_weights[fn][replica.ip] = int(weight)
        client = EtcdClient.from_env()
        logger.info(ip_weights)
        for fn, fn_weights in ip_weights.items():
            etcd_weights = {"ips": [], "weights": []}
            for ip, weight in fn_weights.items():
                etcd_weights["ips"].append(f'{ip}:8080')
                etcd_weights["weights"].append(int(weight))
            key = f'golb/function/{self.cluster}/{fn}'
            value = json.dumps(etcd_weights)
            logger.info(f'Set following in etcd {key} - {value}')
            client.write(key=key, value=value)


class GlobalK8sLoadBalancer(GlobalWrrOptimizer):

    def __init__(self, context: PlatformContext, metrics: Metrics, weight_calculator: WeightCalculator,
                 cluster: str) -> None:
        super().__init__(context, metrics, weight_calculator)
        self.cluster = cluster

    def set_weights(self, weights: Dict[str, Dict[str, float]]):
        # the weights dictionary has the replica ids as keys - not the pod ips!

        replica_service: KubernetesFunctionReplicaService = self.context.replica_service
        ip_weights = defaultdict(dict)
        for fn, fn_weights in weights.items():
            for replica_id, weight in fn_weights.items():
                replica = replica_service.get_function_replica_by_id(replica_id)
                if replica.state != FunctionReplicaState.RUNNING:
                    # it can happen that replicas are shutdown in the meantime
                    continue
                else:
                    ip_weights[fn][replica.ip] = int(weight)
        client = EtcdClient.from_env()

        for fn, fn_weights in ip_weights.items():
            etcd_weights = {"ips": [], "weights": []}
            for ip, weight in fn_weights.items():
                etcd_weights["ips"].append(f'{ip}:8080')
                etcd_weights["weights"].append(weight)
            key = f'golb/function/{self.cluster}/{fn}'
            value = json.dumps(etcd_weights)
            logger.info(f'Set following in etcd {key} - {value}')
            client.write(key=key, value=value)
