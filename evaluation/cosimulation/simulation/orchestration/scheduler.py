from faas.context import PlatformContext
from faas.system import Metrics
from faas.system.scheduling.decentralized import BaseLocalSchedulerConfiguration
from faas.util.constant import zone_label
from faasopts.schedulers.decentralized.balanced.localscheduler import LocalBalancedScheduler
from faasopts.schedulers.decentralized.localscheduler import LocalCpuScheduler
from skippy.core.clustercontext import ClusterContext
from skippy.core.model import Pod, Node
from skippy.core.predicates import Predicate

from evaluation.cosimulation.util.constants import BALANCED_LOCAL_SCHEDULER, CPU_LOCAL_SCHEDULER


class PodZoneEqualsCluster(Predicate):

    def passes_predicate(self, context: ClusterContext, pod: Pod, node: Node) -> bool:
        cluster = pod.spec.labels.get(zone_label)
        if cluster is None:
            return True
        else:
            return node.labels[zone_label] == cluster


def create_scheduler(local_scheduler_config: BaseLocalSchedulerConfiguration,  cluster: str, ctx: PlatformContext, metrics: Metrics):
    # scheduler = Scheduler(env.cluster)
    # scheduler.predicates.append(PodHostEqualsNode())
    # scheduler.predicates.append(PodZoneEqualsCluster())
    # return scheduler
    if local_scheduler_config.scheduler_type == BALANCED_LOCAL_SCHEDULER:
        return LocalBalancedScheduler(local_scheduler_config, ctx, metrics)
    elif local_scheduler_config.scheduler_type == CPU_LOCAL_SCHEDULER:
        return LocalCpuScheduler(local_scheduler_config, ctx, metrics)
    else:
        raise AttributeError('Unknown scheduler type: {}'.format(local_scheduler_config.scheduler_type))
