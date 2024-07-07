import logging
import os
import time
from typing import Dict

from faas.system import FaasSystem
from faas.system.scheduling.decentralized import GlobalScheduler, BaseGlobalSchedulerConfiguration, \
    BaseLocalSchedulerConfiguration
from faasopts.schedulers.decentralized.balanced.localscheduler import LocalBalancedScheduler
from faasopts.schedulers.decentralized.globalk8s import K8sGlobalScheduler
from faasopts.schedulers.decentralized.globalscheduler import CpuGlobalScheduler
from faasopts.schedulers.decentralized.locality.globalscheduler import LocalityGlobalScheduler
from faasopts.schedulers.decentralized.localk8s import K8sLocalScheduler
from faasopts.schedulers.decentralized.localscheduler import LocalCpuScheduler
from faasopts.schedulers.decentralized.pressure.globalscheduler import PressureGlobalScheduler
from galileo.shell.shell import init
from galileo.worker.context import Context
from galileofaas.context.model import GalileoFaasContext
from galileofaas.context.platform.replica.factory import KubernetesFunctionReplicaFactory
from galileofaas.system.core import GalileoFaasMetrics

from evaluation.cosimulation.util.constants import BALANCED_LOCAL_SCHEDULER, PRESSURE_GLOBAL_SCHEDULER, \
    LOCALITY_GLOBAL_SCHEDULER, CPU_GLOBAL_SCHEDULER, CPU_LOCAL_SCHEDULER

logger = logging.getLogger(__name__)
from evaluation.cosimulation.setup import setup_galileo_faas


class GlobalSchedulerDaemon:
    def __init__(self, scheduler_config: BaseGlobalSchedulerConfiguration, storage_local_schedulers, max_scale):
        self.scheduler_config = scheduler_config
        self.max_scale = max_scale
        self.storage_local_schedulers = storage_local_schedulers

    def setup_global_scheduler(self, ctx: GalileoFaasContext, faas_system: FaasSystem, metrics: GalileoFaasMetrics):
        config = self.scheduler_config
        if config.scheduler_type == PRESSURE_GLOBAL_SCHEDULER:
            default_config = BaseGlobalSchedulerConfiguration(config.scheduler_name, LOCALITY_GLOBAL_SCHEDULER, config.delay)
            default_global_scheduler = LocalityGlobalScheduler(default_config, self.storage_local_schedulers, ctx, metrics)

            pressure_scheduler = PressureGlobalScheduler(config, self.storage_local_schedulers, ctx, metrics, lambda: time.time(), KubernetesFunctionReplicaFactory(), default_global_scheduler)
            return pressure_scheduler
        elif config.scheduler_type == LOCALITY_GLOBAL_SCHEDULER:
            locality_scheduler = LocalityGlobalScheduler(config, self.storage_local_schedulers, ctx, metrics)
            return locality_scheduler
        elif config.scheduler_type == CPU_GLOBAL_SCHEDULER:
            cpu_scheduler = CpuGlobalScheduler(self.scheduler_config.scheduler_name, self.storage_local_schedulers, ctx, metrics, self.scheduler_config.delay, self.max_scale)
            return cpu_scheduler
        else:
            raise ValueError("Unknown scheduler type: {}".format(config.scheduler_type))

    def run(self):
        ctx = Context()
        rds = ctx.create_redis()
        g = None
        daemon = None
        try:
            daemon, faas_system, metrics = setup_galileo_faas(self.scheduler_config.scheduler_name)
            daemon.start()

            g = init(rds)
            g['telemd'].start_telemd()

            ctx = daemon.context
            global_scheduler = self.setup_global_scheduler(ctx, faas_system, metrics)

            scheduler = K8sGlobalScheduler(self.scheduler_config.scheduler_name, self.storage_local_schedulers, ctx, faas_system,
                                           global_scheduler, metrics, self.scheduler_config.delay, self.max_scale)
            scheduler.start_schedule()
        except Exception as e:
            logger.exception(e)
        finally:
            if daemon is not None:
                daemon.stop(1)
            if g is not None:
                g['telemd'].stop_telemd()


class LocalSchedulerDaemon:
    def __init__(self, scheduler_config: BaseLocalSchedulerConfiguration):
        self.scheduler_config = scheduler_config

    def run(self):
        ctx = Context()
        rds = ctx.create_redis()
        g = None
        daemon = None

        try:
            daemon, faas_system, metrics = setup_galileo_faas(self.scheduler_config.scheduler_name)
            daemon.start()

            g = init(rds)
            g['telemd'].start_telemd()

            ctx = daemon.context

            local_cpu_scheduler = self.create_local_scheduler(ctx, metrics)
            scheduler = K8sLocalScheduler(self.scheduler_config.scheduler_name, self.scheduler_config.zone, ctx, metrics, self.scheduler_config.delay,
                                          local_cpu_scheduler, self.scheduler_config.global_scheduler_name)
            scheduler.start_schedule()
        finally:
            if daemon is not None:
                daemon.stop(1)
            if g is not None:
                g['telemd'].stop_telemd()

    def create_local_scheduler(self, ctx, metrics):
        if self.scheduler_config.scheduler_type == CPU_LOCAL_SCHEDULER:
            return LocalCpuScheduler(self.scheduler_config, ctx, metrics, self.scheduler_config.delay)
        elif self.scheduler_config.scheduler_type == BALANCED_LOCAL_SCHEDULER:
            return LocalBalancedScheduler(self.scheduler_config, ctx, metrics, self.scheduler_config.delay)
