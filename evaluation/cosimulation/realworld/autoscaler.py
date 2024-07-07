import logging
import multiprocessing
import signal
import threading
import time
from queue import Empty
from typing import Any, Callable

from faas.system import FaasSystem
from faas.system.autoscaler.core import ReconciledBaseAutoscalerConfiguration
from faasopts.autoscalers.api import BaseAutoscaler
from faasopts.autoscalers.base.hpa.decentralized.latency import HorizontalLatencyPodAutoscalerParameters
from faasopts.autoscalers.k8s.hpa.central.latency import HorizontalLatencyPodAutoscaler
from faasopts.autoscalers.k8s.hpa.decentralized.latency import K8sDecentralizedHorizontalLatencyPodAutoscaler
from faasopts.autoscalers.k8s.hpa.distributed.latency import DistributedHorizontalLatencyPodAutoscaler
from faasopts.autoscalers.k8s.pressure.autoscaler import K8sPressureAutoscaler
from faasopts.utils.pressure.api import PressureAutoscalerParameters
from faasopts.utils.pressure.service import K8sPressureService
from galileo.shell.shell import init
from galileo.worker.context import Context
from galileofaas.connections import RedisClient
from galileofaas.context.model import GalileoFaasContext
from galileofaas.context.platform.replica.factory import KubernetesFunctionReplicaFactory
from galileofaas.system.core import GalileoFaasMetrics
from galileofaas.util.pubsub import POISON

from evaluation.cosimulation.orchestration.model import HpaAutoscalerConfiguration, PressureAutoscalerConfiguration
from evaluation.cosimulation.setup import setup_galileo_faas
from evaluation.cosimulation.util.constants import HPA_AUTOSCALER, PRESSURE_AUTOSCALER

logger = logging.getLogger(__name__)

received_signal = False


def signal_handler(signal, frame):
    print('Signal received!')
    global received_signal
    received_signal = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class CentralizedAutoscalerDaemon:
    def __init__(self, cluster: str, reconcile_interval: float, scheduler_name: str, target_duration: float,
                 percentile_duration: float,
                 lookback: int,
                 stop_queue: multiprocessing.Queue):
        self.cluster = cluster
        self.reconcile_interval = reconcile_interval
        self.stop_queue = stop_queue
        self.scheduler_name = scheduler_name
        self.target_duration = target_duration
        self.percentile_duration = percentile_duration
        self.lookback = lookback

    def run(self):
        ctx = Context()
        rds = ctx.create_redis()
        g = None
        daemon = None
        try:
            daemon, faas_system, metrics = setup_galileo_faas(self.scheduler_name)
            daemon.start()

            g = init(rds)
            g['telemd'].start_telemd()

            ctx = daemon.context
            autoscaler = self.setup_autoscaler(ctx, faas_system, metrics)
            print(f'Start autoscaling for {self.cluster}, and {self.scheduler_name}')
            while not received_signal:
                time.sleep(self.reconcile_interval)
                try:
                    self.stop_queue.get_nowait()
                    # if get_nowait returns we know someone put something into the queue
                    return
                except Empty:
                    # otherwise we continue updating
                    start_ts = time.time()
                    autoscaler.run()
                    end_ts = time.time()
                    metrics.log('scaling-duration', end_ts - start_ts)
        finally:
            if daemon is not None:
                daemon.stop(1)
            if g is not None:
                g['telemd'].stop_telemd()

    def setup_autoscaler(self, ctx: GalileoFaasContext, faas_system: FaasSystem,
                         metrics: GalileoFaasMetrics) -> HorizontalLatencyPodAutoscaler:
        scaler_parameters = {
            # function name: HorizontalLatencyPodAutoscalerParameters
            # 'mobilenet': HorizontalLatencyPodAutoscalerParameters(lookback=10, target_time_measure='rtt',
            #                                                       target_duration=self.target_duration),
            # 'busy': HorizontalLatencyPodAutoscalerParameters(lookback=10, target_time_measure='rtt',
            #                                                  target_duration=self.target_duration),
            'pythonpi': HorizontalLatencyPodAutoscalerParameters(lookback=self.lookback, target_time_measure='rtt',
                                                                 target_duration=self.target_duration,
                                                                 percentile_duration=self.percentile_duration),
        }
        return HorizontalLatencyPodAutoscaler(scaler_parameters, ctx, faas_system, metrics, lambda: time.time(),
                                              cluster=self.cluster, replica_factory=ctx.replica_factory)


class DecentralizedAutoscalerUpdateProcess:

    def __init__(self, autoscaler: BaseAutoscaler, zone: str, parameter_parser: Callable[[str], Any]):
        self.autoscaler = autoscaler
        self.zone = zone
        self.rds = None
        self.parameter_parser = parameter_parser
        self.channel = 'galileo/events'

    def run(self) -> None:
        logger.info('Start update process')
        self.rds = RedisClient.from_env()
        try:
            for event in self.rds.sub(self.channel):
                if event['data'] == POISON:
                    break
                msg = event['data']
                split = msg.split(' ', maxsplit=2)
                event = split[1]
                logger.debug(f'Updater got event: {event}')
                if event.startswith(f'autoscaler-{self.zone}-parameters/'):
                    logger.info(f'Autoscaler got update in zone {self.zone}: {split[1]}')
                    params = self.parameter_parser(split[1])
                    self.autoscaler.update(params)
        except Exception as e:
            logger.exception(e)
        finally:
            # self.stop()
            # logger.info(f'update process autoscaler-{self.autoscaler.cluster} shutdown')
            pass

    def stop(self):
        if self.rds is not None:
            self.rds.conn().close()


class ReconciledDecentralizedAutoscalerDaemon:
    def __init__(self, zone: str, global_scheduler_name: str,
                 autoscaler_config: ReconciledBaseAutoscalerConfiguration,
                 stop_queue: multiprocessing.Queue):
        self.zone = zone
        self.stop_queue = stop_queue
        self.reconcile_interval = autoscaler_config.reconcile_interval
        self.scheduler_name = global_scheduler_name
        self.autoscaler_config = autoscaler_config

    def parameter_parser(self) -> Callable[[str], Any]:
        def parse(parameters: str) -> Any:
            if self.autoscaler_config.autoscaler_type == HPA_AUTOSCALER:
                return HorizontalLatencyPodAutoscalerParameters.from_json(parameters)
            elif self.autoscaler_config.autoscaler_type == PRESSURE_AUTOSCALER:
                return PressureAutoscalerParameters.from_json(parameters)

        return parse

    def run(self):
        ctx = Context()
        rds = ctx.create_redis()
        g = None
        daemon = None
        update_thread: threading.Thread = None
        update_process = None
        try:
            daemon, faas_system, metrics = setup_galileo_faas(self.scheduler_name)
            daemon.start()

            g = init(rds)
            g['telemd'].start_telemd()

            ctx = daemon.context
            autoscaler = self.setup_autoscaler(ctx, faas_system, metrics, self.autoscaler_config)
            update_process = DecentralizedAutoscalerUpdateProcess(autoscaler, self.zone, self.parameter_parser())

            def run_update_autoscaler():
                update_process.run()

            update_thread = threading.Thread(target=run_update_autoscaler)
            update_thread.start()

            print(f'Start autoscaling for {self.zone}, and {self.scheduler_name}')
            while not received_signal:
                time.sleep(self.reconcile_interval)
                try:
                    self.stop_queue.get_nowait()
                    # if get_nowait returns we know someone put something into the queue
                    return
                except Empty:
                    # otherwise we continue updating
                    start_ts = time.time()
                    logger.info(ctx.deployment_service.get_deployments())
                    logger.info(autoscaler)
                    autoscaler.run()
                    logger.info(f'Ran autoscaler')
                    end_ts = time.time()
                    metrics.log('scaling-duration', end_ts - start_ts)
        finally:
            if update_thread is not None:
                if update_process is not None:
                    update_process.stop()
                update_thread.join(timeout=5)
            if daemon is not None:
                daemon.stop(1)
            if g is not None:
                g['telemd'].stop_telemd()

    def setup_autoscaler(self, ctx: GalileoFaasContext, faas_system: FaasSystem,
                         metrics: GalileoFaasMetrics,
                         config: ReconciledBaseAutoscalerConfiguration) -> BaseAutoscaler:

        if config.autoscaler_type == HPA_AUTOSCALER:
            config: HpaAutoscalerConfiguration = config
            scaler_parameters = config.zone_parameters[self.zone]
            return K8sDecentralizedHorizontalLatencyPodAutoscaler(scaler_parameters, ctx, faas_system, metrics,
                                                                  lambda: time.time(),
                                                                  cluster=self.zone,
                                                                  replica_factory=ctx.replica_factory)
        elif config.autoscaler_type == PRESSURE_AUTOSCALER:
            config: PressureAutoscalerConfiguration = config
            scaler_parameters = config.zone_parameters[self.zone]
            # TODO write wrapper that publishes calculated pressure values with K8sPressureService
            return K8sPressureAutoscaler(ctx, scaler_parameters, self.zone, KubernetesFunctionReplicaFactory(),
                                         lambda: time.time(), metrics, K8sPressureService(metrics, ctx.rds))


class DistributedAutoscalerDaemon:
    def __init__(self, cluster: str, reconcile_interval: float, scheduler_name: str, target_duration: float,
                 percentile_duration: float,
                 lookback: int,
                 stop_queue: multiprocessing.Queue):
        self.cluster = cluster
        self.reconcile_interval = reconcile_interval
        self.stop_queue = stop_queue
        self.scheduler_name = scheduler_name
        self.target_duration = target_duration
        self.percentile_duration = percentile_duration
        self.lookback = lookback

    def run(self):
        ctx = Context()
        rds = ctx.create_redis()
        g = None
        daemon = None
        try:
            daemon, faas_system, metrics = setup_galileo_faas(self.scheduler_name)
            daemon.start()

            g = init(rds)
            g['telemd'].start_telemd()

            ctx = daemon.context
            autoscaler = self.setup_autoscaler(ctx, faas_system, metrics)
            print(f'Start autoscaling for {self.cluster}, and {self.scheduler_name}')
            while not received_signal:
                time.sleep(self.reconcile_interval)
                try:
                    self.stop_queue.get_nowait()
                    # if get_nowait returns we know someone put something into the queue
                    return
                except Empty:
                    # otherwise we continue updating
                    start_ts = time.time()
                    autoscaler.run()
                    end_ts = time.time()
                    metrics.log('scaling-duration', end_ts - start_ts)
        finally:
            if daemon is not None:
                daemon.stop(1)
            if g is not None:
                g['telemd'].stop_telemd()

    def setup_autoscaler(self, ctx: GalileoFaasContext, faas_system: FaasSystem,
                         metrics: GalileoFaasMetrics) -> DistributedHorizontalLatencyPodAutoscaler:
        scaler_parameters = {
            # function name: HorizontalLatencyPodAutoscalerParameters
            # 'mobilenet': HorizontalLatencyPodAutoscalerParameters(lookback=10, target_time_measure='rtt',
            #                                                       target_duration=self.target_duration),
            # 'busy': HorizontalLatencyPodAutoscalerParameters(lookback=10, target_time_measure='rtt',
            #                                                  target_duration=self.target_duration),
            'pythonpi': HorizontalLatencyPodAutoscalerParameters(lookback=self.lookback, target_time_measure='rtt',
                                                                 target_duration=self.target_duration,
                                                                 percentile_duration=self.percentile_duration),
        }
        return DistributedHorizontalLatencyPodAutoscaler(scaler_parameters, ctx, faas_system, metrics,
                                                         lambda: time.time(),
                                                         cluster=self.cluster, replica_factory=ctx.replica_factory)


def main2():
    stop_queue = multiprocessing.Queue()
    try:
        zones = ['zone-a', 'zone-b', 'zone-c']
        for zone in zones:
            autoscaler = CentralizedAutoscalerDaemon(zone, 5, 'global-scheduler', 10, stop_queue)
            t1 = threading.Thread(target=autoscaler.run)
            t1.start()
        input()
    finally:
        stop_queue.put('__POISON__')


def main():
    stop_queue = multiprocessing.Queue()
    try:
        autoscaler = CentralizedAutoscalerDaemon(None, 5, 'scheduler-zone-c', 0.8, stop_queue)
        autoscaler.run()
    finally:
        stop_queue.put('__POISON__')


if __name__ == '__main__':
    logging.basicConfig(level=logging._nameToLevel['INFO'])

    main()
