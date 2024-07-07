import multiprocessing
import signal
import time
from queue import Empty

from faasopts.loadbalancers.wrr.wrr import SmoothLrtWeightCalculator, RoundRobinWeightCalculator
from galileo.shell.shell import init
from galileo.worker.context import Context
from galileofaas.context.model import GalileoFaasContext
from galileofaas.system.core import GalileoFaasMetrics

from evaluation.cosimulation.setup import setup_galileo_faas
from evaluation.cosimulation.orchestration.loadbalancer.loadbalancer import K8sLoadBalancer, GlobalK8sLoadBalancer

received_signal = False


def signal_handler(signal, frame):
    print('Signal received!')
    global received_signal
    received_signal = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class SmoothLrtLbDaemon:
    def __init__(self, zone: str, reconcile_interval: float, scheduler_name: str, stop_queue: multiprocessing.Queue):
        self.zone = zone
        self.reconcile_interval = reconcile_interval
        self.scheduler_name = scheduler_name
        self.stop_queue = stop_queue

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
            load_balancer = self.setup_load_balancer(ctx, metrics)
            while not received_signal:
                time.sleep(self.reconcile_interval)
                try:
                    self.stop_queue.get_nowait()
                    # if get_nowait returns we know someone put something into the queue
                    break
                except Empty:
                    # otherwise we continue updating
                    load_balancer.update()
        finally:
            if daemon is not None:
                daemon.stop(1)
            if g is not None:
                g['telemd'].stop_telemd()

    def setup_load_balancer(self, ctx: GalileoFaasContext,
                            metrics: GalileoFaasMetrics) -> K8sLoadBalancer:
        smooth_weight_calculator = SmoothLrtWeightCalculator(ctx, lambda: time.time(), 1)
        load_balancer = K8sLoadBalancer(ctx, self.zone, metrics, smooth_weight_calculator)
        return load_balancer

class RrLbDaemon:
    def __init__(self, zone: str, reconcile_interval: float, scheduler_name: str, stop_queue: multiprocessing.Queue):
        self.zone = zone
        self.reconcile_interval = reconcile_interval
        self.scheduler_name = scheduler_name
        self.stop_queue = stop_queue

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
            load_balancer = self.setup_load_balancer(ctx, metrics)
            while not received_signal:
                time.sleep(self.reconcile_interval)
                try:
                    self.stop_queue.get_nowait()
                    # if get_nowait returns we know someone put something into the queue
                    break
                except Empty:
                    # otherwise we continue updating
                    load_balancer.update()
        finally:
            if daemon is not None:
                daemon.stop(1)
            if g is not None:
                g['telemd'].stop_telemd()

    def setup_load_balancer(self, ctx: GalileoFaasContext,
                            metrics: GalileoFaasMetrics) -> K8sLoadBalancer:
        rr_weight_calculator = RoundRobinWeightCalculator()
        load_balancer = K8sLoadBalancer(ctx, self.zone, metrics, rr_weight_calculator)
        return load_balancer


class GlobalLbDaemon:
    """
    zone should always be cloud zone, as this is where the load balancer will be deployed in the centralized
    scenarios
    """

    def __init__(self, zone: str, reconcile_interval: float, scheduler_name: str, stop_queue: multiprocessing.Queue):
        self.zone = zone
        self.reconcile_interval = reconcile_interval
        self.scheduler_name = scheduler_name
        self.stop_queue = stop_queue

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
            load_balancer = self.setup_load_balancer(ctx, metrics)
            while not received_signal:
                time.sleep(self.reconcile_interval)
                try:
                    self.stop_queue.get_nowait()
                    # if get_nowait returns we know someone put something into the queue
                    break
                except Empty:
                    # otherwise we continue updating
                    load_balancer.update()
        finally:
            if daemon is not None:
                daemon.stop(1)
            if g is not None:
                g['telemd'].stop_telemd()

    def setup_load_balancer(self, ctx: GalileoFaasContext,
                            metrics: GalileoFaasMetrics) -> GlobalK8sLoadBalancer:
        smooth_weight_calculator = SmoothLrtWeightCalculator(ctx, lambda: time.time(), 1)
        load_balancer = GlobalK8sLoadBalancer(ctx, metrics, smooth_weight_calculator, self.zone)
        return load_balancer
