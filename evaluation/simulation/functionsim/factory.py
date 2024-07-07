import logging
from typing import Callable, Any

from faas.system import FunctionContainer
from sim.core import Environment
from sim.faas import FunctionSimulator, SimulatorFactory
from sim.faas.loadbalancers import ForwardingClientSimulator, LoadBalancerSimulator

from evaluation.simulation.functionsim.inference import InferenceFunctionSim
from evaluation.simulation.functionsim.training import TrainingFunctionSim

logger = logging.getLogger(__name__)

class ResnetFunctionSimulatorFactory(SimulatorFactory):

    def __init__(self, load_balancer_factory: Callable[[Environment, FunctionContainer], FunctionSimulator]):
        self.load_balancer_factory = load_balancer_factory

    def create(self, env: Environment, fn: FunctionContainer) -> FunctionSimulator:
        if 'inference' in fn.fn_image.image:
            return InferenceFunctionSim(4)
        elif 'training' in fn.fn_image.image:
            return TrainingFunctionSim()
        elif 'galileo-worker' in fn.fn_image.image:
            return ForwardingClientSimulator()
        elif 'load-balancer' == fn.fn_image.image:
            return self.load_balancer_factory(env, fn)