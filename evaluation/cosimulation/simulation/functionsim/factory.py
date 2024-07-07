from typing import Callable

from faas.system import FunctionContainer
from sim.core import Environment
from sim.faas import SimulatorFactory, FunctionSimulator
from sim.faas.loadbalancers import ForwardingClientSimulator
from sim.oracle.oracle import FetOracle, ResourceOracle

from evaluation.simulation.functionsim.inference import InferenceFunctionSim


class CoSimFunctionSimulatorFactory(SimulatorFactory):

    def __init__(self, load_balancer_factory: Callable[[Environment, FunctionContainer], FunctionSimulator],
                 fet_oracle: FetOracle, resource_oracle: ResourceOracle, application):
        self.load_balancer_factory = load_balancer_factory
        self.application = application
        self.fet_oracle = fet_oracle
        self.resource_oracle = resource_oracle

    def create(self, env: Environment, fn: FunctionContainer) -> FunctionSimulator:
        if f'edgerun/{self.application}' in fn.fn_image.image:
            return InferenceFunctionSim(self.fet_oracle, self.resource_oracle, workers=4)
        elif 'galileo' in fn.fn_image.image:
            return ForwardingClientSimulator()
        elif 'load-balancer' in fn.fn_image.image:
            return self.load_balancer_factory(env, fn)
