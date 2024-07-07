from typing import Generator, List

from ext.raith21.oracles import extract_model_type
from faas.system.core import FunctionRequest

from sim import docker
from sim.core import Environment
from sim.faas import HTTPWatchdog, SimFunctionReplica, simulate_data_download, FunctionCharacterization
from sim.faas.watchdogs import WatchdogResponse
from sim.oracle.oracle import FetOracle, ResourceOracle


class InferenceFunctionSim(HTTPWatchdog):

    def __init__(self, fet_oracle: FetOracle, resource_oracle: ResourceOracle, workers: int):
        super().__init__(workers)
        self.fet_oracle = fet_oracle
        self.resource_oracle = resource_oracle
        self.resources = {}

    def deploy(self, env: Environment, replica: SimFunctionReplica):
        # simulate a docker pull command for deploying the function (also done by sim.faassim.DockerDeploySimMixin)
        yield from docker.pull(env, replica.container.image, replica.node.ether_node)

    def setup(self, env: Environment, replica: SimFunctionReplica):
        super().setup(env, replica)
        yield env.timeout(0)

    def teardown(self, env: Environment, replica: SimFunctionReplica):
        yield from super(InferenceFunctionSim, self).teardown(env, replica)
        for resource_index in self.resources:
            env.resource_state.remove_resource(replica, resource_index)
        yield env.timeout(0)

    def claim_resources(self, env: Environment, replica: SimFunctionReplica, request: FunctionRequest):
        resource_characterization = self.resource_oracle.get_resources(replica.node.name, replica.image)

        cpu = resource_characterization.cpu
        gpu = resource_characterization.gpu
        blkio = resource_characterization.blkio
        net = resource_characterization.net
        ram = resource_characterization.ram

        self.resources[request.request_id] = {'cpu': cpu, 'gpu': gpu, 'blkio': blkio, 'net': net, 'ram': ram}

        cpu = env.resource_state.put_resource(replica, 'cpu', cpu)
        gpu = env.resource_state.put_resource(replica, 'gpu', gpu)
        blkio = env.resource_state.put_resource(replica, 'blkio', blkio)
        net = env.resource_state.put_resource(replica, 'net', net)
        ram = env.resource_state.put_resource(replica, 'ram', ram)
        yield env.timeout(0)
        return [cpu, gpu, blkio, net, ram]

    def release_resources(self, env: Environment, replica: SimFunctionReplica, resource_indices: List[int]):
        for resource_index in resource_indices:
            env.resource_state.remove_resource(replica, resource_index)
        yield env.timeout(0)

    def execute(self, env: Environment, replica: SimFunctionReplica, request: FunctionRequest) -> Generator[
        None, None, WatchdogResponse]:
        sample = self.fet_oracle.sample(replica.node.name, replica.image) * 2
        yield env.timeout(sample)
        return WatchdogResponse(
            '',
            200,
            150
        )
