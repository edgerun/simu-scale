import logging
import os
from typing import List, Dict

from faas.system import ScalingConfiguration
from sim.benchmark import BenchmarkBase
from sim.core import Environment
from sim.docker import ImageProperties
from sim.requestgen import SimpleFunctionRequestFactory
from sim.topology import Topology
from sim.util.client import find_clients
from sim.util.loadbalancer import find_lbs

from evaluation.cosimulation.simulation.deployment.cosim import prepare_cosim_deployment
from evaluation.cosimulation.simulation.scenario.client import get_galileo_worker_image_properties, \
    prepare_client_deployments_for_experiment
from evaluation.simulation.deployment.loadbalancer import get_go_load_balancer_image_props, \
    prepare_load_balancer_deployments

logger = logging.getLogger(__name__)


def single_request():
    yield 0.00001


class CoSimBenchmark(BenchmarkBase):

    def __init__(self, topology: Topology, scenario: Dict[str, Dict[str, List[str]]], payload_size: float,
                 application: str, target_cluster: str, application_image_properties: List[ImageProperties],
                 duration: int = None,
                 start_time: float = None, init_deploy=True, clients: List[str] = None):
        self.balancer = 'load-balancer'
        self.application = application
        self.application_image_properties = application_image_properties
        if clients:
            self.clients = clients
        else:
            self.clients = [x.name for x in find_clients(topology)]
        self.target_cluster = target_cluster
        self.payload_size = payload_size
        self.load_balancer_hosts = find_lbs(topology)
        self.scenario = scenario
        self.start_time = start_time
        self.duration = duration

        images = self.prepare_images(application)
        deployments = self.prepare_deployments()
        application_client = f'client-{application}'
        fn_request_factories = self.create_request_factories(application_client)
        arrival_profiles = self.create_arrival_profiles(application_client)
        self.metadata = {'benchmark': f'testbed-{application}-benchmark'}
        self.init_deploy = init_deploy
        super().__init__(images, deployments, arrival_profiles, fn_request_factories, duration)

    def create_request_factories(self, application_client):
        fn_request_factories = {
            application_client: SimpleFunctionRequestFactory(size=self.payload_size)
        }
        return fn_request_factories

    def create_arrival_profiles(self, application_client):
        arrival_profiles = {
            application_client: single_request()
        }
        return arrival_profiles

    def prepare_images(self, application):
        images = []

        galileo_worker_image_properties = get_galileo_worker_image_properties()
        lb_image_properties = get_go_load_balancer_image_props('edgerun/go-load-balancer')
        images.extend(self.application_image_properties)
        images.extend(galileo_worker_image_properties)
        images.extend(lb_image_properties)
        return images

    def prepare_deployments(self):
        deployments = []
        max_scale = int(os.getenv('FUNCTION_MAX_SCALE', '30'))
        inference_scaling_config = ScalingConfiguration(scale_min=1, scale_max=max_scale)

        function_deployments = [prepare_cosim_deployment(
            scaling_config=inference_scaling_config, function=self.application,
            img_properties=self.application_image_properties, target_cluster=self.target_cluster)]
        load_balancer_deployment = prepare_load_balancer_deployments('edgerun/go-load-balancer:0.1.10',
                                                                     self.load_balancer_hosts)
        client_deployments = prepare_client_deployments_for_experiment(self.clients, load_balancer_deployment,
                                                                       function_deployments, self.payload_size,
                                                                       self.scenario,
                                                                       self.start_time, self.duration)
        self.metadata['function_deployments'] = [f.name for f in function_deployments]
        self.metadata['client_deployments'] = [f.name for f in client_deployments]
        self.metadata['load_balancer_deployments'] = [f.name for f in load_balancer_deployment]
        deployments.extend(load_balancer_deployment)
        deployments.extend(client_deployments)
        deployments.extend(function_deployments)
        return deployments

    def run(self, env: Environment):
        deployments = self.prepare_deployments()

        if self.init_deploy:
            for deployment in deployments:
                yield from env.faas.deploy(deployment)
        else:
            for fd in deployments:
                env.metrics.log_function_definition(fd)
                env.metrics.log_function_deployment(fd)
                env.metrics.log_function_image_definitions(fd)
                env.metrics.log_function_container_definitions(fd)
                env.metrics.log_function_deployment_lifecycle(fd, 'deploy')
        # block until replicas become available (scheduling has finished and replicas have been deployed on the node)
        logger.info('waiting for replicas')
        for deployment in deployments:
            yield env.process(env.faas.poll_available_replica(deployment.name))

        # wait for lb updater process to trigger update
        yield env.timeout(2)
        # run workload
        ps = []
        request_factory = SimpleFunctionRequestFactory()
        for deployment in deployments:
            if 'galileo-worker' in deployment.name or 'client' in deployment.name:
                ps.append(env.process(env.faas.invoke(request_factory.generate(env, deployment))))

        if self.duration is not None:
            env.process(self.wait(env, ps))

        # wait for invocation processes to finish
        for p in ps:
            yield p
