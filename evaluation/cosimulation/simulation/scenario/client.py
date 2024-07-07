import logging
from pathlib import Path
from typing import List, Dict

from ether.util import parse_size_string
from faas.system import FunctionContainer, FunctionImage, Function, \
    ScalingConfiguration
from faas.util.constant import client_role_label, hostname_label, zone_label
from requestgen import pre_recorded_profile
from sim.context.platform.deployment.model import SimFunctionDeployment, SimScalingConfiguration, DeploymentRanking
from sim.docker import ImageProperties
from sim.faas.core import SimResourceConfiguration
from sim.faas.loadbalancers import ForwardingClientFunctionContainer

logger = logging.getLogger(__name__)


def prepare_client_deployments_for_experiment(clients: List[str], lb_deployments: List[SimFunctionDeployment],
                                              deployments: List[SimFunctionDeployment], size,
                                              request_patterns: Dict[str, Dict[str, List[str]]],
                                              start_time: float, duration: int) -> List[SimFunctionDeployment]:
    fds = []
    logger.info(f"Preparing client deployments for experiment")
    logger.info(f"Request patterns: {request_patterns}")
    for zone, images in request_patterns.items():
        zone_client = None
        for client in clients:
            if zone in client:
                zone_client = client
        if zone_client is None:
            raise ValueError(f"No client found for {zone} in clients: {clients}")
        for image, request_files in images.items():
            for request_file in request_files:
                path = Path(f'{request_file}')
                if path.is_file():
                    ia_generator = prepare_workload_ia_generator(duration, request_file, start_time)
                    for lb_deployment in lb_deployments:
                        if lb_deployment.labels[zone_label] in zone_client:
                            client_inference_fd = prepare_client_deployment(
                                zone_client,
                                deployments[0],
                                lb_deployment,
                                ia_generator,
                                size
                            )
                            fds.extend([client_inference_fd])
                else:
                    raise ValueError(f"Request file {request_file} not found.")
    return fds


def prepare_workload_ia_generator(duration, request_file, start_time):
    ia_generator = pre_recorded_profile(f'{request_file}')
    now = 0
    while now < start_time:
        try:
            ia = next(ia_generator)
            now += ia
        except StopIteration:
            raise AttributeError(f"start_time argument exceeds duration of request pattern")
    ias = []
    while now < start_time + duration:
        try:
            ia = next(ia_generator)
            ias.append(ia)
            now += ia
        except StopIteration:
            pass

    def ia_gen(ias):
        for ia in ias:
            yield ia

    ia_generator = ia_gen(ias)
    return ia_generator


def prepare_client_deployment(host: str, deployment: SimFunctionDeployment, lb_deployment: SimFunctionDeployment,
                              ia_generator, size):
    # Design time
    zone = lb_deployment.labels["ether.edgerun.io/zone"]
    # client_fn_name = f'client-{host}-{zone}'
    client_image_name = 'edgerun/galileo:4b60c5d'
    client_image = FunctionImage(image=client_image_name)
    client_fn = Function(host, fn_images=[client_image])

    fn_container = FunctionContainer(client_image, SimResourceConfiguration(),
                                     {client_role_label: 'true', 'origin_zone': zone,
                                      hostname_label: host, 'schedulerName': f'local-scheduler-{zone}'})

    client_container = ForwardingClientFunctionContainer(
        fn_container,
        ia_generator,
        size,
        deployment,
        lb_deployment
    )

    client_fd = SimFunctionDeployment(
        client_fn,
        [client_container],
        SimScalingConfiguration(ScalingConfiguration(scale_min=1, scale_max=1, scale_zero=False)),
        DeploymentRanking([client_container])
    )
    return client_fd


def get_galileo_worker_image_properties():
    return [
        ImageProperties('edgerun/galileo', parse_size_string('23M'), tag='4b60c5d', arch='arm32'),
        ImageProperties('edgerun/galileo', parse_size_string('23M'), tag='4b60c5d', arch='x86'),
        ImageProperties('edgerun/galileo', parse_size_string('23M'), tag='4b60c5d', arch='aarch64')
    ]
