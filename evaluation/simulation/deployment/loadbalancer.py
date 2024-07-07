import logging
from typing import List

from ether.util import parse_size_string
from faas.system import DeploymentRanking, ScalingConfiguration
from faas.system import FunctionContainer, FunctionImage, Function
from faas.util.constant import controller_role_label, hostname_label, zone_label, function_label, \
    api_gateway_type_label, pod_type_label
from sim.context.platform.deployment.model import SimFunctionDeployment, SimScalingConfiguration
from sim.docker import ImageProperties
from sim.faas.core import SimResourceConfiguration, Node
from sim.faas.loadbalancers import LoadBalancerFunctionContainer

logger = logging.getLogger(__name__)


def create_load_balancer_deployment(lb_id: str, type: str, host: str, cluster: str):
    lb_fn_name = lb_id + "-" + host
    lb_image_name = type
    lb_image = FunctionImage(image=type)
    lb_fn = Function(lb_fn_name, fn_images=[lb_image],
                     labels={function_label: api_gateway_type_label, controller_role_label: 'true',
                             zone_label: cluster})

    fn_container = FunctionContainer(lb_image, SimResourceConfiguration(),
                                     {function_label: api_gateway_type_label, controller_role_label: 'true',
                                      hostname_label: host, zone_label: cluster, 'origin_zone': cluster,
                                      pod_type_label: api_gateway_type_label,
                                      'schedulerName': f'local-scheduler-{cluster}'})

    lb_container = LoadBalancerFunctionContainer(fn_container)

    lb_fd = SimFunctionDeployment(
        lb_fn,
        [lb_container],
        SimScalingConfiguration(ScalingConfiguration(scale_min=1, scale_max=1, scale_zero=False)),
        DeploymentRanking([fn_container])
    )

    return lb_fd


def get_go_load_balancer_image_props(name: str) -> List[ImageProperties]:
    return [
        # image size from go-load-balancer
        ImageProperties(name, parse_size_string('10M'), tag='0.1.10', arch='arm32v7'),
        ImageProperties(name, parse_size_string('10M'), tag='0.1.10', arch='x86'),
        ImageProperties(name, parse_size_string('10M'), tag='0.1.10', arch='arm64v8'),
    ]


def prepare_load_balancer_deployments(type: str, hosts: List[Node]) -> List[SimFunctionDeployment]:
    def create_id(i: int):
        return f'load-balancer'

    lbs = []
    for idx, host in enumerate(hosts):
        lb_id = create_id(idx)
        lbs.append(create_load_balancer_deployment(lb_id, type, host.name, host.labels[zone_label]))

    return lbs
