import logging
import os
from typing import List

from ether.util import parse_size_string
from faas.system import FunctionContainer, FunctionImage, Function, ScalingConfiguration
from faas.util.constant import worker_role_label, function_label, pod_type_label, \
    function_type_label, zone_label
from sim.context.platform.deployment.model import SimFunctionDeployment, SimScalingConfiguration
from sim.docker import ImageProperties
from sim.faas.core import SimResourceConfiguration
from skippy.core.model import ResourceRequirements

logger = logging.getLogger(__name__)


def prepare_cosim_deployment(scaling_config: ScalingConfiguration, function, img_properties: List[ImageProperties],
                             target_cluster: str) -> SimFunctionDeployment:
    application_label = f'{function}'
    application_img = img_properties[0].name
    tag = img_properties[0].tag
    container_image = f'{application_img}:{tag}'

    application_img = FunctionImage(image=container_image)
    application_fn = Function(application_label, fn_images=[application_img],
                              labels={function_label: application_label})
    memory = int(os.environ.get('MOBILENET_MEMORY', '250M').replace('M', ''))
    cpu = int(os.environ.get('MOBILENET_CPU', '500m').replace('m',''))

    resource_requests = {
        'cpu': cpu,
        'memory': memory * 1024 * 1024  # 250 MB
    }
    application_container = FunctionContainer(application_img,
                                              SimResourceConfiguration(
                                                  requests=ResourceRequirements(resource_requests)),
                                              {worker_role_label: "true", function_label: application_label,
                                               pod_type_label: function_type_label, zone_label: target_cluster,
                                               'origin_zone': target_cluster,
                                               'schedulerName': f'global-scheduler'})
                                               # 'schedulerName': f'local-scheduler-{target_cluster}'})

    application_fd = SimFunctionDeployment(
        application_fn,
        [application_container],
        SimScalingConfiguration(scaling_config)
    )

    return application_fd


def get_pi_image_properties():
    return [
        ImageProperties('edgerun/pythonpi', parse_size_string('25M'), arch='x86', tag='1.0.3'),
    ]
