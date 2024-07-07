import os
from typing import List

from ether.topology import Topology
from faas.system import FunctionImage, Function, FunctionContainer, FunctionDeployment, ScalingConfiguration, \
    DeploymentRanking
from faas.util.constant import function_label, zone_label, client_role_label, controller_role_label
from galileofaas.system.core import KubernetesResourceConfiguration, KubernetesFunctionDeployment
from skippy.core.model import ResourceRequirements


def generate_mobilenet_deployment():
    function_image = FunctionImage(image='edgerun/mobilenet-inference:1.0.0')
    mobilenet_function = Function(name='mobilenet', fn_images=[function_image], labels={'ether.edgerun.io/type': 'fn',
                                                                                        function_label: 'mobilenet'})

    cpu = os.environ.get('MOBILENET_CPU', '250m')
    memory = os.environ.get('MOBILENET_MEMORY', '250M')
    resource_requirements = ResourceRequirements(requests={'cpu': cpu, 'memory': memory})
    resource_config = KubernetesResourceConfiguration(requests=resource_requirements)
    print(resource_config.get_resource_requirements())
    max_scale = int(os.getenv('FUNCTION_MAX_SCALE', '10'))
    function_container = FunctionContainer(fn_image=function_image, resource_config=resource_config)
    function_deployment = FunctionDeployment(fn=mobilenet_function, fn_containers=[function_container]
                                             , scaling_configuration=ScalingConfiguration(scale_max=max_scale)
                                             , deployment_ranking=DeploymentRanking(containers=[function_container]))
    function_kubernetes_deployment = KubernetesFunctionDeployment(deployment=function_deployment,
                                                                  original_name='mobilenet'
                                                                  , namespace='default')
    return function_kubernetes_deployment


def generate_pi_deployment():
    function_image = FunctionImage(image='edgerun/pythonpi:1.0.3')
    function = 'pythonpi'
    mobilenet_function = Function(name=function, fn_images=[function_image], labels={'ether.edgerun.io/type': 'fn',
                                                                                     function_label: function})

    cpu = os.environ.get('MOBILENET_CPU', '500m')
    memory = os.environ.get('MOBILENET_MEMORY', '250M')
    resource_requirements = ResourceRequirements.from_str(memory, cpu)
    resource_config = KubernetesResourceConfiguration(requests=resource_requirements)
    print(resource_config.get_resource_requirements())
    max_scale = int(os.getenv('FUNCTION_MAX_SCALE', '30'))
    function_container = FunctionContainer(fn_image=function_image, resource_config=resource_config)
    function_deployment = FunctionDeployment(fn=mobilenet_function, fn_containers=[function_container]
                                             , scaling_configuration=ScalingConfiguration(scale_max=max_scale)
                                             , deployment_ranking=DeploymentRanking(containers=[function_container]))
    function_kubernetes_deployment = KubernetesFunctionDeployment(deployment=function_deployment,
                                                                  original_name=function
                                                                  , namespace='default')
    return function_kubernetes_deployment


def generate_busy_deployment():
    function_image = FunctionImage(image='aicg4t1/busy-python:1.0.2')
    mobilenet_function = Function(name='busy', fn_images=[function_image], labels={'ether.edgerun.io/type': 'fn',
                                                                                   function_label: 'busy'})

    cpu = os.environ.get('MOBILENET_CPU', '250m')
    memory = os.environ.get('MOBILENET_MEMORY', '250M')
    resource_requirements = ResourceRequirements(requests={'cpu': cpu, 'memory': memory})
    resource_config = KubernetesResourceConfiguration(requests=resource_requirements)
    print(resource_config.get_resource_requirements())
    max_scale = int(os.getenv('FUNCTION_MAX_SCALE', '10'))
    function_container = FunctionContainer(fn_image=function_image, resource_config=resource_config)
    function_deployment = FunctionDeployment(fn=mobilenet_function, fn_containers=[function_container]
                                             , scaling_configuration=ScalingConfiguration(scale_max=max_scale)
                                             , deployment_ranking=DeploymentRanking(containers=[function_container]))
    function_kubernetes_deployment = KubernetesFunctionDeployment(deployment=function_deployment,
                                                                  original_name='busy'
                                                                  , namespace='default')
    return function_kubernetes_deployment


def generate_galileo_deployment(fn_name: str, cluster: str):
    function_image = FunctionImage(image='edgerun/galileo:4b60c5d')
    mobilenet_function = Function(name=fn_name, fn_images=[function_image], labels={'ether.edgerun.io/type': 'fn',
                                                                                    function_label: fn_name, zone_label: cluster})
    resource_requirements = ResourceRequirements.from_str("250M", "250m")
    resource_config = KubernetesResourceConfiguration(requests=resource_requirements)
    print(resource_config.get_resource_requirements())
    function_container = FunctionContainer(fn_image=function_image, resource_config=resource_config,
                                           labels={client_role_label: 'true', 'to_call': 'pythonpi'})
    function_deployment = FunctionDeployment(fn=mobilenet_function, fn_containers=[function_container]
                                             , scaling_configuration=ScalingConfiguration(scale_max=10)
                                             , deployment_ranking=DeploymentRanking(containers=[function_container]))
    function_kubernetes_deployment = KubernetesFunctionDeployment(deployment=function_deployment,
                                                                  original_name=fn_name
                                                                  , namespace='default')
    return function_kubernetes_deployment


def generate_loadbalancer_deployment(fn_name: str, zone: str):
    function_image = FunctionImage(image='edgerun/go-load-balancer:0.1.10')
    mobilenet_function = Function(name=fn_name, fn_images=[function_image],
                                  labels={'ether.edgerun.io/type': 'api-gateway',
                                          function_label: 'api-gateway', zone_label: zone})
    resource_requirements = ResourceRequirements.from_str("250M", "250m")
    resource_config = KubernetesResourceConfiguration(requests=resource_requirements)
    print(resource_config.get_resource_requirements())
    function_container = FunctionContainer(fn_image=function_image, resource_config=resource_config,
                                           labels={'ether.edgerun.io/type': 'api-gateway',
                                                   function_label: 'api-gateway', zone_label: zone})
    function_deployment = FunctionDeployment(fn=mobilenet_function, fn_containers=[function_container]
                                             , scaling_configuration=ScalingConfiguration(scale_max=10)
                                             , deployment_ranking=DeploymentRanking(containers=[function_container]))
    function_kubernetes_deployment = KubernetesFunctionDeployment(deployment=function_deployment,
                                                                  original_name=fn_name
                                                                  , namespace='default')
    return function_kubernetes_deployment

def generate_apigateway_deployment(fn_name: str):
    function_image = FunctionImage(image='edgerun/go-load-balancer:0.1.13')
    mobilenet_function = Function(name=fn_name, fn_images=[function_image],
                                  labels={'ether.edgerun.io/type': 'api-gateway',
                                          function_label: 'api-gateway'})
    resource_requirements = ResourceRequirements.from_str("250M", "250m")
    resource_config = KubernetesResourceConfiguration(requests=resource_requirements)
    print(resource_config.get_resource_requirements())
    function_container = FunctionContainer(fn_image=function_image, resource_config=resource_config,
                                           labels={'ether.edgerun.io/type': 'api-gateway',
                                                   function_label: 'api-gateway'})
    function_deployment = FunctionDeployment(fn=mobilenet_function, fn_containers=[function_container]
                                             , scaling_configuration=ScalingConfiguration(scale_max=10)
                                             , deployment_ranking=DeploymentRanking(containers=[function_container]))
    function_kubernetes_deployment = KubernetesFunctionDeployment(deployment=function_deployment,
                                                                  original_name=fn_name
                                                                  , namespace='default')
    return function_kubernetes_deployment


def generate_old_galileo_deployment():
    function_image = FunctionImage(image='edgerun/galileo:4b60c5d')
    mobilenet_function = Function(name='client', fn_images=[function_image], labels={'ether.edgerun.io/type': 'fn',
                                                                                     function_label: 'client'})
    resource_requirements = ResourceRequirements.from_str("250M", "250m")
    resource_config = KubernetesResourceConfiguration(requests=resource_requirements)
    print(resource_config.get_resource_requirements())
    function_container = FunctionContainer(fn_image=function_image, resource_config=resource_config)
    function_deployment = FunctionDeployment(fn=mobilenet_function, fn_containers=[function_container]
                                             , scaling_configuration=ScalingConfiguration(scale_max=10)
                                             , deployment_ranking=DeploymentRanking(containers=[function_container]))
    function_kubernetes_deployment = KubernetesFunctionDeployment(deployment=function_deployment,
                                                                  original_name='client'
                                                                  , namespace='default')
    return function_kubernetes_deployment



def init_function_deployments(topology: Topology) -> List[KubernetesFunctionDeployment]:
    # deployments = [generate_mobilenet_deployment(), generate_galileo_deployment(), generate_busy_deployment(), generate_pi_deployment()]
    deployments = [generate_pi_deployment(), generate_old_galileo_deployment()]
    # deployments = [generate_pi_deployment()]
    if topology is not None:
        for node in topology.get_nodes():
            if node.labels.get(client_role_label):
                deployment = generate_galileo_deployment(f'galileo-worker-{node.labels[zone_label]}',
                                                         node.labels[zone_label])
                deployments.append(deployment)
            if node.labels.get(controller_role_label):
                deployment = generate_loadbalancer_deployment(f'load-balancer-{node.name}', node.labels[zone_label])
                deployments.append(deployment)
    else:
        deployment = generate_apigateway_deployment('api-gateway')
        deployments.append(deployment)

    return deployments
