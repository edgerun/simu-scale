from typing import List

from galileofaas.system.core import KubernetesFunctionDeployment
from sim.context.platform.deployment.model import SimFunctionDeployment, SimScalingConfiguration


def real_world_to_sim_deployment(k8s_deployments: List[KubernetesFunctionDeployment]) -> List[SimFunctionDeployment]:
    sim_deployments = []
    for k8s_deployment in k8s_deployments:
        sim_scaling_config = SimScalingConfiguration(k8s_deployment.scaling_configuration)
        sim_scaling_config.scaling_config.scale_max = 25
        sim_deployment = SimFunctionDeployment(k8s_deployment.fn, k8s_deployment.fn_containers, sim_scaling_config,
                                               k8s_deployment.deployment_ranking)
        sim_deployments.append(sim_deployment)

    return sim_deployments
