import base64
from typing import Dict

import redis
import requests
from galileo.shell.shell import Galileo, ClientGroup
from galileoexperiments.api.model import ProfilingWorkloadConfiguration
from galileoexperiments.api.profiling import ProfilingApplication
from kubernetes import client
from kubernetes.client import V1ResourceRequirements, V1EnvVar, V1EnvVarSource, V1ObjectFieldSelector


def _load_image_locally(path: str) -> str:
    with open(path, 'rb') as fd:
        return base64.b64encode(fd.read()).decode('utf-8')


def _spawn_zone_group(zone: str, clients: int, g: Galileo, digits: int, fn_name: str, location: str,
                      remote: bool) -> ClientGroup:
    return _spawn_group(g, clients, fn_name, f'{fn_name}-{zone}', digits=digits, labels={'galileo_zone': zone},
                        location=location, remote=remote)


def _prepare_image(url: str):
    r = requests.get(url)
    r.raise_for_status()
    return base64.b64encode(r.content).decode('utf-8')


def _get_image(location: str, remote: bool) -> str:
    if remote:
        data = _prepare_image(location)
    else:
        data = _load_image_locally(location)
    return '{"picture": "%s"}' % data


def _spawn_group(g: Galileo, clients: int, fn_name: str, service_name: str, digits: int, location: str, remote: bool,
                 labels: dict = None):
    path = f'/function/{fn_name}'
    return g.spawn(service_name, clients,
                   parameters={'method': 'post', 'path': path, 'kwargs': {'data':
                                                                              '{"digits": %s, "image": %s}' % (
                                                                                  str(digits),
                                                                                  _get_image(location, remote))}},
                   worker_labels=labels)


class PiProfilingApplication(ProfilingApplication):

    def spawn_group(self, clients: int, rds: redis.Redis, galileo: Galileo,
                    config: ProfilingWorkloadConfiguration) -> ClientGroup:
        zone = config.zone
        digits = int(config.params['service']['digits'])
        fn_name = config.params['service']['name']
        location = config.params['service']['location']
        remote = config.params['service']['remote']

        return _spawn_group(galileo, clients, fn_name, f'{fn_name}-{zone}', digits=digits,
                            labels={'galileo_zone': zone}, location=location, remote=remote)

    def pod_factory(self, pod_name: str, image: str, resource_requests: Dict) -> client.V1Container:
        return client.V1Container(
            image=image,
            name=pod_name,
            ports=[
                client.V1ContainerPort(
                    name="function-port", container_port=8080
                )
            ],
            resources=V1ResourceRequirements(
                requests={"cpu": "500m", "memory": "250M"}
            ),
            env=[
                V1EnvVar('NODE_NAME', value_from=V1EnvVarSource(
                    field_ref=(V1ObjectFieldSelector(field_path='spec.nodeName')))),
                V1EnvVar('write_timeout', '100'),
                V1EnvVar('exec_timeout', '100'),
                V1EnvVar('read_timeout', '100'),
            ]
        )
