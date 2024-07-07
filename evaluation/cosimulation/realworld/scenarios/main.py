import logging
import os
import sys

from galileo.shell.shell import init
from galileo.worker.context import Context
from galileoexperiments.api.model import ScenarioWorkloadConfiguration
from galileoexperiments.experiment.scenario.run import run_scenario_workload
from galileofaas.connections import KubernetesClient
from kubernetes import client
from kubernetes.client import ApiClient

from evaluation.cosimulation.apps.pi import PiProfilingApplication
from evaluation.cosimulation.main import read_json_file

logger = logging.getLogger(__name__)

from evaluation.cosimulation.util.k8s import get_hpa_component_configurations


def main():
    formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
                " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
    logging.basicConfig(level=logging.INFO, format=formatter)
    # logging.basicConfig(level=None)
    if len(sys.argv) != 2:
        logger.error("Usage: python main.py <workload_profile>")
        return

    pid = str(os.getpid())
    with open("/tmp/app.pid", "w") as file:
        file.write(pid)

    workload_profile = sys.argv[1]
    logger.info(f"Workload profile {workload_profile}")
    KubernetesClient.from_env()
    ctx = Context()
    rds = ctx.create_redis()

    config = setup_experiment(rds, workload_profile)
    run_scenario_workload(config)

    logging.info('done')


def extract_weak_node_names():
    v1 = client.CoreV1Api()
    node_list = v1.list_node()
    temp_list = []

    json_data = ApiClient().sanitize_for_serialization(node_list)
    if len(json_data["items"]) != 0:
        for node in json_data["items"]:
            node_name = node["metadata"]["name"]
            if node_name.startswith('w') and 'worker' in node_name:
                temp_list.append(node_name)
    return temp_list


def setup_experiment(rds, workload_profile):
    creator = 'exp-dece_lc-lc'
    pi_image = 'edgerun/pythonpi:1.0.3'
    app_names = {
        pi_image: 'pythonpi'
    }
    master_node = 's10-master-zone-c-0'
    # node to service mapping including the number of service instances
    services = {
        's30-worker-zone-c-0': {
            pi_image: 2
        },
        'w10-worker-zone-a-0': {
            pi_image: 1
        },
        'w20-worker-zone-b-0': {
            pi_image: 1
        }
    }
    # maps nodes that should host applications to zones
    zone_a = 'zone-a'
    zone_b = 'zone-b'
    zone_c = 'zone-c'
    zone_mapping = {
        's30-worker-zone-c-0': zone_c,
        'w10-worker-zone-a-0': zone_a,
        'w20-worker-zone-b-0': zone_b,
    }
    params = {}
    component_envs = get_hpa_component_configurations()
    params['component_envs'] = component_envs
    params['workload'] = workload_profile
    # parameters for each image (used to initialize the clients)
    app_params = {
        pi_image: {
            'service': {
                'name': 'pythonpi',
                'digits': 4000,
                'location': 'data/pictures/dog.jpg',
                'remote': False
            }
        }
    }
    profiling_apps = {
        pi_image: PiProfilingApplication()
    }
    # Instantiate galileo context that includes all dependencies needed to execute an experiment
    g = init(rds)

    # 4 peaks 1681 events
    # test_profile = 'data/profiles/4peaks_2_420.pkl'
    # 4 peaks 3528 events
    # test_profile = 'data/profiles/4peaks_420.pkl'
    # 2 peaks ca 850 events
    # test_profile = 'data/profiles/exp_p_440.pkl'
    # test_profile = 'data/profiles/4peaks_1_540.pkl'
    test_profile = read_json_file(workload_profile)['profiles']
    print(test_profile)
    # client profiles, each starts one client that sends to the zone's load balancer
    profiles = test_profile
    config = ScenarioWorkloadConfiguration(
        creator=creator,
        app_names=app_names,
        master_node=master_node,
        services=services,
        zone_mapping=zone_mapping,
        params=params,
        app_params=app_params,
        profiling_apps=profiling_apps,
        context=g,
        profiles=profiles
    )
    return config


if __name__ == '__main__':
    main()
