import logging
import sys
from typing import Dict

import docker
import kubernetes
from dotenv.main import dotenv_values
from evaluation.cosimulation.main import parse_cli_arguments
from galileofaas.connections import RedisClient

logger = logging.getLogger(__name__)


def read_env_file(env_file: str) -> Dict[str, str]:
    envs = dotenv_values(env_file)
    return envs


def start_co_sim(client: docker.client.DockerClient, docker_image: str, env_file: str, kube_config_path: str,
                 params_folder: str, results_folder: str):
    args = ["python", "-u", "-m", "evaluation.cosimulation.main"]
    args.extend(list(sys.argv[1:10]))
    container_name = 'cosim-container'
    logger.info(f'Starting container: {container_name}')
    environment = read_env_file(env_file)
    volumes = {kube_config_path: {'bind': kube_config_path, 'mode': 'rw'},
               params_folder: {'bind': '/data', 'mode': 'rw'}, results_folder: {'bind': '/results', 'mode': 'rw'}}
    logger.info(f'With args: {args}')
    container = client.containers.run(docker_image, args, detach=True, volumes=volumes, name=container_name,
                                      environment=environment, network_mode="host")
    return container


def event_listener(r: RedisClient):
    client = docker.from_env()
    running_co_sim = None
    try:
        logger.info('waiting for experiment commands')
        running_co_sim = None
        for message in r.sub('experiment'):
            if message['type'] == 'message':
                if message['data'] == 'start':
                    logger.info('starting experiment')
                    start_idx = 10
                    container_image = sys.argv[start_idx]
                    env_file = sys.argv[start_idx + 1]
                    kube_config_path = sys.argv[start_idx + 2]
                    params_folder = sys.argv[start_idx + 3]
                    results_folder = sys.argv[start_idx + 4]
                    logger.info(
                        f'Starting experiment, container image: {container_image}, env file: {env_file}, kube_config_path: {kube_config_path}, params folder: {params_folder}, results folder: {results_folder}')
                    running_co_sim = start_co_sim(client, container_image, env_file, kube_config_path, params_folder,
                                                  results_folder)
                elif message['data'] == 'end':
                    logger.info('ending experiment')
                    if running_co_sim is not None:
                        try:
                            running_co_sim.kill()
                        except Exception as e:
                            logger.warning(f'Wanted to kill and remove container but could not find')
                        finally:
                            running_co_sim.remove()
                            running_co_sim = None

                elif message['data'] == 'terminate':
                    logger.info('terminating experiment')
                    break
    finally:
        r.close()
        if running_co_sim is not None:
            running_co_sim.kill()


def main():
    # basically we want to listen to an experiment start event via redis
    # then we start the cosimulation loop
    # upon an experiment-end event we want to stop the loop and wait for yet another start event
    # we also listen onto a complete end that terminates the main program
    rds = RedisClient.from_env()
    kubernetes.config.load_config()

    # simply check for valid arguments but we pass on sys.argv to the container
    parse_cli_arguments(17)
    event_listener(rds)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
