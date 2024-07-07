import json
import logging
import multiprocessing
import os

from galileofaas.util.pubsub import POISON

from evaluation.cosimulation.realworld.autoscaler import ReconciledDecentralizedAutoscalerDaemon
from evaluation.cosimulation.simulation.util import HpaAutoscalerConfiguration


def main():
    stop_queue = multiprocessing.Queue()

    try:
        config = read_initial_configuration()
        zone = os.getenv('ZONE')
        if zone == 'None':
            zone = None
        scheduler_name = os.getenv('SCHEDULER_NAME')
        if scheduler_name is None:
            raise ValueError('No scheduler name provided')
        daemon = ReconciledDecentralizedAutoscalerDaemon(zone, scheduler_name, config, stop_queue)
        print(f'Running for {zone}')
        print(f'Running for {scheduler_name}')
        print(f'Running with config: {config}')
        daemon.run()
    finally:
        stop_queue.put(POISON)


def read_initial_configuration() -> HpaAutoscalerConfiguration:
    local_config_path = os.getenv('LOCAL_CONFIG_PATH')
    config_file_name = os.path.basename(local_config_path)
    config_volume_path = os.getenv('CONFIG_VOLUME_PATH')
    config_file_path = os.path.join(config_volume_path, config_file_name)
    with open(config_file_path, 'r') as f:
        config = json.load(f)
        config = HpaAutoscalerConfiguration.from_json(config)
        return config


if __name__ == '__main__':
    formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
                " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
    logging.basicConfig(level=logging.INFO, format=formatter)
    main()
