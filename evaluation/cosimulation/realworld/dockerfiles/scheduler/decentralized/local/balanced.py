import json
import logging
import os

from faas.system.scheduling.decentralized import BaseLocalSchedulerConfiguration

from evaluation.cosimulation.realworld.scheduler import LocalSchedulerDaemon

logger = logging.getLogger(__name__)


def main():
    initial_config = read_initial_configuration()
    daemon = LocalSchedulerDaemon(initial_config)
    print(f'Running for {initial_config.zone}')
    print(f'Running for {initial_config.scheduler_name}')
    print(f'Running for global {initial_config.global_scheduler_name}')
    print(f'Running with delay: {initial_config.delay}')
    daemon.run()

def read_initial_configuration() -> BaseLocalSchedulerConfiguration:
    local_config_path = os.getenv('LOCAL_CONFIG_PATH')
    config_file_name = os.path.basename(local_config_path)
    config_volume_path = os.getenv('CONFIG_VOLUME_PATH')
    config_file_path = os.path.join(config_volume_path, config_file_name)
    with open(config_file_path, 'r') as f:
        config = json.load(f)
        config = BaseLocalSchedulerConfiguration.from_json(config)
        return config


if __name__ == '__main__':
    formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
                " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
    logging.basicConfig(level=logging.INFO, format=formatter)
    main()
