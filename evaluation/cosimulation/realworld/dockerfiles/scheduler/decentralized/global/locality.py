import json
import logging
import os

from faas.system.scheduling.decentralized import BaseGlobalSchedulerConfiguration

from evaluation.cosimulation.realworld.scheduler import GlobalSchedulerDaemon

logger = logging.getLogger(__name__)


def main():
    delay = float(os.getenv('DELAY'))
    storage_local_schedulers_dict_string = os.environ.get('STORAGE_LOCAL_SCHEDULERS')
    storage_local_schedulers = json.loads(storage_local_schedulers_dict_string)
    max_scale = float(os.environ.get('FUNCTION_MAX_SCALE'))
    config = read_initial_configuration()
    print(max_scale)
    daemon = GlobalSchedulerDaemon(config, storage_local_schedulers, delay, max_scale)
    print(f'Running for {storage_local_schedulers}')
    print(f'Running for {config.scheduler_name}')
    print(f'Running with delay: {delay}')
    daemon.run()

def read_initial_configuration() -> BaseGlobalSchedulerConfiguration:
    local_config_path = os.getenv('LOCAL_CONFIG_PATH')
    config_file_name = os.path.basename(local_config_path)
    config_volume_path = os.getenv('CONFIG_VOLUME_PATH')
    config_file_path = os.path.join(config_volume_path, config_file_name)
    with open(config_file_path, 'r') as f:
        config = json.load(f)
        config = BaseGlobalSchedulerConfiguration.from_json(config)
        return config



if __name__ == '__main__':
    formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
                " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
    logging.basicConfig(level=logging.INFO, format=formatter)
    main()
