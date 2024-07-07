import logging
import os

from evaluation.cosimulation.realworld.scheduler import LocalSchedulerDaemon

logger = logging.getLogger(__name__)


def main():
    zone = os.getenv('ZONE')
    scheduler_name = os.getenv('SCHEDULER_NAME')
    global_scheduler_name = os.getenv('GLOBAL_SCHEDULER_NAME')
    delay = float(os.getenv('DELAY'))
    daemon = LocalSchedulerDaemon(scheduler_name, zone, delay, global_scheduler_name)
    print(f'Running for {zone}')
    print(f'Running for {scheduler_name}')
    print(f'Running for global {global_scheduler_name}')
    print(f'Running with delay: {delay}')
    daemon.run()


if __name__ == '__main__':
    formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
                " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
    logging.basicConfig(level=logging.INFO, format=formatter)
    main()
