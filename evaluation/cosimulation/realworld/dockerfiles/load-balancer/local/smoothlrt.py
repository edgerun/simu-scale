import logging
import multiprocessing
import os

from galileofaas.util.pubsub import POISON

from evaluation.cosimulation.realworld.loadbalancer import SmoothLrtLbDaemon


def main():
    stop_queue = multiprocessing.Queue()
    try:
        cluster = os.getenv('CLUSTER')
        scheduler_name = os.getenv('SCHEDULER_NAME')
        reconcile_interval = float(os.getenv("RECONCILE_INTERVAL"))
        daemon = SmoothLrtLbDaemon(cluster, reconcile_interval, scheduler_name, stop_queue)
        print(f'Running for {cluster}')
        print(f'Running for {scheduler_name}')
        print(f'Running with reconcile_interval: {reconcile_interval}')
        daemon.run()
    finally:
        stop_queue.put(POISON)


if __name__ == '__main__':
    formatter = " %(asctime)s | %(levelname)-6s | %(process)d | %(threadName)-12s |" \
                " %(thread)-15d | %(name)-30s | %(filename)s:%(lineno)d | %(message)s |"
    logging.basicConfig(level=logging.INFO, format=formatter)
    main()
