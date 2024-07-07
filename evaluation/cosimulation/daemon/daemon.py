import logging
import multiprocessing

import kubernetes
from evaluation.cosimulation.main import parse_cli_arguments, CoSimLoop
from galileofaas.connections import RedisClient

logger = logging.getLogger(__name__)


def event_listener(r: RedisClient, co_sim_input):
    try:
        logger.info('waiting for experiment commands')
        co_sim_loop_process: multiprocessing.Process = None
        co_sim_loop: CoSimLoop = None
        stop_queue: multiprocessing.Queue = None
        for message in r.sub('experiment'):
            if message['type'] == 'message':
                if message['data'] == 'start':
                    logger.info('starting experiment')
                    stop_queue: multiprocessing.Queue = multiprocessing.Queue()
                    co_sim_loop = CoSimLoop(co_sim_input, stop_queue)
                    co_sim_loop_process = co_sim_loop.start()

                elif message['data'] == 'end':
                    logger.info('ending experiment')
                    if co_sim_loop is not None:
                        stop_queue.put(False)
                        co_sim_loop_process.join(timeout=11)
                        co_sim_loop_process.terminate()

                elif message['data'] == 'terminate':
                    logger.info('terminating experiment')
                    break
    finally:
        r.close()


def main():
    # basically we want to listen to an experiment start event via redis
    # then we start the cosimulation loop
    # upon an experiment-end event we want to stop the loop and wait for yet another start event
    # we also listen onto a complete end that terminates the main program
    rds = RedisClient.from_env()
    kubernetes.config.load_config()

    co_sim_input = parse_cli_arguments()
    event_listener(rds, co_sim_input)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
