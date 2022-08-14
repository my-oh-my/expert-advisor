import argparse
import sched
import time
from datetime import datetime
from datetime import timedelta

import schedule
from xtb.wrapper.xtb_client import APIClient, loginCommand

from ea.messaging.producer import MessageProducer, KafkaProducerWrapper
from ea.strategies.indicators.waves import Waves


def run_ea(user_id, password, strategy, settings, backtest_func=None, plot_func=None, message_producer: MessageProducer=None):
    print(f'Running EA at: {datetime.now()}')

    client = APIClient()
    # connect to RR socket, login
    loginResponse = client.execute(loginCommand(userId=user_id, password=password))
    if not loginResponse['status']:
        print('Login failed. Error code: {0}'.format(loginResponse['errorCode']))
        return

    # ea = ExpertAdvisor(client, settings['symbol'], settings['period'], _strategy)
    # current_state = ea.run_strategy(settings, backtest_provider, plot_func)
    # if message_producer is not None:
    #     message_producer.send(current_state)
    # #
    # ea.disconnect()


def main(user_id, password, strategy, settings, backtest_func=None, plot_func=None, message_producer: MessageProducer=None):
    schedule \
        .every(period).minutes.at(":00") \
        .do(
            run_ea,
            user_id=user_id,
            password=password,
            strategy=strategy,
            settings=settings,
            backtest_func=backtest_func,
            plot_func=plot_func,
            message_producer=message_producer
        )
    while True:
        schedule.run_pending()


def ceil_dt(dt, delta):
    return dt + (datetime.min - dt) % delta


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user_id', type=int, required=True)
    parser.add_argument('-p', '--password', type=str, required=True)
    parser.add_argument('--bootstrap_servers', nargs="+", default=['localhost:9092'])
    args = parser.parse_args()

    user_id = args.user_id
    password = args.password
    bootstrap_servers = args.bootstrap_servers

    period = 5
    waves_settings = dict(
        symbol='BITCOIN',
        period=period,
        distance=200
    )

    waves_strategy = Waves(waves_settings)
    message_producer = KafkaProducerWrapper(
        dict(bootstrap_servers=bootstrap_servers, topic='signals')
    )
    #
    # run_ea(user_id, password, _strategy, settings, _strategy.consolidation_backtest, _strategy.plot_chart)
    now = datetime.now()
    scheduled_at = ceil_dt(now, timedelta(minutes=period))
    print(scheduled_at)
    #
    scheduler = sched.scheduler(time.time, time.sleep)
    # scheduler.enterabs(
    #     scheduled_at.timestamp(),
    #     0,
    #     main,
    #     (user_id, password, waves_strategy, waves_settings, waves_strategy.consolidation_backtest, waves_strategy.plot_chart, message_producer)
    # )

    scheduler.run()
