import json
import requests

from ea.misc.logger import logger


class SlackService:
    def __init__(self, url):
        self._url = url

    def send(self, message: str):
        payload = '{"text": "%s"}' % message
        response = requests.post(
            url=self._url,
            data=payload
        )

        logger.info(f'Response from Slack: {response}')
