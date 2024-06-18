import logging
from http import cookiejar
from time import sleep
from random import uniform

import requests
from fake_useragent import UserAgent
from requests.exceptions import HTTPError
from stem import Signal
from stem.control import Controller


PWD = 'yohjiyamamoto'
PORTS = ['9050', '9052', '9053', '9054']


class BlockAll(cookiejar.CookiePolicy):
    return_ok = set_ok = domain_return_ok = path_return_ok = lambda self, *args, **kwargs: False
    netscape = True
    rfc2965 = hide_cookie2 = False


class Scraper():
    def __init__(self, max_extra_wait, max_trys, request_timeout, logger):
        self.password = PWD
        self.ports = PORTS[:]
        self.max_extra_wait = max_extra_wait
        self.max_trys = max_trys
        self.request_timeout = request_timeout
        self.logger = logger
        self.ua = UserAgent()

    def set_tor_session(self, port):
        self.session = requests.session()
        self.session.cookies.set_policy(BlockAll())
        self.session.proxies = {
            'http': f'socks5h://localhost:{port}',
            'https': f'socks5h://localhost:{port}'
        }

    def renew_tor_ip(self):
        with Controller.from_port(port=9051) as controller:
            controller.authenticate(password=self.password)
            
            if not controller.is_newnym_available():
                self.logger.info(f'Wait for new IP addresses: {controller.get_newnym_wait()}')

            while not controller.is_newnym_available():
                pass
            
            controller.signal(Signal.NEWNYM)

    def get_tor_session(self):
        if not self.ports:
            self.renew_tor_ip()
            self.ports = PORTS[:]

        self.set_tor_session(self.ports.pop())

    def get_content(self, url):
        sleep(uniform(0, self.max_extra_wait))

        for i in range(1, self.max_trys + 1):
            self.get_tor_session()

            try:
                headers = {
                    'User-Agent': self.ua.random
                }

                response = self.session.get(url, headers=headers,
                                            timeout=self.request_timeout)
                response.raise_for_status()
            except HTTPError as http_err:
                self.logger.error(f'HTTP Error with requesting {url}: {http_err}')
                self.logger.info(f'Waiting for {self.request_timeout} seconds [{i}/{self.max_trys}]')
                sleep(self.request_timeout)
            except Exception as e:
                self.logger.error(f'Error with requesting {url}: {e}')
                self.logger.info(f'Waiting for {self.request_timeout} seconds [{i}/{self.max_trys}]')
                sleep(self.request_timeout)
            else:
                return response.content


if __name__ == "__main__":
    scraper = Scraper(max_extra_wait=5, max_trys=5, request_timeout=10)

    for _ in range(8):
        print(scraper.get_content('http://ipecho.net/plain'))
