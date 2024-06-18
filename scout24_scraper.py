import argparse
import logging
import sys
from datetime import datetime
from json import dump, load
from os import mkdir
from os.path import exists
from shutil import rmtree

import xlsxwriter
from bs4 import BeautifulSoup
from googletrans import Translator

from scraper import Scraper


OBJECT_KEYS = ['Building name', 'Address', 'City', 'State/Province/Region', 'ZIP/Postal code', 'Country', 'Available surface', 'Ft/Sq2', 'Min. Rent price', 'Max. Rent price', 'Currency', 'Parking ratio', 'Parking price', 'Energy label', 'Estate agent company', 'Estate agent name', 'Estate agent phone number', '_URL', '_ID', '_DATE_SCRAPED', '_CITY_DISTRICT', '_EXTRA_COSTS', '_NUMBER_OF_PARKING_SPOTS', '_IMAGE_URL', '_ESTATE_URL', '_YEAR_OF_CONSTRUCTION', '_BUILDING_STATE', '_FLOOR_PLAN_URL']
OBJECT = {key: None for key in OBJECT_KEYS}
OBJECT['Country'] = 'Germany'
OBJECT['Currency'] = 'EUR'
OBJECT['Ft/Sq2'] = 'Sq2'


class Scout24Scraper():
    def __init__(self, request_timeout, name):
        self.objects = list()
        self.request_timeout = request_timeout
        self.name = name
        self.translator = Translator()

        if exists(f'./{name}'):
            rmtree(f'./{name}')

        mkdir(f'./{name}')

        formatter = logging.Formatter('(%(asctime)s) - %(levelname)s: %(message)s')

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setLevel(logging.DEBUG)
        streamHandler.setFormatter(formatter)
        self.logger.addHandler(streamHandler)

        fileHandler = logging.FileHandler(f'./{name}/{name}.log')
        fileHandler.setLevel(logging.DEBUG)
        fileHandler.setFormatter(formatter)
        self.logger.addHandler(fileHandler)

        self.logger.info(f'Initialized Scraper\n\n')

    def load_objects_from_json(self, path):
        with open(path) as f:
            data = load(f)
            self.objects.extend(data)

        self.logger.info(f'Succesfully loaded objects from {path}\n\n')

    def scrape_ids(self, max_ids, query, max_extra_wait=0):
        self.logger.info(f'Starting with scraping {max_ids} ids from {query}\n')
        scraper = Scraper(max_extra_wait=max_extra_wait, max_trys=5,
                          request_timeout=self.request_timeout, logger=self.logger)
        content = scraper.get_content(query)

        if content is None:
            self.logger.error(f'Error with scraping {max_ids} ids from {query}')
            return

        soup = BeautifulSoup(content, 'html.parser')
        n_pages = int(soup.find('nav', class_='pagination').find('div', class_='palm-hide').find_all('span', class_='pg-item')[-1].text)
        
        for page in range(1, n_pages + 1):
            page_url = f'{query}seite/{page}'
            content = scraper.get_content(page_url)

            if content is None:
                self.logger.error(f'Error with scraping ids from page {page} [{page_url}]')
                continue

            soup = BeautifulSoup(content, 'html.parser')
            links = soup.find_all('a', class_='contact-realtor-button button-secondary palm-hide')
            self.logger.info(f'[{page}/{n_pages}] Scraping ids {len(links)} from {page_url}')

            for link in links:
                obj = dict(OBJECT)
                obj['_ID'] = link['href'].split('/')[4]
                obj['_URL'] = f'https://www.immobilienscout24.de/expose/{obj["_ID"]}#/'
                self.objects.append(obj)

                if len(self.objects) >= max_ids:
                    self.logger.info(f'Succesfully scraped [{len(self.objects)}/{max_ids}] ids\n\n')
                    return

        self.logger.info(f'Succesfully scraped [{len(self.objects)}/{max_ids}] ids\n\n')

    def building_name(self, soup, obj):
        try:
            title = soup.find('h1', {'id': 'expose-title'}).text
            obj['Building name'] = title.split(' |', 1)[0].split(' ¦', 1)[0].split(' -', 1)[0].split(': ', 1)[0]
        except Exception as e:
            self.logger.warning(f'Error with "Building name": {e}')

    def full_address(self, soup, obj):
        try:
            address_soup = soup.find('div', class_='address-block')
        except Exception as e:
            self.logger.warning(f'Error with full address (address-block): {e}')

        try:
            zip_region = address_soup.div.find('span', class_='zip-region-and-country').text.split(' ', 1)
            obj['ZIP/Postal code'] = zip_region[0].strip()
            region = zip_region[1].split(',', 1)
            obj['City'] = region[0].strip()

            if len(region) > 1:
                obj['_CITY_DISTRICT'] = region[1].strip()
        except Exception as e:
            self.logger.warning(f'Error with full address (zip_region): {e}')

        try:
            address = address_soup.div.find('span', class_='block font-nowrap print-hide').text.split(',', 1)[0].strip()
            obj['Address'] = address
        except Exception as e:
            self.logger.warning(f'Error with full address (address): {e}')

    def surface_rent(self, soup, obj):
        try:
            area = float(soup.find('div', class_='is24qa-flaeche is24-value font-semibold').text.replace('.', '').replace(',', '.')[1:-3])
            obj['Available surface'] = area
            obj['Min. Rent price'] = obj['Max. Rent price'] = float(soup.find('div', class_='is24qa-monatl-miete-pro-m² is24-value font-semibold').text.replace(',', '.')[1:-3]) * area
            obj['_EXTRA_COSTS'] = float(soup.find('div', class_='is24qa-nebenkosten is24-value font-semibold').text.strip().split(' ', 2)[0].strip().replace(',', '.')) * area
        except Exception as e:
            self.logger.warning(f'Error with surface and rent: {e}')

    def extra_properties(self, soup, obj):
        try:
            obj['_NUMBER_OF_PARKING_SPOTS'] = int(soup.find('dd', class_='is24qa-anzahl-parkflaechen grid-item three-fifths').text)
        except Exception as e:
            self.logger.warning(f'Error with _NUMBER_OF_PARKING_SPOTS: {e}')

    def image_url(self, soup, obj):
        try:
            obj['_IMAGE_URL'] = soup.find('img', class_='gallery-element is24-fullscreen-gallery-trigger').get('src')
        except Exception as e:
            self.logger.warning(f'Error with images: {e}')

    def estate(self, soup, obj):
        try:
            estate_div = soup.find('div', class_='grid grid-flex grid-align-top grid-justify-start padding-vertical')
            obj['Estate agent company'] = estate_div.find('ul', class_='inline-block line-height-xs').find('span', class_='font-semibold').text
            obj['_ESTATE_URL'] = estate_div.find('a', {'id': 'is24-expose-realtor-box-homepage'}).text
        except Exception as e:
            self.logger.warning(f'Error with estate: {e}')

    def building_energy(self, soup, obj):
        try:
            obj['_YEAR_OF_CONSTRUCTION'] = int(soup.find('dd', class_='is24qa-baujahr grid-item three-fifths').text)
            obj['_BUILDING_STATE'] = self.translator.translate(soup.find('dd', class_='is24qa-objektzustand grid-item three-fifths').text.strip()).text.lower()
        except Exception as e:
            self.logger.warning(f'Error with building/energy: {e}')

    def floor_plan_url(self, soup, obj):
        try:
            obj['_FLOOR_PLAN_URL'] = soup.find('div', {'id': 'is24-ex-floorplans'}).img['src']
        except Exception as e:
            self.logger.warning(f'Error with floor plan url: {e}')

    def scrape(self, max_extra_wait=0):
        self.logger.info(f'Starting with scraping {len(self.objects)} objects\n')
        scraper = Scraper(max_extra_wait=max_extra_wait, max_trys=5,
                          request_timeout=self.request_timeout, logger=self.logger)
        failed = 0

        for i, obj in enumerate(self.objects, 1):
            self.logger.info(f'[{i}/{len(self.objects)}] Scraping {obj["_ID"]} from {obj["_URL"]}')
            content = scraper.get_content(obj['_URL'])

            if content is None:
                self.logger.error('Error with getting content')
                failed += 1
                continue

            soup = BeautifulSoup(content, 'html.parser')
            self.building_name(soup, obj)
            self.full_address(soup, obj)
            self.surface_rent(soup, obj)
            self.extra_properties(soup, obj)
            self.image_url(soup, obj)
            self.estate(soup, obj)
            self.building_energy(soup, obj)
            self.floor_plan_url(soup, obj)
            obj['_DATE_SCRAPED'] = str(datetime.now())

        self.logger.info(f'Succesfully scraped [{len(self.objects) - failed}/{len(self.objects)}] objects\n\n')

    def download_main_images(self, max_extra_wait=0):
        self.logger.info(f'Starting with downloading main images of {len(self.objects)} objects\n')

        if not exists(f'./{self.name}/images'):
            mkdir(f'./{self.name}/images')

        scraper = Scraper(max_extra_wait=max_extra_wait, max_trys=5,
                          request_timeout=self.request_timeout, logger=self.logger)
        failed = 0

        for i, obj in enumerate(self.objects, 1):
            self.logger.info(f'[{i}/{len(self.objects)}] Downloading main image of {obj["_ID"]} from {obj["_URL"]}')

            if obj['_IMAGE_URL'] is None:
                self.logger.warning(f'Object property "_IMAGE_URL" is None')
                failed += 1
                continue

            with open(f'./{self.name}/images/{obj["_ID"]}.png', 'wb') as img:
                content = scraper.get_content(obj['_IMAGE_URL'])

                if content:
                    img.write(content)
                else:
                    self.logger.warning(f'Error with downloading main image from {obj["_IMAGE_URL"]}')
                    failed += 1

        self.logger.info(f'Succesfully downloaded main images of [{len(self.objects) - failed}/{len(self.objects)}] objects\n\n')

    def download_floor_plans(self, max_extra_wait=0):
        self.logger.info(f'Starting with downloading floor plans of {len(self.objects)} objects\n')

        if not exists(f'./{self.name}/images'):
            mkdir(f'./{self.name}/images')

        scraper = Scraper(max_extra_wait=max_extra_wait, max_trys=5,
                          request_timeout=self.request_timeout, logger=self.logger)
        failed = 0

        for i, obj in enumerate(self.objects, 1):
            self.logger.info(f'[{i}/{len(self.objects)}] Downloading floor plan of {obj["_ID"]} from {obj["_URL"]}')

            if obj['_FLOOR_PLAN_URL'] is None:
                self.logger.warning(f'Object property "_FLOOR_PLAN_URL" is None')
                failed += 1
                continue

            with open(f'./{self.name}/images/{obj["_ID"]}_floor_plan.png', 'wb') as img:
                content = scraper.get_content(obj['_FLOOR_PLAN_URL'])

                if content:
                    img.write(content)
                else:
                    self.logger.warning(f'Error with downloading floor plan from {obj["_IMAGE_URL"]}')
                    failed += 1

        self.logger.info(f'Succesfully downloaded floor plans of [{len(self.objects) - failed}/{len(self.objects)}] objects\n\n')
        

    def save_json(self):
        with open(f'./{self.name}/{self.name}.json', 'w') as f:
            dump(self.objects, f)

    def save_xlsx(self, without_underscore):
        path = None

        if without_underscore:
            path = f'./{self.name}/{self.name}.xlsx'
        else:
            path = f'./{self.name}/{self.name}_more.xlsx'

        with xlsxwriter.Workbook(path) as workbook:
            worksheet = workbook.add_worksheet()
            bold = workbook.add_format({'bold': True})

            if without_underscore:
                keys = [key for key in OBJECT_KEYS if key[0] != '_']
                worksheet.write_row(0, 0, data=keys, cell_format=bold)

                for row, obj in enumerate(self.objects, 1):
                    worksheet.write_row(row, 0, data=[obj[key] for key in keys])
            else:
                worksheet.write_row(0, 0, data=OBJECT_KEYS, cell_format=bold)

                for row, obj in enumerate(self.objects, 1):
                    worksheet.write_row(row, 0, data=obj.values())

    def save_data(self):
        self.save_json()
        self.save_xlsx(without_underscore=True)
        self.save_xlsx(without_underscore=False)
        self.logger.info(f'Succesfully saved all objects in ./{self.name}\n\n')


def main():
    parser = argparse.ArgumentParser(description='Web scraper for Immobilienscout24')
    parser.add_argument('-a', '--amount', nargs=1, type=int, required=True)
    parser.add_argument('-n', '--name', nargs=1, type=str, required=True)
    parser.add_argument('-q', '--query', nargs=1, type=str, required=True)
    parser.add_argument('-s', '--state', nargs=1, type=str, required=True)
    parser.add_argument('-w', '--wait', nargs=1, type=int, required=True)

    args = vars(parser.parse_args())

    amount = args['amount'][0]
    name = args['name'][0]
    query = args['query'][0]
    wait = args['wait'][0]
    OBJECT['State/Province/Region'] = args['state'][0]

    scout24Scraper = Scout24Scraper(request_timeout=10, name=name)
    scout24Scraper.scrape_ids(max_ids=amount, max_extra_wait=wait,
                            query=query)
    scout24Scraper.scrape(max_extra_wait=wait)
    scout24Scraper.save_data()
    scout24Scraper.download_main_images(max_extra_wait=wait)
    scout24Scraper.download_floor_plans(max_extra_wait=wait)

    for handler in scout24Scraper.logger.handlers:
        handler.close()
        scout24Scraper.logger.removeHandler(handler)


if __name__ == "__main__":
    main()