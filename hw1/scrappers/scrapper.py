import logging
import requests
from urllib.parse import urljoin
import copy
import json

from pandas import DataFrame
from pandas.io.json import json_normalize

from config import config

logger = logging.getLogger(__name__)


class Scrapper(object):
    """ Scrapper for gathering information about vacancies (HeadHunter) """

    def __init__(self):
        self.vacancies_url = config['vacancies']['url']
        self.headers = {'User-Agent': config['vacancies']['user_agent']}
        self.per_page_count = int(config['vacancies']['per_page_count'])

    def _search_vacancies(self, search_texts, search_params):
        """Search vacancies by search texts

        :param search_texts: list of texts to search
        :param search_params: parameters for searching
        :return: DataFrame with summary information about founded vacancies
        """
        logger.info('Search Vacancies')

        total_found_count = 0
        all_vacancies_df = DataFrame()

        for text in search_texts:
            logger.info('Search Text: {}'.format(text))

            # Get vacancies count founded by search text
            search_params['text'] = text
            search_params['per_page'] = 1
            res = requests.get(self.vacancies_url, params=search_params, headers=self.headers)
            res.raise_for_status()

            hh_df = DataFrame(res.json())
            if hh_df.size == 0:
                logger.info('No vacancies founded')
                continue

            found_count = hh_df.iloc[0]['found']
            logger.info('Found: {}'.format(found_count))
            total_found_count += found_count

            # Calculate pages count founded by search text
            pages_count = (found_count // self.per_page_count) + 1
            logger.info('Pages Count: {}'.format(pages_count))

            # Gather summary information about founded vacancies
            search_params['per_page'] = self.per_page_count
            for page_num in range(pages_count):
                logger.info('Page number: {}'.format(page_num))

                search_params['page'] = page_num
                res = requests.get(self.vacancies_url, params=search_params, headers=self.headers)
                res.raise_for_status()

                hh_df = DataFrame(res.json())
                vacancies_df = json_normalize(hh_df['items'])
                vacancies_df['search_text'] = text
                all_vacancies_df = all_vacancies_df.append(vacancies_df, ignore_index=True)

        logger.info('Total Found: {}'.format(total_found_count))

        return all_vacancies_df

    def _get_vacancies_info(self, vacancies_id):
        """Get detailed vacancies information by vacancies ids

        :param vacancies_id: list of vacancies ids
        :return: DataFrame with detailed vacancies information
        """
        logger.info('Get Vacancies Detail Info')

        total_found_count = 0
        vacancies_detail_df = DataFrame()

        # Get detail vacancies information by vacancies ids
        for vacancy_id in vacancies_id:
            vacancy_url = urljoin(self.vacancies_url, vacancy_id)

            res = requests.get(vacancy_url, headers=self.headers)
            res.raise_for_status()

            vacancies_detail_df = vacancies_detail_df.append(json_normalize(res.json()), ignore_index=True)
            total_found_count += 1

            if (total_found_count % 100) == 0:
                logger.info('Vacancies processed: {}'.format(total_found_count))

        logger.info('Total processed: {}'.format(total_found_count))

        return vacancies_detail_df

    def scrap_process(self, storage, search_texts, search_params):
        """Scrape information about vacancies and save it in storage

        :param storage: storage to use for writing gathered data
        :param search_texts: list of search text for searching
        :param search_params: parameters for searching
        """

        # You can iterate over ids, or get list of objects
        # from any API, or iterate throught pages of any site
        # Do not forget to skip already gathered data
        # Here is an example for you
        vacancies_df = self._search_vacancies(search_texts, search_params)
        list_storage = copy.deepcopy(storage)
        list_storage.file_name = './data/vacancies.json'
        list_storage.write_data(vacancies_df, orient='records')

        if vacancies_df.size == 0:
            logger.error('No vacancies founded')
            return

        # Get unique vacancies id
        vacancies_id = vacancies_df['id'].unique().astype(str)

        # Get detail info for vacancies
        vacancies_detail_df = self._get_vacancies_info(vacancies_id)

        # Write vacancies to json file
        storage.write_data(vacancies_detail_df, orient='records')