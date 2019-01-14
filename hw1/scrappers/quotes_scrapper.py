import logging
import requests

from pandas import DataFrame, Series

from config import config

logger = logging.getLogger(__name__)


class QuotesScrapper(object):
    """Scrapper fot gathering quotes information"""

    def __init__(self):
        self.quotes_url = config['quotes']['url']
        self.access_key = config['quotes']['access_key']

    def _get_quotes(self, currency_name):
        """Get quotes for currencies vs selected currency code

        :param currency_name: currency code for calculating quotes
        :return: DataFrame with quotes vs currency code
        """

        payload = {'access_key': self.access_key}

        # Get information about currency quotes vs USD and fix some information
        # currency codes and quotes
        quotes_usd = requests.get(self.quotes_url, params=payload)
        quotes_usd = Series(quotes_usd.json()['quotes'])
        quotes_usd.index = quotes_usd.index.str.slice(3)
        quotes_usd.index = quotes_usd.index.str.replace('RUB', 'RUR')

        # Calculate quotes vs selected currency
        quotes_curr = quotes_usd[currency_name] / quotes_usd
        quotes_curr['BYR'] *= 10000
        quotes_df = DataFrame(quotes_curr, columns=['currency.rate'])

        return quotes_df

    def scrap_process(self, storage, currency_name):
        """Scrape quotes data vs selected currency code and save in storage

        :param storage: storage for writing scraped code data
        :param currency_name: currency code for quotes scraping
        """

        # Get quotes for currencies vs RUR
        quotes_df = self._get_quotes(currency_name)
        storage.write_data(quotes_df, orient='index')
