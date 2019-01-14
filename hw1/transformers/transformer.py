import logging

import pandas as pd

logger = logging.getLogger(__name__)


class Transformer(object):
    """Transformer for the gathered data"""

    def transform(self, input_storage, output_storage, quotes_storage):
        """ Transform gathered data and save it to storage

        :param input_storage: storage to read data for transforming
        :param output_storage: storage to write transformed data
        :param quotes_storage: storage to read quotes data
        """

        # Get gathered information about vacancies and quotes for currencies vs RUR
        vacancies_detail_df = input_storage.read_data(orient='records')
        quotes_df = quotes_storage.read_data(orient='index')

        # Get subset of the columns from vacancies raw information
        selected_columns = ['id', 'name', 'created_at', 'area.name', 'key_skills', 'specializations',
                            'salary.from', 'salary.to', 'salary.currency', 'salary.gross',
                            'schedule.name', 'experience.name', 'employer.name']
        result_df = vacancies_detail_df[selected_columns].copy()

        # Merge vacancies and quotes info
        result_df = pd.merge(result_df, quotes_df, how='left', left_on='salary.currency', right_index=True)

        # Convert all salaries to russian rubles without considering whether salary is net or gross
        # due to different tax rates for different countries
        result_df['salary.from.rur'] = (result_df['salary.from'] * result_df['currency.rate'])
        result_df['salary.to.rur'] = (result_df['salary.to'] * result_df['currency.rate'])

        # Write transformed data to JSON
        output_storage.write_data(result_df, orient='records')
