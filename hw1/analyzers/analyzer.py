import logging
from tabulate import tabulate

from pandas import DataFrame

logger = logging.getLogger(__name__)


class Analyzer(object):
    """Analyzer for transformed data"""

    def _pretty_print(self, data_frame, title=''):
        """Pretty print DataFrame to console

        :param data_frame: DataFrame to print
        :param title: title to print before printing DataFrame
        """
        print('\n')
        print(title)
        print(tabulate(data_frame, headers='keys', tablefmt='fancy_grid'))

    def analyze(self, storage, top_n=10):
        """Output statistics based on transformed data

        :param storage: storage to read data for analyzing
        :param top_n: number of top results
        """
        result_df = storage.read_data(orient='records')

        # Output vacancies count
        print()
        count_stats = result_df.shape[0]
        print('Vacancies count')
        print(count_stats)

        # Output vacancies period
        date_stats = DataFrame(result_df['created_at'].aggregate(['min', 'max'])).astype(str)
        self._pretty_print(date_stats, 'Vacancies period')

        # Output vacancies by cities
        city_stats = DataFrame(result_df['area.name'].value_counts()[:top_n])
        self._pretty_print(city_stats, 'Top cities with vacancies')

        # Output key skills information from nested json structure
        key_skills_df = storage.read_data(normalize=True, record_path='key_skills')
        key_skills_stats = DataFrame(key_skills_df['name'].str.lower().value_counts().head(top_n))
        self._pretty_print(key_skills_stats, 'Top key skills for vacancies')

        # Output schedule information
        schedule_stats = DataFrame(result_df['schedule.name'].value_counts())
        self._pretty_print(schedule_stats, 'Schedule distribution')

        # Output experience information
        experience_stats = DataFrame(result_df['experience.name'].value_counts())
        self._pretty_print(experience_stats, 'Required experience distribution')

        # Output employer information
        employer_stats = DataFrame(result_df['employer.name'].value_counts().head(top_n))
        self._pretty_print(employer_stats, 'Top companies with vacancies')

        # Output specializations information form nested json structure
        spec_df = storage.read_data(normalize=True, record_path='specializations')
        spec_stats = DataFrame(spec_df['name'].value_counts().head(top_n))
        self._pretty_print(spec_stats, 'Top specializations with vacancies')

        # Output salaries stats regardless of whether salary is net or gross
        # due to different tax rates in different countries
        salary_stats = result_df.aggregate({'salary.from.rur': ['min', 'max', 'mean', 'std'],
                                            'salary.to.rur': ['min', 'max', 'mean', 'std']})
        self._pretty_print(salary_stats, 'Vacancies salary distribution in russian rubles')
