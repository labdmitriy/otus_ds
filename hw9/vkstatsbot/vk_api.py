import logging
from time import sleep

import requests

import pandas as pd
from pandas import DataFrame



logger = logging.getLogger(__name__)

num_columns = ['followers_count']
cat_columns = ['city.title', 'faculty', 'graduation', 'university']
text_column = 'text'
columns = num_columns + cat_columns + [text_column]

class VKApiConnector(object):
    __base_url = "https://api.vk.com/method/"
    __v = 5.78
    __resolve_screen_name_method = "utils.resolveScreenName"
    __users_get_method = 'users.get'
    __wall_get_method = "wall.get"
    __token = NotImplemented
    __client_id = NotImplemented
    __sleep_time = NotImplemented

    @classmethod
    def config(cls, version, client_id, token, sleep_time=0.5):
        cls.version = version
        cls.__sleep_time = sleep_time
        cls.__client_id = client_id
        cls.__token = token

    @classmethod
    def __get_base_params(cls):
        return {
            'v': cls.__v,
            'client_id': cls.__client_id,
            'access_token': cls.__token
        }

    @classmethod
    def resolve_screen_name(cls, screen_name):
        try:
            logger.info("Access {} method".format(cls.__resolve_screen_name_method))
            request_params = cls.__get_base_params()
            request_params['screen_name'] = screen_name

            url = '{}{}'.format(cls.__base_url, cls.__resolve_screen_name_method)
            response = requests.post(url, request_params)

            if not response.ok:
                logger.error(response.text)
                return

            return response.json()['response']
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def get_user_info(cls, user_id, fields):
        try:
            logger.info("Access {} method".format(cls.__users_get_method))

            request_params = cls.__get_base_params()
            request_params['user_ids'] = user_id
            request_params['fields'] = fields

            url = '{}{}'.format(cls.__base_url, cls.__users_get_method)
            response = requests.post(url, request_params)

            if not response.ok:
                logger.error(response.text)
                return

            response_df = pd.io.json.json_normalize(response.json()['response'])
            user_info_df = DataFrame(columns=columns)
            user_info_df = user_info_df.append(response_df, ignore_index=True,
                                               sort=False)

            return user_info_df

        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def get_wall(cls, owner_id):
        try:
            logger.info("Access {} method".format(cls.__wall_get_method))

            wall_count = 100
            posts_count = 0
            offset = 0

            request_params = cls.__get_base_params()
            request_params['owner_id'] = owner_id
            request_params['count'] = wall_count
            request_params['filter'] = 'owner'
            request_params['offset'] = offset

            url = '{}{}'.format(cls.__base_url, cls.__wall_get_method)
            response = requests.post(url, request_params)

            if not response.ok:
                logger.error(response.text)
                return

            total_wall_count = response.json()['response']['count']
            posts = response.json()['response']['items']

            logger.info(
                'user_id: {}, # posts: {}'.format(owner_id, total_wall_count))

            if not posts:
                all_texts_df = DataFrame({'user_id': [owner_id], 'text': ['']})
                return all_texts_df

            texts = pd.io.json.json_normalize(posts)['text']
            texts_df = DataFrame({'user_id': owner_id, 'text': texts})

            all_texts_df = DataFrame(columns=['user_id', 'text'])
            all_texts_df = all_texts_df.append(texts_df, ignore_index=True)
            posts_count += len(texts)

            while True:
                sleep(cls.__sleep_time)
                offset += wall_count
                request_params['offset'] = offset

                response = requests.post(url, request_params)

                posts = response.json()['response']['items']

                if not posts:
                    break

                texts = pd.io.json.json_normalize(posts)['text']
                texts_df = DataFrame({'user_id': owner_id, 'text': texts})
                all_texts_df = all_texts_df.append(texts_df, ignore_index=True)
                posts_count += len(texts)

            return all_texts_df

        except Exception as ex:
            logger.exception(ex)



