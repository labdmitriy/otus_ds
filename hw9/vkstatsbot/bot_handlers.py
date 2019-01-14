import re
from urllib.parse import urlparse

from constants import START_COMMAND_TEXT, NOT_A_VK_LINK, ERROR
from ml import get_all_user_info, predict_gender
from vk_api import VKApiConnector

from ml import ColumnExtractor, TextsAugmenter

vk_profile_link_validator = re.compile(r"^https?://(www.)?(m.)?(vkontakte.ru|vk.com)/.*$")
opt_fields = 'city,education,followers_count'

def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text=START_COMMAND_TEXT)


def text(bot, update):
    entered_text = update.message.text
    if not vk_profile_link_validator.match(entered_text):
        answer = NOT_A_VK_LINK
    else:
        parse_result = urlparse(entered_text)
        path = parse_result.path.lstrip('/')
        data = VKApiConnector.resolve_screen_name(path)
        if data is not None:
            if not data:
                answer = NOT_A_VK_LINK
            else:
                object_id = data['object_id']
                if data['type'] == 'group':
                    object_id = -object_id

                user_info_df = VKApiConnector.get_user_info(object_id, opt_fields)
                all_texts_df = VKApiConnector.get_wall(object_id)
                all_user_info_df = get_all_user_info(user_info_df, all_texts_df)

                predicted_value = predict_gender(all_user_info_df)

                answer = 'Female probability: {}%\nMale probability: {}%'.format(
                    predicted_value[0][0] * 100, predicted_value[0][1] * 100
                )

        else:
            answer = ERROR

    bot.send_message(chat_id=update.message.chat_id, text=answer)
