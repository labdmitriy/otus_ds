import os
import json

import pandas as pd
from pandas.io.json import json_normalize

from storages.storage import Storage


class JSONStorage(Storage):
    """Storage for operations between JSON file and DataFrame"""

    def __init__(self, file_name):
        self.file_name = file_name

    def read_data(self, orient=None, normalize=False, record_path=None):
        """
        :param orient: Indication of expected JSON string format
        :param normalize: Indication of necessary JSON normalization
        :param record_path: Record path for the JSON nested structure for normalization
        :return: DataFrame created from json file
        """

        if not os.path.exists(self.file_name):
            raise FileNotFoundError

        if normalize:
            with open(self.file_name) as f:
                json_data = f.read()

            data_frame = json_normalize(json.loads(json_data), record_path=record_path)
        else:
            data_frame = pd.read_json(self.file_name, orient=orient)

        return data_frame

    def write_data(self, data_frame, orient=None):
        """
        :param data_frame: DataFrame to write as json_file
        :param orient: Indication of expected JSON string format
        """

        data_frame.to_json(self.file_name, orient=orient)

    def append_data(self, data_frame):
        # append_data abstract method was not implemented

        raise NotImplementedError

