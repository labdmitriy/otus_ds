import pandas as pd
from pandas import DataFrame
import numpy as np

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
import pickle


id_column = 'user_id'
text_column = 'text'


class ColumnExtractor(BaseEstimator, TransformerMixin):
    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X[self.columns]


class TextsAugmenter(BaseEstimator, TransformerMixin):
    def __init__(self, user_column, text_column):
        self.user_column = user_column
        self.text_column = text_column

    @staticmethod
    def _augment_user_texts(texts):
        return ' '.join(texts.tolist())

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        augmented_texts = DataFrame(X.groupby(self.user_column)[self.text_column]
                                     .apply(self._augment_user_texts))

        return augmented_texts


class CategoricalImputer(BaseEstimator, TransformerMixin):
    def __init__(self, label=None):
        self.label = label

    def fit(self, X, y=None):
        if self.label:
            self.fill_values_ = self.label
        else:
            self.fill_values_ = X.mode().iloc[0]
        return self

    def transform(self, X):
        return X.fillna(self.fill_values_).values


def augment_user_texts(texts_df):
    text_pipeline = Pipeline([
        ('select columns', ColumnExtractor([id_column, text_column])),
        ('augment user texts', TextsAugmenter(id_column, text_column))
    ])

    user_text = text_pipeline.fit_transform(texts_df)['text'].values
    return user_text


def get_all_user_info(user_info_df, texts_df):
    user_text = augment_user_texts(texts_df)
    user_info_df.loc[0, 'text'] = user_text

    return user_info_df


def predict_gender(user_info_df):
    clf = pickle.load(open('./models/best_model.pkl', 'rb'))
    predicted_probs = np.round(clf.predict_proba(user_info_df), 2).tolist()

    return predicted_probs




