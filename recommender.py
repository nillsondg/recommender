from flask import Flask
import numpy as np
import pandas as pd
import postgres
import sklearn
from collections import OrderedDict
import time
import scipy.sparse as sp
from scipy.sparse.linalg import svds
from sklearn.metrics import mean_squared_error
from math import sqrt
from sklearn.feature_extraction.text import CountVectorizer
import nltk


app = Flask(__name__)


def collaborative_recommend(df):
    print("start calc")
    start = time.time()
    # convert id to categorical
    df['userId_coded'] = pd.Categorical(df.user_id).codes
    df['eventId_coded'] = pd.Categorical(df.event_id).codes
    n_events = df.eventId_coded.unique().shape[0]
    n_users = df.userId_coded.unique().shape[0]

    events_labels = dict(zip(df.eventId_coded, df.event_id))
    users_labels = dict(zip(df.userId_coded, df.user_id))

    # Create two user-item matrices, one for training and another for testing
    train_data_matrix = np.zeros((n_users, n_events))
    for line in df.itertuples():
        train_data_matrix[int(line[5]) - 1, int(line[6]) - 1] = float(line[3])

    k = 20
    from sklearn.neighbors import NearestNeighbors
    neigh = NearestNeighbors(k, algorithm='brute', metric='cosine')
    neigh.fit(train_data_matrix)
    top_k_distances, top_k_users = neigh.kneighbors(train_data_matrix, return_distance=True)

    user_pred_k = np.zeros(train_data_matrix.shape)
    for i in range(train_data_matrix.shape[0]):
        divider = np.array([np.abs(top_k_distances[i].T).sum()])
        if divider != 0:
            user_pred_k[i, :] = top_k_distances[i].T.dot(train_data_matrix[top_k_users][i]) / divider
        else:
            user_pred_k[i, :] = 0

    print(user_pred_k.shape)
    print("ratings predicted")

    n = user_pred_k.shape[0] * user_pred_k.shape[1]
    users = np.empty(n, dtype=int)
    events = np.empty(n, dtype=int)
    ratings = np.empty(n, dtype=float)
    hor_n = user_pred_k.shape[1]
    for user_index in range(user_pred_k.shape[0]):
        for event_index in range(user_pred_k.shape[1]):
            current_index = user_index * hor_n + event_index
            rating = user_pred_k[user_index, event_index]
            users[current_index] = users_labels[user_index]
            events[current_index] = events_labels[event_index]
            ratings[current_index] = rating

        if user_index % 50 == 0:
            print(user_index)
    data = OrderedDict()
    data["userId"] = users
    data["eventId"] = events
    data["rating"] = ratings
    df_res = pd.DataFrame(data)

    print(df_res.shape)
    print("df transformed")
    end = time.time()
    print(end - start)
    print("rmse user based", get_rmse(user_pred_k, train_data_matrix))
    return df_res


def model_based(df):
    print("start calc")
    start = time.time()
    df['userId_coded'] = pd.Categorical(df.user_id).codes
    df['eventId_coded'] = pd.Categorical(df.event_id).codes
    n_events = df.eventId_coded.unique().shape[0]
    n_users = df.userId_coded.unique().shape[0]

    events_labels = dict(zip(df.eventId_coded, df.event_id))
    users_labels = dict(zip(df.userId_coded, df.user_id))

    # Create two user-item matrices, one for training and another for testing
    train_data_matrix = np.zeros((n_users, n_events))
    for line in df.itertuples():
        train_data_matrix[int(line[5]) - 1, int(line[6]) - 1] = float(line[3])

    # get SVD components from train matrix. Choose k.
    u, s, vt = svds(train_data_matrix, k=20)
    s_diag_matrix = np.diag(s)
    x_pred = np.dot(np.dot(u, s_diag_matrix), vt)

    n = x_pred.shape[0] * x_pred.shape[1]
    users = np.empty(n, dtype=int)
    events = np.empty(n, dtype=int)
    ratings = np.empty(n, dtype=float)
    hor_n = x_pred.shape[1]
    for user_index in range(x_pred.shape[0]):
        for event_index in range(x_pred.shape[1]):
            current_index = user_index * hor_n + event_index
            rating = x_pred[user_index, event_index]
            users[current_index] = users_labels[user_index]
            events[current_index] = events_labels[event_index]
            ratings[current_index] = rating

        if user_index % 50 == 0:
            print(user_index)
    data = OrderedDict()
    data["userId"] = users
    data["eventId"] = events
    data["rating"] = ratings
    df_res = pd.DataFrame(data)

    print(df_res.shape)
    print("calculated")
    end = time.time()
    print("calc time", end - start)
    print("rmse model", get_rmse(x_pred, train_data_matrix))
    return df_res


class StemmedCountVectorizer(CountVectorizer):
    def __init__(self, **kv):
        super(StemmedCountVectorizer, self).__init__(**kv)
        self._stemmer = nltk.stem.snowball.RussianStemmer()
    def build_analyzer(self):
        analyzer = super(StemmedCountVectorizer, self).build_analyzer()
        return lambda doc: (self._stemmer.stem(w) for w in analyzer(doc))


def content_based(df_ratings, df_events):
    print("start calc")
    start = time.time()

    # remove items, that not in ratings (temporary)
    df_events = df_events[df_events.id.isin(df_ratings.event_id)]
    # cause nullable description
    df_ratings = df_ratings[df_ratings.event_id.isin(df_events.id)]

    # TODO fix empty string in CountVectorizer
    from stop_words import get_stop_words
    stop_words = get_stop_words('ru')
    vectorizer = StemmedCountVectorizer(
        min_df=1,
        token_pattern=r'[ЁАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдежзийклмнопрстуфхцчшщъыьэюяё]{4,}',
        max_features=2500,
        stop_words=stop_words
    )
    # vectorizer = TfidfVectorizer(max_features=2500, ngram_range=(0, 3), sublinear_tf=True, stop_words=stop_words)
    x = vectorizer.fit_transform(df_events['description'])
    itemprof = x.todense()

    ratmat = df_ratings.pivot(index='user_id', columns='event_id', values='ratings').fillna(0)

    from scipy import linalg, dot
    userprof = dot(ratmat, itemprof) / linalg.norm(ratmat) / linalg.norm(itemprof)
    import sklearn.metrics
    similarity_calc = sklearn.metrics.pairwise.cosine_similarity(userprof, itemprof, dense_output=True)
    events_labels = dict(zip(range(len(ratmat.columns.values)), ratmat.columns.values))
    users_labels = dict(zip(range(len(ratmat.index.values)), ratmat.index.values))

    from collections import OrderedDict
    n = similarity_calc.shape[0] * similarity_calc.shape[1]
    users = np.empty(n, dtype=int)
    events = np.empty(n, dtype=int)
    ratings = np.empty(n, dtype=float)
    hor_n = similarity_calc.shape[1]
    for user_index in range(similarity_calc.shape[0]):
        for event_index in range(similarity_calc.shape[1]):
            current_index = (user_index - 1) * hor_n + event_index
            rating = similarity_calc[user_index, event_index]
            users[current_index] = users_labels[user_index]
            events[current_index] = events_labels[event_index]
            ratings[current_index] = rating

        if user_index % 50 == 0:
            print(user_index)
    data = OrderedDict()
    data["userId"] = users
    data["eventId"] = events
    data["rating"] = ratings
    df_res = pd.DataFrame(data)

    print(df_res.shape)
    print("calculated")
    end = time.time()
    print("calc time", end - start)
    print("rmse content", get_rmse(similarity_calc, ratmat.as_matrix()))
    return df_res


def get_rmse(pred, actual):
    # Ignore nonzero terms.
    pred = pred[actual.nonzero()].flatten()
    actual = actual[actual.nonzero()].flatten()
    return sqrt(mean_squared_error(pred, actual))


@app.route("/")
def hello():
    db = postgres.DB()
    df = db.get_ratings()
    df_desc = db.get_events_desc()

    db.save_ratings(collaborative_recommend(df))
    db.save_model_based_ratings(model_based(df))
    db.save_content_based_ratings(content_based(df, df_desc))

    return "done"


if __name__ == "__main__":
    app.run(host='0.0.0.0')
