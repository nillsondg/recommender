from flask import Flask
import numpy as np
import pandas as pd
import postgres
import sklearn
from collections import OrderedDict
import time

app = Flask(__name__)


def collaborative_recommend():
    start = time.time()
    db = postgres.DB()
    df = db.get_ratings()
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
    db.save_ratings(df_res)
    end = time.time()
    print(end - start)


@app.route("/")
def hello():
    collaborative_recommend()
    return "done"


if __name__ == "__main__":
    app.run(host='0.0.0.0')
