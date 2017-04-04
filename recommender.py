from flask import Flask
import numpy as np
import pandas as pd
import postgres
import sklearn

app = Flask(__name__)


def collaborative_recommend():
    db = postgres.DB()
    df = db.get_ratings()
    # convert id to categorical
    df['userId_coded'] = pd.Categorical(df.user_id).codes
    df['eventId_coded'] = pd.Categorical(df.event_id).codes
    n_events = df.eventId_coded.unique().shape[0]
    n_users = df.userId_coded.unique().shape[0]

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
    df_res = pd.DataFrame([], columns=['userId', "eventId", "rating"])
    for user_index in range(user_pred_k.shape[0]):
        user_id = df.loc[df['userId_coded'] == user_index].iloc[0].user_idN
        it = np.nditer(user_pred_k[0], flags=['f_index'])
        while not it.finished:
            event_id = df.loc[df['eventId_coded'] == it.index].iloc[0].event_id
            df_res.loc[df_res.shape[0]] = [int(user_id), int(event_id), it[0]]
            it.iternext()
        if user_index % 5 == 0:
            print(user_index)

    print("df transformed")
    db.save_ratings(df_res)


@app.route("/")
def hello():
    collaborative_recommend()
    return "done"


if __name__ == "__main__":
    app.run(host='0.0.0.0')
