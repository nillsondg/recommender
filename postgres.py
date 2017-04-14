import psycopg2
import config
import pandas as pd


class DB:
    rating_query = "SELECT favorite_events.event_id, favorite_events.user_id, 1 AS \"ratings\", \
      (date_part('epoch'::text, (favorite_events.created_at)::timestamp with time zone))::integer AS \"timestamp\" \
    FROM favorite_events INNER JOIN events ON favorite_events.event_id = events.id \
    WHERE favorite_events.status = true \
    UNION \
    SELECT hidden_events.event_id, hidden_events.user_id, -1 AS \"ratings\", \
      (date_part('epoch'::text, (hidden_events.created_at)::timestamp with time zone))::integer AS \"timestamp\" \
    FROM hidden_events INNER JOIN events ON hidden_events.event_id = events.id \
    WHERE hidden_events.status = true;"

    description_query = "SELECT id, title, description FROM events;"

    @staticmethod
    def _get_connection():
        return psycopg2.connect(host="127.0.0.1", dbname=config.db_name, user=config.db_user, password=config.db_pwd)

    def get_ratings(self):
        cur = self._get_connection().cursor()
        cur.execute(self.rating_query)
        col_names = []
        for col in cur.description:
            col_names.append(col.name)
        df = pd.DataFrame(cur.fetchall(), columns=col_names)
        cur.close()
        return df

    def get_events_desc(self):
        cur = self._get_connection().cursor()
        cur.execute(self.description_query)
        col_names = []
        for col in cur.description:
            col_names.append(col.name)
        df = pd.DataFrame(cur.fetchall(), columns=col_names)
        cur.close()
        return df

    def save_ratings(self, df):
        cur = self._get_connection().cursor()
        cur.execute("BEGIN;")
        for row in df.itertuples():
            query = "INSERT INTO recommendations_events_collaborative(user_id, event_id, rating, updated_at) VALUES(" + \
                    str(row[1]) + "," + str(row[2]) + "," + str(row[3]) + ", now())" + \
                    "ON CONFLICT(user_id, event_id) DO UPDATE SET rating = " + str(row[3]) + ", updated_at = now();"
            cur.execute(query)
        cur.execute("COMMIT;")
        cur.close()
        print("df saved")

    def save_model_based_ratings(self, df):
        cur = self._get_connection().cursor()
        cur.execute("BEGIN;")
        for row in df.itertuples():
            query = "INSERT INTO recommendations_events_collaborative_model_based(user_id, event_id, rating, updated_at) VALUES(" + \
                    str(row[1]) + "," + str(row[2]) + "," + str(row[3]) + ", now())" + \
                    "ON CONFLICT(user_id, event_id) DO UPDATE SET rating = " + str(row[3]) + ", updated_at = now();"
            cur.execute(query)
        cur.execute("COMMIT;")
        cur.close()
        print("df saved")

    def save_content_based_ratings(self, df):
        cur = self._get_connection().cursor()
        cur.execute("BEGIN;")
        for row in df.itertuples():
            query = "INSERT INTO recommendations_events_content_based(user_id, event_id, rating, updated_at) VALUES(" + \
                    str(row[1]) + "," + str(row[2]) + "," + str(row[3]) + ", now())" + \
                    "ON CONFLICT(user_id, event_id) DO UPDATE SET rating = " + str(row[3]) + ", updated_at = now();"
            cur.execute(query)
        cur.execute("COMMIT;")
        cur.close()
        print("df saved")
