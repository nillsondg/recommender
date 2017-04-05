import psycopg2
import config
import pandas as pd


class DB:
    rating_query = "SELECT favorite_events.event_id, favorite_events.user_id, 1 AS \"ratings\", \
      (date_part('epoch'::text, (favorite_events.created_at)::timestamp with time zone))::integer AS \"timestamp\" \
    FROM favorite_events INNER JOIN events ON favorite_events.event_id = events.id \
    UNION \
    SELECT hidden_events.event_id, hidden_events.user_id, -1 AS \"ratings\", \
      (date_part('epoch'::text, (hidden_events.created_at)::timestamp with time zone))::integer AS \"timestamp\" \
    FROM hidden_events INNER JOIN events ON hidden_events.event_id = events.id;"

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

    def save_ratings(self, df):
        cur = self._get_connection().cursor()
        cur.execute("BEGIN;")
        for row in df.itertuples():
            query = "INSERT INTO recommendations_events_collaborative(user_id, event_id, rating) VALUES(" + \
                    str(row[1]) + "," + str(row[2]) + "," + str(row[3]) + ")" + \
                     "ON CONFLICT(user_id, event_id) DO UPDATE SET rating = " + str(row[3]) + ";"
            cur.execute(query)
        cur.execute("COMMIT;")
        cur.close()
        print("df saved")
