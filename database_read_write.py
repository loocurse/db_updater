from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from main import CONNECTION_PARAMS

engine = create_engine('postgresql://{}:{}@localhost:{}/{}'.format(CONNECTION_PARAMS['user'],
                                                                   CONNECTION_PARAMS['password'],
                                                                   CONNECTION_PARAMS['port'],
                                                                   CONNECTION_PARAMS['database']))


def read_all_db():
    """Reads the SQL database and outputs the dataframe with cols stated below"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM power_energy_consumption "
                       "WHERE date >= date_trunc('month', now()) - interval '6 month' AND "
                       "date < date_trunc('month', now())")
        results = cursor.fetchall()
    df = pd.DataFrame(results, columns=['date', 'time', 'unix_time', 'meter_id', 'user_id',
                                        'energy', 'power', 'device_state', 'device_type'])
    df['date'] = pd.to_datetime(df['date'])
    return df


def update_db(df, table_name, index_to_col=False):
    """Sends the information over to SQL"""
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating database <{table_name}>')
    df.to_sql(table_name, engine, if_exists='replace', index=index_to_col)


def read_cost_savings():
    """Extracts the cost savings graph and returns as a dataframe"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM costsavings_weeks")
        colnames = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
    return pd.DataFrame(results, columns=colnames)


def get_daily_table():
    """Extracts the achievements_daily table"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM achievements_daily")
        colnames = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
    df = pd.DataFrame(results, columns=colnames).set_index('week_day')
    return df


def get_weekly_table():
    """Extracts the achievements_weekly table"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM achievements_weekly")
        colnames = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
    return pd.DataFrame(results, columns=colnames)


def get_today():
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT MAX(date) AS max_date FROM power_energy_consumption")
        latest_date = cursor.fetchall()[0][0]
    return latest_date


def get_yesterday(): return get_today() - timedelta(days=1)


def get_energy_ytd_today(user_id):
    yesterday = get_yesterday().strftime('%Y-%m-%d')
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = f"SELECT * FROM power_energy_consumption WHERE date >= '{yesterday}' AND user_id = {user_id}"
        cursor.execute(query)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=colnames)
    return df
