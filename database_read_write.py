from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from datetime import datetime, timedelta


CONNECTION_PARAMS = dict(database='d53rn0nsdh7eok',
                         user='dadtkzpuzwfows',
                         password='1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410',
                         host='ec2-46-137-79-235.eu-west-1.compute.amazonaws.com',
                         port='5432')

engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(CONNECTION_PARAMS['user'],
                                                            CONNECTION_PARAMS['password'],
                                                            CONNECTION_PARAMS['host'],
                                                            CONNECTION_PARAMS['port'],
                                                            CONNECTION_PARAMS['database']))


def get_table_column(table_name):
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        colnames = [desc[0] for desc in cursor.description]
    return colnames


def read_all_db(user_id=None):
    """Reads the SQL database for the last 6 months and outputs the dataframe with cols stated below"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = "SELECT * FROM power_energy_consumption WHERE date >= date_trunc('hour', now()) - " \
                "interval '6 month' AND date < date_trunc('hour', now())"
        if user_id:
            query = "SELECT * FROM power_energy_consumption WHERE date >= date_trunc('hour', now()) - " \
                f"interval '6 month' AND date < date_trunc('hour', now()) AND user_id = {user_id}"
        cursor.execute(query)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=colnames)
    df['date'] = pd.to_datetime(df['date'])
    return df


def update_db(df, table_name, index_to_col=False):
    """Sends the information over to SQL"""
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating database <{table_name}>')
    print(df.head())
    assert sorted(get_table_column(table_name)) == sorted(
        list(df.columns)), "Table columns are not the same"
    input('Proceed?')
    df.to_sql(table_name, engine, if_exists='replace', index=index_to_col)


def read_cost_savings(user_id=None):
    """Extracts the cost savings graph and returns as a dataframe"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = "SELECT * FROM costsavings_weeks"
        if user_id:
            query = f"SELECT * FROM costsavings_weeks where user_id = {user_id}"
        cursor.execute(query)
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


def get_bonus_table():
    """Extracts the achievements_weekly table"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM achievements_bonus")
        colnames = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
    return pd.DataFrame(results, columns=colnames)


def get_today():
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT MAX(date) AS max_date FROM power_energy_consumption")
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


def get_cumulative_saving(user_id):
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = f"SELECT cum_savings FROM achievements_bonus WHERE user_id = {user_id}"
        cursor.execute(query)
        results = cursor.fetchone()[0]
    return results


def get_energy_points_wallet():
    """Read the database and return a dataframe of all the energy points from users"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = f"SELECT * FROM points_wallet"
        cursor.execute(query)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=colnames)
    return df


def get_presence(user_id):
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = f"SELECT * FROM presence where user_id = {user_id}"
        cursor.execute(query)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=colnames)
    return df


def get_schedules(user_id):
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = f"SELECT * FROM plug_mate_app_scheduledata where user_id = {user_id}"
        cursor.execute(query)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=colnames)
    return df


def custom_query(query):
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=colnames)
    return df