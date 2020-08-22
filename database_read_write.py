from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import copy


DEBUGGING = False  # Turn on for debugging mode

# Read credentials
with open('credentials.json', 'r') as f:
    CONNECTION_PARAMS = json.load(f)

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
    if DEBUGGING:
        print(
            f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] [DEBUGGING] Table <{table_name}>')
        print(df.head())
        assert sorted(get_table_column(table_name)) == sorted(
            list(df.columns)), "Table columns are not the same"
    else:
        print(
            f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating database <{table_name}>')
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


def get_user_ids():
    """Returns a list of all the user_ids"""
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = "SELECT DISTINCT user_id FROM power_energy_consumption"
        cursor.execute(query)
        colnames = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
    return sorted([x[0] for x in results])


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


def load_notif_and_logs(achievement_type, connection):

    cursor = connection.cursor()
    cursor.execute(
        f"SELECT * FROM achievements_{achievement_type}")  # CHANGE ACCORDINGLY
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    df_achievements_info = pd.DataFrame(results, columns=colnames)

    user_ids = sorted(df_achievements_info['user_id'].unique())

    # START OF GETTING NOTIFICATIONS AND UPDATING.

    """Reads the SQL database for the entire output and outputs the dataframe with cols stated below"""
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM notifications")
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    """Reads the SQL database for the entire output and outputs the dataframe with cols stated below"""

    # Notifications stored on database
    sql_notif_df = pd.DataFrame(results, columns=colnames)
    print("User 2 notifications", sql_notif_df['notifications'][1])

    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM user_log")
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
    # Notifications stored on database

    # Initialise user_logs
    user_log_df = pd.DataFrame(results, columns=colnames)

    return [sql_notif_df, user_log_df, user_ids, df_achievements_info]


def notifications_update(achievement_type, achievements_list_to_update):
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Getting the {achievement_type} achievements status')
    start_time = datetime.now()
    # Connect to PostgreSQL database
    connection = psycopg2.connect(**CONNECTION_PARAMS)

    # Initialise data from csv
    all_notif_df = pd.read_csv('.\\tables_csv\\notifications.csv')
    # END OF INSERT SQL

    # Assign variables essential

    if achievement_type == 'daily':
        # Initialise dataframes from the database, Achievement list and Notifications.

        notif_and_logs_list = load_notif_and_logs(achievement_type, connection)
        sql_notif_df = notif_and_logs_list[0]
        user_log_df = notif_and_logs_list[1]
        user_ids = notif_and_logs_list[2]
        df_achievements_info = notif_and_logs_list[3]

        def converttoDictfromJsonString(jsonStr):
            return json.loads(jsonStr)

        sql_notif_df['notifications'] = sql_notif_df['notifications'].apply(
            converttoDictfromJsonString)

        userlog_DataFrame = pd.DataFrame(
            columns=['id', 'user_id', 'type', 'unix_time', 'description'])

        # Initialise Variables
        notificationsDataFrame = pd.DataFrame(
            columns=['id', 'user_id', 'notifications', 'seen'])
        listofDF = []

        unix_time_now = int(time.time())

        for user_id in user_ids:
            print('User ==> ', user_id)
            listofReturns = _check_update_notifications(unix_time_now,
                                                        df_achievements_info[df_achievements_info['user_id'] == user_id].reset_index(
                                                            drop=True),
                                                        user_id, sql_notif_df, all_notif_df, achievement_type, achievements_list_to_update, user_log_df)
            listofDF.append(listofReturns[0])  # for notifications
        notificationsDataFrame = pd.concat(listofDF)
        userlog_DataFrame = listofReturns[1]  # for logs
        userlog_DataFrame.reset_index(drop=True, inplace=True)

        notificationsDataFrame.reset_index(drop=True, inplace=True)

        # Test output before updating DB
        # conversion function:

        def dict2json(dictionary):
            return json.dumps(dictionary, ensure_ascii=False)
        # overwrite the dict column with json-strings
        notificationsDataFrame['notifications'] = notificationsDataFrame.notifications.map(
            dict2json)
        # notificationsDataFrame.to_csv("test_day_notif.csv")

        # userlog_DataFrame.to_csv("test_user_log_day.csv")

        connection.close()
        update_db(notificationsDataFrame,
                  'notifications', index_to_col=False)
        # update_db(userlog_DataFrame,
        #           'notifications', index_to_col=False)

    elif achievement_type == 'weekly':
        # Initialise dataframes from the database, Achievement list and Notifications.

        notif_and_logs_list = load_notif_and_logs(achievement_type, connection)
        sql_notif_df = notif_and_logs_list[0]
        user_log_df = notif_and_logs_list[1]
        user_ids = notif_and_logs_list[2]
        df_achievements_info = notif_and_logs_list[3]
        sql_notif_df['notifications'][0] = json.loads(
            sql_notif_df['notifications'][0])
        userlog_DataFrame = pd.DataFrame(
            columns=['id', 'user_id', 'type', 'unix_time', 'description'])

        notificationsDataFrame = pd.DataFrame(
            columns=['id', 'user_id', 'notifications', 'seen'])

        listofDF = []

        unix_time_now = int(time.time())

        today = get_today().strftime('%a')
        for user_id in user_ids:
            print('User ==> ', user_id)

            # FUNCTION TO GET UPDATED DF _check_update_notifications()

            listofReturns = _check_update_notifications(unix_time_now,
                                                        df_achievements_info[df_achievements_info['user_id'] == user_id].reset_index(
                                                            drop=True),
                                                        user_id, sql_notif_df, all_notif_df, achievement_type, achievements_list_to_update, user_log_df)
            listofDF.append(listofReturns[0])  # for notifications
        notificationsDataFrame = pd.concat(listofDF)
        userlog_DataFrame = listofReturns[1]  # for logs
        userlog_DataFrame.reset_index(drop=True, inplace=True)

        notificationsDataFrame.reset_index(drop=True, inplace=True)

        # Test output before updating DB
        # conversion function:

        def dict2json(dictionary):
            return json.dumps(dictionary, ensure_ascii=False)
        # overwrite the dict column with json-strings
        notificationsDataFrame['notifications'] = notificationsDataFrame.notifications.map(
            dict2json)
        # notificationsDataFrame.to_csv("test_week_notif.csv")

        # userlog_DataFrame.to_csv("test_user_log_week.csv")

        connection.close()
        update_db(notificationsDataFrame,
                  'notifications', index_to_col=False)
        update_db(userlog_DataFrame,
                  'notifications', index_to_col=False)

    elif achievement_type == 'bonus':
        # Initialise dataframes from the database, Achievement list and Notifications.

        notif_and_logs_list = load_notif_and_logs(achievement_type, connection)
        sql_notif_df = notif_and_logs_list[0]
        user_log_df = notif_and_logs_list[1]
        user_ids = notif_and_logs_list[2]
        df_achievements_info = notif_and_logs_list[3]
        sql_notif_df['notifications'][0] = json.loads(
            sql_notif_df['notifications'][0])

        userlog_DataFrame = pd.DataFrame(
            columns=['id', 'user_id', 'type', 'unix_time', 'description'])

        notificationsDataFrame = pd.DataFrame(
            columns=['id', 'user_id', 'notifications', 'seen'])
        listofDF = []

        unix_time_now = int(time.time())
        today = get_today().strftime('%a')

        for user_id in user_ids:
            print('User ==> ', user_id)
            listofReturns = _check_update_notifications(unix_time_now,
                                                        df_achievements_info[df_achievements_info['user_id'] == user_id].reset_index(
                                                            drop=True),
                                                        user_id, sql_notif_df, all_notif_df, achievement_type, achievements_list_to_update, user_log_df)
            listofDF.append(listofReturns[0])  # for notifications
        notificationsDataFrame = pd.concat(listofDF)
        userlog_DataFrame = listofReturns[1]  # for logs
        userlog_DataFrame.reset_index(drop=True, inplace=True)

        notificationsDataFrame.reset_index(drop=True, inplace=True)

        # Test output before updating DB
        # conversion function:

        def dict2json(dictionary):
            return json.dumps(dictionary, ensure_ascii=False)
        # overwrite the dict column with json-strings
        notificationsDataFrame['notifications'] = notificationsDataFrame.notifications.map(
            dict2json)
        # notificationsDataFrame.to_csv("test_bonus_notif.csv")

        # userlog_DataFrame.to_csv("test_user_log_bonus.csv")

        connection.close()
        update_db(notificationsDataFrame,
                  'notifications', index_to_col=False)
        update_db(userlog_DataFrame,
                  'notifications', index_to_col=False)


def _check_update_notifications(unix_time_now, df, user_id, sql_notif_df, all_notif_df, achievement_type, achievements_list_to_update, user_log_df):
    # achievement_titles 1 2 and 3 hard coded.
    '''DAILY'''
    if achievement_type == 'daily':

        NewDict = {}
        today = get_today().strftime('%a')

        datetime_now = datetime.now()
        try:
            datetime_now = datetime_now.strftime(
                "%-d %B %Y, %A")  # 19 August 2020, Wednesday
        except ValueError:
            datetime_now = datetime_now.strftime(
                "%#d %B %Y, %A")  # 19 August 2020, Wednesday

        # Only keep today's achievement's status
        mask = (df['week_day'] == today)
        df = df.loc[mask]
        df.reset_index(drop=True, inplace=True)
        sql_notif_df = sql_notif_df.loc[sql_notif_df['user_id'] == user_id]
        sql_notif_df.reset_index(drop=True, inplace=True)

        for col in df.columns:
            # Replace _allNotifDict to csv table.
            # achievements_list_to_update

            # Every Day
            if col in achievements_list_to_update:  # End of day mark

                _achievementName = col  # name of the achievement
                # _achievementType = 'daily'  # Changes depending on achivement type! Important
                if df[col][0] > 0:
                    _messageType = "success"

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')

                    print("SUCCESS", _messageText)
                    # _messageText = _allNotifDict[_achievementType][_messageType]
                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': _messageType})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.

                    # For logs
                    # cols = 'id','user_id','type','unix_time', 'description'
                    log_description = all_notif_df.loc[all_notif_df['achievement']
                                                       == col]['description'].reset_index(drop=True)[0].replace('\t', '')
                    newList = [0, user_id, 'achievement',
                               unix_time_now, log_description]
                    user_log_df_len = len(user_log_df)
                    user_log_df.loc[user_log_df_len] = newList

                if df[col][0] == 0:
                    _messageType = "failure"
                    # _messageText = _allNotifDict[_achievementType][_messageType]

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')
                    print("FAILURE", _messageText)

                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': "warning"})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.

                else:
                    print("Something wrong with your condition")

            # At 12am
            elif col in achievements_list_to_update:  # 12 AM mark
                _achievementName = col  # name of the achievement
                # _achievementType = 'daily'  # Changes depending on achivement type! Important
                if df[col][0] > 0:
                    _messageType = "success"

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')

                    print("SUCCESS", _messageText)
                    # _messageText = _allNotifDict[_achievementType][_messageType]
                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': _messageType})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.

                    # For logs
                    # cols = 'id','user_id','type','unix_time', 'description'
                    log_description = all_notif_df.loc[all_notif_df['achievement']
                                                       == col]['description'].reset_index(drop=True)[0].replace('\t', '')
                    newList = [0, user_id, 'achievement',
                               unix_time_now, log_description]
                    user_log_df_len = len(user_log_df)
                    user_log_df.loc[user_log_df_len] = newList

                if df[col][0] == 0:
                    _messageType = "failure"
                    # _messageText = _allNotifDict[_achievementType][_messageType]

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')
                    print("FAILURE", _messageText)

                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': "warning"})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.
                else:
                    print("Something wrong with your condition")
            # Every 15 min
            elif col in achievements_list_to_update:  # 15 min mark
                _achievementName = col  # name of the achievement
                # _achievementType = 'daily'  # Changes depending on achivement type! Important
                if df[col][0] > 0:
                    _messageType = "success"

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')

                    print("SUCCESS", _messageText)
                    # _messageText = _allNotifDict[_achievementType][_messageType]
                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': _messageType})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.

                    # For logs
                    # cols = 'id','user_id','type','unix_time', 'description'
                    log_description = all_notif_df.loc[all_notif_df['achievement']
                                                       == col]['description'].reset_index(drop=True)[0].replace('\t', '')
                    newList = [0, user_id, 'achievement',
                               unix_time_now, log_description]
                    user_log_df_len = len(user_log_df)
                    user_log_df.loc[user_log_df_len] = newList

                if df[col][0] == 0:
                    _messageType = "failure"
                    # _messageText = _allNotifDict[_achievementType][_messageType]

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')
                    print("FAILURE", _messageText)

                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': "warning"})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.
                else:
                    print("Something wrong with your condition")

    elif achievement_type == "weekly":
        NewDict = {}
        now_time = time.localtime()

        today = get_today().strftime('%a')

        datetime_now = datetime.now()
        try:
            datetime_now = datetime_now.strftime(
                "%-d %B %Y, %A")  # 19 August 2020, Wednesday
        except ValueError:
            datetime_now = datetime_now.strftime(
                "%#d %B %Y, %A")  # 19 August 2020, Wednesday

        sql_notif_df = sql_notif_df.loc[sql_notif_df['user_id'] == user_id]
        sql_notif_df.reset_index(drop=True, inplace=True)

        for col in df.columns:
            print("Checking ", col)
            # Replace _allNotifDict to csv table.
            # achievements_list_to_update

            # Every Sunday
            if col in achievements_list_to_update:  # End of day mark

                _achievementName = col  # name of the achievement
                # _achievementType = 'daily'  # Changes depending on achivement type! Important
                if df[col][0] > 0:
                    _messageType = "success"

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')

                    print("SUCCESS", _messageText)
                    # _messageText = _allNotifDict[_achievementType][_messageType]
                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': _messageType})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.
                    # For logs
                    # cols = 'id','user_id','type','unix_time', 'description'
                    log_description = all_notif_df.loc[all_notif_df['achievement']
                                                       == col]['description'].reset_index(drop=True)[0].replace('\t', '')
                    newList = [0, user_id, 'achievement',
                               unix_time_now, log_description]
                    user_log_df_len = len(user_log_df)
                    user_log_df.loc[user_log_df_len] = newList

                if df[col][0] == 0:
                    _messageType = "failure"
                    # _messageText = _allNotifDict[_achievementType][_messageType]

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')
                    print("FAILURE", _messageText)

                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': "warning"})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))

                    # Append new dict to table.notifications col (list) on database.
                else:
                    print("Something wrong with your condition")

            # Every Friday
            elif col in achievements_list_to_update:
                _achievementName = col  # name of the achievement
                # _achievementType = 'daily'  # Changes depending on achivement type! Important
                if df[col][0] > 0:
                    _messageType = "success"

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')

                    print("SUCCESS", _messageText)
                    # _messageText = _allNotifDict[_achievementType][_messageType]
                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': _messageType})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))

                    # Append new dict to table.notifications col (list) on database.

                    # For logs
                    # cols = 'id','user_id','type','unix_time', 'description'
                    log_description = all_notif_df.loc[all_notif_df['achievement']
                                                       == col]['description'].reset_index(drop=True)[0].replace('\t', '')
                    newList = [0, user_id, 'achievement',
                               unix_time_now, log_description]
                    user_log_df_len = len(user_log_df)
                    user_log_df.loc[user_log_df_len] = newList

                if df[col][0] == 0:
                    _messageType = "failure"
                    # _messageText = _allNotifDict[_achievementType][_messageType]

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')
                    print("FAILURE", _messageText)

                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': "warning"})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))

                    # Append new dict to table.notifications col (list) on database.
                else:
                    print("Something wrong with your condition")

    elif achievement_type == 'bonus':
        NewDict = {}
        now_time = time.localtime()
        today = get_today().strftime('%a')

        datetime_now = datetime.now()
        try:
            datetime_now = datetime_now.strftime(
                "%-d %B %Y, %A")  # 19 August 2020, Wednesday
        except ValueError:
            datetime_now = datetime_now.strftime(
                "%#d %B %Y, %A")  # 19 August 2020, Wednesday

        sql_notif_df = sql_notif_df.loc[sql_notif_df['user_id'] == user_id]
        sql_notif_df.reset_index(drop=True, inplace=True)

        for col in df.columns:
            # print("Checking ", col)
            # Replace _allNotifDict to csv table.
            # achievements_list_to_update

            # Every Day
            if col in achievements_list_to_update:  # End of day mark

                _achievementName = col  # name of the achievement
                # _achievementType = 'daily'  # Changes depending on achivement type! Important
                if df[col][0] > 0:
                    _messageType = "success"

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')

                    print("SUCCESS", _messageText)
                    # _messageText = _allNotifDict[_achievementType][_messageType]
                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': _messageType})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.

                    # For logs
                    # cols = 'id','user_id','type','unix_time', 'description'
                    log_description = all_notif_df.loc[all_notif_df['achievement']
                                                       == col]['description'].reset_index(drop=True)[0].replace('\t', '')
                    newList = [0, user_id, 'achievement',
                               unix_time_now, log_description]
                    user_log_df_len = len(user_log_df)
                    user_log_df.loc[user_log_df_len] = newList

                if df[col][0] == 0:
                    _messageType = "failure"
                    # _messageText = _allNotifDict[_achievementType][_messageType]

                    _messageText = all_notif_df.loc[all_notif_df['achievement']
                                                    == col][_messageType].reset_index(drop=True)[0].replace('\t', '')
                    print("FAILURE", _messageText)

                    NewDict.update({'timestamp': datetime_now})
                    NewDict.update({'message': _messageText})
                    NewDict.update({'type': "warning"})
                    NewDict.update({'seen': 0})

                    sql_notif_df['notifications'][0]['notifications'].append(
                        copy.deepcopy(NewDict))
                    # Append new dict to table.notifications col (list) on database.
                else:
                    print("Something wrong with your condition")

    return [sql_notif_df, user_log_df]


def get_presence_states(user_id):
    connection = psycopg2.connect(**CONNECTION_PARAMS)
    with connection.cursor() as cursor:
        query = f"SELECT * FROM plug_mate_app_presencedata where user_id = {user_id}"
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


# # IGNORE, FOR TESTING ONLY
# x = 2
# if x == 1:
#     to_update = [
#         'lower_energy_con',
#         'turn_off_end',
#         'complete_all_daily',
#         'tree_first',
#         'tree_fifth',
#         'tree_tenth',
#         'redeem_reward',
#         'first_remote',
#         'first_schedule',
#         'cum_savings'
#     ]

#     notifications_update('daily', to_update)
#     # notifications_update('bonus', to_update)

# elif x == 2:
#     to_update = [
#         'turn_off_leave'
#     ]
#     notifications_update('daily', to_update)

# elif x == 3:
#     to_update = [
#         'cost_saving',
#         'schedule_based',
#         'complete_weekly',
#         'cum_savings',
#     ]

#     notifications_update('weekly', to_update)
