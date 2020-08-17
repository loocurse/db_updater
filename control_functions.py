import psycopg2
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time
import numpy as np


def check_remote_control():
    """
    Checks the database for the device state and detect if remote control is activated.
    Checking frequency: 5 seconds
    """

    # Database and Fibaro credentials
    user = 'dadtkzpuzwfows'
    database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    port = '5432'
    database = 'd53rn0nsdh7eok'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'

    def activate_remote_control(meter_id, command):
        query = requests.post('http://{}/api/devices/{}/action/{}'.format(fibaro_address, meter_id, command),
                             auth=HTTPBasicAuth(fibaro_username, fibaro_password)).json()
        return None

    # Obtain state of all devices from database
    last_recorded_settings = pd.read_csv(
        'tables_csv/remote_control_setting.csv')

    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(user=user, password=database_password, host=host,
                                      port=port, database=database)
        cursor = connection.cursor()

        cursor.execute("SELECT * FROM plug_mate_app_remotedata ORDER BY id")
        query_result = cursor.fetchall()
        latest_settings = pd.DataFrame(
            query_result, columns=[desc[0] for desc in cursor.description])

        # Check state of devices from database with csv file
        diff = [(latest_settings.loc[i, 'user_id'], latest_settings.loc[i, 'device_type'],
                 latest_settings.loc[i, 'device_state'], last_recorded_settings.loc[i, 'device_state'])
                for i in range(len(latest_settings))
                if latest_settings.loc[i, 'device_state'] != last_recorded_settings.loc[i, 'device_state']]

        if len(diff) != 0:
            # Identify meter id based on device type and user id and switch it ON/OFF
            for user_id, device_type, new_state, previous_state in diff:
                if device_type == 'Task Lamp':
                    device_type = 'tasklamp'

                cursor.execute("SELECT meter_id FROM power_energy_consumption WHERE user_id={} AND device_type='{}' "
                               "ORDER BY unix_time DESC LIMIT 1".format(user_id, device_type.lower()))
                meter_id = cursor.fetchone()[0]

                if new_state is np.bool_(True) and previous_state is np.bool_(False):
                    activate_remote_control(meter_id, 'turnOn')
                elif new_state is np.bool_(False) and previous_state is np.bool_(True):
                    activate_remote_control(meter_id, 'turnOff')
                else:
                    raise ValueError('New State: {} | Previous state: {} | Meter id: {}'.format(
                        new_state, previous_state, meter_id))

            # Then update CSV file
            latest_settings.to_csv(
                'tables_csv/remote_control_setting.csv', index=False)

        else:
            pass

    except(Exception, psycopg2.Error) as error:
        if (connection):
            print('Error: ', error)

    finally:
        if (connection):
            cursor.close()
            connection.close()

    return None


def get_remote_state(user_id):

    # Database and Fibaro credentials
    user = 'dadtkzpuzwfows'
    database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    port = '5432'
    database = 'd53rn0nsdh7eok'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'
    # Connect to PostgreSQL database
    connection = psycopg2.connect(user=user, password=database_password, host=host,
                                  port=port, database=database)
    cursor = connection.cursor()

    cursor.execute(
        "SELECT device_state FROM plug_mate_app_remotedata WHERE user_id={}".format(user_id))
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=colnames)
    device_state = df['device_state'].to_list()
    return device_state


def update_device_state():
    """
    Checks the state of each smart meter from Fibaro and updates the database if there are any changes.
    Checking frequency: 1 minute
    """

    # Database and Fibaro credentials
    user = 'dadtkzpuzwfows'
    database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    port = '5432'
    database = 'd53rn0nsdh7eok'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'

    def check_meter_state(meter_id):
        query = requests.get('http://{}/api/devices/{}'.format(fibaro_address, meter_id),
                             auth=HTTPBasicAuth(fibaro_username, fibaro_password)).json()
        if query['properties']['value'] == 'true':
            return True
        elif query['properties']['value'] == 'false':
            return False
        else:
            raise ValueError('Unknown value for device state: {}'.format(
                query['properties']['value']))

    def update_database(meter_ids, device_states):
        try:
            # Connect to database
            connection = psycopg2.connect(user=user, password=database_password, host=host,
                                          port=port, database=database)
            cursor = connection.cursor()

            for i in range(len(meter_ids)):
                # Find device type based on meter id
                cursor.execute("SELECT user_id, device_type FROM power_energy_consumption WHERE meter_id={} "
                               "ORDER BY unix_time DESC LIMIT 1".format(meter_ids[i]))
                query_result = cursor.fetchone()
                user_id, device_type = query_result

                if device_type == 'other':
                    continue
                elif device_type == 'tasklamp':
                    device_type = 'Task Lamp'

                else:
                    # Update device state based on user id and device type
                    cursor.execute("UPDATE plug_mate_app_remotedata SET device_state={} WHERE user_id={} AND "
                                   "device_type='{}'".format(device_states[i], user_id, device_type.capitalize()))

            connection.commit()

        except(Exception, psycopg2.Error) as error:
            if (connection):
                print('Error: ', error)

        finally:
            if (connection):
                cursor.close()
                connection.close()

        return None

    # Access all meter IDS from the database
    last_recorded_state = pd.read_csv('tables_csv/device_state.csv')

    # Check with Fibaro for the device states based on meter IDs
    latest_state = last_recorded_state['meter_id'].apply(check_meter_state)

    # Update CSV file
    assert len(last_recorded_state) == len(latest_state)
    last_recorded_state['last_state'] = latest_state
    last_recorded_state.to_csv('tables_csv/device_state.csv', index=False)

    # Update database
    update_database(last_recorded_state['meter_id'].tolist(), latest_state)

    return None


def schedule_control():
    """
    Checks the current schedules for all users' devices and switch them ON/OFF based on those schedules. However,
    before switching off any devices, do an additional check to see if user is round.
    Checking frequency: 15 minutes
    """

    # Database and Fibaro credentials
    user = 'dadtkzpuzwfows'
    database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    port = '5432'
    database = 'd53rn0nsdh7eok'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'

    def activate_remote_control(meter_id, command):
        query = requests.post('http://{}/api/devices/{}/action/{}'.format(fibaro_address, meter_id, command),
                              auth=HTTPBasicAuth(fibaro_username, fibaro_password)).json()
        return None


    def check_user_presence(user_id):
        cursor.execute("SELECT presence FROM presence WHERE user_id={} ORDER BY unix_time DESC LIMIT 1".format(user_id))
        presence = cursor.fetchone()[0]
        if presence == 0:
            return False
        elif presence == 1:
            return True
        else:
            raise ValueError('Presence information returned {} is not supported.')


    def check_schedule(schedules, current_time, day_of_week, state):
        if state == 'On':
            event_column = 'event_start'
        else:
            event_column = 'event_end'

        # Obtain user id and device type information for devices that needs to be switched ON/OFF
        control_schedule = [(schedules.loc[i, 'user_id'], schedules.loc[i, 'device_type'])
                            for i in range(len(schedules))
                            if current_time in schedules.loc[i, event_column] and day_of_week in schedules.loc[i, 'event_rrule']]

        # Remotely switch ON/OFF devices using meter id (obtained using user id and device type)
        if state == 'On':
            command = 'turnOn'
        else:
            command = 'turnOff'

        for user_id, device_type in control_schedule:
            if check_user_presence(user_id):
                continue
            else:
                cursor.execute("SELECT meter_id FROM power_energy_consumption WHERE user_id={} AND device_type='{}' "
                               "ORDER BY unix_time DESC LIMIT 1".format(user_id, device_type.lower()))
                meter_ids = cursor.fetchall()

                for meter_id in meter_ids:
                    activate_remote_control(meter_id[0], command)

    # Obtain the schedule for all users and device types
    try:
        # Connect to database
        connection = psycopg2.connect(user=user, password=database_password, host=host,
                                      port=port, database=database)
        cursor = connection.cursor()

        # Obtain schedules from database
        cursor.execute("SELECT * FROM plug_mate_app_scheduledata")
        query_result = cursor.fetchall()
        schedules = pd.DataFrame(query_result, columns=[
                                 desc[0] for desc in cursor.description])

        # Check if the starting time of any schedule matches with the current time
        current_time = datetime.today().strftime('%H:%M')
        day_of_week = datetime.today().strftime('%A')

        # Check schedule to see if any devices needs to be switched ON/OFF
        check_schedule(schedules, current_time, day_of_week, 'On')
        check_schedule(schedules, current_time, day_of_week, 'Off')

    except(Exception, psycopg2.Error) as error:
        if (connection):
            print('Error: ', error)

    finally:
        if (connection):
            cursor.close()
            connection.close()

    return None


def check_user_arrival():
    """
    Checks the arrival of each user to his desk and switch ON his devices.
    Checking frequency: 5 seconds
    """

    # Database and Fibaro credentials
    user = 'dadtkzpuzwfows'
    database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    port = '5432'
    database = 'd53rn0nsdh7eok'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'

    def activate_remote_control(meter_id, command):
        query = requests.post('http://{}/api/devices/{}/action/{}'.format(fibaro_address, meter_id, command),
                              auth=HTTPBasicAuth(fibaro_username, fibaro_password)).json()
        return None

    # Access last recorded user presence information
    last_recorded_presence = pd.read_csv('tables_csv/user_presence.csv')

    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(user=user, password=database_password, host=host,
                                      port=port, database=database)
        cursor = connection.cursor()

        # Obtain the latest presence information of all users from database
        cursor.execute("SELECT p.user_id, p.presence, p.unix_time FROM presence p "
                       "INNER JOIN (SELECT user_id, MAX(unix_time) AS LatestTime "
                       "FROM presence GROUP BY user_id) pp  ON p.user_id = pp.user_id AND p.unix_time = pp.LatestTime "
                       "ORDER BY user_id")
        query_result = cursor.fetchall()
        latest_presence = pd.DataFrame(
            query_result, columns=[desc[0] for desc in cursor.description])

        # Obtain user id of user who just arrived at his desk
        assert len(last_recorded_presence) == len(latest_presence)
        arrival_ids = [i for i in range(len(last_recorded_presence))
                       if last_recorded_presence.loc[i, 'presence'] == 0 and latest_presence.loc[i, 'presence'] == 1]

        if len(arrival_ids) != 0:
            # Switch on all devices owned by the arriving user
            for index in arrival_ids:
                cursor.execute("SELECT meter_id FROM meters WHERE user_id={}".format(
                    last_recorded_presence.loc[index, 'user_id']))
                meter_ids = cursor.fetchall()
                for meter_id in meter_ids:
                    activate_remote_control(meter_id[0], 'turnOn')

                # Reset the control activated trackers for different devices
                last_recorded_presence.loc[index,
                                           'control_activated_desktop'] = False
                last_recorded_presence.loc[index,
                                           'control_activated_laptop'] = False
                last_recorded_presence.loc[index,
                                           'control_activated_monitor'] = False
                last_recorded_presence.loc[index,
                                           'control_activated_tasklamp'] = False
                last_recorded_presence.loc[index,
                                           'control_activated_fan'] = False

            last_recorded_presence['presence'] = latest_presence['presence']
            last_recorded_presence.to_csv(
                'tables_csv/user_presence.csv', index=False)

        else:
            pass

    except(Exception, psycopg2.Error) as error:
        if (connection):
            print('Error: ', error)

    finally:
        if (connection):
            cursor.close()
            connection.close()

    return None


def check_user_departure():
    """
    Checks the departure of the user from his desk and switches OFF his devices
    Checking frequency: 1 minute
    """

    # Database and Fibaro credentials
    user = 'dadtkzpuzwfows'
    database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    port = '5432'
    database = 'd53rn0nsdh7eok'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'

    def activate_remote_control(meter_id, command):
        query = requests.post('http://{}/api/devices/{}/action/{}'.format(fibaro_address, meter_id, command),
                              auth=HTTPBasicAuth(fibaro_username, fibaro_password)).json()
        return None

    def check_device(index, device_type):
        if device_type == 'tasklamp':
            processed_device_type = 'Task Lamp'
        else:
            processed_device_type = device_type.capitalize()

        if last_recorded_presence.loc[index, 'control_activated_{}'.format(device_type)] is np.bool_(False):
            # Query for time interval before device should be remotely switched off
            cursor.execute("SELECT presence_setting FROM plug_mate_app_presencedata "
                           "WHERE user_id={} AND device_type='{}'".format(last_recorded_presence.loc[index, 'user_id'],
                                                                         processed_device_type))
            time_interval = cursor.fetchone()[0]

            if time.time() - last_recorded_presence.loc[index, 'last_detected_departure'] > time_interval * 60.0:
                cursor.execute(
                    "SELECT meter_id FROM power_energy_consumption WHERE user_id={} AND device_type='{}' "
                    "ORDER BY unix_time DESC LIMIT 1".format(last_recorded_presence.loc[index, 'user_id'], device_type))
                meter_ids = cursor.fetchall()
                for meter_id in meter_ids:
                    activate_remote_control(meter_id[0], 'turnOff')

                last_recorded_presence.loc[index, 'control_activated_{}'.format(device_type)] = True

            else:
                pass
        else:
            pass

        return None

    # Access last recorded user presence information
    last_recorded_presence = pd.read_csv('tables_csv/user_presence.csv')

    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(user=user, password=database_password, host=host,
                                      port=port, database=database)
        cursor = connection.cursor()

        # Obtain the latest presence information of all users from database
        cursor.execute("SELECT p.user_id, p.presence, p.unix_time FROM presence p "
                       "INNER JOIN (SELECT user_id, MAX(unix_time) AS LatestTime "
                       "FROM presence GROUP BY user_id) pp  ON p.user_id = pp.user_id AND p.unix_time = pp.LatestTime "
                       "ORDER BY user_id")
        query_result = cursor.fetchall()
        latest_presence = pd.DataFrame(
            query_result, columns=[desc[0] for desc in cursor.description])

        # Obtain user id of user who has just left his desk and update last detected departure
        assert len(last_recorded_presence) == len(latest_presence)
        update = [(last_recorded_presence.loc[i, 'user_id'], latest_presence.loc[i, 'unix_time'])
                  for i in range(len(last_recorded_presence))
                  if last_recorded_presence.loc[i, 'presence'] == 1 and latest_presence.loc[i, 'presence'] == 0]

        if len(update) != 0:
            # Update user_presence of user's departure time
            for user_id, unix_time in update:
                update_index = last_recorded_presence['user_id'].tolist().index(
                    user_id)
                last_recorded_presence.loc[update_index, 'presence'] = 0
                last_recorded_presence.loc[update_index,
                                           'last_detected_departure'] = unix_time

        else:
            pass

        # Obtain user id and device type of user who has left his desk for a period longer the duration indicated
        # in the presence based control
        users_absent_index = [i for i in range(len(last_recorded_presence))
                              if last_recorded_presence.loc[i, 'presence'] == 0 and
                              (last_recorded_presence.loc[i, 'control_activated_desktop'] is np.bool_(False) or
                               last_recorded_presence.loc[i, 'control_activated_laptop'] is np.bool_(False) or
                               last_recorded_presence.loc[i, 'control_activated_monitor'] is np.bool_(False) or
                               last_recorded_presence.loc[i, 'control_activated_tasklamp'] is np.bool_(False) or
                               last_recorded_presence.loc[i, 'control_activated_fan'] is np.bool_(False))]

        for index in users_absent_index:
            check_device(index, 'desktop')
            check_device(index, 'laptop')
            check_device(index, 'monitor')
            check_device(index, 'tasklamp')
            check_device(index, 'fan')

        last_recorded_presence.to_csv(
            'tables_csv/user_presence.csv', index=False)

    except(Exception, psycopg2.Error) as error:
        if (connection):
            print('Error: ', error)

    finally:
        if (connection):
            cursor.close()
            connection.close()

    return None
