import psycopg2
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime


def check_remote_control():
    """
    Checks the database for the device state and detect if remote control is activated.
    Checking frequency: 5 seconds
    """

    # Database and Fibaro credentials
    # user = 'dadtkzpuzwfows'
    # database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    # host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    # port = '5432'
    # database = 'd53rn0nsdh7eok'
    user = 'raymondlow'
    database_password = 'password123'
    host = 'localhost'
    port = '5432'
    database = 'plug_mate_dev_db'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'


    def activate_remote_control(meter_id, command):
        # query = requests.get('http://{}/api/devices/{}'.format(fibaro_address, meter_id),
        #                      auth=HTTPBasicAuth(fibaro_username, fibaro_password)).json()
        # print(query)

        # return true if device is on, false if device is off
        return None


    # Obtain state of all devices from database
    last_recorded_settings = pd.read_csv('tables_csv/remote_control_setting.csv')

    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(user=user, password=database_password, host=host,
                                      port=port, database=database)
        cursor = connection.cursor()

        cursor.execute("SELECT * FROM plug_mate_app_remotedata ORDER BY id")
        query_result = cursor.fetchall()
        latest_settings = pd.DataFrame(query_result, columns=[desc[0] for desc in cursor.description])

        # Check state of devices from database with csv file
        diff = [(latest_settings.loc[i,'user_id'], latest_settings.loc[i,'device_type'],
                 latest_settings.loc[i,'device_state'], last_recorded_settings.loc[i,'device_state'])
                for i in range(len(latest_settings))
                if latest_settings.loc[i,'device_state'] != last_recorded_settings.loc[i,'device_state']]

        if len(diff) != 0:
            # Identify meter id based on device type and user id and switch it ON/OFF
            for user_id, device_type, new_state, previous_state in diff:
                cursor.execute("SELECT meter_id FROM power_energy_consumption WHERE user_id={} AND device_type={} "
                               "ORDER BY unix_time DESC LIMIT 1".format(user_id, device_type.lower()))
                meter_id = cursor.fetchone()[0]

                if new_state is True and previous_state is False:
                    activate_remote_control(meter_id, 'turnOn')
                elif new_state is False and previous_state is True:
                    activate_remote_control(meter_id, 'turnOff')
                else:
                    raise ValueError('New State: {} | Previous state: {} | Meter id: {}'.format(new_state, previous_state, meter_id))

            # Then update CSV file
            latest_settings.to_csv('tables_csv/remote_control_setting.csv', index=False)

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

def update_device_state():
    """
    Checks the state of each smart meter from Fibaro and updates the database if there are any changes.
    Checking frequency: 1 minute
    """

    # Database and Fibaro credentials
    # user = 'dadtkzpuzwfows'
    # database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    # host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    # port = '5432'
    # database = 'd53rn0nsdh7eok'
    user = 'raymondlow'
    database_password = 'password123'
    host = 'localhost'
    port = '5432'
    database = 'plug_mate_dev_db'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'


    def check_meter_state(meter_id):
        query = requests.get('http://{}/api/devices/{}'.format(fibaro_address, meter_id),
                             auth=HTTPBasicAuth(fibaro_username, fibaro_password)).json()
        print(query)

        # return true if device is on, false if device is off
        return None


    def update_database_device_state(meter_ids, device_states):
        try:
            # Connect to PostgreSQL database
            connection = psycopg2.connect(user=user, password=database_password, host=host,
                                          port=port, database=database)
            cursor = connection.cursor()

            for i in range(len(meter_ids)):
                # Find device type based on meter id
                cursor.execute("SELECT user_id, device_type FROM power_energy_consumption WHERE meter_id={} "
                               "ORDER BY unix_time DESC LIMIT 1".format(meter_ids[i]))
                user_id, device_type = cursor.fetchone()[0]
                device_type = device_type.capitalize()

                # Update device state based on user id and device type
                cursor.execute("UPDATE plug_mate_app_remotedata SET device_state={} WHERE user_id={} AND "
                               "device_type={}".format(device_states[i], user_id, device_type.capitalize()))

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

    # Check if there are any changes to the state of device
    assert len(last_recorded_state) == len(latest_state)
    diff = [(i, latest_state[i]) for i, item in enumerate(last_recorded_state['last_state']) if latest_state[i] != item]
    index_diff, state_diff = map(list, zip(*diff))

    if len(index_diff) != 0:
        # Update CSV file
        last_recorded_state['last_state'] = latest_state
        last_recorded_state.to_csv('tables_csv/device_state.csv', index=False)

        # Update database
        meter_ids = last_recorded_state.loc[index_diff, 'meter_id'].tolist()
        assert len(meter_ids) == len(state_diff)
        update_database_device_state(meter_ids, state_diff)

    else:
        pass

    return None


def schedule_control():
    """
    Checks the current schedules for all users' devices and switch them ON/OFF based on those schedules. However,
    before switching off any devices, do an additional check to see if user is round.
    Checking frequency: 15 minutes
    """

    # Database and Fibaro credentials
    # user = 'dadtkzpuzwfows'
    # database_password = '1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410'
    # host = 'ec2-46-137-79-235.eu-west-1.compute.amazonaws.com'
    # port = '5432'
    # database = 'd53rn0nsdh7eok'
    user = 'raymondlow'
    database_password = 'password123'
    host = 'localhost'
    port = '5432'
    database = 'plug_mate_dev_db'
    fibaro_address = '172.19.243.58:80'
    fibaro_username = 'admin'
    fibaro_password = 'admin'


    def activate_remote_control(meter_id, command):
        # query = requests.get('http://{}/api/devices/{}'.format(fibaro_address, meter_id),
        #                      auth=HTTPBasicAuth(fibaro_username, fibaro_password)).json()
        # print(query)

        # return true if device is on, false if device is off
        return None


    def check_schedule(state):
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
            cursor.execute("SELECT meter_id FROM power_energy_consumption WHERE user_id={} AND device_type={} "
                           "ORDER BY unix_time DESC LIMIT 1".format(user_id, device_type.lower()))
            meter_ids = cursor.fetchall()

            for meter_id in meter_ids:
                activate_remote_control(meter_id, command)


    # Obtain the schedule for all users and device types
    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(user=user, password=database_password, host=host,
                                      port=port, database=database)
        cursor = connection.cursor()

        # Obtain schedules from database
        cursor.execute("SELECT * FROM plug_mate_app_scheduledata")
        query_result = cursor.fetchall()
        schedules = pd.DataFrame(query_result, columns=[desc[0] for desc in cursor.description])

        # Check if the starting time of any schedule matches with the current time
        current_time = datetime.today().strftime('%H:%M')
        day_of_week = datetime.today().strftime('%A')

        # Check schedule to see if any devices needs to be switched ON/OFF
        check_schedule('On')
        check_scheduld('Off')

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
    # Obtain the presence information of all users from database

    # Check users' presence with csv file
        # If user's presence has changed from not around to around, then switch on all devices and update csv file

        # Else, do nothing

    return None


def check_user_departure():
    """
    Checks the departure of the user from his desk and switches OFF his devices
    Checking frequency: 1 minute
    """
    # Obtain the presence information of all users from database

    # If a user is not around and his devices are ON, obtain his presence information for the last X minutes and check if he is around
        # If he is not around for the past X minutes, then switch off device

        # Else, do nothing

    # Else, do nothing

    return None