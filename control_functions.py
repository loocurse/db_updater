import psycopg2
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth


def check_remote_control():
    """
    Checks the database for the device state and detect if remote control is activated.
    Checking frequency: 5 seconds
    """
    # Obtain state of all devices from database

    # Check state of devices from database with csv file
        # If there are changes, then identify meter id based on device type and user id.
            # If its from on to off, then switch off device

            # Else, switch on device

            # Then update CSV file

        # Else, do nothing

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
                               "ORDER BY unix_time DESC LIMIT 1;".format(meter_ids[i]))
                user_id, device_type = cursor.fetchone()[0]
                device_type = device_type.capitalize()

                # Update device state based on user id and device type
                cursor.execute("UPDATE plug_mate_app_remotedata SET device_state={} WHERE user_id={} AND "
                               "device_type={}".format(device_states[i], user_id, device_type))

            connection.commit()

        except(Exception, psycopg2.Error) as error:
            if (connection):
                print('Error: Failed to extract meter id or insert record or connect to database.', error)

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
    # Obtain the schedule for all users and device types

    # Check if the starting time of any schedule matches with the current time
        # If found, then check if the device is already ON
            # If device is ON, then do nothing

            # Else, switch on device

        # Else, do nothing

    # Check if ending time of any schedule matches with current time
        # If found, check if user is present at the desk for the last 5 minutes
            # If user is present, then do nothing

            # Else, switch off device based on device type, user id, and meter id

        # Else, do nothing

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