

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
    # Access all meter IDS from the database

    # Check with Fibaro for the device states based on meter IDs

    # Check if there are any changes to the state of device
        # If there is a change in state, then update csv file and database

        # Else, do nothing

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