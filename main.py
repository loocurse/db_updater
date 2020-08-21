from graph_functions import *
from achievements_functions import *
from database_read_write import *
from control_functions import *
import schedule
import json

# Read credentials
with open('credentials.json', 'r') as f:
    CONNECTION_PARAMS = json.load(f)


if __name__ == '__main__':
    if DEBUGGING:
        print('WARNING: Debugging mode is turned on, database will not be updated')
    # Update dashboard
    schedule.every().hour.do(graph_hourly_update)
    schedule.every().day.do(graph_daily_update)
    schedule.every().sunday.do(graph_weekly_monthly_update)

    # Update achievements
    schedule.every().hour.at(':00').do(achievements_update_every_15m)
    schedule.every().hour.at(':15').do(achievements_update_every_15m)
    schedule.every().hour.at(':30').do(achievements_update_every_15m)
    schedule.every().hour.at(':45').do(achievements_update_every_15m)
    schedule.every().day.at("23:50").do(achievement_update_everyday_2350)
    schedule.every().sunday.at("23:50").do(achievement_update_every_sunday_2350)

    # Update control features
    schedule.every(5).seconds.do(check_remote_control)
    schedule.every().minutes.do(update_device_state)
    schedule.every().hour.at(':00').do(schedule_control)
    schedule.every().hour.at(':15').do(schedule_control)
    schedule.every().hour.at(':30').do(schedule_control)
    schedule.every().hour.at(':45').do(schedule_control)
    schedule.every(5).seconds.do(check_user_arrival)
    schedule.every().minutes.do(check_user_departure)

    while True:
        schedule.run_pending()
