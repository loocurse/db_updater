from graph_functions import *
from achievements_functions import *
from database_read_write import *
from control_functions import *
import schedule
import json

DEBUGGING = False  # Turn on for debugging mode

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

    schedule.every().hour.at(':00').do(
        achievements_to_update, ['turn_off_leave'])
    schedule.every().hour.at(':15').do(
        achievements_to_update, ['turn_off_leave'])
    schedule.every().hour.at(':30').do(
        achievements_to_update, ['turn_off_leave'])
    schedule.every().hour.at(':45').do(
        achievements_to_update, ['turn_off_leave'])
    schedule.every().day.at("00:05").do(achievements_to_update,
                                        ['daily_schedule', 'daily_presence'])

    for time in ['23:50', '23:55', '00:05']:
        if time == '23:50':
            checklist = ['lower_energy_con', 'turn_off_end',
                         'tree_first', 'tree_fifth', 'tree_tenth', ]
        else:
            checklist = ['complete_all_daily']

        schedule.every().monday.at(time).do(achievements_to_update, checklist)
        schedule.every().tuesday.at(time).do(achievements_to_update, checklist)
        schedule.every().wednesday.at(time).do(achievements_to_update, checklist)
        schedule.every().thursday.at(time).do(achievements_to_update, checklist)
        schedule.every().friday.at(time).do(achievements_to_update, checklist)

    schedule.every().friday.at("23:55").do(
        achievements_to_update, ['complete_daily', 'complete_weekly'])
    schedule.every().sunday.at("23:53").do(
        achievements_to_update, ['cost_saving', 'schedule_based'])
    schedule.every().sunday.at("23:59").do(initialise_achievements)
    schedule.every().sunday.at("23:59").do(add_cost_saving_to_energy_points)

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


# IGNORE TO TEST

def every15minFunction():
    achievements_to_update(['turn_off_leave'])


def everyDayFunction():
    achievements_to_update(['lower_energy_con', 'turn_off_end',
                            'tree_first', 'tree_fifth', 'tree_tenth'])
    achievements_to_update(['complete_all_daily'])


def everyFridayFunction():
    achievements_to_update(['complete_daily', 'complete_weekly'])
    achievements_to_update(['cost_saving', 'schedule_based'])


def every1205amFunction():
    achievements_to_update(['daily_schedule', 'daily_presence'])


# every15minFunction()  # ['turn_off_leave']
# everyDayFunction()
#['lower_energy_con', 'turn_off_end','tree_first', 'tree_fifth', 'tree_tenth']
# ['complete_all_daily']
# everyFridayFunction()
#['complete_daily', 'complete_weekly']
#['cost_saving', 'schedule_based']
# every1205amFunction()
#['daily_schedule', 'daily_presence']
