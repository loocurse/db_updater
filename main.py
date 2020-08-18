from graph_functions import *
from achievements_functions import *
from database_read_write import *
from control_functions import *
import schedule

CONNECTION_PARAMS = dict(database='plug_mate_dev_db',
                         user='raymondlow',
                         password='password123',
                         host='localhost',
                         port='5432')

# CONNECTION_PARAMS = dict(database='d53rn0nsdh7eok',
#                          user='dadtkzpuzwfows',
#                          password='1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410',
#                          host='ec2-46-137-79-235.eu-west-1.compute.amazonaws.com',
#                          port='5432')




if __name__ == '__main__':
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
