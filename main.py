from graph_functions import *
from achievements_functions import *
from database_read_write import *
from control_functions import *
import schedule

# CONNECTION_PARAMS = dict(database='plug_mate_dev_db',
#                          user='raymondlow',
#                          password='password123',
#                          host='localhost',
#                          port='5432')
# CONNECTION_PARAMS = dict(database='postgres',
#                          user='postgres',
#                          password='123456',
#                          host='localhost',
#                          port='5432')

CONNECTION_PARAMS = dict(database='d53rn0nsdh7eok',
                         user='dadtkzpuzwfows',
                         password='1a62e7d11e87864c20e4635015040a6cb0537b1f863abcebe91c50ef78ee4410',
                         host='ec2-46-137-79-235.eu-west-1.compute.amazonaws.com',
                         port='5432')


def initialise_achievements():
    """Run this function every week to reset all achievements achieved"""
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Resetting all achievements')
    df_daily = get_daily_table()
    df_weekly = get_weekly_table()
    df_daily.loc[:, 'lower_energy_con':'complete_all'] = False
    df_weekly.loc[:, 'cost_saving':'complete_weekly'] = False
    update_db(df_daily, 'achievements_daily', index_to_col=True)
    update_db(df_weekly, 'achievements_weekly', index_to_col=True)


if __name__ == '__main__':
    # # Update dashboard
    # schedule.every().hour.do(graph_hourly_update)
    # schedule.every().day.do(graph_daily_update)
    # schedule.every().sunday.do(graph_weekly_monthly_update)
    #
    # # Update achievements
    # schedule.every().hour.do(achievements_update_hourly)
    # schedule.every().day.do(achievements_update_daily)
    # schedule.every().day.at("03:00").do(achievements_check_if_all_devices_off)
    #
    # # Update control features
    # schedule.every(5).seconds.do(check_remote_control)
    # schedule.every().minutes.do(update_device_state)
    # schedule.every(15).minutes.do(schedule_control)
    # schedule.every(5).seconds.do(check_user_arrival)
    # schedule.every().minutes.do(check_user_departure)

    # while True:
    #     schedule.run_pending()

    # graph_hourly_update()
    # graph_daily_update()
    # graph_weekly_monthly_update()

    # check_remote_control()
    update_device_state()
