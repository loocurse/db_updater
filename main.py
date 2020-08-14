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


def initialise_achievements():
    """Run this function every week to reset all achievements achieved"""
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Resetting all achievements')
    df_daily = get_daily_table()
    df_weekly = get_weekly_table()
    df_daily.loc[:, 'lower_energy_con':'complete_all'] = False
    df_weekly.loc[:, 'cost_saving':'complete_weekly'] = False
    update_db(df_daily, 'achievements_daily', index_to_col=True)
    update_db(df_weekly, 'achievements_weekly', index_to_col=True)


if __name__ == '__main__':
    schedule.every().hour.do(achievements_update_hourly)
    schedule.every().hour.do(graph_hourly_update)
    schedule.every().day.do(achievements_update_daily)
    schedule.every().day.do(graph_daily_update)
    schedule.every().sunday.do(graph_weekly_monthly_update)
    schedule.every().day.at("03:00").do(achievements_check_if_all_devices_off)

    while True:
        schedule.run_pending()



