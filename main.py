from graph_functions import *
from achievements_functions import *
from database_read_write import *
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
    initialise_achievements()
    # schedule.every().hour.do(hourly_update)
    # schedule.every().day.do(daily_update)
    # schedule.every().sunday.do(weekly_monthly_update)

    # achievements_update_daily()
    # get_weekly_table()
    # get_daily_table()
    # achievements_update_hourly()
    # update_db(pd.read_csv('achievements_weekly.csv'), 'achievements_weekly')
    # update_db(pd.read_csv('achievements_points.csv'), 'achievement_points')
    # update_db(pd.read_csv('achievements_daily.csv'), 'achievements_daily')
    # update_db(pd.read_csv('achievements_points.csv'), 'achievements_points')
    # update_db(pd.read_csv('achievements_weekly.csv'), 'achievements_weekly')
    # update_db(pd.read_csv('achievements_daily.csv'), 'achievements_daily')
