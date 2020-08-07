from graph_functions import *
from achievements_functions import *
from database_read_write import *
import schedule

CONNECTION_PARAMS = dict(database='plug_mate_dev_db',
                         user='raymondlow',
                         password='password123',
                         host='localhost',
                         port='5432')

def initialise_database():
    """Run this function every week to reset all achievements achieved"""





if __name__ == '__main__':
    # schedule.every().hour.do(hourly_update)
    # schedule.every().day.do(daily_update)
    # schedule.every().sunday.do(weekly_monthly_update)

    achievements_update_daily()
    # get_weekly_table()
    # get_daily_table()
    achievements_update_hourly()
    # update_db(pd.read_csv('achievements_weekly.csv'), 'achievements_weekly')
    # update_db(pd.read_csv('achievements_points.csv'), 'achievement_points')
    # update_db(pd.read_csv('achievements_daily.csv'), 'achievements_daily')
    # update_db(pd.read_csv('achievements_points.csv'), 'achievements_points')
    # update_db(pd.read_csv('achievements_weekly.csv'), 'achievements_weekly')
    # update_db(pd.read_csv('achievements_daily.csv'), 'achievements_daily')

