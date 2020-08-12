import database_read_write
import pandas as pd

points = pd.read_csv('/Users/lucasng/Downloads/db_updater/tables_csv/achievements_points.csv', index_col=['achievement'])['points'].to_dict()


def _lower_energy_con(user_id):
    """Achievement: Clock a lower energy consumption than yesterday"""
    df = database_read_write.get_energy_ytd_today(user_id)
    list = df.groupby(by='date').sum()['power'].to_list()
    return points['lower_energy_con'] if list[0] > list[1] else 0


def _turn_off_leave(user_id):
    """Achievement: Turn off your plug loads when you leave your desk for a long period of time during the day"""
    # Approach: check if plug loads are switched off when presence is not detected
    condition = True
    return points['turn_off_leave'] if condition else 0


def _turn_off_end(user_id):
    """Achievement: Turn off your plug loads during at the end of the day"""
    condition = True
    return points['turn_off_end'] if condition else 0


def _cost_saving(user_id):
    """Achievement: Clock a higher cost savings than last week"""
    week_view = database_read_write.read_cost_savings()
    week_view_user = week_view[week_view.user_id == user_id]
    list = week_view_user['total'][-2:].to_list()
    return points['cost_saving'] if list[1] > list[0] else 0


def _schedule_based(user_id):
    """Achievement: Set next week's schedule-based controls"""
    condition = True
    return points['schedule_based'] if condition else 0


def _complete_daily(user_id):
    """Achievement: Complete all daily achievements for 4 days of the week"""
    if database_read_write.get_today() not in ['Thu','Fri','Sat']:
        return False
    else:
        df = database_read_write.get_daily_table()
        df = df.loc[df.user_id == user_id]
        return points['complete_daily'] if sum(df['complete_all_daily'].to_list()) >= 20*4 else 0


def _tree_first(user_id):
    """Achievement: Save your first tree"""
    condition = True
    return points['tree_first'] if condition else 0


def _tree_fifth(user_id):
    """Achievement: Save your fifth tree"""
    condition = True
    return points['tree_fifth'] if condition else 0


def _tree_tenth(user_id):
    """Achievement: Save your tenth tree"""
    condition = True
    return points['tree_tenth'] if condition else 0


def _redeem_reward(user_id):
    """Achievement: Redeem your first reward from your rewards page"""
    condition = True
    return points['redeem_reward'] if condition else 0


def _first_remote(user_id):
    """Achievement: Try out our remote control feature for the first time"""
    condition = True
    return points['first_remote'] if condition else 0


def _first_schedule(user_id):
    """Achievement: Set your first schedule-based setting"""
    condition = True
    return points['first_schedule'] if condition else 0


def _first_presence(user_id):
    """Achievement: Set your first presence-based setting"""
    condition = True
    return points['first_presence'] if condition else 0


def achievements_update_hourly():
    df = database_read_write.get_daily_table()
    user_ids = sorted(df['user_id'].unique())
    today = database_read_write.get_today().strftime('%a')
    output = pd.DataFrame()
    daily_functions = (_lower_energy_con, _turn_off_leave, _turn_off_end)
    for user_id in user_ids:
        user_df = df.loc[df.user_id == user_id].copy()
        ls = user_df.loc[today]['lower_energy_con':'turn_off_end'].to_list()
        updated_output = []
        for num, status in enumerate(ls):
            if not status:  # if task not yet met, check if task met
                updated_output.append(daily_functions[num](user_id))
            else:
                updated_output.append(status)
        updated_output.append(points['complete_daily']) if all(updated_output) else updated_output.append(0)
        # Replace information
        user_df.at[today, 'lower_energy_con':'complete_all'] = updated_output
        output = pd.concat([output, user_df])
    # Send to DB
    output.reset_index(inplace=True)
    database_read_write.update_db(output, 'achievements_daily')


def achievements_update_daily():
    df = database_read_write.get_weekly_table()
    user_ids = sorted(df['user_id'].unique())
    output = pd.DataFrame()
    for user_id in user_ids:
        user_df = df.loc[df.user_id == user_id].copy().reset_index(drop=True)
        ls = user_df.loc[0]['cost_saving':'complete_daily'].to_list()
        updated_output = []
        weekly_functions = (_cost_saving, _schedule_based, _complete_daily)
        for num, status in enumerate(ls):
            if not status:
                updated_output.append(weekly_functions[num](user_id))
            else:
                updated_output.append(True)
        updated_output.append(points['complete_weekly']) if all(updated_output) else updated_output.append(0)
        user_df.loc[0, 'cost_saving':'complete_weekly'] = updated_output
        output = pd.concat([output, user_df], ignore_index=True)
    database_read_write.update_db(output, 'achievements_weekly')

def check_if_all_devices_off():
    today = database_read_write.get_today()
    df = database_read_write.get_daily_table()
    df['week_day'] = df.index
    df.set_index('id',inplace=True,append=True)
    user_ids = sorted(df['user_id'].unique())
    for user_id in user_ids:
        # Check if devices are all turned off
        df2 = database_read_write.get_energy_ytd_today(user_id)
        df2['date'] = pd.to_datetime(df2['date'])
        df2['datetime'] = pd.to_datetime(df2['date'].astype(str) + " " + df2['time'].astype(str))
        df2 = df2.loc[(df2['date'].dt.date == today) & (df2['datetime'].dt.hour == 3)]
        devices_off = df2['device_state'].sum() == 0

        # Get ID of user
        index = df.index[(df['user_id'] == user_id) & (df['week_day'] == today.strftime('%a'))]
        if devices_off:
            df.at[index,'turn_off_end'] = 10

    # Send to DB

    df.reset_index(drop=True, inplace=True)
    df.reset_index(drop=False,inplace=True)
    df.rename(columns={'index':'id'}, inplace=True)

    database_read_write.update_db(df, 'achievements_daily', index_to_col=False)
