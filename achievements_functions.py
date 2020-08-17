import database_read_write
import pandas as pd
from datetime import datetime
from random import randint

points = pd.read_csv('tables_csv/achievements_points.csv', index_col=['achievement'])['points'].to_dict()


def _lower_energy_con(user_id):
    """Achievement: Clock a lower energy consumption than yesterday"""
    # TODO detect at the end of the day
    df = database_read_write.get_energy_ytd_today(user_id)
    ls = df.groupby(by='date').sum()['power'].to_list()
    if not ls:
        return 0
    return points['lower_energy_con'] if ls[0] > ls[1] else 0


def _turn_off_leave(user_id):
    """Achievement: Turn off your plug loads when you leave your desk for a long period of time during the day"""
    # Approach: check if plug loads are switched off when presence is not detected
    df = database_read_write.get_energy_ytd_today(user_id)
    df = df.loc[df['date'] == database_read_write.get_today()]
    def unix_to_dt(time):
        time = int(time)
        return datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
    df['datetime'] = pd.to_datetime(df['unix_time'].apply(unix_to_dt))

    # print(df.dtypes)
    df = df.groupby(pd.Grouper(key='datetime',freq="H")).sum()
    series1 = df['device_state']
    df2 = database_read_write.get_presence(user_id)
    df2 = df2.loc[df2['date'] == database_read_write.get_today()]
    df2['datetime'] = pd.to_datetime(df2['unix_time'].apply(unix_to_dt))
    df2 = df2.groupby(pd.Grouper(key='datetime',freq="H")).sum()
    series2 = df2['presence']
    # series2 = pd.Series([randint(0,100) for x in range(24)], index=series1.index, name='presence')
    combined = pd.concat([series1,series2],axis=1)
    if sum(series2) < 10:
        return 0
    change = combined.pct_change()
    # TODO change to another table
    condition = change.loc[(change['presence'] < 0) & (change['device_state'] < 0)]
    if not condition.empty:
        return points['turn_off_leave']
    else:
        return 0


def _turn_off_end(user_id):
    """Achievement: Turn off your plug loads during at the end of the day"""
    condition = False
    return points['turn_off_end'] if condition else 0


def _cost_saving(user_id):
    """Achievement: Clock a higher cost savings than last week"""
    # TODO adjust name, read at the end of the week
    week_view = database_read_write.read_cost_savings()
    week_view_user = week_view[week_view.user_id == user_id]
    list = week_view_user['total'][-2:].to_list()
    if not list:
        return 0
    return points['cost_saving'] if list[1] > list[0] else 0


def _schedule_based(user_id):
    """DONE BY MIRABEL
    Achievement: Set next week's schedule-based controls"""
    condition = False
    return points['schedule_based'] if condition else 0


def _complete_daily(user_id):
    """Achievement: Complete all daily achievements for 4 days of the week"""
    # TODO turn off checking on sat
    # TODO remove daily table from daily achievements on weekends
    if database_read_write.get_today() not in ['Thu', 'Fri']:
        return 0
    else:
        df = database_read_write.get_daily_table()
        df = df.loc[df.user_id == user_id]
        return points['complete_daily'] if sum(df['complete_all_daily'].to_list()) >= 20 * 4 else 0


def _tree_first(user_id):
    """Achievement: Save your first tree"""
    saved_kwh = database_read_write.get_cumulative_saving(user_id)
    if saved_kwh == None:
        return 0
    saved_trees = round(saved_kwh * 0.201 * 0.5)
    condition = saved_trees > 1
    return points['tree_first'] if condition else 0


def _tree_fifth(user_id):
    """Achievement: Save your fifth tree"""
    saved_kwh = database_read_write.get_cumulative_saving(user_id)
    if saved_kwh == None:
        return 0
    saved_trees = round(saved_kwh * 0.201 * 0.5)
    condition = saved_trees > 5
    return points['tree_fifth'] if condition else 0


def _tree_tenth(user_id):
    """Achievement: Save your tenth tree"""
    saved_kwh = database_read_write.get_cumulative_saving(user_id)
    saved_trees = round(saved_kwh * 0.201 * 0.5)
    condition = saved_trees > 5
    return points['tree_tenth'] if condition else 0


def _redeem_reward(user_id):
    """Achievement: Redeem your first reward from your rewards page"""
    condition = False
    return points['redeem_reward'] if condition else 0


def _first_remote(user_id):
    """DONE BY MIRABEL
    Achievement: Try out our remote control feature for the first time"""
    condition = False
    return points['first_remote'] if condition else 0


def _first_schedule(user_id):
    """DONE BY MIRABEL
    Achievement: Set your first schedule-based setting"""
    condition = False
    return points['first_schedule'] if condition else 0


def _first_presence(user_id):
    """DONE BY MIRABEL
    Achievement: Set your first presence-based setting"""
    condition = False
    return points['first_presence'] if condition else 0


def _cum_savings(user_id):
    return 0


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
            if status == 0:  # if task not yet met, check if task met
                earned_points = daily_functions[num](user_id)
                updated_output.append(earned_points)
                if earned_points > 0:
                    add_energy_points_wallet(user_id, earned_points)
            else:
                updated_output.append(status)
        updated_output.append(points['complete_daily']) if all(updated_output) else updated_output.append(0)
        # Replace information
        user_df.at[today, 'lower_energy_con':'complete_all_daily'] = updated_output
        output = pd.concat([output, user_df])
    # Send to DB
    output.reset_index(inplace=True)
    database_read_write.update_db(output, 'achievements_daily')


def achievements_update_daily():
    df_weekly = database_read_write.get_weekly_table()
    df_bonus = database_read_write.get_bonus_table()
    user_ids = sorted(df_weekly['user_id'].unique())
    output_weekly = pd.DataFrame()
    output_bonus = pd.DataFrame()
    for user_id in user_ids:
        # Weekly
        user_df = df_weekly.loc[df_weekly.user_id == user_id].copy().reset_index(drop=True)
        ls = user_df.loc[0]['cost_saving':'complete_daily'].to_list()
        updated_output = []
        weekly_functions = (_cost_saving, _schedule_based, _complete_daily)
        for num, status in enumerate(ls):
            if status == 0:
                earned_points = weekly_functions[num](user_id)
                updated_output.append(earned_points)
                if earned_points > 0:
                    add_energy_points_wallet(user_id, earned_points)
            else:
                updated_output.append(status)
        updated_output.append(points['complete_weekly']) if all(updated_output) else updated_output.append(0)
        user_df.loc[0, 'cost_saving':'complete_weekly'] = updated_output
        output_weekly = pd.concat([output_weekly, user_df], ignore_index=True)

        # Bonus
        user_df = df_bonus.loc[df_bonus.user_id == user_id].copy().reset_index(drop=True)
        ls = user_df.loc[0]['tree_first':'cum_savings'].to_list()
        updated_output = []
        bonus_functions = (
        _tree_first, _tree_fifth, _tree_tenth, _redeem_reward, _first_remote, _first_schedule, _first_presence,
        _cum_savings)
        for num, status in enumerate(ls):
            if not status:
                earned_points = bonus_functions[num](user_id)
                updated_output.append(earned_points)
                if earned_points > 0:
                    add_energy_points_wallet(user_id, earned_points)
            else:
                updated_output.append(True)
        user_df.loc[0, 'tree_first':'cum_savings'] = updated_output
        output_bonus = pd.concat([output_bonus, user_df], ignore_index=True)

    database_read_write.update_db(output_weekly, 'achievements_weekly')
    database_read_write.update_db(output_bonus, 'achievements_bonus')


def achievements_check_if_all_devices_off():
    today = database_read_write.get_today()
    df = database_read_write.get_daily_table()
    df['week_day'] = df.index
    df.set_index('id', inplace=True, append=True)
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
            df.at[index, 'turn_off_end'] = 10

    # Send to DB

    df.reset_index(drop=True, inplace=True)
    df.reset_index(drop=False, inplace=True)
    df.rename(columns={'index': 'id'}, inplace=True)

    database_read_write.update_db(df, 'achievements_daily', index_to_col=False)


def add_energy_points_wallet(user_id, points):
    """Adds energy points to user"""
    df = database_read_write.get_energy_points_wallet()
    df.set_index('user_id', inplace=True)
    df.at[user_id, 'points'] += points
    df.reset_index(inplace=True)
    df = df[['id','user_id','points']]
    database_read_write.update_db(df, 'points_wallet')
