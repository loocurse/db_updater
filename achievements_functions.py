import database_read_write
import pandas as pd
from datetime import datetime
import control_functions
import warnings
import numpy as np

warnings.simplefilter(action='ignore', category=FutureWarning)

points = pd.read_csv('tables_csv/achievements_points.csv',
                     index_col=['achievement'])['points'].to_dict()


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
    device_state_list = control_functions.get_remote_state(user_id)

    if True not in device_state_list:
        df = database_read_write.get_presence(user_id)

        def unix_to_dt(time):
            time = int(time)
            return datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

        df['datetime'] = pd.to_datetime(df['unix_time'].apply(unix_to_dt))
        df = df.loc[df['datetime'] >= pd.to_datetime(
            database_read_write.get_today())]
        presence_data = df.tail(1)['presence']
        if np.sum(presence_data) == 0:
            return points['turn_off_leave']
        else:
            return 0

    else:
        return 0


def _turn_off_end(user_id):
    """Achievement: Turn off your plug loads during at the end of the day"""
    # Check if any of the devices are not turned off
    device_state_list = control_functions.get_remote_state(user_id)
    if any(device_state_list):
        return 0
    df = database_read_write.get_presence(user_id)

    def unix_to_dt(time):
        time = int(time)
        return datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

    df['datetime'] = pd.to_datetime(df['unix_time'].apply(unix_to_dt))
    df = df.loc[df['datetime'] >= pd.to_datetime(
        database_read_write.get_today())]
    if sum(df['presence']) < 5:
        return 0
    else:
        return points['turn_off_end']


def _daily_presence(user_id):
    """DONE BY MIRABEL
    Achievement: Activate presence-based control for your devices today"""
    condition = False
    return points['daily_presence'] if condition else 0


def _daily_schedule(user_id):
    """DONE BY MIRABEL
    Achievement: Use schedule-based control for your devices today"""
    condition = False
    return points['daily_schedule'] if condition else 0


def _daily_remote(user_id):
    """DONE BY MIRABEL
    Achievement: Use remote control while you are away from your desk today"""
    condition = False
    return points['daily_remote'] if condition else 0


def _complete_all_daily(user_id, achieved=False):
    daily = database_read_write.get_daily_table()
    daily = daily.loc[daily.user_id == user_id]
    ser = daily.loc[database_read_write.get_today().strftime('%a')].to_list()[:-1]
    if all(ser):
        return points['complete_all_daily']
    else:
        return 0


def _cost_saving(user_id):
    """Achievement: Clock a higher cost savings than last week"""
    # TODO adjust name, read at the end of the week
    week_view_user = database_read_write.read_cost_savings(user_id=user_id)
    list = week_view_user['total'][-2:].to_list()
    if not list:
        return 0
    return points['cost_saving'] if list[1] > list[0] else 0


def _schedule_based(user_id):
    """DONE BY MIRABEL
    Achievement: Set next week's schedule-based controls"""
    schedules = database_read_write.get_schedules(user_id)
    number_of_schedules = len(schedules)
    condition = number_of_schedules > 0
    return points['schedule_based'] if condition else 0


def _complete_daily(user_id):
    """Achievement: Complete all daily achievements for 4 days of the week"""
    # TODO turn off checking on sat
    if database_read_write.get_today() not in ['Fri']:
        return 0
    else:
        df = database_read_write.get_daily_table()
        df = df.loc[df.user_id == user_id]
        return points['complete_daily'] if sum(df['complete_all_daily'].to_list()) >= 20 * 4 else 0


def _complete_all_weekly(user_id):
    weekly = database_read_write.get_weekly_table()
    weekly = weekly.loc[weekly.user_id == user_id].iloc[0].to_list()
    if all(weekly):
        return points['complete_all_weekly']
    else:
        return 0


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
    """Calculates the cumulative savings of user_id"""
    df_bonus = database_read_write.get_bonus_table()
    df = df_bonus.loc[df_bonus.user_id == user_id]
    existing_value = df.iloc[0]['cum_savings']
    # Getting new value
    df = database_read_write.read_all_db(user_id=user_id)
    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby(['date', 'device_type']).sum().reset_index()
    df = df.pivot(index='date', columns='device_type', values='power')

    def process(df):
        output = pd.DataFrame()
        for col in list(df):
            ls = []
            for index, row in df.iterrows():
                value = row[col]
                location = np.where(df.index == index)[0][0]
                average = df[col][:location].mean()
                value -= average
                ls.append(value)
            x = pd.Series(ls, name=col)
            output = pd.concat([output, x], axis=1)
        output.index = df.index
        output.fillna(0, inplace=True)
        output['total'] = output.sum(axis=1)
        return output

    # Aggregate data based on view & set index
    week_view = df.groupby(pd.Grouper(freq='W-MON', label='left')).sum()
    week_view = process(week_view)
    if existing_value == 0:
        week_view = week_view[:-1]['total'].tolist()
        total_savings = sum([x for x in week_view if x > 0])
        return total_savings
    else:
        total_new_savings = week_view.iloc[-1]['total']
        if total_new_savings > 0:
            existing_value += total_new_savings
        return existing_value


FUNCTIONS = {
    'lower_energy_con': _lower_energy_con,
    'turn_off_leave': _turn_off_leave,
    'turn_off_end': _turn_off_end,
    'complete_all_daily': _complete_all_daily,
    'cost_saving': _cost_saving,
    'schedule_based': _schedule_based,
    'daily_presence': _daily_presence,
    'daily_schedule': _daily_schedule,
    'daily_remote': _daily_remote,
    'complete_daily': _complete_daily,
    'tree_first': _tree_first,
    'tree_fifth': _tree_fifth,
    'tree_tenth': _tree_tenth,
    'redeem_reward': _redeem_reward,
    'first_remote': _first_remote,
    'first_schedule': _first_schedule,
    'first_presence': _first_presence,
    'cum_savings': _cum_savings,
}


def _add_energy_points_wallet(user_id, points):
    """Adds energy points to user"""
    df = database_read_write.get_energy_points_wallet()
    df.set_index('user_id', inplace=True)
    df.at[user_id, 'points'] += points
    df.reset_index(inplace=True)
    df = df[['id', 'user_id', 'points']]
    database_read_write.update_db(df, 'points_wallet')


def add_cost_saving_to_energy_points():
    for user_id in database_read_write.get_user_ids():
        df = database_read_write.read_cost_savings(user_id=user_id)
        savings = df['total'].iloc[-1]
        energy_points = savings * 10 if savings > 0 else savings * 5
        _add_energy_points_wallet(user_id, energy_points)


def _update_daily_table(achievements_to_update):
    assert (all(achievement in FUNCTIONS.keys() for achievement in achievements_to_update))
    df_daily = database_read_write.get_daily_table()
    user_ids = sorted(df_daily['user_id'].unique())
    today = database_read_write.get_today().strftime('%a')
    for user_id in user_ids:
        ser_daily = df_daily.loc[df_daily.user_id == user_id].loc[today]
        for col, value in ser_daily.iteritems():
            if col in achievements_to_update and value == 0 and FUNCTIONS[col](user_id) > 0:
                index = df_daily.index[(df_daily['user_id'] == user_id) & (
                        df_daily.index == today)].tolist()[0]
                df_daily.at[index, col] = FUNCTIONS[col](user_id)
                _add_energy_points_wallet(user_id, FUNCTIONS[col](user_id))
    df_daily.insert(1, 'week_day', df_daily.index)
    database_read_write.update_db(df_daily, 'achievements_daily')


def _update_weekly_table(achievements_to_update):
    assert (all(achievement in FUNCTIONS.keys() for achievement in achievements_to_update))
    df_weekly = database_read_write.get_weekly_table()
    user_ids = sorted(df_weekly['user_id'].unique())
    for user_id in user_ids:
        ser_weekly = df_weekly.loc[df_weekly.user_id == user_id].iloc[0]
        for col, value in ser_weekly.iteritems():
            if col in achievements_to_update and value == 0 and FUNCTIONS[col](user_id) > 0:
                index = df_weekly.index[(df_weekly['user_id'] == user_id)].tolist()[0]
                df_weekly.at[index, col] = FUNCTIONS[col](user_id)
                _add_energy_points_wallet(user_id, FUNCTIONS[col](user_id))


def _update_bonus_table(achievements_to_update):
    assert (all(achievement in FUNCTIONS.keys() for achievement in achievements_to_update))
    df_bonus = database_read_write.get_bonus_table()
    user_ids = sorted(df_bonus['user_id'].unique())
    for user_id in user_ids:
        ser_bonus = df_bonus.loc[df_bonus.user_id == user_id].iloc[0]
        for col, value in ser_bonus.iteritems():
            if col in achievements_to_update and col == "cum_savings":
                index = df_bonus.index[(df_bonus['user_id'] == user_id)].tolist()[0]
                df_bonus.at[index, col] = FUNCTIONS[col](user_id)
            elif col in achievements_to_update and value == 0 and FUNCTIONS[col](user_id) > 0:
                index = df_bonus.index[(df_bonus['user_id'] == user_id)].tolist()[0]
                df_bonus.at[index, col] = FUNCTIONS[col](user_id)
                _add_energy_points_wallet(user_id, FUNCTIONS[col](user_id))

    database_read_write.update_db(df_bonus, 'achievements_bonus')


def _initialise_achievements():
    """Run this function every week to reset all achievements achieved, except cum_savings"""
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Resetting all achievements')
    df_daily = database_read_write.get_daily_table()
    df_weekly = database_read_write.get_weekly_table()
    df_bonus = database_read_write.get_bonus_table()
    for df in [df_daily, df_weekly, df_bonus]:
        for col in list(df):
            if col in FUNCTIONS.keys() and col != "cum_savings":
                df[col] = 0
    df_daily.insert(1, 'week_day', df_daily.index)
    database_read_write.update_db(df_daily, 'achievements_daily')
    database_read_write.update_db(df_weekly, 'achievements_weekly')
    database_read_write.update_db(df_bonus, 'achievements_bonus')


def achievement_update_everyday_2350():
    to_update = [
        'lower_energy_con',
        'turn_off_end',
        'complete_all_daily',
        'tree_first',
        'tree_fifth',
        'tree_tenth',
        'redeem_reward',
        'first_remote',
        'first_schedule',
        'cum_savings'
    ]
    _update_daily_table(to_update)
    _update_bonus_table(to_update)


def achievements_update_every_15m():
    to_update = [
        'turn_off_leave',
    ]
    _update_daily_table(to_update)


def achievement_update_every_sunday_2350():
    to_update = [
        'cost_saving',
        'schedule_based',
        'complete_weekly',
        'cum_savings',
    ]
    _update_weekly_table(to_update)
    _initialise_achievements()
    add_cost_saving_to_energy_points()
