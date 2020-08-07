import database_read_write
import pandas as pd


def _lower_energy_con(user_id):
    """Achievement: Clock a lower energy consumption than yesterday"""
    df = database_read_write.get_energy_ytd_today(user_id)
    list = df.groupby(by='date').sum()['power'].to_list()
    return True if list[0] > list[1] else False


def _turn_off_leave(user_id):
    """Achievement: Turn off your plug loads when you leave your desk for a long period of time during the day"""
    # Approach: check if plug loads are switched off when presence is not detected
    return False


def _turn_off_end(user_id):
    """Achievement: Turn off your plug loads during at the end of the day"""
    return False


def _cost_saving(user_id):
    """Achievement: Clock a higher cost savings than last week"""
    week_view = database_read_write.read_cost_savings()
    week_view_user = week_view[week_view.user_id == user_id]
    list = week_view_user['total'][-2:].to_list()
    return True if list[1] > list[0] else False


def _schedule_based(user_id):
    """Achievement: Set next week's schedule-based controls"""
    return False


def _complete_daily(user_id):
    """Achievement: Complete all daily achievements for 4 days of the week"""
    if database_read_write.get_today() not in ['Thu','Fri','Sat']:
        return False
    else:
        df = database_read_write.get_daily_table()
        df = df.loc[df.user_id == user_id]
        return True if sum(df['complete_all'].to_list()) > 4 else False


def _tree_first(user_id):
    """Achievement: Save your first tree"""
    return False


def _tree_fifth(user_id):
    """Achievement: Save your fifth tree"""
    return False


def _tree_tenth(user_id):
    """Achievement: Save your tenth tree"""
    return False


def _redeem_reward(user_id):
    """Achievement: Redeem your first reward from your rewards page"""
    return False


def _first_remote(user_id):
    """Achievement: Try out our remote control feature for the first time"""
    return False


def _first_schedule(user_id):
    """Achievement: Set your first schedule-based setting"""
    return False


def _first_presence(user_id):
    """Achievement: Set your first presence-based setting"""
    return False


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
                updated_output.append(True)
        updated_output.append(True) if all(updated_output) else updated_output.append(False)
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
        updated_output.append(True) if all(updated_output) else updated_output.append(False)
        user_df.loc[0, 'cost_saving':'complete_weekly'] = updated_output
        output = pd.concat([output, user_df], ignore_index=True)
    database_read_write.update_db(output, 'achievements_weekly')

