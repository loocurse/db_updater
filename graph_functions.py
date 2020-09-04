from database_read_write import *
import pandas as pd
import datetime as dt
import dateutil.relativedelta
import copy
from datetime import date
from datetime import timezone
import numpy as np
import pytz
from dateutil import relativedelta
from datetime import datetime

pd.set_option('mode.chained_assignment', None)


def _initialise_variables(df):
    # Initialise some variables
    global singapore_tariff_rate
    singapore_tariff_rate = 0.201

    def generate_month(unix_time):
        return dt.datetime.fromtimestamp(unix_time).strftime('%b')

    def generate_year(unix_time):
        return dt.datetime.fromtimestamp(unix_time).strftime('%Y')

    def generate_dateAMPM(unix_time):
        return dt.datetime.fromtimestamp(unix_time).strftime('%I:%M%p')

    def generate_hour(unix_time):
        return dt.datetime.fromtimestamp(unix_time).strftime('%H')

    def generate_kWh(val):
        return round(val / (1000 * 60), 5)

    def generate_cost(kwh):
        return round(kwh * singapore_tariff_rate, 5)

    df['month'] = df['unix_time'].apply(generate_month)
    df['year'] = df['unix_time'].apply(generate_year)
    df['dates_AMPM'] = df['unix_time'].apply(generate_dateAMPM)
    df['hours'] = df['unix_time'].apply(generate_hour)
    df['power_kWh'] = df['power'].apply(generate_kWh)
    df['cost'] = df['power_kWh'].apply(generate_cost)
    df['device_type'] = df['device_type'].str.capitalize()
    return df


def initialise_variables(df, type):
    # Initialise some variables
    global singapore_tariff_rate
    singapore_tariff_rate = 0.201

    def generate_month(unix_time):
        return dt.datetime.fromtimestamp(unix_time).strftime('%b')

    def generate_year(unix_time):
        return dt.datetime.fromtimestamp(unix_time).strftime('%Y')

    def generate_dateAMPM(unix_time):
        return dt.datetime.fromtimestamp(unix_time).strftime('%I:%M%p')

    def generate_hour(unix_time):
        return dt.datetime.fromtimestamp(unix_time).strftime('%H')

    def generate_kWh(val):
        return round(val / (1000 * 60), 9)

    def generate_cost(kwh):
        return round(kwh * singapore_tariff_rate, 9)

    if type == 'year':
        df['year'] = df['unix_time'].apply(generate_year)
        df['power_kWh'] = df['power'].apply(generate_kWh)
        df['cost'] = df['power_kWh'].apply(generate_cost)
        df['device_type'] = df['device_type'].str.capitalize()

    else:
        df['month'] = df['unix_time'].apply(generate_month)
        df['year'] = df['unix_time'].apply(generate_year)
        df['dates_AMPM'] = df['unix_time'].apply(generate_dateAMPM)
        df['hours'] = df['unix_time'].apply(generate_hour)
        df['power_kWh'] = df['power'].apply(generate_kWh)
        df['cost'] = df['power_kWh'].apply(generate_cost)
        df['device_type'] = df['device_type'].str.capitalize()
    return df


def _weekFunction(df):
    global df_week_pie, df_week, df_week_random
    df = _initialise_variables(df)

    # end_date = str(datetime.today().strftime('%d/%-m/%Y'))
    df = _initialise_variables(df)
    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    # print(end)

    # Start of week function

    # 1. Filter past 4 weeks using timedelta 28 days
    # For Line Chart

    # Create new dataframe
    df_week_random = df

    # Get names of indexes for which column Date only has values 4 weeks before 29/2/2020
    df_week_random['date'] = pd.to_datetime(df_week_random['date'])
    start = end - dt.timedelta(28)
    mask = (df_week_random['date'] > start) & (df_week_random['date'] <= end)

    # Delete these row indexes from dataFrame
    df_week_random = df_week_random.loc[mask]
    df_week_random.reset_index(drop=True, inplace=True)
    # print(df_week_random)
    # 2. Append new column called Week

    # Df_week for later
    # TODO SettingWithCopyWarning here
    df_week_random['week'] = None

    # 3. Label weeks 1, 2, 3, 4 based on start date

    start = end - dt.timedelta(7)
    # If datetime > start & datetime <= end ==> Week 1
    # idx = df.index[df['BoolCol']] # Search for indexes of value in column
    # df.loc[idx] # Get rows with all the columns
    df_week_random.loc[(df_week_random['date'] > start) & (
        df_week_random['date'] <= end), ['week']] = "{}".format(start.strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(7))) &
                       (df_week_random['date'] <= (end - dt.timedelta(7))), ['week']] = "{}".format(
        (start - dt.timedelta(7)).strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(14))) &
                       (df_week_random['date'] <= (end - dt.timedelta(14))), ['week']] = "{}".format(
        (start - dt.timedelta(14)).strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(21))) &
                       (df_week_random['date'] <= (end - dt.timedelta(21))), ['week']] = "{}".format(
        (start - dt.timedelta(21)).strftime('%d %b'))

    # df_week_random.loc[(df_week_random['date'] > start) & ( df_week_random['date'] <= end), ['week']] = "{} - {
    # }".format(start.strftime('%d %b'), end.strftime('%d %b')) df_week_random.loc[(df_week_random['date'] > (start -
    # dt.timedelta(7))) & (df_week_random['date'] <= (end - dt.timedelta(7))), ['week']] = "{} - {}".format((start -
    # dt.timedelta(7)).strftime('%d %b'), (end - dt.timedelta(7)).strftime('%d %b')) df_week_random.loc[(
    # df_week_random['date'] > (start - dt.timedelta(14))) & (df_week_random['date'] <= (end - dt.timedelta(14))),
    # ['week']] = "{} - {}".format((start - dt.timedelta(14)).strftime('%d %b'), (end - dt.timedelta(14)).strftime(
    # '%d %b')) df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(21))) & (df_week_random['date'] <=
    # (end - dt.timedelta(21))), ['week']] = "{} - {}".format((start - dt.timedelta(21)).strftime('%d %b'),
    # (end - dt.timedelta(21)).strftime('%d %b'))

    df_week_random.reset_index(drop=True, inplace=True)

    # Initiate df for line
    df_week = copy.deepcopy(df_week_random)  # already filtered past 4 weeks
    # already filtered past 4 weeks
    df_week_pie = copy.deepcopy(df_week_random)

    # 4. Aggregate by Week

    # Line Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum',
                             'date': 'first'}  # sum power when combining rows.
    df_week = df_week.groupby(
        ['week'], as_index=False).aggregate(aggregation_functions)
    df_week.reset_index(drop=True, inplace=True)

    # Pie Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_week_pie = df_week_pie.groupby(
        ['device_type', 'week'], as_index=False).aggregate(aggregation_functions)
    df_week_pie.reset_index(drop=True, inplace=True)

    # End of week function

    # df_week_line.to_csv(".\\Data Tables\\df_week_line.csv")
    # df_week_pie.to_csv(".\\Data Tables\\df_week_pie.csv")

    return df_week, df_week_pie


def _hourFunction(df):
    # end_date = '24/7/2020'
    # end_date = str(datetime.today().strftime('%d/%-m/%Y'))
    df = _initialise_variables(df)
    global df_hour_pie, df_hour

    # Line Chart

    # Create new dataframe
    df_hour = copy.deepcopy(df)
    # Get last 24 hours only
    now = datetime.now()
    timezone = pytz.timezone('Asia/Singapore')
    timestamp_now = now.replace(
        tzinfo=timezone).timestamp()
    start = now - dt.timedelta(hours=12)

    # df_hour.to_csv("comapare.csv")
    # mask = (df_hour['date'] > start) & (df_hour['date'] <= today)
    df_hour['date'] = pd.to_datetime(df_hour['date'])
    start = dt.datetime.strptime(str(start), '%Y-%m-%d %H:%M:%S.%f')
    timestamp_start = start.replace(
        tzinfo=timezone).timestamp()
    mask = df_hour['unix_time'] > timestamp_start
    # print(start)
    # print(timestamp_start)

    # Delete these row indexes from dataFrame
    df_hour = df_hour.loc[mask]
    df_hour.reset_index(drop=True, inplace=True)
    # print(df_hour)
    # Create new instance of df for Piechart
    df_hour_pie = copy.deepcopy(df_hour)

    # Line chart
    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum',
                             'dates_AMPM': 'first'}  # sum power when combining rows.
    df_hour = df_hour.groupby(
        ['date', 'hours'], as_index=False).aggregate(aggregation_functions)
    df_hour.reset_index(drop=True, inplace=True)
    # Optional Convert to %d/%m/%Y
    df_hour['date'] = df_hour['date'].dt.strftime('%d/%m/%Y')
    # print(df_hour)
    # df_hour.to_csv('df_hour_test.csv')
    # Pie Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum',
                             'dates_AMPM': 'first'}  # sum power when combining rows.
    df_hour_pie = df_hour_pie.groupby(
        ['date', 'hours', 'device_type'], as_index=False).aggregate(aggregation_functions)
    df_hour_pie.reset_index(drop=True, inplace=True)

    # Optional Convert to %d/%m/%Y
    df_hour_pie['date'] = df_hour_pie['date'].dt.strftime('%d/%m/%Y')
    # End of hour function

    return df_hour, df_hour_pie


def _dayFunction(df):
    global df_day, df_day_pie

    df = _initialise_variables(df)
    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    # Start of Day function
    # Line Chart

    # Create new dataframe
    df_day = df

    # Get last 7 days
    # Get names of indexes for which column Date only has values 7 days before end_date
    df_day['date'] = pd.to_datetime(df_day['date'])

    start = end - dt.timedelta(7)
    mask = (df_day['date'] > start) & (df_day['date'] <= end)
    # Delete these row indexes from dataFrame
    df_day = df_day.loc[mask]
    df_day.reset_index(drop=True, inplace=True)

    # Create df for Piechart
    df_day_pie = copy.deepcopy(df_day)

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum'}  # sum power when combining rows.
    df_day = df_day.groupby(['date'], as_index=False).aggregate(
        aggregation_functions)
    df_day.reset_index(drop=True, inplace=True)

    # Optional Convert to %d/%m/%Y
    df_day['date_withoutYear'] = df_day['date'].dt.strftime('%d/%m')

    # Pie Chart

    # Aggregate data based on device type for past 7 days

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_day_pie = df_day_pie.groupby(
        ['device_type', 'date'], as_index=False).aggregate(aggregation_functions)
    df_day_pie.reset_index(drop=True, inplace=True)
    # End of day function

    return df_day, df_day_pie


def _monthFunction(df):
    global df_month, df_month_pie
    df = _initialise_variables(df)

    # START OF MONTH FUNCTION
    # 1) For Line Chart
    # Create new dataframe
    '''Insert SQL Code'''
    df_month = copy.deepcopy(df)
    '''Output SQL Code'''

    # Filter data for Last 6 months
    # Get names of indexes for which column Date only has values 6 months before 1/2/2020
    df_month['date'] = pd.to_datetime(df_month['date'])
    end = datetime.today().replace(day=1)
    start = end - dateutil.relativedelta.relativedelta(months=5)
    start = start.replace(day=1)
    print(end, 'to', start)

    mask = (df_month['date'] > start) & (df_month['date'] <= end)

    # Delete these row indexes from dataFrame
    df_month = df_month.loc[mask]
    df_month.reset_index(drop=True, inplace=True)
    # Create df_month_pie for Piechart
    df_month_pie = copy.deepcopy(df_month)

    # Aggregate Data into Months based on last 6 months
    aggregation_functions = {'power': 'sum', 'time': 'first',
                             'power_kWh': 'sum', 'unix_time': 'first',
                             'cost': 'sum'}  # sum power when combining rows.
    df_month = df_month.groupby(
        ['month', 'year'], as_index=False).aggregate(aggregation_functions)
    df_month = df_month.sort_values(
        by=['unix_time'], ascending=True, ignore_index=True)
    df_month.reset_index(drop=True, inplace=True)

    # 2) For Pie Chart
    # Aggregate Data into Months based on last 6 months

    aggregation_functions = {'power': 'sum', 'time': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_month_pie = df_month_pie.groupby(
        ['device_type', 'month', 'year'], as_index=False).aggregate(aggregation_functions)
    df_month_pie.reset_index(drop=True, inplace=True)

    # # END OF MONTH FUNCTION

    return df_month, df_month_pie


def _yearFunction(df):
    # end_date = '24/7/2020'
    # end_date = str(datetime.today().strftime('%d/%-m/%Y'))
    type = 'year'
    df = initialise_variables(df, type)
    global df_year_pie, df_year

    # Line Chart

    # Create new dataframe
    df_year = copy.deepcopy(df)
    df_year_pie = df

    # Line chart
    # Aggregate data

    aggregation_functions = {'power': 'sum', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_year = df_year.groupby(
        ['year'], as_index=False).aggregate(aggregation_functions)
    df_year.reset_index(drop=True, inplace=True)
    # # Optional Convert to %d/%m/%Y
    # df_year['date'] = df_year['date'].dt.strftime('%d/%m/%Y')

    # Pie Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_year_pie = df_year_pie.groupby(
        ['year', 'device_type'], as_index=False).aggregate(aggregation_functions)
    df_year_pie.reset_index(drop=True, inplace=True)

    # # Optional Convert to %d/%m/%Y
    # df_year_pie['date'] = df_year_pie['date'].dt.strftime('%d/%m/%Y')
    # End of hour function

    # Add in cost and energy column
    df_year.to_csv('.\\manager_csv\\manager_df_year.csv')
    df_year_pie.to_csv('.\\manager_csv\\manager_df_year_pie.csv')

    return df_year, df_year_pie


def _calculate_cost(power):
    """Convert W into cost"""
    kwh = power / (1000 * 60)
    singapore_tariff_rate = 0.201
    cost = singapore_tariff_rate * kwh
    return cost


def _cost_savings(df):
    """Takes in the raw data file and converts it into the last 6 week/month worth of aggregated data"""
    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby(['date', 'device_type']).sum().reset_index()
    df = df.pivot(index='date', columns='device_type', values='power')

    # Converts watts to dollars and finds the difference between each cell and the average
    def process(df):
        output = pd.DataFrame()
        for col in list(df):
            ls = []
            for index, row in df.iterrows():
                value = row[col]
                location = np.where(df.index == index)[0][0]
                average = df[col][:location].mean()
                value -= average
                value = _calculate_cost(value)
                ls.append(value)
            x = pd.Series(ls, name=col)
            output = pd.concat([output, x], axis=1)
        output.index = df.index
        output['total'] = output.sum(axis=1)
        return output

    # Aggregate data based on view & set index
    week_view = df.groupby(pd.Grouper(freq='W-MON', label='left')).sum()
    month_view = df.groupby(pd.Grouper(freq='M', label='left')).sum()
    week_view = process(week_view)
    month_view = process(month_view)
    # week_view.fillna(0, inplace=True)
    # month_view.fillna(0, inplace=True)

    week_view['week'] = week_view.index
    week_view['week'] = week_view['week'].dt.strftime('%-d %b')
    # week_view = week_view.set_index('week')

    month_view['month'] = month_view.index
    month_view['month'] = month_view['month'].dt.strftime('%b')
    # month_view = month_view.set_index('month')

    if month_view.shape[0] > 7:
        month_view = month_view[-7:-1]
    else:
        month_view = month_view[:-1]
    if week_view.shape[0] > 7:
        week_view = week_view[-7:-1]
    else:
        week_view = week_view[:-1]
    return week_view, month_view


def graph_hourly_update():
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating hourly consumption')
    df = read_all_db()
    hourly_line = pd.DataFrame()
    hourly_pie = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_data, pie_data = _hourFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_data.insert(0, 'user_id', user_id)
        pie_data.insert(0, 'user_id', user_id)
        hourly_line = pd.concat([hourly_line, line_data], ignore_index=True)
        hourly_pie = pd.concat([hourly_pie, pie_data], ignore_index=True)

    # hourly_line.to_csv('.\\users_csv\\hourly_line.csv')
    update_db(hourly_line.reset_index(drop=True), 'historical_today_line')
    update_db(hourly_pie.reset_index(drop=True), 'historical_today_pie')
    # print('Complete hourly update in {} seconds.'.format(datetime.now() - start_time))


def graph_daily_update():
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating daily consumption')
    start_time = datetime.now()
    df = read_all_db()
    daily_line = pd.DataFrame()
    daily_pie = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_data, pie_data = _dayFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_data.insert(0, 'user_id', user_id)
        pie_data.insert(0, 'user_id', user_id)
        daily_line = pd.concat([daily_line, line_data], ignore_index=True)
        daily_pie = pd.concat([daily_pie, pie_data], ignore_index=True)

    daily_line.to_csv('.\\users_csv\\daily_line.csv')

    update_db(daily_line.reset_index(drop=True), 'historical_days_line')
    update_db(daily_pie.reset_index(drop=True), 'historical_days_pie')
    print('Complete daily update in {} seconds.'.format(
        datetime.now() - start_time))


def graph_weekly_monthly_update():
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating weekly and monthly consumption')
    start_time = datetime.now()
    df = read_all_db()
    weekly_line = pd.DataFrame()
    weekly_pie = pd.DataFrame()
    monthly_line = pd.DataFrame()
    monthly_pie = pd.DataFrame()
    weekly_costsavings = pd.DataFrame()
    monthly_costsavings = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_week, pie_week = _weekFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_week.insert(0, 'user_id', user_id)
        pie_week.insert(0, 'user_id', user_id)
        weekly_line = pd.concat([weekly_line, line_week], ignore_index=True)
        weekly_pie = pd.concat([weekly_pie, pie_week], ignore_index=True)

        line_month, pie_month = _monthFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_month.insert(0, 'user_id', user_id)
        pie_month.insert(0, 'user_id', user_id)
        monthly_line = pd.concat([monthly_line, line_month], ignore_index=True)
        monthly_pie = pd.concat([monthly_pie, pie_month], ignore_index=True)

        savings_week, savings_month = _cost_savings(
            df[df['user_id'] == user_id].reset_index(drop=True))
        savings_week.insert(0, 'user_id', user_id)
        savings_month.insert(0, 'user_id', user_id)
        weekly_costsavings = pd.concat(
            [weekly_costsavings, savings_week], ignore_index=True)
        monthly_costsavings = pd.concat(
            [monthly_costsavings, savings_month], ignore_index=True)

    weekly_costsavings.fillna(0, inplace=True)
    monthly_costsavings.fillna(0, inplace=True)

    weekly_line.to_csv('.\\users_csv\\weekly.csv')
    monthly_line.to_csv('.\\users_csv\\monthly.csv')

    update_db(weekly_line.reset_index(drop=True), 'historical_weeks_line')
    update_db(weekly_pie.reset_index(drop=True), 'historical_weeks_pie')
    update_db(monthly_line.reset_index(drop=True), 'historical_months_line')
    update_db(monthly_pie.reset_index(drop=True), 'historical_months_pie')
    update_db(weekly_costsavings.reset_index(drop=True), 'costsavings_weeks')
    update_db(monthly_costsavings.reset_index(drop=True), 'costsavings_months')
    print('Completed weekly and monthly update in {} seconds.'.format(
        datetime.now() - start_time))


def manager_graph_daily_update():
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating MANAGER daily consumption')
    start_time = datetime.now()
    df = manager_read_all_db()
    daily_line = pd.DataFrame()
    daily_pie = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_data, pie_data = _dayFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_data.insert(0, 'user_id', user_id)
        pie_data.insert(0, 'user_id', user_id)
        daily_line = pd.concat([daily_line, line_data], ignore_index=True)
        daily_pie = pd.concat([daily_pie, pie_data], ignore_index=True)

    # daily_line.to_csv('.\\manager_csv\\daily_line.csv')
    daily_pie.to_csv('.\\manager_csv\\manager_daily_piechart.csv')

    # update_db(daily_line.reset_index(drop=True), 'historical_days_line')
    # update_db(daily_pie.reset_index(drop=True), 'historical_days_pie')
    print('Complete daily update in {} seconds.'.format(
        datetime.now() - start_time))


def manager_graph_weekly_monthly_update():
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating MANAGER weekly and monthly consumption')
    start_time = datetime.now()
    df = manager_read_all_db()
    weekly_line = pd.DataFrame()
    weekly_pie = pd.DataFrame()
    monthly_line = pd.DataFrame()
    monthly_pie = pd.DataFrame()
    # weekly_costsavings = pd.DataFrame()
    # monthly_costsavings = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_week, pie_week = _manager_weekFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_week.insert(0, 'user_id', user_id)
        pie_week.insert(0, 'user_id', user_id)
        weekly_line = pd.concat([weekly_line, line_week], ignore_index=True)
        weekly_pie = pd.concat([weekly_pie, pie_week], ignore_index=True)

        line_month, pie_month = _manager_monthFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_month.insert(0, 'user_id', user_id)
        pie_month.insert(0, 'user_id', user_id)
        monthly_line = pd.concat([monthly_line, line_month], ignore_index=True)
        monthly_pie = pd.concat([monthly_pie, pie_month], ignore_index=True)

        # savings_week, savings_month = _cost_savings(
        #     df[df['user_id'] == user_id].reset_index(drop=True))
        # savings_week.insert(0, 'user_id', user_id)
        # savings_month.insert(0, 'user_id', user_id)
        # weekly_costsavings = pd.concat(
        #     [weekly_costsavings, savings_week], ignore_index=True)
        # monthly_costsavings = pd.concat(
        #     [monthly_costsavings, savings_month], ignore_index=True)

    # weekly_costsavings.fillna(0, inplace=True)
    # monthly_costsavings.fillna(0, inplace=True)

    weekly_pie.to_csv('.\\manager_csv\\manager_weekly_piechart.csv')
    monthly_pie.to_csv('.\\manager_csv\\manager_monthly_piechart.csv')

    # update_db(weekly_line.reset_index(drop=True), 'historical_weeks_line')
    # update_db(weekly_pie.reset_index(drop=True), 'historical_weeks_pie')
    # update_db(monthly_line.reset_index(drop=True), 'historical_months_line')
    # update_db(monthly_pie.reset_index(drop=True), 'historical_months_pie')
    # update_db(weekly_costsavings.reset_index(drop=True), 'costsavings_weeks')
    # update_db(monthly_costsavings.reset_index(drop=True), 'costsavings_months')
    print('Completed weekly and monthly update in {} seconds.'.format(
        datetime.now() - start_time))


def _manager_weekFunction(df):
    global df_week_pie, df_week, df_week_random
    df = _initialise_variables(df)

    # end_date = str(datetime.today().strftime('%d/%-m/%Y'))
    df = _initialise_variables(df)
    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    # print(end)

    # Start of week function

    # 1. Filter past 4 weeks using timedelta 28 days
    # For Line Chart

    # Create new dataframe
    df_week_random = df

    # Get names of indexes for which column Date only has values 4 weeks before 29/2/2020
    df_week_random['date'] = pd.to_datetime(df_week_random['date'])
    start = end - dt.timedelta(28)
    mask = (df_week_random['date'] > start) & (df_week_random['date'] <= end)

    # Delete these row indexes from dataFrame
    df_week_random = df_week_random.loc[mask]
    df_week_random.reset_index(drop=True, inplace=True)
    # print(df_week_random)
    # 2. Append new column called Week

    # Df_week for later
    # TODO SettingWithCopyWarning here
    df_week_random['week'] = None

    # 3. Label weeks 1, 2, 3, 4 based on start date

    start = end - dt.timedelta(7)
    # If datetime > start & datetime <= end ==> Week 1
    # idx = df.index[df['BoolCol']] # Search for indexes of value in column
    # df.loc[idx] # Get rows with all the columns
    df_week_random.loc[(df_week_random['date'] > start) & (
        df_week_random['date'] <= end), ['week']] = "{}".format(start.strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(7))) &
                       (df_week_random['date'] <= (end - dt.timedelta(7))), ['week']] = "{}".format(
        (start - dt.timedelta(7)).strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(14))) &
                       (df_week_random['date'] <= (end - dt.timedelta(14))), ['week']] = "{}".format(
        (start - dt.timedelta(14)).strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(21))) &
                       (df_week_random['date'] <= (end - dt.timedelta(21))), ['week']] = "{}".format(
        (start - dt.timedelta(21)).strftime('%d %b'))

    # df_week_random.loc[(df_week_random['date'] > start) & ( df_week_random['date'] <= end), ['week']] = "{} - {
    # }".format(start.strftime('%d %b'), end.strftime('%d %b')) df_week_random.loc[(df_week_random['date'] > (start -
    # dt.timedelta(7))) & (df_week_random['date'] <= (end - dt.timedelta(7))), ['week']] = "{} - {}".format((start -
    # dt.timedelta(7)).strftime('%d %b'), (end - dt.timedelta(7)).strftime('%d %b')) df_week_random.loc[(
    # df_week_random['date'] > (start - dt.timedelta(14))) & (df_week_random['date'] <= (end - dt.timedelta(14))),
    # ['week']] = "{} - {}".format((start - dt.timedelta(14)).strftime('%d %b'), (end - dt.timedelta(14)).strftime(
    # '%d %b')) df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(21))) & (df_week_random['date'] <=
    # (end - dt.timedelta(21))), ['week']] = "{} - {}".format((start - dt.timedelta(21)).strftime('%d %b'),
    # (end - dt.timedelta(21)).strftime('%d %b'))

    df_week_random.reset_index(drop=True, inplace=True)

    # Initiate df for line
    df_week = copy.deepcopy(df_week_random)  # already filtered past 4 weeks
    # already filtered past 4 weeks
    df_week_pie = copy.deepcopy(df_week_random)

    # 4. Aggregate by Week

    # Line Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum',
                             'date': 'first'}  # sum power when combining rows.
    df_week = df_week.groupby(
        ['week'], as_index=False).aggregate(aggregation_functions)
    df_week.reset_index(drop=True, inplace=True)

    # Pie Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_week_pie = df_week_pie.groupby(
        ['device_type', 'week'], as_index=False).aggregate(aggregation_functions)
    df_week_pie.reset_index(drop=True, inplace=True)

    # End of week function

    # df_week_line.to_csv(".\\Data Tables\\df_week_line.csv")
    # df_week_pie.to_csv(".\\Data Tables\\df_week_pie.csv")

    return df_week, df_week_pie


def _manager_dayFunction(df):
    global df_day, df_day_pie

    df = _initialise_variables(df)
    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    # Start of Day function
    # Line Chart

    # Create new dataframe
    df_day = df

    # Get last 7 days
    # Get names of indexes for which column Date only has values 7 days before end_date
    df_day['date'] = pd.to_datetime(df_day['date'])

    start = end - dt.timedelta(7)
    mask = (df_day['date'] > start) & (df_day['date'] <= end)
    # Delete these row indexes from dataFrame
    df_day = df_day.loc[mask]
    df_day.reset_index(drop=True, inplace=True)

    # Create df for Piechart
    df_day_pie = copy.deepcopy(df_day)

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum'}  # sum power when combining rows.
    df_day = df_day.groupby(['date'], as_index=False).aggregate(
        aggregation_functions)
    df_day.reset_index(drop=True, inplace=True)

    # Optional Convert to %d/%m/%Y
    df_day['date_withoutYear'] = df_day['date'].dt.strftime('%d/%m')

    # Pie Chart

    # Aggregate data based on device type for past 7 days

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_day_pie = df_day_pie.groupby(
        ['device_type', 'date'], as_index=False).aggregate(aggregation_functions)
    df_day_pie.reset_index(drop=True, inplace=True)
    # End of day function

    return df_day, df_day_pie


def _manager_monthFunction(df):
    global df_month, df_month_pie
    df = _initialise_variables(df)

    # START OF MONTH FUNCTION
    # 1) For Line Chart
    # Create new dataframe
    '''Insert SQL Code'''
    df_month = copy.deepcopy(df)
    '''Output SQL Code'''

    # Filter data for Last 6 months
    # Get names of indexes for which column Date only has values 6 months before 1/2/2020
    df_month['date'] = pd.to_datetime(df_month['date'])
    end = datetime.today().replace(day=1)
    start = end - dateutil.relativedelta.relativedelta(months=5)
    start = start.replace(day=1)
    print(end, 'to', start)

    mask = (df_month['date'] > start) & (df_month['date'] <= end)

    # Delete these row indexes from dataFrame
    df_month = df_month.loc[mask]
    df_month.reset_index(drop=True, inplace=True)
    # Create df_month_pie for Piechart
    df_month_pie = copy.deepcopy(df_month)

    # Aggregate Data into Months based on last 6 months
    aggregation_functions = {'power': 'sum', 'time': 'first',
                             'power_kWh': 'sum', 'unix_time': 'first',
                             'cost': 'sum'}  # sum power when combining rows.
    df_month = df_month.groupby(
        ['month', 'year'], as_index=False).aggregate(aggregation_functions)
    df_month = df_month.sort_values(
        by=['unix_time'], ascending=True, ignore_index=True)
    df_month.reset_index(drop=True, inplace=True)

    # 2) For Pie Chart
    # Aggregate Data into Months based on last 6 months

    aggregation_functions = {'power': 'sum', 'time': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_month_pie = df_month_pie.groupby(
        ['device_type', 'month', 'year'], as_index=False).aggregate(aggregation_functions)
    df_month_pie.reset_index(drop=True, inplace=True)

    # # END OF MONTH FUNCTION

    return df_month, df_month_pie


def manager_graph_yearly_update():
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating manager yearly consumption')
    start_time = datetime.now()
    df = read_yearly_consumption_db()
    yearly_manager_line = pd.DataFrame()
    yearly_manager_pie = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_data, pie_data = _yearFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_data.insert(0, 'user_id', user_id)
        pie_data.insert(0, 'user_id', user_id)
        yearly_manager_line = pd.concat(
            [yearly_manager_line, line_data], ignore_index=True)
        yearly_manager_pie = pd.concat(
            [yearly_manager_pie, pie_data], ignore_index=True)

    # update_db(yearly_manager_line.reset_index(drop=True),
    #           'historical_manager_years_line')
    # update_db(yearly_manager_pie.reset_index(drop=True),
    #           'historical_manager_years_pie')
    # print('Complete yearly update in {} seconds.'.format(
    #     datetime.now() - start_time))


def managerGetAverageFunction(type=None):
    # Get Average Daily Consumption for Past 6 months

    daily_usage_7m = pd.read_csv('manager_csv/manager_7m_daily.csv')
    weekly_usage_7m = pd.read_csv('manager_csv/manager_7m_weekly.csv')
    monthly_usage_1y = pd.read_csv('manager_csv/manager_1y_monthly.csv')
    yearly_usage_3y = pd.read_csv('manager_csv/manager_df_year.csv')
    daily_usage_7m['date'] = pd.to_datetime(
        daily_usage_7m['date'], format='%Y-%m-%d')

    weekly_usage_7m['date'] = pd.to_datetime(
        weekly_usage_7m['date'], format='%Y-%m-%d')

    monthly_usage_1y['date'] = pd.to_datetime(
        monthly_usage_1y['date'], format='%Y-%m-%d')

    listofDf = [daily_usage_7m, weekly_usage_7m,
                monthly_usage_1y, yearly_usage_3y]
    types = ['daily', 'weekly', 'monthly', 'yearly']

    x = 0
    df_avg = pd.DataFrame(
        [], columns=["id", "type", "avg_energy", "avg_cost", "date"])

    for i in listofDf:
        if types[x] == 'daily':
            df_day_avg = i
            df_last_dates = copy.deepcopy(df_day_avg.tail(7))  # Last 7 days
            # print(df_last_dates)
            for dates in df_last_dates['date']:
                # Filter past 6 months from reference date
                six_months_before_date = dates - \
                    relativedelta.relativedelta(months=6)

                print(dates, ' to ', six_months_before_date)

                # Create mask and filter past 6 months
                mask = (df_day_avg['date'] > six_months_before_date) & (
                    df_day_avg['date'] <= dates)
                # Delete these row indexes from dataFrame
                df_day_avg = df_day_avg.loc[mask]
                df_day_avg.reset_index(drop=True, inplace=True)

                dfSeries = [len(df_avg), types[x], df_day_avg['power_kWh'].mean(),
                            df_day_avg['cost'].mean(), dates]
                dfSeries = pd.Series(dfSeries, index=df_avg.columns)
                df_avg = df_avg.append(dfSeries, ignore_index=True)

        elif types[x] == 'weekly':
            # print('week')
            df_weekly_avg = i
            df_last_dates = copy.deepcopy(
                df_weekly_avg.tail(4))  # Last 4 weeks

            for dates in df_last_dates['date']:
                # Filter past 6 months from reference date
                six_months_before_date = dates - \
                    relativedelta.relativedelta(months=6)

                print(dates, ' to ', six_months_before_date)

                # Create mask and filter past 6 months
                mask = (df_weekly_avg['date'] > six_months_before_date) & (
                    df_weekly_avg['date'] <= dates)
                # Delete these row indexes from dataFrame
                df_weekly_avg = df_weekly_avg.loc[mask]
                df_weekly_avg.reset_index(drop=True, inplace=True)

                dfSeries = [len(df_avg), types[x], df_weekly_avg['power_kWh'].mean(),
                            df_weekly_avg['cost'].mean(), dates]
                dfSeries = pd.Series(dfSeries, index=df_avg.columns)
                df_avg = df_avg.append(dfSeries, ignore_index=True)

        elif types[x] == 'monthly':
            # print('month')
            df_monthly_avg = i
            df_last_dates = copy.deepcopy(
                df_monthly_avg.tail(6))  # Last 6 months

            for dates in df_last_dates['date']:
                # Filter past 6 months from reference date
                six_months_before_date = dates - \
                    relativedelta.relativedelta(months=6)

                print(dates, ' to ', six_months_before_date)

                # Create mask and filter past 6 months
                mask = (df_monthly_avg['date'] > six_months_before_date) & (
                    df_monthly_avg['date'] <= dates)
                # Delete these row indexes from dataFrame
                df_monthly_avg = df_monthly_avg.loc[mask]
                df_monthly_avg.reset_index(drop=True, inplace=True)

                dfSeries = [len(df_avg), types[x], df_monthly_avg['power_kWh'].mean(),
                            df_monthly_avg['cost'].mean(), dates]
                dfSeries = pd.Series(dfSeries, index=df_avg.columns)
                df_avg = df_avg.append(dfSeries, ignore_index=True)

        elif types[x] == 'yearly':

            df_year_avg = i
            for year in df_year_avg['year']:
                dfSeries = [len(df_avg), types[x], df_year_avg['power_kWh'].mean(),
                            df_year_avg['cost'].mean(), dates]
                dfSeries = pd.Series(dfSeries, index=df_avg.columns)
                df_avg = df_avg.append(dfSeries, ignore_index=True)
        x += 1
    df_avg.to_csv(
        ".\\manager_csv\\manager_AverageDailyWeeklyMonthlyYearly.csv")

    update_db(df_avg.reset_index(drop=True), 'building_consumption_summary')


def manager_generate_daily_average(df):
    global df_day, df_day_pie

    df = _initialise_variables(df)

    # Start of Day function
    # Line Chart

    # Create new dataframe
    df_day = df

    # Create df for Piechart
    df_day_pie = copy.deepcopy(df_day)

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum'}  # sum power when combining rows.
    df_day = df_day.groupby(['date'], as_index=False).aggregate(
        aggregation_functions)
    df_day.reset_index(drop=True, inplace=True)

    # Optional Convert to %d/%m/%Y
    df_day['date_withoutYear'] = df_day['date'].dt.strftime('%d/%m')

    # Pie Chart

    # Aggregate data based on device type for past 7 days

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_day_pie = df_day_pie.groupby(
        ['device_type', 'date'], as_index=False).aggregate(aggregation_functions)
    df_day_pie.reset_index(drop=True, inplace=True)
    # End of day function

    return df_day, df_day_pie


def manager_generate_weekly_average(df):
    global df_week_pie, df_week, df_week_random
    df = _initialise_variables(df)

    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    # print(end)
    # Create new dataframe
    df_week_random = copy.deepcopy(df)

    # Start of week function

    # For Line Chart
    aggregation_functions = {'power': 'sum', 'power_kWh': 'sum',
                             'cost': 'sum'}
    df['date'] = pd.to_datetime(df['date']) - pd.to_timedelta(7, unit='d')
    df = df.groupby([pd.Grouper(key='date', freq='W-MON')]
                    ).aggregate(aggregation_functions).reset_index().sort_values('date')

    df_week = df

    # For pie chart
    # Get names of indexes for which column Date only has values 4 weeks before 29/2/2020
    df_week_random['date'] = pd.to_datetime(df_week_random['date'])
    start = end - dt.timedelta(28)
    mask = (df_week_random['date'] > start) & (df_week_random['date'] <= end)

    # Delete these row indexes from dataFrame
    df_week_random = df_week_random.loc[mask]
    df_week_random.reset_index(drop=True, inplace=True)
    # print(df_week_random)
    # 2. Append new column called Week

    # Df_week for later
    # TODO SettingWithCopyWarning here
    df_week_random['week'] = None

    # 3. Label weeks 1, 2, 3, 4 based on start date

    start = end - dt.timedelta(7)
    # If datetime > start & datetime <= end ==> Week 1
    # idx = df.index[df['BoolCol']] # Search for indexes of value in column
    # df.loc[idx] # Get rows with all the columns
    df_week_random.loc[(df_week_random['date'] > start) & (
        df_week_random['date'] <= end), ['week']] = "{}".format(start.strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(7))) &
                       (df_week_random['date'] <= (end - dt.timedelta(7))), ['week']] = "{}".format(
        (start - dt.timedelta(7)).strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(14))) &
                       (df_week_random['date'] <= (end - dt.timedelta(14))), ['week']] = "{}".format(
        (start - dt.timedelta(14)).strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(21))) &
                       (df_week_random['date'] <= (end - dt.timedelta(21))), ['week']] = "{}".format(
        (start - dt.timedelta(21)).strftime('%d %b'))

    df_week_random.reset_index(drop=True, inplace=True)

    # Initiate df for line
    df_week_pie = df_week_random  # already filtered past 4 weeks

    # 4. Aggregate by Week
    # Pie Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_week_pie = df_week_pie.groupby(
        ['device_type', 'week'], as_index=False).aggregate(aggregation_functions)
    df_week_pie.reset_index(drop=True, inplace=True)

    # End of week function

    # df_week_line.to_csv(".\\Data Tables\\df_week_line.csv")
    # df_week_pie.to_csv(".\\Data Tables\\df_week_pie.csv")

    return df_week, df_week_pie


def manager_generate_monthly_average(df):
    global df_month, df_month_pie

    end_date = str(datetime.today().strftime('%d/%m/%Y'))
    df = _initialise_variables(df)

    # START OF MONTH FUNCTION
    # 1) For Line Chart
    # Create new dataframe
    '''Insert SQL Code'''
    df_month = copy.deepcopy(df)
    '''Output SQL Code'''

    # Aggregate Data into Months based on last 6 months
    aggregation_functions = {'power': 'sum', 'time': 'first',
                             'power_kWh': 'sum', 'date': 'first',
                             'cost': 'sum'}  # sum power when combining rows.
    df_month = df_month.groupby(
        ['month', 'year'], as_index=False).aggregate(aggregation_functions)
    df_month = df_month.sort_values(
        by=['date'], ascending=True, ignore_index=True)
    df_month.reset_index(drop=True, inplace=True)

    df_month_pie = copy.deepcopy(df)

    # 2) For Pie Chart
    # Aggregate Data into Months based on last 6 months

    aggregation_functions = {'power': 'sum', 'time': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_month_pie = df_month_pie.groupby(
        ['device_type', 'month', 'year'], as_index=False).aggregate(aggregation_functions)
    df_month_pie.reset_index(drop=True, inplace=True)

    # # END OF MONTH FUNCTION

    return df_month, df_month_pie


def manager_average_graph_daily_update():
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating daily consumption')
    df = manager_read_7m_consumption_db()
    daily_line = pd.DataFrame()
    daily_pie = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_data, pie_data = manager_generate_daily_average(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_data.insert(0, 'user_id', user_id)
        pie_data.insert(0, 'user_id', user_id)
        daily_line = pd.concat([daily_line, line_data], ignore_index=True)
        daily_pie = pd.concat([daily_pie, pie_data], ignore_index=True)

    daily_line.to_csv('.\\manager_csv\\manager_7m_daily.csv')
    daily_pie.to_csv('.\\manager_csv\\manager_7m_daily_pie.csv')
    # update_db(daily_line.reset_index(drop=True), 'historical_days_line')
    # update_db(daily_pie.reset_index(drop=True), 'historical_days_pie')
    # print('Complete daily update in {} seconds.'.format(
    #     datetime.now() - start_time))


def manager_graph_average_monthly_weekly_update():
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating weekly and monthly consumption')
    start_time = datetime.now()
    df = manager_read_7m_consumption_db()
    df2 = manager_read_1y_consumption_db()
    weekly_line = pd.DataFrame()
    weekly_pie = pd.DataFrame()
    monthly_line = pd.DataFrame()
    monthly_pie = pd.DataFrame()

    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_week, pie_week = manager_generate_weekly_average(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_week.insert(0, 'user_id', user_id)
        pie_week.insert(0, 'user_id', user_id)
        weekly_line = pd.concat([weekly_line, line_week], ignore_index=True)
        weekly_pie = pd.concat([weekly_pie, pie_week], ignore_index=True)
        line_month, pie_month = manager_generate_monthly_average(
            df2[df2['user_id'] == user_id].reset_index(drop=True))
        line_month.insert(0, 'user_id', user_id)
        pie_month.insert(0, 'user_id', user_id)
        monthly_line = pd.concat([monthly_line, line_month], ignore_index=True)
        monthly_pie = pd.concat([monthly_pie, pie_month], ignore_index=True)

    monthly_line.to_csv('.\\manager_csv\\manager_1y_monthly.csv')
    monthly_pie.to_csv('.\\manager_csv\\manager_1y_monthly_pie.csv')

    weekly_line.to_csv('.\\manager_csv\\manager_7m_weekly.csv')
    weekly_pie.to_csv('.\\manager_csv\\manager_7m_weekly_pie.csv')
    print('Completed weekly and monthly update in {} seconds.'.format(
        datetime.now() - start_time))


def usersGetAverageFunction(type=None):
    # Get Average Daily Consumption for Past 6 months

    daily_usage_7m = pd.read_csv('users_csv/users_daily_line.csv')
    weekly_usage_7m = pd.read_csv('users_csv/users_weekly_line.csv')
    monthly_usage_1y = pd.read_csv('users_csv/users_monthly_line.csv')
    hourly_usage_7m = pd.read_csv('users_csv/users_hourly_line.csv')
    daily_usage_7m['date'] = pd.to_datetime(
        daily_usage_7m['date'], format='%Y-%m-%d')

    weekly_usage_7m['date'] = pd.to_datetime(
        weekly_usage_7m['date'], format='%Y-%m-%d')

    monthly_usage_1y['date'] = pd.to_datetime(
        monthly_usage_1y['date'], format='%Y-%m-%d')
    hourly_usage_7m['date'] = pd.to_datetime(
        hourly_usage_7m['date'], format='%Y-%m-%d')

    listofDf = [daily_usage_7m, weekly_usage_7m,
                monthly_usage_1y, hourly_usage_7m]
    types = ['daily', 'weekly', 'monthly', 'hourly']

    x = 0
    df_avg = pd.DataFrame(
        [], columns=["id", "user_id", "type", "avg_energy", "avg_cost", "date"])

    user_ids = sorted(daily_usage_7m['user_id'].unique())

    for i in listofDf:
        if types[x] == 'daily':
            for user_id in user_ids:

                df_day_avg = i
                df_day_avg[df_day_avg['user_id'] ==
                           user_id].reset_index(drop=True)

                df_last_dates = copy.deepcopy(
                    df_day_avg.tail(7))  # Last 7 days

                for dates in df_last_dates['date']:
                    # Filter past 6 months from reference date
                    six_months_before_date = dates - \
                        relativedelta.relativedelta(months=6)

                    print(dates, ' to ', six_months_before_date)

                    # Create mask and filter past 6 months
                    mask = (df_day_avg['date'] > six_months_before_date) & (
                        df_day_avg['date'] <= dates)
                    # Delete these row indexes from dataFrame
                    df_day_avg = df_day_avg.loc[mask]
                    df_day_avg.reset_index(drop=True, inplace=True)

                    dfSeries = [len(df_avg), user_id, types[x], df_day_avg['power_kWh'].mean(),
                                df_day_avg['cost'].mean(), dates]
                    dfSeries = pd.Series(dfSeries, index=df_avg.columns)
                    df_avg = df_avg.append(dfSeries, ignore_index=True)

        elif types[x] == 'weekly':
            for user_id in user_ids:

                print('week')
                df_weekly_avg = i

                df_weekly_avg[df_weekly_avg['user_id']
                              == user_id].reset_index(drop=True)

                df_last_dates = copy.deepcopy(
                    df_weekly_avg.tail(4))  # Last 4 weeks

                for dates in df_last_dates['date']:
                    # Filter past 6 months from reference date
                    six_months_before_date = dates - \
                        relativedelta.relativedelta(months=6)

                    print(dates, ' to ', six_months_before_date)

                    # Create mask and filter past 6 months
                    mask = (df_weekly_avg['date'] > six_months_before_date) & (
                        df_weekly_avg['date'] <= dates)
                    # Delete these row indexes from dataFrame
                    df_weekly_avg = df_weekly_avg.loc[mask]
                    df_weekly_avg.reset_index(drop=True, inplace=True)

                    dfSeries = [len(df_avg), user_id, types[x], df_weekly_avg['power_kWh'].mean(),
                                df_weekly_avg['cost'].mean(), dates]
                    dfSeries = pd.Series(dfSeries, index=df_avg.columns)
                    df_avg = df_avg.append(dfSeries, ignore_index=True)

        elif types[x] == 'monthly':
            print('month')
            df_monthly_avg = i
            for user_id in user_ids:

                df_monthly_avg[df_monthly_avg['user_id']
                               == user_id].reset_index(drop=True)

                df_last_dates = copy.deepcopy(
                    df_monthly_avg.tail(6))  # Last 6 months

                for dates in df_last_dates['date']:
                    # Filter past 6 months from reference date
                    six_months_before_date = dates - \
                        relativedelta.relativedelta(months=6)

                    print(dates, ' to ', six_months_before_date)

                    # Create mask and filter past 6 months
                    mask = (df_monthly_avg['date'] > six_months_before_date) & (
                        df_monthly_avg['date'] <= dates)
                    # Delete these row indexes from dataFrame
                    df_monthly_avg = df_monthly_avg.loc[mask]
                    df_monthly_avg.reset_index(drop=True, inplace=True)

                    dfSeries = [len(df_avg), user_id, types[x], df_monthly_avg['power_kWh'].mean(),
                                df_monthly_avg['cost'].mean(), dates]
                    dfSeries = pd.Series(dfSeries, index=df_avg.columns)
                    df_avg = df_avg.append(dfSeries, ignore_index=True)
        elif types[x] == 'hourly':
            print('hour')
            df_hour_avg = i

            for user_id in user_ids:

                df_hour_avg[df_hour_avg['user_id'] ==
                            user_id].reset_index(drop=True)
                df_last_dates = copy.deepcopy(
                    df_hour_avg.tail(12))  # Last 12 hours
                df_last_dates['date'] = pd.to_datetime(df_last_dates['date'])
                df_last_dates = df_last_dates.reset_index(drop=True)
                print(df_last_dates)
                for dates in df_last_dates['date']:
                    # Filter past 6 months from reference date
                    six_months_before_date = dates - \
                        relativedelta.relativedelta(months=6)

                    print(dates, ' to ', six_months_before_date)
                    print(six_months_before_date)
                    # Create mask and filter past 6 months
                    mask = (df_hour_avg['date'] > six_months_before_date) & (df_hour_avg['date'] <= dates
                                                                             )
                    # Delete these row indexes from dataFrame
                    df_hour_avg = df_hour_avg.loc[mask]
                    df_hour_avg.reset_index(drop=True, inplace=True)

                    dfSeries = [len(df_avg), user_id, types[x], df_hour_avg['power_kWh'].mean(),
                                df_hour_avg['cost'].mean(), dates]
                    dfSeries = pd.Series(dfSeries, index=df_avg.columns)
                    df_avg = df_avg.append(dfSeries, ignore_index=True)
        x += 1

    df_avg.to_csv(".\\users_csv\\users_AverageDailyWeeklyMonthlyYearly.csv")
    update_db(df_avg.reset_index(drop=True), 'users_consumption_summary')


'''For Users Baseline'''


def users_hourly_update():
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating hourly consumption')
    start_time = datetime.now()
    df = read_all_db()
    hourly_line = pd.DataFrame()
    hourly_pie = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_data, pie_data = _users_average_hourFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_data.insert(0, 'user_id', user_id)
        pie_data.insert(0, 'user_id', user_id)
        hourly_line = pd.concat([hourly_line, line_data], ignore_index=True)
        hourly_pie = pd.concat([hourly_pie, pie_data], ignore_index=True)

    hourly_line.to_csv('.\\users_csv\\users_hourly_line.csv')
    # update_db(hourly_line.reset_index(drop=True), 'historical_today_line')
    # update_db(hourly_pie.reset_index(drop=True), 'historical_today_pie')
    # print('Complete hourly update in {} seconds.'.format(datetime.now() - start_time))


def users_daily_update():
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating daily consumption')
    start_time = datetime.now()
    df = read_all_db()
    daily_line = pd.DataFrame()
    daily_pie = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_data, pie_data = _users_average_dayFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_data.insert(0, 'user_id', user_id)
        pie_data.insert(0, 'user_id', user_id)
        daily_line = pd.concat([daily_line, line_data], ignore_index=True)
        daily_pie = pd.concat([daily_pie, pie_data], ignore_index=True)

    daily_line.to_csv('.\\users_csv\\users_daily_line.csv')

    # update_db(daily_line.reset_index(drop=True), 'historical_days_line')
    # update_db(daily_pie.reset_index(drop=True), 'historical_days_pie')
    print('Complete daily update in {} seconds.'.format(
        datetime.now() - start_time))


def users_weekly_monthly_update():
    print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating weekly and monthly consumption')
    start_time = datetime.now()
    df = read_all_db()
    weekly_line = pd.DataFrame()
    weekly_pie = pd.DataFrame()
    monthly_line = pd.DataFrame()
    monthly_pie = pd.DataFrame()
    weekly_costsavings = pd.DataFrame()
    monthly_costsavings = pd.DataFrame()
    user_ids = sorted(df['user_id'].unique())

    for user_id in user_ids:
        line_week, pie_week = _users_average_weekFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_week.insert(0, 'user_id', user_id)
        pie_week.insert(0, 'user_id', user_id)
        weekly_line = pd.concat([weekly_line, line_week], ignore_index=True)
        weekly_pie = pd.concat([weekly_pie, pie_week], ignore_index=True)

        line_month, pie_month = _users_average_monthFunction(
            df[df['user_id'] == user_id].reset_index(drop=True))
        line_month.insert(0, 'user_id', user_id)
        pie_month.insert(0, 'user_id', user_id)
        monthly_line = pd.concat([monthly_line, line_month], ignore_index=True)
        monthly_pie = pd.concat([monthly_pie, pie_month], ignore_index=True)

        savings_week, savings_month = _cost_savings(
            df[df['user_id'] == user_id].reset_index(drop=True))
        savings_week.insert(0, 'user_id', user_id)
        savings_month.insert(0, 'user_id', user_id)
        weekly_costsavings = pd.concat(
            [weekly_costsavings, savings_week], ignore_index=True)
        monthly_costsavings = pd.concat(
            [monthly_costsavings, savings_month], ignore_index=True)

    weekly_costsavings.fillna(0, inplace=True)
    monthly_costsavings.fillna(0, inplace=True)

    weekly_line.to_csv('.\\users_csv\\users_weekly_line.csv')
    monthly_line.to_csv('.\\users_csv\\users_monthly_line.csv')

    # update_db(weekly_line.reset_index(drop=True), 'historical_weeks_line')
    # update_db(weekly_pie.reset_index(drop=True), 'historical_weeks_pie')
    # update_db(monthly_line.reset_index(drop=True), 'historical_months_line')
    # update_db(monthly_pie.reset_index(drop=True), 'historical_months_pie')
    # update_db(weekly_costsavings.reset_index(drop=True), 'costsavings_weeks')
    # update_db(monthly_costsavings.reset_index(drop=True), 'costsavings_months')
    print('Completed weekly and monthly update in {} seconds.'.format(
        datetime.now() - start_time))


def _users_average_hourFunction(df):

    df = _initialise_variables(df)
    global df_hour_pie, df_hour

    # Line Chart

    # Create new dataframe
    df_hour = copy.deepcopy(df)

    # print(df_hour)
    # Create new instance of df for Piechart
    df_hour_pie = copy.deepcopy(df_hour)

    # Line chart
    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first',                         'year': 'first', 'power_kWh': 'sum', 'cost': 'sum',
                             'dates_AMPM': 'first'}  # sum power when combining rows.
    df_hour = df_hour.groupby(
        ['date', 'hours'], as_index=False).aggregate(aggregation_functions)
    df_hour.reset_index(drop=True, inplace=True)
    # Optional Convert to %d/%m/%Y
    df_hour['date'] = df_hour['date'].dt.strftime('%Y-%m-%d')
    # Pie Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum',
                             'dates_AMPM': 'first'}  # sum power when combining rows.
    df_hour_pie = df_hour_pie.groupby(
        ['date', 'hours', 'device_type'], as_index=False).aggregate(aggregation_functions)
    df_hour_pie.reset_index(drop=True, inplace=True)

    # Optional Convert to %d/%m/%Y
    df_hour_pie['date'] = df_hour_pie['date'].dt.strftime('%d/%m/%Y')
    # End of hour function

    return df_hour, df_hour_pie


def _users_average_dayFunction(df):
    global df_day, df_day_pie

    df = _initialise_variables(df)
    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    # Start of Day function
    # Line Chart

    # Create new dataframe
    df_day = df

    # Create df for Piechart
    df_day_pie = copy.deepcopy(df_day)

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum'}  # sum power when combining rows.
    df_day = df_day.groupby(['date'], as_index=False).aggregate(
        aggregation_functions)
    df_day.reset_index(drop=True, inplace=True)

    # Optional Convert to %d/%m/%Y
    df_day['date_withoutYear'] = df_day['date'].dt.strftime('%d/%m')

    # Pie Chart

    # Aggregate data based on device type for past 7 days

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_day_pie = df_day_pie.groupby(
        ['device_type', 'date'], as_index=False).aggregate(aggregation_functions)
    df_day_pie.reset_index(drop=True, inplace=True)
    # End of day function

    return df_day, df_day_pie


def _users_average_weekFunction(df):
    global df_week_pie, df_week, df_week_random
    df = _initialise_variables(df)

    # end_date = str(datetime.today().strftime('%d/%-m/%Y'))
    df = _initialise_variables(df)
    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    # print(end)

    # Start of week function
    df_week_random = copy.deepcopy(df)
    # 2. Append new column called Week

    # Df_week for later
    # TODO SettingWithCopyWarning here
    df_week_random['week'] = None

    # 3. Label weeks 1, 2, 3, 4 based on start date

    start = end - dt.timedelta(7)
    # If datetime > start & datetime <= end ==> Week 1
    # idx = df.index[df['BoolCol']] # Search for indexes of value in column
    # df.loc[idx] # Get rows with all the columns
    df_week_random.loc[(df_week_random['date'] > start) & (
        df_week_random['date'] <= end), ['week']] = "{}".format(start.strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(7))) &
                       (df_week_random['date'] <= (end - dt.timedelta(7))), ['week']] = "{}".format(
        (start - dt.timedelta(7)).strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(14))) &
                       (df_week_random['date'] <= (end - dt.timedelta(14))), ['week']] = "{}".format(
        (start - dt.timedelta(14)).strftime('%d %b'))
    df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(21))) &
                       (df_week_random['date'] <= (end - dt.timedelta(21))), ['week']] = "{}".format(
        (start - dt.timedelta(21)).strftime('%d %b'))

    # df_week_random.loc[(df_week_random['date'] > start) & ( df_week_random['date'] <= end), ['week']] = "{} - {
    # }".format(start.strftime('%d %b'), end.strftime('%d %b')) df_week_random.loc[(df_week_random['date'] > (start -
    # dt.timedelta(7))) & (df_week_random['date'] <= (end - dt.timedelta(7))), ['week']] = "{} - {}".format((start -
    # dt.timedelta(7)).strftime('%d %b'), (end - dt.timedelta(7)).strftime('%d %b')) df_week_random.loc[(
    # df_week_random['date'] > (start - dt.timedelta(14))) & (df_week_random['date'] <= (end - dt.timedelta(14))),
    # ['week']] = "{} - {}".format((start - dt.timedelta(14)).strftime('%d %b'), (end - dt.timedelta(14)).strftime(
    # '%d %b')) df_week_random.loc[(df_week_random['date'] > (start - dt.timedelta(21))) & (df_week_random['date'] <=
    # (end - dt.timedelta(21))), ['week']] = "{} - {}".format((start - dt.timedelta(21)).strftime('%d %b'),
    # (end - dt.timedelta(21)).strftime('%d %b'))

    df_week_random.reset_index(drop=True, inplace=True)

    # Initiate df for line
    df_week = copy.deepcopy(df_week_random)  # already filtered past 4 weeks
    # already filtered past 4 weeks
    df_week_pie = copy.deepcopy(df_week_random)

    # 4. Aggregate by Week

    # Line Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum', 'cost': 'sum',
                             'date': 'first'}  # sum power when combining rows.
    df_week = df_week.groupby(
        ['week'], as_index=False).aggregate(aggregation_functions)
    df_week.reset_index(drop=True, inplace=True)

    # Pie Chart

    # Aggregate data

    aggregation_functions = {'power': 'sum', 'month': 'first', 'time': 'first',
                             'year': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_week_pie = df_week_pie.groupby(
        ['device_type', 'week'], as_index=False).aggregate(aggregation_functions)
    df_week_pie.reset_index(drop=True, inplace=True)

    # End of week function

    # df_week_line.to_csv(".\\Data Tables\\df_week_line.csv")
    # df_week_pie.to_csv(".\\Data Tables\\df_week_pie.csv")

    return df_week, df_week_pie


def _users_average_monthFunction(df):
    global df_month, df_month_pie
    df = _initialise_variables(df)

    # START OF MONTH FUNCTION
    # 1) For Line Chart
    # Create new dataframe
    '''Insert SQL Code'''
    df_month = copy.deepcopy(df)
    '''Output SQL Code'''

    # Create df_month_pie for Piechart
    df_month_pie = copy.deepcopy(df_month)

    # Aggregate Data into Months based on last 6 months
    aggregation_functions = {'power': 'sum', 'date': 'first',
                             'power_kWh': 'sum', 'unix_time': 'first',
                             'cost': 'sum'}  # sum power when combining rows.
    df_month = df_month.groupby(
        ['month', 'year'], as_index=False).aggregate(aggregation_functions)
    df_month = df_month.sort_values(
        by=['unix_time'], ascending=True, ignore_index=True)
    df_month.reset_index(drop=True, inplace=True)

    # 2) For Pie Chart
    # Aggregate Data into Months based on last 6 months

    aggregation_functions = {'power': 'sum', 'time': 'first', 'power_kWh': 'sum',
                             'cost': 'sum'}  # sum power when combining rows.
    df_month_pie = df_month_pie.groupby(
        ['device_type', 'month', 'year'], as_index=False).aggregate(aggregation_functions)
    df_month_pie.reset_index(drop=True, inplace=True)

    # # END OF MONTH FUNCTION

    return df_month, df_month_pie


if __name__ == "__main__":

    # graph_hourly_update()
    manager_graph_daily_update()
    manager_graph_weekly_monthly_update()
    # usersGetAverageFunction()
    # managerGetAverageFunction()
