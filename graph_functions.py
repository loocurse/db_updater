from database_read_write import *
import pandas as pd
import datetime as dt
import dateutil.relativedelta
import copy
from datetime import date

pd.set_option('mode.chained_assignment', None)


def _initialise_variables(df):
    # Initialise some variables
    global singapore_tariff_rate
    singapore_tariff_rate = 0.201

    def generate_month(unix_time):
        return dt.datetime.utcfromtimestamp(unix_time).strftime('%b')

    def generate_year(unix_time):
        return dt.datetime.utcfromtimestamp(unix_time).strftime('%Y')

    def generate_dateAMPM(unix_time):
        return dt.datetime.utcfromtimestamp(unix_time).strftime('%I:%M%p')

    def generate_hour(unix_time):
        return dt.datetime.utcfromtimestamp(unix_time).strftime('%H')

    def generate_kWh(val):
        return round(val / 1000, 3)

    def generate_cost(kwh):
        return round(kwh * singapore_tariff_rate, 4)

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
    print(end)

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
    print(df_week_random)
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
    df_week = df_week_random  # already filtered past 4 weeks
    df_week_pie = df_week_random  # already filtered past 4 weeks

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
    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    # Line Chart

    # Create new dataframe
    df_hour = df

    # Get last 24 hours only

    df_hour['date'] = pd.to_datetime(df_hour['date'])

    mask = (df_hour['date'] == end)

    # Delete these row indexes from dataFrame
    df_hour = df_hour.loc[mask]
    df_hour.reset_index(drop=True, inplace=True)

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

    # print(df_hour.head(), ' Hour Line Chart')
    # print(df_hour_pie.head(), ' Hour Pie Chart')

    # df_hour.to_csv(".\\Data Tables\\df_hour_line.csv")
    # df_hour_pie.to_csv(".\\Data Tables\\df_hour_pie.csv")

    return df_hour, df_hour_pie


def _dayFunction(df):
    global df_day, df_day_pie

    # end_date = '24/7/2020'
    # end_date = str(datetime.today().strftime('%d/%-m/%Y'))
    df = _initialise_variables(df)
    today = date.today()

    end = today.strftime("%d/%m/%Y")
    end = dt.datetime.strptime(end, '%d/%m/%Y')
    print(end)
    # Start of Day function
    # Line Chart

    # Create new dataframe
    df_day = df

    # Get last 7 days
    # Get names of indexes for which column Date only has values 7 days before end_date
    df_day['date'] = pd.to_datetime(df_day['date'])
    # end = end_date
    # end = dt.datetime.strptime(end, '%d/%m/%Y')
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

    # print(df_day.head(), ' Day Line Chart')
    # print(df_day_pie.head(), ' Day Pie Chart')
    df_day.to_csv(".\\Data Tables\\df_day_line.csv")
    df_day_pie.to_csv(".\\Data Tables\\df_day_pie.csv")

    return df_day, df_day_pie


def _monthFunction(df):
    global df_month, df_month_pie
    # end_date = '24/07/2020'
    end_date = str(datetime.today().strftime('%d/%m/%Y'))
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
    end = end_date
    end_first_day_date = dt.datetime.strptime(
        end, '%d/%m/%Y').replace(day=1)

    start = end_first_day_date - \
        dateutil.relativedelta.relativedelta(months=5)

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
    # print(df_month.head(), ' Month Line Chart')
    # print(df_month_pie.head(), ' Month Pie Chart')

    # '''Output to Server for All Users'''
    # df_month.to_csv(".\\Data Tables\\df_month_line.csv")
    # df_month_pie.to_csv(".\\Data Tables\\df_month_pie.csv")

    return df_month, df_month_pie

    # END OF MONTH FUNCTION
    # print(df_month.head(), ' Month Line Chart')
    # print(df_month_pie.head(), ' Month Pie Chart')

    '''Output to Server for All Users'''
    # df_month.to_csv(".\\Data Tables\\df_month_line.csv")
    # df_month_pie.to_csv(".\\Data Tables\\df_month_pie.csv")

    return df_month, df_month_pie


def _calculate_cost(power):
    """Convert W into cost"""
    kwh = power / 1000
    singapore_tariff_rate = 0.201
    cost = singapore_tariff_rate * kwh
    return cost


def _cost_savings(df):
    """Takes in the raw data file and converts it into the last 6 week/month worth of aggregated data"""
    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby(['date', 'device_type']).sum().reset_index()
    df = df.pivot(index='date', columns='device_type', values='power')

    # Converts watts to dollars and finds the difference between each cell and the average
    for col in list(df):
        df[col] = df[col].apply(_calculate_cost)
        df[col] = df[col] - df[col].mean()

    # Calculate total
    df['total'] = df.sum(axis=1)

    # Aggregate data based on view & set index
    week_view = df.groupby(pd.Grouper(freq='W-MON')).sum()
    month_view = df.groupby(pd.Grouper(freq='M')).sum()

    week_view['week'] = week_view.index
    week_view['week'] = week_view['week'].dt.strftime('%-d %b')
    # week_view = week_view.set_index('week')

    month_view['month'] = month_view.index
    month_view['month'] = month_view['month'].dt.strftime('%b')
    # month_view = month_view.set_index('month')
    # print(month_view.shape)
    if month_view.shape[0] < 3:
        return week_view[-7:-1], month_view
    else:
        return week_view[-7:-1], month_view[-7:-1]


def _cumulative_savings(user_id):
    """Calculates the cumulative savings and uploads the value to the database"""
    df = get_entire_table()
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    week_view = df.groupby(pd.Grouper(freq='W-MON')).sum()['power']
    # week_view = week_view.apply(_calculate_cost)
    total_savings = 0
    # print('hello')
    for num, item in enumerate(week_view.tolist()):
        avg = sum(week_view.tolist()[:num])/(num+1)
        # print(item)
        saving = avg - item
        # print(saving)
        if saving > 0:
            total_savings += saving
    kwh = round(total_savings / 1000, 3)
    cost = round(_calculate_cost(total_savings), 2)
    trees = round(cost/2, 2)
    return kwh, cost, trees


if __name__ == '__main__':
    print(_cumulative_savings(pd.read_csv('tables_csv/generator_6m.csv')))


def graph_hourly_update():
    print(
        f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Updating hourly consumption')
    start_time = datetime.now()
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

    update_db(weekly_line.reset_index(drop=True), 'historical_weeks_line')
    update_db(weekly_pie.reset_index(drop=True), 'historical_weeks_pie')
    update_db(monthly_line.reset_index(drop=True), 'historical_months_line')
    update_db(monthly_pie.reset_index(drop=True), 'historical_months_pie')
    update_db(weekly_costsavings.reset_index(drop=True), 'costsavings_weeks')
    update_db(monthly_costsavings.reset_index(drop=True), 'costsavings_months')
    print('Completed weekly and monthly update in {} seconds.'.format(
        datetime.now() - start_time))
