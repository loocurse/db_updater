# Power Energy Generator
## Brief
The power energy generator is an object that generates data for the `power_energy_consumption` table for the plug mate project.
## Examples
There are two ways of generating data: either by setting a start date, or by setting the time difference. The default for the end date is today.
##### Setting time delta
Generate data for one user for two years, from two days ago to today, and export it to a csv file. 
```python
    generator = PowerGenerator()
    generator.time_delta = timedelta(days=365*2)
    generator.generate_data()
    generator.to_csv('example.csv')
```
##### Setting start date
Generate data for five users from the start of 2020 till today, and save it to variable df.
```python
    from datetime import date

    generator = PowerGenerator()
    generator.start_date = date(2020,1,1)
    generator.num_of_users = 5
    df = generator.generate_data()
```

## Parameters
#### *start_date*
> Type: `datetime.date` 

Indicates the first date of the generated data
#### *end_date* 
> Type: `datetime.date` 
>
> Default: `datetime.date.today()`

Indicate the end date (inclusive) of the generated data.
#### *time_delta*
> Type: `datetime.timedelta()`

If you did not indicate a start date, you can also include a time delta. If both start date and time delta are assigned, the class will make use of the start date.
#### *plug_id*
> Type: `int`
>
> Default: `0`
> 
Just a place holder to fill in the column. Not yet made dynamic in code
#### *considered_days*
> Type: `list`
>
> Default: `[1,2,3,...,31]`

When generating for long periods of time, the file can get large. This parameter gives you an option to generator a variable number of days in a month. For instance, when `generator.considered_days = [1,2,3]`, the resulting dataset would have Jan 1, Jan 2, Jan 3, Feb 1, Feb 2, Feb 3... days only. 
#### *num_of_users*
> Type: `int`
>
> Default: `1`

Indicates the number of unique user_id's there are. 
#### *plm (plug load mean)*
> Type: `dictionary`
>
> Default: `dict(desktop=20,
                        monitor=16,
                        tasklamp=4,
                        laptop=12,
                        fan=3,
                        others=5
                        )`

The mean energy consumption for each appliance
#### *mf (month factor)*
> Type: `list`
>
> Default: `[1.3,  # Jan
                   1.2,  # Feb
                   1.4,  # Mar
                   1.7,  # Apr
                   1.9,  # May
                   2.1,  # Jun
                   1.7,  # Jul
                   1.4,  # Aug
                   1.3,  # Sep
                   1.4,  # Oct
                   1.4,  # Nov
                   1.3,  # Dec
                   ]`

In order to simulate how there are lawl periods in the year, this parameter multiplies the generated data based on the month by the month factor.

## Other parameters
The following parameters can be changed within the code, but I was lazy to make it a class variable. Therefore, to change it, you'd have to edit it in code itself.

#### *probability_hour* 
> Type: `list`
>
> Default: `[0.00,  # 12am
                                    0.00,  # 1am
                                    0.00,  # 2am
                                    0.00,  # 3am
                                    0.00,  # 4am
                                    0.00,  # 5am
                                    0.00,  # 6am
                                    0.00,  # 7am
                                    0.30,  # 8am
                                    0.50,  # 9am
                                    0.70,  # 10am
                                    0.50,  # 11am
                                    0.25,  # 12pm
                                    0.50,  # 1pm
                                    0.65,  # 2pm
                                    0.60,  # 3pm
                                    0.60,  # 4pm
                                    0.70,  # 5pm
                                    0.60,  # 6pm
                                    0.45,  # 7pm
                                    0.20,  # 8pm
                                    0.10,  # 9pm
                                    0.00,  # 10pm
                                    0.00,  # 11pm
                                    ]`

This represents the factor of multiplication for each hour in the day.
#### Weekend factor
```python
if day.weekday() == 5:  # Saturday
    probability_hour = [i * 0.05 for i in probability_hour]
if day.weekday() == 6:  # Sunday
    probability_hour = [0 for i in probability_hour]
```
These edit the `probability_hour` variable when the generated day falls on a weekend.


        
