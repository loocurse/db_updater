import pandas as pd
from datetime import date, timedelta, datetime
from numpy.random import binomial, normal
from time import mktime


class PowerGenerator:
    def __init__(self):
        self.start_date = None
        self.end_date = date.today()
        self.time_delta = None
        self.plug_id = 0
        self.considered_days = [x for x in range(1, 32)]
        self.num_of_users = 1

        # Columns of dataframe
        self._date = []
        self._time = []
        self._unix_time = []
        self._device_state = []
        self._power = []
        self._energy = []
        self._user_id = []
        self._meter_id = []
        self._device_type = []

        # Factors (plm = plug load means, mf = month factor)
        self.plm = dict(desktop=20,
                        monitor=16,
                        tasklamp=4,
                        laptop=12,
                        fan=3,
                        others=5
                        )
        self.mf = [1.3,  # Jan
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
                   ]

    def _generate_list(self):
        """
        Checks if user has input a start_date or time_delta, and generates the 
        appropriate list of dates that the generator will be 

        Args: None

        Returns:
            list: list of dates that the generator will generate from

        Raises:
            ValueError: If timedelta or start_date not defined, raise an error
        """
        if self.start_date != None:
            delta = self.end_date - self.start_date  # as timedelta
            self.dates = [self.start_date +
                          timedelta(days=i) for i in range(delta.days + 1)]
        elif self.time_delta != None:
            self.start_date = self.end_date - self.time_delta
            delta = self.time_delta
            self.dates = [self.start_date +
                          timedelta(days=i) for i in range(delta.days + 1)]
        else:
            raise ValueError('Indicate either a timedelta or a start date')

    def _generate_data_for_user(self, user):
        """
        Generate data for one user, and appends it into the object's lists

        Args:
            user (int): the user_id

        Returns: None
        """
        self._generate_list()
        for plugload in list(self.plm):
            self.plug_id += 1
            for day in self.dates:  # iteration through each day
                month_factor = self.mf[day.month - 1]
                plug_load_mean = self.plm[plugload]
                probability_hour = [0.00,  # 12am
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
                                    ]

                # Weekend factor
                if day.weekday() == 5:  # Saturday
                    probability_hour = [i * 0.05 for i in probability_hour]
                if day.weekday() == 6:  # Sunday
                    probability_hour = [0 for i in probability_hour]

                # iteration through each hour
                for hour, prob in enumerate(probability_hour):
                    rng = binomial(1, prob, 60) * normal(plug_load_mean, 2,
                                                         60) * month_factor  # random number generator
                    # make 0's have a normal dist
                    usage = [x if x > 0 else abs(normal(0, 0.3)) for x in rng]
                    self._device_state += [1 if x > 0 else 0 for x in rng]

                    # iteration through each day
                    for minute, usage in enumerate(usage):
                        if day.day in self.considered_days:
                            timestamp = datetime(
                                day.year, day.month, day.day, hour, minute, 20)

                            # Adding data
                            self._date.append(timestamp.strftime('%Y-%m-%d'))
                            self._time.append(timestamp.strftime('%H:%M:%S'))
                            self._unix_time.append(
                                mktime(timestamp.timetuple()))
                            self._power.append(round(usage, 2))
                            self._energy.append(round(usage * 0.7, 2))
                            self._user_id.append(user)
                            self._meter_id.append(self.plug_id)
                            self._device_type.append(plugload)

    def generate_data(self):
        """
        Generates data for us

        Args: None
        Returns: None
        """
        for user in range(self.num_of_users):
            self._generate_data_for_user(user)

    def to_df(self):
        
        df = pd.DataFrame(
            list(zip(self._date, self._time, self._unix_time, self._meter_id, self._user_id, self._energy, self._power,
                     self._device_state, self._device_type)),
            columns=['date', 'time', 'unix_time', 'meter_id', 'user_id', 'energy', 'power', 'device_state',
                     'device_type'])
        return df

    def to_csv(self, filename):
        df = self.to_df()
        df.to_csv(filename, index=False)


if __name__ == '__main__':
    generator = PowerGenerator()
    # generator.time_delta = timedelta(days=2)
    generator.start_date = date(2020,1,1)
    generator.end_date = date(2020,1,5)
    generator.num_of_users = 3
    generator.generate_data()
    generator.to_csv('data_generator/example.csv')
