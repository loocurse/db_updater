# DB updater

## Schedule
| Type   | Activity           | Description                                                                                | Scheduling frequency     |
|--------|--------------------|--------------------------------------------------------------------------------------------|----------------------    |
| daily  | daily_presence     | Activate presence-based control for your devices today                                     | Triggered by User Interaction |
| daily  | daily_schedule     | Use schedule-based control for your devices today                                          | Triggered by User Interaction |
| daily  | daily_remote       | Use remote control while you are away from your desk today                                 | Triggered by User Interaction |

| daily  | lower_energy_con   | Clock a lower energy consumption than yesterday                                            | Every weekday at 11:50pm |
| daily  | turn_off_leave     | Turn off your plug loads when you leave your desk for a long period of time during the day | Every 15 mins            |
| daily  | turn_off_end       | Turn off your plug loads during at the end of the day                                      | Every weekday at 11:50pm |
| daily  | complete_all_daily | Complete all daily achievements                                                            | Every weekday at 11:50pm |
| weekly | cost_saving        | Clock a higher cost savings than last week                                                 | Every Sunday at 11:50pm  |
| weekly | schedule_based     | Set next week's schedule-based controls                                                    | Every Sunday at 11:50pm  |
| weekly | complete_daily     | Complete all daily achievements for 4 days of the week                                     | Every Friday at 11:50pm  |
| weekly | complete_weekly    | Complete all weekly achievements                                                           | Every Friday at 11:50pm  |
| bonus  | tree_first         | Save your first tree                                                                       | Every day at 11:50pm     |
| bonus  | tree_fifth         | Save your fifth tree                                                                       | Every day at 11:50pm     |
| bonus  | tree_tenth         | Save your tenth tree                                                                       | Every day at 11:50pm     |
| bonus  | redeem_reward      | Redeem your first reward from your rewards page                                            | Triggered by User Interaction |
| bonus  | first_remote       | Try out our remote control feature for the first time                                      | Triggered by User Interaction |
| bonus  | first_schedule     | Set your first schedule-based setting                                                      | Triggered by User Interaction |
| bonus  | first_presence     | Set your first presence-based setting                                                      | Triggered by User Interaction |
