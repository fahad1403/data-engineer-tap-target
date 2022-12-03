# Data Pipeline Project

**Taps** - The data source from which data will be extracted is known as the tap. There are ready-made Taps available on the **Singer** site which can be used as it is, or custom taps can also be created. For this project I have built a custom tap to get astronomy data from sunrise-sunset.org/api. 

**API Documentation**: https://sunrise-sunset.org/api

### The tap performs the following function:
- Historical Load : loads historical data since 1 Jan 2020
- Incremental Load : Append today’s data to existing target
- Transform data : Transform the timestamp from UTC to IST 

To load the data to destination I have used **Target**. This is a pre-built target to load data:

meltano / target-sqlite · GitLab : https://gitlab.com/meltano/target-sqlite


___
## Run a tap with target specifying function name

#### Once inside the target-sqlite directory, open the terminal and run below command
- for historical load:
```
python3 ../tap_code/main.py historical_load | target-sqlite --config config.json
```
  - for incremental load:
```
python3 ../tap_code/main.py incremental_load | target-sqlite --config config.json
```

### Info:
The tap for historical load has async implementation which is commented out for time being, where async requests are sent for faster retrieval of data as historical load requires sending lot of request for each date to get data.
