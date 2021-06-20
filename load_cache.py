from datetime import datetime

import pybaseball as pybll


pybll.cache.enable()
START_DATE = "2021-04-01"
TODAY = str(datetime.now().date())
ENFOREMENT_DATE = "2021-06-15"

data = pybll.statcast("2021-04-01", TODAY)
