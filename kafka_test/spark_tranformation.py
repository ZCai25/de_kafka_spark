import re
import pandas as pd
from pyspark.sql.functions import udf


# Extracting host pattern
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'

# Extracting timestamps
ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s[+-]\d{4})\]'

# Extracting HTTP 
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'

# Extracting HTTP Status Codes 
status_pattern = r'\s(\d{3})\s'

# Extracting HTTP Response Content Size
content_size_pattern = r'\s(\d+)$'

# fucntion for parsing the time 

from pyspark.sql.functions import udf

month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(text):
    """ Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(text[7:11]),
      month_map[text[3:6]],
      int(text[0:2]),
      int(text[12:14]),
      int(text[15:17]),
      int(text[18:20])
    )


