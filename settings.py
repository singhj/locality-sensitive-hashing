import sys, os, logging

dir_path = os.path.dirname([p for p in sys.path if p][0])
LOG_FILENAME = dir_path+'/Serial.log'

max_bits = 32
max_mask = 2**max_bits - 1

DATABASES = {
    'default': {
        'ENGINE': '',       # Add 'cassandra' or 'gae' or leave blank
        'KEYSPACE': 'datathinks',    # Keyspace if using Cassandra
        'NAME': '',     # Or path to database file if using sqlite3.
        'USER': '',     # 
        'PASSWORD': '', # Not used with sqlite3.
        'HOST': '',     # Set to empty string for localhost
        'PORT': '',     # Set to empty string for default.
    },
}
    
