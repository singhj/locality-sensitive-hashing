DATABASES = {
    'default': {
        'ENGINE': 'cassandra',       # Add 'cassandra' or 'gae' or leave blank
        'KEYSPACE': 'datathinks',    # Keyspace if using Cassandra
        'NAME': '',     # Or path to database file if using sqlite3.
        'USER': '',     # 
        'PASSWORD': '', # Not used with sqlite3.
        'HOST': '',     # Set to empty string for localhost
        'PORT': '',     # Set to empty string for default.
    },
}
    
