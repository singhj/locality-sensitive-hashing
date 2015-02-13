import sys, os, logging

dir_path = os.path.dirname([p for p in sys.path if p][0])
LOG_FILENAME = dir_path+'/Serial.log'

max_bits = 32
max_mask = 2**max_bits - 1

bands=15            # Number of buckets per document 
rows=5              # Number of minhashes that should be combined to give us the bucket ID.
shingle_type='c4'   # Whether we are using word shingles (w) or character shingles (c4, for four characters)
minhash_modulo=7001 # A large divisor

DATABASES = {
    'default': {
        'ENGINE': 'datastore',       # Add 'cassandra' or 'datastore' or leave blank
        'KEYSPACE': 'datathinks',    # Keyspace if using Cassandra

        # the stuff below this line is not used at the moment but may come in handy if we ever user other dbs
        'NAME': '',     # Or path to database file if using sqlite3.
        'USER': '',     #
        'PASSWORD': '', # Not used with sqlite3.
        'HOST': '',     # Set to empty string for localhost
        'PORT': '',     # Set to empty string for default.
    },
}

from os import environ
env_is_appengine = 'INSTANCE_ID' in environ.keys()

try:
    app_version = os.environ['CURRENT_VERSION_ID']
    isProd = True
except:
    app_version = 'local'
    isProd = False
gaOthr = 'UA-26503514-4'
gaProd = 'UA-26503514-4'
gaCode = gaProd if isProd else gaOthr

# env2url = {
#         'isProd': 'http://open-lsh.datathinks.org/',
#         'local': 'http://localhost:8080',
#         }
# url2env = dict((v,k) for k, v in env2url.iteritems())
# 
# extName = env2url['isProd']
# application_id = app_identity.get_application_id()
# application_name = 'OpenLSH'

