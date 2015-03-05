import logging, json
from inspect import currentframe, getframeinfo
from google.appengine.ext import ndb
from lsh_matrix import *
import twitter_settings

class AppOpenLSH(ndb.Model):
    is_open = ndb.IntegerProperty(default = 1, indexed = False)
    twitter_consumer_key = ndb.StringProperty(default = '', indexed = False)
    twitter_consumer_secret = ndb.StringProperty(default = '', indexed = False)
    twitter_access_token_key = ndb.StringProperty(default = '', indexed = False)
    twitter_access_token_secret = ndb.StringProperty(default = '', indexed = False)

    @ndb.transactional
    def twitter_consumer(self, k='', sec=''):
        key = self.key
        ent = key.get()
        ent.twitter_consumer_key = k
        ent.twitter_consumer_secret = sec
        ent.put()
        return ent

    @ndb.transactional
    def twitter_access_token(self, k='', sec=''):
        key = self.key
        ent = key.get()
        ent.twitter_access_token_key = k
        ent.twitter_access_token_secret = sec
        ent.put()
        return ent

class DemoUser(ndb.Model):
    email = ndb.StringProperty()
    nickname = ndb.StringProperty()

class DemoUserInteraction(ndb.Model):
    asof = ndb.DateTimeProperty(auto_now_add=True)
    ds_key = ndb.StringProperty()
    fetching = ndb.IntegerProperty(default = 0)
    calculating = ndb.IntegerProperty(default = 0)
    calc_done = ndb.BooleanProperty(default = False)
    tweets = ndb.PickleProperty(default = [])
    calc_stats = ndb.TextProperty()

    def filename(self):
        return 'user_id: {user_id}, asof: {asof}'.format(asof = self.asof.isoformat()[:19], user_id = self.key.parent().id())

    @ndb.transactional
    def indicate_fetch_begun(self):
        key = self.key
        ent = key.get()
        ent.fetching += 1
        ent.put()
        return ent

    @ndb.transactional
    def indicate_fetch_ended(self):
        key = self.key
        ent = key.get()
        ent.fetching -= 1
        ent.put()
        return ent

    @ndb.transactional
    def indicate_calc_begun(self):
        key = self.key
        ent = key.get()
        if 0 == ent.calculating:
            ent.calc_stats = json.dumps(dict())
        ent.calculating += 1
        ent.calc_done = False
        ent.put()
        return ent

    @ndb.transactional
    def indicate_calc_ended(self, batch_stats):
        key = self.key
        ent = key.get()
        ent.calculating -= 1
#         logging.debug('%d', self.calculating)
        if 0 == ent.calculating:
            ent.calc_done = True
        calc_stats = json.loads(ent.calc_stats)
        for stat in batch_stats:
            if stat not in calc_stats.keys():
                calc_stats[stat] = 0.0
            calc_stats[stat] += batch_stats[stat]
        ent.calc_stats = json.dumps(calc_stats)
        ent.put()
        logging.info('<indicate_calc_ended %d %s %s/>', ent.calculating, ent.calc_done, ent.calc_stats)
        return ent
    
    @ndb.transactional
    def extend_tweets(self, tweets):
        key = self.key
        ent = key.get()
        ent.tweets.extend(tweets)
        ent.put()
        return ent

    @ndb.transactional
    def set_ds_key(self, ds_key):
        key = self.key
        ent = key.get()
        if not ent.ds_key:
            ent.ds_key = ds_key
            ent.put()
        else:
            if ds_key != ent.ds_key:
                logging.error('changing ds_key from %s to %s? Makes no sense!', ent.ds_key, ds_key)
        return ent

    @staticmethod
    def latest_for_user(user):
        if not user:
            return None
        frameinfo = getframeinfo(currentframe())
        logging.info('file %s, line %s, user_id %s', frameinfo.filename, frameinfo.lineno+1, user.user_id())
        demo_user_key = ndb.Key(DemoUser, user.user_id())
        frameinfo = getframeinfo(currentframe())
        logging.info('file %s, line %s', frameinfo.filename, frameinfo.lineno+1)
        dui = DemoUserInteraction.query(ancestor=demo_user_key).order(-DemoUserInteraction.asof).get()
        frameinfo = getframeinfo(currentframe())
        logging.info('file %s, line %s', frameinfo.filename, frameinfo.lineno+1)
        return dui

    def purge(self):
        if self.ds_key:
            matrix = Matrix.find(ds_key = self.ds_key)
            if matrix:
                matrix.purge()

