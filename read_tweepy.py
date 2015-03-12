import session, time, datetime, logging, hashlib, json
from collections import defaultdict
from inspect import currentframe, getframeinfo

import tweepy
from tweepy import StreamListener
from tweepy.api import API
import settings, twitter_settings

from google.appengine.api import users
from google.appengine.ext import ndb

from utils.deferred import deferred
from models import *
from lsh_matrix import *

DEFAULT_NUM_TWEETS = 600
TWEET_BATCH_SIZE = 40

def get_tweets(duik, old_duik):
    this_app = AppOpenLSH.get_or_insert('KeyOpenLSH')
    auth = tweepy.OAuthHandler(this_app.twitter_consumer_key, this_app.twitter_consumer_secret)
    auth.set_access_token(this_app.twitter_access_token_key, this_app.twitter_access_token_secret)
    api = tweepy.API(auth)
    listen = TwitterStatusListener(duik, old_duik, auth)
    stream = tweepy.Stream(auth, listen)

    try:
        stream.sample()
    except tweepy.TweepError:
        logging.error("error with streaming api")
        stream.disconnect()
    return (listen.tweets)

class TwitterStatusListener(StreamListener):

    def __init__(self, duik, old_duik, auth):
        api = tweepy.API(auth)
        StreamListener.__init__(self, api=api)
        self.api = api or API()

        self.old_duik = old_duik
        self.duik = duik
        self.matrix = None

        frameinfo = getframeinfo(currentframe())
        logging.debug('file %s, line %s auth %s duik, %s', frameinfo.filename, frameinfo.lineno+1, auth, duik)

    def on_connect(self):
        """Called once connected to streaming server.
        This will be invoked once a successful response
        is received from the server. Allows the listener
        to perform some work prior to entering the read loop.
        """
        self.tweets = list()
        self.cursor = 0
        dui = ndb.Key(urlsafe = self.duik).get()
        if dui:
            dui.indicate_fetch_begun()
        frameinfo = getframeinfo(currentframe())
        logging.debug('file %s, line %s Twitter connection ready', frameinfo.filename, frameinfo.lineno+1)

    def launch_lsh_calc(self):
        # store tweets and kick off run_lsh

        tw_from = self.cursor
        tw_till = len(self.tweets)
        dui = ndb.Key(urlsafe = self.duik).get()
        dui = dui.extend_tweets(self.tweets[tw_from:tw_till])
        self.cursor = len(self.tweets)

        if not self.matrix:
            Matrix._initialize()
            MatrixRow._initialize()
            self.matrix = Matrix.create(filename = dui.filename(), source = 'tweets', file_key = self.duik)
            if self.matrix:
                dui = dui.set_ds_key(self.matrix.ds_key)
        if self.matrix:
            timestamp = datetime.datetime.utcnow().isoformat()
            deferred.defer(run_lsh, self.duik, self.tweets[tw_from:tw_till], self.matrix.ds_key, tw_from, timestamp)
        else:
            frameinfo = getframeinfo(currentframe())
            logging.error('file %s, line %s Matrix is missing', frameinfo.filename, frameinfo.lineno+1)
            

    def on_status(self, status):
        """Called when a new status arrives"""

        text = status.text #.encode('utf-8')
        self.tweets.append(text) 

        if len(self.tweets) < DEFAULT_NUM_TWEETS:
            if len(self.tweets) % TWEET_BATCH_SIZE == 0:
                self.launch_lsh_calc()
            return True
        else:
            self.launch_lsh_calc()
            self.wrapup()
            return False # this should trigger closing the connection

    def on_error(self, status_code):
        frameinfo = getframeinfo(currentframe())
        logging.error('file %s, line %s Twitter Error %s', frameinfo.filename, frameinfo.lineno+1, str(status_code))
        return False

    def on_timeout(self):
        frameinfo = getframeinfo(currentframe())
        logging.warning('file %s, line %s Twitter Timeout sleep(60)', frameinfo.filename, frameinfo.lineno+1)
        time.sleep(60)
        return
    
    def wrapup(self):
        # Done getting the new tweets, delete the old dui
        if self.old_duik:
            old_dui = ndb.Key(urlsafe = self.old_duik).get()
            old_dui.purge()
            old_dui.key.delete()
        dui = ndb.Key(urlsafe = self.duik).get()
        if dui:
            dui.indicate_fetch_ended()

class TwitterLogin(session.BaseRequestHandler):
    def get(self):
        this_app = AppOpenLSH.get_or_insert('KeyOpenLSH')
        if not this_app.twitter_consumer_key:
            this_app = this_app.twitter_consumer(twitter_settings.consumer_key, twitter_settings.consumer_secret)
#         this_app = this_app.twitter_access_token()
        auth = tweepy.OAuthHandler(this_app.twitter_consumer_key, this_app.twitter_consumer_secret)
        # Redirect user to Twitter to authorize
        url = auth.get_authorization_url()
        logging.debug("TwitterLogin url=%s", url)
        self.session['request_token_key'] = auth.request_token.key
        self.session['request_token_secret'] = auth.request_token.secret
        logging.debug('request_token_key %s, request_token_secret %s',  self.session['request_token_key'], self.session['request_token_secret'])
        self.redirect(url)

    def post(self):
        self.get()

class TwitterLogout(session.BaseRequestHandler):
    def get(self):
        if users.is_current_user_admin():
            this_app = AppOpenLSH.get_or_insert('KeyOpenLSH')
            this_app.twitter_access_token()
            self.session['tw_logged_in'] = False
        self.redirect('/')
        return

    def post(self):
        self.get()

class TwitterCallback(session.BaseRequestHandler):
    def get_args(self):
        """
        All the args a request was called with
        """
        rqst = self.request
        args = rqst.arguments()
        resp = {}
        for arg in args:
            resp[arg] = repr(rqst.get_all(arg))
        return resp

    def get(self):
        resp = self.get_args()
        rqst = self.request
        verifier = rqst.get('oauth_verifier')

        this_app = AppOpenLSH.get_or_insert('KeyOpenLSH')
        auth = tweepy.OAuthHandler(this_app.twitter_consumer_key, this_app.twitter_consumer_secret)
        auth.set_request_token(self.session['request_token_key'], self.session['request_token_secret'])

        try:
            auth.get_access_token(verifier)
            self.session['auth.access_token.key'] = auth.access_token.key
            self.session['auth.access_token.secret'] = auth.access_token.secret

            this_app.twitter_access_token(auth.access_token.key, auth.access_token.secret)
            self.session['tw_logged_in'] = True
            self.session['tweets'] = []

            frameinfo = getframeinfo(currentframe())
            logging.debug('file %s, line %s auth %s %s', frameinfo.filename, frameinfo.lineno+1, auth.access_token.key, auth.access_token.secret)
        except tweepy.TweepError:
            logging.error('Error! Failed to get access token.')
        
        self.redirect('/')

class TwitterAgent(session.BaseRequestHandler):
    def post(self):
        try:
            this_app = AppOpenLSH.get_or_insert('KeyOpenLSH')
            auth = tweepy.OAuthHandler(this_app.twitter_consumer_key, this_app.twitter_consumer_secret)
            auth.set_access_token(this_app.twitter_access_token_key, this_app.twitter_access_token_secret)
            api = tweepy.API(auth)
            if api:
                self.session['tw_logged_in'] = True
#                 this_app.twitter_access_token_key = self.session['auth.access_token.key']
#                 this_app.twitter_access_token_secret = self.session['auth.access_token.secret']
#                 this_app.put()
            else:
                logging.error('API not found')
                self.session['tw_logged_in'] = False
                this_app.twitter_access_token()

                raise NotLoggedIn("Not logged in into twitter")
            
            u = users.get_current_user()

            old_dui = DemoUserInteraction.latest_for_user(u)
            if old_dui:
                old_duik = old_dui.key.urlsafe()
            else:
                old_duik = None

            demo_user_key = ndb.Key(DemoUser, u.user_id())
            demo_user = demo_user_key.get()
            demo_user = demo_user.refresh()

            dui = DemoUserInteraction(parent = demo_user_key)
            duikey = dui.put()
            self.session['duik'] = duikey.urlsafe() if dui else None

            authkey = this_app.twitter_access_token_key
            authsec = this_app.twitter_access_token_secret
#             frameinfo = getframeinfo(currentframe())
#             logging.info('file %s, line %s auth %s %s', frameinfo.filename, frameinfo.lineno+1, authkey, authsec)
    
            ################################
            deferred.defer(get_tweets, self.session['duik'], old_duik)
            ################################
            start = time.time()
            fetching = False
            for i in xrange(5):
                time.sleep((1+i)**2) # Time for the fetches to get going, exponential back off
                dui = dui.refresh()
                if dui.fetching:
                    fetching = True
                    logging.info('Fetch began within %6.2f seconds, proceeding...', time.time()-start)
                    break
            if not fetching:
                logging.error('Fetch did not begin within %6.2f seconds, ???', time.time()-start)
            self.redirect('/')
        
        except:
            frameinfo = getframeinfo(currentframe())
            logging.error('file %s, line %s Exception', frameinfo.filename, frameinfo.lineno+1)
            self.redirect('/')
            return
        
        self.redirect('/')

class TweetLine(object):
    @staticmethod
    def parse(tweet_tuple):
        doc_id = str(tweet_tuple[0])
        text = tweet_tuple[1]
        text = text.lower()
        text = text.replace('http://t.co/','')
        text = ' '.join(text.split())
        return doc_id, text

def run_lsh(duik, tweets, ds_key, offset, timestamp):
    def tweets_generator(tweets, offset, number):
        line_count = 0
        while line_count < number:
            try:
                yield line_count+offset, tweets[line_count]
                line_count += 1
            except IndexError:
                frameinfo = getframeinfo(currentframe())
                logging.error('file %s, line %s, NumTweets %s, offset %s, line_count %s',\
                              frameinfo.filename, frameinfo.lineno+1, len(tweets), offset, line_count)
                raise StopIteration

    tweets_iterator = tweets_generator(tweets, offset, len(tweets))

    duikey = ndb.Key(urlsafe = duik)
    dui = duikey.get()
    if len(dui.tweets) < offset+len(tweets):
        frameinfo = getframeinfo(currentframe())
        logging.error('file %s, line %s needed %s tweets but dui has %s',\
                     frameinfo.filename, frameinfo.lineno+1, offset+len(tweets), len(dui.tweets))

    matrix = Matrix.find(ds_key)
    if matrix:
        dui = dui.indicate_calc_begun()
        all_stats = defaultdict(float)
        logging.debug('<TextWorker filename={filename} tweets received so far={count}>'\
                     .format(filename = matrix.filename, count = len(dui.tweets)))
        line_count = 0
        for line in tweets_iterator:
            stats = {}
            doc_id, text = TweetLine.parse(line)
            line_count += 1
            doc = matrix.create_doc(doc_id, text, stats)
            for stat in stats:
                all_stats[stat] += stats[stat]
            stats = dict()
        logging.debug('</TextWorker filename={filename} stats={batch_stats}>'\
                     .format(filename = matrix.filename, batch_stats = all_stats))
        dui = dui.indicate_calc_ended(all_stats)
    else:
        frameinfo = getframeinfo(currentframe())
        logging.error('Matrix %s not found, file %s, line %s', dui.filename(), frameinfo.filename, frameinfo.lineno+1)
        return

class pair_similarity(object):
    def __init__(self, div, text1, text2, sim):
        self.div = div
        self.text1 = text1
        self.text2 = text2
        self.sim = sim

def lsh_report(duik, ds_key):

    def report(duik, tweet_set_buckets, tweet_sets, matrix_bands):

        def set_line_report(tweet_text_dict, ttdk):
            """ report one tweet or maybe a bunch of identical ones """
            num_identicals = len(tweet_text_dict[ttdk])
            rpt_identicals = '' if (num_identicals == 1) else (' (%d identical)' % num_identicals)
            return '<p>&nbsp;&nbsp;&nbsp;&nbsp; %s%s</p>' % (ttdk, rpt_identicals)
        def set_pair_report(tweet_text_dict, ttdk1, ttdk2, sim):
            """ report one tweet or maybe a bunch of identical ones """
            def quotify(string):
                retval = string.replace('"', '\\"').replace('\n',' ')
                return retval
            div_id = '%s-%s' % (tweet_text_dict[ttdk1][0], tweet_text_dict[ttdk2][0])
            psim = pair_similarity(div_id, quotify(ttdk1), quotify(ttdk2), sim)
            return psim

        msg_same = ''
        similar_sets = []
        same_sets = []
        dui = ndb.Key(urlsafe = duik).get()
        tweets = dui.tweets
        accounted_ids = list()
        for set_hash in tweet_set_buckets:
            tweet_ids = tweet_sets[set_hash]
            tweet_text_dict = defaultdict(list)
            for twid in tweet_ids:
                tweet_text_dict[tweets[twid]].append(twid)

            max_tweet_len = max([len(t) for t in tweet_text_dict])
            if max_tweet_len < 4:
                # For very short tweets, nothing would have been shingled, so buckets are meaningless
                continue

            if len(tweet_text_dict.keys()) == 1:
                accounted_ids.extend(tweet_ids)
                for ttdk in tweet_text_dict:
                    same_sets.append((len(tweet_ids), ttdk,))
            else:
#                 sub_msg = ''
                similar_set = []
                for ttdk1 in tweet_text_dict:
                    for ttdk2 in tweet_text_dict:
                        if ttdk1 < ttdk2:
#                             frameinfo = getframeinfo(currentframe())
#                             logging.debug('visiting file %s, line %s', frameinfo.filename, frameinfo.lineno)
#                             logging.debug('tweet_id pair %s, %s')
                            (id1, tw1) = TweetLine.parse((tweet_text_dict[ttdk1][0], ttdk1,))
                            (id2, tw2) = TweetLine.parse((tweet_text_dict[ttdk1][0], ttdk2,))
                            sh1 = MatrixRow.shingle_text(tw1, settings.shingle_type)
                            sh2 = MatrixRow.shingle_text(tw2, settings.shingle_type)
                            try:
                                similarity = float(len(sh1 & sh2)) / float(len(sh1 | sh2))
                            except ZeroDivisionError:
                                similarity = 0.0
                            if similarity < 0.125:
                                continue
                            accounted_ids.extend(tweet_text_dict[ttdk1])
                            accounted_ids.extend(tweet_text_dict[ttdk2])
                            div_id = '%s-%s' % (tweet_text_dict[ttdk1][0], tweet_text_dict[ttdk2][0])
                            similar_set.append(set_pair_report(tweet_text_dict, ttdk1, ttdk2, int(0.5 + 100 * similarity)))
                if len(similar_set) > 0:
                    similar_sets.append(similar_set)
                    
        return similar_sets, same_sets, set(accounted_ids)

    try:
        matrix = Matrix.find(ds_key)
#     logging.debug(str(matrix))
        matrix_rows = matrix.find_child_rows()
    except AttributeError:
        logging.error('Unable to find matrix_rows for ds_key %s', ds_key)
        raise
    dui = ndb.Key(urlsafe = duik).get()
    dui_id = ndb.Key(urlsafe = duik).id()
#     logging.debug('LshTweets %s, %d, %d rows', dui, dui_id, len(matrix_rows))
    bucket_tweets = defaultdict(list)

    row_count = 0
    for matrix_row in matrix_rows:
        row_count += 1
        for bkt in matrix_row.buckets:
            bucket_tweets[bkt].append(int(matrix_row.doc_id))
    bkt_count = len(bucket_tweets.keys())
    logging.debug('LshTweets %s for %d rows has %d buckets', dui, row_count, bkt_count)
    tweet_sets = {}
    tweet_set_buckets = defaultdict(list)
    for bkt in bucket_tweets:
        if len(bucket_tweets[bkt]) > 1:
            tweet_ids = bucket_tweets[bkt]
            composite_set_key = '|'.join([str(_id) for _id in sorted(tweet_ids)])
            set_hash = '%07d' % (int(hashlib.md5(composite_set_key).hexdigest(), 16) % 10000000)
            tweet_sets[set_hash] = tweet_ids
            tweet_set_buckets[set_hash].append(bkt)

    similar_sets, same_sets, accounted_ids = report(duik, tweet_set_buckets, tweet_sets, matrix.bands)
    return similar_sets, same_sets, accounted_ids
            
class LshTweets(session.BaseRequestHandler):
    @staticmethod
    def show(session):
        duik = session['duik']
        dui = ndb.Key(urlsafe = duik).get() if duik else None
        try:
            ds_key = dui.ds_key
            session['lsh_results'] = lsh_report(duik, ds_key)
        except AttributeError: 
            session['lsh_results'] = 'Error has occurred. Staff has been notified.'
            logging.error('LshTweets.show unable to find dui key %s', duik)

urls = [
     ('/twitter_login', TwitterLogin),
     ('/twitter_callback', TwitterCallback),
     ('/twitter_read_node', TwitterAgent),
     ('/twitter_logout', TwitterLogout),
]
