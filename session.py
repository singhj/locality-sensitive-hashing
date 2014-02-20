import os
import logging
import webapp2
from webapp2_extras import sessions
from webapp2_extras import sessions_ndb
class BaseRequestHandler(webapp2.RequestHandler):
    """ Catch any exceptions and log them, including traceback
        todo: need to execute super if debug, and also todo: need to display error page to user
        All other request handlers here inherit from this base class.

        todo: take advantage of webapp2 exception goodies.
        """

    def __init__(self, request, response):
        """ webapp2 needs these reset each handler invocation"""

        self.initialize(request, response)
        logging.getLogger().setLevel(logging.DEBUG)

    # webapp2 sessions
    def dispatch(self):
        # Get a session store for this request.
        self.session_store = sessions.get_store(request=self.request)

        try:
            # Dispatch the request.
            webapp2.RequestHandler.dispatch(self)
        finally:
            # Save all sessions.
            self.session_store.save_sessions(self.response)

    @webapp2.cached_property
    def session(self):
        # Returns a database session (using default cookie?)
        return self.session_store.get_session(name='db_session', factory=sessions_ndb.DatastoreSessionFactory)
