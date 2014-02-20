from google.appengine.ext import db
import webapp2


class Like(db.Model):
    like_id = db.StringProperty(required=True)
    name = db.StringProperty(required=True)
    def __str__(self):
        return '%s(%s)' % (self.like_id, self.name)
    @db.ComputedProperty
    def url(self):
        return 'http://www.facebook.com/pages/w/%s' % self.like_id
 
class User(db.Model):
    user_id = db.StringProperty(required=True)
    access_token = db.StringProperty(required=True, default='')
    name = db.StringProperty(required=True)
    picture = db.StringProperty(required=True)
    email = db.StringProperty()
    friends = db.StringListProperty()
    likes = db.StringListProperty()
    dirty = db.BooleanProperty()
    date_updated = db.DateTimeProperty(auto_now=True, auto_now_add=True)
    date_added = db.DateTimeProperty(auto_now=False, auto_now_add=True)
    
    def __str__(self):
        return '%s(%s)' % (self.user_id, self.name)

class bulkdelete(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        colls = ['Like', 'User', 
                 "FileMetadata",
                 "_AE_MR_MapreduceControl",
                 "_AE_MR_MapreduceState",
                 "_AE_MR_ShardState",
                 "_AE_MR_TaskPayload",
                 "_AE_MR_MapreduceControl",
                 "_AE_MR_ShardState",
                 ]
        for coll in colls:
            q = db.GqlQuery("SELECT __key__ FROM " + coll)
            if 0 == q.count():
                break
            db.delete(q.fetch(5000))

urls = [
     ('/clean', bulkdelete),
]
