from google.appengine.ext import blobstore
import urllib

def get_all_blob_info():
    for blob_info in blobstore.BlobInfo.all():
        blob_key = blob_info.key()
        blob_reader = blobstore.BlobReader(blob_key)

        yield blob_info, blob_reader

def create_upload_url(url):
    updated_url = "/" + url

    return blobstore.create_upload_url(updated_url)

def get_reader(key):
    if not isinstance(key, str):
        updated_key = str(urllib.unquote(key))
    else:
        updated_key = key

    return blobstore.BlobReader(updated_key)

def get_info(key_objs):

    return blobstore.BlobInfo.get(key_objs)