import sys, os, re, time, math, random, struct, zipfile, operator, csv, hashlib, uuid, pdb, types
import settings

from collections import defaultdict
import logging

logging.basicConfig(filename=settings.LOG_FILENAME, level=logging.DEBUG)

try:
    from google.appengine.ext import deferred
except ImportError:
    class deferred(object):
        @staticmethod
        def defer(*args, **kwargs):
            args1 = args[1:]
            args[0](*args1, **kwargs)

from utils.levenshtein import levenshtein
from lsh_matrix import Matrix, MatrixRow
from utils.procache import Cache

class PeerbeltLine(object):
    text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)
    @staticmethod
    def parse(line):
        found_pattern = PeerbeltLine.text_file_pattern.search(line)
        doc_id = found_pattern.group(1)
        html = found_pattern.group(2)
        udata = html.decode("utf-8")
        html = udata.encode("ascii","ignore")
        html = html.replace('\\n',' ').replace('\\t',' ').replace("'", "''")
        return doc_id, html
         
shingle_cache = Cache(max_size = 1)

def lsh_text(LineFormat, zip_reader, filename, matrix_key, text_filename):
    logging.info('<TextWorker filename={filename} text_filename={text_filename}>'\
        .format(filename=filename, text_filename=text_filename))

    text_file_pattern = re.compile('^."id":"([^"]*):html","text":"(.*".*).', flags=re.DOTALL)
    infolist = zip_reader.infolist()
    Matrix._initialize()
    MatrixRow._initialize()
    dataset = Matrix.find(matrix_key)
    for info in infolist:
        if info.filename == text_filename:
            break

    with zip_reader.open(info) as text_reader:
        logging.debug('Reading file %s', info.filename)
        stats = {}
        for line in text_reader:
            doc_id, text = LineFormat.parse(line)
            doc = dataset.create_doc(doc_id, text, stats)
            stats = {}
    logging.info('</TextWorker filename={filename} text_filename={text_filename}>'\
        .format(filename=filename, text_filename=text_filename))

def lsh_zipfile(LineFormat, zip_reader, source, filename, file_key = ''):
    infolist = zip_reader.infolist()
    dummydoc = MatrixRow.create()            # force the creation of the table
    dataset = Matrix.create(source, filename, file_key)    # force the creation of the table and filling it with a row
    dataset = Matrix.find(dataset.ds_key)
    start = time.time()
    all_stats = defaultdict(float)
    new_docs_count = 0
    docs_cache = Cache(max_size = 15)
    for info in infolist:
        with zip_reader.open(info) as text_reader:
            logging.debug('Reading file %s', info.filename)
            deferred.defer(lsh_text, LineFormat, zip_reader, filename, matrix_key = dataset.ds_key, text_filename = info.filename)
    return

def main():
    """
    Read input zip file, minhash the documents in it and put them in buckets
    The zip file should have been created with data_prep/prepare_blobstore_zips
    """
    try:
        filename = os.path.abspath(sys.argv[1])
    except IndexError:
        print 'filename not provided'
        exit(1)
    try:
        zip_reader = zipfile.ZipFile(filename)
    except IOError:
        print 'unable to read file {file}'.format(file = filename)
        exit(1)
    except zipfile.BadZipfile:
        print 'file {file} is not a zip file'.format(file = filename)
        exit(1)

    lsh_zipfile(PeerbeltLine, zip_reader, 'bash', filename)

if __name__ == "__main__":
    main()
