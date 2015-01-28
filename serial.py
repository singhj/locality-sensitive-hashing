import sys, os, re, time, math, random, struct, zipfile, operator, csv, hashlib, uuid, pdb, types
import settings

from collections import defaultdict
import logging

logging.basicConfig(filename=settings.LOG_FILENAME, level=logging.DEBUG)

from utils.levenshtein import levenshtein
from lsh_matrix import Matrix, MatrixRow
from utils.procache import Cache

text_file_pattern = re.compile('^{"id":"([^"]*):html","text":"(.*)}', flags=re.DOTALL)
shingle_cache = Cache(max_size = 1)

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

    infolist = zip_reader.infolist()
    dummydoc = MatrixRow.create()            # force the creation of the table
    dataset = Matrix.create('bash', filename)    # force the creation of the table and filling it with a row
    # logging.debug('%s %s', dataset.ds_key, dataset.filename)
    dataset = Matrix.find(dataset.ds_key)
    start = time.time()
    all_stats = defaultdict(float)
    new_docs_count = 0
    docs_cache = Cache(max_size = 15)
    for info in infolist:
        with zip_reader.open(info) as file_reader:
            logging.debug('Reading file %s', info.filename)
            stats = {}
            for line in file_reader.readlines():
                found_pattern = text_file_pattern.search(line)
                doc_id = found_pattern.group(1)
                html = found_pattern.group(2)
                udata = html.decode("utf-8")
                html = udata.encode("ascii","ignore")
                html = html.replace('\\n',' ').replace('\\t',' ').replace("'", "''")
                doc = dataset.create_doc(doc_id, html, stats)
                docs_cache.set(doc_id, (html, doc.buckets if doc.buckets else [], doc.minhashes))
                if not stats['found']:
                    new_docs_count += 1
                    for stat in stats:
                        if stat != 'found':
                            all_stats[stat] += stats[stat]
                stats = {}
            end = time.time()
            if new_docs_count:
                logging.info('File %s %d seconds, stats: %s over %d docs', info.filename, int(0.5+end-start), all_stats, new_docs_count)
            start = end 
    if new_docs_count:
        for stat in all_stats:
            if stat != 'found':
                all_stats[stat] /= new_docs_count
        logging.info('Average stats: %s over %d docs', all_stats, new_docs_count)
    
    outname = filename.replace('.zip', '.dists.csv')
    doc_ids = docs_cache.keys()
    with open(outname, 'wb') as out_handler:
        fileout = csv.writer(out_handler, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        fileout.writerow(['doc_i', 'doc_j', 'com_bkts', 'jac_dist'])
        for idx in xrange(len(doc_ids)):
            (ihtml, ibkts, imhs) = docs_cache.get(doc_ids[idx])
            for jdx in xrange(idx+1, len(doc_ids)):
                (jhtml, jbkts, jmhs) = docs_cache.get(doc_ids[jdx])
                com_bkts = len(set(ibkts) & set(jbkts))
                jac_dist = 1.0 - reduce(lambda x, y: x+y, map(lambda a,b: a == b, imhs,jmhs)) / float(len(imhs)) 
                lev_dist = ''
                logging.debug(' %s | %s, %3d %6.3f %s %s', doc_ids[idx], doc_ids[jdx], 
                              com_bkts, jac_dist, lev_dist, sorted(list(set(ibkts) & set(jbkts))))
                csv_line = [doc_ids[idx], doc_ids[jdx], com_bkts, jac_dist, lev_dist]
                csv_line.extend(sorted(list(set(ibkts) & set(jbkts))))
                fileout.writerow(csv_line)

if __name__ == "__main__":
    main()
