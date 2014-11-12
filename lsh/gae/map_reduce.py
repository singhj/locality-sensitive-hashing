import operator, struct, math, sys, logging, datetime
from lsh.utils.similarity import compute_positive_hash
from lsh.shingles.shingles import _get_list_of_shingles
from repositories.gae.dataset import get_random_bits, calculate_max_hashes

def map(dataset, text, id=None):
    logging.info("OpenLSH > map() called.")
    start = datetime.datetime.utcnow()

    hashes = calculate_max_hashes(dataset.rows, dataset.bands)

    if len(dataset.random_seeds) < hashes:
        dataset.random_seeds = get_random_bits(hashes)
        dataset.put()

    sh_type = dataset.shingle_type
    modulo = dataset.minhash_modulo
    seeds = list(dataset.random_seeds)

    minhashes = calc_minhashes(text, sh_type, hashes, seeds, modulo)

    buckets = []
    buckets_per_band = dataset.buckets_per_band

    for band in xrange(dataset.bands):
        minhashes_in_band = [minhashes[band*dataset.rows + row] for row in xrange(dataset.rows)]
        if len(set(minhashes_in_band)) <= 1:
            buckets.append( (band * buckets_per_band) + hash(minhashes_in_band[0]) % buckets_per_band )

    end = datetime.datetime.utcnow()

    if 0 == (start.second % 20):
        logging.info('id %s, length %d, time %d', id, len(text), int((end-start).total_seconds()))

    for bkt in buckets:
        yield (bkt, '/view/%s/%s' % (dataset.filename, id))

def reduce(key, values):
    logging.info("OpenLSH > reduce() called.")
    yield (key, values)

#TODO Create single methods for the lsh code below and one driver function to call

#LSH functions for calculating minhash signatures, creating list of shingles
def calc_minhashes(parsed_text, sh_type, hashes, seeds, modulo):
    def minhashes_for_shingles(shingles, sh_type, hashes, seeds, modulo):
        def calc_onehash(sh_type, shingle, seed, modulo):
            def c4_hash(shingle):
                h = struct.unpack('<i',shingle)[0]
                return  h % ((sys.maxsize + 1) * 2)
            if sh_type == 'c4':
                return operator.xor(c4_hash(shingle), long(seed)) % modulo
            else:
                return operator.xor(compute_positive_hash(shingle), long(seed)) % modulo

        minhashes = [sys.maxsize for _ in xrange(hashes)]
        for shingle in shingles:
            for hno in xrange(hashes):
                h_value = calc_onehash(sh_type, shingle, seeds[hno], modulo)
                minhashes[hno] = min(h_value, minhashes[hno])
        return minhashes

    shingles = parsed_text.split() if sh_type=='w' else set(_get_list_of_shingles(parsed_text))
    minhashes = minhashes_for_shingles(shingles, sh_type, hashes, seeds, modulo)
    return minhashes