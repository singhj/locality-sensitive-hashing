import datetime
import logging

from repositories.gae.dataset import get_random_bits, calculate_max_hashes

class LshMapBase(object):

    @classmethod
    def map(cls, data):

        #pre process data
        dataset, _id, text = cls.pre_process(data)
        ds_key = dataset.key

        start = datetime.datetime.utcnow()

        hashes = calculate_max_hashes(dataset.rows, dataset.bands)

        if len(dataset.random_seeds) < hashes:
            dataset.random_seeds = get_random_bits(hashes)
            dataset.put()

        sh_type = dataset.shingle_type
        modulo = dataset.minhash_modulo
        seeds = list(dataset.random_seeds)

        minhashes = calc_minhashes(_id, text, ds_key, sh_type, hashes, seeds, modulo)

        buckets = []
        buckets_per_band = dataset.buckets_per_band
        for band in xrange(dataset.bands):
            minhashes_in_band = [minhashes[band*dataset.rows + row] for row in xrange(dataset.rows)]
            if len(set(minhashes_in_band)) <= 1:
                buckets.append( (band * buckets_per_band) + hash(minhashes_in_band[0]) % buckets_per_band )

        end = datetime.datetime.utcnow()
        if 0 == (start.second % 20):
            logging.info('id %s, length %d, time %d', _id, len(text), int((end-start).total_seconds()))

        for bkt in buckets:
            yield (bkt, '/view/%s/%s' % (dataset.filename, _id))

    @classmethod
    def pre_process(cls, data):
        pass