from operator import xor
import heapq
from lsh.utils.random_number_generator import RNG
from lsh.utils.similarity import jaccard_similarity
from lsh.shingles.shingles import shingle_generator, ShingleType

DEFAULT_NUM_RANDOM_NUMS = 200 #TODO should create a config to set this
DEFAULT_BITS = 32 #TODO should create a config to set this

# we want to use the same random numbers across all documents we check, that is why I'm using
# a thread safe, singleton to generate my random numbers
RANDOM_NUMBERS = RNG.instance(DEFAULT_NUM_RANDOM_NUMS, DEFAULT_BITS)

def run(shingles_set):
    """
    Generates minhash values for each shingle in the given set.
    :param shingles_set: shingles from one document (this set represents one document)
    :return: set of minhash values (long integers) for a given set of shingles (document)
    """

    #basic minhash implementation algorithm steps...
    min_hash_values = []

    if shingles_set:
        for shingle in shingles_set:
            # reset min-heap as each shingle should get it's own min-heap
            # to calculate the minimum hash value
            min_heap = []

            for count in range(0, DEFAULT_NUM_RANDOM_NUMS):

                #step 1: calculate hash values for current shingle
                if count == 0:
                    shingle_hash = calc_hash(shingle)
                else:
                    #TODO perhaps we should abstract this out so that users can configure there
                    #own hashing function(s)?
                    num = RANDOM_NUMBERS[count]
                    shingle_hash = do_xor(calc_hash(shingle), num)

                #step 2: add to min-heap
                heapq.heappush(min_heap, shingle_hash)

            #step 3: select minimum value from min-heap and add to return list
            min_value = heapq.heappop(min_heap) #note, default behavior for pop gives min value
            min_hash_values.append(min_value)

    return set(min_hash_values)

def calc_hash(value):
    return hash(value)

def do_xor(a, b):
    return xor(a,b)