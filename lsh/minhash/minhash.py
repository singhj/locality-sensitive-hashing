from operator import xor
import heapq
from lsh.utils.similarity import compute_positive_hash

from lsh.utils.random_number_generator import RNG

DEFAULT_NUM_RANDOM_NUMS = 200 #TODO should create a config to set this
DEFAULT_BITS = 32 #TODO should create a config to set this

# we want to use the same random numbers across all documents we check, that is why I'm using
# a thread safe, singleton to generate my random numbers
RANDOM_NUMBERS = RNG.instance(DEFAULT_NUM_RANDOM_NUMS, DEFAULT_BITS)

def run(shingles_list):
    """
    Generates minhash values for each shingle in the given list.
    :param shingles_list: shingles from one document (this list represents one document)
    :return: list of minhash values (long integers) for a given list of shingles (document)
    """

    #basic minhash implementation algorithm steps...
    min_hash_values = []

    if shingles_list:
        for shingle in shingles_list:
            # reset min-heap as each shingle should get it's own min-heap
            # to calculate the minimum hash value
            min_heap = []

            for count in range(0, DEFAULT_NUM_RANDOM_NUMS):
                #step 1: calculate hash values for current shingle
                if count == 0:
                    shingle_hash = calc_hash(shingle)
                else:
                    #TODO refactor so that users can specify their own hashing function(s)
                    num = RANDOM_NUMBERS[count]
                    shingle_hash = do_xor(calc_hash(shingle), num)

                #step 2: add to min-heap
                heapq.heappush(min_heap, shingle_hash)

            #step 3: select minimum value from min-heap and add to return list
            min_value = heapq.heappop(min_heap) #note, default behavior for pop gives min value
            min_hash_values.append(min_value)

    return min_hash_values

#TODO refactor so that users can specify their own hashing function(s)
def calc_hash(value):
     return compute_positive_hash(value)

def do_xor(a, b):
    return xor(a,b)