from operator import xor
from lsh.utils.random_number_generator import RNG
import heapq

DEFAULT_NUM_RANDOM_NUMS = 200 #TODO should create global config to set this
DEFAULT_BITS = 32 #TODO should create global config to set this

RANDOM_NUMBERS = RNG.instance(DEFAULT_NUM_RANDOM_NUMS, DEFAULT_BITS)

def run(shingles_generator):

    #basic minhash implementation algorithm steps...
    min_hash_values = []

    if shingles_generator:
        for shingle in shingles_generator():
            # reset min-heap as each shingle should get it's own min-heap
            # to calculate the minimum hash value
            min_heap = []

            for count in range(0, DEFAULT_NUM_RANDOM_NUMS):

                #step 1: calculate hash values for current shingle
                if count == 0:
                    shingle_hash = calc_hash(shingle)
                else:
                    num = RANDOM_NUMBERS[count]
                    shingle_hash = do_xor(calc_hash(shingle), num)

                #step 2: add to min-heap
                heapq.heappush(min_heap, shingle_hash)

            #step 3: select minimum value from min-heap and add to return list
            min_value = heapq.heappop(min_heap) #note, default behavior for pop gives min value
            min_hash_values.append(min_value)

    return min_hash_values

def calc_hash(value):
    return hash(value)

def do_xor(a, b):
    return xor(a,b)