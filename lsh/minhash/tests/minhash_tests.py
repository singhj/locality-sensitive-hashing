"""
 The following functions tests the expected results of the shingling + minhash algorithms. These are not
 unit tests as they are not testing the logic in minhash.py functions.

 These are functions we can run to quickly (yet informally) verify that changes we made to the algorithm don't
 break it. Will create a more formal method for testing algorithm correctness later.
"""

from lsh.utils.similarity import jaccard_similarity
from lsh.shingles.shingles import shingle_generator, ShingleType
from lsh.minhash.minhash import run

def faux_generator_string():
    yield "abcde fghijcklm nop."

def faux_generator_string_2():
    yield "abcdefghi jcklmnop qrs!"


def test_similarity_of_two_sets_using_k_shingles():

    print ".....Testing k-shingles (shingling, minhash & calc jaccard similarity)\n"

    min_values_list_k_shingles = None
    for shingle, original_document in shingle_generator(faux_generator_string()):
        print shingle
        min_values_list_k_shingles = run(shingle)
        print "number of min_hash values -> %s" % str(len(min_values_list_k_shingles))
        print min_values_list_k_shingles
        print

    min_values_list_k_shingles_2 = None
    for shingle, original_document in shingle_generator(faux_generator_string_2()):
        print shingle
        min_values_list_k_shingles_2 = run(shingle)
        print "number of min_hash values -> %s" % str(len(min_values_list_k_shingles_2))
        print min_values_list_k_shingles_2
        print

    # calculate jaccard similarity - should be approx 82% similar
    similarity_ratio = jaccard_similarity(set(min_values_list_k_shingles), set(min_values_list_k_shingles_2))
    print "Asserting jaccard similarity should be ~82%\n"

    assert similarity_ratio >= .82

def faux_generator_string_words():
    yield "do or do not there is no try!"

def faux_generator_string_words_2():
    yield "do or do not there is no try...or just give up!"


def test_similarity_of_two_sets_using_w_shingles():

    print ".....Testing w-shingles (shingling, minhash & calc jaccard similarity)\n"

    min_values_list_w_shingles = None
    for shingle, original_document in shingle_generator(faux_generator_string_words(), type=ShingleType.W_SHINGLES):
        print shingle
        min_values_list_w_shingles = run(shingle)
        print "number of min_hash values -> %s" % str(len(min_values_list_w_shingles))
        print min_values_list_w_shingles
        print

    min_values_list_w_shingles_2 = None
    for shingle, original_document in shingle_generator(faux_generator_string_words_2(), type=ShingleType.W_SHINGLES):
        print shingle
        min_values_list_w_shingles_2 = run(shingle)
        print "number of min_hash values -> %s" % str(len(min_values_list_w_shingles_2))
        print min_values_list_w_shingles_2
        print

    # calculate jaccard similarity - should be approx 44% similar
    similarity_ratio = jaccard_similarity(set(min_values_list_w_shingles), set(min_values_list_w_shingles_2))
    print "Asserting jaccard similarity should be ~44%\n"

    assert similarity_ratio >= .44