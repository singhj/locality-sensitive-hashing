from nose import tools as nt
from lsh.utils.similarity import jaccard_similarity

def test_sets_exact_match_returns_1():
    #set up
    faux_set_1 = set(["abcdef"])
    faux_set_2 = set(["abcdef"])

    #execute
    results = jaccard_similarity(faux_set_1, faux_set_2)

    #asserts
    nt.eq_(results, 1.0)

def test_sets_not_similar_returns_0():
    #set up
    faux_set_1 = set(["abcdef"])
    faux_set_2 = set(["test_set_not_similar"])

    #execute
    results = jaccard_similarity(faux_set_1, faux_set_2)

    #asserts
    nt.eq_(results, 0.0)

def test_sets_at_least_50_percent_similar():
    #set up
    faux_set_1 = set(["abcdef"])
    faux_set_2 = set(["abcd", "abcdef"])

    #execute
    results = jaccard_similarity(faux_set_1, faux_set_2)

    #asserts
    assert results >= .50

def test_both_sets_empty():
    #set up
    faux_set_1 = set([])
    faux_set_2 = set([])

    #execute
    results = jaccard_similarity(faux_set_1, faux_set_2)

    #asserts
    nt.eq_(results,0)