import sys

def compute_positive_hash(value):
    return hash(value) % ((sys.maxsize + 1) * 2)

def jaccard_similarity(set1, set2):
    return __calculate_jaccard_similairty(set1, set2)

def __calculate_jaccard_similairty(set1, set2):

    if not set1 or len(set1) == 0:
        return 0

    if not set2 or len(set2) == 0:
        return 0

    x = len(set1.intersection(set2))
    y = len(set1.union(set2))

    return x / float(y)
