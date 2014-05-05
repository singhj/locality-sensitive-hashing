

def jaccard_similarity(set1, set2):
    return _calculate_jaccard_similairty(set1, set2)

def __calculate_jaccard_similairty(set1, set2):
    x = len(set1.intersection(set2))
    y = len(set1.union(set2))

    return x / float(y)