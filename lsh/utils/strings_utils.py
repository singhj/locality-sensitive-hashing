import re
import string

DEFAULT_TOKENIZER_DELIMITER = ' '

def remove_all_whitespace(str):
    """
        Strips all whitespace from a given string.
        :return: new string without whitespaces, will return the original string if it is empty or None
    """
    if str:
        return re.sub(r'\s+', '', str)
    else:
        return str

def tokenize(str, delimiter=DEFAULT_TOKENIZER_DELIMITER):
    """
        Splits a string by a given delimiter. Default delimiter is a single whitespace.
        :return: list of string tokens, will return the original string if it is empty or None
    """

    if str:
        return str.split(delimiter)
    else:
        return str

def normalize(str):
    """
        Normalizes the string making string all lower case and removes all punctuation.
        :param str: string to be normalized
        :return: normalized string, if str is None or empty it returns the original string
    """

    if str:
        if isinstance(str, unicode):
            not_letters_or_digits = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~'
            translate_to = u''
            translate_table = dict((ord(char), translate_to) for char in not_letters_or_digits)
            return str.translate(translate_table)
        else:
            return str.lower().translate(string.maketrans("",""), string.punctuation)
    else:
        return str

def get_stem(word):
    #TODO: Research stemming libraries and implement method using library functions
    return word