import lsh.utils.strings_utils as string_utils
from nose import tools as nt

def test_remove_all_whitespace():
    # set up
    test_str = 'a b c d'
    expected_results = 'abcd'

    # execution
    actual_results = string_utils.remove_all_whitespace(test_str)

    # asserts
    nt.eq_(expected_results, actual_results)


def test_remove_all_whitespace_str_param_none_return_none():
    # set up
    test_str = None
    expected_results = None

    # execution
    actual_results = string_utils.remove_all_whitespace(test_str)

    # asserts
    nt.eq_(expected_results, actual_results)

def test_remove_all_whitespace_no_white_space_to_remove():
    # set up
    test_str = 'abcd'
    expected_results = 'abcd'

    # execution
    actual_results = string_utils.remove_all_whitespace(test_str)

    # asserts
    nt.eq_(expected_results, actual_results)

def test_tokenize_use_default_delimiter_empty_space():
    # set up
    test_str = 'a b c d'
    expected_results = ['a','b','c','d']

    # execute
    actual_results = string_utils.tokenize(test_str)

    # asserts
    nt.eq_(expected_results, actual_results)

def test_tokenize_use_forward_slash_delimiter():
    # set up
    test_str = 'a/b/c/d'
    expected_results = ['a','b','c','d']

    # execute
    actual_results = string_utils.tokenize(test_str, delimiter='/')

    # asserts
    nt.eq_(expected_results, actual_results)

def test_normalize():
    # set up
    test_str = 'AbCD!.'
    expected_results = 'abcd'

     # execute
    actual_results = string_utils.normalize(test_str)

    # asserts
    nt.eq_(expected_results, actual_results)


def test_normalize_str_param_none_return_none():
    # set up
    test_str = None
    expected_results = None

     # execute
    actual_results = string_utils.normalize(test_str)

    # asserts
    nt.eq_(expected_results, actual_results)