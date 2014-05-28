from mock import patch
from nose import tools as nt
from nose.tools import raises
from lsh.shingles.shingles import ShingleType
import lsh.shingles.shingles as shgl

@patch('lsh.shingles.shingles._k_shingles_generator')
def test_shingle_generator_k_shingles_yield_set_of_strings(mock_k_shingles_gen):
    # set up
    type = ShingleType.K_SHINGLES
    size = 4

    faux_set = get_faux_set_of_k_shingles()
    faux_string_generator = generator_string()

    mock_k_shingles_gen.return_value = yield faux_set

    # execute
    actual_results = next(shgl.shingle_generator(faux_string_generator, size=size, type=type))

    # asserts
    mock_k_shingles_gen.assert_called_once_with(faux_string_generator, size)
    nt.eq_(actual_results, faux_set)

@patch('lsh.shingles.shingles._w_shingles_generator')
def test_shingle_generator_w_shingles_yield_set_of_tuples(mock_w_shingles_gen):
     # set up
    type = ShingleType.W_SHINGLES
    size = 4

    faux_set = get_faux_set_of_w_shingles()
    faux_word_generator = generator_words()

    mock_w_shingles_gen.return_value = yield faux_set

    # execute
    actual_results = next(shgl.shingle_generator(faux_word_generator, size=size, type=type))

    # asserts
    mock_w_shingles_gen.assert_called_once_with(faux_word_generator, size)
    nt.eq_(actual_results, faux_set)

@raises(ValueError)
def test_shingle_generator_invalid_shingle_type_raise_value_error():

    # execute
    next(shgl.shingle_generator(generator_words(), type="blah"))

def test_w_shingles_generator():

    # set up
    size = 4
    faux_generator = generator_words()
    expected_results = get_faux_set_of_w_shingles()

    # execute
    actual_results = next(shgl._w_shingles_generator(faux_generator, size))

    # assert
    nt.eq_(actual_results, expected_results)


def test_w_shingles_generator_empty_doc_in_generator_yield_None():
    # set up
    size = 4
    faux_generator = generator_empty()
    expected_results = None

    # execute
    actual_results = next(shgl._w_shingles_generator(faux_generator, size))

    # assert
    nt.eq_(actual_results, expected_results)

def test_k_shingles_generator():
    # set up
    size = 4
    faux_generator = generator_string()
    expected_results = get_faux_set_of_k_shingles()

    # execute
    actual_results = next(shgl._k_shingles_generator(faux_generator, size))

    # assert
    nt.eq_(actual_results, expected_results)

def test_k_shingles_generator_empty_doc_in_generator_yield_None():
    # set up
    size = 4
    faux_generator = generator_empty()
    expected_results = None

    # execute
    actual_results = next(shgl._k_shingles_generator(faux_generator, size))

    # assert
    nt.eq_(actual_results, expected_results)

def test_get_list_of_shingles_return_non_empty_list():
    # set up
    size = 4
    faux_doc = next(generator_string())
    expected_results = get_faux_list_of_four_chars_long_strings()

    # execute
    actual_results = shgl._get_list_of_shingles(faux_doc, size)

    # asserts
    nt.eq_(actual_results, expected_results)

def test_get_list_of_shingles_none_doc_param_return_empty_list():
    # set up
    expected_results = []

    # execute
    actual_results = shgl._get_list_of_shingles(None, 4)

    # asserts
    nt.eq_(actual_results, expected_results)

#test helpers
def generator_empty():
    yield ""

def generator_string():
    yield "abcd efghij cklmn op."

def generator_words():
    yield "do or do not there is no try!"

def get_faux_list_of_four_chars_long_strings():
    return ['abcd', 'bcd ', 'cd e', 'd ef', ' efg', 'efgh', 'fghi', 'ghij', 'hij ', 'ij c', 'j ck', ' ckl', 'cklm', 'klmn', 'lmn ', 'mn o', 'n op', ' op.']

def get_faux_set_of_k_shingles():
    return set(['abcd', 'bcde', 'cdef', 'fghi', 'hijc', 'mnop', 'lmno', 'defg', 'cklm', 'efgh', 'ghij', 'jckl', 'klmn', 'ijck'])

def get_faux_set_of_w_shingles():
    return set([('do', 'or', 'do', 'not'), ('do', 'not', 'there', 'is'), ('there', 'is', 'no', 'try'), ('or', 'do', 'not', 'there'), ('not', 'there', 'is', 'no')])