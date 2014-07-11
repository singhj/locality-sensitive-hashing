from nose.tools.nontrivial import with_setup
from lsh.utils.random_number_generator import RNG
from lsh.utils.random_number_generator import generate_random_numbers
from nose import tools as nt

def setup_func():
    "set up test fixtures"
    pass

def teardown_func():

    # "None" out instance and random_numbers so each test has fresh instance
    # this is needed since RNG is a thread safe singleton
    setattr(RNG, "__instance", None)
    setattr(RNG, "__random_numbers", None)

@with_setup(setup_func, teardown_func)
def test_single_instance_created():
    # set up
    faux_rng_1 = RNG.instance()
    faux_rng_2 = RNG.instance()

    # asserts
    assert faux_rng_1 is faux_rng_2

def test_correct_number_of_random_numbers_generated():

    #set up
    expected_list_size = 5

    #execute
    results_random_numbers = generate_random_numbers(n=5)

    #asserts
    nt.eq_(expected_list_size, len(results_random_numbers))