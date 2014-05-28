from mock import patch
from nose import tools as nt
from nose.tools import raises, with_setup
from data_queues.fifo_queue import FIFOQueue

# test helpers
def _side_effect_none_queue():
    FIFOQueue.__class__.__instance = FIFOQueue.instance()
    FIFOQueue.__class__.__queue = None
    return FIFOQueue.__class__.__instance

def setup_func():
    "set up test fixtures"
    pass

def teardown_func():

    # empty queue and "None" out instance and queue so each test has fresh instance
    # this is needed since FIFOQueue is a thread sage singleton

    while not FIFOQueue.instance().empty():
        FIFOQueue.instance().dequeue()

    FIFOQueue.instance().__class__.__queue = None
    FIFOQueue.instance().__class__.__instance = None

# tests
@with_setup(setup_func, teardown_func)
def test_single_instance_created():
    # set up
    faux_queue_1 = FIFOQueue.instance()
    faux_queue_2 = FIFOQueue.instance()

    # asserts
    assert faux_queue_1 is faux_queue_2

@raises(Exception)
@with_setup(setup_func, teardown_func)
@patch.object(FIFOQueue, 'instance')
def test_enqueue_no_instance_created_exception_raised(mock_instance):
    # set up
    mock_instance.side_effect = _side_effect_none_queue
    faux_queue = FIFOQueue.instance()

    # execute
    faux_queue.enqueue("test_item")

@with_setup(setup_func, teardown_func)
def test_enqueue_add_one_item():
    # set up
    faux_queue = FIFOQueue.instance()
    expected_results = "test_item"

    # execute
    faux_queue.enqueue("test_item")

    # asserts
    nt.eq_(faux_queue.size(), 1)

    actual_result = faux_queue.dequeue()
    nt.eq_(actual_result, expected_results)

@raises(Exception)
@with_setup(setup_func, teardown_func)
@patch.object(FIFOQueue, 'instance')
def test_dequeue_no_instance_created_exception_raised(mock_instance):
    # set up
    mock_instance.side_effect = _side_effect_none_queue
    faux_queue = FIFOQueue.instance()

    # execute
    faux_queue.dequeue("test_item")

@with_setup(setup_func, teardown_func)
def test_dequeue_add_two_items_remove_one():
    # set up
    faux_queue = FIFOQueue.instance()

    # execute
    faux_queue.enqueue("test_item_1")
    faux_queue.enqueue("test_item_2")

    # asserts
    nt.eq_(faux_queue.size(), 2)

    actual_result = faux_queue.dequeue()
    nt.eq_(actual_result, "test_item_1")

    nt.eq_(faux_queue.size(), 1)

@raises(Exception)
@with_setup(setup_func, teardown_func)
@patch.object(FIFOQueue, 'instance')
def test_size_no_instance_created_exception_raised(mock_instance):
    # set up
    mock_instance.side_effect = _side_effect_none_queue
    faux_queue = FIFOQueue.instance()

    # execute
    faux_queue.size()

@with_setup(setup_func, teardown_func)
def test_size():
    # set up
    faux_queue = FIFOQueue.instance()
    faux_queue.enqueue("test_item_1")
    faux_queue.enqueue("test_item_2")
    faux_queue.enqueue("test_item_3")

    # execute
    actual_result = faux_queue.size()

    # asserts
    nt.eq_(actual_result, 3)

@raises(Exception)
@with_setup(setup_func, teardown_func)
@patch.object(FIFOQueue, 'instance')
def test_empty_no_instance_created_exception_raised(mock_instance):
    # set up
    mock_instance.side_effect = _side_effect_none_queue
    faux_queue = FIFOQueue.instance()

    # execute
    faux_queue.empty()

@with_setup(setup_func, teardown_func)
def test_empty():
    # set up
    faux_queue = FIFOQueue.instance()

    # execute
    actual_result = faux_queue.empty()

    nt.eq_(actual_result, True)
