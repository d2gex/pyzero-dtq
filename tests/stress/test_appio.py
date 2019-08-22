import pytest

from collections import deque
from tests.stress import appio
from unittest.mock import patch, Mock, call


@pytest.fixture
def app():
    return appio.AppIO()


def test_do_something(app):

    assert not app.results
    with patch('time.sleep') as mock_time_sleep:
        wait = 1
        result = 3
        app.do_something(result, wait)

    mock_time_sleep.assert_called_with(wait)
    assert len(app.results)
    assert app.results.pop() == result


def test_accept(app):
    assert app.accept(Mock) is True


def test_run(app):

    # (1) Thread is called as expected
    num_tasks = 10
    response = 1
    wait = 2
    info = [(response, wait) for _ in range(num_tasks)]
    tasks = {'topic': 'A', 'info': info}
    with patch.object(appio, 'Thread') as mock_thread:
        app.run(tasks)

    assert mock_thread.call_count == num_tasks

    # (2) Time is called as expected
    with patch('time.sleep') as mock_time_sleep:
        app.run(tasks)

    for _call in mock_time_sleep.call_args_list:
        assert _call == call(wait)

    # (3) Results are the one expected
    app.results = deque()
    with patch('time.sleep'):
        results = app.run(tasks)
    assert all(keyword in results for keyword in ('topic', 'info'))
    assert results['info'] == sum(item[0] for item in info)
