import logging
import unittest
import mock
import json
from sumo_handler import SumoHandler, LogFormatter

# These are intentionally different than the kwarg defaults
TEST_URL = 'https://sumologic.com/receiver/v1/http/851A5E58-4EF1-7291-F947-F614A76ACB21'
TEST_VERIFY = False
TEST_TIMEOUT = 27
TEST_FLUSH_INTERVAL = 5.0
TEST_QUEUE_SIZE = 1111
TEST_DEBUG = True
TEST_RETRY_COUNT = 1
TEST_RETRY_BACKOFF = 0.1
TEST_MAX_PAYLOAD_ITEMS = 10


class TestHandler(unittest.TestCase, LogFormatter):

    def format_record(self, record: logging.LogRecord):
        return {
            'filename': record.filename,
            'name': record.name,
            'funcName': record.funcName,
            'levelName': record.levelname,
            'msg': record.getMessage(),
            'processName': record.processName,
        }

    def test_init(self):
        handler = SumoHandler(
            url=TEST_URL,
            verify_https=TEST_VERIFY,
            timeout=TEST_TIMEOUT,
            flush_interval_seconds=TEST_FLUSH_INTERVAL,
            queue_size=TEST_QUEUE_SIZE,
            debug=TEST_DEBUG,
            retry_count=TEST_RETRY_COUNT,
            retry_backoff=TEST_RETRY_BACKOFF,
            max_payload_items=TEST_MAX_PAYLOAD_ITEMS
        )
        
        self.assertIsNotNone(handler)
        self.assertIsInstance(handler, SumoHandler)
        self.assertIsInstance(handler, logging.Handler)
        self.assertEqual(handler.url, TEST_URL)
        self.assertEqual(handler.verify_https, TEST_VERIFY)
        self.assertEqual(handler.timeout, TEST_TIMEOUT)
        self.assertEqual(handler.flush_interval_seconds, TEST_FLUSH_INTERVAL)
        self.assertEqual(handler.queue.maxsize, TEST_QUEUE_SIZE)
        self.assertEqual(handler.debug, TEST_DEBUG)
        self.assertEqual(handler.retry_count, TEST_RETRY_COUNT)
        self.assertEqual(handler.retry_backoff, TEST_RETRY_BACKOFF)

        self.assertFalse(logging.getLogger('requests').propagate)
        self.assertFalse(logging.getLogger('sumo_handler').propagate)

    @mock.patch('requests.Session.post')
    def test_blocking_sumo_worker(self, mock_request: mock.Mock):
        handler = SumoHandler(
            url=TEST_URL,
            verify_https=TEST_VERIFY,
            timeout=TEST_TIMEOUT,
            flush_interval_seconds=0,
            queue_size=TEST_QUEUE_SIZE,
            debug=TEST_DEBUG,
            retry_count=TEST_RETRY_COUNT,
            retry_backoff=TEST_RETRY_BACKOFF,
            max_payload_items=TEST_MAX_PAYLOAD_ITEMS,
            formatter=self
        )

        # Silence root logger
        log = logging.getLogger('')
        for h in log.handlers:
            log.removeHandler(h)

        log = logging.getLogger('test')
        log.addHandler(handler)
        log.warning('hello!')
        log.removeHandler(handler)

        expected_output = json.dumps({
            "filename": "test_handler.py",
            "name": "test",
            "funcName": "test_blocking_sumo_worker",
            "levelName": "WARNING",
            "msg": "hello!",
            "processName": "MainProcess"
        })

        mock_request.assert_called_once_with(
            TEST_URL,
            verify=TEST_VERIFY,
            data=expected_output,
            timeout=TEST_TIMEOUT,
        )

    @mock.patch('requests.Session.post')
    def test_threaded_sumo_worker(self, mock_request):
        handler = SumoHandler(
            url=TEST_URL,
            verify_https=TEST_VERIFY,
            timeout=TEST_TIMEOUT,
            flush_interval_seconds=TEST_FLUSH_INTERVAL,
            queue_size=TEST_QUEUE_SIZE,
            debug=TEST_DEBUG,
            retry_count=TEST_RETRY_COUNT,
            retry_backoff=TEST_RETRY_BACKOFF,
            max_payload_items=TEST_MAX_PAYLOAD_ITEMS,
            formatter=self
        )

        # Silence root logger
        log = logging.getLogger('')
        for h in log.handlers:
            log.removeHandler(h)

        log = logging.getLogger('test')
        log.addHandler(handler)
        log.warning('hello!')
        log.warning('hello again!')
        log.removeHandler(handler)

        handler.force_flush()

        expected_output = "\n".join([json.dumps({
            "filename": "test_handler.py",
            "name": "test",
            "funcName": "test_threaded_sumo_worker",
            "levelName": "WARNING",
            "msg": "hello!",
            "processName": "MainProcess"
        }), json.dumps({
            "filename": "test_handler.py",
            "name": "test",
            "funcName": "test_threaded_sumo_worker",
            "levelName": "WARNING",
            "msg": "hello again!",
            "processName": "MainProcess"
        })])

        mock_request.assert_called_once_with(
            TEST_URL,
            verify=TEST_VERIFY,
            data=expected_output,
            timeout=TEST_TIMEOUT,
        )
