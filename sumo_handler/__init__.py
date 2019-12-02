import atexit
import json
import logging
import traceback
from threading import Timer

import requests
from urllib3 import Retry, disable_warnings
from requests.adapters import HTTPAdapter
from queue import Queue, Full, Empty


instances = []  # For keeping track of running class instances


class Metrics:
    metrics: str
    content_type: str
    meta_data: dict

    def __str__(self):
        return self.metrics

    def __init__(self, payload, content_type='application/vnd.sumologic.prometheus', meta_data=dict):
        self.metrics = payload
        self.content_type = content_type
        self.meta_data = meta_data


# Called when application exit imminent (main thread ended / got kill signal)
@atexit.register
def perform_exit():
    for instance in instances:
        try:
            instance.shutdown()
        except:
            pass


def force_flush():
    for instance in instances:
        try:
            instance.force_flush()
        except:
            pass


class LogFormatter:
    def format_record(self, record: logging.LogRecord):
        if isinstance(record.msg, Metrics) or isinstance(record.msg, dict):
            return record.msg
        return {
            'filename': record.filename,
            'created': record.created,
            'name': record.name,
            'funcName': record.funcName,
            'levelName': record.levelname,
            'msg': record.getMessage(),
            'processName': record.processName,
        }


class SumoHandler(logging.Handler):
    """
    A logging handler to send events to Sumo Logic
    """
    def __init__(self, url,
                 verify_https=True, timeout=60, flush_interval_seconds=15.0,
                 queue_size=5000, debug=False, retry_count=5,
                 retry_backoff=2.0, proxies=None, max_payload_items=100,
                 formatter=None):

        global instances
        instances.append(self)
        logging.Handler.__init__(self)

        self.enabled = str(url).__contains__('sumologic.com/receiver/v1/http')
        if not self.enabled:
            print('Sumo Logic log handler disabled, no valid collector URL provided')
            return

        self.url = url
        self.verify_https = verify_https
        self.timeout = timeout
        self.flush_interval_seconds = flush_interval_seconds
        self.SIGTERM = False  # 'True' if application requested exit
        self.timer = None
        # It is possible to get 'behind' and never catch up, so we limit the queue size
        self.queue = Queue(maxsize=queue_size)
        self.debug = debug
        self.session = requests.Session()
        self.retry_count = retry_count
        self.retry_backoff = retry_backoff
        self.proxies = proxies
        self.max_payload_items = max_payload_items
        self.formatter = formatter or LogFormatter()

        self.write_debug_log("Starting debug mode")

        self.write_debug_log("Preparing to override loggers")
        # prevent infinite recursion by silencing requests and urllib3 loggers
        logging.getLogger('requests').propagate = False
        logging.getLogger('urllib3').propagate = False

        # and do the same for ourselves
        logging.getLogger(__name__).propagate = False

        # disable all warnings from urllib3 package
        if not self.verify_https:
            disable_warnings()
       
        if self.proxies is not None:
            self.session.proxies = self.proxies
           
        # Set up automatic retry with back-off
        self.write_debug_log("Preparing to create a Requests session")
        retry = Retry(total=self.retry_count,
                      backoff_factor=self.retry_backoff,
                      method_whitelist=False,  # Retry for any HTTP verb
                      status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retry))

        self.start_worker_thread()

        self.write_debug_log("Class initialize complete")

    def emit(self, log_record: logging.LogRecord):
        if not self.enabled:
            return

        self.write_debug_log("emit() called")
        try:
            payload = self.format_record(log_record)
        except Exception as e:
            self.write_log("Exception in Sumo logging handler: %s" % str(e))
            self.write_log(traceback.format_exc())
            return

        if self.flush_interval_seconds > 0:
            try:
                self.write_debug_log("Writing record to log queue")
                # Put log message into queue; worker thread will pick up
                self.queue.put_nowait(payload)
            except Full:
                self.write_log("Log queue full; log data will be dropped.")
        else:
            # Flush log immediately; is blocking call
            self.queue.put(payload)
            self._sumo_worker()

    def close(self):
        self.shutdown()
        logging.Handler.close(self)

    #
    # helper methods
    #

    def start_worker_thread(self):
        # Start a worker thread responsible for sending logs
        if self.enabled and self.flush_interval_seconds > 0:
            self.write_debug_log("Preparing to spin off first worker thread Timer")
            self.timer = Timer(self.flush_interval_seconds, self._sumo_worker)
            self.timer.daemon = True  # Auto-kill thread if main process exits
            self.timer.start()

    def write_log(self, log_message):
        print("[SumoHandler] " + log_message)

    def write_debug_log(self, log_message):
        if self.debug:
            print("[SumoHandler DEBUG] " + log_message)

    def format_record(self, record: logging.LogRecord):
        self.write_debug_log("format_record() called")
        return self.formatter.format_record(record)

    def _post_payload(self, str_payload, json_payload, headers):
        r = self.session.post(
            self.url,
            data=str_payload,
            json=json_payload,
            headers=headers,
            verify=self.verify_https,
            timeout=self.timeout
        )
        r.raise_for_status()  # Throws exception for 4xx/5xx status

    def _send_payloads(self, payloads: list):
        if len(payloads) == 1:
            return self._send_payload(payloads[0])

        logs = []
        for item in payloads:
            if isinstance(item, Metrics):
                self._send_payload(item)
            else:
                logs.append(item)
        if len(logs) > 0:
            self._send_payload(
                "\n".join(json.dumps(item) for item in logs)
            )

    def _send_payload(self, payload):
        if not payload:
            return

        if isinstance(payload, str):
            return self._post_payload(payload, None, None)

        if isinstance(payload, Metrics):
            self._post_payload(str(payload), None, {'Content-Type': payload.content_type})
            if payload.meta_data:
                # send the metrics also as json log line
                new_payload = payload.meta_data
                new_payload['metrics'] = payload.metrics
                self._send_payload(new_payload)
            return

        self._post_payload(None, payload, {'Content-Type': 'application/json'})

    def _sumo_worker(self):
        if not self.enabled:
            return

        self.write_debug_log("_sumo_worker() called")

        if self.flush_interval_seconds > 0:
            # Stop the timer. Happens automatically if this is called
            # via the timer, does not if invoked by force_flush()
            self.timer.cancel()

        payloads = self.fetch_payloads()

        if payloads:
            self.write_debug_log("Payload available for sending")
            self.write_debug_log("Destination URL is " + self.url)

            try:
                self.write_debug_log("Sending payloads")
                self._send_payloads(payloads)
                self.write_debug_log("Payloads sent successfully")

            except Exception as e:
                try:
                    self.write_log("Exception in Sumo logging handler: %s" % str(e))
                    self.write_log(traceback.format_exc())
                except:
                    self.write_debug_log("Exception encountered," +
                                         "but traceback could not be formatted")

        else:
            self.write_debug_log("No payload was available to send")

        # Restart the timer
        if self.flush_interval_seconds > 0:
            timer_interval = self.flush_interval_seconds
            if self.SIGTERM:
                self.write_debug_log("Timer reset aborted due to SIGTERM received")
            else:
                if not self.queue.empty():
                    self.write_debug_log("Queue not empty, scheduling timer to run immediately")
                    timer_interval = 1.0  # Start up again right away if queue was not cleared

                self.write_debug_log("Resetting timer thread")
                self.timer = Timer(timer_interval, self._sumo_worker)
                self.timer.daemon = True  # Auto-kill thread if main process exits
                self.timer.start()
                self.write_debug_log("Timer thread scheduled")

    def fetch_payloads(self):
        result = []
        while not self.queue.empty():
            self.write_debug_log("Recursing through queue")
            try:
                item = self.queue.get(block=False)
                result.append(item)
                self.queue.task_done()
                self.write_debug_log("Queue task completed")
            except Empty:
                self.write_debug_log("Queue was empty")

            # If the payload is getting very long, stop reading and send immediately.
            if not self.SIGTERM and len(result) >= self.max_payload_items:
                self.write_debug_log("Payload maximum size exceeded, sending immediately")
                break

        return result

    def force_flush(self):
        if not self.enabled:
            return

        if self.timer:
            self.timer.cancel()

        self.write_debug_log("Force flush requested")
        self._sumo_worker()

    def shutdown(self):
        if not self.enabled:
            return

        self.write_debug_log("Immediate shutdown requested")

        # Only initiate shutdown once
        if self.SIGTERM:
            return

        self.write_debug_log("Setting instance SIGTERM=True")
        self.SIGTERM = True

        if self.flush_interval_seconds > 0:
            self.timer.cancel()  # Cancels the scheduled Timer, allows exit immediately

        self.write_debug_log("Starting up the final run of the worker thread before shutdown")
        # Send the remaining items that might be sitting in queue.
        self._sumo_worker()
