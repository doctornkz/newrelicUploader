"""
Based on bza.py (Blazemeter API client) module, and adopted as-is for NewRelic API
"""

import copy
import json
import logging
import os
import sys
import time
import traceback
import uuid
from functools import wraps
from ssl import SSLError

import requests
from requests.exceptions import ReadTimeout

from bzt import TaurusInternalException, TaurusNetworkError, TaurusConfigError
from bzt.engine import Reporter
from bzt.engine import Singletone
from bzt.modules.aggregator import DataPoint, KPISet, ResultsProvider, AggregatorListener
from bzt.six import iteritems, URLError
from bzt.utils import open_browser
from bzt.utils import dehumanize_time

from newrelic_telemetry_sdk import GaugeMetric, CountMetric, SummaryMetric, MetricClient

NETWORK_PROBLEMS = (IOError, URLError, SSLError, ReadTimeout, TaurusNetworkError)

def send_with_retry(method):
    @wraps(method)
    def _impl(self, *args, **kwargs):
        if not isinstance(self, NewRelicUploader):
            raise TaurusInternalException("send_with_retry should only be applied to NewRelicUploader methods")

        try:
            method(self, *args, **kwargs)
        except (IOError, TaurusNetworkError):
            self.log.debug("Error sending data: %s", traceback.format_exc())
            self.log.warning("Failed to send data, will retry in %s sec...", self._session.timeout)
            try:
                time.sleep(self._session.timeout)
                method(self, *args, **kwargs)
                self.log.info("Succeeded with retry")
            except NETWORK_PROBLEMS:
                self.log.error("Fatal error sending data: %s", traceback.format_exc())
                self.log.warning("Will skip failed data and continue running")

    return _impl


class Session(object):
    def __init__(self):
        super(Session, self).__init__()
        self.dashboard_url = 'https://onenr.io/PLACEHOLDER'
        self.timeout = 30
        self.logger_limit = 256
        self.token = None
        self.token_file = None
        self.log = logging.getLogger(self.__class__.__name__)
        self.http_session = requests.Session()
        self._retry_limit = 5
        self.uuid = None

        try: 
            self.metric_client = MetricClient(os.environ["NEW_RELIC_INSERT_KEY"])

        except Exception:
            self.log.error("Error in NR Client initialization: %s", traceback.format_exc())
            self.log.info("Exiting...")
            exit(0)

    def _request(self, data=None, headers=None, method=None, raw_result=False, retry=True):
        """
        :param url: str
        :type data: Union[dict,str]
        :param headers: dict
        :param method: str
        :return: dict
        """
        retry_limit = self._retry_limit


        while True:
            try:
                response = self.metric_client.send_batch(data)
                response.raise_for_status()
                self.log.debug("Status code from API: %d", response.status)
            except:
                if retry and retry_limit:
                    retry_limit -= 1
                    self.log.warning("Problem with API, connectivity. Retry...")
                    continue
                raise
            break
        
        return 0

    def ping(self):
        """ Quick check if we can access the service """
        result = self.metric_client.send({}) 
        if result.status >= 300:
            raise Exception('HTTP status code from API is not 2xx, check access permissions or firewall settings', result.status) 


    def send_kpi_data(self, data, is_check_response=True, submit_target=None):
        """
        Sends online data

        """
        response = self._request(data)

        if response != 0:
            self.log.error("Response incorrect - %d, exiting... ", response)
            exit(1)

    def client_close(self):
        self.log.debug("Closing NewRelic client...")
        self.metric_client.close()


class NewRelicUploader(Reporter, AggregatorListener, Singletone):
    """
    Reporter class

    :type _session: bzt.sfa.Session or None
    """

    def __init__(self):
        super(NewRelicUploader, self).__init__()
        self.browser_open = 'start'
        self.project = 'myproject'
        self.custom_tags = {}
        self.additional_tags = {}
        self.kpi_buffer = []
        self.send_interval = 30
        self._last_status_check = time.time()
        self.last_dispatch = 0
        self.results_url = None
        self._test = None
        self._master = None
        self._session = None
        self.first_ts = sys.maxsize
        self.last_ts = 0
        self._dpoint_serializer = DatapointSerializerNF(self)
        self.log = logging.getLogger(self.__class__.__name__)

    def token_processor(self):
        # Read from config file
        token = self.settings.get("token", "")
        if token:
            self.log.info("Token found in config file")
            return token
        self.log.info("Token not found in config file")
        # Read from environment
        try:
            token = os.environ['NEW_RELIC_INSERT_KEY']
            self.log.info("Token found in NEW_RELIC_INSERT_KEY environment variable")
            return token
        except:
            self.log.info("Token not found in NEW_RELIC_INSERT_KEY environment variable")
            pass
        # Read from file
        try:
            token_file = self.settings.get("token-file","")
            if token_file:
                with open(token_file, 'r') as handle:
                        token = handle.read().strip()
                        self.log.info("Token found in file %s:", token_file)
                        return token
            else:
                self.log.info("Parameter token_file is empty or doesn't exist")
        except:
            self.log.info("Token can't be retrieved from file: %s, please check path or access", token_file)
            
        return None

    def prepare(self):
        """
        Read options for uploading, check that they're sane
        """
        super(NewRelicUploader, self).prepare()
        self.send_interval = dehumanize_time(self.settings.get("send-interval", self.send_interval))
        self.browser_open = self.settings.get("browser-open", self.browser_open)
        self.project = self.settings.get("project", self.project)
        self.custom_tags = self.settings.get("custom-tags", self.custom_tags)
        self._dpoint_serializer.multi = self.settings.get("report-times-multiplier", self._dpoint_serializer.multi)
        
        
        token = self.token_processor()
        if not token:
            raise TaurusConfigError("No NewRelic API key provided")

        # direct data feeding case

        self.sess_id = str(uuid.uuid4())
        self.additional_tags.update({'project': self.project, 'id': self.sess_id})
        self.additional_tags.update(self.custom_tags)
        self._session = Session()
        self._session.log = self.log.getChild(self.__class__.__name__)
        self._session.token = token
        self._session.dashboard_url = self.settings.get("dashboard-url", self._session.dashboard_url).rstrip("/")
        self._session.timeout = dehumanize_time(self.settings.get("timeout", self._session.timeout))

        try:
           self._session.ping()  # to check connectivity and auth
        except Exception:
           raise

        if isinstance(self.engine.aggregator, ResultsProvider):
            self.engine.aggregator.add_listener(self)

    def startup(self):
        """
        Initiate online test
        """

        super(NewRelicUploader, self).startup()

        self.results_url = self._session.dashboard_url
        self.log.info("Started data feeding: %s", self.results_url)
        if self.browser_open in ('start', 'both'):
            open_browser(self.results_url)

    def post_process(self):
        """
        Upload results if possible
        """
        self.log.debug("KPI bulk buffer len in post-proc: %s", len(self.kpi_buffer))
        self.log.info("Sending remaining KPI data to server...")

        self.__send_data(self.kpi_buffer, False, True)
        
        self.kpi_buffer = []

        if self.browser_open in ('end', 'both'):
            open_browser(self.results_url)
        self.log.info("Report link: %s", self.results_url)
        
        self._session.client_close() 
        

    def check(self):
        """
        Send data if any in buffer
        """
        self.log.debug("KPI bulk buffer len: %s", len(self.kpi_buffer))
        if self.last_dispatch < (time.time() - self.send_interval):
            self.last_dispatch = time.time()
            if len(self.kpi_buffer):
                self.__send_data(self.kpi_buffer)
                self.kpi_buffer = []
        return super(NewRelicUploader, self).check()

    @send_with_retry
    def __send_data(self, data, do_check=True, is_final=False):

        """
        :type data: list[bzt.modules.aggregator.DataPoint]
        """

        self.log.debug("Length of data to serialize: %d", len(data))
        serialized = self._dpoint_serializer.get_kpi_body(data, self.additional_tags, is_final)

        self._session.send_kpi_data(serialized, do_check)

    def aggregated_second(self, data):
        """
        Send online data
        :param data: DataPoint
        """
        self.kpi_buffer.append(data)



class DatapointSerializerNF(object):
    def __init__(self, owner):
        """
        :type owner: NewRelicUploader
        """
        super(DatapointSerializerNF, self).__init__()
        self.owner = owner
        self.multi = 1000  # multiplier factor for reporting
        self.log = logging.getLogger(self.__class__.__name__)

    def get_kpi_body(self, data_buffer, tags, is_final):
        # - reporting format:
        #   {labels: <data>,    # see below
        #    sourceID: <id of BlazeMeterClient object>,
        #    [is_final: True]}  # for last report
        #
        # - elements of 'data' are described in __get_label()
        #
        # - elements of 'intervals' are described in __get_interval()
        #   every interval contains info about response codes have gotten on it.
        nr_metrics = []

        if data_buffer:
            self.owner.first_ts = min(self.owner.first_ts, data_buffer[0][DataPoint.TIMESTAMP])
            self.owner.last_ts = max(self.owner.last_ts, data_buffer[-1][DataPoint.TIMESTAMP])

            # fill 'Timeline Report' tab with intervals data
            # intervals are received in the additive way
            for dpoint in data_buffer:
                time_stamp = dpoint[DataPoint.TIMESTAMP]
                for label, kpi_set in iteritems(dpoint[DataPoint.CURRENT]):
                    nrtags = copy.deepcopy(tags)
                    nrtags.update({'label': label or 'OVERALL'})
                    nr_batch = self.__convert_data(kpi_set, time_stamp * self.multi, nrtags)
                    nr_metrics.extend(nr_batch)

        self.log.debug("Custom metrics in batch: %d", len(nr_batch))

        return nr_metrics

    def __convert_data(self, item, timestamp, nrtags):

        # Overall stats : RPS, Threads, procentiles and mix/man/avg
        tmin = int(self.multi * item[KPISet.PERCENTILES]["0.0"]) if "0.0" in item[KPISet.PERCENTILES] else 0
        tmax = int(self.multi * item[KPISet.PERCENTILES]["100.0"]) if "100.0" in item[KPISet.PERCENTILES] else 0
        tavg = self.multi * item[KPISet.AVG_RESP_TIME]
        
        nrtags["timestamp"] = timestamp 
        self.log.debug("Timestamp in data convertion: %d", timestamp)
        
        data = [
            GaugeMetric('bztRPS', item[KPISet.SAMPLE_COUNT], nrtags, end_time_ms=timestamp),
            GaugeMetric('bztThreads', item[KPISet.CONCURRENCY], nrtags, end_time_ms=timestamp),
            GaugeMetric('bztFailures', item[KPISet.FAILURES], nrtags, end_time_ms=timestamp),
            GaugeMetric('bztmin', tmin, nrtags, end_time_ms=timestamp),
            GaugeMetric('bztmax', tmax, nrtags, end_time_ms=timestamp),
            GaugeMetric('bztavg', tavg, nrtags, end_time_ms=timestamp) 
        ]

        for p in item[KPISet.PERCENTILES]:
            tperc = int(self.multi * item[KPISet.PERCENTILES][p])
            data.append(GaugeMetric('bztp' + p, tperc, nrtags, end_time_ms=timestamp))

        # Detailed info : Error
        for rcode in item[KPISet.RESP_CODES]:
            error_tags = copy.deepcopy(nrtags)
            error_tags['rc'] = rcode
            rcnt = item[KPISet.RESP_CODES][rcode]
            data.append(GaugeMetric('bztcode', rcnt, error_tags, end_time_ms=timestamp))

        return data
