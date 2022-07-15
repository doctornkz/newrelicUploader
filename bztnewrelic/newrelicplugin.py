"""
Based on bza.py (Blazemeter API client) module, and adopted as-is for NewRelic API
"""

import copy
import datetime
import logging
import os
import sys
import time
import traceback
import uuid
from functools import wraps
from ssl import SSLError
from urllib.error import URLError

import bzt.engine
import requests
from requests.exceptions import ReadTimeout

from bzt import TaurusInternalException, TaurusNetworkError, TaurusConfigError
from bzt.engine import Reporter
from bzt.engine import Singletone
from bzt.modules.aggregator import DataPoint, KPISet, ResultsProvider, AggregatorListener
from bzt.utils import open_browser
from bzt.utils import dehumanize_time

from newrelic_telemetry_sdk import GaugeMetric, MetricClient
from python_graphql_client import GraphqlClient

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
        self.metric_client = None
        self.dashboard_url = 'https://onenr.io/PLACEHOLDER'

        self.timeout = 30
        self.logger_limit = 256
        self.token = None
        self.token_file = None
        self.log = logging.getLogger(self.__class__.__name__)
        self.http_session = requests.Session()
        self._retry_limit = 5
        self.uuid = None

    def client_init(self):
        try:
            self.metric_client = MetricClient(self.token)

        except Exception:
            self.log.error('Error in NR Client initialization: %s', traceback.format_exc())
            self.log.info('Exiting...')
            exit(0)

    def _request(self, data=None, headers=None, method=None, raw_result=False, retry=True):
        """
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
            except BaseException:
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
            raise Exception('HTTP status code from API is not 2xx, check access permissions or firewall settings',
                            result.status)

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
        self.sess_id = None
        self.dashboard_template_path = None
        self.account_id = None
        self._dashboard = None
        self.api_endpoint = None
        self.static_report = None
        self.time_start = None
        self.browser_open = 'none'
        self.project = 'myproject'
        self.custom_tags = {}
        self.additional_tags = {}
        self.kpi_buffer = []
        self.send_interval = 5
        self.last_dispatch = 0
        self.results_url = None
        self._session = None
        self.first_ts = sys.maxsize
        self.last_ts = 0
        self.dashboard_generator_on = False
        self.dashboard_url = 'https://one.newrelic.com/dashboards'  # default URL

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
        except BaseException:
            self.log.info("Token not found in NEW_RELIC_INSERT_KEY environment variable")
            pass
            # Read from file
            token_file = self.settings.get("token-file", "")
            if token_file:
                try:
                    with open(token_file, 'r') as handle:
                        token = handle.read().strip()
                        self.log.info("Token found in file %s:", token_file)
                        return token
                except BaseException:
                    self.log.info("Token can't be retrieved from file: %s, please check path or access", token_file)
            else:
                self.log.info("Parameter token_file is empty or doesn't exist")

        return None

    # TODO: deduplicate
    def api_token_processor(self):
        # Read from config file
        api_token = self.settings.get("api-token", "")
        if api_token:
            self.log.info("API token found in config file")
            return api_token
        self.log.info("API token not found in config file")
        # Read from environment
        try:
            api_token = os.environ['NEW_RELIC_API_KEY']
            self.log.info("Token found in NEW_RELIC_API_KEY environment variable")
            return api_token
        except BaseException:
            self.log.info("API token not found in NEW_RELIC_API_KEY environment variable")
            pass
            # Read from file
            api_token_file = self.settings.get("api-token-file", "")
            if api_token_file:
                try:
                    with open(api_token_file, 'r') as handle:
                        api_token = handle.read().strip()
                        self.log.info("Token found in file %s:", api_token_file)
                        return api_token
                except BaseException:
                    self.log.info("Token can't be retrieved from file: %s, please check path or access", api_token_file)
            else:
                self.log.info("Parameter api_token_file is empty or doesn't exist")

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

        #### Dashboard manager related settings: 
        self.static_report = self.settings.get("static-report", 'false')
        self.api_endpoint = self.settings.get("api-endpoint", 'https://api.newrelic.com/graphql')
        self.account_id = self.settings.get("account-id", '')

        api_token = self.api_token_processor()
        if not api_token:
            self.log.warning("NewRelic API token is not provided, dashboard generator is disabled.")
            self.dashboard_generator_on = False
        else:
            self.log.info("NewRelic API token is provided, dashboard generator is activated.")
            self.dashboard_generator_on = True

        # Dashboard management

        self._dashboard = DashboardManager()
        self._dashboard.api_token = api_token
        self._dashboard.account_id = self.account_id
        self._dashboard.client_init()

        if self.account_id == '':
            self.account_id = self._dashboard.get_account_id()
            if self.account_id == '':
                self.log.warning('NewRelic AccountId is not set. Please check `account-id` config setting ')
                self.log.warning('Dashboard generator is disabled.')
                self.dashboard_generator_on = False
            else:
                self._dashboard.account_id = self.account_id

        if not self._dashboard.api_check():
            self.log.warning('NewRelic API token is wrong or enpoint is not correct.')
            self.log.warning('Dashboard generator is disabled.')
            self.dashboard_generator_on = False

        # Check dashboard template 
        self.dashboard_template_path = self.settings.get("dashboard-template-path", '')
        try:
            with open(self.dashboard_template_path, 'r', encoding='utf-8') as template:
                self.log.info('Using existing template from %s' % self.dashboard_template_path)
                self._dashboard.template = template.read()
        except FileNotFoundError as fnfe:
            self.log.warning('Problem with template file, %s' % fnfe)
            self.dashboard_generator_on = False
            self.log.warning('Dashboard generator is disabled.')
        except Exception as e:
            self.log.warning('Problem with template', e)
            self.dashboard_generator_on = False
            self.log.warning('Dashboard generator is disabled.')

        if self.dashboard_generator_on:
            self.dashboard_url = self._dashboard.dashboard_link(self.project)

        # direct data feeding case

        token = self.token_processor()
        if not token:
            raise TaurusConfigError("NewRelic Ingest key is not provided")

        self.sess_id = str(uuid.uuid4())
        self.additional_tags.update({'project': self.project, 'id': self.sess_id})
        self.additional_tags.update(self.custom_tags)
        self._session = Session()

        self._session.log = self.log.getChild(self.__class__.__name__)
        self._session.token = token
        self._session.client_init()
        self._session.dashboard_url = self.dashboard_url
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

        self.time_start = time.time() * 1000

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

        # noinspection PyTypeChecker
        self.__send_data(self.kpi_buffer, False, True)

        self.kpi_buffer = []

        if self.browser_open in ('end', 'both'):
            open_browser(self.results_url)

        ### If permlink will fail on first step, we will generate at on last moment
        if self.dashboard_generator_on:
            self.dashboard_url = self._dashboard.dashboard_link(self.project)

        self.log.info("Report link: %s", self.dashboard_url)

        if self.static_report:
            self._dashboard.create_pdf(self.time_start, time.time() * 1000)

        self._session.client_close()

    def check(self):
        """
        Send data if any in buffer
        """
        self.log.debug("KPI bulk buffer len: %s", len(self.kpi_buffer))
        if self.last_dispatch < (time.time() - self.send_interval):
            self.last_dispatch = time.time()
            if len(self.kpi_buffer):
                # noinspection PyTypeChecker
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

    def aggregated_second(self, data: DataPoint):
        """
        Send online data
        :type data: DataPoint
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
                for label, kpi_set in dpoint[DataPoint.CURRENT].items():
                    nrtags = copy.deepcopy(tags)
                    nrtags.update({'label': label or 'OVERALL'})
                    nr_batch = self.__convert_current_data(kpi_set, time_stamp * self.multi, nrtags)
                    nr_metrics.extend(nr_batch)

                    self.log.debug("Current metrics in batch: %d", len(nr_batch))

                for label, kpi_set in dpoint[DataPoint.CUMULATIVE].items():
                    nrtags = copy.deepcopy(tags)
                    nrtags.update({'label': label or 'OVERALL'})
                    nr_batch_cumulative = self.__convert_cumulative_data(kpi_set, time_stamp * self.multi, nrtags)
                    nr_metrics.extend(nr_batch_cumulative)

                    self.log.debug("Cumulative metrics in batch: %d", len(nr_batch))

        return nr_metrics


    def __convert_current_data(self, item, timestamp, nrtags):

        # Overall stats : RPS, Threads, procentiles and mix/man/avg
        tmin = int(self.multi * item[KPISet.PERCENTILES]["0.0"]) if "0.0" in item[KPISet.PERCENTILES] else 0
        tmax = int(self.multi * item[KPISet.PERCENTILES]["100.0"]) if "100.0" in item[KPISet.PERCENTILES] else 0
        tavg = self.multi * item[KPISet.AVG_RESP_TIME]
        tlat = self.multi * item[KPISet.AVG_LATENCY]
        tconn = self.multi * item[KPISet.AVG_CONN_TIME]

        nrtags["timestamp"] = timestamp
        self.log.debug("Timestamp in data convertion: %d", timestamp)

        data = [
            GaugeMetric('bztRPS', item[KPISet.SAMPLE_COUNT], nrtags, end_time_ms=timestamp),
            GaugeMetric('bztThreads', item[KPISet.CONCURRENCY], nrtags, end_time_ms=timestamp),
            GaugeMetric('bztFailures', item[KPISet.FAILURES], nrtags, end_time_ms=timestamp),
            GaugeMetric('bztmin', tmin, nrtags, end_time_ms=timestamp),
            GaugeMetric('bztmax', tmax, nrtags, end_time_ms=timestamp),
            GaugeMetric('bztavg', tavg, nrtags, end_time_ms=timestamp),
            GaugeMetric('bztlat', tlat, nrtags, end_time_ms=timestamp),
            GaugeMetric('bztconn', tconn, nrtags, end_time_ms=timestamp)
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

    def __convert_cumulative_data(self, item, timestamp, nrtags):
        # Cumulative stats : Procentiles only

        nrtags["timestamp"] = timestamp
        self.log.debug("Timestamp in data convertion: %d", timestamp)

        data = []

        for p in item[KPISet.PERCENTILES]:
            tperc = int(self.multi * item[KPISet.PERCENTILES][p])
            data.append(GaugeMetric('bztpc' + p, tperc, nrtags, end_time_ms=timestamp))

        return data


class DashboardManager:

    ### Client initialization 
    def __init__(self) -> None:
        self.client = None
        self.api_token = None
        self.log = logging.getLogger(self.__class__.__name__)
        self.api_endpoint = 'https://api.newrelic.com/graphql'
        self.api_token = None
        self.static_report = 'false'
        self.template = ''
        self.dashboard_guid = ''
        self.retry_limit = 5
        self.account_id = ''

    def client_init(self):

        headers = {
            'API-Key': self.api_token,
            'Content-Type': 'application/json'
        }

        try:
            self.client = GraphqlClient(endpoint=self.api_endpoint, headers=headers)
        except Exception:
            self.log.error('Error in NR Client initialization: %s', traceback.format_exc())
            self.log.info('Exiting...')
            exit(0)

        ### Check authorization

    def api_check(self):

        query = '''
        {
        actor {
            user {
            name
            }
        }
        }
        '''

        try:
            data = self.client.execute(query=query)
            token_owner = data['data']['actor']['user']['name']
            self.log.info('Auth is successful, token from %s ', token_owner)
            return True
        except Exception as e:
            self.log.error('Something wrong with API : %s', e)
            return False

    ### Check dashboard with NAME [project]
    def dashboard_link(self, project):

        # TODO: Improve dashboard naming, remove hardcoded part
        search_query = f"name LIKE '%Load Tests [{project}]%'"

        search_dashboard = '''
        {
        actor {
            entitySearch(query: "%s") {
            count
            query
            results {
                entities {
                guid
                permalink
                }
            }
            }
        }
        }
        ''' % search_query

        data = self.client.execute(query=search_dashboard)
        try:
            if data['data']['actor']['entitySearch']['count'] > 0:
                self.log.info('Dashboard found:  %s ', f'Load Tests [{project}]')
                self.dashboard_guid = data['data']['actor']['entitySearch']['results']['entities'][1]['guid']
                return data['data']['actor']['entitySearch']['results']['entities'][1]['permalink']
            else:
                self.log.info(f'Dashboard for project "{project}" doesnt exist, creating')
                return self.dashboard_create(project)
        except Exception as e:
            self.log.warning('Problem with GraphQL dashboard response, %s' % e)

    def dashboard_create(self, project):
        dashboard_create_query = self.template.replace(
            'PROJECT_PLACE_HOLDER', project).replace(
            'ACCOUNT_PLACE_HOLDER', str(self.account_id)
        )
        try:
            data = self.client.execute(query=dashboard_create_query)
            self.log.info(f'Dashboard for project "{project}" created, sending the link')
            guid = data['data']['dashboardCreate']['entityResult']['guid']
            time.sleep(3)
            search_dashboard_query = '''
            {
            actor {
                entitySearch(query: "parentId ='%s'") {
                count
                query
                results {
                    entities {
                    guid
                    permalink
                    }
                }
                }
            }
            }
            ''' % guid

            retry = self.retry_limit

            while True:
                try:
                    data = self.client.execute(query=search_dashboard_query)
                    permalink = data['data']['actor']['entitySearch']['results']['entities'][0]['permalink']
                    self.dashboard_guid = data['data']['actor']['entitySearch']['results']['entities'][0]['guid']
                    return permalink
                except Exception:
                    if retry > 0:
                        retry -= 1
                        self.log.warning('Permalink is not ready yet, sleeping 10 sec')
                        time.sleep(10)
                        continue
                    self.log.warning('Permalink is not ready yet, failing back to default link')
                    return "https://one.newrelic.com/dashboards"

                ### If permalink is not ready, skipping whole url generation.
        except BaseException:
            self.log.warning(f'Dashboard for project {project} can not be created, possible problems are: ')
            self.log.warning('Template rendering, API access, unexpected letters in project, or account-id.')
            self.log.warning('Check the documentation. Meanwhile sending default link for NewRelic dashboards')
            return "https://one.newrelic.com/dashboards"

    def create_pdf(self, time_start, time_end):
        self.log.info('PDF report generation is coming. Waiting all data in place.')
        time.sleep(10)
        now = datetime.datetime.now()
        date_time = now.strftime("%Y-%m-%d-%H-%M-%S")

        pdf_link_query = '''
        mutation {
                dashboardCreateSnapshotUrl(
                    guid: "%s", 
                    params: {
                        timeWindow: {
                            beginTime: %d, 
                            endTime: %d
                                    }
                            }
                    )
            } 
        ''' % (self.dashboard_guid, time_start, time_end)

        try:
            retry = self.retry_limit
            data = self.client.execute(query=pdf_link_query)
            pdf_link = data['data']['dashboardCreateSnapshotUrl']
            while pdf_link is None and retry != 0:
                self.log.warning('Problem with PDF link generating, denied of service, retrying...')
                retry -= 1

            self.log.info('PDF report link %s' % pdf_link)
            r = requests.get(pdf_link, allow_redirects=True)
            r.raise_for_status()
            try:
                report_filename = f'static_report_{date_time}.pdf'
                with open(report_filename, 'wb') as f:
                    f.write(r.content)
                self.log.info('Static report saved as %s' % report_filename)
            except BaseException:
                self.log.warning('Problem with PDF retrieving, network or firewall problem.')
        except BaseException:
            self.log.warning('Problem with PDF link generating, denied of service')

    def get_account_id(self):
        accounts_query = '''
            {
            actor {
                accounts {
                id
                }
            }
            }
        '''
        try:
            data = self.client.execute(query=accounts_query)
            count = len(data['data']['actor']['accounts'])
            first_account_id = data['data']['actor']['accounts'][0]['id']
            self.log.info(f'Found {count} accounts, will use {first_account_id} as default, use account-id to redefine')
            return first_account_id
        except Exception as e:
            self.log.warning('Problem with GraphQL accounts response, %s' % e)
            return ''
