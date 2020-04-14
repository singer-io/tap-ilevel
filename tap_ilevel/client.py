import backoff
from zeep import Client
from zeep.exceptions import Fault
from zeep.wsse.username import UsernameToken

from singer import metrics
import singer

LOGGER = singer.get_logger()


class Server5xxError(Exception):
    pass

class Server429Error(Exception):
    pass

class Server408Error(Exception):
    pass

class ZeepClient(Client):

    def __init__(self,
                 username,
                 password,
                 is_sandbox,
                 wsdl_year,
                 wsdl_quarter):
        self.__username = username
        self.__password = password
        self.__is_sandbox = is_sandbox
        self.__wsdl_year = wsdl_year
        self.__wsdl_quarter = wsdl_quarter

        if self.__is_sandbox == 'true':
            self.__subdomain = 'sandservices'
        else:
            self.__subdomain = 'services'

        self.wsdl_url = 'https://{}.ilevelsolutions.com/DataService/Service/{}/{}/DataService.svc?singleWsdl'.format(
            self.__subdomain, self.__wsdl_year, self.__wsdl_quarter)

    def __enter__(self):
        self.client = Client(
            wsdl=self.wsdl_url,
            wsse=UsernameToken(
                username=self.__username,
                password=self.__password))
        return self


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, Server408Error, Server429Error),
                          max_tries=7,
                          factor=3)
    def request(self, stream_name, service_name, data_key, **kwargs):
        service = self.client._get_service(service_name)
        with metrics.http_request_timer(stream_name) as timer:
            try:
                response = service(**kwargs)
            except Fault as err:
                error_details = '{}'.format(err.detail)
                error_list_5xx = ['500', '501', '502', '503', '504', '505']
                error_5xx = any(ele in error_details for ele in error_list_5xx) 

                if '408' in error_details:
                    raise Server408Error(error_details)
                elif '429' in error_details:
                    raise Server429Error(error_details)
                elif error_5xx:
                    raise Server5xxError(error_details)
                else:
                    LOGGER.error(error_details)
                    raise RuntimeError(error_details)

        return response[data_key]
