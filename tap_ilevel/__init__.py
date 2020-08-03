#!/usr/bin/env python3

import sys
import json
import argparse
from decimal import Decimal
from suds.plugin import MessagePlugin
from suds.client import Client
from suds.wsse import *
from suds.sax.attribute import Attribute

import singer
from singer import utils, metadata

from tap_ilevel.discover import discover
from tap_ilevel.sync import sync

LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    'username',
    'password',
    'start_date'
]

class SoapFixer(MessagePlugin):
    def marshalled(self, context):
        # Alter the envelope so that the xsd namespace is allowed
        context.envelope.nsprefixes['xsd'] = 'http://www.w3.org/2001/XMLSchema'
        # Go through every node in the document and apply the fix function to patch up
        # incompatible XML.
        context.envelope.walk(self.fix_any_type_string)

    def fix_any_type_string(self, element):
        """Used as a filter function with walk in order to fix errors.
        If the element has a certain name, give it a xsi:type=xsd:int. Note that the nsprefix xsd
        must also be added in to make this work."""
        # Fix elements which have these names
        fix_names = ['DataItemValue']
        if element.name in fix_names:
            type_and_value = element.text.split('_')
            element.attributes.append(Attribute('xsi:type', type_and_value[0]))
            element.setText(type_and_value[1])


def do_discover():
    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():
    LOGGER.info('Running main method....')

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    state = {}
    if parsed_args.state:
        state = parsed_args.state

    config = {}
    if parsed_args.config:
        config = parsed_args.config

    wsdl_year = config.get('wsdl_year', '2019')
    wsdl_quarter = config.get('wsdl_quarter', 'Q1')
    sandbox_flag = config.get('is_sandbox', 'false')
    is_sandbox = False
    if sandbox_flag == "true" or sandbox_flag == "True":
        is_sandbox = True

    LOGGER.info('init: is sandbox: %s', config.get('is_sandbox'))
    if is_sandbox:
        sandbox = 'sand'
    else:
        sandbox = ''

    url = 'https://{}services.ilevelsolutions.com/DataService/Service/{}/{}/DataService.svc'.format(
        sandbox, wsdl_year, wsdl_quarter)
    LOGGER.info('init: url is %s', url)
    wsdl_url = url + '?singleWsdl'
    plugin = SoapFixer()
    client = Client(wsdl_url, plugins=[plugin])

    username = config.get('username')
    password = config.get('password')

    security = Security()
    token = UsernameToken(username, password)
    security.tokens.append(token)
    timestamp = Timestamp(600)  # i.e. 10 minutes
    security.tokens.append(timestamp)

    #
    endpoint_url = url + '/Soap11NoWSA'
    client.set_options(
        port='CustomBinding_IDataService2',
        location=endpoint_url,
        wsse=security)

    if parsed_args.discover:
        do_discover()
    elif parsed_args.catalog:
        sync(client=client,
             config=parsed_args.config,
             catalog=parsed_args.catalog,
             state=state)


if __name__ == '__main__':
    main()
