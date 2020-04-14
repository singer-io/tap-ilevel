#!/usr/bin/env python3

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_ilevel.client import ZeepClient
from tap_ilevel.discover import discover
from tap_ilevel.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'username',
    'password',
    'is_sandbox',
    'wsdl_year',
    'wsdl_quarder',
    'start_date'
]


def do_discover():

    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with ZeepClient(username=parsed_args.config['username'],
                    password=parsed_args.config['password'],
                    is_sandbox=parsed_args.config['is_sandbox'],
                    wsdl_year=parsed_args.config['wsdl_year'],
                    wsdl_quarter=parsed_args.config['wsdl_quarter']) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(client=client,
                 config=parsed_args.config,
                 catalog=parsed_args.catalog,
                 state=state)

if __name__ == '__main__':
    main()
