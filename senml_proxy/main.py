import argparse
import logging

from senml_proxy import proxy


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument('--mqtt-endpoint', '-m')
    p.add_argument('--gnocchi-endpoint', '-g')

    g = p.add_argument_group('Logging options')
    g.add_argument('--verbose', '-v',
                   action='store_const',
                   const='INFO',
                   dest='loglevel')
    g.add_argument('--debug',
                   action='store_const',
                   const='DEBUG',
                   dest='loglevel')

    p.set_defaults(loglevel='WARNING')

    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=args.loglevel)

    server = proxy.SenmlProxy(
        gnocchi_endpoint=args.gnocchi_endpoint,
        mqtt_endpoint=args.mqtt_endpoint,
    )

    server.start()


if __name__ == '__main__':
    main()
