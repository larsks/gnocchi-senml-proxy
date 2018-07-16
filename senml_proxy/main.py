import argparse
import json
import logging

from senml_proxy import proxy


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument('--config', '-f')

    g = p.add_argument_group('MQTT Options')
    g.add_argument('--mqtt-endpoint', '-m')
    g.add_argument('--mqtt-username')
    g.add_argument('--mqtt-password')
    g.add_argument('--topic', '-t',
                   action='append')

    g = p.add_argument_group('Gnocchi Options')
    g.add_argument('--gnocchi-endpoint', '-g')
    g.add_argument('--gnocchi-username')
    g.add_argument('--gnocchi-password')

    g = p.add_argument_group('Logging options')
    g.add_argument('--verbose', '-v',
                   action='store_const',
                   const='INFO',
                   dest='loglevel')
    g.add_argument('--debug',
                   action='store_const',
                   const='DEBUG',
                   dest='loglevel')
    g.add_argument('--quiet',
                   action='store_const',
                   const='WARNING',
                   dest='loglevel')

    p.add_argument('--dump-config',
                   action='store_true')

    return p.parse_args()


def main():
    try:
        args = parse_args()
        config = {}

        if args.config is not None:
            with open(args.config) as fd:
                config.update(json.load(fd))

        config.update((k, v) for k, v in vars(args).items() if v is not None)

        if args.dump_config:
            print(json.dumps(config, indent=2))
            return

        logging.basicConfig(level=config.get('loglevel', 'WARNING'))
        server = proxy.SenmlProxy(config)

        server.start()
    except KeyboardInterrupt:
        pass
