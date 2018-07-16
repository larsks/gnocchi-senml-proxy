import datetime
import gnocchiclient.auth
import gnocchiclient.v1.client
import json
import jsonschema
import logging
import os
import paho.mqtt.client
import pkg_resources
import pwd
import queue
import threading
import time
import urllib

LOG = logging.getLogger(__name__)


def current_user():
    return pwd.getpwuid(os.getuid()).pw_name


class MQTTClient(threading.Thread):
    default_mqtt_endpoint = 'mqtt://localhost'
    default_topics = ['sensor/#']

    def __init__(self, app):
        super().__init__(daemon=True)
        self.app = app

        self.init_mqtt()

    def init_mqtt(self):
        self.mqtt = paho.mqtt.client.Client()
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_disconnect = self.on_disconnect
        self.mqtt.on_message = self.on_message
        self.mqtt.enable_logger()

    def connect_mqtt(self):
        mqtt_endpoint = self.app.config.get(
            'mqtt_endpoint', self.default_mqtt_endpoint)

        parts = urllib.parse.urlparse(mqtt_endpoint)
        if parts.scheme != 'mqtt':
            raise ValueError('unknown scheme in %s', mqtt_endpoint)

        LOG.info('connecting to mqtt server @ %s', mqtt_endpoint)

        m_hostport = [parts.hostname]
        if parts.port:
            m_hostport.append(parts.port)

        self.mqtt.connect(*m_hostport)
        self.mqtt.loop_forever()

    def run(self):
        LOG.info('starting mqtt client')
        self.disconnected = False
        self.connect_mqtt()

    def on_connect(self, client, userdata, flags, rc):
        if self.disconnected:
            LOG.warning('reconnected to mqtt server')
            self.disconnected = False
        else:
            LOG.info('connected to mqtt server')

        topics = self.app.config.get('topics', self.default_topics)
        LOG.debug('subscribing to %s', topics)
        self.mqtt.subscribe([(topic, 0) for topic in topics])

    def on_disconnect(self, client, userdata, rc):
        self.disconnected = True
        LOG.warning('disconnected from mqtt server')

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except json.decoder.JSONDecodeError:
            LOG.error('received invalid message: bad json payload')
            return

        try:
            jsonschema.validate(data, self.app.schema)
        except jsonschema.exceptions.ValidationError as err:
            LOG.error('received invalid message: %s', err.message)
            return

        if not data['bn'].startswith('urn:dev'):
            LOG.error('received invalid message: unknown naming scheme')
            return

        sensor_id = data['bn'][8:]
        measures = {}
        for event in data['e']:
            if 'v' not in event:
                LOG.warning('ignoring measure %s without a numeric value',
                            event['n'])
                continue

            measure = {}
            if 'bt' in data or 't' in event:
                measure['timestamp'] = data.get('bt', 0) + event.get('t', 0)
            else:
                measure['timestamp'] = datetime.datetime.utcnow().isoformat()

            if 'v' in event:
                measure['value'] = data.get('bv', 0) + event['v']
            elif 'vb' in event:
                measure['value'] = event['vb']
            elif 's' in event:
                measure['value'] = data.get('bs', 0) + event['s']

            if not event['n'] in measures:
                measures[event['n']] = {'measures': []}

            measures[event['n']]['measures'].append(measure)

        self.app.q.put((sensor_id, measures))


class GnocchiClient(threading.Thread):
    default_gnocchi_endpoint = 'http://localhost:8041'

    def __init__(self, app):
        super().__init__(daemon=True)
        self.app = app

    def run(self):
        LOG.info('starting gnocchi client')

        self.connect_gnocchi()

        while True:
            sensor_id, measures = self.app.q.get()
            self.publish(sensor_id, measures)

    def connect_gnocchi(self):
        gnocchi_username = self.app.config.get(
            'gnocchi_username', current_user())
        gnocchi_endpoint = self.app.config.get(
            'gnocchi_endpoint', self.default_gnocchi_endpoint)

        LOG.info('connecting to gnocchi server @ %s', gnocchi_endpoint)

        auth_plugin = gnocchiclient.auth.GnocchiBasicPlugin(
            user=gnocchi_username,
            endpoint=gnocchi_endpoint)
        self.gnocchi = gnocchiclient.v1.client.Client(
            session_options={'auth': auth_plugin})

    def inner_publish(self, sensor_id, measures):
        for i in range(2):
            try:
                self.gnocchi.metric.batch_resources_metrics_measures(
                    measures={sensor_id: measures},
                    create_metrics=True)
                break
            except gnocchiclient.exceptions.BadRequest:
                if i > 0:
                    raise

                LOG.info('creating new sensor resource id=%s', sensor_id)
                self.gnocchi.resource.create('sensor', dict(
                    id=sensor_id
                ))

    def publish(self, sensor_id, measures):
        while True:
            try:
                self.inner_publish(sensor_id, measures)
                break
            except gnocchiclient.exceptions.ConnectionFailure:
                LOG.warning('failed to connect to gnocchi (retrying)')
                time.sleep(5)
                continue
            except gnocchiclient.exceptions.ClientException as err:
                if isinstance(err.message, dict) and 'cause' in err.message:
                    message = err.message['cause']
                else:
                    message = err.message

                LOG.error('failed to publish measures to gnocchi: %s',
                          message)
                break


class SenmlProxy:
    def __init__(self, config):

        self.config = config
        self.init_schema()
        self.init_queue()

    def init_queue(self):
        self.q = queue.Queue()

    def start(self):
        self.tasks = [MQTTClient(self),
                      GnocchiClient(self)]

        LOG.debug('starting tasks')
        for t in self.tasks:
            t.start()

        LOG.debug('waiting for tasks to finish')
        for t in self.tasks:
            t.join()

    def init_schema(self):
        with pkg_resources.resource_stream(__name__,
                                           'schema/senml.json') as fd:
            self.schema = json.load(fd)
