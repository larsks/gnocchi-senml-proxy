import datetime
import gnocchiclient.auth
import gnocchiclient.v1.client
import json
import jsonschema
import logging
import paho.mqtt.client
import pkg_resources
import queue
import threading
import time
import urllib

LOG = logging.getLogger(__name__)


class MQTTClient(threading.Thread):
    def __init__(self, app):
        super().__init__(daemon=True)
        self.app = app

    def run(self):
        LOG.info('starting mqtt client')
        self.connect_mqtt()

    def connect_mqtt(self):
        parts = urllib.parse.urlparse(self.app.mqtt_endpoint)
        m_hostport = [parts.hostname]
        if parts.port:
            m_hostport.append(parts.port)

        self.mqtt = paho.mqtt.client.Client()
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message
        self.mqtt.enable_logger()
        self.mqtt.connect(*m_hostport)
        self.mqtt.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        LOG.info('connected to %s', self.app.mqtt_endpoint)
        self.subscribe()

    def subscribe(self):
        LOG.info('subscribing to %s', self.app.topics)
        self.mqtt.subscribe([(topic, 0) for topic in self.app.topics])

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
                measure['value'] = event['v']

            if not event['n'] in measures:
                measures[event['n']] = {'measures': []}

            measures[event['n']]['measures'].append(measure)

        self.app.q.put((sensor_id, measures))


class GnocchiClient(threading.Thread):
    def __init__(self, app):
        super().__init__(daemon=True)
        self.app = app

    def run(self):
        LOG.info('starting gnocchi client')

        while True:
            try:
                self.connect_gnocchi()
                break
            except gnocchiclient.exceptions.ClientException as err:
                LOG.error('failed to connect to gnocchi: %s', err)
                time.sleep(5)

        while True:
            sensor_id, measures = self.app.q.get()
            self.publish(sensor_id, measures)

    def connect_gnocchi(self):
        parts = urllib.parse.urlparse(self.app.gnocchi_endpoint)
        if parts.port:
            g_hostport = '{0.hostname}:{0.port}'.format(parts)
        else:
            g_hostport = '{0.hostname}'.format(parts)

        g_url = urllib.parse.urlunparse(
            (parts.scheme, g_hostport, parts.path,
             parts.params, parts.query, parts.fragment)
        )
        auth_plugin = gnocchiclient.auth.GnocchiBasicPlugin(
            user=parts.username,
            endpoint=g_url)
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
                LOG.warning('failed to connect to gnocchi')
                time.sleep(5)
                continue
            except gnocchiclient.exceptions.ClientException as err:
                if isinstance(err.message, dict):
                    message = err.message['cause']
                else:
                    message = err.message

                LOG.error('failed to publish measures to gnocchi: %s',
                          message)
                break


class SenmlProxy:
    topics = ['sensor/#']
    gnocchi_endpoint = 'http://sensors@localhost:8041'
    mqtt_endpoint = 'mqtt://localhost'

    def __init__(self,
                 topics=None,
                 gnocchi_user=None,
                 gnocchi_endpoint=None,
                 mqtt_endpoint=None):

        if topics is not None:
            self.topics = topics
        if gnocchi_endpoint is not None:
            self.gnocchi_endpoint = gnocchi_endpoint
        if mqtt_endpoint is not None:
            self.mqtt_endpoint = mqtt_endpoint

        self.init_schema()
        self.init_queue()

    def init_queue(self):
        self.q = queue.Queue()

    def start(self):
        self.mqtt = MQTTClient(self)
        self.mqtt.start()
        self.gnocchi = GnocchiClient(self)
        self.gnocchi.start()

        self.mqtt.join()
        self.gnocchi.join()

    def init_schema(self):
        with pkg_resources.resource_stream(__name__,
                                           'schema/senml.json') as fd:
            self.schema = json.load(fd)

