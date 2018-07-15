import datetime
import gnocchiclient.auth
import gnocchiclient.v1.client
import json
import jsonschema
import logging
import paho.mqtt.client
import pkg_resources
import urllib

LOG = logging.getLogger(__name__)


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

    def start(self):
        self.connect_mqtt()
        self.connect_gnocchi()
        self.init_gnocchi_resources()

    def init_gnocchi_resources(self):
        try:
            self.gnocchi.resource_type.create({'name': 'sensor'})
        except gnocchiclient.exceptions.ResourceTypeAlreadyExists:
            pass

    def init_schema(self):
        with pkg_resources.resource_stream(__name__,
                                           'schema/senml.json') as fd:
            self.schema = json.load(fd)

    def connect_mqtt(self):
        parts = urllib.parse.urlparse(self.mqtt_endpoint)
        m_hostport = [parts.hostname]
        if parts.port:
            m_hostport.append(parts.port)

        self.mqtt = paho.mqtt.client.Client()
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message
        self.mqtt.enable_logger()
        self.mqtt.loop_start()
        self.mqtt.connect_async(*m_hostport)

    def connect_gnocchi(self):
        parts = urllib.parse.urlparse(self.gnocchi_endpoint)
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

    def on_connect(self, client, userdata, flags, rc):
        LOG.info('connected to %s', self.mqtt_endpoint)
        self.subscribe()

    def subscribe(self):
        LOG.info('subscribing to %s', self.topics)
        self.mqtt.subscribe([(topic, 0) for topic in self.topics])

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except json.decoder.JSONDecodeError:
            LOG.error('received invalid message: bad json payload')
            return

        try:
            jsonschema.validate(data, self.schema)
        except jsonschema.exceptions.ValidationError as err:
            LOG.error('received invalid message: %s', err.message)
            return

        if not data['bn'].startswith('urn:dev'):
            LOG.error('received invalid message: unknown naming scheme')
            return

        sensor_id = data['bn'][8:]
        metrics = {}
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

            if not event['n'] in metrics:
                metrics[event['n']] = {'measures': []}

            metrics[event['n']]['measures'].append(measure)

        for i in range(2):
            try:
                self.gnocchi.metric.batch_resources_metrics_measures(
                    measures={sensor_id: metrics}, create_metrics=True)
                break
            except gnocchiclient.exceptions.ResourceNotFound:
                if i > 0:
                    LOG.error('failed to store measures for %s', sensor_id)
                    break

                LOG.info('creating new resource for sensor %s', sensor_id)
                self.gnocchi.resource.create('sensor', dict(
                    id=sensor_id
                ))
