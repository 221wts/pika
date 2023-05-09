# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import json
import pika
from pika.exchange_type import ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class ExamplePublisher(object):
    """
    可以处理  RabbitMQ 的 channel和connection 异常情况的生产者实例。

    如果RabbitMQ关闭连接，会进行重新开启。在输出中会显示问题，一般来说这些问题是有限的，比如权限问题或者socket连接超时。
    生产者中使用了 delivery confirm, 并且实现了如何跟踪发送的消息是否已经被RabbitMQ确认了。
    """
    EXCHANGE = 'message'
    EXCHANGE_TYPE = ExchangeType.topic
    PUBLISH_INTERVAL = 1
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self, amqp_url):
        """
        初始化 发布者对象，传入的URL会用来连接RabbitMQ。

        :param str amqp_url: The URL for connecting to RabbitMQ
        """
        self._connection = None
        self._channel = None

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False
        self._url = amqp_url

    def connect(self):
        """
        这个方法连接到RabbitQM，返回的是连接句柄。连接建立后，pika 会回调 on_connection_open 。

        :rtype: pika.SelectConnection

        """
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        """
        这个方法在与RabbitMQ建立连接后被pika回调一次。传入的是连接句柄对象，当然，在这里其实并没有使用。
        这个的作用是建立channel。
        :param pika.SelectConnection _unused_connection: The connection

        """
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """
        无法与RabbitMQ建立连接的时候pika回调当前方法。

        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error

        """
        LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        """
        当 RabbitMQ 的连接意外关闭时 pika 回调当前方法。
        如果生产者停止就停止ioloop，否则就重新开启?

        :param pika.connection.Connection connection: 关闭的connection对象
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: %s',
                           reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self):
        """
        这个方法发送 Channel.Open RPC 命令给RabbitMQ开启一个新的channel。
        RabbitMQ发送 Channel.OpenOK RPC 回复确定channel开启后，回调 on_channel_open 方法。

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        这个方法在channel开启后被pika回调。传入的是channel对象。
        一旦channel开启，我们将声明要使用的exchange。

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """
        告诉pika在RabbitMQ意外关闭channel后调用 on_channel_closed 方法.
        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """
        RabbitMQ 意外关闭channel的时候 pika 回调当前函数.
        一般来说，channel的一般关闭是由于协议错误，比如使用不同的参数二次定义exchange或者queue。
        这种情况下，会关闭连接。

        :param pika.channel.Channel channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        """
        调用 Exchange.Declare RPC 命令建立exchange。完成后，pika回调self.on_exchange_declareok。

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declaring exchange %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(self.on_exchange_declareok,
                               userdata=exchange_name)
        self._channel.exchange_declare(exchange=exchange_name,
                                       exchange_type=self.EXCHANGE_TYPE,
                                       callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        """
        RabbitMQ 完成 Exchange.Declare RPC 命令后被pika调用。

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)

        """
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """
        调用 Queue.Declare RPC 命令建立queue。完成后，pika回调 self.on_queue_declareok。

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(queue=queue_name,
                                    callback=self.on_queue_declareok)

    def on_queue_declareok(self, _unused_frame):
        """
        Queue.Declare RPC 调用完成后pika回调当前函数.
        这个函数里面通过调用Queue.Bind RPC命令，我们使用routing绑定queue和exchange。
        命令完成后，pika会回调这里设置的 self.on_bindok 方法。

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, self.QUEUE,
                    self.ROUTING_KEY)
        self._channel.queue_bind(self.QUEUE,
                                 self.EXCHANGE,
                                 routing_key=self.ROUTING_KEY,
                                 callback=self.on_bindok)

    def on_bindok(self, _unused_frame):
        """
        收到Queue.BindOk响应后pika回调当前函数。一旦我们确定我们初始化并绑定了，就可以开始publish了。
        """
        LOGGER.info('Queue bound')
        self.start_publishing()

    def start_publishing(self):
        """
        当前方法开启 delivery confirm 并将第一个消息发送到RabbitMQ。
        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        """
        发送 Confirm.Select RPC 命令给RabbitMQ在当前channel上开启delivery confirm.
        关闭的方法只能是关闭channel，创建一个新的.

        当message被RabbitMQ确认之后，无论消息是被确认还是被拒绝都会调用 self.on_delivery_confirmation

        """
        LOGGER.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """
        当RabbitMQ对Basic.Publish RPC命令响应时回调。
        传入的参数包含推送的消息的delivery tag、消息是否被ack等信息。
        目前这个函数中的功能仅仅是对消息的状态信息进行保留，记录消息的数量信息。


        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        ack_multiple = method_frame.method.multiple
        delivery_tag = method_frame.method.delivery_tag

        LOGGER.info('Received %s for delivery tag: %i (multiple: %s)',
                    confirmation_type, delivery_tag, ack_multiple)

        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1

        del self._deliveries[delivery_tag]

        if ack_multiple:
            for tmp_tag in list(self._deliveries.keys()):
                if tmp_tag <= delivery_tag:
                    self._acked += 1
                    del self._deliveries[tmp_tag]
        """
        NOTE: 
        有些时候，可能会需要检查self._deliveries中移除的条目并尝试进行再次投递。
        """

        LOGGER.info(
            'Published %i messages, %i have yet to be confirmed, '
            '%i were acked and %i were nacked', self._message_number,
            len(self._deliveries), self._acked, self._nacked)

    def schedule_next_message(self):
        """
        如果当前的连接没有关闭的话，调度下一个消息进行发送。
        """
        LOGGER.info('Scheduling next message for %0.1f seconds',
                    self.PUBLISH_INTERVAL)
        self._connection.ioloop.call_later(self.PUBLISH_INTERVAL,
                                           self.publish_message)

    def publish_message(self):
        """
        如果当前并没有停止，publish一个message到RabbitMQ。设置的列表将会用来在方法中检查发送的信息。
        一旦消息被发送，调度发送下一个消息。这里进行调度的原因在于，这样可以通过修改PUBLISH_INTERVAL常量提高发送速度。
        """
        if self._channel is None or not self._channel.is_open:
            return

        hdrs = {u'مفتاح': u' قيمة', u'键': u'值', u'キー': u'値'}
        properties = pika.BasicProperties(app_id='example-publisher',
                                          content_type='application/json',
                                          headers=hdrs)

        message = u'مفتاح قيمة 键 值 キー 値'
        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                    json.dumps(message, ensure_ascii=False),
                                    properties)
        self._message_number += 1
        self._deliveries[self._message_number] = True
        LOGGER.info('Published message # %i', self._message_number)
        self.schedule_next_message()

    def run(self):
        """
        运行实例代码，开始IOLoop
        """
        while not self._stopping:
            self._connection = None
            self._deliveries = {}
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()   # 开启连接，各种处理与配置方法都配置好
                self._connection.ioloop.start()     # 启动IO监听
            except KeyboardInterrupt:
                self.stop()     # 关闭connection和channel
                if (self._connection is not None and
                        not self._connection.is_closed):
                    # 连接没有关闭的时候，继续ioloop. 不太理解为什么这里连接是没有关闭的.
                    self._connection.ioloop.start()

        LOGGER.info('Stopped')

    def stop(self):
        """
        设置_stopping标志位。调用self.close_channel()和self.close_connection()。
        """
        LOGGER.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """
        发送Channel.Close RPC 命令关闭chanell.
        """
        if self._channel is not None:
            LOGGER.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        """关闭与RabbitMQ的连接"""
        if self._connection is not None:
            LOGGER.info('Closing connection')
            self._connection.close()


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    example = ExamplePublisher(
        'amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat=3600'
    )
    example.run()


if __name__ == '__main__':
    main()
