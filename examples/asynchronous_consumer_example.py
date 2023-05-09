# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import logging
import time
import pika
from pika.exchange_type import ExchangeType

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class ExampleConsumer(object):
    """
    这个是一个消费者实例，可以处理预期RabbitMA意外的channel和connection情况。

    如果RabbitMQ关闭连接，这个类会停下来，并检测是否有必要重新连接。
    可以通过对output的观察,输出中出现的连接断开的原因是有限的，比如权限问题或者socket超时

    如果channel被关闭了，会在输出中指名哪个特定的命令有问题。
    """
    EXCHANGE = 'message'
    EXCHANGE_TYPE = ExchangeType.topic
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self, amqp_url):
        """
        创建一个消费者类实例，传入一个AMQP URL用来连接到RabbitMQ

        :param str amqp_url: 连接到的AMQP URL

        """
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False
        # 在生产环境下，更大的prefetch值可以提高consumer的吞吐量
        self._prefetch_count = 1

    def connect(self):
        """
        这个方法连接到RabbitMQ，返回连接句柄。一旦连接建立，pika就会调用on_connection_open方法。（连接回调）
        :rtype: pika.SelectConnection
        """
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        """
        在pika连接到RabbitMQ的时候会回调这个函数。
        如果需要的话，传递句柄给连接对象。但是在当前的实例中，仅仅是标记为未使用。

        :param pika.SelectConnection _unused_connection: The connection
        实际上会调用self.open_channel
        """
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """
        如果与RabbitMQ的连接无法建立的话，会被pika调用，同样是回调函数。

        :param pika.SelectConnection _unused_connection: 连接对象
        :param Exception err: 错误Error

        """
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """
        如果与RabbitMQ的连接意外关闭，pika会回调这个方法。因为这种关闭是意料之外的，所以会尝试重建连接。

        :param pika.connection.Connection connection: 关闭的connection对象
        :param Exception reason: 失去连接展示的原因.

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        """
        如果连接无法连接或者连接关闭的时候被调用。表明必须重新连接设置标志位，终止ioloop

        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """
        通过发出Channel.Open RPC 命令开启一个与RabbitMQ的连接。
        当RabbitMQ表示channel已经开启,pika会调用self.on_channel_open回调.
        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """
        在channel被开启的时候会被pika回调，传入的参数是channel对象，所以在这个函数中可以进行使用。
        一旦channel被打开，我们就会将exchange声明为可用(调用self.setup_exchange)。exchange的名字是类配置

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """
        这个方法告诉pika在channel意外被RabbitMQ关闭的时候调用on_channel_closed方法。
        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """
        在RabbitMQ意外关闭channel的时候被pika调用。
        如果尝试做一些违反协议的事情，比如使用不同的参数重新声明交换或者队列，通道通常会关闭。
        在这种情况下，我们会关闭连接以终止当前对象。

        :param pika.channel.Channel: 被关闭的channel
        :param Exception reason: channel为什么被关闭

        """
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def setup_exchange(self, exchange_name):
        """
        通过调用 Exchange.Declare RPC 命令在RabbitMQ上设置一个exchange。
        完成后，pika会调用`self.on_exchange_declareok`。

        :param str|unicode exchange_name: 需要声明的exhange的name

        """
        LOGGER.info('Declaring exchange: %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        """
        在Exchange.Declare RPC命名完成后被pika调用

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)

        """
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """
        通过调用 Queue.Declare RPC 命令在RabbitMQ上启动一个queue。
        完成后 pika 会调用 self.on_queue_declareok
        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declaring queue %s', queue_name)
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        """
        当在 self.setup_queue 中的 Queue.Declare RPC 调用完成后被 pika 调用。
        在这个方法里面，通过调用 Queue.Bind RPC 命令，通过routing_key 将exchange和queue绑定。
        该命令结束后 pika 会调用 self.on_bindok 方法。

        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)

        """
        queue_name = userdata
        LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, queue_name,
                    self.ROUTING_KEY)
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        """
        当 Queue.Bind 方法调用成功后被 pika 调用。在这里会调用 self.set_qos 设置 prefetch 数量。

        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
        :param str|unicode userdata: Extra user data (queue name)

        """
        LOGGER.info('Queue bound: %s', userdata)
        self.set_qos()

    def set_qos(self):
        """
        这个方法设置消费者的 prefetch 来限制每次获取的消息的数量。
        缓冲区满了之后，只有消费者进行了ack，RabbitMQ才会再次推送。可以通过改变 prefetch 适应系统的性能要求。
        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """
        在Basic.Qos 方法完成后被pika调用。
        在这里，我们通过调用start_consuming来开始消息消费。这个会调用对应的RPC命令.

        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame

        """
        LOGGER.info('QOS set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        """
        这个方法在第一次进行调用的时候通过调用add_on_cancel_callback设置消费者对象，添加的回调在RabbitMQ停止消费者的时候会被调用。
        然后发出 Basic.Consume RPC 命令，获取到 RabbitMQ 返回中的消费者唯一标识符.这个标识符在我们希望终止消费的时候会用到。
        on_message 被传入到 channel.basic_consume 中，在完整收到一个message后pika会对on_message进行回调.
        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """
        添加了一个 self.on_consumer_cancelled 回调，如果RabbitMQ因为某些原因终止了consumer的运行会触发回调。
        如果RabbitMQ取消了consumer，那么pika会调用 self.on_consumer_cancelled。
        """
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """
        当RabbitMQ发送 Basic.Cancel 给一个消费者已经接收的消息的时候回调该函数。作用是关闭channel。

        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        """
        一个消息从RabbitMQ发送过来的时候被回调。

        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method。
            携带exhange, routing key,delivery tag和消息标志。
        :param pika.Spec.BasicProperties: properties，BasicProperties实例
        :param bytes body: The message body，消息体

        """
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """
        通过发送 Basic.Ack RPC 方法来确认消息，携带delivery tag指定特定消息

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """
        发送 Basic.Cancel RPC 命令告诉 RabbitMQ将要停止消费
        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """
        RabbitMQ 确认一个消费者的取消之后 pika 调用该方法。在这里会关闭channel，进而触发 on_channel_closed 来关闭connection。
        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)

        """
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def close_channel(self):
        """
        调用 Channel.Close RPC 命令关闭 channel.
        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        """
        运行示例消费者，连接到RabbitMQ并开始 IOLoop 进入阻塞并允许 Selection 进行操作.
        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """
        停止消费者与 RabbitMQ 的连接（确保停止消费）。
        当RabbitMQ同意了，之后，pika会调用 on_cancelok 关闭channel和connection。
        这个回调是在channel.basic_cancel中设置的。
        这里要重启ioloop是因为当键盘输入CTRL-C的时候会触发异常。这个异常会终止IOLoop，但是pika需要通过这个与RabbitMQ进行通信。
        启动IOLoop之前发出的所有命令都将被缓冲，但是不会被处理。
        """
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()   # 停止消费者,内部会先关闭channel,在关闭connection
                self._connection.ioloop.start()     # 连接关闭的时候，ioloop会自动停止.
            else:
                self._connection.ioloop.stop()  # 消费者已经停止了.直接终止ioloop
            LOGGER.info('Stopped')


class ReconnectingExampleConsumer(object):
    """
    消费者实例, 在内嵌的ExampleConsumer表明需要重新连接的时候会进行重新连接。
    """

    def __init__(self, amqp_url):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._consumer = ExampleConsumer(self._amqp_url)

    def run(self):
        """
            实际上的业务上的consumer会调用这个。
        """
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = ExampleConsumer(self._amqp_url)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    amqp_url = 'amqp://guest:guest@localhost:5672/%2F'
    consumer = ReconnectingExampleConsumer(amqp_url)
    consumer.run()


if __name__ == '__main__':
    main()
