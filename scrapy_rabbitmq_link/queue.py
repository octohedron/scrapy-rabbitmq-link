# system packages
import sys
import time
import logging
from scrapy.http import Request

# module packages
import connection

logger = logging.getLogger(__name__)


class Base(object):
    """Per-spider queue/stack base class"""

    def __init__(self, connection_url, spider, key, exchange=None):
        """Initialize per-spider RabbitMQ queue.

        Parameters:
            connection_url -- rabbitmq connection url
            spider -- scrapy spider instance
            key -- key for this queue (e.g. "%(spider)s")
        """
        self.spider = spider
        self.key = key % {'spider': spider.name}
        self.connection_url = connection_url
        self.server = None
        self.connect()

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, url):
        """Push an url"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop an url"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        self.channel.queue_purge(self.key)

    def connect(self):
        """Make a connection"""
        if self.server:
            try:
                self.server.close()
            except:
                pass
        self.server = connection.connect(self.connection_url)
        self.channel = connection.get_channel(self.server, self.key)

class SpiderQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        declared = self.channel.queue_declare(self.key, passive=True)
        return declared.method.message_count

    def _try_operation(function):
        """Wrap unary method by reconnect procedure"""
        def wrapper(self, *args, **kwargs):
            retries = 0
            while retries < 10:
                try:
                    return function(self, *args, **kwargs)
                except Exception as e:
                    retries += 1
                    msg = 'Function %s failed. Reconnecting... (%d times)' %\
                            (str(function), retries)
                    logger.info(msg)
                    self.connect()
                    time.sleep((retries-1)*5)
            return None
        return wrapper

    @_try_operation
    def basic_get(self, queue):
        return self.channel.basic_get(queue=queue)

    @_try_operation
    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    @_try_operation
    def push(self, url):
        """Push an url"""
        self.channel.basic_publish(
            exchange='',
            routing_key=self.key,
            body=url
        )

    def pop(self):
        """Pop an url"""
        method_frame, header, url = self.basic_get(queue=self.key)
        delivery_tag = method_frame.delivery_tag\
                if hasattr(method_frame, 'delivery_tag')\
                else None
        return url, delivery_tag

__all__ = ['SpiderQueue']
