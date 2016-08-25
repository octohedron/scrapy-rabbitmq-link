import connection

import logging

from scrapy.http import Request

class Base(object):
    """Per-spider queue/stack base class"""

    def __init__(self, server, spider, key, exchange=None):
        """Initialize per-spider RabbitMQ queue.

        Parameters:
            server -- rabbitmq connection
            spider -- spider instance
            key -- key for this queue (e.g. "%(spider)s:queue")
        """
        self.spider = spider
        self.key = key % {'spider': spider.name}
        self.server = server

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
        self.server.queue_purge(self.key)


class SpiderQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        response = self.server.queue_declare(self.key, passive=True)
        rlen = response.method.message_count
        return rlen

    def push(self, url):
        """Push an url"""
        self.server.basic_publish(
            exchange='',
            routing_key=self.key,
            body=url
        )

    def pop(self):
        """Pop an url"""
        method_frame, header, url = self.server.basic_get(queue=self.key)
        delivery_tag = method_frame.delivery_tag \
                        if hasattr(method_frame, 'delivery_tag') \
                        else None
        return url, delivery_tag

__all__ = ['SpiderQueue']
