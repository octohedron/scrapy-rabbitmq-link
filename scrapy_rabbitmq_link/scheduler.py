__author__ = 'mbriliauskas'

import sys
import time
import signal
import logging
import connection

from scrapy.http import Request
from scrapy.dupefilters import BaseDupeFilter
from scrapy_rabbitmq_link.queue import RabbitMQQueue

# default values
SCHEDULER_PERSIST = True
SCHEDULER_IDLE_BEFORE_CLOSE = 0

logger = logging.getLogger(__name__)


class IScheduler(object):
    """ Base Scrapy scheduler class. """

    def __init__(self):
        raise NotImplementedError

    def open(self, spider):
        """Start scheduling"""
        raise NotImplementedError

    def close(self, reason):
        """Stop scheduling"""
        raise NotImplementedError

    def enqueue_request(self, request):
        """Add request to queue"""
        raise NotImplementedError

    def next_request(self):
        """Pop a request"""
        raise NotImplementedError

    def has_pending_requests(self):
        """Check if queue is not empty"""
        raise NotImplementedError


class Scheduler(IScheduler):
    #TODO: to be extended in future
    @staticmethod
    def _ensure_settings(settings, key):
        if not settings.get(key):
            msg = 'Please set "key" at settings.' % key
            raise NotImplementedError(msg)


class RabbitMQScheduler(Scheduler):
    """ A RabbitMQ Scheduler for Scrapy. """

    def __init__(self, connection_url, persist, idle_before_close,
                 *args, **kwargs):
        self.connection_url = connection_url
        self.persist = persist
        self.idle_before_close = idle_before_close
        self.stats = None
        self.waiting = False
        self.closing = False

    @classmethod
    def from_settings(cls, settings):
        cls._ensure_settings(settings, 'RABBITMQ_CONNECTION_PARAMETERS')
        connection_url = settings.get('RABBITMQ_CONNECTION_PARAMETERS')
        persist = settings.get('SCHEDULER_PERSIST', SCHEDULER_PERSIST)
        idle_before_close = settings.get('SCHEDULER_IDLE_BEFORE_CLOSE',
            SCHEDULER_IDLE_BEFORE_CLOSE)
        return cls(connection_url, persist, idle_before_close)

    @classmethod
    def from_crawler(cls, crawler):
        scheduler = cls.from_settings(crawler.settings)
        scheduler.stats = crawler.stats
        signal.signal(signal.SIGINT, scheduler.on_sigint)
        return scheduler

    def __len__(self):
        return len(self.queue)

    def open(self, spider):
        if not hasattr(spider, '_make_request'):

            """* Backwards compatible *"""
            if hasattr(spider, '_callback') and\
                hasattr(spider, '_modify_request'):
                msg  = 'Spider methods _callback and _modify_request were '
                msg += 'pushed to _make_request and soon will no longer '
                msg += 'be supported. Please provide method _make_request '
                msg += 'to turn RabbitMQ message into Scrapy request object.'
                logger.warning(msg)
            else:
                msg += 'Method _make_request not found in spider. '
                msg += 'Please provide it to turn RabbitMQ message '
                msg += 'into Scrapy request object.'
                raise NotImplementedError(msg)

        self.spider = spider
        self.queue = self._make_queue(spider, spider.queue_key)
        if hasattr(spider, 'fwd_queue_key'):
            self.fwd_queue = self._make_queue(spider, spider.fwd_queue_key)

        self.df = BaseDupeFilter()

        if self.idle_before_close < 0:
            self.idle_before_close = 0

        if len(self.queue):
            logger.info("[Scheduler] Resuming crawling (%d urls scheduled)" %\
                    len(self.queue))
        else:
            logger.info("[Scheduler] No items to crawl in %s" % spider.queue_key)

    def _make_queue(self, spider, key):
        return RabbitMQQueue(self.connection_url, spider, key)

    def on_sigint(self, signal, frame):
        self.closing = True

    def close(self, reason):
        if  not self.persist:
            self.queue.clear()

    def enqueue_request(self, request):
        """ Enqueues request to main queues back
        """
        if  self.queue:
            if  self.stats:
                self.stats.inc_value('scheduler/enqueued/rabbitmq',
                        spider=self.spider)
            self.queue.push(request.url)
        return True

    def next_request(self):
        """ Creates and returns a request to fire
        """
        if self.closing:
            return

        if len(self):
            self.waiting = False
            method_frame, headers, body = self.queue.pop()
            if  body:
                if self.stats:
                    self.stats.inc_value('scheduler/dequeued/rabbitmq',
                            spider=self.spider)

                if hasattr(self.spider, '_make_request'):
                    return self.spider._make_request(method_frame, headers,
                            body)
                else:
                    """ * Clause to be deleted in upcoming versions * """
                    if hasattr(self.spider, '_callback') and\
                        hasattr(self.spider, '_modify_request'):
                        delivery_tag = method_frame.delivery_tag \
                            if hasattr(method_frame, 'delivery_tag') \
                            else None
                        request = Request(
                            url=url,
                            meta=dict(delivery_tag=delivery_tag),
                            callback=self.spider._callback)
                        return self.spider._modify_request(request)
                    else:
                        msg = 'Please provide _make_request method in spider class.'
                        raise NotImplementedError(msg)
        else:
            if not self.waiting:
                msg = 'Queue (%s) is empty. Waiting for messages...'
                self.waiting = True
                logger.info(msg % self.queue.key)
            time.sleep(10)
            return None

    def has_pending_requests(self):
        return not self.closing

    def make_request(self, url, delivery_tag):
        """ Creates a request to url and including a callback.
            Will no longer be supported in upcoming versions.
        """
        request = Request(
            url=url,
            meta=dict(delivery_tag=delivery_tag),
            callback=spider._callback)
        return spider._modify_request(request)


class SaaS(RabbitMQScheduler):
    """ Scheduler as a RabbitMQ service.
    """
    def __init__(self, connection_url, persist, idle_before_close, *args,
                 **kwargs):
        super(SaaS, self).__init__(connection_url, persist, idle_before_close,
                                   *args, **kwargs)

    def ack_message(self, delivery_tag):
        self.queue.ack(delivery_tag)

    def requeue_message(self, body, headers=None):
        self.queue.push(body, headers)

    def forward_message(self, body, headers=None):
        self.fwd_queue.push(body, headers)
