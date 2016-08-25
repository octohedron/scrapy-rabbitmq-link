__author__ = 'mbriliauskas'

import logging
import connection

from scrapy.http import Request
from scrapy.utils.misc import load_object
from scrapy.dupefilters import BaseDupeFilter

# default values
SCHEDULER_PERSIST = True
SCHEDULER_QUEUE_KEY = '%(spider)s'
SCHEDULER_QUEUE_CLASS = 'scrapy_rabbitmq_link.queue.SpiderQueue'
SCHEDULER_QUEUE_PUSHING = True
SCHEDULER_IDLE_BEFORE_CLOSE = 0

class Scheduler(object):
    """ A RabbitMQ Scheduler for Scrapy.
    """

    def __init__(self, connected, persist, queue_key, queue_cls, idle_before_close, queue_pushing, *args, **kwargs):
        self.queue_key = queue_key
        self.connected = connected
        self.persist = persist
        self.queue_cls = queue_cls
        self.idle_before_close = idle_before_close
        self.stats = None
        self.queue_pushing = queue_pushing
        self.server = connection.channel(connected, queue_key % {'spider':'member_page'})

    def __len__(self):
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        persist = settings.get('SCHEDULER_PERSIST', SCHEDULER_PERSIST)
        queue_key = settings.get('SCHEDULER_QUEUE_KEY', SCHEDULER_QUEUE_KEY)
        queue_cls = load_object(settings.get('SCHEDULER_QUEUE_CLASS', SCHEDULER_QUEUE_CLASS))
        idle_before_close = settings.get('SCHEDULER_IDLE_BEFORE_CLOSE', SCHEDULER_IDLE_BEFORE_CLOSE)
        queue_pushing = settings.get('SCHEDULER_QUEUE_PUSHING', SCHEDULER_QUEUE_PUSHING)
        connected = connection.from_settings(settings)
        return cls(connected, persist, queue_key, queue_cls, idle_before_close, queue_pushing)

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls.from_settings(crawler.settings)
        instance.stats = crawler.stats
        return instance

    def open(self, spider):
        spdir = dir(spider)
        if  '_modify_request' not in spdir:
            raise NotImplementedError('Spider must contain _modify_request(self, request) method.\nNow contains : %s' % str(spdir))
        if  '_callback' not in spdir:
            raise NotImplementedError('Spider must contain _callback(self, response) method.\nNow contains : %s' % str(spdir))

        self.spider = spider
        self.queue = self.queue_cls(self.server, spider, self.queue_key)
        self.df = BaseDupeFilter()

        if self.idle_before_close < 0:
            self.idle_before_close = 0

        if len(self.queue):
            logging.info("[Scheduler] Resuming crawling (%d urls scheduled)" % len(self.queue))

    def close(self, reason):
        if  not self.persist:
            self.queue.clear()

    def enqueue_request(self, request):
        if  self.queue_pushing:
            if  self.stats:
                self.stats.inc_value('scheduler/enqueued/rabbitmq', spider=self.spider)
            self.queue.push(url)
	    return True

    def next_request(self):
        """ Creates and returns a request to fire
        """

        url, delivery_tag = self.queue.pop()
        if  url and self.stats:
            self.stats.inc_value('scheduler/dequeued/rabbitmq', spider=self.spider)
            return self.make_request(url, delivery_tag)

    def has_pending_requests(self):
        return len(self) > 0

    def make_request(self, url, delivery_tag):
        """ Creates a request to url and including a callback
        """

        spider = self.spider
        request = Request(
            url=url,
            meta=dict(delivery_tag=delivery_tag),
            callback=spider._callback)
        return spider._modify_request(request)
