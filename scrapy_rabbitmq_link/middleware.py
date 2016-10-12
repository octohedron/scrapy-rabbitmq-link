import pika
import logging

RABBITMQ_ACKNOWLEDGE_ON_RESPONSE_STATUS = [200, 404]

class RabbitMQMiddleware(object):
    """ Middleware used to acknowledge successful messages
    """

    def __init__(self, settings):
        self.ack_status = settings.get('RABBITMQ_ACKNOWLEDGE_ON_RESPONSE_STATUS', RABBITMQ_ACKNOWLEDGE_ON_RESPONSE_STATUS)
        self.init = True

    @classmethod
    def from_settings(self, settings):
        return RabbitMQMiddleware(settings)

    def ensure_init(self, spider):
        if  self.init:
            self.spider = spider
            self.queue = spider.crawler.engine.slot.scheduler.queue
            self.stats = spider.crawler.stats
            self.init = False

    def inc_stat(self, stat):
            self.stats.inc_value('scheduler/acking/%(stat)s/rabbitmq' % {'stat': stat},
            spider=self.spider)

    def process_response(self, request, response, spider):
        self.ensure_init(spider)
        ack = response.status in self.ack_status
        if  ack and not is_a_picture(response):
            self.ack_response(request, response)
        elif is_a_picture(response):
            self.process_picture(response)
        else:
            self.requeue_message(request, response)
        return response

    def ack_response(self, request, response):
        if  self.check_delivery_tag(request):
            delivery_tag = request.meta.get('delivery_tag')
            self.queue.ack(delivery_tag)
            logging.info('Acked (%(status)d): %(url)s' %
                {'url': response.url, 'status': response.status})
            self.inc_stat('acked')

    def check_delivery_tag(self, request):
        if  'delivery_tag' not in request.meta:
            logging.error('Request %(request)s does not have a deliver tag.' %
                {'request': request})
        return 'delivery_tag' in request.meta

    def process_picture(self, response):
        logging.info('Picture (%(status)d): %(url)s',
            {'url': response.url, 'status': response.status})
        self.inc_stat('picture')

    def requeue_message(self, request, response):
        if  self.check_delivery_tag(request):
            delivery_tag = request.meta.get('delivery_tag')
            self.queue.push(response.url)
            self.queue.basic_ack(delivery_tag=delivery_tag)
            logging.info('Requeued (%(status)d): %(url)s',
                {'url': response.url, 'status': response.status})
            self.inc_stat('requeued')

def is_a_picture(response):
    picture_exts = ['.png', '.jpg']
    return any([response.url.endswith(ext) for ext in picture_exts])
