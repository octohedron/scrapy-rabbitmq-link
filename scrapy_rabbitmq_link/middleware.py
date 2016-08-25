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
	    self.server = spider.crawler.engine.slot.scheduler.queue.server
	    self.stats = spider.crawler.stats
	    self.init = False

    def inc_stat(self, stat, spider):
	    self.stats.inc_value('scheduler/acking/%(stat)s/rabbitmq' % {'stat': stat},
		    spider=spider)

    def process_response(self, request, response, spider):
        self.ensure_init(spider)

        if  response.status in self.ack_status and not is_a_picture(request):
            self.server.basic_ack(delivery_tag=request.meta.get('delivery_tag'))
            logging.info('Acked (%(status)d): %(url)s' %
		        {'url': request.url, 'status': response.status})
            self.inc_stat('acked', spider)
        elif is_a_picture(request):
            logging.info('Picture (%(status)d): %(url)s',
		        {'url': request.url, 'status': response.status})
            self.inc_stat('picture', spider)
        else:
            logging.info('Unacked (%(status)d): %(url)s',
		        {'url': request.url, 'status': response.status})
            self.inc_stat('unacked', spider)
            return response

def is_a_picture(request):
    return request.url.endswith('.jpg')
