import pika

RABBITMQ_ACKNOWLEDGE_ON_RESPONSE_STATUS = [200, 404]

class RabbitMQMiddleware(object):
    """ Middleware used to acknowledge successful messages
    """

    def __init__(self, settings): 
        self.ack_status = settings.get('RABBITMQ_ACKNOWLEDGE_ON_RESPONSE_STATUS', RABBITMQ_ACKNOWLEDGE_ON_RESPONSE_STATUS)
    
    @classmethod
    def from_settings(self, settings):
        return RabbitMQMiddleware(settings)

    def process_response(self, request, response, spider):
        if  response.status in self.ack_status:
            server = spider.crawler.engine.slot.scheduler.queue.server
            server.basic_ack(delivery_tag=request.meta.get('delivery_tag'))
        return response
