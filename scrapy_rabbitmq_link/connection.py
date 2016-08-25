# -*- coding: utf-8 -*-

import pika

def channel(connected, queue_name):
    """ Init method returning prepared channel for consuming
    """

    channel = connected.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    channel.confirm_delivery()

    return channel

def from_settings(settings):
    """ Factory method that returns an instance of connection
    """

    connection_parameters = settings.get('RABBITMQ_CONNECTION_PARAMETERS')
    connected = pika.BlockingConnection(pika.URLParameters(connection_parameters))
    
    return connected
