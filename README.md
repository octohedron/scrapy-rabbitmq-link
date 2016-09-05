## A RabbitMQ Scheduler for Scrapy Framework.

Scrapy-rabbitmq-link is a tool that lets you feed URLs from RabbitMQ to Scrapy spiders, using the [Scrapy framework](http://doc.scrapy.org/en/latest/index.html).

This is a modified version of scrapy-rabbitmq published by Royce Haynes in [GitHub](https://github.com/roycehaynes/scrapy-rabbitmq).

## Installation

Using pip, type in your command-line prompt

```
pip install scrapy-rabbitmq-link
```
 
Or clone the repo and inside the scrapy-rabbitmq-link directory, type

```
python setup.py install
```

## Usage

### Step 1: In your scrapy settings, add the following config values:

```
# Enables scheduling storing requests queue in rabbitmq.
SCHEDULER = "scrapy_rabbitmq_link.scheduler.Scheduler"

# Don't cleanup rabbitmq queues, allows to pause/resume crawls.
SCHEDULER_PERSIST = True

# Schedule requests using a priority queue. (default)
SCHEDULER_QUEUE_CLASS = 'scrapy_rabbitmq_link.queue.SpiderQueue'

# Set expression for RabbitMQ URLs queue key
SCHEDULER_QUEUE_KEY = '%(spider)s'

# Provide host and port to RabbitMQ daemon
RABBITMQ_CONNECTION_PARAMETERS = 'amqp://user:pass@host:port/vhost'

# Response status to mark message as acknowledged and remove from queue
RABBITMQ_ACKNOWLEDGE_ON_RESPONSE_STATUS = [200, 404]

# Set middleware to status a successful remote procedure call
DOWNLOADER_MIDDLEWARES = {
    'scrapy_rabbitmq_link.middleware.RabbitMQMiddleware': 999
}

```

### Step 2: Add request building methods to Spider : _modify_request and _callback

#### Example: custom_spider.py

```
from time import time
from scrapy.spiders import Spider

class CustomSpider(Spider):
    """ Make requests using urls from RabbitMQ queue named same as spider
    """
    
    name = 'custom_spider'

    # modify a request before firing. request already contains url received from RabbitMQ
    def _modify_request(self, request):
        request.meta['time'] = time()
        return request

    # callback to the response received
    def _callback(self, response):
        return self.parse(response)

    def parse(self, response):
        # extract stuff from response
        return
```

### Step 3: Push URLs to RabbitMQ

#### Example: push_urls_to_queue.py

```
#!/usr/bin/env python
import pika
import settings

connection = pika.BlockingConnection(pika.URLParameters(settings.RABBITMQ_CONNECTION_PARAMETERS))
channel = connection.channel()

# set queue name
queue = '%(spider)s' % {'spider':'custom'}

# publish links to queue
for url in open('urls.txt'):
    url = url.strip(' \n\r')
    channel.basic_publish(exchange='',
                    routing_key=queue,
                    body=url,
                    pika.BasicProperties(
                        content_type='text/plain',
                        delivery_mode=1
                    ))

connection.close()

```


### Step 4: Run spider using [scrapy client](http://doc.scrapy.org/en/1.0/topics/shell.html)

```
scrapy crawl custom_spider
```

## Contributing and Forking

See [Contributing Guidlines](CONTRIBUTING.MD)

## Releases

See the [changelog](CHANGELOG.md) for release details.

| Version | Release Date |
| :-----: | :----------: |
|  0.1.0  | 2016-08-23 |

## Copyright & License

Copyright for portions of project "scrapy-rabbitmq" are held by Royce Haynes (c) 2014 as part of project "scrapy-rabbitmq-link". All other copyright for project "scrapy-rabbitmq-link" are held by Mantas Briliauskas (c) 2016.
