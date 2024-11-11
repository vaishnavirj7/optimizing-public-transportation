import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(
            self,
            topic_name_pattern,
            message_handler,
            is_avro=True,
            offset_earliest=False,
            sleep_secs=1.0,
            consume_timeout=0.1
    )
        """Consumer object for asynchronous use."""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_Secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {

            # TODO - configure 
        }
        # TODO - Create the consumer
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            # self.consumer = AvroConsumer(...)
        else:
            # self.consumer = Consumer(...)
            pass

        # TODO Configure the AvroConsumer and subscribe to the topics. Think how 'on_assign' callback should be invoked.
        # 
        # TODO self.consumer.subscribe(...)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place."""
        # TODO - If topic is configured to use `offset_Earliest` set partition offset to the beginning or earliest
        logger.info('on_assign is complete - skipping')
        for partition in partitions:
            pass
            # TODO
        
        logger.info('partitions assigned for %s', self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from Kafka topic."""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_Secs)
    
    def _consume(self):
        """Polls for a message. Returns 1 if message received, else 0."""
        # TODO : Poll Kafka for messages. Handle errors/exceptions.
        # Return 0/1 accordingly.
        logger.info('-consume is incomplete. Skipping.')
        return 0

    def close(self):
        """Cleans up any open kafka consumers."""
        # TODO Clean up the kafka consumer.