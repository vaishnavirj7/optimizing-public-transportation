import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

"""Producer base class providing common utilities and functionality"""

class Producer:
    """Defines and provides common functionality amongst Producers."""
    # Tracks existing topics across all Producer instances

    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema, 
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object"""
        self.topic_name = topic_name
        self.key_schema =key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            # TODO - Configure broker properties
        }

        # If topic is not created, create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        
        # TODO : Configure the AvroProducer 
        # self.producer = AvroProducer()

    def create_topic(self):
        """Creates the producer topic """
        # TODO: Create topic for this producer.

    def time_millis(self):
        return int(round(time.time() * 1000))
    
    def close(self):
        """Prepares the producer for exit"""
        # TODO - Cleanup code



