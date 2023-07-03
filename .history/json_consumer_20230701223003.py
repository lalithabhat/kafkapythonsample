from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from config import config

def set_consumer_configs():
    config['group.id'] = 'temp_group'
    config['auto.offset.reset'] = 'earliest'

class Order(object):
    def __init__(self, Description, Product, Price, OrderNumber):
        self.Description = Description
        self.Product = Product
        self.Price = Price
        self.OrderNumber = OrderNumber

schema_str = """{
      "properties": {
      "CreatedById": {
      "connect.index": 2,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "CreatedDate": {
      "connect.index": 1,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "connect.type": "int64",
          "connect.version": 1,
          "title": "org.apache.kafka.connect.data.Timestamp",
          "type": "integer"
        }
      ]
    },
    "Description__c": {
      "connect.index": 6,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "EventUuid": {
      "connect.index": 3,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "Order_Number__c": {
      "connect.index": 4,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "connect.type": "float64",
          "type": "number"
        }
      ]
    },
    "Price__c": {
      "connect.index": 7,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "connect.type": "float64",
          "type": "number"
        }
      ]
    },
    "Product_Name__c": {
      "connect.index": 5,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "ReplayId": {
      "connect.index": 0,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "_EventType": {
      "connect.index": 9,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "_ObjectType": {
      "connect.index": 8,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    }
  },
  "title": "io.confluent.salesforce.Orders__e",
  "type": "object"
}"""

def dict_to_order(dict, ctx):
    return Order(dict['city'], dict['reading'], dict['unit'], dict['timestamp'])

if __name__ == '__main__':
    topic = 'temp_readings'

    json_deserializer = JSONDeserializer(schema_str, from_dict=dict_to_order)
    set_consumer_configs()
    consumer = Consumer(config)
    consumer.subscribe([topic])
    while True:
        try:
            event = consumer.poll(1.0)
            if event is None:
                continue
            temp = json_deserializer(event.value(), 
                SerializationContext(topic, MessageField.VALUE))
            if temp is not None:
                print(f'Latest order is {temp.OrderNumber} {temp.Price} {temp.Product} {temp.Description}.')

        except KeyboardInterrupt:
            break

    consumer.close()