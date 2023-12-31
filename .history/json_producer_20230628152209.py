from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from config import config, sr_config
import time
class Temperature(object):
    def __init__(self, city, reading, unit, timestamp):
        self.city = city
        self.reading = reading
        self.unit = unit
        self.timestamp = timestamp
schema_str = """{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Temperature",
    "description": "Temperature sensor reading",
    "type": "object",
    "properties": {
      "city": {
        "description": "City name",
        "type": "string"
      },
      "reading": {
        "description": "Current temperature reading",
        "type": "number"
      },
      "unit": {
        "description": "Temperature unit (C/F)",
        "type": "string"
      },
      "timestamp": {
        "description": "Time of reading in ms since epoch",
        "type": "number"
      }
    }
  }"""
def temp_to_dict(temp, ctx):
    return {"city":temp.city, 
            "reading":temp.reading,
            "unit":temp.unit, 
            "timestamp":temp.timestamp}
data = [Temperature('London', 12, 'C', round(time.time()*1000)),
        Temperature('Chicago', 63, 'F', round(time.time()*1000)),
        Temperature('Berlin', 14, 'C', round(time.time()*1000)),
        Temperature('Madrid', 18, 'C', round(time.time()*1000)),
        Temperature('Phoenix', 78, 'F', round(time.time()*1000))]