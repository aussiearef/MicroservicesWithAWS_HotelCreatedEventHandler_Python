import os
import json
from datetime import datetime
from typing import List

from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.event_processor.handler import CoroutineHandler
from aws_lambda_powertools.event_processor.models import SNSEvent
from elasticsearch import Elasticsearch, helpers
from boto3 import client
from boto3.dynamodb.conditions import Key

logger = Logger(service="HotelCreatedEventHandler")
db_client = client("dynamodb")
table = db_client.Table("hotel-created-event-ids")

host = os.getenv("host")
user_name = os.getenv("userName")
password = os.getenv("password")
index_name = os.getenv("indexName")

es = Elasticsearch(host, http_auth=(user_name, password), scheme="https", port=443)

class Hotel:
    def __init__(self, name: str, city_name: str, price: int, rating: int,
                 user_id: str, id: str, creation_date_time: str, file_name: str):
        self.name = name
        self.city_name = city_name
        self.price = price
        self.rating = rating
        self.user_id = user_id
        self.id = id
        self.creation_date_time = creation_date_time
        self.file_name = file_name


class HotelCreatedEventHandler(CoroutineHandler):
    async def process_event(self, sns_event: SNSEvent, context: LambdaContext) -> None:
        logger.info("Lambda was invoked.")
        logger.info(f"Found {len(sns_event.records)} records in SNS Event")

        for event_record in sns_event.records:
            event_id = event_record.sns.message_id
            found_item = table.query(KeyConditionExpression=Key("eventId").eq(event_id))
            if not found_item["Items"]:
                table.put_item(Item={"eventId": event_id})

            hotel = json.loads(event_record.sns.message, object_hook=lambda d: Hotel(**d))

            index_action = {
                "_index": index_name,
                "_id": hotel.id,
                "_source": {
                    "name": hotel.name,
                    "cityName": hotel.city_name,
                    "price": hotel.price,
                    "rating": hotel.rating,
                    "userId": hotel.user_id,
                    "creationDateTime": datetime.fromisoformat(hotel.creation_date_time).isoformat(),
                    "fileName": hotel.file_name
                }
            }

            response = helpers.bulk(es, [index_action])
            if response[0].get("index", {}).get("error"):
                logger.error(f"Server Error: {response[0]['index']['error']['reason']}")

