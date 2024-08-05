import random
import os
import json
import requests

from quixstreams import Application
from quixstreams.sources import BaseSource
from quixstreams.models.messages import KafkaMessage

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

class StibWaitTimeSource(BaseSource):
    def get_waittime(self):
        response = requests.get("https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/waiting-time-rt-production/exports/json?lang=en&timezone=Europe%2FBerlin")
        try:
            response.raise_for_status()
        except Exception:
            print(response.headers)
            raise

        return response.json()

    def __iter__(self):
        while True:
            for item in self.get_waittime():
                passingtimes = json.loads(item["passingtimes"])
                for passingtime in passingtimes:
                    print(passingtime)
                    yield self.serialize(   
                        key=item["pointid"],
                        value={
                            "pointId": item["pointid"],
                            "lineId": item["lineid"],
                            "destination": passingtime.get("destination", {}).get("fr", ""),
                            "arrival_time": passingtime["expectedArrivalTime"],
                            "message": passingtime.get("message", "")
                        }
                    )

            self.sleep(5 * 60)


class StibStopsDetailsSource(BaseSource):

    def __init__(self):
        self.details = {}

        response = requests.get("https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/stop-details-production/exports/json?lang=en&timezone=Europe%2FBerlin")
        response.raise_for_status()
        data = response.json()
        for item in data:
            name = json.loads(item["name"])
            coordinates = json.loads(item["gpscoordinates"])

            self.details[item["id"]] = {
                "coordinates": {
                    "lat": coordinates["latitude"],
                    "long": coordinates["longitude"],
                },
                "name": name["fr"]
            }

StopDetails = StibStopsDetailsSource()
def enrich(value):
    detail = StopDetails.details.get(value["pointId"], {})
    value.update({
        "destination_coordinates": detail.get("coordinates", {"lat": 0, "long": 0}),
        "destination_name": detail.get("name", "unknown"),
    })
    return value

def main():
    app = Application(
        consumer_group="stib-waitime-source",
        auto_create_topics=True,
        loglevel="DEBUG",
        auto_offset_reset="earliest"
    )
    
    source_topic = app.topic(os.environ["source_output"])
    transform_topic = app.topic(os.environ["transform_output"])
    
    sdf = app.dataframe(source=StibWaitTimeSource(), topic=source_topic)
    # sdf = app.dataframe(topic=topic)
    # sdf = sdf.filter(lambda value: value["line"] == "29")

    sdf = sdf.apply(enrich)

    sdf.print()
    sdf.to_topic(transform_topic)

    app.run(sdf)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
