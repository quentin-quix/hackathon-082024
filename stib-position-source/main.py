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



class StibVehiculePositionSource(BaseSource):
    def get_vehicule_positions(self):
        response = requests.get("https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/vehicle-position-rt-production/records?limit=100")
        try:
            response.raise_for_status()
        except Exception:
            print(response.headers)
            raise

        data = response.json()
        return data["results"]

    def __iter__(self):
        while True:
            for line in self.get_vehicule_positions():
                for vehicle in json.loads(line["vehiclepositions"]):
                    vehicle["lineId"] = line["lineid"]
                    yield self.serialize(line["lineid"], vehicle)

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
        consumer_group="stib-vehicule-position-source",
        auto_create_topics=True,
        loglevel="DEBUG",
        auto_offset_reset="earliest"
    )
    
    source_topic = app.topic(os.environ["source_output"])
    transform_topic = app.topic(os.environ["transform_output"])
    
    sdf = app.dataframe(source=StibVehiculePositionSource(), topic=source_topic)
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
