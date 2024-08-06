import requests
import json

from quixstreams.sources import IterableBaseSource

class StibVehiculePositionSource(IterableBaseSource):
    def get_vehicule_positions(self):
        response = requests.get("https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/vehicle-position-rt-production/records?limit=100", headers={"Authorization": "Apikey 4d0b37aa165b9e393ecf9fff5a887fe0e2bf8717a4d1fa8f5b138635"})
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

            self.sleep(20)

class StibStopsDetailsSource:

    def __init__(self):
        self.details = {}

        response = requests.get("https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/stop-details-production/exports/json?lang=en&timezone=Europe%2FBerlin", headers={"Authorization": "Apikey 4d0b37aa165b9e393ecf9fff5a887fe0e2bf8717a4d1fa8f5b138635"})
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

class StibWaitTimeSource(IterableBaseSource):
    def get_waittime(self):
        response = requests.get("https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/waiting-time-rt-production/exports/json?lang=en&timezone=Europe%2FBerlin", headers={"Authorization": "Apikey 4d0b37aa165b9e393ecf9fff5a887fe0e2bf8717a4d1fa8f5b138635"})
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

            self.sleep(20)