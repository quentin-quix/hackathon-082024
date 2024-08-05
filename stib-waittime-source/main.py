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
                    yield self.serialize(
                        key=item["pointid"],
                        value={
                            "pointid": item["pointid"],
                            "lineid": item["lineid"],
                            "destination": passingtime["destination"]["fr"],
                            "arrival_time": passingtime["expectedArrivalTime"],
                            "message": passingtime.get("message", "")
                        }
                    )

            self.sleep(5 * 60)


def main():
    app = Application(
        consumer_group="stib-waitime-source",
        auto_create_topics=True,
        loglevel="DEBUG",
        auto_offset_reset="earliest"
    )
    
    topic= app.topic(os.environ["output"])
    sdf = app.dataframe(source=StibWaitTimeSource(), topic=topic)
    # sdf = app.dataframe(topic=topic)
    # sdf = sdf.filter(lambda value: value["line"] == "29")
    sdf.print()

    app.run(sdf)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
