import os
import functools

from dotenv import load_dotenv

from quixstreams import Application

from ..sources import StibStopsDetailsSource, StibWaitTimeSource

def enrich(details, value):
    detail = details.get(value["pointId"], {})
    value.update({
        "destination_coordinates": detail.get("coordinates", {"lat": 0, "long": 0}),
        "destination_name": detail.get("name", "unknown"),
    })
    return value

def main():
    load_dotenv()
    
    StopDetails = StibStopsDetailsSource()

    app = Application(
        consumer_group="stib-waitime-source",
        auto_create_topics=True,
        loglevel="DEBUG",
        auto_offset_reset="earliest"
    )
    
    source_topic = app.topic(os.environ["source_output"])
    transform_topic = app.topic(os.environ["transform_output"])
    
    sdf = app.dataframe(source=StibWaitTimeSource(), topic=source_topic)
    sdf = sdf.apply(functools.partial(enrich, StopDetails.details))

    sdf.print()
    sdf.to_topic(transform_topic)

    app.run(sdf)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")
