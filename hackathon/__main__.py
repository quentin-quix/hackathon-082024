import os

from .apps import position, waittime

app = os.environ.get("QUIX_APP")

def main():
    if app == "position":
        position.main()
    elif app == "waittime":
        waittime.main()
    else:
        raise RuntimeError(f"unknown app '{app}'")

if __name__ == "__main__":
    main()
