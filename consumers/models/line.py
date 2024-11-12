"""Contains functionality related to Lines"""
import json
import logging

from models import Station

logger = logging.getLogger(__name__)

class Line:
    def __init__(self, color):
        """Creates a line."""
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = ""
        elif self.color == "red":
            self.color_code = ""
        elif self.color == "green":
            self.color_code = ""
        self.stations = {}
    
    def _handle_station(self, value):
        """Adds the station to this Line's data model."""
        if value["line"] != self.color:
            return
        self.station[value["station_id"]] = Station.from_message(value)
    
    def _handle_arrival(self, message):
        """Updates train locations"""
        value = message.value()
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            if prev_station is not None:
                prev_station.handle_departure(prev_dir)
            else:
                logger.debug("Unable to handle previous station due to missing station")
        else:
            logger.debug("unable to handle previous station due to missing previous info")

        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        if station is None:
            logger.debug("Unable to handle message due to missing station ")