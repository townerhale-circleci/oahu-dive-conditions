"""API clients for environmental data sources."""

from src.clients.buoy_client import BuoyClient, BuoyError
from src.clients.cwb_client import CWBClient, CWBError
from src.clients.noaa_tides_client import NOAATidesClient, NOAATidesError
from src.clients.nws_client import NWSClient, NWSError
from src.clients.openweathermap_client import OpenWeatherMapClient
from src.clients.pacioos_client import PacIOOSClient, PacIOOSError
from src.clients.usgs_client import USGSClient, USGSError

__all__ = [
    "BuoyClient",
    "BuoyError",
    "CWBClient",
    "CWBError",
    "NOAATidesClient",
    "NOAATidesError",
    "NWSClient",
    "NWSError",
    "OpenWeatherMapClient",
    "PacIOOSClient",
    "PacIOOSError",
    "USGSClient",
    "USGSError",
]
