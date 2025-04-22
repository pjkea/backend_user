import requests
from datetime import datetime, timedelta
import urllib.parse


def calculate_distance_and_eta(
        origin_longitude,
        origin_latitude,
        destination_address,
        api_key,
        travel_mode,
        departure_time,
        traffic_model
):
    """
    Calculates distance and ETA from coordinates to an address using Google Distance Matrix API

    Args:
        origin_longitude (float): Longitude of the starting point
        origin_latitude (float): Latitude of the starting point
        destination_address (str): The destination address as a string
        api_key (str): Your Google Maps API key
        travel_mode (str, optional): Mode of travel: 'driving', 'walking', 'bicycling', 'transit'. Defaults to 'driving'.

    Returns:
        dict: Distance and duration information with ETA

    Raises:
        ValueError: If required parameters are missing
        Exception: If API request fails
    """
    # Validate inputs
    if not all([origin_longitude, origin_latitude, destination_address, api_key]):
        raise ValueError("Missing required parameters")

    # Format the origin coordinates
    origin = f"{origin_latitude},{origin_longitude}"

    # Encode the destination address for URL
    destination = urllib.parse.quote(destination_address)

    # Build the API URL
    url = f"https://maps.googleapis.com/maps/api/distancematrix/json?origins={origin}&destinations={destination}&mode={travel_mode}&key={api_key}"

    try:
        # Make the API request
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        data = response.json()

        # Check for API errors
        if data.get('status') != 'OK':
            error_message = data.get('error_message', 'Unknown error')
            raise Exception(f"Google API error: {data.get('status')} - {error_message}")

        # Check if we have results
        if not data.get('rows') or not data['rows'][0].get('elements') or not data['rows'][0]['elements'][0]:
            raise Exception("No results found")

        element = data['rows'][0]['elements'][0]

        # Check if the route was found
        if element.get('status') != 'OK':
            raise Exception(f"Route error: {element.get('status')}")

        # Calculate ETA
        eta = calculate_eta(element['duration']['value'])

        # Extract the distance and duration information
        result = {
            "origin_coordinates": {
                "longitude": origin_longitude,
                "latitude": origin_latitude
            },
            "destination_address": destination_address,
            "distance": {
                "text": element['distance']['text'],
                "value": element['distance']['value']  # Distance in meters
            },
            "duration": {
                "text": element['duration']['text'],
                "value": element['duration']['value']  # Duration in seconds
            },
            "eta": eta
        }

        return result

    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {str(e)}")
    except Exception as e:
        raise Exception(f"Error calculating distance: {str(e)}")


def calculate_eta(duration_in_seconds):
    """
    Calculates the estimated time of arrival based on current time and duration

    Args:
        duration_in_seconds (int): Travel duration in seconds

    Returns:
        dict: Object containing ETA in different formats
    """
    now = datetime.now()
    eta_datetime = now + timedelta(seconds=duration_in_seconds)

    return {
        "timestamp": int(eta_datetime.timestamp()),
        "iso": eta_datetime.isoformat(),
        "formatted": eta_datetime.strftime("%I:%M %p")
    }

# Example usage:
# api_key = "YOUR_GOOGLE_API_KEY"
# result = calculate_distance_and_eta(
#     origin_longitude=-73.935242,
#     origin_latitude=40.730610,
#     destination_address="1600 Amphitheatre Parkway, Mountain View, CA",
#     api_key=api_key
# )
# print(result)