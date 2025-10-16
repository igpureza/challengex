import requests 
import os 
from dotenv import load_dotenv
from search_public import geocode_address
from search_public import geocode_address_fallback

load_dotenv()

if __name__ == "__main__":
    MAPBOX_TOKEN = os.getenv('MAPBOX_TOKEN')

    test_addresses = [
        {
            'address': '1600 Pennsylvania Avenue NW',
            'city': 'Washington',
            'state': 'DC',
            'zip': '20500'
        },
        {
            'address': '3775 SEAPORT BLVD',
            'city': 'WEST SACRAMENTO',
            'state': 'CA',
            'zip': '95691'
        },
        {
            'address': '1 Main Street',
            'city': 'New York',
            'state': 'NY',
            'zip': '10001'
        }
    ]

    print("Testing Mapbox geocoding...\n")

    for i, addr in enumerate(test_addresses, 1):
        print(f"{i}. Testing: {addr['address']}, {addr['city']}, {addr['state']}")
        
        lat, lon = geocode_address(
            addr['address'],
            addr['city'],
            addr['state'],
            addr['zip'],
            MAPBOX_TOKEN
        )

        if lat and lon:
            print(f"Success: ({lat}, {lon})")
        else:
            print(f"Falied to geocode")
        print()