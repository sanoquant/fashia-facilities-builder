import asyncio
import aiohttp
import sqlite3
import json
import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()
APPLE_MAPS_API_TOKEN = os.getenv("APPLE_MAPS_API_TOKEN")

async def get_access_token(session):
    """
    Retrieves a new access token from the authentication endpoint.
    """
    url = "https://maps-api.apple.com/v1/token"
    headers = {
        'Authorization': f'Bearer {APPLE_MAPS_API_TOKEN}'
    }
    try:
        async with session.post(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return data['accessToken']
            else:
                print(f"Error obtaining access token: HTTP {response.status}")
                return None
    except Exception as e:
        print(f"Error obtaining access token: {e}")
        return None

async def geocode_address(session, address_hash, address, city, state_code, zip_code, api_token):
    """
    Sends a geocode request to the Apple Maps API for a given address.
    """
    full_address = f"{address}, {city}, {state_code}, {zip_code}"
    encoded_address = urllib.parse.quote(full_address)
    url = f"https://maps-api.apple.com/v1/geocode?q={encoded_address}"
    headers = {
        'Authorization': f'Bearer {api_token}'
    }
    try:
        async with session.get(url, headers=headers, timeout=4) as response:
            if response.status == 401:
                print(f"Token expired for address {full_address}, refreshing token.")
                new_token = await get_access_token(session)
                if new_token:
                    return await geocode_address(session, address_hash, address, city, state_code, zip_code, new_token)
                else:
                    print(f"Error refreshing token for address {full_address}.")
                    return None
            if response.status != 200:
                print(f"Error fetching data for address {full_address}: HTTP {response.status}")
                return None
            data = await response.json()
            if 'error' in data or len(data.get('results', [])) == 0:
                print(f"Error geocoding address {full_address}: {data}")
                return None
            latitude = data['results'][0]['coordinate']['latitude']
            longitude = data['results'][0]['coordinate']['longitude']
            return {
                'address_hash': address_hash,
                'latitude': latitude,
                'longitude': longitude
            }
    except asyncio.TimeoutError:
        print(f"Request timed out for address {full_address}")
    except Exception as e:
        print(f"Request failed for address {full_address}: {e}")
    return None

def load_addresses_from_db(db_path):
    """
    Loads addresses from the SQLite database that are not already in the address_geolocation table.
    Normalizes zip codes to ensure they have 5 digits by padding with leading zeros.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT a.address_hash, a.address, a.city, s.State_code, a.zip_code
        FROM addresses a
        JOIN states s ON a.state_id = s.state_id
        WHERE a.address_hash NOT IN (SELECT address_hash FROM address_geolocation);
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    addresses = [
        {
            'address_hash': row[0],
            'address': row[1],
            'city': row[2],
            'state_code': row[3],
            'zip_code': str(row[4]).zfill(5)
        }
        for row in rows
    ]
    return addresses

def save_results_to_db(results, db_path):
    """
    Saves the geocoding results to the address_geolocation table in the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO address_geolocation (address_hash, latitude, longitude)
        VALUES (?, ?, ?)
    """
    cursor.executemany(insert_query, [(r['address_hash'], r['latitude'], r['longitude']) for r in results])

    conn.commit()
    conn.close()
    print(f"Saved {len(results)} geocoding results to the database.")

async def process_addresses(addresses, api_token):
    """
    Processes a list of addresses and performs geocoding in parallel using chunks.
    """
    MAX_CONCURRENT_REQUESTS = 80
    results = []
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(addresses), MAX_CONCURRENT_REQUESTS):
            chunk = addresses[i:i + MAX_CONCURRENT_REQUESTS]
            chunk_results = await asyncio.gather(
                *(geocode_address(session, address['address_hash'], address['address'], address['city'], address['state_code'], address['zip_code'], api_token) for address in chunk)
            )
            results.extend(filter(None, chunk_results))
    return results

async def main():
    """
    Main function to orchestrate loading data, performing geocoding, and saving results.
    """
    db_path = 'facilities.db'

    # Load addresses that need geocoding
    addresses = load_addresses_from_db(db_path)

    if not addresses:
        print("No addresses found that need geocoding.")
        return

    # Get access token and perform geocoding
    async with aiohttp.ClientSession() as session:
        access_token = await get_access_token(session)
        if access_token:
            results = await process_addresses(addresses, access_token)
            save_results_to_db(results, db_path)
        else:
            print("Failed to obtain access token.")

if __name__ == "__main__":
    asyncio.run(main())