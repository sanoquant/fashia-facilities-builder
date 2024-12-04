import asyncio
import aiohttp
import pandas as pd
import json
import urllib.parse
import os
from dotenv import load_dotenv

load_dotenv()
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

async def get_access_token(session):
    """
    Retrieves a new access token from the authentication endpoint.
    """
    url = "https://maps-api.apple.com/v1/token"
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}'
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

def load_addresses_from_csv(input_csv, states_csv):
    """
    Loads addresses and state mappings from CSV files and returns a list of dictionaries with the required data.
    """
    df_addresses = pd.read_csv(input_csv)
    df_states = pd.read_csv(states_csv)
    state_mapping = df_states.set_index('StateID')['StateCode'].to_dict()
    addresses = []
    for _, row in df_addresses.iterrows():
        state_code = state_mapping.get(row['state_id'], None)
        if not state_code:
            print(f"Warning: State ID {row['state_id']} not found in states.csv")
            continue
        address = {
            'address_hash': row['address_hash'],
            'address': row['address'],
            'city': row['city'],
            'state_code': state_code,
            'zip_code': row['zip_code']
        }
        addresses.append(address)
    return addresses

def save_results_to_csv(results, output_csv):
    """
    Saves the geocoding results to a CSV file.
    """
    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")

async def main():
    """
    Main function to orchestrate loading data, performing geocoding, and saving results.
    """
    input_csv = 'addresses.csv'
    states_csv = 'states.csv'
    output_csv = 'geocoded_addresses.csv'
    addresses = load_addresses_from_csv(input_csv, states_csv)
    async with aiohttp.ClientSession() as session:
        access_token = await get_access_token(session)
        if access_token:
            results = await process_addresses(addresses, access_token)
            save_results_to_csv(results, output_csv)
        else:
            print("Failed to obtain access token.")

if __name__ == "__main__":
    asyncio.run(main())