import requests 
import json
import time
import os 
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("https://", adapter)
    return session

session = make_session()

def search_public_projects(keyword, award_group="contracts", limit=50):
    search_url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

    award_types = {
        "contracts": ["A", "B", "C", "D"],
        "grants": ["02", "03","04", "05"],
        "other": ["06", "10"] 
    }

    payload = {
        "filters": {
            "keywords" : [keyword],
            "award_type_codes": award_types[award_group],
            "time_period": [{
                "start_date": "2015-01-01",
                "end_date": "2024-12-31"
            }]
        },
        "fields": ["Award ID", "Recipient Name", "Award Amount", "Description", "Place of Performance City Code", "Place of Performance State Code"],
        "limit": limit, 
        "page": 1,
        "sort": "Award Amount",
        "order": "desc"
    }

    response = session.post(search_url, json=payload, timeout=30)

    if response.status_code != 200:
        print(f"Search error: {response.status_code}")
        print(response.text)
        return []
    
    results = response.json().get('results', [])

    include_keywords = ['public', 'visitor', 'community', 'facility', 'building', 'construction', 'renovation', 'park', 'museum', 'library']
    '''exclude_keywords = [
        'software license',
        'it services',
        'consulting services',
        'technical support contract', 
        'maintenance agreement',
        'training services'
    ]'''

    public_projects = []
    for award in results:
        description = (award.get('Description') or '').lower()
        recipient = (award.get('Recipient Name') or '').lower()

        '''
        should_exclude = any(kw in description for kw in exclude_keywords)

        if not should_exclude:
            public_projects.append(award)
        '''

        #Include description matches or recipient is gov/nonprofit
        has_good_description = any(kw in description for kw in include_keywords)
        is_public_recipient = any(word in recipient for word in ['city of', 'county of', 'state of', 'university', 'park service', 'department', 'commission'])

        if has_good_description or is_public_recipient:
            public_projects.append(award)


    if (len(public_projects) != 0):
        print(f"Found {len(public_projects)} public projects (from {len(results)} total)")
        print(f"Description: {public_projects[0].get('Description')}")
        print(f"Recipient: {public_projects[0].get('Recipient Name')}")

    return public_projects


def search_all_award_types(keyword, limit_per_group=25):
    #Search both contracts and grants for a keyword since API won't let us search them at the same time

    all_projects = []

    #Search contracts
    contracts = search_public_projects(keyword, "contracts", limit_per_group)
    all_projects.extend(contracts)

    #Wait in between API calls
    time.sleep(1.0)

    #Search grants 
    grants = search_public_projects(keyword, "grants", limit_per_group)
    all_projects.extend(grants)

    return all_projects

def get_award_details(award_id):
    #Get full details including location

    url = f"https://api.usaspending.gov/api/v2/awards/{award_id}/"
    response = session.get(url, timeout = 30)

    if response.status_code == 200:
        return response.json()
    else:
        return None
    

def geocode_address(address, city, state, zip_code, mapbox_token):
    #Convert address to lat/lon using MapBox (forward geocoding)

    if not city or not state:
        return None, None

    url = "https://api.mapbox.com/search/geocode/v6/forward"

    if address:
        params = {
            'address_line1': address,
            'place': city, 
            'region': state, 
            'country': 'US',
            'access_token': mapbox_token,
            'limit': 1, 
        }

        if zip_code:
            params['postcode'] = zip_code


        try:
            response = session.get(url, params=params, timeout = 30)

            if response.status_code == 200:
                data = response.json()

                #Check if we got results 
                if data.get('features') and len(data['features']) > 0:
                    feature = data['features'][0]

                    lon, lat = feature['geometry']['coordinates']

                    return lat, lon
            
        except Exception as e:
            print(f"Geocoding exception: {e}")
    #Fallback: use city + state
    print(f"No street address, geocoding city center: {city}, {state}")
    return geocode_address_fallback(city, state, mapbox_token)

def geocode_address_fallback(city, state, mapbox_token):

    url = "https://api.mapbox.com/search/geocode/v6/forward"
    
    params = {
        'q': f"{city}, {state}, USA",
        'access_token': mapbox_token,
        'limit': 1,
        'types': 'place',
        'country': 'US'
    }

    try:
        response = session.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('features') and len(data['features']) > 0:
                feature = data['features'][0]
                lon, lat = feature['geometry']['coordinates']
                return lat, lon
                
        return None, None
        
    except Exception as e:
        print(f"City fallback failed: {e}")
        return None, None

load_dotenv()

#This function transforms API data into a database ready format
def prepare_project_data(award_details, mapbox_token):
    #Extract and prepare data for database

    print("\n---Preparing Project Data ---")

    location = award_details.get('place_of_performance', {})
    print(f"Extracting location...")
    print(f"  Address: {location.get('address_line1')}")
    print(f"  City: {location.get('city_name')}")
    print(f"  State: {location.get('state_code')}")

    project = {
        'source_id': award_details.get('id'),
        'title': (award_details.get('description') or 'N/A')[:255],  # Truncate to 255 chars
        'agency': award_details.get('awarding_agency', {}).get('toptier_agency', {}).get('name'),
        'recipient': award_details.get('recipient', {}).get('recipient_name'),
        'amount': award_details.get('total_obligation'),
        'fiscal_year': award_details.get('fiscal_year'),
        'description': award_details.get('description'),
        'city': location.get('city_name'),
        'state': location.get('state_code'),
        'address': location.get('address_line1'),
        'zip_code': location.get('zip5'),
        'category': None,  # We'll categorize later
        'latitude': None,
        'longitude': None
    }

    print(f"\nExtracted project info:")
    print(f"  Title: {project['title'][:60]}...")
    print(f"  Amount: ${project['amount']:,.2f}")
    print(f"  Agency: {project['agency']}")

    if project['address'] or project['city'] and project['state'] and project['zip_code']:
        print(f"\nGeocoding address...")

        lat, lon = geocode_address(
            project['address'],
            project['city'],
            project['state'],
            project['zip_code'],
            mapbox_token
        )

        project['latitude'] = lat
        project['longitude'] = lon

        if lat and lon:
            print(f"Geocoded: ({lat}, {lon})")
        else:
            print("Geocoding failed")
    else:
        print("Skipping geocoding")
    
    return project

def collect_and_prepare_data(keywords, mapbox_token, projects_per_keyword=10):
    """
    Complete pipeline: Search -> Get Details -> Geocode -> Prepare for DB

    Args:
        keywords: List of keywords to search (e.g., ["museum", "park"])
        mapbox_token: Mapbox API token 
        projects_per_keyword: How many projects to collect per keyword

    Returns: 
        List of dictionaries, each representing a project ready for database
    """

    all_projects = []

    for keyword in keywords:
        print(f"Processing keyword: '{keyword}'")

        #1) Search for awards (both contracts and grants)
        search_results = search_all_award_types(keyword, limit_per_group=projects_per_keyword)

        print(f"\nFound {len(search_results)} awards total")
        print(f"Processing up to {projects_per_keyword}...\n")

        #Process each award and take only the top N projects
        for i, award in enumerate(search_results[:projects_per_keyword], 1):
            award_id = award.get('generated_internal_id')

            print(f"[{i}/{projects_per_keyword}] Processing {award_id}...")

            #2) Get full details from API
            details = get_award_details(award_id)
            if not details:
                print(f"Failed to get details, skipping")
                continue

            #3) Prepare project data (includes geocoding)
            project = prepare_project_data(details, mapbox_token)

            #Save if successfully geocoded
            if project['latitude'] and project['longitude']:
                all_projects.append(project)
                print(f"{project['title'[:50]]}... at ({project['latitude']}, {project['longitude']})")   
            else:
                print(f"Could not geocode: {project['title'][:50]}...")   

            time.sleep(0.5)
        
        print(f"\n Completed '{keyword}': {len([p for p in all_projects if keyword.lower() in p.get('title,', '').lower()])} prpjects added")

    return all_projects    

def save_to_json(projects, filename='projects_ready_for_db.json'):
    """
    Save prepared data to JSON file and print summary statistics 

    Args: 
        projects: list of project dictionaries
        filename: Where to save the JSON file 
    """              

    #Save to file 
    with open(filename, 'w') as f: 
        json.dump(projects, f, indent=2)

    print(f"Saved {len(projects)} projects to {filename}")

    #Get number of geocodes
    successful_geocode = len([p for p in projects if p['latitude']])
    failed_geocode = len(projects) - successful_geocode

    total_amount = sum(p['amount'] for p in projects if p['amount'])

    #Count by state
    states = {}
    for p in projects: 
        state = p.get('state')
        if state:
            states[state] = states.get(state, 0) + 1

    #Count by agency
    agencies = {}
    for p in projects:
        agency = p.get('agency', 'Unknown')
        if agency:
            agencies[agency] = agencies.get(agency, 0) + 1

    print(f"\nSUMMARY STATISTICS:")
    print(f"Total projects: {len(projects)}")
    print(f"Successfull geocoded: {successful_geocode}")
    print(f"Failed geocoding: {failed_geocode}")
    print(f"Total funding: ${total_amount:,.0f}")

    print(f"\nProjects by state:")
    for state, count in sorted(states.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"{state}: {count}")

    print(f"\nTop agencies:")
    for agency, count in sorted(agencies.items(), key=lambda x : x[1], reverse=True)[:5]:
        print(f"{agency}: {count}")

    #Sample project 
    '''if projects:
        print(f"\nSAMPLE PROJECT:")
        print(f"{'='*60}")
        sample = projects[0]
        print(f"  Title: {sample['title']}")
        print(f"  Amount: ${sample['amount']:,.0f}")
        print(f"  Location: {sample['city']}, {sample['state']}")
        print(f"  Coordinates: ({sample['latitude']}, {sample['longitude']})")
        print(f"  Agency: {sample['agency']}")
        print(f"  Recipient: {sample['recipient']}")
    '''

if __name__ == "__main__":
    MAPBOX_TOKEN = os.getenv('MAPBOX_TOKEN')

    print("="*60)
    print("USASpending Data Collection Pipeline")
    print("="*60)

    keywords = [
        "museum",
        "park",
        "library",
        "community center",
        "recreation center"
    ]

    projects_per_keyword = 5

    print(f"\nSearching for {projects_per_keyword} projects per keyword")
    print(f"Keywords: {', '.join(keywords)}")

    start_time = time.time()

    projects = collect_and_prepare_data(
        keywords=keywords,
        mapbox_token=MAPBOX_TOKEN,
        projects_per_keyword=projects_per_keyword
    )

    elapsed_time = time.time() - start_time

    save_to_json(projects)
    
    #search_public_projects("museum", "contracts", 20)
    print(f"Data ready for database!")
