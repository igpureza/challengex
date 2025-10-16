import requests 
import json

import requests
import json

'''url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

payload = {
    "filters": {},
    "limit": 5
}

print("Testing...")
print(f"Python requests version: {requests.__version__}")
print(f"URL: {url}")
print(f"Payload type: {type(payload)}")
print(f"Payload: {payload}\n")

response = requests.post(url, json=payload)

print(f"Status: {response.status_code}")
print(f"Response headers: {response.headers}")
print(f"Response text: {response.text}")
'''


def search_awards():
   
    search_url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

    payload = {
        "filters": {
            "award_type_codes": ["A","B","C","D"]
        },
        "limit": 5, 
        "page": 1,
        "fields": [
          "Award ID",
          "Recipient Name",
          "Award Amount",
          "Description"
      ]

       
    }

    print("Searching for awards ...\n")
    search_response = requests.post(search_url, json=payload)

    if search_response.status_code == 200:
        results = search_response.json().get('results', [])
        if results:
            first_award = results[0]
            award_id = first_award.get('generated_internal_id')

            print(f"Found award ID: {award_id}")
            print(f"Award: {first_award.get('Description', 'N/A')[:100]}...\n")

            #Get full details
            details_url = f"https://api.usaspending.gov/api/v2/awards/{award_id}/"
            print(f"Fetching details from: {details_url}\n")

            details_response = requests.get(details_url)

            if details_response.status_code == 200:
                data = details_response.json()

                print("Success! Here's the full data: \n")
                print(json.dumps(data, indent = 2))

                with open('award_details.json', 'w') as f:
                    json.dump(data, f, indent = 2)
                    print("\n Saved to award_details.json")
            else: 
                print(f"Details request failed: {details_response.status_code}")
        else:    
            print(f"No results found")
    else:
        print(f"Search failed: {search_response.status_code}")


if __name__== "__main__":
    search_awards()






    

      