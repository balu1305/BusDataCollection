#only manual input given to file
#note that 1st letter is always upper case and rest of all are lowercase letters
src_name = "Hyderabad"
dst_name = "Tirupati"
#delay_days=1
next_day = "26-Jun-2024"


import requests
import pandas as pd
import datetime
import time
import shlex
import os
import json
import numpy as np
import csv

# List of cities with their IDs
cities = {
    "Mumbai": 462,
    "Hyderabad": 124,
    "Bangalore": 122,
    "Chennai": 123,
    "Vizag": 248,
    "Vijayawada": 134,
    "Guntur": 137,
    "Tirupati": 71756,
    "Madurai": 126,
    "Coimbatore": 141,
    "Trichy": 71929,
    "Pondicherry": 233,
    "Pune": 130,
    "Indore": 313,
    "Nagpur": 624,
    "Goa": 210,
    "Warangal": 95479,
    "Nellore": 131,
    "Kochi": 216,
    "Dharwad": 167,
    "Mysore": 129,
    "Mangaluru": 95222,
    "Thiruvananthapuram": 71425
}
#date_of_extraction = (datetime.datetime.now() + datetime.timedelta(days=delay_days))
#next_day = (datetime.datetime.now() + datetime.timedelta(days=delay_days)).strftime('%d-%b-%Y')
main_filepath=fr"C:\Users\T VIJAYA BALAJI\Desktop\final_data_collection\{src_name}_to_{dst_name}\data_on_{next_day}"
os.makedirs(main_filepath,exist_ok=True)
print(next_day)

#1 getting different buses in a route
src_id = cities[src_name]
dst_id = cities[dst_name]




def fetch_data(from_city_id, to_city_id, src_name, dst_name, max_retries=10, retry_delay=5):
    #next_day = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%d-%b-%Y')
    url = f'https://www.redbus.in/search/SearchV4Results?fromCity={from_city_id}&toCity={to_city_id}&DOJ={next_day}&sectionId=0&groupId=0&limit=0&offset=0&sort=0&sortOrder=0&meta=true&returnSearch=0'
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
    }
    
    for attempt in range(max_retries):
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            print(f"404 Not Found: The requested resource for {src_name} to {dst_name} does not exist.")
            return None  # Exit if resource not found
        else:
            print(f"Attempt {attempt + 1}: Access Denied with error code: {response.status_code}")
            time.sleep(retry_delay)  # Wait before the next retry

    print(f"Failed to retrieve data after {max_retries} attempts.")
    return None

#data = fetch_data(src_id, dst_id, src_name, dst_name)
#data


summary_data = []

data = fetch_data(src_id, dst_id, src_name, dst_name)
if data:
        buses_info = [
            {
                    "travelsName": inv['travelsName'],
                    "totalRatings": inv['totalRatings'],
                    "TotaReviews":inv['numberOfReviews'],
                    "departureTime": inv['departureTime'],
                    "arrivalTime": inv['arrivalTime'],
                    "operatorId": inv['operatorId'],
                    "routeId": inv['routeId'],
                    "BusType": inv['busType'],
                    "AC": "YES" if inv.get('isAc', False) else "NO",
                    "Seater": "YES" if inv.get('isSeater', False) else "NO",
                    "Sleeper": "YES" if inv.get('isSleeper', False) else "NO",
            }
                for inv in data.get('inventories', [])
            ]
            
df = pd.DataFrame(buses_info)
# Add extra rows for summaries
total_buses = len(df)
highly_rated_buses = sum(df['totalRatings'] >= 4)
summary_rows = pd.DataFrame({
                "travelsName": ["Total Buses", "Buses Rated >= 4"],
                "totalRatings": [total_buses, highly_rated_buses],
                "departureTime": ["", ""],
                "arrivalTime": ["", ""],
                "operatorId": ["", ""],
                "routeId": ["", ""],
                "BusType": ["", ""],
                "AC": ["", ""],
                "Seater": ["", ""],
                "Sleeper": ["", ""]
            })
df = pd.concat([df, summary_rows], ignore_index=True)
now=datetime.datetime.now()
now = now.strftime("%I%p,%d-%m-%Y")            
filename = f"buses_on_{next_day}_at_{now}.csv"
filepath  = os.path.join(main_filepath,filename)
df.to_csv(filepath, index=False)
print(f"Saved {filename}")

#2 saving seat data of each bus in diffrent directories
#input for cell : csv file contining different buses
now=datetime.datetime.now()
now = now.strftime("%I%p,%d-%m-%Y")           
filename = f"buses_on_{next_day}_at_{now}.csv"
buses_csv_file_path  = os.path.join(main_filepath,filename)

dummyfilename=f"buses_on_{next_day}"
output_dir=os.path.join(main_filepath,dummyfilename)


#step-1:first setting our curl command
#this function takes tok, fromCityId, toCityId,operator_id,route_id as input and returns curl command important thing for using this function is giving token input argumrnt for the function as it is not taken from any exterior csv files by default im giving token as some value
def generate_curl_command(tok, fromCityId, toCityId,operator_id,route_id):
    # Calculate the next day's date
    #next_day = (datetime.datetime.now() + datetime.timedelta(days=delay_days)).strftime('%d-%b-%Y')    
    # Base cURL command template
    curl_command_template = '''
    curl 'https://www.redbus.in/search/seatlayout/{route_id}/{date}/{operator_id}?isRedDealApplicable=false&tok={tok}&srcCountry=IND&InvPos=1' \
      -H 'accept: application/json, text/plain, */*' \
      -H 'accept-language: en-US,en;q=0.9' \
      -H 'cookie: jfpj=386e43f4851d3c7d15936f880c19eeb7; _gcl_au=1.1.1651692991.1715575898; tvc_smc_bus=google / organic / (not set); mriClientId=WD5c509415-c921-4880-ab5b-ad9a968ccd55; gClId=1195023281.1715575899; rtcInline=V2; dynamic_custinfo=V1; srcCountry=IND; destCountry=IND; _fbp=fb.1.1715575924793.1120299924; srpUserTypeVal=GUEST; singleSeatCoachMark=2; rbuuid=512ed1b0-110b-11ef-98bc-155d27275705; userSessionId=ID_hd5t56d1l; rb_fpData=%7B%22browserName%22%3A%22Chrome%22%2C%22browserVersion%22%3A%22125.0.0.0%22%2C%22os%22%3A%22Windows%22%2C%22osVersion%22%3A%2210%22%2C%22screenSize%22%3A%221536%2C816%22%2C%22screenDPI%22%3A1.25%2C%22screenResolution%22%3A%221920x1080%22%2C%22screenColorDepth%22%3A24%2C%22aspectRatio%22%3A%2216%3A9%22%2C%22systemLanguage%22%3A%22en-US%22%2C%22connection%22%3A%224g%22%2C%22userAgent%22%3A%22mozilla/5.0%20%28windows%20nt%2010.0%3B%20win64%3B%20x64%29%20applewebkit/537.36%20%28khtml%2C%20like%20gecko%29%20chrome/125.0.0.0%20safari/537.36%7CWin32%7Cen-US%22%2C%22timeZone%22%3A5.5%7D; PrimoDetails=%7B%22lastUpdated%22%3A%222024-05-13%22%2C%22lastShown%22%3A%222024-05-21%22%7D; country=IND; currency=INR; defaultlanguage=en; language=en; selectedCurrency=INR; abExps=["rtcInline","dynamic_custinfo"]; abExpsVariants=rtcInline:V2?dynamic_custinfo:V1; tvc_session_alive_bus=1; _gid=GA1.2.319393201.1716373399; _gat_UA-9782412-15=1; _dc_gtm_UA-9782412-15=1; reqOrigin=IND; mriClientIdSetDate=5%2F22%2F24%2010%3A23%3A18%20AM; bCore=1; defaultCountry=IND; deviceSessionId=9ac6deb2-0de7-4b20-bfe8-89d368afde55; mriSessionId=WD64475fb4-f557-48b7-83d0-b25b93f53db5; lzFlag=0; ak_bmsc=E38D836E2B2730F2D6C6BB99391EECB8~000000000000000000000000000000~YAAQlbcsMYJyj22PAQAAAabTnxfj82vLRKhm0rVw1tPN/7C3V/fgXrHT2TEfWe9Xmap3gCy5u9YXAKkeTtkEfYtTMHjo5Ao4sDeu+N++Z1ii1hnkQq72j7R4msCw+Qzg7Lgn2l96lJfJY2KGOYo7u2Ov3AH6ZH//Aa5LcAvPv8jp7xbyGAI8DF1RxZmXVB8gYphPoYSZBr6jLI9YwxUQ+zRlqRXARNDe/POGEYmgxfJbIEjcfNKiAQF+lj93t7oenXuzbfbAWSvgAvhi7Z4vEWRa0+RdunkyFaz24f6QqnkNMqmFXcWbmvwjOUaTpL9HV+J53ol8Sg4/t6lV6vs4AxKTtQXuU/xjfeJCoXDE7jIXm2ShwliyKNXJtv/HR+L9NIA4Uq3snlY/DMiQsbhNd4qCk09A7aRN5gbjLNTaMMMEc5pSPenXpB0pN/CG96X8TsLa5b6ubrR0mCs0; moe_uuid=753cb754-3e23-4350-bf29-e4520bf8dd35; isBrowserFP=true; Branch_BrowserFingerPrintID=1216998847458037070; CountryName=INDIA; _ga=GA1.2.1195023281.1715575899; searchId=9cbe2c87741b4254b9532dde86b82a97; bm_sv=089DFD73A6BC507E91CD2DF0A0D29533~YAAQlbcsMed3j22PAQAAiu7TnxcLdq7ZwUnGxk0PRHY1UutQCg23eTGz4vxaVNUgHXppoUHEc+LpI6VnNWdkqfey4cApMWHVtug7cJxna1ssGdvomMTQgvfg00k3VIGGY4oegTbFN3uSvncE5Zdoxm6/NTGY3sCoAXkzOXcDI3zEaag9y4R7eukZGQnoJktqNUP2LoOTfBeUg5DDzX6W6WNhKEGnT4FxuXuGRDI2ASHNbbrdaK+OfCzJE6t7+csr~1; resumeBook=true; _ga_3NXW5V9V8S=GS1.2.1716373399.28.1.1716373424.35.0.0; _VTok={tok}; _ga_1SE754V89Y=GS1.1.1716373398.36.1.1716373424.34.0.117391036' \
      -H 'referer: https://www.redbus.in/search?fromCityId={fromCityId}&srcCountry=IND&toCityId={toCityId}&destCountry=IND&onward={date}&opId=0&busType=Any' \
      -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36' \
 '''
    # Format the cURL command with the provided arguments
    curl_command = curl_command_template.format(tok=tok, fromCityId=fromCityId, toCityId=toCityId, date=next_day,operator_id=operator_id,route_id=route_id)    
    return curl_command

#step-2:changing my curl request to response.json
#this function takes curl command as input and returns data of json file we can take json data of particualr curl
def curl_to_response_json(curl_command):
    tokens = shlex.split(curl_command)
    headers, data, url = {}, None, None
    it = iter(tokens)
    for token in it:
        if token == 'curl': continue
        elif token.startswith('http'): url = token
        elif token in ['-X', '--request']: method = next(it).upper()
        elif token in ['-H', '--header']: header = next(it); key, value = header.split(':', 1); headers[key.strip()] = value.strip()
        elif token in ['-d', '--data', '--data-raw', '--data-binary']: data = next(it)
    if headers.get('Content-Type') == 'application/json' and data: data = json.loads(data)
    for attempt in range(5):
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200: return response.json()
        elif response.status_code == 404: print("404 Not Found."); return None
        else: print(f"Attempt {attempt+1}: Error {response.status_code}"); time.sleep(2)
    print("Failed to retrieve data after 5 attempts."); return None

#step-3:saving my json file to my required format csv file
#takes data as input which is assumed to be a json file and saving the output csv file  
def response_json_to_required_csv_format(data,directory_for_saving_seat_data_of_particular_route,operator_id,route_id):
    # Navigate to the 'seatlist' which contains the relevant data
    seatlist = data.get('seatlist', [])

    # Prepare a list to hold the data
    extracted_data = []

    # Extract data from each seat
    for seat in seatlist:
        fares_amount = seat.get('fares', {}).get('amount', None)
        id = seat.get('Id', None)
        is_available = seat.get('IsAvailable', None)
        
        # Append the extracted data as a tuple to the list
        extracted_data.append((id, fares_amount, is_available))

    # Convert the list of tuples into a DataFrame
    df = pd.DataFrame(extracted_data, columns=['seat', 'Fares_Amount', 'Is_Available'])
    df['occupied'] = df['Is_Available'].apply(lambda x: 0 if x else 1)
    df['revenue'] = df['Fares_Amount'] * df['occupied']

    travels_name = data.get('Travels', 'Unknown_Travel')
    fromcity=data.get('FromCity','Unkown_city')
    tocity=data.get('ToCity','Unkown_city')
    

    # Add a final row with the total revenue
    final_row = pd.DataFrame({
        'seat': ['Travels Name','journey route','operator_id','route_id'],
        'Fares_Amount': ['','','',''],
        'Is_Available': ['','','',''],
        'occupied': ['','','',''],
        'revenue': [f'{travels_name}',f'{fromcity}-{tocity}',operator_id,route_id]
    })

    # Append the row to the DataFrame
    df = pd.concat([df, final_row], ignore_index=True)

    # Get the current date and time
#    current_datetime = datetime.datetime.now()

    # Format the current date and time
 #   formatted_datetime = current_datetime.strftime("%I.%M%p,%d-%m-%Y")
    
    now=datetime.datetime.now()    
    now = now.strftime("%d-%m-%Y,%H-%M") 
    file_name_format = f"{now}.csv"
    #filename changed for convinece here
    #from     file_name_format = f"{travels_name}_{operator_id}_{route_id}_seat_data_on_{formatted_datetime}_at_{now}.csv"
    #to     file_name_format = f"seat_data_on_{formatted_datetime}_at_{now}.csv"


    os.makedirs(directory_for_saving_seat_data_of_particular_route, exist_ok=True)

    # Generate the file name
    file_name = os.path.join(directory_for_saving_seat_data_of_particular_route,file_name_format )

    try:
       df.to_csv(file_name, index=False)
       print(f"Data extraction complete and saved to '{file_name}'.")
    except Exception as e:
       print(f"Failed to save data to '{file_name}': {e}")   


def process_single_route(file_path):
    df = pd.read_csv(file_path)
    df = df[:-2]

    for index, row in df.iterrows():
        tok = 62480135398
        operator_id = int(row['operatorId'])
       # print(operator_id)
        route_id = int(row['routeId'])
       # print(route_id)
        fromCityId = cities[src_name]
       # print(fromCityId)
        toCityId = cities[dst_name]        
       # print(toCityId)
        output_directory = os.path.join(output_dir, f"{row['travelsName']}_{operator_id}_{route_id}")
        os.makedirs(output_directory, exist_ok=True)
        if not np.isnan(fromCityId) and not np.isnan(toCityId):
          curl_command = generate_curl_command(tok, fromCityId, toCityId, operator_id, route_id)
          data = curl_to_response_json(curl_command)
          if data:
            response_json_to_required_csv_format(data, output_directory,operator_id,route_id)
          else:
            print(f"Failed to retrieve data for row index {index}")

process_single_route(buses_csv_file_path)
print("sucesfully fetched data :)")

