import pandas as pd
import requests
import json
import logging
import traceback
import time

def return_classification_headers():
    """
    Returns headers required for API requests.
    """
    return {
        'accept': 'application/ld+json',
        'Content-Type': 'application/merge-patch+json',
        'Authorization': 'Bearer 5t36bZGVkg3EAedmKcJKeYeaLLMgHjpB2zimBJP3HGN62nBY84vxqaA2ggGyssCZ'
    }

def return_classification_headers_for_post():
    """
    Returns headers required for API requests.
    """
    return {
        'accept': 'application/ld+json',
        'Content-Type': 'application/ld+json',
        'Authorization': 'Bearer 5t36bZGVkg3EAedmKcJKeYeaLLMgHjpB2zimBJP3HGN62nBY84vxqaA2ggGyssCZ'
    }

def return_organisation_headers():
    """
    Returns headers required for API requests.
    """
    return {
        'accept': 'application/ld+json',
        'Content-Type': 'application/merge-patch+json',
        'Authorization': 'Bearer xhjVCchLCSxYBua9bMR586PuAKPAPrUrtG9H9e2SC3cJoVvh5uHgXcB5E5pc2HEJ'
    }

def read_excel_data():
    """
    Reads Excel data from the given file path.
    """
    file_path = r"C:\Users\sarojsinha\Downloads\2024_City_State_Country Cleaning_v1.xlsx"
    return pd.read_excel(file_path, skiprows=1)


################################################################## CASE - 1 ################################################################################
# Where Corrected City Name
    # = CORRECT, no action
    # is null, no action (here)
    # anything else:
        # PATCH Classification /cities/[city_id]
        # {"name":"[Corrected City Name]"}

def patch_corected_cities_name():

    # Configure logging
    logging.basicConfig(filename='patch_city_case_1.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def patch_city_payload(city_id, city_name):
        """
        Constructs the URL and payload for patching city data.
        """
        url = f"https://classification-dev.dom-non-prod.with.digital/api/cities/{city_id}"
        payload = json.dumps({"name": city_name})
        return url, payload


    def patch_city(city_id, city_name):
        """
        Sends a PATCH request to update city data.
        """
        url, payload = patch_city_payload(city_id, city_name)
        headers = return_classification_headers()
        try:
            response = requests.patch(url, headers=headers, data=payload)
            response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
            logging.info(f'Corrected City Name updated for city_id {city_id}.')
        except requests.exceptions.RequestException as e:
            logging.error(f'Error updating city_id {city_id}: {e}')

    def process_row(row):
        """
        Processes a single row from the Excel data.
        """
        corrected_city_name = row['Corrected City Name']
        city_id = row['city_id']
        if corrected_city_name != "CORRECT" and not pd.isnull(corrected_city_name) and isinstance(city_id,int):
            patch_city(city_id, corrected_city_name)

    def process_excel_for_corrected_cities_name():
        """
        Processes the entire Excel file.
        """
        excel_data = read_excel_data()
        for _, row in excel_data.iterrows():
            process_row(row)

    # Call the main function to start processing
    try:
        process_excel_for_corrected_cities_name()
    except Exception as e:
        logging.error(f'An error occurred during processing: {e}')

# CASE - 1 --- The below function call is to update patch corrected city name
# patch_corected_cities_name()


################################################################## CASE - 2 ################################################################################
# Where city_id
    # = NEW or null
        # POST Classification /cities/
        #   {"name":"[Corrected City Name]",
        #    "subMarket":"/api/sub-markets/[Mapped State ID]",
        #   "country":"/api/countries/[Remapped Country ID]"}


def create_new_cities():
    """
        This function is to create a new city data with the above payload and parameters.
    """

    # Configure logging
    logging.basicConfig(filename='create_new_cities.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def create_city_payload(city_name,mapped_state_id, remapped_country_id):
        """
        Constructs the URL and payload for patching city data.
        """
        url = f"https://classification-dev.dom-non-prod.with.digital/api/cities"
        # data = f'{{"name":"{city_name}","subMarket":"/api/sub-markets/{mapped_state_id}","country":"/api/countries/{remapped_country_id}"}}'

        payload = json.dumps({
                    "name": f"{city_name}",
                    "subMarket": f"/api/sub-markets/{mapped_state_id}",
                    "country": f"/api/countries/{remapped_country_id}"
                    })
        return url, payload
    
    def create_city(city_name,mapped_state_id, remapped_country_id):
        """
        Sends a POST request to create new city data.
        """
        url, payload = create_city_payload(city_name,mapped_state_id, remapped_country_id)
        headers = return_classification_headers_for_post()
        try:
            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
            logging.info(f'New City Created with the deatils- name={city_name}, Mapped State ID={mapped_state_id}, Remapped Country ID={remapped_country_id}.')
        except requests.exceptions.RequestException as e:
            logging.error(f'Error creating City {city_name}: {e}')

    def process_row(row):
        """
        Processes a single row from the Excel data.
        """
        city_id = row['city_id']
        city_name = row['Corrected City Name']
        mapped_state_id = row['Mapped State ID']
        remapped_country_id = row['Remapped Country ID']

        if not isinstance(city_id, int) and (city_id is None or city_id.lower().startswith(("new",))):
            try:
                create_city(city_name, mapped_state_id, remapped_country_id)
            except Exception as e:
                print(f"An error occurred while creating the city-{city_name}: {e}")

    def process_excel_for_creating_cities():
        """
        Processes the entire Excel file.
        """
        excel_data = read_excel_data()
        for _, row in excel_data.iterrows():
            process_row(row)

    # Call the main function to start processing
    try:
        process_excel_for_creating_cities()
    except Exception as e:
        logging.error(f'An error occurred during processing: {e}') 

# CASE - 2 --- The below function call is to create new cities
# create_new_cities()

################################################################## CASE - 3 ################################################################################
# Where Remapped Country ID
    # is not null
        # PATCH Classification /cities/[city_id]
        # {"country":"/api/countries/[Remapped Country ID]"}

    # GET Organisation /locations?city=[city_id]
        # For each result
        # PATCH /locations/[x]
        # {"country":"[Remapped Country ID]"}

    # is null, no action

#################################################################### PART-1 ###############################################################################

# The below function is to patch classification /cities/[city_id] where Remapped Country ID is not null

def patch_cities_with_remapped_country_id():
    """
    This funtion is to patch classification.cities with the new [Remapped Country ID]
    """

    # Configure logging
    logging.basicConfig(filename='patch_cities_remapped_country_id_case_3.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def remapped_country_id_payload(city_id, remapped_country_id):
        """
        Constructs the URL and payload for patching country data.
        """
        url = f"https://classification-dev.dom-non-prod.with.digital/api/cities/{city_id}"
        payload = json.dumps({"country": f"/api/countries/{remapped_country_id}"})
        return url, payload

    def patch_remapped_country_id(city_id, remapped_country_id):
        """
        Sends a PATCH request to update city data.
        """
        url, payload = remapped_country_id_payload(city_id, remapped_country_id)
        headers = return_classification_headers()
        try:
            response = requests.patch(url, headers=headers, data=payload)
            response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
            logging.info(f'Remapped Country ID updated for city_id {city_id}.')
        except requests.exceptions.RequestException as e:
            logging.error(f'Error updating Remapped Country ID for city_id {city_id}: {str(e)}')

    def process_row(row):
        """
        Processes a single row from the Excel data.
        """
        remapped_country_id = row["Remapped Country ID"]
        city_id = row["city_id"]

        if (
            not isinstance(remapped_country_id, int)
            and not pd.isnull(remapped_country_id)
            and remapped_country_id.strip() != ""
            and remapped_country_id != "CORRECT"
            and remapped_country_id != "CHECKED"
        ):
            patch_remapped_country_id(city_id, remapped_country_id)

    def process_excel_for_remapped_country_id():
        """
        Processes the entire Excel file.
        """
        excel_data = read_excel_data()

        for _, row in excel_data.iterrows():
            process_row(row)

    try:
        process_excel_for_remapped_country_id()
    except Exception as e:
        logging.error(f'An error occurred during processing: {e}')

# CASE- III (PART-1) Function calling to patch the classification.cities with the [Remapped Country ID]
# patch_cities_with_remapped_country_id()

#################################################################### PART-2 ###############################################################################

# The below function is to get organistaion /locations?city=[city_id] and patch for each result /locations/[x]

def get_organisation_and_patch_locations_for_remapped_country_id():

    # Configure logging
    logging.basicConfig(filename='patch_location_for_remapped_country_id_case_3.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def get_organisation(city_id):
        """
        Constructs the URL and payload.
        """
        url = f"https://organisation-dev.dom-non-prod.with.digital/api/locations?city={city_id}"
        headers = return_organisation_headers()
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error occurred while fetching organisation data: {e}")
            return {}

    def process_row(row):
        """
        Processes a single row from the Excel data.
        """
        remapped_country_id = row.get('Remapped Country ID')
        city_id = row.get('city_id')
        result_dict = {}
        
        if (
            not isinstance(remapped_country_id, int)
            and not pd.isnull(remapped_country_id)
            and remapped_country_id.strip() != ""
            and remapped_country_id != "CORRECT"
            and remapped_country_id != "CHECKED"
        ):
            result_dict[city_id] = remapped_country_id

        return result_dict

    def process_excel_for_remapped_country_id():
        """
        Processes the entire Excel file.
        """
        all_ids_to_patch = []
        excel_data = read_excel_data()
        
        try:
            for _, row in excel_data.iterrows():
                row_dict = process_row(row)
                if row_dict:  # Check if the dictionary is not empty
                    all_ids_to_patch.append(row_dict)
        except Exception as e:
            logging.error(f"Error occurred while processing Excel data: {e}")

        return all_ids_to_patch

    def get_all_locations_to_patch():
        cities_id_and_remapped_country_id_list = process_excel_for_remapped_country_id()
        locations_to_patch_list = []

        for cities_id_and_remapped_country_id_dict in cities_id_and_remapped_country_id_list:
            for city_id, remapped_country_id in cities_id_and_remapped_country_id_dict.items():
                payload = json.dumps({"country": remapped_country_id})
                dict_resp = get_organisation(city_id)
                try:
                    members = dict_resp.get('hydra:member', [])
                    for member in members:
                        location_id = member.get('id')
                        if location_id:
                            locations_to_patch_list.append((location_id, payload))
                except Exception as e:
                    logging.error(f"Error occurred while parsing organisation data: {e}")

        return locations_to_patch_list

    def patch_locations(locations_to_patch):
        base_url = "https://organisation-dev.dom-non-prod.with.digital/api/locations/"
        
        for location_id, payload in locations_to_patch:
            url_to_patch = f"{base_url}{location_id}"

            try:
                resp = requests.patch(url_to_patch, headers=return_organisation_headers(), data=payload)
                resp.raise_for_status()
                logging.info(f'Remapped Country ID updated for location- {location_id}.')
            except requests.exceptions.RequestException as e:
                logging.error(f"Error occurred while patching location data: {e}")

    try:
        locations_to_patch_list = get_all_locations_to_patch()
        patch_locations(locations_to_patch_list)
    except Exception as e:
        logging.error(f"Error occurred: {e}")

# The below function calling is for getting the organisation data by city id and then patching each location
# get_organisation_and_patch_locations_for_remapped_country_id()

################################################################## CASE - 4 ################################################################################
# Where Mapped State ID
    # is not null
        # PATCH Classification /cities/[city_id]
            # {"subMarket":"/api/sub-markets/[Mapped State ID]"}

        # GET Organisation /locations?city=[city_id]
            # For each result
            # PATCH /locations/[x]
            # {"subMarket":[Mapped State ID]}


#################################################################### PART-1 ###############################################################################

# The below function is to patch classification /cities/[city_id] when Mapped State ID is not null
def patch_mapped_state_id():

    # Configure logging
    logging.basicConfig(filename='patch_mapped_state_id_case_4.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def mapped_state_id_payload(city_id, mapped_state_id):
        """
        Constructs the URL and payload for patching city data.
        """
        url = f"https://classification-dev.dom-non-prod.with.digital/api/cities/{city_id}"
        payload = json.dumps({"subMarket": f"/api/sub-markets/{mapped_state_id}"})
        return url, payload

    def patch_mapped_state_id(city_id, mapped_state_id):
        """
        Sends a PATCH request to update city data.
        """
        url, payload = mapped_state_id_payload(city_id, mapped_state_id)
        headers = return_classification_headers()
        try:
            response = requests.patch(url, headers=headers, data=payload)
            response.raise_for_status()
            logging.info(f'Submarket updated for city_id={city_id}.')
        except requests.exceptions.RequestException as e:
            logging.error(f'Error updating submarket for city_id={city_id}: {e}')

    def process_row(row):
        """
        Processes a single row from the Excel data.
        """
        mapped_state_id = row['Mapped State ID']
        city_id = row['city_id']

        if (
            isinstance(city_id, int)
            and not pd.isnull(mapped_state_id)
            and mapped_state_id != "CORRECT"
            and mapped_state_id != "CHECKED"
            and mapped_state_id != "Not Found"
        ):
        
            patch_mapped_state_id(city_id, mapped_state_id)

    def process_excel_for_mapped_state_id():
        """
        Processes the entire Excel file.
        """
        excel_data = read_excel_data()
        # filtered_data = excel_data[excel_data['city_id'].isin([262,474])]
        for _, row in excel_data.iterrows():
            process_row(row)

    try:
        process_excel_for_mapped_state_id()
    except Exception as e:
        logging.error(f'An error occurred during processing: {e}')

# The below Function is to patch the subMarket in classification.cities 
# patch_mapped_state_id()

#################################################################### PART-2 ###############################################################################

def get_organisation_and_patch_locations_for_mapped_state_id():
    """
    This function is to get organistaion /locations?city=[city_id] and patch for each result /locations/[x]
    """
    # Configure logging
    logging.basicConfig(filename='get_organisation_and_patch_locations_for_mapped_state_id_case_4_1.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def get_organisation(city_id):
        """
        Constructs the URL and payload for patching city data.
        """
        url = f"https://organisation-dev.dom-non-prod.with.digital/api/locations?city={city_id}"
        headers = return_organisation_headers()
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error occurred while fetching organisation data for city ID {city_id}: {e}")
            return {}

    def process_row(row):
        """
        Processes a single row from the Excel data.
        """
        mapped_state_id = row.get('Mapped State ID')
        city_id = row.get('city_id')
        
        if (
            isinstance(city_id, int)
            and not pd.isnull(mapped_state_id)
            and mapped_state_id not in {"CORRECT", "CHECKED", "Not Found"}
        ):
            return city_id, mapped_state_id

    def process_excel_for_submarkets():
        """
        Processes the entire Excel file.
        """
        excel_data = read_excel_data()
        all_ids_to_patch = [process_row(row) for _, row in excel_data.iterrows() if process_row(row)]
        return all_ids_to_patch

    def get_locations_to_patch():
        """
        This function is to get all the locations that are in all the cities.
        """
        cities_and_submarket_list = process_excel_for_submarkets()
        print('cities_and_submarket_list length',len(cities_and_submarket_list))
        locations_to_patch = []

        for city_id, submarket_id in cities_and_submarket_list:
            payload = json.dumps({"subMarket": submarket_id})
            dict_resp = get_organisation(city_id)

            try:
                members = dict_resp.get('hydra:member', [])
                locations_to_patch.extend((member.get('id'), payload) for member in members if member.get('id'))
            except Exception as e:
                logging.error(f"Error occurred while parsing organisation data for city ID {city_id}: {e}")

        return locations_to_patch

    def patch_locations(locations_to_patch):
        """
        This function is to patch all the locations.
        """
        base_url = "https://organisation-dev.dom-non-prod.with.digital/api/locations/"
        headers = return_organisation_headers()
        
        for location_id, payload in locations_to_patch:
            url_to_patch = f"{base_url}{location_id}"
            try:
                resp = requests.patch(url_to_patch, headers=headers, data=payload)
                resp.raise_for_status()
                logging.info(f'Mapped State ID updated for locations:{location_id}.')
            except requests.exceptions.RequestException as e:
                logging.error(f"Error occurred while patching location data: {e}")

    try:
        locations_to_patch = get_locations_to_patch()
        print('length of locations_to_patch to patch', len(locations_to_patch))
        patch_locations(locations_to_patch)
    except Exception as e:
        logging.error(f"Error occurred: {e}")

start_time = time.time()
#  The below function calling is for getting the organisation data by city id and then patching each location
get_organisation_and_patch_locations_for_mapped_state_id()
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed time - [get_organisation_and_patch_locations_for_mapped_state_id FUNCTION] : {elapsed_time:.2f} seconds")



################################################################## CASE - 5 ################################################################################
# Where Delete
    #  =Yes
        # GET Organisation /locations?city=[city_id]
            # For each result
            # PATCH Organisation /locations/[x]
            # {"city":[Remapped City ID]}

        # PATCH Classification /cities/[city_id]
            # {"deletedAt":"[time of execution in "yyyy-MM-dd hh:mm:ss"]"}

#################################################################### PART-1 ###############################################################################
# The below function is to get organisation /locations?city=[city_id] and for each result patch Organisation /locations/[x] --> {"city":[Remapped City ID]}
# In the below we have patched only those records where remapped_city_id is integer and delete == "Yes"
def get_organisation_and_patch_locations_for_remapped_city_id():

    # Configure logging
    logging.basicConfig(filename='get_organisation_and_patch_locations_for_remapped_city_id_case_5.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


    def get_organisation(city_id):
        """
        Constructs the URL and payload.
        """
        url = f"https://organisation-dev.dom-non-prod.with.digital/api/locations?city={city_id}"
        headers = return_organisation_headers()
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error occurred while fetching organisation data: {e}")
            return {}

    def process_row(row):
        """
        Processes a single row from the Excel data.
        """
        remapped_city_id = row.get('Remapped City ID')
        city_id = row.get('city_id')
        delete = row.get('Delete')
        result_dict = {}
        
        if isinstance(remapped_city_id, int) and delete == "Yes":
            result_dict[city_id] = remapped_city_id

        return result_dict

    def process_excel_for_remapped_city_id():
        """
        Processes the entire Excel file.
        """
        all_ids_to_patch = []
        excel_data = read_excel_data()
        
        try:
            top_10_rows = excel_data[:20]
            for _, row in top_10_rows.iterrows():
                row_dict = process_row(row)
                if row_dict:  # Check if the dictionary is not empty
                    all_ids_to_patch.append(row_dict)
        except Exception as e:
            logging.error(f"Error occurred while processing Excel data: {e}")

        return all_ids_to_patch

    def get_locations_to_patch():
        cities_remapped_list = process_excel_for_remapped_city_id()
        locations_to_patch = []

        for cities_remappped_dict in cities_remapped_list:
            for city_id, remapped_city_id in cities_remappped_dict.items():
                payload = json.dumps({"city": remapped_city_id})
                dict_resp = get_organisation(city_id)

                try:
                    members = dict_resp.get('hydra:member', [])
                    for member in members:
                        location_id = member.get('id')
                        if location_id:
                            locations_to_patch.append((location_id, payload))
                except Exception as e:
                    logging.error(f"Error occurred while parsing organisation data: {e}")

        return locations_to_patch

    def patch_locations(locations_to_patch):
        base_url = "https://organisation-dev.dom-non-prod.with.digital/api/locations/"
        
        for location_id, payload in locations_to_patch:
            url_to_patch = f"{base_url}{location_id}"

            try:
                resp = requests.patch(url_to_patch, headers=return_organisation_headers(), data=payload)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"Error occurred while patching location data: {e}")

    try:
        locations_to_patch = get_locations_to_patch()
        patch_locations(locations_to_patch)
    except Exception as e:
        logging.error(f"Error occurred: {e}")

# The below function calling is for getting the organisation data by city id and then patching each location for delete action
# get_organisation_and_patch_locations_for_remapped_city_id()

#################################################################### PART-2 ###############################################################################
# The below function is to patch Classification /cities/[city_id] {"deletedAt":"[time of execution in "yyyy-MM-dd hh:mm:ss"]"}

def patch_deletedAt_time():

    def get_the_time_of_execution():
        from datetime import datetime
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        return formatted_datetime

    def deletedAt_payload(city_id):
        """
        Constructs the URL and payload for patching city data.
        """
        url = f"https://classification-dev.dom-non-prod.with.digital/api/cities/{city_id}"
        payload = json.dumps({"deletedAt": f"{get_the_time_of_execution()}"})
        return url, payload
    
    def patch_deletedAt_column(city_id):
        """
        Sends a PATCH request to update city data.
        """
        url, payload = deletedAt_payload(city_id)
        headers = return_classification_headers()
        requests.patch(url, headers=headers, data=payload)

    def process_row(row):
        """
        Processes a single row from the Excel data.
        """
        city_id = row['city_id']
        
        if isinstance(city_id,int):
            patch_deletedAt_column(city_id)
            print(f'Submarket updated for city_id {city_id}.')
            

    def process_excel_for_submarkets():
        """
        Processes the entire Excel file.
        """
        excel_data = read_excel_data()
        top_10_rows = excel_data.head(30)

        for _, row in top_10_rows.iterrows():
            process_row(row)

    process_excel_for_submarkets()

# The below function call is to patch the deletedAt column
# patch_deletedAt_time()
