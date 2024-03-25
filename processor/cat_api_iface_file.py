#!/usr/bin/python3

import requests
import base64
import json
from datetime import datetime,timezone


key_id = "fleet_mgmt_cust_2969765373_cc_client"
key_secret = "OuotFtu7ubkwt3zIKVhU77AD6THBXivLPRX97iE5cQzW7huIggDti4m22P8xvh62"

token_endpoint = "https://fedlogin.cat.com/as/token.oauth2"
fleet_overview_endpoint = "https://services.cat.com/telematics/iso15143/fleet/1"


class cat_api_iface:

    def __init__(self, key_id=None, key_secret=None, token=None):
        self.key_id = key_id
        self.key_secret = key_secret
        self.token = token

        self.equipment = {}

    def retrieve_token(self, key_id, key_secret):

        base64_encoded_key = base64.b64encode(f"{key_id}:{key_secret}".encode("utf-8")).decode("utf-8")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + base64_encoded_key,
        }

        data = {
            "grant_type": "client_credentials",
            "scope": "manage:all",
        }

        response = requests.post(token_endpoint, headers=headers, data=data)
        response_json = response.json()
        self.token = response_json["access_token"]
        return self.token


    def cat_api_get(self, endpoint):

        if self.token is None:
            self.retrieve_token(self.key_id, self.key_secret)

        headers = {
            "Authorization": "Bearer " + self.token,
            "Accept": "application/iso15143-snapshot+json",
        }

        response = requests.get(endpoint, headers=headers)
        return response.json()

    def parse_equipment_result(self, result):
        for equipment in result['Equipment']:
            serialNumber = equipment['EquipmentHeader']['SerialNumber']
            self.equipment[serialNumber] = equipment

    def get_equipment_attribute(self, serialNumber, attribute_name, inner_name):
        result1 = None
        try:
            result1 = self.equipment[serialNumber][attribute_name][inner_name]
        except KeyError:
            print("KeyError: ", serialNumber, attribute_name, inner_name)
        return result1
    
    def get_equipment_latest_upload_dt(self, serialNumber):
        dt = self.get_equipment_attribute(serialNumber, "CumulativeOperatingHours", "Datetime")
        if dt is None: return None
        dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").astimezone(timezone.utc)
        return dt

    def get_equipment_latest_age(self, serialNumber):
        dt = self.get_equipment_latest_upload_dt(serialNumber)
        if dt is None: return None
        return datetime.now(timezone.utc) - dt

    def get_equipment_latlong(self, serialNumber):
        lat = self.get_equipment_attribute(serialNumber, "Location", "Latitude")
        long = self.get_equipment_attribute(serialNumber, "Location", "Longitude")
        if lat is None or long is None: return None
        return (lat, long)
    
    def get_equipment_engine_running(self, serialNumber):
        return self.get_equipment_attribute(serialNumber, "EngineStatus", "Running")
    
    def get_equipment_hours(self, serialNumber):
        return self.get_equipment_attribute(serialNumber, "CumulativeOperatingHours", "Hour")

    def get_equipment_fuel_level_percent(self, serialNumber):
        return self.get_equipment_attribute(serialNumber, "FuelRemaining", "Percent")
    
    def get_equipment_fuel_consumed_litres(self, serialNumber):
        return self.get_equipment_attribute(serialNumber, "FuelUsed", "FuelConsumed")
    
    def get_equipment_odo_kms(self, serialNumber):
        return self.get_equipment_attribute(serialNumber, "Distance", "Odometer")

    def retrieve_fleet_data(self):
        fleet_overview = self.get_fleet_overview()
        self.parse_equipment_result(fleet_overview)
            
    def get_fleet_overview(self):
        endpoint = "https://services.cat.com/telematics/iso15143/fleet/1"
        return self.cat_api_get(endpoint)

    def get_equipment_overview(self, make, model, serial):
        endpoint_prefix = "https://services.cat.com/telematics/iso15143/fleet/equipment/makeModelSerial/"
        endpoint = endpoint_prefix + str(make) + "/" + str(model) + "/" + str(serial)
        result = self.cat_api_get(endpoint)
        self.parse_equipment_result(result)
        return result

    def print_all_equipment(self):
        for serial in self.equipment:
            print(serial)
            # print(self.equipment[serial])
            print("Latest Upload Age: ", self.get_equipment_latest_age(serial))
            print("LatLong: ", self.get_equipment_latlong(serial))
            print("Engine Running: ", self.get_equipment_engine_running(serial))
            print("Hours: ", self.get_equipment_hours(serial))
            print("Fuel Level: ", self.get_equipment_fuel_level_percent(serial))
            print("Fuel Consumed: ", self.get_equipment_fuel_consumed_litres(serial))
            print("Odometer: ", self.get_equipment_odo_kms(serial))
            print("")


if __name__ == "__main__":

    cat_api = cat_api_iface(
        key_id=key_id,
        key_secret=key_secret,
    )

    # cat_api.retrieve_fleet_data()
    # cat_api.print_all_equipment()

    # test = cat_api.get_equipment_overview("CAT", "789C", "2BW90008")
    # test = cat_api.get_equipment_overview("CAT", "MT4400AC", "SE401448")
    test = cat_api.get_equipment_overview("CAT", "D11T", "AMA00883")

    print(json.dumps(test["Equipment"][0]))
    print(type((test["Equipment"][0])))
    # cat_api.print_all_equipment()

    # fleet_overview = cat_api.get_fleet_overview()
    # json_output = json.dumps(fleet_overview, indent=4)

    # equipment_overview = cat_api.get_equipment_overview("CAT", "789C", "2BW90008")
    # # equipment_overview = cat_api.get_equipment_overview("CAT", "MT4400AC", "SE401448")
    # # equipment_overview = cat_api.get_equipment_overview("caterpillar", "MT4400AC", "2bw90008")
    # json_output = json.dumps(equipment_overview, indent=4)

    # print(json_output)

    # ## Write the json to a file
    # with open("output.json", "w") as file:
    #     file.write(json_output)




