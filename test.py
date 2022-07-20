#!py
import requests
import json
import logging, sys
import pprint
import re
import ipaddress
from json import JSONDecodeError
from json import loads, dumps
import cherwell_fields

#logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename='runner.log', filemode='w', format='%(asctime)s %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

# CHERWELL CONFIG - SERVER BUSOBID:
busobId = ""
sorting = {
    "fieldId": "",  # Friendly Name
    "sortDirection": 1,  # Ascending
}

def get_token(*exceptions, api_key, username, password, brightsolid_url):
    """get cherwell token"""
    tokenbody = {
        "Accept": "application/json",
        "grant_type": "password",
        "client_id": api_key,
        "username": username,
        "password": password,
    }
    url = f"{brightsolid_url}token?auth_mode=Internal&api_key={api_key}"
    try:
        token_request = requests.post(url, tokenbody)
        return token_request.json()["access_token"]
    except exceptions or Exception as e:
        raise e
        logging.exception(e)

def get_record(*exceptions, headers, brightsolid_url, field_id, field_value):
    """get businessObject"""
    searchResultsRequest = {
        "busObID": busobId,
        "filters": [
            {
                "fieldId": f"BO:,FI:{field_id}",  # host name
                "Operator": "eq",
                # 1: get a specific record by "host name"
                "Value": field_value,
            },
        ],
        "fields": [ cherwell_fields.Last_Audited_By, cherwell_fields.Imported_From, cherwell_fields.RecID, cherwell_fields.FriendlyName, 
        cherwell_fields.AssetType, cherwell_fields.AssetStatus, cherwell_fields.RackPowerAllocation, cherwell_fields.RackStatus, 
        cherwell_fields.Town_City, cherwell_fields.CI_Owner, cherwell_fields.Manufacturer, cherwell_fields.Model, cherwell_fields.Asset_Tag, 
        cherwell_fields.Serial_Number, cherwell_fields.Aditional_ID, cherwell_fields.Version, cherwell_fields.Kaseya_Agent_GUID, 
        cherwell_fields.Kaseya_Machine_Group, cherwell_fields.OS_Build, cherwell_fields.CPU_Cores, cherwell_fields.CPU_Sockets, 
        cherwell_fields.CPU_Speed, cherwell_fields.CPU_Model, cherwell_fields.Memory, cherwell_fields.OS_Storage_Size, cherwell_fields.Data_Storage_Size, 
        cherwell_fields.component_id, cherwell_fields.tags, cherwell_fields.subnet, cherwell_fields.Region, cherwell_fields.Av_Zone, cherwell_fields.vpc, 
        cherwell_fields.account, cherwell_fields.Application_Service, cherwell_fields.Virtualisation_Tech, cherwell_fields.Management_VC, 
        cherwell_fields.Host, cherwell_fields.Last_Host_Update, cherwell_fields.vCores_per_CPU, cherwell_fields.Rack_Status, cherwell_fields.Usable_Rack_Size, 
        cherwell_fields.Rack_Power_Allocation, cherwell_fields.Key_Number, cherwell_fields.Combination_Code, cherwell_fields.End_User, 
        cherwell_fields.Site_Name, cherwell_fields.Room, cherwell_fields.Environment, cherwell_fields.Deployed_By, cherwell_fields.Rack, 
        cherwell_fields.Host_Name, cherwell_fields.Internal_IP_Address, cherwell_fields.External_IP_Address, cherwell_fields.Cluster_member_of, 
        cherwell_fields.Cluster_Underlying, cherwell_fields.Backup_Job, cherwell_fields.Backup_Server, cherwell_fields.Domain_Name, 
        cherwell_fields.Product_Name, cherwell_fields.Product_Number, cherwell_fields.Description, cherwell_fields.minion_field, 
        cherwell_fields.minion_uuid, cherwell_fields.primary_mac_address, cherwell_fields.primary_ip, cherwell_fields.secondary_ip, 
        cherwell_fields.secondary_mac_address, cherwell_fields.saltmaster, cherwell_fields.saltversion, cherwell_fields.ip_gateway
        ],
        "includeAllFields": "false",
        "sorting": sorting,
        "pageNumber": 1,
        "pageSize": 10,
    }

    search_uri = brightsolid_url + "api/V1/getsearchresults"
    try:
        searchresponse_request = requests.post(
            url=search_uri, headers=headers, json=searchResultsRequest
        )
        searchresponse_request = searchresponse_request.json()["businessObjects"]
        return searchresponse_request
    except exceptions or Exception as e:
        raise e
        logging.exception(e)

def filter_record(search_response, fieldtype):
    """get busObRecId + busObPublicId to update existing record"""
    for x in search_response:
        return x[fieldtype]

def post_record(*exceptions, token, url, update_cherwell, public_id, record_id):
    """create/update the record in cherwell"""
    data = {
        "busObId": "",
        "busObPublicId": public_id,
        "busObRecId": record_id,
        "fields": update_cherwell,
        "persist": True,
    }
    try:
        requests.post(url=url, headers={ "Authorization" : f"Bearer {token}", "Content-Type": "application/json"}, data=json.dumps(data))
    except exceptions or Exception as e:
        raise e
        logging.exception(e)

def run(grains, interfaces):
    """get machine by unique_identifiers and update/create machine in cherwell"""
    password = ""
    api_key = ""
    username = ""
    brightsolid_url = ""
    exceptions = [ConnectionError, KeyError, JSONDecodeError, UnboundLocalError, requests.exceptions.HTTPError, TypeError, ConnectionError, UnicodeError]

    # dump grains pulled from salt:
    grains = loads(dumps(grains))
    grains = grains['grains']
    host = grains['host']
    logging.info(host)
    logging.info(grains)

    # dump interfaces pulled from salt
    interfaces = loads(dumps(interfaces))
    # set interfaces to values in return ('ret') key from returned minion data block
    interfaces = interfaces['interfaces']['ret']
    logging.info(interfaces)

    # create empty lists and populate them with appropriate data
    # use the primary key to determine if primary or secondary
    primary_ip_list = []
    secondary_ip_list = []
    secondary_mac_address_list = []
    for interface in adapter_info:
        if interface['primary'] == "true":
            primary_ip_list.append(interface['address'])
            primary_mac_address = interface['hwaddr']
            gateway = interface['gateway']
            # add adapter field to cherwell blueprint so we can add named adapter
        else:
            secondary_ip_list.append(interface['address'])
            secondary_mac_address_list.append(interface['hwaddr'])

    logging.info(primary_ip_list)
    logging.info(secondary_ip_list)
    if len(secondary_mac_address_list) > 0:
        secondary_ip_list = "None"
        secondary_mac_address_list = "None"

    token = get_token(*exceptions, api_key=api_key, username=username, password=password, brightsolid_url=brightsolid_url)

    # SEARCH FOR RECORD AND INSERT VALUES BELOW:
    host_name_value = host
    friendly_name_value = ""
    internal_ip_value = ""
    
    field_values = [host_name_value, friendly_name_value, internal_ip_value]
    field_ids= [cherwell_fields.Host_Name, cherwell_fields.FriendlyName, cherwell_fields.Internal_IP_Address]

    # search for a record by values in field_values(1. host 2. friendly name 3. internal ip address)) 
    for value, id_values in zip(field_values, field_ids):
        if value == "":
            pass
        else:
            search_response = get_record(*exceptions, headers={ "Authorization" : f"Bearer {token}"}, brightsolid_url=brightsolid_url, field_id=id_values, field_value=value)
            if search_response == []:
                continue
            else:
                logging.info("found record using {value}")
                break

    cherwell_field_ids = [cherwell_fields.saltversion, cherwell_fields.saltmaster, cherwell_fields.minion_uuid, 
    cherwell_fields.minion_field, cherwell_fields.CPU_Model, cherwell_fields.Domain_Name, cherwell_fields.Host_Name, cherwell_fields.Product_Name, 
    cherwell_fields.Serial_Number, cherwell_fields.Aditional_ID, cherwell_fields.Memory, cherwell_fields.vCores_per_CPU]
    salt_fields = ['saltversion', 'master', 'uuid', 'id', 'cpu_model', 'domain', 'fqdn', 'productname', 'serialnumber', 'server_id', 'mem_total', 'num_cpus']
    update_cherwell = []
    # set __grains__ equal to grains:
    

    nested_fields = [cherwell_fields.ip_gateway, cherwell_fields.primary_ip, cherwell_fields.primary_mac_address, cherwell_fields.secondary_ip, cherwell_fields.secondary_mac_address]
    
    #nested_fields = [cherwell_fields.primary_ip, cherwell_fields.secondary_ip]
    nested_values = [gateway, primary_ip_list, primary_mac_address, secondary_ip_list, secondary_mac_address_list]
    logging.info(nested_values)

    # CREATE LIST OF PULLED RECORDS FIELD_ID'S + VALUES TO COMPARE AND AVOID POSTING DUPLICATES:
    if len(search_response) != 0:
        cherwell_field_values = search_response[0]["fields"]
        cherwell_fieldid_list = []
        cherwell_value_list = []
        for dictionary in cherwell_field_values:
            for key, value in dictionary.items():
                if key == "fieldId":
                    cherwell_fieldid_list.append(value)
                if key == "value":
                    cherwell_value_list.append(value)
        cherwell_comparison_list = dict(zip(cherwell_fieldid_list, cherwell_value_list))

        # GET SALT VALUES FOR EXISTING CHERWELL FIELDS:
        for x, y in zip(cherwell_field_ids, salt_fields):
            for key, value in __grains__.items():
                if key == y:
                    if str(value) not in list(cherwell_comparison_list.values()):
                        update_cherwell.append({"dirty": True, "fieldId": x, "value": value})
        for field, value in zip(nested_fields, nested_values):
            if value not in list(cherwell_comparison_list.values()):
                update_cherwell.append({"dirty": True, "fieldId": field, "value": value})
    else:
        for x, y in zip(cherwell_field_ids, salt_fields):
            for key, value in __grains__.items():
                if key == y:
                    update_cherwell.append({"dirty": True, "fieldId": x, "value": value})
        for field, value in zip(nested_fields, nested_values):
            update_cherwell.append({"dirty": True, "fieldId": field, "value": value})

     # GET THE PUBLICID AND RECORDID OF THE EXISTING RECORD
    public_id = filter_record(search_response=search_response, fieldtype='busObPublicId')
    record_id = filter_record(search_response=search_response, fieldtype='busObRecId')
    url = brightsolid_url + "api/V1/savebusinessobject" 

    if search_response == []:
        """CREATE NEW RECORD"""     
        update_cherwell.append({"dirty": True, "fieldId": "", "value": host})
        update_cherwell.append({"dirty": True, "fieldId": "", "value": host})
        post_record(*exceptions, token=token, url=url, update_cherwell=update_cherwell, public_id="", record_id="")   
    else:
        """UPDATE EXISTING RECORD"""
        post_record(*exceptions, token=token, url=url, update_cherwell=update_cherwell, public_id=public_id, record_id=record_id)
