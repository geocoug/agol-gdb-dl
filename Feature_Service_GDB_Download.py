## AUTHORS:
##        Caleb Grant, Integral Consulting, Inc. (CG)
##
## PURPOSE:
##        Download Feature Services from ArcGIS Online in Geodatabase format.
##
## NOTES:
##
#############################################################################################################

import sys

if sys.version_info < (3,):
    print('Must use Python version 3')
    sys.exit()

import os
import json
import urllib.request
import time
import datetime
import json
import contextlib
import pandas as pd
from shutil import copyfile
from getpass import getpass

print('This program is dependent on "Active_AGOL_ServiceIds_For_Backup.xlsx".')
print('If the workbook is not in the same directory as the script, it will not work.')
print('------------------------------------------------------------------------------\n')

print('Enter your ArcGIS Online username.')
username = input("Username: ")
print('')
print('Enter your ArcGIS Online password for <{}>'.format(username))
password = getpass()
print('\n')
##username = ''
##password = ''

def sendRequest(request):
    response = urllib.request.urlopen(request)
    readResponse = response.read()
    jsonResponse = json.loads(readResponse)
    return jsonResponse

## Generate token endpoint and parameters.
url = "https://arcgis.com/sharing/rest/generateToken"
data = {'username': username,
        'password': password,
        'referer': "https://www.arcgis.com",
        'f': 'json'}

try:
    ## Request token
    request = urllib.request.Request(url, urllib.parse.urlencode(data).encode("utf-8"))
    jsonResponse = sendRequest(request)
    token = jsonResponse['token']
except Exception as error:
    print(error)
    print('ERROR: Token generation failed.')
    print('Check your username and password.\n')
    print('Exiting program.')
    sys.exit()

## REST Services endpoint
serviceURL = "" # ex: https://services.arcgis.com/ORG_ID/arcgis/rest/services
## Data encoding parameters
data = {'f' : 'json',
    'token': token}

## Get list of available services
request = urllib.request.Request(serviceURL, urllib.parse.urlencode(data).encode("utf-8"))
jsonResponse = sendRequest(request)
services = jsonResponse['services']

## Create backup log with timestamp
_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")


## Read list of active services into Pandas DataFrame
file = 'Active_AGOL_ServiceIds_For_Backup.xlsx'
df = pd.read_excel(file, 'Active')
# Get column names
headers = list(df.columns)


## Create archive folder
today = datetime.datetime.now()
today_format = today.strftime("%Y-%m-%d")
archive_dir = os.path.join(os.getcwd(), today_format)
if os.path.exists(archive_dir) == False:
    os.mkdir(archive_dir)
else:
    duplicate_int = 1
    while True:
        archive_dir = os.path.join(os.getcwd(), "{}__{}".format(today_format, duplicate_int))
        if os.path.exists(archive_dir) == False:
            os.mkdir(archive_dir)
            break
        else:
            duplicate_int += 1

log_path = os.path.join(archive_dir, 'log_{}.txt'.format(_datetime))
with open(log_path, "a+") as backup_log:
    backup_log.write("ServiceName, StartTime, EndTime, SecondsElapsed, ReplicaURL\n")
backup_log.close()

## Create temp dictionary to parse Excel dataframe
##   New dictionary gets made per row in Excel table
service_dict = {}
## Iterate through rows DataFrame
for index, row in df.iterrows():
    ## Iterate through columns in row - write values to DataFrame
    for header in headers:
        service_dict.update({header: row[header]})

    ## Set service variables
    serviceID = service_dict['ServiceId']
    serviceName = service_dict['ServiceName']
    layers = service_dict['Layers']
    tables = service_dict['Tables']
    print('Creating replica for <{}>.'.format(serviceName))

    ## Create variable to track execution time
    start_time = datetime.datetime.now()
    show_start_time = start_time.strftime("%m-%d-%Y %H:%M:%S %p")
    print("Start Time: ", show_start_time)

    ## Iterate through all services in REST Services Directory
    for service in services:
        service_name = service['name']
        service_url = service['url']
        request = urllib.request.Request(service_url, urllib.parse.urlencode(data).encode("utf-8"))
        response = sendRequest(request)
        ## Check if Service ID response matches Service ID in current DataFrame.
        if response['serviceItemId'] != serviceID:
            continue
        else:
            ## Create backup log
            with open(log_path, "a+") as backup_log:
                ## Create Service replica name
                replicaName = "{}_replica".format(service_name)
                ## Define layers to include in replica (all of them)
                replicaLayers = [x['id'] for x in response['layers']]
                ## Compile URL endpoint for replica creation
                replicaURL = serviceURL + "/{}/FeatureServer/createReplica".format(service_name)
                ## Replica request parameters
                data = {'f' : 'json',
                    'replicaName' : replicaName,
                    'layers' : replicaLayers,
                    'returnAttachments' : 'true',
                    'returnAttachmentsDatabyURL' : 'false',
                    'syncModel' : 'none',
                    'dataFormat' : 'filegdb',
                    'async' : 'true',
                    'token': token}
                try:
                    # Create replica request with parameters and encoding
                    request = urllib.request.Request(replicaURL, urllib.parse.urlencode(data).encode("utf-8"))
                    # Request replica
                    jsonResponse = sendRequest(request)
                except:
                    print('Failed request to <{}>'.format(serviceName))
                    print('Skipping feature service.\n')
                    continue

                # Get job status URL from replica response
                responseUrl = jsonResponse['statusUrl']
                url = "{}?f=json&token={}".format(responseUrl, token)
                request = urllib.request.Request(url)
                # Check job status
                jsonResponse = sendRequest(request)
                while not jsonResponse.get("status") == "Completed":
                    time.sleep(5)
                    request = urllib.request.Request(url)
                    jsonResponse = sendRequest(request)

                ## URL to request for replica result/output
                jres = jsonResponse['resultUrl']
                ## Add token to URL path
                url = "{0}?token={1}".format(jres, token)
                ## Request for output
                f = urllib.request.urlopen(url)

                ## Create output path and check if already exists
                if not os.path.exists(archive_dir + "\\" + "{}__{}.zip".format(serviceName, _datetime)):
                    pass
                else:
                    os.remove(archive_dir + "\\" + "{}__{}.zip".format(serviceName, _datetime))

                with open(archive_dir + "\\" + "{}__{}.zip".format(serviceName, _datetime), "wb") as local_file:
                    local_file.write(f.read())

                ## Calculate total execution run time
                end_time = datetime.datetime.now()
                show_end_time = end_time.strftime("%m-%d-%Y %H:%M:%S %p")
                print("End Time: ", show_end_time)
                calc_runtime = end_time - start_time
                print("Time elapsed (seconds): ", calc_runtime.seconds)
                print('')

                backup_log.write("{}, {}, {}, {}, {}\n".format(serviceName, show_start_time, show_end_time, calc_runtime.seconds, url))
            backup_log.close()

print('Output Directory: ', archive_dir)
print('Complete')
