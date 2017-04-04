#!/usr/bin/env python

from __future__ import print_function
import httplib2
import os
import csv
import base64
import io
import re
import shutil
import hashlib
from apiclient import http

from apiclient import discovery
from apiclient.http import MediaIoBaseDownload

from oauth2client import client, tools
from oauth2client.file import Storage
from oauth2client import tools
from apiclient import errors

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/<gmail/drive>-python-extraction.json
SCOPES = 'https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'API Python'
# Change this variable to the USB Drive to download the information
STORAGE_PATH =  'G:\\'
# Builds the MD5 has dictionary to store after all the files are downloaded
mdf5_dic={}



def get_credentials(jsonfile):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.
    The jsonfile distinguishes between the gmail api and google drive jsonfiles.
    This will create two files in the credentials folder stored at location specified
    by variable STORAGE_PATH.

    Returns:
        Credentials, the obtained credential.
    """
    credential_dir = os.path.join(STORAGE_PATH, '.credentials')

    # Creates the .credentials directory if it doesnt exist
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,jsonfile)

    store = Storage(credential_path)
    credentials = store.get()

    # If there are no credentials in STORAGE_PATH or credentials located there are corrupted
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def get_Emails():
    """
        Extract emails and attachments from a gmail account and stores them
        in STORAGE_PATH/messages folder. Since emails can have multiple versions
        the gmail message id was used for the subfolder structure to contain message
        content together. Messages are equivalent to emails.
    """
    # Builds service to make gmail requests
    credentials = get_credentials('gmail-python-extraction.json')
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('gmail', 'v1', http=http)

    # Creates a request to Google API to retrieve list of messages. includeSpamTrash parameter set
    # to true pulls in spam and trash messages with all the good messages and versions numbers.
    results = service.users().messages().list(userId='me',includeSpamTrash=True).execute()
    messages = results.get('messages', [])
    # message_num will be used later to compare the number of messages listed in the call 
    # with the number of messages downloaded into STORAGE_PATH folder
    messages_num=len(messages)
    if not messages:
        # returns string below if no message are found
        print('No messages found.')
    else:
        # Create the message folder in the STORAGE_PATH if it does not exist
        if not os.path.exists(STORAGE_PATH+'messages/'):
            os.makedirs(STORAGE_PATH+'messages/')

        # This for loop will iterate through each message for extraction
        for messageId in messages:

            # Create a specific message folder in the 'STORAGE_PATH/messages' folder if it does not exist
            # named after the messageId
            if not os.path.exists(STORAGE_PATH+'messages/'+messageId['id']+'/'):
                os.makedirs(STORAGE_PATH+'messages/'+messageId['id']+'/')

            # Opens another request to get contents of the message using a get request
            message = service.users().messages().get(userId='me',id=messageId['id']).execute()            

            # Copies over the contents of the message returned into a csv file
            with open(STORAGE_PATH+'messages/'+messageId['id']+'/'+messageId['id']+'_csv.csv', 'w') as csv_file:
                writer =csv.writer(csv_file)
                for key, value in message.items():
                    writer.writerow([key, value])

            # Opens another request to get contents of the message using a get request in particular for the raw format
            message = service.users().messages().get(userId='me', id=messageId['id'],format='raw').execute()

            # Stores the raw file into the specific message directory
            file_data = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
            with open(STORAGE_PATH+'messages/'+messageId['id']+'/'+messageId['id']+'_raw.txt','wb') as f:
                f.write(file_data)
            
            
            # modified code from http://stackoverflow.com/questions/25832631/download-attachments-from-gmail-using-gmail-api              
            # To get the attachment data we call the get request again
            message = service.users().messages().get(userId='me', id=messageId['id']).execute()

            # Attachments can either be embedded into the message or attached. They exist in either the 
            # (1) ['payload']['parts'][][body][data]
            # (2) ['payload']['parts'][]['body']['attachmentId']
            if 'parts' in message['payload']:
                for part in message['payload']['parts']:
                    if part['filename']:
                        # Gets the data from scenario 1
                        if 'data' in part['body']:
                            data=part['body']['data']
                        # Gets the data from scenario 2
                        else:
                            att_id=part['body']['attachmentId']
                            att=service.users().messages().attachments().get(userId='me', messageId=messageId['id'],id=att_id).execute()
                            data=att['data']
                        
                        # Opens a base64 decoder to write the data attached to the message to the unique messageId folder in messages
                        file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                        with open(STORAGE_PATH+'messages/'+messageId['id']+'/'+part['filename'],'wb') as f:
                            f.write(file_data)

    # Compares the number of extracted messages form the list call and compares the quantity to the number of 
    # messages stored in STORAGE_PATH + messages
    # To improve check - keep the list of items and compare the messageID values
    if(messages_num == len([name for name in os.listdir(STORAGE_PATH+'messages/')])):
        print("Number of files downloaded and API list count match")
    else:
        print("Number of messages extracted and observed over list api call do not match.")
        print("Number of files from google api: "+str(messages_num))
        print("Number of files extracted: "+ str(len([name for name in os.listdir(STORAGE_PATH+'messages/')])))

def get_Drive():
    """
        Extracts google drive files and metadata. If the Downloading gets stuck at 0 percent it is a bug I am working on
        it is within the try catch statement so hitting control-C will continue to the next google drive file. 
        Tt is usually a google-app presentation type. The file does download. It will print in after hitting ctrl-C and display
        the name. You will see it in the STORAGE_PATH/drivefiles folder.
    """
    # Builds service to make google drive requests
    credentials = get_credentials('drive-python-extraction.json')
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)

    # Creates a request to Google Drive API to retrieve list of files in the drive.
    # field=files(*) makes sure the google drive call retrieves all fields available for the file.
    # Ths makes sure we extract all the metadata fields avaiable from the drive api
    results = service.files().list(fields="files(*)").execute()

    # Separates the results according to files and stores the list in items
    # items is a structures with list details on all the files 
    items = results.get('files', [])

    if not items:
        # returns string below if no drive files are found
        print('No files found.')
    else:
        # Create the drivesfiles folder in the STORAGE_PATH if it does not exist
        if not os.path.exists(STORAGE_PATH+'drivefiles/'):
            os.makedirs(STORAGE_PATH+'drivefiles/')
        # Create the metadata folder in the STORAGE_PATH/drivesfiles folder if it does not exist
        if not os.path.exists(STORAGE_PATH+'drivefiles/metadata'):
            os.makedirs(STORAGE_PATH+'drivefiles/metadata/')

        #  items is further broken down to each individual file on the drive
        for item in items:     
            # Opens a csv writer type to store all the metadata fields into a CSV in a key value pair format
            with open(STORAGE_PATH+'drivefiles/metadata/'+item['name']+'_metatdata.csv', 'w') as csv_file:
                writer =csv.writer(csv_file)
                for key, value in item.items():
                    writer.writerow([key, value])

            # Depending if the file is stored as a google-app it needs to use an export call to get data to 
            # write to certain types documented in the google api site.
            # https://developers.google.com/drive/v3/web/manage-downloads
            # I picked pdf as all the google-app files support it
            if "google-apps" in item['mimeType']:
                try:
                    if any(x in item['mimeType'] for x in ["presentation","document","spreadsheet"]):
                        # Use regular expression to replace the suffice with pdf
                        regx = re.compile('\\..+')
                        item['name'] = regx.sub('.pdf',item['name'])
                        # If there is no period in the name assume there is no extension and append .pdf
                        if '.' not in item['name']:
                            item['name'] = item['name']+'.pdf'
                        # Makees the export request to get the data and writes it the file into the 
                        # STORAGE/driverfiles folder using the MediaIO downloader
                        request = service.files().export(fileId=item['id'],mimeType='application/pdf')
                        fh = io.FileIO(STORAGE_PATH+'drivefiles/'+item['name'], 'wb')
                        downloader = MediaIoBaseDownload(fh, request)
                        # Continues to download the file until 100% is reached then done is updated to true
                        done = False
                        while done is False:
                            status, done = downloader.next_chunk()
                            print ("Download %d%%." % int(status.progress() * 100))
                except:
                    # The Downloader might get stuck on the google-app presentation type. hit ctrl-C to enter
                    # this except section. It will escaoe 1 download. Verify the printed exception downloaded after
                     print(item['name']+ '   -   ' + item['mimeType']+'   -   ' + item['id'])
                     continue
            else:
                # This downloaded the non-google-app files aka regular files using a get_media request.
                request = service.files().get_media(fileId=item['id'])
                fh = io.FileIO(STORAGE_PATH+'drivefiles/'+item['name'], 'wb')
                downloader = MediaIoBaseDownload(fh, request)
                done=False
                # Downloads the file. Once completed at 100 percent, done changes to true. Exiting the while loop
                while done is False:
                    status, done = downloader.next_chunk()
                    print("Download %d%%." % int(status.progress() * 100))


def get_recursive_dir(src_path,des_path):
    '''
        Used to explore the local google drive folder. Everytime a folder is encounter it recursively
        calls itself for that folder. Once all files and folders are visited the funtion will terminate
        and return to the parent directory to continue.
    '''
    # creates directory in storage container
    if not os.path.exists(des_path):
        os.makedirs(des_path)
    # updates the src_files to all files and folders in the src_path
    src_files = os.listdir(src_path)
    # iterates through each item in src_fiels as name
    for name in src_files:
        # Skips this file because it can have be write protected and kill program
        if 'desktop.ini' in name:
            continue
        # updates the full path of the iterating file or folder
        full_src_name = os.path.join(src_path, name)
        full_des_name = os.path.join(des_path, name)
        # If name is a file the function copies the content over
        if (os.path.isfile(full_src_name)):
            shutil.copy(full_src_name, full_des_name)
        # If name is a directory the function recursively calls get_recuresive_dir function
        elif (os.path.isdir(full_src_name)):
            get_recursive_dir(full_src_name,full_des_name)



def get_local_Drive():
    # Gets the absoluate path for the user's home folder
    homeDir=os.path.expanduser('~')
    # checks if local Google drive folder exists to extract the data then calls recursive function to get files
    if os.path.exists(homeDir+'/Google Drive'):
        get_recursive_dir(homeDir+'/Google Drive/',STORAGE_PATH+'localDriveFiles/')

    # folder path for google drive db files
    local_cloud_dir = (homeDir+'/AppData/Local/Google/Drive/user_default/')    

    # Checks if the google drive directory for google db fiels exist then copies the files to a subfolder to the
    # STORAGE_PATH\localDriveFiles\local_db_files
    if os.path.exists(local_cloud_dir):
        des_db_path = STORAGE_PATH+'localDriveFiles/local_db_files/'
        # creates the localDriverFiles folder if it does not exist
        if not os.path.exists(des_db_path):
            os.makedirs(des_db_path)
        if os.path.exists(local_cloud_dir+'snapshot.db'):
            shutil.copy(local_cloud_dir+'snapshot.db',des_db_path+'snapshot.db')
        if os.path.exists(local_cloud_dir+'sync_config.db'):
            shutil.copy(local_cloud_dir+'sync_config.db',des_db_path+'sync_config.db')
        if os.path.exists(local_cloud_dir+'uploader.db'):
            shutil.copy(local_cloud_dir+'uploader.db',des_db_path+'uploader.db')

def get_recursive_md5(path):    
    # gets a list of files and folders in path
    files = os.listdir(path)
    # iterates through each file or folder as name
    for name in files:
        # combines for full absolute path of file or folder
        full_src_name = os.path.join(path, name)
        # Skips irrelevant files or folders
        if STORAGE_PATH+'.' in full_src_name or STORAGE_PATH+'System Volume Information' in full_src_name:
            continue
        # If the name element is a file then perform hash functio on the file
        if (os.path.isfile(full_src_name)):
            hash_md5 = hashlib.md5()
            # read the file in as bytes and perform hash func
            with open(full_src_name, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            # get MD5 value and tore it into the MD5 dictionary
            md5_returned = hash_md5.hexdigest()
            mdf5_dic[full_src_name] = md5_returned
        # if the name element is a folder then call the recursive MD5 function call to explore more files and folders
        elif (os.path.isdir(full_src_name)):
            get_recursive_md5(full_src_name)

def construct_md5_table():
    # calls to MD5 all files in the storage container to store as a csv
    get_recursive_md5(STORAGE_PATH)
    # Creates the md5 csv files and
    if os.path.exists(STORAGE_PATH+'/md5_table.csv'):
        os.remove(STORAGE_PATH+'/md5_table.csv')
    # Stores all the MD5 values in the md5 dictionary into a csv files
    with open(STORAGE_PATH+'/md5_table.csv', 'w') as csv_file:
        writer =csv.writer(csv_file)
        for key, value in mdf5_dic.items():
            writer.writerow([key, value])

# If you prefer to run specific functions from main comment them out
def main():
    get_Emails() # Get email/messages from the cloud using gmail api
    get_Drive() # Get google drive files from the cloud using google drive api
    get_local_Drive() # Get google drive files if user has google local drive app
    construct_md5_table() # Computes all teh MD5 hash values and stores them ito a csv file


if __name__ == '__main__':
    main()