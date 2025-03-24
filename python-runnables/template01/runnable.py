import dataiku
from dataiku.runnables import Runnable
from dataiku.runnables import utils
import json
import urllib.request
import os
import random
import string
import logging
import shutil
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('project_template_runnable')

# Define Google Drive file URLs as constants for easy future updates
POLICY_FILES = [
    {
        "url": "https://drive.google.com/file/d/1ebSYwnqfh9jHD9z4QfahX2Y-RETTl7Qm/view",
        "filename": "Health and Safety.pdf"
    },
    {
        "url": "https://drive.google.com/file/d/17Kv9H9GxhghBf8IDCkZ66GZ26MiG2RcE/view",
        "filename": "Travel Policy.pdf"
    },
    {
        "url": "https://drive.google.com/file/d/1O_yK34h1129mWR3P4k2BaM8J-O6wBCfM/view",
        "filename": "IT Policy.pdf"
    }
]

def create_random_temp_dir():
    """Create a random subdirectory in /tmp"""
    random_dir_name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    temp_dir = os.path.join("/tmp", random_dir_name)
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"Created temporary directory: {temp_dir}")
    return temp_dir

def convert_gdrive_url(url):
    """
    Convert Google Drive sharing URL to direct download URL
    
    Args:
        url (str): Google Drive URL
        
    Returns:
        str: Direct download URL
    """
    # Check if it's a Google Drive URL
    if "drive.google.com" in url and "/file/d/" in url:
        # Extract file ID using regex
        match = re.search(r'/file/d/([^/]+)', url)
        if match:
            file_id = match.group(1)
            direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            logger.info(f"Converted Google Drive URL to: {direct_url}")
            return direct_url
    
    # Return original URL if not a Google Drive URL or couldn't extract ID
    return url

def download_file(url, filename, temp_dir):
    """
    Downloads a file from a URL to the specified temporary directory
    
    Args:
        url (str): The URL to download the file from
        filename (str): The name to save the file as
        temp_dir (str): Path to the temporary directory
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Convert Google Drive URL if needed
        download_url = convert_gdrive_url(url)
        
        # Create full path for downloaded file
        file_path = os.path.join(temp_dir, filename)
        
        # Download the file
        logger.info(f"Downloading from {download_url} to {file_path}")
        urllib.request.urlretrieve(download_url, file_path)
        
        # Verify file was downloaded successfully
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            logger.info(f"Successfully downloaded {filename} ({os.path.getsize(file_path)} bytes)")
            return True
        else:
            logger.error(f"Downloaded file {filename} is empty or missing")
            return False
            
    except Exception as e:
        logger.error(f"Error downloading {url}: {str(e)}")
        return False

class MyRunnable(Runnable):

    def __init__(self, unused, config, plugin_config):
        # Note that, as all macros, it receives a first argument
        # which is normally the project key, but which is irrelevant for project creation macros
        self.config = config

    def get_progress_target(self):
        return (4, 'NONE')

    def run(self, progress_callback):
        # Get the identity of the end DSS user
        user_client = dataiku.api_client()
        user_auth_info = user_client.get_auth_info()

        # Automatically create a privileged API key and obtain a privileged API client
        # that has administrator privileges.
        admin_client = utils.get_admin_dss_client("creation1", user_auth_info)

        # The project creation macro must create the project. Therefore, it must first assign
        # a unique project key. This helper makes this easy
        project_key = utils.make_unique_project_key(admin_client, self.config["projectName"])

        # The macro must first perform the actual project creation.
        # We pass the end-user identity as the owner of the newly-created project
        logger.info("Creating project")
        admin_client.create_project(project_key, self.config["projectName"], user_auth_info["authIdentifier"])
        progress_callback(1)
        
        # Now, this macro sets up the default Python code environment, using the one specified by the user
        logger.info("Configuring project")
        project = user_client.get_project(project_key)

        # Move the project to the current project folder, passed in the config as _projectFolderId
        project.move_to_folder(user_client.get_project_folder(self.config['_projectFolderId']))
        
        progress_callback(2)
        
        # Create a managed folder in the project
        folder_name = self.config.get("managedFolderName", "Policies")
        logger.info(f"Creating managed folder: {folder_name}")
        managed_folder = project.create_managed_folder(folder_name)
        
        progress_callback(3)
        
        # Use the predefined Google Drive URLs
        logger.info("Downloading policy files to managed folder")
        file_urls = self.config.get("fileUrls", POLICY_FILES)
        
        # Create temp directory with random name
        temp_dir = create_random_temp_dir()
        
        try:

            # Track results
            results = {
                "success": 0,
                "failed": 0,
                "files": []
            }
            
            # Download all files to the temp directory
            for url_data in file_urls:
                url = url_data["url"]
                filename = url_data.get("filename")
                
                if not filename:
                    # Try to extract filename from URL
                    filename = os.path.basename(url.split('?')[0])
                    if not filename or filename == "":
                        # Default filename if we couldn't extract a meaningful one
                        random_prefix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
                        filename = f"downloaded_file_{random_prefix}.pdf"
                
                success = download_file(url, filename, temp_dir)
                if success:
                    results["success"] += 1
                    results["files"].append(filename)
                else:
                    results["failed"] += 1
            
            # Only upload if we successfully downloaded at least one file
            if results["success"] > 0:
                logger.info(f"Uploading {results['success']} files to managed folder")
                
                # Upload entire directory to managed folder
                managed_folder.upload_folder("/", temp_dir)
                logger.info(f"Successfully uploaded files: {', '.join(results['files'])}")
                
            logger.info(f"Download summary: {results['success']} successful, {results['failed']} failed")
            
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
        
        finally:
            # Clean up the temporary directory and all its contents
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                    logger.info(f"Removed temporary directory: {temp_dir}")
            except Exception as e:
                logger.error(f"Failed to remove temporary directory {temp_dir}: {str(e)}")
        progress_callback(4)
        # A project creation macro must return a JSON object containing a `projectKey` field with the newly-created
        # project key
        return json.dumps({"projectKey": project_key})