import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.environ.get('ACCESS_TOKEN')
reddit_username=os.environ.get('REDDIT_USERNAME')

def rate_limit_test():
 
    for i in range(1000):
        headers = {"Authorization": f"bearer {access_token}", "User-Agent": f"ChangeMeClient/0.1 by {reddit_username}"}
        response = requests.get(f"https://oauth.reddit.com/r/Cricket/new?limit=100", headers=headers)
        jsonResponse=response.json()
        title = jsonResponse['data']['children'][i%100]['data']['title']
        print("---------------------------------------------------------  API Request Sent", i, " ---------------------------------------------------------------")
        print()
        print(title)
        print()
        
        

rate_limit_test()        