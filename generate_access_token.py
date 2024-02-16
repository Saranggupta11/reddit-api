import requests
import requests.auth
from dotenv import load_dotenv
import os

load_dotenv()

client_id = os.environ.get('CLIENT_ID')
client_secret = os.environ.get('CLIENT_SECRET')
reddit_username=os.environ.get('REDDIT_USERNAME')
reddit_pasword=os.environ.get('REDDIT_PASSWORD')

client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
post_data = {"grant_type": "password", "username": reddit_username, "password": reddit_pasword}
headers = {"User-Agent": f"ChangeMeClient/0.1 by {reddit_username}"}
response = requests.post("https://www.reddit.com/api/v1/access_token", auth=client_auth, data=post_data, headers=headers)


# prints the bearer access token that can use used to authorise api requests
# access token expiry - 86400 seconds
print(response.json())