import requests
from elasticsearch import Elasticsearch,helpers
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.environ.get('ACCESS_TOKEN')
reddit_username=os.environ.get('REDDIT_USERNAME')

 
es = Elasticsearch(['http://localhost:9200'])
 
index_name = 'subreddit_posts'


#preparing es actions for bulk indexing documents
def prepare_es_actions(all_data, data_type):
    actions = []
    for data in all_data:
        id = data['data']['name']
        if data_type == 'comments':
            source = {
                'body': data['data']['body'],
                'kind': data['kind'],
                'post_id': data['data']['link_id'],
                'up_votes': data['data']['ups']
            }
        elif data_type == 'posts':
            source = {
                'title': data['data']['title'],
                'up_votes': data['data']['ups'],
                'kind': data['kind'],
                'num_comments': data['data']['num_comments']
            }
        source["timestamp"] = datetime.now()
        actions.append({
            "_index": index_name,
            "_type": "_doc",
            "_id": id,
            "_source": source
        })
    return actions


def get_posts_for_subreddit(subreddit_name,data_type,after):
    headers = {"Authorization": f"bearer {access_token}", "User-Agent": f"ChangeMeClient/0.1 by {reddit_username}"}
    response = requests.get(f"https://oauth.reddit.com/r/{subreddit_name}/new?limit=100&after={after}", headers=headers)

    jsonResponse=response.json()
    all_posts_data=jsonResponse['data']['children']
    
    actions=prepare_es_actions(all_posts_data,data_type)
    
    helpers.bulk(es, actions)
    print("posts pushed to ES")
    
    return jsonResponse['data']['after']

        

def get_comments_for_subreddit(subreddit_name,data_type,after):
    headers = {"Authorization": f"bearer {access_token}", "User-Agent": f"ChangeMeClient/0.1 by {reddit_username}"}
    response = requests.get(f"https://oauth.reddit.com/r/{subreddit_name}/comments?limit=100&after={after}", headers=headers)

    jsonResponse=response.json()
    all_comments_data=jsonResponse['data']['children']
    
    actions=prepare_es_actions(all_comments_data,data_type)
    
    helpers.bulk(es, actions)    
    print("comments pushed to ES ")
    
    return jsonResponse['data']['after']
  
   
#can fetch only upto 1000 posts and 1000 comments for a given subreddit
def fetch_data(subreddit_name, data_type):
    after = ""
    while True:
        if data_type == "posts":
            after = get_posts_for_subreddit(subreddit_name,data_type,after)
        elif data_type == "comments":
            after = get_comments_for_subreddit(subreddit_name,data_type,after)
        print(after)
        if after is None:
            break

if __name__ == "__main__":
    fetch_data("Cricket", "posts")
    fetch_data("Cricket", "comments")
        
    


    











 









