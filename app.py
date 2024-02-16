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


def get_posts_for_subreddit(subreddit_name,after):
    headers = {"Authorization": f"bearer {access_token}", "User-Agent": f"ChangeMeClient/0.1 by {reddit_username}"}
    response = requests.get(f"https://oauth.reddit.com/r/{subreddit_name}/new?limit=100&after={after}", headers=headers)

    jsonResponse=response.json()
    
    actions=prepare_es_actions(jsonResponse['data']['children'],'posts')
    
    helpers.bulk(es, actions)
    print("posts pushed to ES")
    
    return jsonResponse['data']['after']

        

def get_comments_for_subreddit(subreddit_name,after):
    headers = {"Authorization": f"bearer {access_token}", "User-Agent": f"ChangeMeClient/0.1 by {reddit_username}"}
    response = requests.get(f"https://oauth.reddit.com/r/{subreddit_name}/comments?limit=100&after={after}", headers=headers)

    jsonResponse=response.json()
    
    actions=prepare_es_actions(jsonResponse['data']['children'],'comments')
    
    helpers.bulk(es, actions)    
    print("comments pushed to ES ")
    
    return jsonResponse['data']['after']



    

def fetch_comments(subreddit_name):
    after=""  
    while 1:     
        after=get_comments_for_subreddit(subreddit_name,after) 
        print(after) 
        if after is None:
            break
        
def fetch_posts(subreddit_name):
    after=""  
    while 1:     
        after=get_posts_for_subreddit(subreddit_name,after) 
        print(after) 
        if after is None:
            break    
                
        
if __name__ == "__main__":
    fetch_posts("Cricket")
    fetch_comments("Cricket")    
        
    


    











 









