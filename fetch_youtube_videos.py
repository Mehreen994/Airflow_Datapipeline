import requests
from airflow.models import Variable

#some apis need permission(api_key) to access data.
#step1: definining a function to get data from api
def fetch_youtube_episodes (channel_id ,max_results=5):
    api_key = Variable.get("YOUR_API_KEY")
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part" : "snippet",
        "channelId" : channel_id,
        "maxResults" : max_results,
        "order" : "date",
        "type" : "video",
        "key" : "AIzaSyCfdRiLUToWBHmLkGobyGobxf4CO7ONbuM",
    }

    response = requests.get(url,params)
    print(response.raise_for_status())
    videos = response.json().get("items",[])

    episodes =[
        {
          "video_id": video["id"],
            "title": video["snippet"]["title"],
            "description": video["snippet"]["description"],
            "published_at": video["snippet"]["publishedAt"],
            "channel_title": video["snippet"]["channelTitle"],
            "thumbnails": video["snippet"]["thumbnails"],
         }
        for video in videos
]
    return episodes
