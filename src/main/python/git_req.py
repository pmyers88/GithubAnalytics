#!venv/bin/python
import requests
import re
import datetime
from time import time, sleep


HEADERS = {
    'Authorization': 'token %s' % 'MY_SERCRET_TOKEN'
}


graph = open('graph.txt', 'a')
usernames = open('usernames.txt', 'a')
next_url = 'https://api.github.com/users'
limit_response = requests.get('https://api.github.com/rate_limit',
                              headers=HEADERS)
remaining = int(limit_response.headers['X-RateLimit-Remaining'])
while True:
    response = requests.get(next_url, headers=HEADERS)
    remaining = int(response.headers['X-RateLimit-Remaining'])
    reset = int(response.headers['X-RateLimit-Reset'])
    link = response.headers['link']
    match = re.match('<(.*?)>; rel="next"', link)
    if match:
        next_url = match.group(1)
    print "Next URL: %s" % next_url
    print "Requests Remaining: %d" % remaining
    users_json = response.json()
    if remaining <= len(users_json):
        wait_seconds = reset - time() + 10
        print ('Not enough requests remaining; Going to sleep until %s' %
               datetime.datetime.fromtimestamp(reset + 10)
               .strftime('%Y-%m-%d %H:%M:%S'))
        sleep(wait_seconds)
        print 'I\'m awake! Gonna make more requests now...'
    for user in users_json:
        id = user['id']
        usernames.write('%d %s\n' % (id, user['login']))
        url = '/'.join([user['url'], 'followers'])
        followers = requests.get(url, headers=HEADERS)
        remaining = int(followers.headers['X-RateLimit-Remaining'])
        for follower in followers.json():
            graph.write('%d %d\n' % (follower['id'], id))
graph.close()
usernames.close()
