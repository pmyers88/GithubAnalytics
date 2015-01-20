#!venv/bin/python
import requests
import re

URL_BASE = 'https://api.github.com'

class GithubRequest():

    def __init__(self, token):
        self._headers = {'Authorization': 'token %s' % token}
        self.set_remaining_reset()

    def set_remaining_reset(self):
        limit_response = requests.get('/'.join([URL_BASE, 'rate_limit']),
            headers=self._headers)
        self.requests_remaining = int(limit_response.headers['X-RateLimit-Remaining'])
        self.rate_reset = int(limit_response.headers['X-RateLimit-Reset'])

    def get(self, url, params=None):
        response = {}
        if self.requests_remaining > 0:
            entities = requests.get(url, params=params, headers=self._headers)
            response['remaining'] = int(entities.headers['X-RateLimit-Remaining'])
            self.requests_remaining = response['remaining']
            response['reset'] = int(entities.headers['X-RateLimit-Reset'])
            self.rate_reset = response['reset']
            response['next_page'] = self.parse_link(entities.headers.get('link',
                ''), 'next')
            response['entities'] = entities.json()
        else:
            response['remaining'] = self.requests_remaining
            response['reset'] = self.rate_reset
        return response

    def parse_link(self, link, rel):
        match = re.match('<(.*?)>; rel="%s"' % rel, link)
        url = None
        if match:
            url = match.group(1)
        return url

def main():
    graph = open('graph.txt', 'a')
    usernames = open('usernames.txt', 'a')
    req = GithubRequest('MY_SECRET_API_KEY')
    next_page = '/'.join([URL_BASE, 'users'])
    while True:
        users = req.get(next_page)
        if not users['entities']:
            break
        next_page = users['next_page']
        print "Next URL: %s" % next_page
        print "Requests Remaining: %d" % users['remaining']
        for user in users['entities']:
            id = user['id']
            usernames.write('%d %s\n' % (id, user['login']))
            print 'Getting followers for user %s' % user['login']
            next_followers_page = user['followers_url']
            while True:
                followers = req.get(next_followers_page)
                next_followers_page = followers.get('next_page')
                print "Next followers URL: %s" % next_followers_page
                print "Requests Remaining: %d" % followers['remaining']
                for follower in followers['entities']:
                        graph.write('%d %d\n' % (follower['id'], id))
                if not next_followers_page:
                    break
    graph.close()
    usernames.close()

if __name__ == '__main__':
    main()
