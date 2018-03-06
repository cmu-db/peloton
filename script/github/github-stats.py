#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import sys
import os
import re
import json
import time
import urllib2
import argparse
import urllib
import json
import logging
from datetime import datetime
from pprint import pprint

from pygithub3 import Github

## ==============================================
## LOGGING
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## CONFIGURATION
## ==============================================

GITHUB_USER = 'cmu-db'
GITHUB_REPO = 'peloton'
GITHUB_REPO_API_URL = "https://api.github.com/repos/%s/%s" % (GITHUB_USER, GITHUB_REPO)

FIRST_COMMIT = '35823950d500314811212282bd68c101e34b9a06'

# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser()
    aparser.add_argument('token', type=str, help='Github API Token')
    aparser.add_argument("--debug", action='store_true')
    args = vars(aparser.parse_args())
    
    ## ----------------------------------------------
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)

    ## ----------------------------------------------
    
    gh = Github(token=args['token'])
    r = gh.repos.get(user=GITHUB_USER, repo=GITHUB_REPO)

    num_commits = 0
    collaborators = set()
    c = gh.repos.commits.list(user=GITHUB_USER, repo=GITHUB_REPO)
    for x in c.iterator():
        if not x.author is None:
            collaborators.add(x.author.login)
        num_commits += 1
        #pprint(dir(x.author.login))
        #for k in sorted(x.__dict__.keys()):
            #print k, "=>", x.__dict__[k]
        #pprint(dir(x))
        #
        #print "="*100
        
    # Hash of the latest commit is fetched through the GitHub API
    target_url = GITHUB_REPO_API_URL + "/git/refs/heads/master"
    contents = urllib2.urlopen(target_url).read()
    if len(contents) > 0:
        contents = json.loads(contents)
        #pprint(contents)
        target_url = GITHUB_REPO_API_URL + "/compare/" + FIRST_COMMIT + "..." + contents["object"]["sha"]
        contents = urllib2.urlopen(target_url).read()
        github_data = json.loads(contents)
        if "total_commits" in github_data:
            num_commits = int(github_data["total_commits"])
        ## IF
    ## IF
        
    data = {
        "created": int(time.time()),
        "commits": num_commits,
        "commits_url": r.commits_url,
        "contributors": len(collaborators),
        "contributors_url": r.contributors_url,
        "forks": r.forks_count,
        "stars": r.stargazers_count,
        "stars_url": r.stargazers_url,
        "watchers": r.watchers_count,
        "issues": r.open_issues_count,
        "issues_url": r.issues_url,
        "subscribers": r.subscribers_count,
        "subscribers_url": r.subscribers_url,
        "network": r.network_count,
    }
    print json.dumps(data)
    
## MAIN
