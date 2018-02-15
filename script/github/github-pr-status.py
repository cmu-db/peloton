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
import requests
import csv
from StringIO import StringIO
from datetime import datetime
from pprint import pprint

import smtplib
from email.mime.text import MIMEText

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

CACHE_FILEPATH = "/tmp/github-cache/%s.cache"

EMAIL_FROM = "no-reply@pelotondb.io"

# ==============================================
# pr_format
# ==============================================
def pr_format(pr):
    assert not pr is None
    ret = "%5s - %s [%s]\n" % ('#'+str(pr.number), pr.title, pr.user["login"])
    ret += "        %s" % (pr.html_url)
    return (ret)
## DEF    

# ==============================================
# get_current_czar
# ==============================================
def get_current_czar(url, cache=False):
    
    data = None
    cache_file = CACHE_FILEPATH % "schedule"
    if cache and os.path.exists(cache_file):
        LOG.debug("Cached Schedule '%s'" % cache_file)
        with open(cache_file, "r") as fd:
            data = fd.read()
    else:
        LOG.debug("Downloading schedule from %s" % url)
        response = requests.get(url)
        assert response.status_code == 200, \
            "Failed to download schedule '%s'" % url
        data = response.content
        LOG.debug("Creating schedule cached file '%s'" % cache_file)
        with open(cache_file, "w") as fd:
            fd.write(data)
    ## IF

    # The CSV file should have two columns:
    #   (1) The date "Month Year"
    #   (2) Email address
    current_date = datetime.now().strftime("%B %Y")
    current_czar = None
    first = True
    for row in csv.reader(StringIO(data)):
        if first:
            first = False
            continue
        if row[0].strip() == current_date:
            current_czar = row[1].strip()
            break
    ## FOR
    if current_czar is None:
        raise Exception("Failed to find current assignment for '%s'" % current_date)
    
    LOG.debug("Current Czar ('%s'): %s" % (current_date, current_czar))
    return (current_czar)
## DEF


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser()
    aparser.add_argument('token', type=str, help='Github API Token')
    aparser.add_argument('schedule', type=str, help='Schedule CSV URL')
    aparser.add_argument('--send', action='store_true', help='Send status report to current czar')
    aparser.add_argument('--send-to', type=str, help='Send status report to given email')
    aparser.add_argument('--gmail-user', type=str, help='Gmail username')
    aparser.add_argument('--gmail-pass', type=str,help='Gmail password')
    aparser.add_argument("--cache", action='store_true', help="Enable caching of raw Github requests (for development only)")
    aparser.add_argument("--debug", action='store_true')
    args = vars(aparser.parse_args())
    
    ## ----------------------------------------------
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)

    if args['cache']:
        cache_dir = os.path.dirname(CACHE_FILEPATH)
        if not os.path.exists(cache_dir):
            LOG.debug("Creating cache directory '%s'" % cache_dir)
            os.makedirs(cache_dir)
    ## IF
        

    ## ----------------------------------------------
    
    
    # Download the schedule CSV and figure out who is the czar this month
    current_czar = get_current_czar(args['schedule'], cache=args['cache'])

    ## ----------------------------------------------
    
    gh = Github(token=args['token'])
    r = gh.repos.get(user=GITHUB_USER, repo='peloton')
    
    ## --------------------------------
    ## PR MONITOR
    ## --------------------------------
    # List of pull requests:
    pull_requests = gh.pull_requests.list(state='open', user=GITHUB_USER, repo=GITHUB_REPO).all()

    open_pulls = { }
    for pr in pull_requests:
        #if pr.number != 1109: continue
        
        #print pr
        #pprint(pr.__dict__)
        #print "="*100
        
        # Get labels for this PR
        issue = gh.issues.get(pr.number, user=GITHUB_USER, repo=GITHUB_REPO)
        labels = [ i.name for i in issue.labels ]
        
        # Get events for this PR
        #events = gh.issues.events.list_by_issue(pr.number, user='cmu-db', repo=GITHUB_REPO).all()
        #for e in events:
            #pprint(e.__dict__)
            #print "-"*20
        #sys.exit(0)
        
        
        # Get review comments for this issue
        cache_reviews = CACHE_FILEPATH % str(pr.number)
        data = None
        if 'cache' in args and args['cache'] and os.path.exists(cache_reviews):
            LOG.debug("CACHED REVIEW COMMENTS '%s'" % cache_reviews)
            with open(cache_reviews, "r") as fd:
                data = fd.read()
        else:
            LOG.debug("Retrieving data from Github: '%s'" % pr.review_comments_url)
            data = urllib.urlopen(pr.review_comments_url).read()
            with open(cache_reviews, "w") as fd:
                fd.write(data)
        # IF
        reviews = json.loads(data)
        
        open_pulls[pr.number] = (pr, labels, reviews)
    ## FOR
    
    status = {
        "NeedReviewers":    [ ],
        "ReviewMissing":    [ ],
        "FollowUp":         [ ],
        "ReadyToMerge":     [ ],
        "NoLabels":         [ ],
    }
        
    
    all_recieved_reviews = { }
    all_reviewers = { }
    for pr, labels, reviews in open_pulls.values():
        LOG.debug("PR #%d - LABELS: %s", pr.number, labels)
        
        # Step 1: Get the list of 'ready_for_review' PRs that do not have
        # an assigned reviewer
        if 'ready_for_review' in labels and \
            len(pr.requested_reviewers) == 0:

            status['NeedReviewers'].append(pr.number)
            continue
        # IF
        
        # Step 2: Get the list of PRs that are 'ready_for_review' 
        # and have a reviewer, but that reviewer hasn't put anything in yet
        # Or they are 'accepted' but not merged
        if 'ready_for_review' in labels or \
            'accepted' in labels:
        
            reviewers = set()
            for rr in pr.requested_reviewers:
                reviewers.add(rr["login"])
            ## FOR
            
            recieved_reviews = set()
            for r in reviews:
                recieved_reviews.add(r["user"]["login"])
            ## FOR
            if pr.user["login"] in recieved_reviews:
                recieved_reviews.remove(pr.user["login"])
            
            if len(recieved_reviews) > 0:
                if 'accepted' in labels:
                    status['ReadyToMerge'].append(pr.number)
                else:
                    status['FollowUp'].append(pr.number)
            else:
                status['ReviewMissing'].append(pr.number)
                
                
            all_reviewers[pr.number] = map(str, reviewers)
            all_recieved_reviews[pr.number] = map(str, recieved_reviews)
                
            LOG.debug("PR #%d - REVIEWERS: %s", pr.number, ",".join(reviewers))
            LOG.debug("PR #%d - RECEIVED REVIEWS: %s", pr.number, ",".join(recieved_reviews))
        # IF
        
        # Step 4: Mark any PRs without labels
        if len(labels) == 0:
            status["NoLabels"].append(pr.number)
        # IF
    ## FOR
    
    content = ""
    linebreak = "-"*60 + "\n\n"
    
    ## NO LABELS
    content += "*NO LABELS*\n\n"
    if len(status['NoLabels']) == 0:
        content += "*NONE*\n\n"
    else:
        for pr_num in sorted(status['NoLabels']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak
    
    ## READY TO MERGE
    content += "*READY TO MERGE*\n\n"
    if len(status['ReadyToMerge']) == 0:
        content += "*NONE*\n\n"
    else:
        for pr_num in sorted(status['ReadyToMerge']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak
    
    ## READY FOR REVIEW, NO ASSIGNMENT
    content += "*READY FOR REVIEW WITHOUT ASSIGNMENT*\n\n"
    if len(status['NeedReviewers']) == 0:
        content += "*NONE*\n\n"
    else:
        for pr_num in sorted(status['NeedReviewers']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak
    
    ## MISSING REVIEWS
    content += "*WAITING FOR REVIEW*\n\n"
    if len(status['ReviewMissing']) == 0:
        content += "*NONE*\n\n"
    else:
        for pr_num in sorted(status['ReviewMissing']):
            if len(all_reviewers[pr_num]) == 0:
                all_reviewers[pr_num] = [ '*NONE*' ]
            if len(all_recieved_reviews[pr_num]) == 0:
                all_recieved_reviews[pr_num] = [ '*NONE*' ]
            
            content += pr_format(open_pulls[pr_num][0]) + "\n"
            content += "        Assigned Reviewers: %s\n" % (",".join(all_reviewers[pr_num]))
            content += "        Reviews Provided: %s\n\n" % (",".join(all_recieved_reviews[pr_num]))
    #content += linebreak
        
        
    if "send" in args and args["send"]:
        send_to = current_czar
        if "send_to" in args and args["send_to"]:
            send_to = args["send_to"]
            
        LOG.debug("Sending status report to '%s'" % send_to)
        msg = MIMEText(content)
        msg['Subject'] = "%s PR Status Report (%s)" % (GITHUB_REPO.title(), datetime.now().strftime("%Y-%m-%d"))
        msg['From'] = EMAIL_FROM
        msg['To'] = send_to
        msg['Reply-To'] = "no-reply@db.cs.cmu.edu"
        
        server_ssl = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server_ssl.ehlo() # optional, called by login()
        server_ssl.login(args["gmail_user"], args["gmail_pass"])
        # ssl server doesn't support or need tls, so don't call server_ssl.starttls() 
        server_ssl.sendmail(msg['From'], [msg['To']], msg.as_string())
        #server_ssl.quit()
        server_ssl.close()
        
        LOG.info("Status email sent to '%s'" % send_to)
    else:
        print content
    ## IF
        
## MAIN
