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

CACHE_FILEPATH = "/tmp/%d.github-cache"

EMAIL_FROM = "pavlo@cs.cmu.edu"

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
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser()
    aparser.add_argument('token', type=str, help='Github API Token')
    aparser.add_argument('--send', type=str, help='Send status report to given email')
    aparser.add_argument('--gmail-user', type=str, help='Gmail username')
    aparser.add_argument('--gmail-pass', type=str,help='Gmail password')
    aparser.add_argument("--cache", action='store_true', help="Enable caching of raw Github requests (for development only)")
    aparser.add_argument("--debug", action='store_true')
    args = vars(aparser.parse_args())
    
    ## ----------------------------------------------
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)

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
        cache_reviews = CACHE_FILEPATH % pr.number
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
        LOG.debug("Pull Request #%d - LABELS: %s", pr.number, labels)
        
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
                
                
            all_reviewers[pr.number] = reviewers
            all_recieved_reviews[pr.number] = recieved_reviews
                
            LOG.debug("REVIEWERS: %s", ",".join(reviewers))
            LOG.debug("RECEIVED REVIEWS: %s", ",".join(recieved_reviews))
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
        content += "**NONE**\n\n"
    else:
        for pr_num in sorted(status['NoLabels']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak
    
    ## READY TO MERGE
    content += "*READY TO MERGE*\n\n"
    if len(status['ReadyToMerge']) == 0:
        content += "**NONE**\n\n"
    else:
        for pr_num in sorted(status['ReadyToMerge']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak
    
    ## READY FOR REVIEW, NO ASSIGNMENT
    content += "*READY FOR REVIEW WITHOUT ASSIGNMENT*\n\n"
    if len(status['NeedReviewers']) == 0:
        content += "**NONE**\n\n"
    else:
        for pr_num in sorted(status['NeedReviewers']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak
    
    
    ## MISSING REVIEWS
    content += "*WAITING FOR REVIEW*\n\n"
    if len(status['ReviewMissing']) == 0:
        content += "**NONE**\n\n"
    else:
        for pr_num in sorted(status['ReviewMissing']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
            content += "        Assigned Reviewers: %s\n" % (list(all_recieved_reviews[pr_num]))
    #content += linebreak
        
        
    if "send" in args and args["send"]:
        msg = MIMEText(content)
        msg['Subject'] = "%s PR Status Report (%s)" % (GITHUB_REPO.title(), datetime.now().strftime("%Y-%m-%d"))
        msg['From'] = EMAIL_FROM
        msg['To'] = args["send"]
        msg['Reply-To'] = "no-reply@db.cs.cmu.edu"
        
        server_ssl = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server_ssl.ehlo() # optional, called by login()
        server_ssl.login(args["gmail_user"], args["gmail_pass"])
        # ssl server doesn't support or need tls, so don't call server_ssl.starttls() 
        server_ssl.sendmail(msg['From'], [msg['To']], msg.as_string())
        #server_ssl.quit()
        server_ssl.close()
        
        LOG.info("Status email sent to '%s'" % args["send"])
    else:
        print content
    ## IF
        
## MAIN
