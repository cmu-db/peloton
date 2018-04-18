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
import time
import pytz
import dateutil.parser
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
GITHUB_REPO_API_URL = "https://api.github.com/repos/%s/%s" % (GITHUB_USER, GITHUB_REPO)

CACHE_FILEPATH = "/tmp/github-cache/%s.cache"

EMAIL_FROM = "no-reply@pelotondb.io"
EMAIL_PREFIX = "[Peloton] "

FIRST_COMMIT = '35823950d500314811212282bd68c101e34b9a06'


EMAILS_TO_IGNORE = [
    "noreply.github.com",
    "macbook-pro.local",
    "cc.cmu.edu",
    "vagrant.vm",
]

LABELS_TO_IGNORE = [
    "class-project",
]

# ==============================================
# pr_format
# ==============================================
def pr_format(pr):
    assert not pr is None
    
    ret = "%5s - %s [%s]\n" % ('#'+str(pr.number), pr.title, pr.user["login"])
    ret += "        Last Updated: %s\n" % pr.updated_at.strftime("%Y-%m-%d")
    ret += "        %s" % (pr.html_url)
    
    return (ret)
## DEF

# ==============================================
# get_url
# ==============================================
def get_url(url, name, read_cache=False):
    data = None
    cache_file = CACHE_FILEPATH % name
    if read_cache and os.path.exists(cache_file):
        LOG.debug("Read Cached File '%s' for '%s'" % (cache_file, url))
        with open(cache_file, "r") as fd:
            data = fd.read()
    else:
        LOG.debug("Downloading data from %s" % url)
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Failed to download '%s'" % url)
        data = response.content
        LOG.debug("Creating cached file '%s'" % cache_file)
        with open(cache_file, "w") as fd:
            fd.write(data)
    ## IF
    return (data)
## DEF

# ==============================================
# get_user_emails
# ==============================================
def get_user_emails(gh, email_file, cache=False):
    users = { }
    newest_commit = None
    load_all = True
    
    if os.path.exists(email_file):
        LOG.debug("Reading Email Mapping '%s'" % (email_file))
        with open(email_file, "r") as fd:
            for row in csv.reader(fd):
                # The first line is the date of the newest
                # commit that we have seen. If we end up seeing
                # a commit that is older than this, then we can
                # stop fetching things from Github
                if newest_commit is None:
                    newest_commit = datetime.fromtimestamp(int(row[0]))
                    newest_commit = newest_commit.replace(tzinfo=pytz.UTC)
                    
                # Otherwise then each line in the CSV
                # will contain the Github login in the first
                # column and then a variable-length number of 
                # columns for all the emails that the account 
                # has that we found.
                else:
                    users[row[0]] = row[1:]
        ## WITH
        assert not newest_commit is None
        load_all = False
    ## IF

    # Loop through until we get nothing or find a commit that
    # is older than our newest_commit
    page = 1
    missing_author = 0
    stop = False
    while stop == False:
        url = GITHUB_REPO_API_URL + "/commits?page=%d" % page
        try:
            contents = get_url(url, 'commits%d'%page, read_cache=cache)
        except:
            stop = True
            break
            
        if len(contents) > 0:
            #contents = json.loads(contents)
            #url = GITHUB_REPO_API_URL + "/compare/" + FIRST_COMMIT + "..." + contents["object"]["sha"]
            #contents = get_url(url, 'compare', read_cache=cache)
            github_data = json.loads(contents)
            #pprint (github_data)
            
            for c in github_data:
                if c is None: continue
                #pprint(c)
                #sys.exit(0)
            
                if not "author" in c or c["author"] is None:
                    #pprint(c)
                    #sys.exit(1)
                    missing_author = missing_author + 1
                    continue
                if not "login" in c["author"]:
                    #pprint(c)
                    #sys.exit(1)
                    missing_author = missing_author + 1
                    continue
                
                login = c["author"]["login"]
                email = c["commit"]["author"]["email"]
                
                # Get the commit timestamp
                commit_ts = dateutil.parser.parse(c["commit"]["author"]["date"])
                if newest_commit is None or newest_commit < commit_ts:
                    newest_commit = commit_ts
                    LOG.debug("Newest Commit TS: %s" % str(commit_ts))
                elif load_all == False and newest_commit > commit_ts:
                    LOG.debug("Stop gathering commits [commit:%s > newest:%s]" % (str(commit_ts), str(newest_commit)))
                    stop = True
                    break
                ## IF
                
                ignore = False
                for e in EMAILS_TO_IGNORE:
                    if email.find(e) != -1:
                        LOG.debug("Ignoring email '%s'" % email)
                        ignore = True
                        break
                if ignore or email.find("@") == -1: continue
                
                if not login in users:
                    users[login] = [ ]
                if not email in users[login]:
                    users[login].append(email)
        ## IF
        page = page + 1
    ## WHILE
    
    LOG.debug("Saving Email Mapping '%s'" % (email_file))
    with open(email_file, "w") as fd:
        writer = csv.writer(fd, quoting=csv.QUOTE_ALL)
        
        # Write out the newest timestamp
        if not newest_commit is None:
            writer.writerow([int(time.mktime(newest_commit.timetuple()))])
        else:
            writer.writerow([0])
        
        for login in sorted(users.keys()):
            if len(users[login]) == 0: continue
            writer.writerow([ login ] + users[login])
    ## WITH
    
    LOG.debug("Total Commits Missing Authors: %d" % missing_author)
    return (users)
## DEF

# ==============================================
# get_current_czar
# ==============================================
def get_current_czar(url, cache=False):

    data = get_url(url, "schedule", read_cache=cache)

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
# send_email
# ==============================================
def send_email(args, subject, contents, send_from, send_to, send_cc=[]):
    msg = MIMEText(contents)
    msg['Subject'] = EMAIL_PREFIX + subject
    msg['From'] = send_from
    msg['To'] = send_to
    if len(send_cc) > 0: msg['CC'] = ",".join(send_cc)
    msg['Reply-To'] = "no-reply@db.cs.cmu.edu"

    server_ssl = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server_ssl.ehlo() # optional, called by login()
    server_ssl.login(args["gmail_user"], args["gmail_pass"])
    server_ssl.sendmail(msg['From'], [msg['To']], msg.as_string())
    server_ssl.close()
    
    #print "Subject:", msg['Subject']
    #print "From:", msg['From']
    #print "To:", msg['To']
    #if 'CC' in msg: print "CC:", msg['CC']
    #print contents
## DEF


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser()
    aparser.add_argument('token', type=str, help='Github API Token')
    aparser.add_argument('schedule', type=str, help='Schedule CSV URL')
    aparser.add_argument('emails', type=str, help='Mapping file (Github->email)')
    aparser.add_argument('--send-status', action='store_true', help='Send status report to current czar')
    aparser.add_argument('--send-reminders', action='store_true', help='Send PR review reminders')
    aparser.add_argument('--override', type=str, help='All emails will go to this account')
    aparser.add_argument('--gmail-user', type=str, help='Gmail username')
    aparser.add_argument('--gmail-pass', type=str,help='Gmail password')
    aparser.add_argument("--cache", action='store_true', help="Enable caching of raw Github requests (for development only)")
    aparser.add_argument("--debug", action='store_true')
    args = vars(aparser.parse_args())

    ## ----------------------------------------------

    if args['debug']:
        LOG.setLevel(logging.DEBUG)

    cache_dir = os.path.dirname(CACHE_FILEPATH)
    if not os.path.exists(cache_dir):
        LOG.debug("Creating cache directory '%s'" % cache_dir)
        os.makedirs(cache_dir)

    ## ----------------------------------------------


    # Download the schedule CSV and figure out who is the czar this month
    current_czar = get_current_czar(args['schedule'], cache=args['cache'])
    
    ## ----------------------------------------------

    gh = Github(token=args['token'])
    r = gh.repos.get(user=GITHUB_USER, repo=GITHUB_REPO)

    ## ----------------------------------------------
    
    # Get the mapping of Github acctnames to emails
    user_emails = get_user_emails(gh, args["emails"], cache=args["cache"])

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
        
        # Skip any PRs with labels we should ignore
        should_ignore = set(labels).intersection(set(LABELS_TO_IGNORE))
        if should_ignore:
            LOG.debug("PR %d has labels that we need to ignore: %s" % (pr.number, list(should_ignore)))
            continue
        ## IF
        

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
    content += "=== NO LABELS ===\n\n"
    if len(status['NoLabels']) == 0:
        content += "*NONE*\n\n"
    else:
        for pr_num in sorted(status['NoLabels']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak

    ## READY TO MERGE
    content += "=== READY TO MERGE ===\n\n"
    if len(status['ReadyToMerge']) == 0:
        content += "*NONE*\n\n"
    else:
        for pr_num in sorted(status['ReadyToMerge']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak

    ## READY FOR REVIEW, NO ASSIGNMENT
    content += "=== READY FOR REVIEW WITHOUT ASSIGNMENT ===\n\n"
    if len(status['NeedReviewers']) == 0:
        content += "*NONE*\n\n"
    else:
        for pr_num in sorted(status['NeedReviewers']):
            content += pr_format(open_pulls[pr_num][0]) + "\n\n"
    content += linebreak

    ## MISSING REVIEWS
    content += "=== WAITING FOR REVIEW ===\n\n"
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

    if "send_status" in args and args["send_status"]:
        send_to = current_czar
        if "override" in args and args["override"]:
            send_to = args["override"]
        subject = "%s PR Status Report (%s)" % (GITHUB_REPO.title(), datetime.now().strftime("%Y-%m-%d"))
        
        send_email(args, subject, content, EMAIL_FROM, send_to)
        LOG.info("Sent status email to '%s'" % send_to)
    else:
        print content
    ## IF
    
    # Send reminder emails for missing PRs
    if "send_reminders" in args and args["send_reminders"]:
        if "override" in args and args["override"]:
            master_sender = args["override"]
        else:
            master_sender = current_czar
        
        for pr_num in sorted(status['ReviewMissing']):
            for reviewer in all_reviewers[pr_num]:
                if reviewer in all_recieved_reviews[pr_num]:
                    continue
                if not reviewer in user_emails:
                    LOG.warn("No email available for login '%s'" % reviewer)
                    continue
                
                subject = "Missing Review for PR #%d" % pr_num
                if "override" in args and args["override"]:
                    send_to = args["override"]
                else:
                    send_to = ",".join(user_emails[reviewer])
                send_cc = [ master_sender ] 
                
                content = "Please add your review for the following PR:\n\n" + \
                          pr_format(open_pulls[pr_num][0])
                            
                send_email(args, subject, content, EMAIL_FROM, send_to, send_cc)
                LOG.info("Sent reminder email to '%s'" % send_to)
            ## FOR
        ## FOR
    ## IF

## MAIN
