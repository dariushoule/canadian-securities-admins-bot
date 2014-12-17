# -*- coding: utf-8 -*-

import json
import datetime
import turbotlib
import requests
import time
import re
import urllib
from bs4 import BeautifulSoup

# Global request session
session = requests.Session()

# Global post data seed/continue
with open("post_body_seed.raw", "r") as pb_seed:
    post_body_seed = pb_seed.read()

with open("post_body_continue.raw", "r") as pb_continue:
    post_body_continue = pb_continue.read()

with open("post_body_detail.raw", "r") as pb_continue:
    post_body_detail = pb_continue.read()

last_view_state = ""
last_view_generator = ""
last_validation = ""
url_start = "http://www.securities-administrators.ca/nrs/nrsearch.aspx?id=850"

##
# retrieve_post will attempt to return a completed POST request, retrying on failure.
# Re-attempts try up to 5 times, waiting longer each time.
#
# @param url The web address
# @param postBody The post body
# @attempt attempt The current number of tries

def retrieve(url, method, data, attempt=1):
    connection_exception = False
    headers = {"X-MicrosoftAjax": "Delta=true",
               "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
               "Accept": "*/*",
               "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
               "Cache-Control": "no-cache",
               "Pragma": "no-cache"}

    try:
        req = requests.Request(method, url, data=data, headers=headers)
        prepared = req.prepare()
        response = session.send(prepared)

    except requests.exceptions.RequestException:
        connection_exception = True

    if (connection_exception or response.status_code != requests.codes.ok) and attempt <= 5:
        turbotlib.log("There was a failure reaching or understanding the host, waiting and retrying...")
        turbotlib.log("Failure: " + response.text)
        time.sleep(attempt * 5)
        return retrieve(url, method, data, attempt + 1)

    return response


def get_record_count(response):
    match = re.search(r'There are (\d+) records found', response,  re.DOTALL)
    return match.group(0)


def get_asp_resp_var(response, var):
    match = re.search(r'\|' + var + '\|(.*?)\|', response, re.DOTALL)
    return match.group(1)


def get_result_table(response):
    match = re.search(r'(<table class="gridview_style".*?</table>)', response, re.DOTALL)
    return BeautifulSoup(match.group(0))


def process_page(url, post_body, page_number):
    global last_view_state
    global last_validation
    global last_view_generator

    turbotlib.log("Requesting rows %d - %d" % ((page_number * 100 - 100), (page_number * 100)))

    body = post_body.replace("[PAGE_NUMBER]", str(page_number)) \
        .replace("[VIEW_STATE]", last_view_state)               \
        .replace("[VALIDATION]", last_validation)               \
        .replace("[GENERATOR]", last_view_generator)

    req = retrieve(url, "POST", body)
    last_view_state     = urllib.quote(get_asp_resp_var(req.text, "__VIEWSTATE"))
    last_validation     = urllib.quote(get_asp_resp_var(req.text, "__EVENTVALIDATION"))
    last_view_generator = urllib.quote(get_asp_resp_var(req.text, "__VIEWSTATEGENERATOR"))
    table = get_result_table(req.text)

    for tr in table.find_all('tr'):
        tds = tr.find_all('td')

        if len(tds) == 2:
            print {
                'firm': tds[0].text,
                'jurisdiction': tds[1].text,
                'sample_date': datetime.datetime.now().isoformat(),
                'source_url': url_start
            }

    print req.text
    return req.text


def process_pages(url):
    record_count = None
    page_number = 1

    while record_count is None or (page_number * 100) < record_count:
        response_text = process_page(url, post_body_seed if (page_number == 1) else post_body_continue, page_number)

        # Ensure the number of records haven't changed during run
        check_count = get_record_count(response_text)
        if record_count is not None and record_count != check_count:
            raise Exception("The data set changed during a load, we need a re-run.")
        else:
            record_count = check_count

        if not record_count > 0:
            raise Exception("The data set is empty.")

        if page_number > 3:
            break

        page_number += 1

    turbotlib.log("Run finished!")


turbotlib.log("Starting run...")
process_pages(url_start)

#--
#for n in range(0, 20):
#    data = {"number": n,
#            "company": "Company %s Ltd" % n,
#            "message": "Hello %s" % n,
#            "sample_date": datetime.datetime.now().isoformat(),
#            "source_url": "http://somewhere.com/%s" % n}
#    # The Turbot specification simply requires us to output lines of JSON
#    print json.dumps(data)
