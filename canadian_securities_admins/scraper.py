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


# Global post data page/detail requests
with open("post_body_seed.raw", "r") as pb_seed:
    post_body_seed = pb_seed.read()

with open("post_body_continue.raw", "r") as pb_continue:
    post_body_continue = pb_continue.read()

with open("post_body_control.raw", "r") as pb_detail:
    post_body_control = pb_detail.read()


# Global application state
last_view_state = ""
last_view_generator = ""
last_validation = ""
url_start = "http://www.securities-administrators.ca/nrs/nrsearch.aspx?id=850"
broken_rows_regex = re.compile(r'<div id="ctl[0-9]+_bodyContent_dlstFirmLocations_ctl[0-9]+_rptCategories_ctl[0-9]+_pnlRevocationDate">(.*?</div>.*?)</div>', re.DOTALL)


##
# retrieve will attempt to return a completed request, retrying on failure.
# Re-attempts try up to 5 times, waiting longer each time.
#
# @param url The web address
# @param method The HTTP method
# @param data The payload
# @param attempt The current number of tries
#
# @return The response data (including headers) or None on failure

def retrieve(url, method, data, attempt=1):
    response = None
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

        if response is not None and response.text is not None:
            turbotlib.log("Failure was: " + response.text)

        time.sleep(attempt * 5)
        return retrieve(url, method, data, attempt + 1)

    return response


##
# get_record_count will retrieve the reported number of rows embedded in the response
#
# @param response The text of the response
#
# @return The number of rows to process as int

def get_record_count(response):
    match = re.search(r'There are (\d+) records found', response,  re.DOTALL)
    return int(match.group(1))


##
# get_asp_resp_var will retrieve an ASP.net formatted variable in an async response
#
# @param response The text of the response
#
# @return The value of the variable

def get_asp_resp_var(response, var):
    match = re.search(r'\|' + var + '\|(.*?)\|', response, re.DOTALL)
    return match.group(1)


##
# get_result_table will retrieve a table of data from the async page response
#
# @param response The text of the response
#
# @return The bs4 object with table

def get_result_table(response):
    match = re.search(r'(<table class="gridview_style".*?</table>)', response, re.DOTALL)
    return BeautifulSoup(match.group(0))


##
# get_details_div will retrieve a div containing data from the async detail response
#
# @param response The text of the response
#
# @return The bs4 object with div

def get_details_div(response):
    match = re.search(r'(<div id="ctl00_bodyContent_divSearchResults".*</div>)', response, re.DOTALL)
    return BeautifulSoup(match.group(0))


##
# generate_body will build the body payload for a page request
#
# @param page_number The current page
#
# @return A data string

def generate_body(page_number):
    body = post_body_seed if page_number == 1 else post_body_continue
    return body.replace("[PAGE_NUMBER]", str(page_number))     \
            .replace("[VIEW_STATE]", last_view_state)          \
            .replace("[VALIDATION]", last_validation)          \
            .replace("[GENERATOR]", last_view_generator)


##
# generate_body_detail will build the body payload for a detail request
#
# @param control_id The id of the control we're sending as the event target
# @param view_state The current ASP.NET viewstate
#
# @return A data string

def generate_body_control(control_id, view_state):
    return post_body_control.replace("[CONTROL_ID]", control_id) \
            .replace("[VIEW_STATE]", view_state['view'])         \
            .replace("[VALIDATION]", view_state['validation'])   \
            .replace("[GENERATOR]",  view_state['generator'])


##
# process_details will perform the href action on a firm link to retrieve its details.
#
# @param url The url of the form to process
# @param control_href The details link href
#
# @return A dictionary containing its locations and associated data

def process_details(url, control_href):
    return_dict = []

    control_id = urllib.quote(control_href.replace("javascript:__doPostBack('", '').replace("','')", ''))
    details_req = retrieve(url, "POST", generate_body_control(control_id, {
                                                                 'view'      : last_view_state,
                                                                 'validation': last_validation,
                                                                 'generator' : last_view_generator}))

    detail_view_state = {'view'      : urllib.quote(get_asp_resp_var(details_req.text, "__VIEWSTATE")),
                         'validation': urllib.quote(get_asp_resp_var(details_req.text, "__EVENTVALIDATION")),
                         'generator' : urllib.quote(get_asp_resp_var(details_req.text, "__VIEWSTATEGENERATOR"))}

    if "ctl00_bodyContent_lbtnShowFirmHistorical" in details_req.text:
        history_req = retrieve(url, "POST", generate_body_control("ctl00%24bodyContent%24lbtnShowFirmHistorical", detail_view_state))
    else:
        history_req = None
        history_entries = []

    resp_markup = get_details_div(details_req.text)
    locations_entries = resp_markup.select("#ctl00_bodyContent_dlstFirmLocations > tr > td")

    if history_req is not None:

        # Fix super broken tables on history resp
        history_text = history_req.text
        for match in broken_rows_regex.finditer(history_text):
            history_text = history_text.replace(match.group(0), match.group(1))

        history_markup = get_details_div(history_text)
        history_entries = history_markup.select("#ctl00_bodyContent_dlstFirmLocations > tr > td")

    for entry in (locations_entries + history_entries):
        entry_dict = {'jurisdiction': entry.select('.sectiontitle > span')[0].text.strip()}
        locations_table = entry.find('table', recursive=False).find('tbody', recursive=False)
        locations_rows = locations_table.find_all("tr", recursive=False)

        categories = []
        for row in locations_rows:
            field = row.select('th > span') or row.select('th')
            if len(field) > 0:
                field = field[0].text.strip()

                if field == "Category":
                    categories.append({'category': row.find('td').text.strip()})

                elif field == "From":
                    categories[-1]['from']   = row.find('td').text.strip()

                elif field == "To":
                    categories[-1]['to']     = row.find('td').text.strip()

                elif field == "Status":
                    categories[-1]['status'] = row.find('td').text.strip()

                elif field == "Terms & Conditions":
                    entry_dict['status']     = row.select('td > span')[0].text.strip()

                elif field == "Contact Information":
                    entry_dict['contact']    = ""
                    for contact in row.select('td table td'):
                        entry_dict['contact'] += "\n" + "\n".join(contact.strings)

        if 'contact' in entry_dict:
            entry_dict['contact'] = entry_dict['contact'].replace('View Other Addresses', '').strip()

        entry_dict['categories'] = categories
        return_dict.append(entry_dict)

    return return_dict


##
# process_page will perform a retrieval on a specific page and format the output
#
# @param url The url of the form to process
# @param control_href The href of the control we're sending as the event target
# @param discard_data Determine if we throw away the data or process it
#
# @return The text of the processed request

def process_page(url, page_number, discard_data=False):
    global last_view_state
    global last_validation
    global last_view_generator

    req = retrieve(url, "POST", generate_body(page_number))
    last_view_state     = urllib.quote(get_asp_resp_var(req.text, "__VIEWSTATE"))
    last_validation     = urllib.quote(get_asp_resp_var(req.text, "__EVENTVALIDATION"))
    last_view_generator = urllib.quote(get_asp_resp_var(req.text, "__VIEWSTATEGENERATOR"))

    if discard_data:
        return req.text

    table = get_result_table(req.text)
    for tr in table.find_all('tr'):
        tds = tr.find_all('td')

        if len(tds) == 2:
            a = tds[0].find('a')
            details = process_details(url, a['href'])
            primary = {'firm': tds[0].text.strip(),
                            'all_jurisdictions': tds[1].text.strip(),
                            'sample_date': datetime.datetime.now().isoformat(),
                            'source_url': url_start}

            if len(details) > 0:
                for detail in details:
                    categories = detail.pop('categories', [])

                    if len(categories) > 0:
                        for category in categories:
                            print json.dumps(dict(primary.items() + detail.items() + category.items()))
                    else:
                        print json.dumps(dict(primary.items() + detail.items()))

            else:
                print json.dumps(primary)

    return req.text


##
# process_pages will iterate over the pages in the form, stopping when we've processed all rows or when the application
# state is invalid.
#
# @param url The url of the form to process

def process_pages(url):
    record_count = None
    page_number = 1

    while record_count is None or (page_number * 100 - 100) < record_count:
        turbotlib.log("Requesting rows %d - %d" % ((page_number * 100 - 100), (page_number * 100)))

        # Strage behavior on server: first call returns page 1 results but page must be > 1 to not get null resp
        # However, not a problem and subsequent calls work as expected.
        response_text = process_page(url, 2 if page_number == 1 else page_number)

        # Ensure the number of records haven't changed during run
        check_count = get_record_count(response_text)
        if record_count is not None and record_count != check_count:
            raise Exception("The data set changed during a load, we need a re-run.")
        else:
            record_count = check_count

        if not record_count > 0:
            raise Exception("The data set is empty.")

        page_number += 1

    turbotlib.log("Run finished!")

# ----------------------------------------------------------------------------------------------------------------------

turbotlib.log("Starting run...")
turbotlib.log("Getting initial view state...")
init_req      = retrieve(url_start, "GET", "")
document = BeautifulSoup(init_req.text)
last_view_state     = urllib.quote(document.find(id='__VIEWSTATE')['value'])
last_validation     = urllib.quote(document.find(id='__EVENTVALIDATION')['value'])
last_view_generator = urllib.quote(document.find(id='__VIEWSTATEGENERATOR')['value'])
process_page(url_start, 1, True) # first request returns junk data, discard it
process_pages(url_start)