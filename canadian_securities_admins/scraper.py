# -*- coding: utf-8 -*-

import json
import datetime
import os
import turbotlib
import requests
import time
import re
import urllib
import sqlite3
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
url_start = "http://www.securities-administrators.ca/nrs/nrsearchResult.aspx?ID=1325"
broken_rows_regex = re.compile(r'<div id="ctl[0-9]+_bodyContent_dlstFirmLocations_ctl[0-9]+_rptCategories_ctl[0-9]+_pnlRevocationDate">(.*?</div>.*?)</div>', re.DOTALL)
broken_ind_rows_regex = re.compile(r'<div id="ctl[0-9]+_bodyContent_dlstIndLocations_ctl[0-9]+_dlstIndFirms_ctl[0-9]+_rptCategories_ctl[0-9]+_pnlRevocationDate">', re.DOTALL)


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

    try:
        return BeautifulSoup(match.group(0))
    except:
        return None


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
# Query for an individual in the cache
#
# @param name The individual's name
# @param jurisdiction The jurisdiction of the firm
# @param firm The name of the firm
#
# @return A user row

def get_individual(name, jurisdiction, firm):
    query = "SELECT * FROM individuals WHERE jurisdiction=? AND name=? AND firm=?"
    return usersDB.execute(query, (jurisdiction, name, firm)).fetchone()


##
# Retrieve the company roster (historical inclusive) for a firm, stores in cache
#
# @param link The link containing the firm's postback
# @param url The seed url
# @param individuals_view_state The previous viewstate to work off of
# @param name The firm name

def get_and_store_individuals_for_firm(link, url, individuals_view_state, name):
    control_id = urllib.quote(link['href'].replace("javascript:__doPostBack('", '').replace("','')", ''))
    individual_details_req = retrieve(url, "POST", generate_body_control(control_id, individuals_view_state))

    if "ctl00_bodyContent_lbtnShowIndHistorical" in individual_details_req.text:
        individuals_history_view_state = {'view'      : urllib.quote(get_asp_resp_var(individual_details_req.text, "__VIEWSTATE")),
                                          'validation': urllib.quote(get_asp_resp_var(individual_details_req.text, "__EVENTVALIDATION")),
                                          'generator' : urllib.quote(get_asp_resp_var(individual_details_req.text, "__VIEWSTATEGENERATOR"))}

        history_req = retrieve(url, "POST", generate_body_control("ctl00%24bodyContent%24lbtnShowIndHistorical", individuals_history_view_state))
    else:
        history_req = None
        history_entries = []

    resp_markup = get_details_div(individual_details_req.text)
    locations_entries = resp_markup.select("#ctl00_bodyContent_dlstIndLocations > tr > td")

    if history_req is not None:

        # Fix super broken tables on history responses
        history_text = re.sub(broken_ind_rows_regex, r"", history_req.text)

        history_markup = get_details_div(history_text)
        if history_markup is not None:
            history_entries = history_markup.select("#ctl00_bodyContent_dlstIndLocations > tr > td")

    for entry in (locations_entries + history_entries):
        entry_dict = {'name': name, 'jurisdiction': entry.select('.sectiontitle > span')[0].text.strip()}

        locations_table = entry.select('tbody')
        locations_rows = locations_table[0].find_all("tr", recursive=False)

        categories = []
        for row in locations_rows:
            field = row.select('th > span') or row.select('th')
            if len(field) > 0:
                field = field[0].text.strip()

                if field == "Firm":
                    entry_dict['firm'] = row.find('td').text.strip()

                elif field == "Category":
                    categories.append({'category': row.find('td').text.strip()})

                elif field == "From":
                    categories[-1]['from']   = row.find('td').text.strip()

                elif field == "To":
                    categories[-1]['to']     = row.find('td').text.strip()

                elif field == "Status":
                    categories[-1]['status'] = row.find('td').text.strip()

                elif field == "Terms & Conditions":
                    entry_dict['terms']     = row.select('td > span')[0].text.strip()

                elif field == "Contact Information":
                    entry_dict['contact']    = ""
                    for contact in row.select('td table td'):
                        entry_dict['contact'] += "\n" + "\n".join([x.strip() for x in contact.strings])

        if 'contact' in entry_dict:
            entry_dict['contact'] = re.sub('\s+', ' ', entry_dict['contact'].replace('View Other Addresses', '')).strip()

        entry_dict['categories'] = categories

        usersDB.execute("INSERT INTO individuals (jurisdiction, name, firm, terms, contact, categories) values (?, ?, ?, ?, ?, ?) ",
               (entry_dict['jurisdiction'],
                entry_dict['name'],
                entry_dict['firm'],
                entry_dict['terms'] if 'terms' in entry_dict else '',
                entry_dict['contact'] if 'contact' in entry_dict else '',
                json.dumps(entry_dict['categories']) if 'categories' in entry_dict else ''))

    usersDB.commit()


##
# get_registered_individuals will lookup all individuals belonging to a firm and their current/historical license data
#
# @param url The url of the form to process
# @param control_href The details link href
# @param view_state The previous view state to work off of
# @param firm_jurisdiction The jurisdiction of the individual's firm
# @param firm_name The name of the invdividual's firm
#
# @return A dictionary containing its locations and associated data
def get_registered_individuals(url, control_href, view_state, firm_jurisdiction, firm_name):
    return_array = []
    turbotlib.log("Retrieving individuals for current or historical firm: " + firm_name + " in: " + firm_jurisdiction)

    control_id = urllib.quote(control_href.replace("javascript:__doPostBack('", '').replace("','')", ''))
    individuals_page_req = retrieve(url, "POST", generate_body_control(control_id, view_state))

    if "Your search returned no records, please try searching again" in individuals_page_req.text:
        return []

    num_individuals = get_record_count(individuals_page_req.text)
    processed_individuals = 0
    last_processed_individuals = 0
    ind_page = 1

    while True:
        individuals_view_state = {'view'      : urllib.quote(get_asp_resp_var(individuals_page_req.text, "__VIEWSTATE")),
                                  'validation': urllib.quote(get_asp_resp_var(individuals_page_req.text, "__EVENTVALIDATION")),
                                  'generator' : urllib.quote(get_asp_resp_var(individuals_page_req.text, "__VIEWSTATEGENERATOR"))}

        individual_links = BeautifulSoup(individuals_page_req.text).select('tr > td > a')
        for link in individual_links:
            try:
                if "lbtnIndDetail" not in link['href']:
                    continue
            except:
                continue

            processed_individuals += 1

            name = link.text.strip()
            individual_dict = get_individual(name, firm_jurisdiction, firm_name)

            if individual_dict is None:
                get_and_store_individuals_for_firm(link, url, individuals_view_state, name)

                individual_dict = get_individual(name, firm_jurisdiction, firm_name)
                if individual_dict is not None:
                    return_array.append(individual_dict)

            else:
                return_array.append(individual_dict)

        if processed_individuals < num_individuals:
            if last_processed_individuals == processed_individuals:
                turbotlib.log('Warning: broke out of possible infinite loop trying to retrieve all individuals for firm.')
                break

            ind_page += 1
            control_id = urllib.quote('ctl00$bodyContent$lbtnPager{0}'.format(ind_page))
            individuals_page_req = retrieve(url, "POST", generate_body_control(control_id, individuals_view_state))

            last_processed_individuals = processed_individuals
        else:
            break

    return return_array


##
# process_details will perform the href action on a firm link to retrieve its details.
#
# @param url The url of the form to process
# @param control_href The details link href
# @param firm_name The name of the firm
#
# @return A dictionary containing its locations and associated data

def process_details(url, control_href, firm_name):
    return_dict = {'entries': [], 'historical_names': ''}

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

        # Store history view state
        history_view_state = {'view'      : urllib.quote(get_asp_resp_var(history_req.text, "__VIEWSTATE")),
                              'validation': urllib.quote(get_asp_resp_var(history_req.text, "__EVENTVALIDATION")),
                              'generator' : urllib.quote(get_asp_resp_var(history_req.text, "__VIEWSTATEGENERATOR"))}

        # Check for previous names of the company
        old_names = history_markup.select('#ctl00_bodyContent_pnlFirmOtherNames td')
        for name_row in old_names:
            if "Previous Name:" not in name_row.text:
                return_dict['historical_names'] += name_row.text.strip() + "\n\n"

    for entry in (locations_entries + history_entries):
        entry_dict = {'jurisdiction': entry.select('.sectiontitle > span')[0].text.strip()}
        locations_table = entry.find('table', recursive=False).find('tbody', recursive=False)
        locations_rows = locations_table.find_all("tr", recursive=False)

        categories = []
        for row in locations_rows:

            # retrieve registered and permitted individuals
            potential_permitted_links = row.select('span > a')
            if len(potential_permitted_links) > 0:
                for link in potential_permitted_links:
                    if "Registered and Permitted Individuals" in link.text:
                        referring_view_state = detail_view_state
                        if entry in history_entries:
                            referring_view_state = history_view_state

                        entry_dict['individuals'] = get_registered_individuals(url, link['href'], referring_view_state, entry_dict['jurisdiction'], firm_name)
                        break


            # retrieve key value style data
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
                    entry_dict['terms']     = row.select('td > span')[0].text.strip()

                elif field == "Contact Information":
                    entry_dict['contact']    = ""
                    for contact in row.select('td table td'):
                        entry_dict['contact'] += "\n" + "\n".join([x.strip() for x in contact.strings])

        if 'contact' in entry_dict:
            entry_dict['contact'] = re.sub('\s+', ' ', entry_dict['contact'].replace('View Other Addresses', '')).strip()

        entry_dict['categories'] = categories
        return_dict['entries'].append(entry_dict)

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

    records = []

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
            firm_name = tds[0].text.strip()
            firm_information = process_details(url, a['href'], firm_name)
            details = firm_information['entries']

            primary = {'firm': firm_name,
                            'all_jurisdictions': tds[1].text.strip(),
                            'sample_date': datetime.datetime.now().isoformat(),
                            'source_url': url_start,
                            'historical_names': firm_information['historical_names']}

            if len(details) > 0:
                for detail in details:
                    categories = detail.pop('categories', [])

                    if len(categories) > 0:
                        for category in categories:
                            records.append(json.dumps(dict(primary.items() + detail.items() + category.items())))
                    else:
                        records.append(json.dumps(dict(primary.items() + detail.items())))

            else:
                records.append(json.dumps(primary))

    with open('%s/records.dump' % turbotlib.data_dir(), "a") as dump:
        for record in records:
            print record
            dump.write(record)
        dump.close()

    return req.text


##
# reset_state will erase and in-progress databases / record files and reset the internal page counter to zero

def reset_state():
    turbotlib.save_var("page", 1)
    turbotlib.save_var("check_count", None)

    try:
        os.remove('%s/records.dump' % turbotlib.data_dir())
    except:
        pass

    try:
        os.remove('%s/individuals.db' % turbotlib.data_dir())
    except:
        pass


##
# process_pages will iterate over the pages in the form, stopping when we've processed all rows or when the application
# state is invalid.
#
# @param url The url of the form to process

def process_pages(url):

    # Attempt to resume if we can
    try:
        page_number = turbotlib.get_var("page")
        record_count = turbotlib.get_var("check_count")
    except KeyError:
        page_number = 1
        record_count = None

    if page_number > 1:
        turbotlib.log("Resuming run from page {0}".format(page_number))

        with open('%s/records.dump' % turbotlib.data_dir(), "r") as dump:
            for record in dump:
                print record
            dump.close()

    # iterate over whole or remaining data set
    while record_count is None or (page_number * 100 - 100) < record_count:
        turbotlib.log("Requesting rows %d - %d" % ((page_number * 100 - 100), (page_number * 100)))

        # Strange behavior on server: first call returns page 1 results but page must be > 1 to not get null resp
        # However, not a problem and subsequent calls work as expected.
        response_text = process_page(url, 2 if page_number == 1 else page_number)

        # Ensure the number of records haven't changed during run
        check_count = get_record_count(response_text)
        turbotlib.save_var("check_count", check_count)
        if record_count is not None and record_count != check_count:
            reset_state()
            raise Exception("The data set changed during parsing, we need a re-run.")
        else:
            record_count = check_count

        if not record_count > 0:
            raise Exception("The data set is empty.")

        page_number += 1
        turbotlib.save_var("page", page_number)

    turbotlib.log("Run finished!")
    reset_state()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# ----------------------------------------------------------------------------------------------------------------------

turbotlib.log("Starting run...")

# create individuals cache
usersDB = sqlite3.connect('%s/individuals.db' % turbotlib.data_dir())
usersDB.row_factory = dict_factory
usersDB.execute("CREATE TABLE IF NOT EXISTS individuals(jurisdiction, name, firm, terms, contact, categories)")
usersDB.commit()

turbotlib.log("Getting initial view state...")
init_req      = retrieve(url_start, "GET", "")
document = BeautifulSoup(init_req.text)

last_view_state     = urllib.quote(document.find(id='__VIEWSTATE')['value'])
last_validation     = urllib.quote(document.find(id='__EVENTVALIDATION')['value'])
last_view_generator = urllib.quote(document.find(id='__VIEWSTATEGENERATOR')['value'])

process_page(url_start, 1, True) # first request returns junk data, discard it
process_pages(url_start)
usersDB.close()