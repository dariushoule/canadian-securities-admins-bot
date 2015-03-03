import sys
import json
from datetime import datetime
import turbotlib


def date_formatter(date):
    if len(date) <= 0 or date is None:
        return ''

    try:
        return datetime.strptime(date, "%B %d, %Y").isoformat()[:-9]
    except:
        turbotlib.log("Failure parsing date: " + date)
        return ''

while True:
    line = sys.stdin.readline()
    if not line:
        break
    raw_record = json.loads(line)

    licence_record = {
        "company_name": raw_record.get('firm', 'Unknown'),
        "company_jurisdiction": raw_record.get('jurisdiction', 'Unknown'),
        "source_url": raw_record['source_url'],
        "sample_date": raw_record['sample_date'],
        "status": raw_record.get('status', 'Unknown'),
        "jurisdiction_classification": raw_record.get('category', 'Unknown'),
        "category": 'Financial',
        "confidence": 'HIGH',
        "start_date": date_formatter(raw_record.get('from', '')),
        "end_date": date_formatter(raw_record.get('to', ''))
    }

    print json.dumps(licence_record)