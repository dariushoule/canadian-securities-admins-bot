import sys
import json

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
        "start_date": raw_record.get('from', ''),
        "end_date": raw_record.get('to', '')
    }

    print json.dumps(licence_record)