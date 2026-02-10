import re
import time

def extract(record):
    updates = []
    # Extract dates from the record's line field
    if "line" in record:
        line = record["line"]
        date_pattern = re.compile(r'(\b\d{4}[-/]\d{2}[-/]\d{2}\b|\b\d{2}[-/]\d{2}[-/]\d{4}\b)')
        dates = date_pattern.findall(line)

        mostRecentYear = None
        mostRecentMonth = None
        mostRecentDay = None
    
        for date in dates:
            try:
                if '-' in date:
                    parts = date.split('-')
                else:
                    parts = date.split('/')
                
                if len(parts[0]) == 4:  # Format: YYYY-MM-DD
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                else:  # Format: MM-DD-YYYY
                    month, day, year = int(parts[0]), int(parts[1]), int(parts[2])

                if (mostRecentYear is None or year > mostRecentYear or
                    (year == mostRecentYear and month > mostRecentMonth) or
                    (year == mostRecentYear and month == mostRecentMonth and day > mostRecentDay)):
                    mostRecentYear = year
                    mostRecentMonth = month
                    mostRecentDay = day
            except ValueError:
                continue  # Skip invalid date formats
            except ValueError:
                continue  # Skip invalid date formats

            if mostRecentYear is not None:
                updates.append({"recencyYear": mostRecentYear})
            if mostRecentMonth is not None:
                updates.append({"recencyMonth": mostRecentMonth})
            if mostRecentDay is not None:
                updates.append({"recencyDay": mostRecentDay})

        return updates