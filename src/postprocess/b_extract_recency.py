from utils.logs import get_logger
from utils.regex import DATE_PATTERN_REGEX
import re, time

logger = get_logger(__name__)

currentYear = time.localtime().tm_year
currentMonth = time.localtime().tm_mon

def extract(record):
    if "line" in record:
        line = record["line"]
        dates = DATE_PATTERN_REGEX.findall(line)

        mostRecentYear = None
        mostRecentMonth = None
        mostRecentDay = None

        for date in dates:
            try:
                if '-' in date:
                    parts = date.split('-')
                else:
                    parts = date.split('/')
                
                if len(parts[0]) == 4:
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                else:
                    month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
                
                if year > currentYear or (year == currentYear and month > currentMonth) or year < 2000:
                    continue  # Skip bad dates

                if mostRecentYear is None:
                    mostRecentYear = year
                if mostRecentMonth is None:
                    mostRecentMonth = month
                if mostRecentDay is None:
                    mostRecentDay = day

                if (mostRecentYear is None or year > mostRecentYear or
                    (year == mostRecentYear and month > mostRecentMonth) or
                    (year == mostRecentYear and month == mostRecentMonth and day > mostRecentDay)):
                    mostRecentYear = year
                    mostRecentMonth = month
                    mostRecentDay = day
            except ValueError:
                continue

            if mostRecentYear is not None:
                record["recencyYear"] = mostRecentYear
            if mostRecentMonth is not None:
                record["recencyMonth"] = mostRecentMonth
            if mostRecentDay is not None:
                record["recencyDay"] = mostRecentDay

    return record