#!/usr/bin/env python3
import sys

DATE = 1
PASSENGER_CONT = 3
PU_LOC_ID = 7
PAYMENT_TYPE = 9

for line in sys.stdin:
    if not line:
        continue

    values = line.split(',')

    try:
        year, month, _ = values[DATE].split('-')
    except ValueError:
        continue
    except IndexError:
        continue

    passenger_count = values[PASSENGER_CONT]
    pu_location_id = values[PU_LOC_ID]
    payment_type = values[PAYMENT_TYPE]

    if payment_type == '2':
        print(f'{year}-{month}\t{pu_location_id}\t{passenger_count}')
