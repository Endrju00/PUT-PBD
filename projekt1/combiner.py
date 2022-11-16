#!/usr/bin/env python3
import sys

current_key = None
current_count = 0

for line in sys.stdin:
    month, pu_id, passenger_count = line.split('\t')
    key = (month, pu_id)
    passenger_count = int(passenger_count)

    if current_key == key:
        current_count += passenger_count
    else:
        if current_key:
            print(f'{current_key[0]}\t{current_key[1]}\t{current_count}')
        current_count = passenger_count
        current_key = key

if current_key == key:
    print(f'{current_key[0]}\t{current_key[1]}\t{current_count}')
