import json
import glob
import datetime
import os

json_files = glob.glob('[12][09][0-9][0-9]/*/*.json')

all_data = []
for file_path in json_files:
    try:
        if os.path.basename(file_path) == 'cve.json':
            continue
        with open(file_path, encoding='utf-8') as f:
            all_data.append(json.load(f))
    except Exception as e:
        print(f'Error reading {file_path}: {e}')

all_data_sorted = sorted(all_data, key=lambda x: len(x.keys()), reverse=True)

today = datetime.datetime.now().strftime('%Y%m%d')

output_file_dated = f'all_cve_{today}.json'
with open(output_file_dated, 'w', encoding='utf-8') as out_dated:
    json.dump(all_data_sorted, out_dated, ensure_ascii=False, separators=(',', ':'))

output_file_all = 'vulnrichment_all.json'
with open(output_file_all, 'w', encoding='utf-8') as out_all:
    json.dump(all_data_sorted, out_all, ensure_ascii=False, separators=(',', ':'))
