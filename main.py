# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import argparse
import ast
import io
import json
import os
import sys
import tarfile
from json import JSONDecodeError

import requests as requests

API_KEY = ''
URL = ''
MUSICBRAINZ_DUMP_URL = ''
MUSICBRAINZ_DUMP_TIMESTAMP = ''


def download_extract(url, compressed_file, output):
    print(f'Downloading {compressed_file}...')
    res = requests.get(url, stream=True)
    res.raise_for_status()

    with open(compressed_file, mode='wb') as cf:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                cf.write(chunk)

    with tarfile.open(compressed_file, mode='r') as tar:
        print(f'Extracting {output} from {compressed_file}...')
        tar.extract(output, path=MUSICBRAINZ_DUMP_TIMESTAMP)


def mb_extract(mb_entity):
    global MUSICBRAINZ_DUMP_TIMESTAMP
    global MUSICBRAINZ_DUMP_URL

    if not os.path.exists(MUSICBRAINZ_DUMP_TIMESTAMP):
        os.mkdir(MUSICBRAINZ_DUMP_TIMESTAMP)

    download_extract(f'{MUSICBRAINZ_DUMP_URL}/{mb_entity}.tar.xz', f'{MUSICBRAINZ_DUMP_TIMESTAMP}/{mb_entity}.tar.xz', f'mbdump/{mb_entity}')
    with open(f'{MUSICBRAINZ_DUMP_TIMESTAMP}/mbdump/{mb_entity}', mode='r', encoding='utf-8') as dump:
        for line in dump:
            yield line


def main():
    parser = argparse.ArgumentParser(
        prog='MusicBrainz Database Extractor bot for Artmetadata Project',
        description='This program downloads and extracts part or the entire MusicBrainz Database and imports it to '
                    'Artmetadata Project '
    )
    parser.add_argument('-i', '--url')
    parser.add_argument('-a', '--api-key')
    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    global API_KEY
    global URL
    global MUSICBRAINZ_DUMP_URL
    global MUSICBRAINZ_DUMP_TIMESTAMP

    API_KEY = args.api_key if args.api_key else os.environ['ARTMETADATA_API_KEY']
    URL = args.url if args.url else os.environ['ARTMETADATA_URL']
    MUSICBRAINZ_DUMP_URL = os.environ.setdefault('MUSICBRAINZ_DUMP_URL', 'http://ftp.musicbrainz.org/pub/musicbrainz/data/json-dumps/')

    # Verificar dump mais recente
    if 'MUSICBRAINZ_DUMP_TIMESTAMP' not in os.environ:
        print('Timestamp not set. Checking latest dump...')
        MUSICBRAINZ_DUMP_TIMESTAMP = requests.get(f'{MUSICBRAINZ_DUMP_URL}LATEST').text.rstrip()
        print('Most recent dump is:', MUSICBRAINZ_DUMP_TIMESTAMP)
    else:
        MUSICBRAINZ_DUMP_TIMESTAMP = os.environ['MUSICBRAINZ_DUMP_TIMESTAMP']

    MUSICBRAINZ_DUMP_URL += MUSICBRAINZ_DUMP_TIMESTAMP

    mb_extract('area')
    mb_extract('artist')
    mb_extract('event')
    mb_extract('instrument')
    mb_extract('label')
    mb_extract('place')
    mb_extract('recording')
    mb_extract('release-group')
    mb_extract('release')
    mb_extract('series')
    mb_extract('work')



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
