import argparse
from collections import defaultdict
from datetime import (
    datetime,
    timedelta,
)
from inflection import (
    camelize,
    titleize,
)
import logging
import os
import shutil
import sys
from tabulate import tabulate
import urllib.request
from zipfile import ZipFile

LOG = logging.getLogger(__name__)

RECORD_TYPES = [
    'header',
    'amateur',
    'entity',
    'vanity',
]

FILENAME_TEMPLATE = 'a_am_{}.zip'
DAYS = [
    'mon',
    'tue',
    'wed',
    'thu',
    'fri',
    'sat',
    'sun',
]

class People:
    def __init__(self):
        self.people = defaultdict(Person)

    @classmethod
    def from_folder(cls, pathname):
        people = People()
        for record_type in RECORD_TYPES:
            people.load_records(pathname, record_type)
        return people

    def load_records(self, pathname, record_type):
        cls = globals()[camelize(record_type)]
        for record in cls.load_file(pathname):
            self.people[record.unique_system_identifier].add_record(record_type,
                record)

    def __str__(self):
        return ', '.join(self.records.keys())

class Person:
    def __init__(self):
        self.records = {}

    def add_record(self, record_type, record):
        self.records[record_type] = record

    @property
    def id(self):
        return self.unique_system_identifier

    def __getattr__(self, attr):
        DEFAULT = '-'
        val = None
        for _, record in self.records.items():
            if hasattr(record, attr):
                val = getattr(record, attr)
                if not val:
                    continue
                if attr in record.__class__.TRANSFORMS:
                    val = record.__class__.TRANSFORMS[attr](val)
                break
        if val is None:
            return DEFAULT
        return val

class Record:
    # Overwritten by children
    RECORD_TYPE = []
    FIELDS = []
    TRANSFORMS = {}

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def from_line(cls, line):
        fields = {f: v for f, v in zip(cls.FIELDS, line.split('|'))}
        return cls(**fields)

    @classmethod
    def load_file(cls, pathname):
        path = os.path.join(pathname, '{}.dat'.format(cls.RECORD_TYPE))
        with open(path, 'r') as f:
            records = []
            for line in f.read().split('\n'):
                if line:
                    records.append(cls.from_line(line))
            return records

class Header(Record):
    RECORD_TYPE = 'HD'
    FIELDS = [
        'record_type',
        'unique_system_identifier',
        'uls_file_number',
        'ebf_number',
        'call_sign',
        'license_status',
        'radio_service_code',
        'grant_date',
        'expired_date',
        'cancellation_date',
        'eligibility_rule_num',
        'reserved',
        'alien',
        'alien_government',
        'alien_corporation',
        'alien_officer',
        'alien_control',
        'revoked',
        'convicted',
        'adjudged',
        'reserved',
        'common_carrier',
        'non_common_carrier',
        'private_comm',
        'fixed',
        'mobile',
        'radiolocation',
        'satellite',
        'test',
        'interconnected_service',
        'certifier_first_name',
        'certifier_mi',
        'certifier_last_name',
        'certifier_suffix',
        'certifier_title',
        'female',
        'black',
        'native_american',
        'hawaiian',
        'asian',
        'white',
        'hispanic',
        'effective_date',
        'last_action_date',
        'auction_id',
        'broadcast_services',
        'band_manager',
        'broadcast_services',
        'alien_ruling',
        'licensee_name_change',
    ]

class Amateur(Record):
    RECORD_TYPE = 'AM'
    FIELDS = [
        'record_type',
        'unique_system_identifier',
        'uls_file_number',
        'ebf_number',
        'call_sign',
        'operator_class',
        'group_code',
        'region_code',
        'trustee_call_sign',
        'trustee_indicator',
        'physician_certification',
        've_signature',
        'systematic_call_sign_change',
        'vanity_call_sign_change',
        'vanity_relationship',
        'previous_call_sign',
        'previous_operator_class',
        'trustee_name',
    ]

    def __str__(self):
        return self.operator_class

class Entity(Record):
    RECORD_TYPE = 'EN'
    FIELDS = [
        'record_type',
        'unique_system_identifier',
        'uls_file_number',
        'ebf_number',
        'call_sign',
        'entity_type',
        'licensee_id',
        'entity_name',
        'first_name',
        'mi',
        'last_name',
        'suffix',
        'phone',
        'fax',
        'email',
        'street_address',
        'city',
        'state',
        'zip_code',
        'po_box',
        'attention_line',
        'sgin',
        'fCC_registration_number',
        'applicant_type_code',
        'applicant_type_code_other',
        'status_code',
        'status_date',
    ]
    TRANSFORMS = {
        'entity_name': titleize,
        'first_name': titleize,
        'last_name': titleize,
    }

    def __str__(self):
        return self.entity_name

class Vanity(Record):
    RECORD_TYPE = 'VC'
    FIELDS = [
        'record_type',
        'unique_system_identifier',
        'uls_file_number',
        'ebf_number',
        'order_of_preference',
        'requested_call_sign',
    ]

def build_fcc_link(filename):
    return 'http://wireless.fcc.gov/uls/data/daily/{}'.format(filename)

def download_fcc_data(link, filename):
    urllib.request.urlretrieve(link, filename)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='log more verbosely'
    )

    subparsers = parser.add_subparsers()

    print_parser = subparsers.add_parser(
        'print',
        help='Print FCC additions for given --day'
    )
    print_parser.add_argument(
        '--day',
        choices=range(7),
        type=int,
        help='day of week (0 is Monday, 6 is Sunday)'
    )
    print_parser.add_argument(
        '--operator-class',
        choices=['T', 'G', 'A', 'E'],
        help='specific operator class to search for'
    )
    print_parser.set_defaults(function=print_data)

    clean_parser = subparsers.add_parser(
        'clean',
        help='Clean downloaded data'
    )
    clean_parser.set_defaults(function=clean)

    return parser.parse_args()

def print_data(args):
    day = (datetime.now() - timedelta(days=1)).weekday()
    if args.day is not None:
        day = args.day
    filename = FILENAME_TEMPLATE.format(DAYS[day])
    link = build_fcc_link(filename)

    download_fcc_data(link, filename)

    zipfile = ZipFile(filename)
    if len(zipfile.namelist()) <= 1:
        print('Incomplete zipfile retrieved from FCC (probably need to go back '
            'a day)', file=sys.stderr)
        sys.exit(1)

    zipfile.extractall(filename.replace('.zip', ''))

    attributes = [
        'entity_name',
        'call_sign',
        'operator_class',
    ]
    people = People.from_folder(filename.replace('.zip', ''))
    data = []
    for _, person in people.people.items():
        if args.operator_class and person.operator_class != args.operator_class:
            continue
        row = []
        for attr in attributes:
            row.append(getattr(person, attr))
        data.append(row)
    print(tabulate(data, headers=attributes))

def clean(args):
    for filename in [FILENAME_TEMPLATE.format(d) for d in DAYS]:
        if os.path.isfile(filename):
            LOG.debug('Deleting {}'.format(filename))
            os.remove(filename)
        dirname = filename.replace('.zip', '')
        if os.path.isdir(dirname):
            LOG.debug('Deleting {}'.format(dirname))
            shutil.rmtree(dirname)
    LOG.info('Cleaned successfully')

def main():
    args = parse_args()
    LOG.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    args.function(args)


if __name__ == '__main__':
    main()
