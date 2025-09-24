import argparse
import os
import time
import datetime
from anytype2csv_utils import (
    extract_archive,
    build_csv
)

def main():
    """
    Entry point for the anytype2csv tool.
    Parses command-line arguments, extracts the archive, prepares directories, loads messages,
    and converts protobuf data to CSV based on user options.
    """
    os.environ['TZ'] = 'Europe/Brussels'
    time.tzset()

    parser = argparse.ArgumentParser(
        prog="anytype2csv.py",
        description="Prototype Any Protobuf to CSV"
    )
    parser.add_argument('filepath', help='path of export file to be converted to CSV')
    parser.add_argument('-d', '--debug', help='turn debug on', action='store_true')
    parser.add_argument('-t', '--types', help='dump specific type to CSV.')
    parser.add_argument('-f', '--fields', help='dump specific fields to CSV.')

    args = parser.parse_args()
    debug = args.debug

    pbfile = args.filepath
    workdir = os.path.dirname(pbfile)
    basename = os.path.splitext(os.path.basename(pbfile))[0]
    pbdir = os.path.join(workdir, basename)

    dump_types = args.types.split(',') if args.types else None
    dump_fields = args.fields.split(',') if args.fields else None

    try:
        extract_archive(pbfile, pbdir, debug=debug)
    except Exception as err:
        print(err)
        exit(1)

    build_csv(pbdir, dump_types, dump_fields, debug)

if __name__ == "__main__":
    main()
