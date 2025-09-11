import argparse
import os
import re
from datetime import datetime
from any2csv_utils import (
    extract_archive,
    ensure_directories,
    load_single_message_from_file,
    proto_to_csv,
    dump_data,
    build_cache
)

def main():
    """
    Entry point for the any2csv tool.
    Parses command-line arguments, extracts the archive, prepares directories, loads messages,
    and converts protobuf data to CSV based on user options.
    """
    parser = argparse.ArgumentParser(
        prog="any2csv.py",
        description="Prototype Any Protobuf to CSV"
    )
    parser.add_argument('filepath', help='path of export file to be converted to CSV')
    parser.add_argument('-d', '--debug', help='turn debug on', action='store_true')
    parser.add_argument('-t', '--types', help='dump specific type to CSV.')
    parser.add_argument('-f', '--fields', help='dump specific fields to CSV.')

    args = parser.parse_args()
    debug = args.debug

    pbfile = args.filepath
    dump_types = args.types.split(',') if args.types else None
    dump_fields = args.fields.split(',') if args.fields else None

    workdir = os.path.dirname(pbfile)
    basename = os.path.splitext(os.path.basename(pbfile))[0]
    pbdir = os.path.join(workdir, basename)

    try:
        extract_archive(pbfile, pbdir, debug=debug)
    except Exception as err:
        print(err)
        exit(1)

    csvdir, datadir = ensure_directories(pbdir)

    regex = re.compile(r'(.*pb$)')
    unknown_options = {}
    unknown_types = {}

    my_cache = build_cache(pbdir, regex, load_single_message_from_file)

    # Gather proto messages to export
    objdir = os.path.join(pbdir, 'objects')
    messages = []
    for root, dirs, files in os.walk(objdir):
        for file in files:
            if regex.match(file):
                msg = load_single_message_from_file(os.path.join(objdir, file))
                if msg is not None:
                    messages.append(msg)

    outtime = datetime.now().strftime("%Y%m%d-%H%M%S")
    proto_to_csv(messages, os.path.join(csvdir, f'any2csv-output-{outtime}.csv'),
                 dump_types, dump_fields, my_cache, pbdir, unknown_types, unknown_options)

    if debug:
        for domain in ['types', 'relations']:
            messages = []
            objdir = os.path.join(pbdir, domain)
            messages = dump_data(objdir, regex, datadir, debug, load_single_message_from_file)

        print("Unknown types:")
        for ut in unknown_types.keys():
            print(ut, unknown_types[ut])

        print("Unknown options:")
        for uo in unknown_options.keys():
            print(uo, unknown_options[uo])

if __name__ == "__main__":
    main()
