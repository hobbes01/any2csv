import argparse
import os
import zipfile
import csv
import re
from datetime import datetime

import pandas as pd

from google.protobuf import text_format # Or use binary parsing

from snapshot_pb2 import SnapshotWithType
from models_pb2 import RelationFormat

from pprint import pprint

# Parse command line
parser=argparse.ArgumentParser(
        prog="any2csv.py",
        description="Prototype Any Protobuf to CSV")
parser.add_argument('filepath',
                    help='path of export file to be converted to CSV')
parser.add_argument('-d', '--debug',
                    help='turn debug on',
                    action='store_true')
parser.add_argument('-t', '--types',
                    help='dump specific type to CSV.')
parser.add_argument('-f', '--fields',
                    help='dump specific fields to CSV.')
    
args=parser.parse_args()
debug = args.debug
if debug:
    print(args.filepath, args.debug, args.types, args.fields)

# Extract archive and prepare workspace
pbfile = args.filepath
dump_all = True if args.types is None else False 
dump_types = args.types.split(',') if args.types is not None else None
dump_fields = args.fields.split(',') if args.fields is not None else None

workdir = os.path.dirname(pbfile)
basename = os.path.splitext(os.path.basename(pbfile))[0]
pbdir = workdir+'/'+basename
if debug:
    print(workdir, basename, pbdir)

try:
    if not zipfile.is_zipfile(pbfile):
        print('No Zip file provided?')
        exit()
    else:
        with zipfile.ZipFile(pbfile, mode="r") as archive:
            if debug:
                archive.printdir()
            archive.extractall(pbdir)
except Exception as err:
    print(err, 'No Zip file provided?')
    exit()

csvdir = pbdir+'/csv/'
try:
    os.mkdir(csvdir)
except FileExistsError:
    pass

datadir = pbdir+'/data/'
try:
    os.mkdir(datadir)
except FileExistsError:
    pass

# Loads messages in '*.pb' files belonging to an Anytype export
def load_single_message_from_file(filepath):
    message = SnapshotWithType()
    try:
        with open(filepath, "rb") as f: # "rb" for read binary
            message.ParseFromString(f.read())
        return message
    except IOError as e:
        #print(f"Error reading or parsing file: {e}")
        return None
    except Exception as e: # Catch other parsing errors
        print(f"Error parsing protobuf message: {e}")
        return None

# Helper to read relation options
def read_rel_option(option):
    if option == "":
        return ""

    msg = None

    filepath = pbdir + "/relationsOptions/" + option + ".pb"
    try:
        msg = load_single_message_from_file(filepath)
    except Exception as e:
        pass

    if msg is None:
        filepath = pbdir + "/objects/" + option + ".pb"
        try:
            msg = load_single_message_from_file(filepath)
        except Exception as e:
            pass

    if msg is None:
        filepath = pbdir + "/relations/" + option + ".pb"
        try:
            msg = load_single_message_from_file(filepath)
        except Exception as e:
            pass

    if msg is None:
        filepath = pbdir + "/types/" + option + ".pb"
        try:
            msg = load_single_message_from_file(filepath)
        except Exception as e:
            pass

    if msg is None:
        unknown_options[option] = unknown_options.get(option, 0) + 1
        return ""

    return(msg.snapshot.data.details.fields['name'].string_value)

# Read a piece of object data
def read_data(proto_data, field):
    found = 0
    fld = proto_data.snapshot.data.details.fields[field]
    fld_format = None
    
    for i in proto_data.snapshot.data.relationLinks:
        if i.key == field:
            fld_format = i.format
            found = 1
            break
    
    if not found:
        return ""

    match fld_format:
        case RelationFormat.longtext | RelationFormat.shorttext:
            return fld.string_value
        case RelationFormat.number:
            return (fld.number_value)
        case RelationFormat.status | RelationFormat.tag:
            ret = ""
            for i in fld.list_value.values:
                if ret != "":
                    ret+= ', ' + read_rel_option(i.string_value)
                else:
                    ret = read_rel_option(i.string_value)
            return ret
        case RelationFormat.object:
            ret = ""
            if fld.list_value.values:
                for i in fld.list_value.values:
                    if ret != "":
                        ret+= ', ' + read_rel_option(i.string_value)
                    else:
                        ret = read_rel_option(i.string_value)
            else:
                ret = read_rel_option(fld.string_value)
            return ret
        case RelationFormat.date:
            return datetime.fromtimestamp(fld.number_value)
        case RelationFormat.file:
            return fld.string_value
        case RelationFormat.checkbox:
            return fld.bool_value
        case RelationFormat.url:
            return fld.string_value
        case RelationFormat.email:
            return fld.string_value
        case RelationFormat.phone:
            return fld.string_value
        case RelationFormat.emoji:
            return fld.string_value
        case RelationFormat.relations:
            return fld.string_value
        case _:
            unknown_types[field] = unknown_types.get(field, 0) + 1

            return ""

def proto_to_csv(proto_data_list, csv_file_path, types_to_extract, fields_to_extract):
    delimiter = '|'
    df = pd.DataFrame()
    if 'Object type' in my_cache['relations'].keys():
        objtyperel = my_cache['relations']['Object type']
    else:
        print('Couldn\'t find Object type information - aborting specific type extraction.')
        return

    for proto_data in proto_data_list:
        if types_to_extract is not None:
            if read_data(proto_data, objtyperel) not in types_to_extract:
                continue

        row = {}
        fld = proto_data.snapshot.data.details.fields

        for i in fld.keys():
            if i in my_cache['revrel'].keys():
                key = my_cache['revrel'][i]
            else:
                key = i
            if fields_to_extract is not None:
                if key not in fields_to_extract:
                    continue
            row[key] = read_data(proto_data, i)
        rowdf = pd.DataFrame.from_dict([row])
        dftmp = pd.concat([ df, rowdf], sort=True)
        df = dftmp
    df.to_csv(csv_file_path, sep=delimiter)

def dump_data(objdir):
    m = []
    for root, dirs, files in os.walk(objdir):
        for file in files:
            if regex.match(file):
                msg = load_single_message_from_file(objdir+file)
                if debug:
                    with open(datadir + file + ".data", "w") as f:
                        f.write(str(msg))
                m.append(msg)
    return m

# Look at *.pb files only
regex = re.compile('(.*pb$)')

unknown_options = {}
unknown_types = {}

# Gather available types / relations / objects
# in dictionnaries
# eg. { [pb file basename, typename], ... }
objdir = ""
my_cache = {}
for obj in [ 'types', 'relations', 'objects' ]:
    dict = {}
    dict2 = {}
    objdir = pbdir + "/" + obj + "/"
    for root, dirs, files in os.walk(objdir):
        for file in files:
            if regex.match(file):
                msg = load_single_message_from_file(objdir+file)
                match obj:
                    case 'types' | 'objects':
                        dict[file[0:-3]] = msg.snapshot.data.details.fields['name'].string_value
                    case 'relations':
                        # keys are Field name, values are Field protobuf name (relation)
                        dict[msg.snapshot.data.details.fields['name'].string_value] = msg.snapshot.data.key
                        # keys are Field protobuf name (relation), values are Field name
                        dict2[msg.snapshot.data.key] = msg.snapshot.data.details.fields['name'].string_value 
    match obj:
        case 'types' | 'objects':
            my_cache[obj] = dict
        case 'relations':
            my_cache[obj] = dict
            my_cache['revrel'] = dict2

# Available [ 'objects', 'relations', 'relationsOptions', 'templates', 'types' ]:
# types = [ 'Activity' ]
# fields = [ 'name', 'tag', 'Duration', 'Cost' ]
# Work on objects
if dump_types is not None:
    proto_to_csv(messages, csvdir+'output-'+domain+'-types.csv', dump_types, dump_fields)

if dump_all:
    proto_to_csv(messages, csvdir+'output-'+domain+'-all.csv', None, None)

if debug:
    # Dump readabme protobuf files
    for domain in [ 'types', 'relations', 'objects']:
       messages = []
       objdir = pbdir + "/" + domain + "/"
       messages = dump_data(objdir)

    # Dump references that were not resolved
    # (object or type not exported for example)
    print("Unknown types:")
    for ut in unknown_types.keys():
        print(ut, unknown_types[ut])

    print("Unknown options:")
    for uo in unknown_options.keys():
        print(uo, unknown_options[uo])
    
