import os
import zipfile
import re
from datetime import datetime

import pandas as pd

from google.protobuf import text_format  # Or use binary parsing

from snapshot_pb2 import SnapshotWithType
from models_pb2 import RelationFormat

def extract_archive(pbfile, pbdir, debug=False):
    if not zipfile.is_zipfile(pbfile):
        raise ValueError("No Zip file provided?")
    with zipfile.ZipFile(pbfile, mode="r") as archive:
        if debug:
            archive.printdir()
        archive.extractall(pbdir)

def ensure_directories(pbdir):
    csvdir = os.path.join(pbdir, 'csv')
    datadir = os.path.join(pbdir, 'data')
    os.makedirs(csvdir, exist_ok=True)
    os.makedirs(datadir, exist_ok=True)
    return csvdir, datadir

def load_single_message_from_file(filepath):
    message = SnapshotWithType()
    try:
        with open(filepath, "rb") as f:
            message.ParseFromString(f.read())
        return message
    except IOError:
        return None
    except Exception as e:
        print(f"Error parsing protobuf message: {e}")
        return None

def read_rel_option(option, pbdir, unknown_options, load_single_message_from_file):
    if option == "":
        return ""
    msg = None
    search_paths = [
        os.path.join(pbdir, "relationsOptions", f"{option}.pb"),
        os.path.join(pbdir, "objects", f"{option}.pb"),
        os.path.join(pbdir, "relations", f"{option}.pb"),
        os.path.join(pbdir, "types", f"{option}.pb"),
    ]
    for filepath in search_paths:
        msg = load_single_message_from_file(filepath)
        if msg is not None:
            break
    if msg is None:
        unknown_options[option] = unknown_options.get(option, 0) + 1
        return ""
    return msg.snapshot.data.details.fields['name'].string_value

def read_data(proto_data, field, pbdir, unknown_types, unknown_options, my_cache):
    found = False
    fld = proto_data.snapshot.data.details.fields[field]
    fld_format = None

    for i in proto_data.snapshot.data.relationLinks:
        if i.key == field:
            fld_format = i.format
            found = True
            break

    if not found:
        return ""

    match fld_format:
        case RelationFormat.longtext | RelationFormat.shorttext:
            return fld.string_value
        case RelationFormat.number:
            return fld.number_value
        case RelationFormat.status | RelationFormat.tag:
            ret = ""
            for i in fld.list_value.values:
                if ret != "":
                    ret += ', ' + read_rel_option(i.string_value, pbdir, unknown_options, load_single_message_from_file)
                else:
                    ret = read_rel_option(i.string_value, pbdir, unknown_options, load_single_message_from_file)
            return ret
        case RelationFormat.object:
            ret = ""
            if fld.list_value.values:
                for i in fld.list_value.values:
                    if ret != "":
                        ret += ', ' + read_rel_option(i.string_value, pbdir, unknown_options, load_single_message_from_file)
                    else:
                        ret = read_rel_option(i.string_value, pbdir, unknown_options, load_single_message_from_file)
            else:
                ret = read_rel_option(fld.string_value, pbdir, unknown_options, load_single_message_from_file)
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

def proto_to_csv(proto_data_list, csv_file_path, types_to_extract, fields_to_extract, my_cache, pbdir, unknown_types, unknown_options):
    delimiter = '|'
    df = pd.DataFrame()
    if 'Object type' in my_cache['relations'].keys():
        objtyperel = my_cache['relations']['Object type']
    else:
        print('Couldn\'t find Object type information - aborting specific type extraction.')
        return

    for proto_data in proto_data_list:
        if types_to_extract is not None:
            if read_data(proto_data, objtyperel, pbdir, unknown_types, unknown_options, my_cache) not in types_to_extract:
                continue

        row = {}
        fld = proto_data.snapshot.data.details.fields

        for i in fld.keys():
            key = my_cache['revrel'].get(i, i)
            if fields_to_extract is not None and key not in fields_to_extract:
                continue
            row[key] = read_data(proto_data, i, pbdir, unknown_types, unknown_options, my_cache)
        rowdf = pd.DataFrame.from_dict([row])
        df = pd.concat([df, rowdf], sort=True)
    df.to_csv(csv_file_path, sep=delimiter)

def dump_data(objdir, regex, datadir, debug, load_single_message_from_file):
    m = []
    for root, dirs, files in os.walk(objdir):
        for file in files:
            if regex.match(file):
                msg = load_single_message_from_file(os.path.join(objdir, file))
                if debug:
                    with open(os.path.join(datadir, f"{file}.data"), "w") as f:
                        f.write(str(msg))
                m.append(msg)
    return m

def build_cache(pbdir, regex, load_single_message_from_file):
    my_cache = {}
    for obj in ['types', 'relations', 'objects']:
        dict1 = {}
        dict2 = {}
        objdir = os.path.join(pbdir, obj)
        for root, dirs, files in os.walk(objdir):
            for file in files:
                if regex.match(file):
                    msg = load_single_message_from_file(os.path.join(objdir, file))
                    match obj:
                        case 'types' | 'objects':
                            dict1[file[0:-3]] = msg.snapshot.data.details.fields['name'].string_value
                        case 'relations':
                            dict1[msg.snapshot.data.details.fields['name'].string_value] = msg.snapshot.data.key
                            dict2[msg.snapshot.data.key] = msg.snapshot.data.details.fields['name'].string_value
        match obj:
            case 'types' | 'objects':
                my_cache[obj] = dict1
            case 'relations':
                my_cache[obj] = dict1
                my_cache['revrel'] = dict2
    return my_cache
