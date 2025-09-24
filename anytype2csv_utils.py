import os
import zipfile
import re
from datetime import datetime

import pandas as pd

from google.protobuf import text_format  # Or use binary parsing

from snapshot_pb2 import SnapshotWithType
from models_pb2 import RelationFormat

def extract_archive(pbfile, pbdir, debug=False):
    """
    Extracts the provided zip archive containing protobuf export files to a target directory.

    Args:
        pbfile (str): Path to the zip file to extract.
        pbdir (str): Directory where the archive should be extracted.
        debug (bool, optional): If True, prints archive contents. Defaults to False.

    Raises:
        ValueError: If the provided file is not a zip file.
    """
    if not zipfile.is_zipfile(pbfile):
        raise ValueError("No Zip file provided?")
    with zipfile.ZipFile(pbfile, mode="r") as archive:
        if debug:
            archive.printdir()
        archive.extractall(pbdir)

def ensure_directories(pbdir):
    """
    Ensures the output and data directories exist inside the given workspace directory.

    Args:
        pbdir (str): Path to the workspace directory.

    Returns:
        tuple[str, str]: Paths to the csv and data directories.
    """
    csvdir = os.path.join(pbdir, 'csv')
    datadir = os.path.join(pbdir, 'data')
    os.makedirs(csvdir, exist_ok=True)
    os.makedirs(datadir, exist_ok=True)
    return csvdir, datadir

def load_single_message_from_file(filepath):
    """
    Loads a protobuf message from a binary file.

    Args:
        filepath (str): Path to the protobuf file.

    Returns:
        SnapshotWithType | None: The parsed message, or None if reading/parsing failed.
    """
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

def read_rel_option(option, pbdir, unknown_options):
    """
    Resolves a relation option to its human-readable name by searching in several directories.

    Args:
        option (str): The key of the relation option.
        pbdir (str): Path to the workspace directory.
        unknown_options (dict): Dict to track unknown options.
        load_single_message_from_file (Callable): Function to load message from file.

    Returns:
        str: The resolved option name, or an empty string if not found.
    """
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
    """
    Reads and interprets the value of a specific field from a protobuf data object.

    Args:
        proto_data: The protobuf data object.
        field (str): The protobuf field name.
        pbdir (str): Path to the workspace directory.
        unknown_types (dict): Dict to track unknown field types.
        unknown_options (dict): Dict to track unknown options.
        my_cache (dict): Cache of relation/type mappings.

    Returns:
        Any: The interpreted value of the field, type depends on the relation format.
    """
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
                    ret += ', ' + read_rel_option(i.string_value, pbdir, unknown_options)
                else:
                    ret = read_rel_option(i.string_value, pbdir, unknown_options)
            return ret
        case RelationFormat.object:
            ret = ""
            if fld.list_value.values:
                for i in fld.list_value.values:
                    if ret != "":
                        ret += ', ' + read_rel_option(i.string_value, pbdir, unknown_options)
                    else:
                        ret = read_rel_option(i.string_value, pbdir, unknown_options)
            else:
                ret = read_rel_option(fld.string_value, pbdir, unknown_options)
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
    """
    Converts a list of protobuf data objects to a CSV file.

    Args:
        proto_data_list (list): List of loaded protobuf data messages.
        csv_file_path (str): Path to output CSV file.
        types_to_extract (list | None): List of types to extract, or None for all.
        fields_to_extract (list | None): List of fields to extract, or None for all.
        my_cache (dict): Cache of relation/type mappings.
        pbdir (str): Path to workspace directory.
        unknown_types (dict): Dict to track unknown field types.
        unknown_options (dict): Dict to track unknown options.
    """
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
    #df.sort_values('Creation date', inplace=True)
    df.to_csv(csv_file_path, sep=delimiter)

def dump_data(objdir, regex, datadir, debug):
    """
    Loads all protobuf messages from a directory matching a filename regex.

    Args:
        objdir (str): Directory to scan for protobuf files.
        regex (re.Pattern): Compiled regex pattern for file matching.
        datadir (str): Directory for debug output (optional).
        debug (bool): If True, writes debug info to files.
        load_single_message_from_file (Callable): Function to load message from file.

    Returns:
        list: List of loaded protobuf messages.
    """
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

def build_cache(pbdir, regex):
    """
    Builds a cache of mappings for types, relations, and objects in the workspace.

    Args:
        pbdir (str): Path to the workspace directory.
        regex (re.Pattern): Compiled regex pattern for file matching.
        load_single_message_from_file (Callable): Function to load message from file.

    Returns:
        dict: Cache dictionary containing mappings for 'types', 'relations', 'objects', and 'revrel'.
    """
    my_cache = {}
    for obj in ['types', 'relations', 'objects']:
        dict1 = {}
        dict2 = {}
        objdir = os.path.join(pbdir, obj)
        for root, dirs, files in os.walk(objdir):
            dirs.sort()
            for file in sorted(files):
                if regex.match(file):
                    msg = load_single_message_from_file(os.path.join(objdir, file))
                    match obj:
                        case 'types' | 'objects':
                            dict1[file[0:-3]] = msg.snapshot.data.details.fields['name'].string_value
                        case 'relations':
                            # Strangely enough, relations with the same name can exist...
                            i=1
                            name0 = msg.snapshot.data.details.fields['name'].string_value
                            name = name0
                            while name in dict1:
                                name = name0 + str(i)
                                i+=1
                            dict1[name] = msg.snapshot.data.key
                            dict2[msg.snapshot.data.key] = name
        match obj:
            case 'types' | 'objects':
                my_cache[obj] = dict1
            case 'relations':
                my_cache[obj] = dict1
                my_cache['revrel'] = dict2
    return my_cache

def build_csv(pbdir, dump_types, dump_fields, debug):
    unknown_options = {}
    unknown_types = {}

    csvdir, datadir = ensure_directories(pbdir)

    regex = re.compile(r'(.*pb$)')

    my_cache = build_cache(pbdir, regex)

    # Gather proto messages to export
    objdir = os.path.join(pbdir, 'objects')
    messages = []
    for root, dirs, files in os.walk(objdir):
        dirs.sort()
        for file in sorted(files):
            if regex.match(file):
                msg = load_single_message_from_file(os.path.join(objdir, file))
                if msg is not None:
                    messages.append(msg)

    outtime = datetime.now().strftime("%Y%m%d-%H%M%S")
    outcsv = os.path.join(csvdir, f'anytype2csv-output-{outtime}.csv')
    proto_to_csv(messages, outcsv,
                 dump_types, dump_fields, my_cache, pbdir, unknown_types, unknown_options)

    if debug:
        for domain in ['types', 'relations', 'objects']:
            messages = []
            objdir = os.path.join(pbdir, domain)
            messages = dump_data(objdir, regex, datadir, debug)

        print("Unknown types:")
        for ut in unknown_types.keys():
            print(ut, unknown_types[ut])

        print("Unknown options:")
        for uo in unknown_options.keys():
            print(uo, unknown_options[uo])

    return outcsv
