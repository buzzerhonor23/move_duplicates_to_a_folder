#!/usr/bin/env python
import sys
import os
import hashlib
import shutil
from datetime import datetime


def chunk_reader(fobj, chunk_size=51200):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, first_chunk_only=False, hash=hashlib.sha1):
    hashobj = hash()
    file_object = open(filename, 'rb')

    if first_chunk_only:
        hashobj.update(file_object.read(2048))
    else:
        for chunk in chunk_reader(file_object):
            # hashobj.update(chunk)
            hashobj.update(file_object.read(51200))
            # hashobj.update(file_object.read(10240))
            # hashobj.update(file_object.read(5120))
    hashed = hashobj.digest()

    file_object.close()
    return hashed


def check_for_duplicates(paths, hash=hashlib.sha1):
    hashes_by_size = {}
    hashes_by_size_count = 0
    hashes_by_size_duplicate_count = 0
    hashes_on_1k = {}
    hashes_on_1k_count = 0
    hashes_full = {}
    hashes_full_count = 0
    keyword = 'AAE'
    now = datetime.now()
    # logfile = "/volume1/Family Photos/" + now.strftime("%H_%M_%S") + ".txt"
    logfile = "/home/rc/Pictures/" + now.strftime("%H_%M_%S") + ".txt"
    file = open(logfile, "w")
    
    starttime = "StartTime: " + now.strftime("%m/%d/%Y, %H:%M:%S") + "\n"
    file.write(starttime)
    file.flush()
    
    FilesToBeDeleted = {}
    FilesToBeDeleted["delete"] = []

    for path in paths:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)

                if "@eaDir" in full_path:
                    continue
                elif "@SynoEAStream" in full_path:
                    continue
                else:
                    pass

                try:
                    # if the target is a symlink (soft one), this will 
                    # dereference it - change the value to the actual target file
                    full_path = os.path.realpath(full_path)
                    file_size = os.path.getsize(full_path)
                except (OSError,):
                    # not accessible (permissions, etc) - pass on
                    continue

                if file_size < 1:
                    continue
                
                duplicate = hashes_by_size.get(file_size)

                if duplicate:
                    hashes_by_size[file_size].append(full_path)
                    hashes_by_size_duplicate_count += 1
                else:
                    hashes_by_size[file_size] = []  # create the list for this file size
                    hashes_by_size[file_size].append(full_path)
                    hashes_by_size_count += 1

    now = datetime.now()
    endtime = "Time: " + now.strftime("%m/%d/%Y, %H:%M:%S") + "\n"
    file.write(endtime)
    file.write("Total files : %s Duplicates by file size: %s \n" % (hashes_by_size_count, hashes_by_size_duplicate_count) )
    file.flush()

    # For all files with the same file size, get their hash on the 1st 1024 bytes
    for __, files in hashes_by_size.items():
        if len(files) < 2:
            continue    # this file size is unique, no need to spend cpy cycles on it

        for filename in files:
            try:
                small_hash = get_hash(filename, first_chunk_only=True)
            except (OSError,):
                # the file access might've changed till the exec point got here 
                continue

            duplicate = hashes_on_1k.get(small_hash)
            if duplicate:
                hashes_on_1k[small_hash].append(filename)
                hashes_on_1k_count += 1
            else:
                hashes_on_1k[small_hash] = []          # create the list for this 1k hash
                hashes_on_1k[small_hash].append(filename)
                
    now = datetime.now()
    endtime = "Time: " + now.strftime("%m/%d/%Y, %H:%M:%S") + "\n"
    file.write(endtime)
    file.write("Total files : %s Duplicates by file size: %s, first Hash: %s \n" % (hashes_by_size_count, hashes_by_size_duplicate_count, hashes_on_1k_count) )
    file.flush()
    
    # For all files with the hash on the 1st 1024 bytes, get their hash on the full file - collisions will be duplicates
    for __, files in hashes_on_1k.items():
        if len(files) < 2:
            continue    # this hash of fist 1k file bytes is unique, no need to spend cpy cycles on it

        for filename in files:
            try: 
                full_hash = get_hash(filename, first_chunk_only=False)
            except (OSError,):
                # the file access might've changed till the exec point got here 
                continue

            duplicate = hashes_full.get(full_hash)
            if duplicate:
                hashes_full_count += 1
                file.write("Duplicate found: %s and %s \n" % (filename, duplicate))
                if "AmazonBackup" in filename:
                    FilesToBeDeleted["delete"].append(filename)
                elif "AmazonBackup" in duplicate:
                    FilesToBeDeleted["delete"].append(duplicate)
                else:
                    FilesToBeDeleted["delete"].append(duplicate)
            else:
                hashes_full[full_hash] = filename
    
    now = datetime.now()
    endtime = "Time: " + now.strftime("%m/%d/%Y, %H:%M:%S") + "\n"
    file.write(endtime)
    
    print("Total files : %s Duplicates by file size: %s, first Hash: %s, second Hash: %s \n" % (hashes_by_size_count, hashes_by_size_duplicate_count, hashes_on_1k_count, hashes_full_count)) 
    file.write("Total files : %s Duplicates by file size: %s, first Hash: %s, second Hash: %s \n" % (hashes_by_size_count, hashes_by_size_duplicate_count, hashes_on_1k_count, hashes_full_count) )
    
    for __, files in FilesToBeDeleted.items():
        for filename in files:
            try: 
                shutil.move(filename, "/home/rc/Pictures/duplicate/" + os.path.basename(filename))
                file.write("Moved %s \n" % (filename))
            except (OSError, IOError):
                continue
    
    now = datetime.now()
    endtime = "Time: " + now.strftime("%m/%d/%Y, %H:%M:%S") + "\n"
    file.write(endtime)
    
if sys.argv[1:]:
    check_for_duplicates(sys.argv[1:])
else:
    print("Please pass the paths to check as parameters to the script")