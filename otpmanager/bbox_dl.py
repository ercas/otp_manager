#!/usr/bin/env python3

import datetime
import glob
import json
import multiprocessing
import os
import requests
import sys
import tempfile

# Describes whether or not existing files will be overwritten
OVERWRITE = True

# How many threads to use to download GTFS feeds in parallel (use 1 for no
# multiprocessing)
THREADS = 4

def save_file(url, output_path, live_output = True, overwrite = OVERWRITE,
              desired_extension = None):
    """ Save a URL to a file

    Args:
        url: A string containing the URL to be saved.
        output_path: A string containing the path that the file will be saved
            to.
        live_output: A bool describing whether or not the amount of data
            transferred will be shown in real time.
        overwrite: A bool describing whether or not existing files will be
            overwritten.

    Returns:
        True if the download succeeds, False if the download fails.
    """

    print(url)
    try:
        response = requests.get(url, stream = True)
    except:
        print("=> Download failed: %s" % url)
        return False
    if (response.status_code == 200):

        # urls with a forward slash at the end might not have a valid file name
        # assigned by transitland_dl
        if (os.path.isdir(output_path)):
            output_path += "untitled"

        if (overwrite):
            if (os.path.exists(output_path)):
                print("Overwriting existing file")
        else:
            # Bruteforce for a unique filename - append .n until x.n is
            # available, where x is the desired output path and n is an
            # incrementing integer
            orig_path_list = output_path.split(".")
            i = 1

            while (os.path.exists(output_path)):
                orig_path = output_path
                output_path = ".".join(
                    orig_path_list[:-1] + [str(i), orig_path_list[-1]]
                )
                print("%s exists; renaming to %s" % (
                    orig_path, output_path
                ))
                i += 1

        try:
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size = 512):
                    if (chunk):
                        f.write(chunk)
                        if (live_output):
                            sys.stdout.write("\r=> %s (%dkb)" % (output_path,
                                                                 f.tell()/1024))
                            sys.stdout.flush()
                sys.stdout.write("\r=> %s (%dkb)" % (output_path,
                                                     f.tell()/1024))
                sys.stdout.flush()
            print("")

            if (desired_extension is not None):
                if (not output_path.endswith(desired_extension)):
                    desired_output_path = "%s.%s" % (
                        output_path, desired_extension
                    )
                    print("Renaming: %s -> %s" % (
                        output_path, desired_output_path
                    ))
                    os.rename(output_path, desired_output_path)

            return True
        except Exception as err:
            print("")
            print("=> Error: %s (%s)" % (err, url))


    print("=> Download failed: %s" % url)
    return False

def save_file_mp_wrapper(task):
    """ Wrapper for save_file

    Passes arguments into save_file
    """
    success = save_file(**task["args"])
    if (success):
        tempfile.mkstemp(dir = task["counter_dir"])

def transitland_dl(output_directory, left, bottom, right, top, dryrun = False):
    """ Simple interface for the the transit.land

    Downloads all GTFS feeds for a given bounding box using the transit.land
    Datastore API

    Args:
        output_directory: A string describing the path that downloaded GTFS
            feeds will be saved to.
        left: A float representing the leftmost part of the bounding box.
        bottom: A float representing the bottommost part of the bounding box.
        right: A float representing the rightmost part of the bounding box.
        top: A float representing the topmost part of the bounding box.
        dryrun: A bool describing whether or not the URL should be printed but
            not downloaded. Nothing is returned if dryrun is True.

    Returns:
        The number of GTFS feeds downloaded, or False if none were.
    """
    url = "https://transit.land/api/v1/feeds?bbox=%f,%f,%f,%f" % (left, bottom,
                                                                  right, top)
    print("Querying transit.land for feeds:")
    print(url)
    response = requests.get(url)
    if (response.status_code == 200):
        data = json.loads(response.content.decode())

        # There should be at least 1 feed
        if (len(data["feeds"]) > 0):
            downloaded_feeds = 0
            if (dryrun):
                print("\n".join(data["feeds"]))
                downloaded_feeds = len(data["feeds"])
            else:
                if (not os.path.isdir(output_directory)):
                    os.mkdir(output_directory)

                # Use multiprocessing to download GTFS feeds in parallel if
                # THREADS > 1
                if (THREADS > 1):
                    print("\nUsing multiprocessing: %d parallel downloads\n" %
                          THREADS)
                    # Hacky way of tracking completed downloads: create a temporary
                    # directoy in /tmp/ and add files to it when downloads finish
                    # successfully; we can find how many downloads finished
                    # successfully by counting the number of items in this directory
                    counter_dir = tempfile.TemporaryDirectory(prefix = "bbox_dl")
                    tasks = [{
                        "counter_dir": counter_dir.name,
                        "args": {
                            "url": feed["url"],
                            "output_path": "%s/%s" % (
                                output_directory,
                                feed["url"].split("/")[-1]
                            )
                        },
                        "live_output": False,
                        "desired_extension": "zip"
                    } for feed in data["feeds"]]

                    pool = multiprocessing.Pool(THREADS)
                    pool.map(save_file_mp_wrapper, tasks)
                    pool.close()
                    pool.join()

                    downloaded_feeds = len(os.listdir(counter_dir.name))
                    counter_dir.cleanup()

                # Single threaded
                else:
                    for feed in data["feeds"]:
                        success = save_file(
                            url = feed["url"],
                            output_path = "%s/%s" % (
                                output_directory,
                                feed["url"].split("/")[-1]
                            ),
                            desired_extension = "zip"
                        )
                        if (success):
                            downloaded_feeds += 1

            if (downloaded_feeds > 0):
                return downloaded_feeds

    print("=> Failed")
    return False

def overpass_dl(output_path, left, bottom, right, top, ways_only = False,
                min_size = 10e3, dryrun = False):
    """ Simple interface for the OpenStreetMap Overpass API

    Constructs and download an exported subset of the OSM planet using the
    Overpass API (https://wiki.openstreetmap.org/wiki/Overpass_API)

    Args:
        output_path: A string describing the path that the exported .osm file
            will be written to.
        left: A float representing the leftmost part of the bounding box.
        bottom: A float representing the bottommost part of the bounding box.
        right: A float representing the rightmost part of the bounding box.
        top: A float representing the topmost part of the bounding box.
        ways_only: A bool describing whether or not to download an OSM file
            containing only nodes used in ways, a.k.a no points of interest
        min_size: A number describing the minimum expected size of an OSM file.
            The OSM download will be considered failed if the OSM file is less
            than this many bytes in size.
        dryrun: A bool describing whether or not the URL should be printed but
            not downloaded. Nothing is returned if dryrun is True.

    Returns:
        True if the .osm was successfully downloaded; False if it was not.
    """

    if (ways_only):
        url = ("http://overpass.osm.rambler.ru/cgi/interpreter?data=way[\"highway\"]"
               "(%f,%f,%f,%f);(._;>;);out;" % (bottom, left, top, right))
    else:
        url = ("https://overpass-api.de/api/map?bbox=%f,%f,%f,%f" % 
               (left,bottom,right,top))
    if (dryrun):
        print(url)
    else:
        if (save_file(url, output_path)):
            if (os.stat(output_path).st_size < min_size):
                print("Downloaded OSM file is smaller than %s bytes" % min_size)
                return False
            else:
                return True
