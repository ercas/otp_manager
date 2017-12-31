#!/usr/bin/env python3

import atexit
import contextlib
import datetime
import glob
import json
import os
import socket
import subprocess
import time
import zipfile

from . import bbox_dl

# Array containing ports that can be used by this script
DEFAULT_PORT_ALLOCATION_RANGE = range(8100, 8200)

# Characters to be replaced by "_"
ILLEGAL_CHARACTERS = ["(", ")", "?"]

# Name of the file to be stored in each graph directory containing info about
# otpmanager's build progress
CONFIG_FILENAME = "otpmanager.json"

DEFAULT_PORT = 8080

# Directory to store graphs in
DEFAULT_GRAPH_ROOT_DIR = "graphs/"

# How long between STDOUT messages during startup before the process is
# considered to be dead
DEFAULT_TIMEOUT = 600

def log_name(label):
    """ Create a timestamped log filename given a label """

    return "%s_%s.log" % (label, datetime.datetime.now().isoformat())

def remove_illegal_characters(string):
    """ Remove illegal file path characters from a string """
    for character in ILLEGAL_CHARACTERS:
        string = string.replace(character, "_")
    return string

def port_available(port):
    """ Find if a port is in use

    From http://stackoverflow.com/a/35370008

    Args:
        port: The port to be checked.

    Returns:
        True if the port is available; False if it is in use.
    """

    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        if sock.connect_ex(("localhost", port)) == 0:
            return False
        else:
            return True

def find_ports(port_range, num_ports = 2):
    """ Find available ports in the given range

    Args:
        port_range: The range of possible ports to use.
        num_ports: The number of ports that must be found and returned.

    Returns:
        An integer of the first available port or False if all ports are in use.

    """
    results = []
    for port in port_range:
        if (port_available(port)):
            results.append(port)
        if (len(results) == num_ports):
            return results
    return False

def print_wide(string, columns = 80, padding = "="):
    """ Print a string taking up the number of columns specified

    Args:
        string: The string to be printed.
        columns: The number of columns to take up.
        padding: The character to be used to pad the empty columns.
    """
    before = "%s %s " % (padding * 2, string)
    print("%s%s" % (before, padding * (columns - len(before))))

class JavaManager(object):
    """ Generic class containing functions to assist in the setting up,
    starting, and stopping of Java-based routing engines

    Attributes:
        graph_name: A string containing the name of the graph being worked
            on.
        using_gtfs: A bool describing whether or not GTFS feeds have been
            loaded. If require_gtfs is specified, this will always be True.
        bbox: A tuple containing the leftmost, bottommost, rightmost, and
            topmost coordinates.
        proc: The process running the routing engine, if self.start runs and
            succeeds; None otherwise.
        log: The current file being used to soak up the routing engine's STDOUT
            and STDERR, or None if the routing engine is not currently running.
        port: The port that the routing engine is serving HTTP on, self.start
            runs and succeeds.
    """

    def __init__(self, graph_name, left, bottom, right, top,
                 jar_path, graph_root_dir = DEFAULT_GRAPH_ROOT_DIR):
        """ Initializes JavaManager class

        Args:
            graph_name: The name of the graph, to be stored in GRAPH_ROOT_DIR.
                connections.
            left: A floating point of the leftmost coordinate.
            bottom: A floating point of the bottommost coordinate.
            right: A floating point of the rightmost coordinate.
            top: A floating point of the topmost coordinate.
            jar_path: The path to the routing manager's jar file.
            graph_root_dir: The path to store all graphs in.
        """

        self.graph_root_dir = remove_illegal_characters(graph_root_dir)
        self.graph_name = remove_illegal_characters(graph_name)
        self.using_gtfs = False
        self.bbox = (left, bottom, right, top)
        self.jar_path = jar_path

        self.proc = None
        self.proc_output = None
        self.graph_subdir = None
        self.graph_config_path = None
        self.graph_config = None

    def write_config(self):
        with open(self.graph_config_path, "w") as f:
            json.dump(self.graph_config, f)

    def monitor_proc(self, listeners = [], show_output = True,
                    timeout = DEFAULT_TIMEOUT):
        """ Monitor the running process

        Monitor the output of a running process and perform actions if
        necessary.

        Args:
            listeners: A list of dictionaries, structured as such:
                {
                    "substring": A substring that triggers this listener to
                        fire. For example, if substring is "ERROR", then this
                        listener would fire whenever "ERROR" appears in the
                        output of the process.
                    "return_value": The value to be returned when this listener
                        fires.
                    "kill": A bool telling whether or not to kill the process
                        when this listener fires.
                    "callback": A function that will be fired when this
                        listener fires. (OPTIONAL)
                }
            show_output: A bool telling whether or not output should be
                written to STDOUT.
            timeout: The process will be killed if it produces no output in
                this amount of time. This can be set to False if there should
                be no timeout.

        Returns:
            False on timeout or the value of listener["return_value"]
            otherwise.

        """

        last_activity = time.time()

        with open(self.proc_output, "r") as otp_output:
            while (self.proc is not None):
                line = otp_output.readline().rstrip()

                if (len(line) > 0):
                    last_activity = time.time()

                    if (show_output):
                        print(">> %s" % line)

                    for listener in listeners:
                        if (listener["substring"] in line):
                            if ("kill_otp" in listener):
                                if (listener["kill_otp"]):
                                    time.sleep(1)
                                    self.terminate()
                            if ("return_value" in listener):
                                return listener["return_value"]
                            if ("callback" in listener):
                                listener["callback"]()
                            return

                else:
                    if (timeout is not False):
                        if (time.time() - last_activity > timeout):
                            print("\nKilling OTP; no stdout/stderr activity "
                                  "in last %d seconds" % timeout)
                            self.terminate()
                            return False

                    time.sleep(0.1)

            print("Terminating monitor loop...")

    def download_osm(self, output_dir, **overpass_dl_kwargs):
        """ Wrapper for bbox_dl.overpass_dl

        Args:
            output_dir: A string containing the path to the directory to store
                the OSM file in.

        Returns:
            True if the OSM file was downloaded; False if otherwise.
        """

        osm = bbox_dl.overpass_dl(
            "%s/map-%s.osm" % (
                output_dir,
                datetime.datetime.now().isoformat()
            ),
            *self.bbox,
            **overpass_dl_kwargs
        )
        print("Downloaded OSM: %s" % str(osm))

        if (osm is not False):
            return True

        return False

    def download_gtfs(self, output_dir):
        """ Wrapper for bbox_dl.transitland_dl

        Args:
            output_dir: A string containing the path to the directory to store
                the GTFS feeds in.

        Returns:
            True if at least one GTFS feed was downloaded; returns False
            otherwise.
        """

        gtfs = bbox_dl.transitland_dl("%s" % output_dir, *self.bbox)
        print("Downloaded GTFS: %s" % str(gtfs))

        if (gtfs is not False):
            return True

        return False

    def terminate(self, *dummy_args, **dummy_kwargs):
        """ Terminate the running process """

        if (self.proc is not None):
            print("Killing process %d" % self.proc.pid)
            self.proc.kill()
            self.proc = None
        else:
            print("No running process to kill")

    def setup_graph_init(self):
        """ Stage 1 of setup: create graph subdirectories and config file

        Returns:
            True on successful completion
        """

        self.graph_subdir = "%s/%s/" % (self.graph_root_dir, self.graph_name)

        if (not os.path.exists(self.graph_root_dir)):
            os.mkdir(self.graph_root_dir)
        if (not os.path.exists(self.graph_subdir)):
            os.mkdir(self.graph_subdir)

        self.graph_config_path = "%s/%s" % (self.graph_subdir, CONFIG_FILENAME)
        if (os.path.exists(self.graph_config_path)):
            with open(self.graph_config_path, "r") as f:
                self.graph_config = json.load(f)
        else:
            self.graph_config = {
                "osm_download_time": False,
                "gtfs_download_time": False,
                "otp_graph_build_time": False,
                "gh_graph_build_time": False
            }

        return True

    def setup_download_data(self, ways_only, min_osm_size, require_gtfs):
        """ Stage 2 of setup: download data

        Args:
            require_gtfs: If True, then returns False if no GTFS files were
                found

        Returns:
            True on successful completion
        """

        print_wide("Downloading OSM from Overpass API")
        if (not self.graph_config["osm_download_time"]):
            if (self.download_osm(
                self.graph_subdir,
                ways_only = ways_only,
                min_size = min_osm_size
            )):
                self.graph_config["osm_download_time"] = datetime.datetime.now().isoformat()
                self.write_config()
            else:
                print("OSM downloading failed")
                return False
        else:
            print("OSM already downloaded")

        print_wide("Downloading GTFS feeds")
        if (not self.graph_config["gtfs_download_time"]):
            if (self.download_gtfs(self.graph_subdir)):
                self.using_gtfs = True
                self.graph_config["gtfs_download_time"] = datetime.datetime.now().isoformat()
                self.write_config()
            else:
                self.using_gtfs = False
                print("GTFS downloading failed")

                if (require_gtfs):
                    return False
                else:
                    print("Resuming anyway")
        else:
            print("GTFS already downloaded")
        print("")

        return True

    def start(self, port = DEFAULT_PORT, dynamically_allocate_ports = True,
              port_allocation_range = DEFAULT_PORT_ALLOCATION_RANGE,
              ways_only = True, min_osm_size = 10e3, require_gtfs = False,
              auto_download_jar = True):
        """ Set up and start up an OTP instance

        Downloads the files necessary for and starts up and manages an instance
        of OpenTripPlanner (OTP). OTP will not be launched if an OSM file and at
        least 1 GTFS feed is retrieved.

        Args:
            port: The port to serve OTP on.
            dynamically_allocate_ports: If True, overrides the port
                argument and instead chooses the first available port from
                port_allocation_range.
            port_allocation_range: A list of ports that OTP can use.
            ways_only: A bool describing whether or not to download an OSM file
                containing only nodes used in ways, a.k.a no points of interest
            min_osm_size: A number describing the minimum expected size of an
                OSM file. The OSM download will be considered failed if the OSM
                file is less than this many bytes in size.
            require_gtfs: A bool that describes if the presence of a GTFS feed
                is required for OTP to be started. If False, OTP will start even
                if no GTFS feeds could be found.
            auto_download_jar: A bool describing if OTP should be downloaded to
                the otp_path if it cannot be found.

        Returns:
            True if OTP is started up successfully; False if not.
        """

        # Defined by JavaManager
        if (not self.setup_graph_init()):
            return False

        # Defined by JavaManager
        if (not self.setup_download_data(ways_only, min_osm_size, require_gtfs)):
            return False

        atexit.register(self.terminate)

        # Defined by individual routing engine managers
        if (not self.setup_routing_engine(auto_download_jar)):
            return False

        # Ready
        print_wide("Starting routing engine")
        for i in range(3):
            if (self.start_proc(port, dynamically_allocate_ports,
                                port_allocation_range)):
                print("Listening on port %d\n" % self.port)
                return True
            else:
                print("Could not start routing engine")
        print("\nFailed to start routing engine")
        return False

class OTPManager(JavaManager):

    def setup_routing_engine(self, auto_download_jar):
        """ Stage 3 of setup: download jar and perform intital graph build

        Args:
            auto_download_jar: A bool describing if the routing engine should
                be downloaded if it cannot be found.

        Returns:
            True on successful completion
        """

        # Download routing engine if it doesn't exist locally
        if (not os.path.isfile(self.jar_path)):
            if (auto_download_jar):
                print("Downloading routing engine")
                if (not bbox_dl.save_file(
                    url = "https://repo1.maven.org/maven2/org/opentripplanner"
                          "/otp/1.1.0/otp-1.1.0-shaded.jar",
                    output_path = self.jar_path, live_output = True
                )):
                    return False
            else:
                print("No routing engine found")
                return False

        return False

        # Initial graph build
        print_wide("Building graph")
        if (not self.graph_config["otp_graph_build_time"]):
            if (self.build_graph()):
                self.graph_config["otp_graph_build_time"] = datetime.datetime.now().isoformat()
                self.write_config()
            else:
                print("Graph building failed")
                return False
        else:
            print("Graph already built")
        print("")

        return True

    def build_graph(self):
        """ Attempts to build a graph with OTP

        Attempts to build a graph from previously downloaded data

        Returns:
            True if successful; False if not.
        """

        self.proc_output = log_name("otpmanager_graph_build")
        fp = open(self.proc_output, "w")

        self.proc = subprocess.Popen(
            [
                "java", "-jar", self.jar_path,
                "--basePath", ".",
                "--build", self.graph_subdir
            ],
            stdout = fp,
            stderr = fp
        )

        print("PID: %d" % self.proc.pid)

        return self.monitor_proc([
            {
                "substring": "Exception in thread",
                "kill_otp": True,
                "return_value": False,
                "callback": fp.close
            },
            {
                "substring": "Graph written",
                "kill_otp": True,
                "return_value": True,
                "callback": fp.close
            }
        ])

    def start_proc(self, port, dynamically_allocate_ports,
                    port_allocation_range):
        """ Attempts to start an OTP instance

        Attempts to start up an OTP instance, using the graph built by
        self.build_graph.

        Args:
            port: The port to serve OTP on.
            dynamically_allocate_ports: If True, overrides the port
                argument and instead chooses the first available port from
                port_allocation_range.
            port_allocation_range: A list of ports that OTP can use.

        Returns:
            True if OTP was succesfully started; False otherwise.
        """

        if (dynamically_allocate_ports):
            ports = find_ports(port_allocation_range, 2)
            if (ports):
                self.port = ports[0]
            else:
                print("No ports between %d and %d are available." % (
                    port_allocation_range[0], port_allocation_range[-1]
                ))
                return False
        else:
            self.port = port,

        self.proc_output = log_name("otpmanager")
        fp = open(self.proc_output, "w")

        self.proc = subprocess.Popen(
            [
                "java", "-jar", self.jar_path,
                "--basePath", ".",
                "--router", self.graph_name,
                "--port", str(self.port),
                "--securePort", str(ports[1]),
                "--inMemory"
            ],
            stdout = fp,
            stderr = fp
        )
        print("OTP PID: %d" % self.proc.pid)

        return self.monitor_proc([
            {
                "substring": "Exception in thread",
                "kill_otp": True,
                "return_value": False,
                "callback": fp.close
            },
            {
                "substring": "Grizzly server running",
                "kill_otp": False,
                "return_value": True,
                "callback": fp.close
            }
        ], timeout = False)

class GraphHopperManager(JavaManager):

    def setup_routing_engine(self, auto_download_jar):
        """ Stage 3 of setup: download jar and perform intital graph build

        Args:
            auto_download_jar: A bool describing if the routing engine should
                be downloaded if it cannot be found.

        Returns:
            True on successful completion
        """

        # Download routing engine if it doesn't exist locally
        if (not os.path.isdir(self.jar_path)):
            if (auto_download_jar):
                out_zip = "graphhopper-web-0.9.0-bin.zip"

                print("Downloading routing engine")
                if (not bbox_dl.save_file(
                    url = "https://graphhopper.com/public/releases/graphhopper-web-0.9.0-bin.zip",
                    output_path = out_zip, live_output = True
                )):
                    return False

                print("Unpacking routing engine")
                os.mkdir(self.jar_path)
                with zipfile.ZipFile(out_zip, "r") as z:
                    z.extractall(self.jar_path)
                os.remove(out_zip)
            else:
                print("No routing engine found")
                return False

        # Initial graph build
        print_wide("Building graph")
        if (not self.graph_config["gh_graph_build_time"]):
            if (self.build_graph()):
                self.graph_config["gh_graph_build_time"] = datetime.datetime.now().isoformat()
                self.write_config()
            else:
                print("Graph building failed")
                return False
        else:
            print("Graph already built")
        print("")

        return True

    def build_gh_startup_args(self, port = None):
        args = [
            "java", "-jar", glob.glob("%s/*.jar" % self.jar_path)[0],
            "jetty.resourcebase=%s/webapp" % self.jar_path,
            "config=%s/config-example.properties" % self.jar_path,
            "datareader.file=" + glob.glob("%s/*.osm" % self.graph_subdir)[0],
            "graph.flag_encoders=car,foot,bike"
        ]
        if (port):
            args.append("jetty.port=%d" % port)
        return args

    def build_graph(self):
        """ Attempts to build a graph with OTP

        Attempts to build a graph from previously downloaded data

        Returns:
            True if successful; False if not.
        """

        self.proc_output = log_name("otpmanager_graph_build")
        fp = open(self.proc_output, "w")

        self.proc = subprocess.Popen(
            self.build_gh_startup_args(),
            stdout = fp,
            stderr = fp
        )

        print("PID: %d" % self.proc.pid)

        return self.monitor_proc([
            {
                "substring": "Exception in thread",
                "kill_otp": True,
                "return_value": False,
                "callback": fp.close
            },
            {
                "substring": "loaded graph",
                "kill_otp": True,
                "return_value": True,
                "callback": fp.close
            }
        ])

    def start_proc(self, port, dynamically_allocate_ports,
                    port_allocation_range):
        """ Attempts to start a GraphHopper instance

        Attempts to start up a GraphHopper instance, using the graph built by
        self.build_graph.

        Args:
            port: The port to serve OTP on.
            dynamically_allocate_ports: If True, overrides the port argument
                and instead chooses the first available port from
                port_allocation_range.
            port_allocation_range: A list of ports that OTP can use.

        Returns:
            True if OTP was succesfully started; False otherwise.
        """

        if (dynamically_allocate_ports):
            ports = find_ports(port_allocation_range, 2)
            if (ports):
                self.port = ports[0]
            else:
                print("No ports between %d and %d are available." % (
                    port_allocation_range[0], port_allocation_range[-1]
                ))
                return False
        else:
            self.port = port,

        self.proc_output = log_name("otpmanager")
        fp = open(self.proc_output, "w")

        self.proc = subprocess.Popen(
            self.build_gh_startup_args(self.port),
            stdout = fp,
            stderr = fp
        )
        print("GraphHopper PID: %d" % self.proc.pid)

        return self.monitor_proc([
            {
                "substring": "Exception in thread",
                "kill_otp": True,
                "return_value": False,
                "callback": fp.close
            },
            {
                "substring": "Started server at HTTP",
                "kill_otp": False,
                "return_value": True,
                "callback": fp.close
            }
        ], timeout = False)

