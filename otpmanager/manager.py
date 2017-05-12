#!/usr/bin/env python3

import atexit
import contextlib
import datetime
import os
import signal
import socket
import subprocess
import time

from . import bbox_dl

# Array containing ports that can be used by this script
DEFAULT_PORT_ALLOCATION_RANGE = range(8100, 8200)

DEFAULT_PORT = 8080
DEFAULT_SECURE_PORT = 8081

# OTP: Directory to store graphs in
DEFAULT_GRAPH_ROOT_DIR = "graphs/"

# OTP: Location of the OTP jar
DEFAULT_OTP_PATH = "otp-1.1.0-shaded.jar"

# OTP: How long between STDOUT messages during startup before the OTP is
# considered to be dead
DEFAULT_TIMEOUT = 600

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
    """ Find available ports in the PORTS range

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

class OTPManager(object):
    """ Class responsible for setting up, starting, and stopping OpenTripPlanner

    Attributes:
        graph_name: A string containing the name of the graph being worked
            on.
        bbox: A tuple containing the leftmost, bottommost, rightmost, and
            topmost coordinates.
        otp: The process running OpenTripPlanner, if self.start runs and
            succeeds.
        port: The port that OpenTripPlanner is serving HTTP on, if
            self.start runs and succeeds.
        secure_port: The port that OpenTripPlanner is serving HTTPS on, if
            self.start runs and succeeds.

    """

    def __init__(self, graph_name, left, bottom, right, top,
                 otp_path = DEFAULT_OTP_PATH,
                 graph_root_dir = DEFAULT_GRAPH_ROOT_DIR):
        """ Initializes OTPManager class and returns True if OTP can be used

        Args:
            graph_name: The name of the graph, to be stored in GRAPH_ROOT_DIR.
                connections.
            left: A floating point of the leftmost coordinate.
            bottom: A floating point of the bottommost coordinate.
            right: A floating point of the rightmost coordinate.
            top: A floating point of the topmost coordinate.

        """

        self.graph_root_dir = graph_root_dir
        self.graph_name = graph_name
        self.bbox = (left, bottom, right, top)
        self.otp_path = otp_path

        self.otp = None

    def start(self, port = DEFAULT_PORT, secure_port = DEFAULT_SECURE_PORT,
              dynamically_allocate_ports = True,
              port_allocation_range = DEFAULT_PORT_ALLOCATION_RANGE,
              require_gtfs = False):
        """ Set up and start up an OTP instance

        Downloads the files necessary for and starts up and manages an instance
        of OpenTripPlanner (OTP). OTP will not be launched if an OSM file and at
        least 1 GTFS feed is retrieved.

        Args:
            port: The port to serve OTP on.
            secure_port: The OTP secure port (preferably port + 1).
            dynamically_allocate_ports: If True, overrides the port and
                secure_port arguments and instead chooses the first available
                port from port_allocation_range.
            port_allocation_range: A list of ports that OTP can use.
            require_gtfs: A bool that describes if the presence of a GTFS feed
                is required for OTP to be started. If False, OTP will start even
                if no GTFS feeds could be found.

        Returns:
            True if OTP is started up successfully; False if not.
        """

        downloaded_gtfs = "%s/%s/downloaded_gtfs" % (self.graph_root_dir,
                                                     self.graph_name)
        downloaded_osm = "%s/%s/downloaded_osm" % (self.graph_root_dir,
                                                    self.graph_name)
        built_graph = "%s/%s/built_graph" % (self.graph_root_dir,
                                             self.graph_name)
        output_dir = "%s/%s/" % (self.graph_root_dir, self.graph_name)

        if (not os.path.isfile(self.otp_path)):
            print("Could not find OTP")
            return False
        if (not os.path.exists(self.graph_root_dir)):
            os.mkdir(self.graph_root_dir)
        if (not os.path.exists(output_dir)):
            os.mkdir(output_dir)

        atexit.register(self.stop_otp)

        print_wide("Downloading OSM from Overpass API")
        if (not os.path.exists(downloaded_osm)):
            if (self.download_osm(output_dir)):
                with open(downloaded_osm, "w") as f:
                    pass
            else:
                print("OSM downloading failed")
                return False
        else:
            print("OSM already downloaded")

        print_wide("Downloading GTFS feeds")
        if (not os.path.exists(downloaded_gtfs)):
            if (self.download_gtfs(output_dir)):
                with open(downloaded_gtfs, "w") as f:
                    pass
            else:
                print("GTFS downloading failed")
                if (require_gtfs):
                    return False
                else:
                    print("Resuming anyway")
        else:
            print("GTFS already downloaded")

        print("")
        print_wide("Building graph")
        if (not os.path.exists(built_graph)):
            if (self.build_graph()):
                with open(built_graph, "w") as f:
                    pass
            else:
                print("Graph building failed")
                return False
        else:
            print("Graph already built")

        print("")
        print_wide("Starting OTP")
        for i in range(3):
            if (self.start_otp(port, secure_port, dynamically_allocate_ports,
                                 port_allocation_range)):
                print("OTP ready on ports %d and %d\n" % (self.port,
                                                          self.secure_port))
                return True
            else:
                print("Could not start OTP")

        print("\nNot using OTP")
        return False

    def monitor_otp(self, listeners = [], show_output = True,
                    timeout = DEFAULT_TIMEOUT):
        """ Monitor a running OTP process

        Monitor the output of a running OTP process and perform actions if
        necessary.

        Args:
            listeners: A list of dictionaries, structured as such:
                {
                    "substring": A substring of OTP output thst triggers this
                        listener to fire. For example, if substring is
                        "ERROR", then this listener would fire whenever "ERROR"
                        appears in the output of OTP.
                    "return_value": The value to be returned when this listener
                        fires.
                    "kill_otp": A bool telling whether or not to kill the
                        running OTP instance when this listener fires.
                    "callback": A function that will be fired when this listener
                        fires. (OPTIONAL)
                }
            show_output: A bool telling whether or not OTP output should be
                written to STDOUT.
            timeout: OTP will be killed if it produces no output in this amount
                of time. This can be set to False if there should be no timeout.

        Returns:
            False on timeout or the value of listener["callback"] otherwise.

        """
        last_activity = time.time()
        while True:
            line = self.otp.stdout.readline().decode().rstrip()

            if (len(line) > 0):
                last_activity = time.time()
                if (show_output):
                    print("OTP: %s" % line)

                for listener in listeners:
                    if (listener["substring"] in line):
                        if ("kill_otp" in listener):
                            if (listener["kill_otp"]):
                                self.stop_otp()
                        if ("return_value" in listener):
                            return listener["return_value"]
                        if ("callback" in listener):
                            listener["callback"]()
                        return
            else:
                if (timeout is not False):
                    if (time.time() - last_activity > timeout):
                        print("\nKilling OTP; no stdout activity in last %d"
                              "seconds" % timeout)
                        self.stop_otp()
                        return False

                time.sleep(0.1)

    def download_osm(self, output_dir):
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
            *self.bbox
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

    def build_graph(self):
        """ Attempts to build a graph with OTP

        Attempts to build a graph from the data downloaded by
        self.download_components

        Returns:
            True if successful; False if not.

        """

        self.otp = subprocess.Popen(
            [
                "java", "-jar", self.otp_path,
                "--basePath", ".",
                "--build", "%s/%s" % (self.graph_root_dir, self.graph_name)
            ],
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE
        )

        print("OTP PID: %d" % self.otp.pid)
        return self.monitor_otp([
            {
                "substring": "ERROR",
                "kill_otp": True,
                "return_value": False
            },
            {
                "substring": "Graph written",
                "kill_otp": True,
                "return_value": True
            }
        ])

    def start_otp(self, port, secure_port, dynamically_allocate_ports,
                    port_allocation_range):
        """ Attempts to start an OTP instance

        Attempts to start up an OTP instance, using the graph built by
        self.build_graph.

        Args:
            port: The port to serve OTP on.
            secure_port: The OTP secure port (preferably port + 1).
            dynamically_allocate_ports: If True, overrides the port and
                secure_port arguments and instead chooses the first available
                port from port_allocation_range.
            port_allocation_range: A list of ports that OTP can use.

        Returns:
            True if OTP was succesfully started; False otherwise.
        """

        if (dynamically_allocate_ports):
            ports = find_ports(port_allocation_range, 2)
            if (ports):
                self.port = ports[0]
                self.secure_port = ports[1]
            else:
                print("No ports between %d and %d are available." % (
                    port_allocation_range[0], port_allocation_range[-1]
                ))
                return False
        else:
            self.port = port,
            self.secure_port = secure_port

        self.otp = subprocess.Popen(
            [
                "java", "-jar", self.otp_path,
                "--basePath", ".",
                "--router", self.graph_name,
                "--port", str(self.port),
                "--securePort", str(self.secure_port),
                "--inMemory"
            ],
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE
        )

        print("OTP PID: %d" % self.otp.pid)
        return self.monitor_otp([
            {
                "substring": "ERROR",
                "kill_otp": True,
                "return_value": False
            },
            {
                "substring": "Grizzly server running",
                "kill_otp": False,
                "return_value": True
            }
        ], timeout = False)

    def stop_otp(self, *dummy_args, **dummy_kwargs):
        """ Stop the running OTP instance """

        if (self.otp is not None):
            print("Killing OTP process %d" % self.otp.pid)
            self.otp.kill()
            self.otp = None
        else:
            print("No running OTP process to kill")
