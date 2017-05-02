otp_manager
===========

``otp_manager`` provides the ``OTPManager`` class that manages the setup,
startup, and monitoring of an `OpenTripPlanner
<http://www.opentripplanner.org/>`_ (OTP) instance. This is meant to be used in
conjuction with `route_distances <https://github.com/ercas/route_distances>`_,
a library to interface with various routing engines from Python, including OTP.

OTPManager's functionality includes:

* Downloading the necessary OSM XML file using the
  `Overpass API <https://wiki.openstreetmap.org/wiki/Overpass_API>`_
* Finding GTFS feeds using the `Transitland API <https://transit.land/>`_ and
  downloading them
* Building the ``Graph.obj`` using OTP
* Running an OTP router using the generated graph, dynamically allocating ports
  if specified (default behaviour)
* For all steps involving OTP, monitor progress and terminating OTP if it
  freezes or encounters an error
* Provide access to the OTP subprocess and information about it

Simple usage, paired with `route_distances`:

::

    import otp_manager
    import route_distances

    manager = otp_manager.OTPManager(
        "boston", -71.191155, 42.227926, -70.748802, 42.400819999999996,
        otp_path = "/home/user/otp-1.1.0-shaded.jar"
    )
    manager.start()

    # Here we use manager.port to find what port OTP was bound to because it was
    # dynamically allocated, so we can't be sure what port it is. This behaviour
    # can be overridden if necessary.
    router = route_distances.OTPDistances("localhost:%d" % manager.port)
    print(router.calculate(-71.08930, 42.33877, -71.07743, 42.34954, "walk"))

    manager.stop_otp()

..

OTP will automatically be stopped when the script exits.
