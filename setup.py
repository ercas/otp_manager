#!/usr/bin/env python3

import setuptools

setuptools.setup(
    name = "otp_manager",
    version = "1.0.0",
    license = "Apache",
    description = "Manages the setup, startup, and monitoring of an"
                  "OpenTripPlanner (OTP) instance",
    packages = ["otp_manager"],
    install_requires = ["requests"]
)
