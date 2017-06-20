#!/usr/bin/env python3

import setuptools

setuptools.setup(
    name = "otpmanager",
    version = "1.4.0",
    license = "Apache",
    description = "Manages the setup, startup, and monitoring of an"
                  "OpenTripPlanner (OTP) instance",
    packages = ["otpmanager"],
    install_requires = ["requests"]
)
