#!/bin/bash

# Copyright 2014 Telefonica Investigacion y Desarrollo, S.A.U
#
# This file is part of Orion Context Broker.
#
# Orion Context Broker is free software: you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# Orion Context Broker is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
# General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Orion Context Broker. If not, see http://www.gnu.org/licenses/.
#
# For those usages not covered by this license please contact with
# iot_support at tid dot es

set -e

BUILD_DEPS=(
 'libboost-all-dev' \
 'libcurl4-openssl-dev' \
 'libgcrypt-dev' \
 'libgnutls28-dev' \
 'libsasl2-dev' \
 'libssl-dev' \
 'uuid-dev' \
 'zlib1g-dev' \
)

BUILD_TOOLS=(
 'apt-transport-https' \
 'bzip2' \
 'ca-certificates' \
 'cmake' \
 'curl' \
 'wget' \
 'dirmngr' \
 'g++' \
 'gcc' \
 'git' \
 'gnupg' \
 'make' \
 'scons' \
)

apt-get -y install -f --no-install-recommends \
    ${BUILD_TOOLS[@]} \
    ${BUILD_DEPS[@]}