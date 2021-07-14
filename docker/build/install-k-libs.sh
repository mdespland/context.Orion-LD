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

echo
echo -e "\e[1;32m Debian Builder: installing k libs \e[0m"
for kproj in kbase klog kalloc kjson khash
do
    git clone https://gitlab.com/kzangeli/${kproj}.git ${ROOT_FOLDER}/$kproj
done


for kproj in kbase klog kalloc khash
do
    cd ${ROOT_FOLDER}/$kproj
    git checkout release/0.8
    make
    make install
done

#
# kjson
#
cd ${ROOT_FOLDER}/kjson
git checkout release/0.8.1
make
make install
