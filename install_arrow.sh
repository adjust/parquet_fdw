#!/usr/bin/env bash

apt-get update
apt-get install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
apt-get update
apt-get install -y -V libarrow-dev libparquet-dev
