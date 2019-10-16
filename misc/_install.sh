#!/bin/bash

set -e
set -u

cd $(dirname $0)

install -o root -m 644 logtubed.service /lib/systemd/system/logtubed.service

if ! [ -e /etc/logtubed.yml ]
then
    install -o root -m 644 logtubed.yml     /etc/logtubed.yml
fi

systemctl daemon-reload
systemctl stop logtubed

install -o root -s -m 755 logtubed      /usr/bin/logtubed

if ! id -u logtubed > /dev/null 2>&1
then
    useradd -d /var/lib/logtubed logtubed
fi

