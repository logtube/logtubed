#!/bin/bash

set -e
set -u

cd $(dirname $0)

install -m 644 logtubed.service /lib/systemd/system/logtubed.service
if ! -e /etc/logtubed.yml; then
    install -m 644 logtubed.yml     /etc/logtubed.yml
fi

systemd daemon-reload
systemd stop logtubed

install -s -m 755 logtubed      /usr/bin/logtubed

if ! id -u logtubed > /dev/null 2>&1; then
    useradd -d /var/lib/logtubed logtubed
fi

