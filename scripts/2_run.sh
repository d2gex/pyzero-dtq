#!/usr/bin/env bash

TARGET=/home/ubuntu/pyzero_dtq
ip=$(/sbin/ip -o -4 addr list eth0 | awk '{print $4}' | cut -d/ -f1);
echo IP=${ip} > ${TARGET}/ip.txt;
sudo systemctl start pyzero_dtq_stress_test