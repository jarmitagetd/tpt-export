#!/bin/bash
clear
echo creating group tpt-export
sudo groupadd tpt-export
sleep 5
echo creating user tpt-export
sudo useradd -g tpt-export tpt-export
sleep 5
echo changing ownership of tptexport folder, sub directories and objects to tpt-export:tpt-export
cd ../
sudo chown -R tpt-export:tpt-export tpt-export/
sleep 5
echo changing permissions on tptexport folder, sub directories and objects to 0755
sudo chmod -R 0755 tpt-export
sleep 5
echo OK ready to export stuff, enjoy!