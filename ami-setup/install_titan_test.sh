#!/bin/sh
cd /home/ec2-user
mkdir -p testspace
cd testspace
wget http://s3.thinkaurelius.com/downloads/titan/titan-0.5.0-hadoop2.zip
unzip titan-0.5.0-hadoop2.zip
rm titan-0.5.0-hadoop2.zip
cd titan-0.5.0-hadoop2
mkdir -p data
cd data
aws s3 cp s3://nwd-user-storage/lee/titan/sample-data.zip ./
unzip sample-data.zip
rm sample-data.zip