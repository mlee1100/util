#!/bin/sh

sudo yum install gcc-c++ -y

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR
mkdir temp_7z
cp ./source/p7zip_15.14.1_src_all.tar.bz2 temp_7z/
cd temp_7z
tar -jxvf p7zip_15.14.1_src_all.tar.bz2
cd p7zip_15.14.1
sudo make
sudo make install
cd $DIR
sudo rm -r temp_7z
