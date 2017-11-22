#!/bin/sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR
mkdir temp_rar
cd temp_rar
wget http://www.rarlab.com/rar/rarlinux-x64-5.3.b4.tar.gz
tar -zxvf rarlinux-*.tar.gz
sudo cp ./rar/rar /usr/local/bin/
sudo cp ./rar/unrar /usr/local/bin/
cd $DIR
rm -r temp_rar
