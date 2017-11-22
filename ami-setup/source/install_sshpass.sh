SCRIPT=$(readlink -f "$0")
SOURCE=$SCRIPT"/source"
cd $SOURCE
tar -zxvf sshpass-1.06.tar.gz
cd sshpass-1.06
./configure
sudo make install
cd $SOURCE
sudo rm -r sshpass-1.06