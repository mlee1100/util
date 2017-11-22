#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEMPDIR="temp_redis"

cd $DIR
mkdir $TEMPDIR
cd $TEMPDIR
wget http://download.redis.io/redis-stable.tar.gz
tar -xvzf redis-stable.tar.gz

cd redis-stable
sudo make
sudo make install

cd $DIR
sudo rm -r $TEMPDIR

# this is optional, but not doing it causes issues when running CleProcess for PRM.
redis-server --daemonize yes #this starts redis-server running
redis-cli config set stop-writes-on-bgsave-error no # this makes it so rdb backups are not generated