#!/bin/sh
NOW=$(date +"%Y%m%d")
BUCKETLOC="s3://nwd-user-storage/lee/server-setup/"
AWSLOCATION="/usr/bin/"
ZIPNAME="util."$NOW".zip"
ZIPPATH=$HOME"/"$ZIPNAME
# echo $ZIPPATH

if [ -f "$ZIPPATH" ]
then
	rm $ZIPPATH
fi

cd $HOME
zip -r $ZIPNAME $UTILDIR > /dev/null

COMMAND=$AWSLOCATION"aws s3 cp "$ZIPPATH" "$BUCKETLOC" > /dev/null"

eval $COMMAND

rm $ZIPPATH