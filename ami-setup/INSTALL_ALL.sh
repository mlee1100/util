#!/bin/sh
INSTALLDIR=$UTILDIR"/ami-setup/"

SCRIPTSTORUN=("install_packages.sh" "install_7z.sh" "install_rar.sh" "install_redis.sh" "install_sshpass.sh")

for SCRIPT in "${SCRIPTSTORUN[@]}"
do
	eval "sudo sh "$SCRIPT
done