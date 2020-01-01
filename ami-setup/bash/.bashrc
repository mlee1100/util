# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# Completion adds
complete -f '*.*' gzip
complete -f '*.*' aws

USER_HOME="/home/ec2-user"

# do not write pyc files when executing python
export PYTHONDONTWRITEBYTECODE=1

# User specific aliases and functions
export UTILDIRNAME="util"
export UTILDIR=$USER_HOME"/"$UTILDIRNAME
export UTILSCRIPT=$UTILDIR"/script"
export NETWISE_PRODUCTION_EXPORTER_DIR=$USER_HOME"/netwise_production_exporters"
export NETWISE_PRODUCTION_EXPORTER=$NETWISE_PRODUCTION_EXPORTER_DIR"/Export.py"
export S3DIR=$USER_HOME"/s3"
export REDIS="/usr/local/bin/"
export REDISCLI=$REDIS"redis-cli"
export REDISSERVER=$REDIS"redis-server"
export SERVERNAME="Admin"
export GNUPGHOME=$USER_HOME"/.gnupg/"

# RISK environment variables
USER_HOME="/home/ec2-user"

if [ "$HOSTNAME" == "ip-172-31-7-152" ]; then
  export RISKREPO=$USER_HOME"/nwd_risk"
else
  export RISKREPO=$USER_HOME"/nwd_risk"
fi

export EXPORT_LOG_DIR=$USER_HOME"/netwise_export_executables/netwise_export_manager/log/"
export EXPORT_EXE_DIR=$USER_HOME"/netwise_export_executables/"

export RISK_DROPOFF=$RISKREPO"/export"
export RISKPROPERTIESDIR=$RISKREPO"/properties/"
export RISKEXPORTDIR=$RISKREPO"/export/"
export RISKVERIFICATIONDIR=$RISKREPO"/verification/"
export RISKVERIFICATIONRETURNDIR=$RISKREPO"/sftp_temp_return_location/"
export RISKLOGDIR=$RISKREPO"/log/"
export RISKSFTPDIR="/sftp/"
export RISKSCRIPTDIR=$RISKREPO"/script/"
export RISKSTAGING=$RISKSCRIPTDIR"/staging/"
export RISKSFTPLOGDIR="/var/log/"
export RISKREPORTDIR=$RISKSFTPDIR"reports/"
export RISKVBANKDIR=$RISKSFTPDIR"lamanna/pickup/"
export RISKAPPRISSDIR=$RISKSFTPDIR"jacobson/pickup/"
export RISKBACKUPDIR=$RISKREPO"/backup/"

export NEUSTAR_RETARGETTING=$NETWISE_PRODUCTION_EXPORTER_DIR"/settings/neustar/Neustar.Retargeting.Default.SETTINGS.py"

# shortcut aliases
alias flf='sudo find ~/ -path '$S3DIR' -prune -o -type f -printf "%s\t%p\n" | sort -n | tail -20 | sort -n -r'
alias getip="curl 'https://api.ipify.org?format=json' && echo ''"
alias gpid="ps ax | grep"
alias p="python"

alias lsn="screen -ls"
alias rs="screen -h 10000 -d -r"
alias sn="echo $STY"
alias ns="screen -h 10000 -S"
alias la="ls -lha"
alias lat="ls -lhat"
alias lh="ls -lh"
alias reset-bash="echo -e \\033c"
alias dot-rm="sudo find "$USER_HOME" -path "$S3DIR" -prune -o -name '._*' -exec rm -v {} \;"
alias ds-rm="sudo find "$USER_HOME" -path "$S3DIR" -prune -o -name '.DS_Store' -exec rm -v {} \;"
alias pyc-rm="sudo find "$USER_HOME" -path "$S3DIR" -prune -o -name '*.pyc' -exec rm -v {} \;"
alias cm="dot-rm && ds-rm && pyc-rm;"

alias sample="python "$UTILSCRIPT"/Sampler.py"
alias tablesample="python "$UTILSCRIPT"/RandomSampleExport.py"
alias fillrate="python "$UTILSCRIPT"/GetFillRate.py"
alias fillrate-multi="python "$UTILSCRIPT"/GetFillrateMulti.py"
alias reformat="python "$UTILSCRIPT"/FormatConvert.py"
alias multireformat="python "$UTILSCRIPT"/FormatConvertMulti.py"
alias email="python "$UTILSCRIPT"/SendEmail.py"
alias dedup="python "$UTILSCRIPT"/Dedup.py"
alias scriptkill="python "$UTILSCRIPT"/KillScript.py"
alias EXPORT="python "$NETWISE_PRODUCTION_EXPORTER_DIR"/Export.py"
alias nom="python34 "$UTILSCRIPT"/nom.py"
alias combine-files="python "$UTILSCRIPT"/CombineFiles.py"
alias streamunzip="python "$UTILSCRIPT"/StreamUnzip.py"
alias remove-hits="python "$UTILSCRIPT"/RemoveGoodHits.py"
alias re-encode="python "$UTILSCRIPT"/ReEncode.py"
alias memory-print="python "$UTILSCRIPT"/MemoryPrint.py"
alias getscheme="python "$UTILSCRIPT"/GetJsonScheme.py"
alias getjsonfillrate="python "$UTILSCRIPT"/GetJsonFillrate.py"
alias stream-to-json="python "$UTILSCRIPT"/StreamToJson.py"
alias csv2parquet="python "$UTILSCRIPT"/Csv2Parquet.py"

# use nano as default editor
export VISUAL=nano

#add to PATH
export PATH=$PATH":"$UTILDIR
export CON_CONFIG_PATH=$UTILDIR"/connection_config.json"
export PATH=$PATH:/usr/local/bin/apache-maven-3.3.9/bin
export PYTHONPATH=$UTILSCRIPT":"$UTILDIR

# check the window size after each command and, if necessary,
# update the values of LINES and COLUMNS.
shopt -s checkwinsize


#custom var changed
export JAVA_HOME="/usr/lib/jvm/jre-1.8.0"
export PYTHONASYNCIODEBUG=1
#export JAVA_HOME="/usr/lib/jvm/jre"


# git aliases
alias gc='git checkout'
alias gm='git merge'
alias gb='git branch'
alias gs='git status'
alias ga='git add'
alias gd='git diff'

# git alias functions
git_merge_master() {
	if [ "$#" -eq 1 ]; then
		git checkout master
		git pull
		git checkout $1
		git merge master
	else
		echo 'invalid number of parameters'
	fi
}
alias gmm=git_merge_master

s3_mount() {
	if [ "$#" -eq 2 ]; then
		ACCOUNT=$1
		BUCKET=$2
		mkdir -p $S3DIR/$BUCKET
		sudo umount $S3DIR/$BUCKET &> /dev/null
		sudo /usr/local/bin/s3fs $BUCKET $S3DIR/$BUCKET -o passwd_file=/etc/passwd-s3fs-$ACCOUNT -o allow_other -o umask=022 -o stat_cache_expire=10 -o enable_noobj_cache -o enable_content_md5
		echo 'complete'
	else
		echo 'invalid number of parameters'
	fi
}
alias s3-mount=s3_mount


S3TOMOUNT=("nwd nwd-user-storage" "nwd nwd-exports" "nwd nwd-imports" "root lee-private-storage")

s3_mount_all() {
	for BUCKET in "${S3TOMOUNT[@]}"
	do
		eval "s3-mount "$BUCKET
	done
}
alias s3-mount-all=s3_mount_all

find_dir() {
	DIR=$1
	COMMAND="sudo find / -path "$S3DIR" -prune -o -type d -name '*"$DIR"*' | grep -Ev '"$S3DIR"'"
	eval $COMMAND
}
alias find-dir=find_dir

find_file() {
	FILE=$1
	COMMAND="sudo find / -path "$S3DIR" -prune -o -type f -name '*"$FILE"*' | grep -Ev '"$S3DIR"'"
	eval $COMMAND
}
alias find-file=find_file

find_in_s3() {
	PART=$1
	for bucket in $(aws s3 ls s3:// | awk '{print $3}')
	do
		echo $bucket
		aws s3 ls --recursive "s3://"$bucket | awk '{print "    "$4}' | grep $PART
	done
}
alias find-in-s3=find_in_s3

s3_unzip() {
	INPUT=$1
	OUTPUT=$2
	ORIGINAL_SIZE=$(aws s3 ls $INPUT | awk '{print $3}')
	ESTIMATED_SIZE=$(( $ORIGINAL_SIZE * 20 ))
	aws s3 cp $INPUT - | gzip -d -c | aws s3 cp - $OUTPUT --expected-size=$ESTIMATED_SIZE
}
alias s3-unzip=s3_unzip

unzip_to_s3() {
	INPUT=$1
	OUTPUT=$2
	ORIGINAL_SIZE=$(ls -l $INPUT | awk '{print $5}')
	ESTIMATED_SIZE=$(( $ORIGINAL_SIZE * 20 ))
	gzip -d -c $INPUT | aws s3 cp - $OUTPUT --expected-size=$ESTIMATED_SIZE
}
alias unzip-to-s3=unzip_to_s3

list_s3_paths() {
	DIR=$1
	aws s3 ls $@ | awk -v DIR=$DIR '{print DIR$4}'
}
alias list-s3-paths=list_s3_paths

reload_profile() {
	source /home/ec2-user/.bash_profile
}
alias reload-profile=reload_profile

activate() {
    env=$1
    eval "source ~/.envs/"$env"/bin/activate"
}
alias activate=activate

getalias() {
    a=$1
    echo $(alias $a | grep -oP '(?<='"'"')[^'"'"']+(?='"'"')')
}
alias getalias=getalias



# ssh pass aliases
alias server-powerlytics="sshpass -p '"$SERVER_POWERLYTICS_PASSWORD"' ssh -o StrictHostKeyChecking=no "$SERVER_POWERLYTICS_USER"@"$SERVER_POWERLYTICS_HOST
alias server-prm-dev-app="sshpass -p '"$SERVER_PRM_DEV_APP_PASSWORD"' ssh -o StrictHostKeyChecking=no "$SERVER_PRM_DEV_APP_USER"@"$SERVER_PRM_DEV_APP_HOST
alias server-prm-dev-processing="sshpass -p '"$SERVER_PRM_DEV_PROCESSING_PASSWORD"' ssh -o StrictHostKeyChecking=no "$SERVER_PRM_DEV_PROCESSING_USER"@"$SERVER_PRM_DEV_PROCESSING_HOST
alias server-prm-prod-app="sshpass -p '"$SERVER_PRM_PROD_APP_PASSWORD"' ssh -o StrictHostKeyChecking=no "$SERVER_PRM_PROD_APP_USER"@"$SERVER_PRM_PROD_APP_HOST
alias server-prm-prod-processing="sshpass -p '"$SERVER_PRM_PROD_PROCESSING_PASSWORD"' ssh -o StrictHostKeyChecking=no "$SERVER_PRM_PROD_PROCESSING_USER"@"$SERVER_PRM_PROD_PROCESSING_HOST
alias sftp-risk="sshpass -p '"$SFTP_RISK_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_RISK_USER"@"$SFTP_RISK_HOST":"$SFTP_RISK_PICKUPDIR
alias sftp-risk-empire-dropoff="sshpass -p '"$SFTP_EMPIRE_DROPOFF_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_EMPIRE_DROPOFF_USER"@"$SFTP_EMPIRE_DROPOFF_HOST":"$SFTP_EMPIRE_DROPOFF_DROPOFFDIR
alias sftp-risk-empire-pickup="sshpass -p '"$SFTP_EMPIRE_PICKUP_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_EMPIRE_PICKUP_USER"@"$SFTP_EMPIRE_PICKUP_HOST":"$SFTP_EMPIRE_PICKUP_PICKUPDIR
alias sftp-risk-triax="sshpass -p '"$SFTP_TRIAX_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_TRIAX_USER"@"$SFTP_TRIAX_HOST":"$SFTP_TRIAX_PICKUPDIR
alias sftp-risk-appriss="sshpass -p '"$SFTP_APPRISS_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_APPRISS_USER"@"$SFTP_APPRISS_HOST":"$SFTP_APPRISS_PICKUPDIR
alias sftp-risk-test="sshpass -p '"$SFTP_RISK_TEST_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_RISK_TEST_USER"@"$SFTP_RISK_TEST_HOST
alias sftp-risk-transunion="sshpass -p '"$SFTP_TRANSUNION_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_TRANSUNION_USER"@"$SFTP_TRANSUNION_HOST":"$SFTP_TRANSUNION_DROPOFFDIR
alias sftp-sovrn="sshpass -p '"$SFTP_SOVRN_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_SOVRN_USER"@"$SFTP_SOVRN_HOST":"$SFTP_SOVRN_DROPOFFDIR
alias sftp-datamyx="sshpass -p '"$SFTP_DATAMYX_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_DATAMYX_USER"@"$SFTP_DATAMYX_HOST
alias sftp-experian="sshpass -p '"$SFTP_EXPERIAN_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_EXPERIAN_USER"@"$SFTP_EXPERIAN_HOST":"$SFTP_EXPERIAN_DROPOFFDIR
alias sftp-transunion="sshpass -p '"$SFTP_TRANSUNION_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_TRANSUNION_USER"@"$SFTP_TRANSUNION_HOST":"$SFTP_TRANSUNION_DROPOFFDIR
alias ftp-thomson-reuters="lftp -u "$FTP_THOMSON_REUTERS_USER","$FTP_THOMSON_REUTERS_PASSWORD" "$FTP_THOMSON_REUTERS_HOST
alias ftp-select-systems="lftp -e 'set ssl:verify-certificate false' -u "$FTP_SELECT_SYSTEMS_USER","$FTP_SELECT_SYSTEMS_PASSWORD" "$FTP_SELECT_SYSTEMS_HOST":"$FTP_SELECT_SYSTEMS_DROPOFFDIR
alias sftp-meritdirect="sshpass -p '"$SFTP_MERITDIRECT_PASSWORD"' sftp -o PubkeyAuthentication=no "$SFTP_MERITDIRECT_USER"@"$SFTP_MERITDIRECT_HOST":"$SFTP_MERITDIRECT_DROPOFFDIR
alias ftp-salutary='lftp -p '$FTP_SALUTARY_PORT' -e "set ssl:verify-certificate false" -u '$FTP_SALUTARY_USER','$FTP_SALUTARY_PASSWORD' '$FTP_SALUTARY_HOST
alias sftp-neustar="sshpass -p '"$SFTP_NEUSTAR_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_NEUSTAR_USER"@"$SFTP_NEUSTAR_HOST
alias sftp-neustar-onboarding="sshpass -p '"$SFTP_NEUSTAR_ONBOARDING_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_NEUSTAR_ONBOARDING_USER"@"$SFTP_NEUSTAR_ONBOARDING_HOST
alias sftp-liveramp="sshpass -p '"$SFTP_LIVERAMP_PASSWORD"' sftp -o StrictHostKeyChecking=no "$SFTP_LIVERAMP_USER"@"$SFTP_LIVERAMP_HOST":"$SFTP_LIVERAMP_DROPOFFDIR
alias sftp-trusignal="sshpass -p '"$SFTP_TRUSIGNAL_PASSWORD"' sftp -o StrictHostKeyChecking=no -o PubkeyAuthentication=no "$SFTP_TRUSIGNAL_USER"@"$SFTP_TRUSIGNAL_HOST":"$SFTP_TRUSIGNAL_DROPOFFDIR
alias ftp-oceanos='lftp -e "set ssl:verify-certificate false" -u '$FTP_OCEANOS_USER','$FTP_OCEANOS_PASSWORD' '$FTP_OCEANOS_HOST
alias ftp-infogroup-omni='lftp -e "set ssl:verify-certificate false" -u '$FTP_INFOGROUP_OMNI_USER','$FTP_INFOGROUP_OMNI_PASSWORD' '$FTP_INFOGROUP_OMNI_HOST
alias sftp-infogroup-sapphire="sshpass -p '"$SFTP_INFOGROUP_SAPPHIRE_PASSWORD"' sftp -o StrictHostKeyChecking=no -o PubkeyAuthentication=no "$SFTP_INFOGROUP_SAPPHIRE_USER"@"$SFTP_INFOGROUP_SAPPHIRE_HOST":"$SFTP_INFOGROUP_SAPPHIRE_DROPOFFDIR
alias ftp-infogroup-mfg='lftp -e "set ssl:verify-certificate false" -u '$FTP_INFOGROUP_MFG_USER','$FTP_INFOGROUP_MFG_PASSWORD' '$FTP_INFOGROUP_MFG_HOST
alias ftp-alc='lftp -e "set ssl:verify-certificate false" -u '$FTP_ALC_USER','$FTP_ALC_PASSWORD' '$FTP_ALC_HOST
alias ftp-webbula='lftp -e "set ssl:verify-certificate false" -u '$FTP_WEBBULA_USER','$FTP_WEBBULA_PASSWORD' '$FTP_WEBBULA_HOST
alias ftp-ktf='lftp -e "set ssl:verify-certificate false" -u '$FTP_KTF_USER','$FTP_KTF_PASSWORD' '$FTP_KTF_HOST
alias sftp-netprospex="sshpass -p '"$SFTP_NETPROSPEX_PASSWORD"' sftp -o StrictHostKeyChecking=no -o PubkeyAuthentication=no "$SFTP_NETPROSPEX_USER"@"$SFTP_NETPROSPEX_HOST":"$SFTP_NETPROSPEX_DROPOFFDIR

# sql login aliases
alias sql-intelldata="mysql -u"$SQL_INTELLDATA_USER" -p"$SQL_INTELLDATA_PASSWORD" -h"$SQL_INTELLDATA_HOST" -A"
alias sql-prm-dev="mysql -u"$SQL_PRM_DEV_USER" -p"$SQL_PRM_DEV_PASSWORD" -h"$SQL_PRM_DEV_HOST" -A"
alias sql-prm-prod="mysql -u"$SQL_PRM_PROD_USER" -p"$SQL_PRM_PROD_PASSWORD" -h"$SQL_PRM_PROD_HOST" -A"
alias sql-contacts="mysql -u"$SQL_CONTACTS_USER" -p"$SQL_CONTACTS_PASSWORD" -h"$SQL_CONTACTS_HOST" -A"
alias sql-contacts-v2="mysql -u"$SQL_CONTACTSV2_USER" -p"$SQL_CONTACTSV2_PASSWORD" -h"$SQL_CONTACTSV2_HOST" -A"
alias sql-contacts-v2-prod="mysql -u"$SQL_CONTACTSV2PROD_USER" -p"$SQL_CONTACTSV2PROD_PASSWORD" -h"$SQL_CONTACTSV2PROD_HOST" -A"
alias sql-powerlytics="mysql -u"$SQL_POWERLYTICS_USER" -p"$SQL_POWERLYTICS_PASSWORD" -h"$SQL_POWERLYTICS_HOST" -A"
alias sql-risk-dev="mysql -u"$SQL_RISK_DEV_USER" -p"$SQL_RISK_DEV_PASSWORD" -h"$SQL_RISK_DEV_HOST" -A"
alias sql-risk-prod="mysql -u"$SQL_RISK_PROD_USER" -p"$SQL_RISK_PROD_PASSWORD" -h"$SQL_RISK_PROD_HOST" -A"
alias sql-mfg="mysql -u"$SQL_MFG_USER" -p"$SQL_MFG_PASSWORD" -h"$SQL_MFG_HOST" -A"
alias sql-linkedin="mysql -u"$SQL_LINKEDIN_USER" -p"$SQL_LINKEDIN_PASSWORD" -h"$SQL_LINKEDIN_HOST" -A"
alias sql-crawler-dev="mysql -u"$SQL_CRAWLER_DEV_USER" -p"$SQL_CRAWLER_DEV_PASSWORD" -h"$SQL_CRAWLER_DEV_HOST" -A"
alias sql-reference="mysql -u"$SQL_REFERENCE_USER" -p"$SQL_REFERENCE_PASSWORD" -h"$SQL_REFERENCE_HOST" -A"
alias sql-b2bemails="mysql -u"$SQL_B2BEMAILS_USER" -p"$SQL_B2BEMAILS_PASSWORD" -h"$SQL_B2BEMAILS_HOST" -A"
alias sql-consumer="mysql -u"$SQL_CONSUMER_USER" -p"$SQL_CONSUMER_PASSWORD" -h"$SQL_CONSUMER_HOST" -A"
alias sql-consumer-aurora="mysql -u"$SQL_CONSUMER_AURORA_USER" -p"$SQL_CONSUMER_AURORA_PASSWORD" -h"$SQL_CONSUMER_AURORA_HOST" -A"
alias sql-staging="mysql -u"$SQL_STAGING_USER" -p"$SQL_STAGING_PASSWORD" -h"$SQL_STAGING_HOST" -A"
alias sql-oxyleads="mysql -u"$SQL_OXYLEADS_USER" -p"$SQL_OXYLEADS_PASSWORD" -h"$SQL_OXYLEADS_HOST" -A"
alias redshift="PGPASSWORD="$REDSHIFT_CONTACTS_PASSWORD" psql -h "$REDSHIFT_CONTACTS_HOST" -U "$REDSHIFT_CONTACTS_USER" -d "$REDSHIFT_CONTACTS_DATABASE_DEFAULT" -p 5439"
alias redshift-processor="PGPASSWORD=nwdPROC123! psql -h "$REDSHIFT_CONTACTS_HOST" -U processor -d "$REDSHIFT_CONTACTS_DATABASE_DEFAULT" -p 5439"
