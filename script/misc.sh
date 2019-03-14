split -d -a 3 -l 200000 --additional-suffix=.csv ~/temp/email_hippo_initial_file_dedup.csv ~/temp/eh_files/initial_domain_emails_



s3://nwd-imports/liveramp/matchback-reports/coreg_emails_2016_06_01.psv_Matchback
s3://nwd-imports/liveramp-matchbacks/2018_01_md5_only/nwd_email_md5_file_2018_01_18.csv_Matchback.gz
s3://nwd-imports/liveramp-matchbacks/2017-06/nwd_email_md5_file_2017_06_22.csv_Matchback.gz
s3://nwd-imports/liveramp-matchbacks/2016-12/email-only/md5s_deduped_2016_12_08.csv_Matchback
s3://nwd-imports/liveramp-matchbacks/2017-08/nwd_email_md5_file_2017_07_25.csv_Matchback.gz
s3://nwd-imports/liveramp-matchbacks/email_only_2016_02_07/nwd_md5s_2017_02_07.csv_Matchback
s3://nwd-imports/liveramp-matchbacks/liveramp_email_specific_matchback_032816_432gb.psv.zip

unzip liveramp_email_specific_matchback_032816_432gb.psv.zip


# ----------

aws s3 cp s3://nwd-exports/redshift/staging_businesses_v8.0.0_2019_02_01/staging_businesses_v8.0.0_2019_02_01.psv ./ &&\
mv staging_businesses_v8.0.0_2019_02_01.psv staging_businesses_2019_01_30.psv &&\
split -l 688204 production_company_matches_2019_02_02.psv production_company_matches_2019_02_02.part -da 4 --additional-suffix=".psv" &&\
rm production_company_matches_2019_02_02.psv &&\
tail -n +2 production_company_matches_2019_02_02.part0000.psv > production_company_matches_2019_02_02.part0000.psv.t &&\
mv production_company_matches_2019_02_02.part0000.psv.t production_company_matches_2019_02_02.part0000.psv &&\
aws s3 sync ./ s3://nwd-exports/redshift/production_company_matches/ &&\
rm ./*



aws s3 cp s3://nwd-exports/consumer-data/DOB_2018_10_10/DOB_2018_10_10.psv ./ &&\
gzip DOB_2018_10_10.psv &&\
aws s3 cp DOB_2018_10_10.psv.gz s3://nwd-exports/consumer-data/DOB_2018_10_10/ &&\
aws s3 rm s3://nwd-exports/consumer-data/DOB_2018_10_10/DOB_2018_10_10.psv &&\
rm DOB_2018_10_10.psv.gz

aws s3 cp s3://nwd-exports/consumer-data/Risk_2018_10_17/Risk_2018_10_17.psv ./ &&\
gzip Risk_2018_10_17.psv &&\
aws s3 cp Risk_2018_10_17.psv.gz s3://nwd-exports/consumer-data/Risk_2018_10_17/ &&\
aws s3 rm s3://nwd-exports/consumer-data/Risk_2018_10_17/Risk_2018_10_17.psv &&\
rm Risk_2018_10_17.psv.gz

aws s3 cp s3://nwd-exports/consumer-data/Voter_2018_10_05/Voter_2018_10_05.psv ./ &&\
gzip Voter_2018_10_05.psv &&\
aws s3 cp Voter_2018_10_05.psv.gz s3://nwd-exports/consumer-data/Voter_2018_10_05/ &&\
aws s3 rm s3://nwd-exports/consumer-data/Voter_2018_10_05/Voter_2018_10_05.psv &&\
rm Voter_2018_10_05.psv.gz



aws s3 cp s3://nwd-exports/consumer-data/Coreg_2019_02_13/Coreg_2019_02_13.psv ./ &&\
gzip Coreg_2019_02_13.psv &&\
aws s3 cp Coreg_2019_02_13.psv.gz s3://nwd-exports/consumer-data/Coreg_2019_02_13/ &&\
aws s3 rm s3://nwd-exports/consumer-data/Coreg_2019_02_13/Coreg_2019_02_13.psv &&\
rm Coreg_2019_02_13.psv.gz

