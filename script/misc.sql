load data local infile 'people_with_email_TN_additional_verified_20180613.unique.csv'
into table `B2B_22903_localblox_md5_20180620`
lines terminated by '\n'
(md5)
set id=null, source='people_with_email_TN_additional_verified_20180613.unique.csv';

load data local infile 'people_with_email_TN_unverified_20180613.unique.csv'
into table `B2B_22903_localblox_md5_20180620`
lines terminated by '\n'
(md5)
set id=null, source='people_with_email_TN_unverified_20180613.json.gz';

load data local infile 'people_with_email_TN_verified_20180613.unique.csv'
into table `B2B_22903_localblox_md5_20180620`
lines terminated by '\n'
(md5)
set id=null, source='people_with_email_TN_verified_20180613.json.gz';



load data local infile 'TOTLBIZDEV_1MM_TN_HashedEmail_MD5.csv'
into table `B2B_22903_localblox_md5_20180620`
lines terminated by '\n'
(md5)
set id=null, source='TOTLBIZDEV_1MM_TN_HashedEmail_MD5.csv';


load data local infile '53860_netwise_uscon_test.unique.csv'
into table `B2B_22903_localblox_md5_20180620`
lines terminated by '\n'
(md5)
set id=null, source='53860_netwise_uscon_test.txt';




load data local infile 'db_bombora_ip_domain_append_v2.csv'
into table `b2b_23234_ip_linkage_test_results`
fields terminated by '|' enclosed by '"' escaped by ''
lines terminated by '\n'
ignore 1 lines
(isISP,ipAddress,@probability,companyName,websiteURL,hqAddress1,hqAddress2,hqCity,hqStateProvReg,hqZip,hqCountry,parentCompany,parentAddress,parentCity,parentStateProvReg,parentZIP,parentCountry,phone,industry,orgWatch,revenue,employees,stockExchange,tickerSymbol,domain,@provider_record_id)
set id=null, provider_record_id=null, probability=nullif(@probability, '');
