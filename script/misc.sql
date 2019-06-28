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



load data local infile 'nwd_liveramp_2018_06_25_13_37_42.min.psv'
into table `B2B_23426_LiveRamp_Matchback_20180625`
fields terminated by '|'
lines terminated by '\n'
(
`person_id`,
`version`,
`adsquare - mobile aaid`,
`adsquare - mobile idfa`,
`appnexus - mobile aaid`,
`appnexus - cookie`,
`appnexus - mobile idfa`,
`bluekai - mobile aaid`,
`bluekai - cookie`,
`bluekai - mobile idfa`,
`bombora - cookie`,
`centro (sitescout) - mobile aaid`,
`centro (sitescout) - cookie`,
`centro (sitescout) - mobile idfa`,
`cross pixel - cookie`,
`demandbase - cookie`,
`exelate - cookie`,
`eyeota - cookie`,
`lotame - mobile aaid`,
`lotame - cookie`,
`lotame - mobile idfa`,
`mediamath - cookie`,
`quantcast - cookie`,
`tapfwd - mobile aaid`,
`tapfwd - mobile idfa`,
`tru optik - cookie`,
`the trade desk - cookie`,
`zeotap - mobile aaid`,
`zeotap - mobile idfa`,
`any_destination`
)
set id=null;

alter table B2B_23426_LiveRamp_Matchback_20180625
add index (`person_id`),
add index (`version`),
-- add index (`adsquare - mobile aaid`),
-- add index (`adsquare - mobile idfa`),
-- add index (`appnexus - mobile aaid`),
-- add index (`appnexus - cookie`),
-- add index (`appnexus - mobile idfa`),
-- add index (`bluekai - mobile aaid`),
-- add index (`bluekai - cookie`),
-- add index (`bluekai - mobile idfa`),
-- add index (`bombora - cookie`),
-- add index (`centro (sitescout) - mobile aaid`),
-- add index (`centro (sitescout) - cookie`),
-- add index (`centro (sitescout) - mobile idfa`),
-- add index (`cross pixel - cookie`),
-- add index (`demandbase - cookie`),
-- add index (`exelate - cookie`),
-- add index (`eyeota - cookie`),
-- add index (`lotame - mobile aaid`),
-- add index (`lotame - cookie`),
-- add index (`lotame - mobile idfa`),
-- add index (`mediamath - cookie`),
-- add index (`quantcast - cookie`),
-- add index (`tapfwd - mobile aaid`),
-- add index (`tapfwd - mobile idfa`),
-- add index (`tru optik - cookie`),
-- add index (`the trade desk - cookie`),
-- add index (`zeotap - mobile aaid`),
-- add index (`zeotap - mobile idfa`),
add index (`any_destination`)
;

load data local infile 'IT_LinkedInHandles_for_Netwise.paths.utf8.txt'
into table `B2B_23426_TechTarget_LI_URL_Paths`
fields terminated by ','
lines terminated by '\n'
(
source_url_path
)
set id=null;






load data local infile '2016-06-01.psv'
into table `onboarded_md5s`
fields terminated by '|' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(md5, onboarded_date)
set id=null
;

load data local infile '2016-12-08.psv'
into table `onboarded_md5s`
fields terminated by '|' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(md5, onboarded_date)
set id=null
;

load data local infile '2017-02-07.psv'
into table `onboarded_md5s`
fields terminated by '|' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(md5, onboarded_date)
set id=null
;

load data local infile '2017-06-22.psv'
into table `onboarded_md5s`
fields terminated by '|' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(md5, onboarded_date)
set id=null
;

load data local infile '2017-07-25.psv'
into table `onboarded_md5s`
fields terminated by '|' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(md5, onboarded_date)
set id=null
;

load data local infile '2018-01-18.psv'
into table `onboarded_md5s`
fields terminated by '|' enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines
(md5, onboarded_date)
set id=null
;

create table alc_consumer_segments_2018_09_05 (
id bigint(20) unsigned not null auto_increment,
`person_id` bigint(20) unsigned,
`first_name` varchar(100) not null,
`middle_initial` char(1) not null,
`last_name` varchar(100) not null,
`address1` varchar(255) not null,
`address2` varchar(255) not null,
`city` varchar(255) not null,
`state` char(2) not null,
`zip` varchar(16) not null
`zip4` char(4) not null,
`email_1` varchar(255),
`email_2` varchar(255),
`email_3` varchar(255),
`email_4` varchar(255),
`email_5` varchar(255),
`email_6` varchar(255),
`email_7` varchar(255),
`email_8` varchar(255),
`phone` varchar(25),
`affluent_dads` tinyint(1) not null,
`affluent_empty_nesters` tinyint(1) not null,
`affluent_families_with_children` tinyint(1) not null,
`affluent_moms` tinyint(1) not null,
`luxury_travelers` tinyint(1) not null,
`cruise_travel` tinyint(1) not null,
`domestic_travel` tinyint(1) not null,
`international_travel` tinyint(1) not null,
`travel_purchase_online` tinyint(1) not null,
`travel_purchase_retail` tinyint(1) not null,
`families_with_children_0_2` tinyint(1) not null,
`families_with_children_3_5` tinyint(1) not null,
`families_with_children_6_10` tinyint(1) not null,
`families_with_children_11_15` tinyint(1) not null,
`families_with_children_16_17` tinyint(1) not null,
`families_with_children_single_moms` tinyint(1) not null,
`families_with_children_single_dads` tinyint(1) not null,
`families_with_children_grandparents` tinyint(1) not null,
`childrens_product_purchases_baby_care` tinyint(1) not null,
`childrens_product_purchases_general` tinyint(1) not null,
`childrens_product_purchases_apparel` tinyint(1) not null,
`childrens_product_purchases_back_to_school` tinyint(1) not null,
`childrens_product_purchases_learning_and_activity_toys` tinyint(1) not null,
`purchases_antiques` tinyint(1) not null,
`purchases_apparel_childrens` tinyint(1) not null,
`purchases_apparel_mens` tinyint(1) not null,
`purchases_apparel_womens` tinyint(1) not null,
`purchases_art` tinyint(1) not null,
`purchases_baby_care` tinyint(1) not null,
`purchases_books` tinyint(1) not null,
`purchases_books_audio` tinyint(1) not null,
`purchases_childrens_back_to_school` tinyint(1) not null,
`purchases_childrens_general` tinyint(1) not null,
`purchases_childrens_learning_and_activity_toys` tinyint(1) not null,
`purchases_club_membership` tinyint(1) not null,
`purchases_collectibles` tinyint(1) not null,
`purchases_computing_home_office` tinyint(1) not null,
`purchases_crafts` tinyint(1) not null,
`purchases_dvds_videos` tinyint(1) not null,
`purchases_female_oriented_products_services` tinyint(1) not null,
`purchases_food_products_services` tinyint(1) not null,
`purchases_garden_products_services` tinyint(1) not null,
`purchases_gifts` tinyint(1) not null,
`purchases_health_and_beauty` tinyint(1) not null,
`purchases_high_end_appliances` tinyint(1) not null,
`purchases_hunting` tinyint(1) not null,
`purchases_jewelry` tinyint(1) not null,
`purchases_luggage` tinyint(1) not null,
`purchases_magazines` tinyint(1) not null,
`purchases_magazines_cooking` tinyint(1) not null,
`purchases_magazines_do_it_yourself` tinyint(1) not null,
`purchases_magazines_family` tinyint(1) not null,
`purchases_magazines_female_oriented` tinyint(1) not null,
`purchases_magazines_financial` tinyint(1) not null,
`purchases_magazines_gardening` tinyint(1) not null,
`purchases_magazines_health` tinyint(1) not null,
`purchases_magazines_male_oriented` tinyint(1) not null,
`purchases_magazines_photography` tinyint(1) not null,
`purchases_magazines_money` tinyint(1) not null,
`purchases_male_oriented_products_services` tinyint(1) not null,
`purchases_military_momorabilia_weaponry` tinyint(1) not null,
`purchases_musical_instruments` tinyint(1) not null,
`purchases_pets` tinyint(1) not null,
`purchases_photography_video_equipment` tinyint(1) not null,
`purchases_upscale_products_services` tinyint(1) not null,
`purchases_travel` tinyint(1) not null,
`houseshold_income_<75000` tinyint(1) not null,
`houseshold_income_75000_99999` tinyint(1) not null,
`houseshold_income_100000_149999` tinyint(1) not null,
`houseshold_income_150000_199999` tinyint(1) not null,
`houseshold_income_200000_249999` tinyint(1) not null,
`houseshold_income_>250000` tinyint(1) not null,
`affluent_consumers` tinyint(1) not null,
`home_owners` tinyint(1) not null,
`renters` tinyint(1) not null,
`home_value_<100000` tinyint(1) not null,
`home_value_100000_199999` tinyint(1) not null,
`home_value_200000_299999` tinyint(1) not null,
`home_value_300000_399999` tinyint(1) not null,
`home_value_400000_499999` tinyint(1) not null,
`home_value_500000_749999` tinyint(1) not null,
`home_value_750000_999999` tinyint(1) not null,
`home_value_>1000000` tinyint(1) not null,
`credit_cards_american_express_gold_premium` tinyint(1) not null,
`credit_cards_american_express_regular` tinyint(1) not null,
`credit_cards_discover_gold_premium` tinyint(1) not null,
`credit_cards_discover_regular` tinyint(1) not null,
`credit_cards_mastercard_gold_premium` tinyint(1) not null,
`credit_cards_mastercard_regular` tinyint(1) not null,
`credit_cards_retail_gas_premium` tinyint(1) not null,
`credit_cards_retail_gas_regular` tinyint(1) not null,
`credit_cards_upscale_department_store` tinyint(1) not null,
`credit_cards_visa_gold_premium` tinyint(1) not null,
`credit_cards_visa_regular` tinyint(1) not null,
`credit_rating_>800` tinyint(1) not null,
`credit_rating_700_799` tinyint(1) not null,
`credit_rating_600_699` tinyint(1) not null,
`credit_rating_500_599` tinyint(1) not null,
`credit_rating_<500` tinyint(1) not null,
`seasonal_back_to_school_shoppers` tinyint(1) not null,
`seasonal_affluent_back_to_school_shoppers` tinyint(1) not null,
`seasonal_back_to_school_shoppers_with_pre_school_aged_kids` tinyint(1) not null,
`seasonal_back_to_school_shoppers_with_elementary_school_aged_kids` tinyint(1) not null,
`seasonal_back_to_school_shoppers_with_middle_school_aged_kids` tinyint(1) not null,
`seasonal_back_to_school_shoppers_with_high_school_aged_kids` tinyint(1) not null,
`seasonal_valentines_day_shoppers` tinyint(1) not null,
`seasonal_affluent_valentines_day_shoppers` tinyint(1) not null,
`seasonal_presidents_day_shoppers` tinyint(1) not null,
`seasonal_easter_shoppers` tinyint(1) not null,
`seasonal_mothers_day_shoppers` tinyint(1) not null,
`seasonal_fathers_day_shoppers` tinyint(1) not null,
`seasonal_independence_day_shoppers` tinyint(1) not null,
`seasonal_halloween_shoppers` tinyint(1) not null,
`seasonal_thanksgiving_shoppers` tinyint(1) not null,
`seasonal_black_friday_shoppers` tinyint(1) not null,
`seasonal_cyber_monday_shoppers` tinyint(1) not null,
`seasonal_christmas_and_holiday_shoppers` tinyint(1) not null,
`seasonal_affluent_christmas_and_holiday_shoppers` tinyint(1) not null,
`seasonal_new_years_shoppers` tinyint(1) not null,
primary key (id)
) engine=innodb collate=utf8_unicode_ci;


load data local infile 'ALC_Aggregated_20190226.psv'
into table `alc_consumer_segments_2019_02_27`
fields terminated by '|' escaped by '\\'
lines terminated by '\r\n'
(@person_id,
first_name,
middle_initial,
last_name,
address1,
address2,
city,
state,
zip,
zip4,
@email_1,
@email_2,
@email_3,
@email_4,
@email_5,
@email_6,
@email_7,
@email_8,
phone,
@affluent_dads,
@affluent_empty_nesters,
@affluent_families_with_children,
@affluent_moms,
@luxury_travelers,
@cruise_travel,
@domestic_travel,
@international_travel,
@travel_purchase_online,
@travel_purchase_retail,
@families_with_children_0_2,
@families_with_children_3_5,
@families_with_children_6_10,
@families_with_children_11_15,
@families_with_children_16_17,
@families_with_children_single_moms,
@families_with_children_single_dads,
@families_with_children_grandparents,
@childrens_product_purchases_baby_care,
@childrens_product_purchases_general,
@childrens_product_purchases_apparel,
@childrens_product_purchases_back_to_school,
@childrens_product_purchases_learning_and_activity_toys,
@purchases_antiques,
@purchases_apparel_childrens,
@purchases_apparel_mens,
@purchases_apparel_womens,
@purchases_art,
@purchases_baby_care,
@purchases_books,
@purchases_books_audio,
@purchases_childrens_back_to_school,
@purchases_childrens_general,
@purchases_childrens_learning_and_activity_toys,
@purchases_club_membership,
@purchases_collectibles,
@purchases_computing_home_office,
@purchases_crafts,
@purchases_dvds_videos,
@purchases_female_oriented_products_services,
@purchases_food_products_services,
@purchases_garden_products_services,
@purchases_gifts,
@purchases_health_and_beauty,
@purchases_high_end_appliances,
@purchases_hunting,
@purchases_jewelry,
@purchases_luggage,
@purchases_magazines,
@purchases_magazines_cooking,
@purchases_magazines_do_it_yourself,
@purchases_magazines_family,
@purchases_magazines_female_oriented,
@purchases_magazines_financial,
@purchases_magazines_gardening,
@purchases_magazines_health,
@purchases_magazines_male_oriented,
@purchases_magazines_photography,
@purchases_magazines_money,
@purchases_male_oriented_products_services,
@purchases_military_momorabilia_weaponry,
@purchases_musical_instruments,
@purchases_pets,
@purchases_photography_video_equipment,
@purchases_upscale_products_services,
@purchases_travel,
@houseshold_income_lt75000,
@houseshold_income_75000_99999,
@houseshold_income_100000_149999,
@houseshold_income_150000_199999,
@houseshold_income_200000_249999,
@houseshold_income_gt250000,
@affluent_consumers,
@home_owners,
@renters,
@home_value_lt100000,
@home_value_100000_199999,
@home_value_200000_299999,
@home_value_300000_399999,
@home_value_400000_499999,
@home_value_500000_749999,
@home_value_750000_999999,
@home_value_gt1000000,
@credit_cards_american_express_gold_premium,
@credit_cards_american_express_regular,
@credit_cards_discover_gold_premium,
@credit_cards_discover_regular,
@credit_cards_mastercard_gold_premium,
@credit_cards_mastercard_regular,
@credit_cards_retail_gas_premium,
@credit_cards_retail_gas_regular,
@credit_cards_upscale_department_store,
@credit_cards_visa_gold_premium,
@credit_cards_visa_regular,
@credit_rating_gt800,
@credit_rating_700_799,
@credit_rating_600_699,
@credit_rating_500_599,
@credit_rating_lt500,
@seasonal_back_to_school_shoppers,
@seasonal_affluent_back_to_school_shoppers,
@seasonal_back_to_school_shoppers_with_pre_school_aged_kids,
@seasonal_back_to_school_shoppers_with_elem_school_aged_kids,
@seasonal_back_to_school_shoppers_with_middle_school_aged_kids,
@seasonal_back_to_school_shoppers_with_high_school_aged_kids,
@seasonal_valentines_day_shoppers,
@seasonal_affluent_valentines_day_shoppers,
@seasonal_presidents_day_shoppers,
@seasonal_easter_shoppers,
@seasonal_mothers_day_shoppers,
@seasonal_fathers_day_shoppers,
@seasonal_independence_day_shoppers,
@seasonal_halloween_shoppers,
@seasonal_thanksgiving_shoppers,
@seasonal_black_friday_shoppers,
@seasonal_cyber_monday_shoppers,
@seasonal_christmas_and_holiday_shoppers,
@seasonal_affluent_christmas_and_holiday_shoppers,
@seasonal_new_years_shoppers)
SET id=NULL,
person_id = NULLIF(@person_id, ''),
email_1 = NULLIF(@email_1, ''),
email_2 = NULLIF(@email_2, ''),
email_3 = NULLIF(@email_3, ''),
email_4 = NULLIF(@email_4, ''),
email_5 = NULLIF(@email_5, ''),
email_6 = NULLIF(@email_6, ''),
email_7 = NULLIF(@email_7, ''),
email_8 = NULLIF(@email_8, ''),
`affluent_dads` = COALESCE(NULLIF(@affluent_dads, ''), 0),
`affluent_empty_nesters` = COALESCE(NULLIF(@affluent_empty_nesters, ''), 0),
`affluent_families_with_children` = COALESCE(NULLIF(@affluent_families_with_children, ''), 0),
`affluent_moms` = COALESCE(NULLIF(@affluent_moms, ''), 0),
`luxury_travelers` = COALESCE(NULLIF(@luxury_travelers, ''), 0),
`cruise_travel` = COALESCE(NULLIF(@cruise_travel, ''), 0),
`domestic_travel` = COALESCE(NULLIF(@domestic_travel, ''), 0),
`international_travel` = COALESCE(NULLIF(@international_travel, ''), 0),
`travel_purchase_online` = COALESCE(NULLIF(@travel_purchase_online, ''), 0),
`travel_purchase_retail` = COALESCE(NULLIF(@travel_purchase_retail, ''), 0),
`families_with_children_0_2` = COALESCE(NULLIF(@families_with_children_0_2, ''), 0),
`families_with_children_3_5` = COALESCE(NULLIF(@families_with_children_3_5, ''), 0),
`families_with_children_6_10` = COALESCE(NULLIF(@families_with_children_6_10, ''), 0),
`families_with_children_11_15` = COALESCE(NULLIF(@families_with_children_11_15, ''), 0),
`families_with_children_16_17` = COALESCE(NULLIF(@families_with_children_16_17, ''), 0),
`families_with_children_single_moms` = COALESCE(NULLIF(@families_with_children_single_moms, ''), 0),
`families_with_children_single_dads` = COALESCE(NULLIF(@families_with_children_single_dads, ''), 0),
`families_with_children_grandparents` = COALESCE(NULLIF(@families_with_children_grandparents, ''), 0),
`childrens_product_purchases_baby_care` = COALESCE(NULLIF(@childrens_product_purchases_baby_care, ''), 0),
`childrens_product_purchases_general` = COALESCE(NULLIF(@childrens_product_purchases_general, ''), 0),
`childrens_product_purchases_apparel` = COALESCE(NULLIF(@childrens_product_purchases_apparel, ''), 0),
`childrens_product_purchases_back_to_school` = COALESCE(NULLIF(@childrens_product_purchases_back_to_school, ''), 0),
`childrens_product_purchases_learning_and_activity_toys` = COALESCE(NULLIF(@childrens_product_purchases_learning_and_activity_toys, ''), 0),
`purchases_antiques` = COALESCE(NULLIF(@purchases_antiques, ''), 0),
`purchases_apparel_childrens` = COALESCE(NULLIF(@purchases_apparel_childrens, ''), 0),
`purchases_apparel_mens` = COALESCE(NULLIF(@purchases_apparel_mens, ''), 0),
`purchases_apparel_womens` = COALESCE(NULLIF(@purchases_apparel_womens, ''), 0),
`purchases_art` = COALESCE(NULLIF(@purchases_art, ''), 0),
`purchases_baby_care` = COALESCE(NULLIF(@purchases_baby_care, ''), 0),
`purchases_books` = COALESCE(NULLIF(@purchases_books, ''), 0),
`purchases_books_audio` = COALESCE(NULLIF(@purchases_books_audio, ''), 0),
`purchases_childrens_back_to_school` = COALESCE(NULLIF(@purchases_childrens_back_to_school, ''), 0),
`purchases_childrens_general` = COALESCE(NULLIF(@purchases_childrens_general, ''), 0),
`purchases_childrens_learning_and_activity_toys` = COALESCE(NULLIF(@purchases_childrens_learning_and_activity_toys, ''), 0),
`purchases_club_membership` = COALESCE(NULLIF(@purchases_club_membership, ''), 0),
`purchases_collectibles` = COALESCE(NULLIF(@purchases_collectibles, ''), 0),
`purchases_computing_home_office` = COALESCE(NULLIF(@purchases_computing_home_office, ''), 0),
`purchases_crafts` = COALESCE(NULLIF(@purchases_crafts, ''), 0),
`purchases_dvds_videos` = COALESCE(NULLIF(@purchases_dvds_videos, ''), 0),
`purchases_female_oriented_products_services` = COALESCE(NULLIF(@purchases_female_oriented_products_services, ''), 0),
`purchases_food_products_services` = COALESCE(NULLIF(@purchases_food_products_services, ''), 0),
`purchases_garden_products_services` = COALESCE(NULLIF(@purchases_garden_products_services, ''), 0),
`purchases_gifts` = COALESCE(NULLIF(@purchases_gifts, ''), 0),
`purchases_health_and_beauty` = COALESCE(NULLIF(@purchases_health_and_beauty, ''), 0),
`purchases_high_end_appliances` = COALESCE(NULLIF(@purchases_high_end_appliances, ''), 0),
`purchases_hunting` = COALESCE(NULLIF(@purchases_hunting, ''), 0),
`purchases_jewelry` = COALESCE(NULLIF(@purchases_jewelry, ''), 0),
`purchases_luggage` = COALESCE(NULLIF(@purchases_luggage, ''), 0),
`purchases_magazines` = COALESCE(NULLIF(@purchases_magazines, ''), 0),
`purchases_magazines_cooking` = COALESCE(NULLIF(@purchases_magazines_cooking, ''), 0),
`purchases_magazines_do_it_yourself` = COALESCE(NULLIF(@purchases_magazines_do_it_yourself, ''), 0),
`purchases_magazines_family` = COALESCE(NULLIF(@purchases_magazines_family, ''), 0),
`purchases_magazines_female_oriented` = COALESCE(NULLIF(@purchases_magazines_female_oriented, ''), 0),
`purchases_magazines_financial` = COALESCE(NULLIF(@purchases_magazines_financial, ''), 0),
`purchases_magazines_gardening` = COALESCE(NULLIF(@purchases_magazines_gardening, ''), 0),
`purchases_magazines_health` = COALESCE(NULLIF(@purchases_magazines_health, ''), 0),
`purchases_magazines_male_oriented` = COALESCE(NULLIF(@purchases_magazines_male_oriented, ''), 0),
`purchases_magazines_photography` = COALESCE(NULLIF(@purchases_magazines_photography, ''), 0),
`purchases_magazines_money` = COALESCE(NULLIF(@purchases_magazines_money, ''), 0),
`purchases_male_oriented_products_services` = COALESCE(NULLIF(@purchases_male_oriented_products_services, ''), 0),
`purchases_military_momorabilia_weaponry` = COALESCE(NULLIF(@purchases_military_momorabilia_weaponry, ''), 0),
`purchases_musical_instruments` = COALESCE(NULLIF(@purchases_musical_instruments, ''), 0),
`purchases_pets` = COALESCE(NULLIF(@purchases_pets, ''), 0),
`purchases_photography_video_equipment` = COALESCE(NULLIF(@purchases_photography_video_equipment, ''), 0),
`purchases_upscale_products_services` = COALESCE(NULLIF(@purchases_upscale_products_services, ''), 0),
`purchases_travel` = COALESCE(NULLIF(@purchases_travel, ''), 0),
`houseshold_income_<75000` = COALESCE(NULLIF(@houseshold_income_lt75000, ''), 0),
`houseshold_income_75000_99999` = COALESCE(NULLIF(@houseshold_income_75000_99999, ''), 0),
`houseshold_income_100000_149999` = COALESCE(NULLIF(@houseshold_income_100000_149999, ''), 0),
`houseshold_income_150000_199999` = COALESCE(NULLIF(@houseshold_income_150000_199999, ''), 0),
`houseshold_income_200000_249999` = COALESCE(NULLIF(@houseshold_income_200000_249999, ''), 0),
`houseshold_income_>250000` = COALESCE(NULLIF(@houseshold_income_gt250000, ''), 0),
`affluent_consumers` = COALESCE(NULLIF(@affluent_consumers, ''), 0),
`home_owners` = COALESCE(NULLIF(@home_owners, ''), 0),
`renters` = COALESCE(NULLIF(@renters, ''), 0),
`home_value_<100000` = COALESCE(NULLIF(@home_value_lt100000, ''), 0),
`home_value_100000_199999` = COALESCE(NULLIF(@home_value_100000_199999, ''), 0),
`home_value_200000_299999` = COALESCE(NULLIF(@home_value_200000_299999, ''), 0),
`home_value_300000_399999` = COALESCE(NULLIF(@home_value_300000_399999, ''), 0),
`home_value_400000_499999` = COALESCE(NULLIF(@home_value_400000_499999, ''), 0),
`home_value_500000_749999` = COALESCE(NULLIF(@home_value_500000_749999, ''), 0),
`home_value_750000_999999` = COALESCE(NULLIF(@home_value_750000_999999, ''), 0),
`home_value_>1000000` = COALESCE(NULLIF(@home_value_gt1000000, ''), 0),
`credit_cards_american_express_gold_premium` = COALESCE(NULLIF(@credit_cards_american_express_gold_premium, ''), 0),
`credit_cards_american_express_regular` = COALESCE(NULLIF(@credit_cards_american_express_regular, ''), 0),
`credit_cards_discover_gold_premium` = COALESCE(NULLIF(@credit_cards_discover_gold_premium, ''), 0),
`credit_cards_discover_regular` = COALESCE(NULLIF(@credit_cards_discover_regular, ''), 0),
`credit_cards_mastercard_gold_premium` = COALESCE(NULLIF(@credit_cards_mastercard_gold_premium, ''), 0),
`credit_cards_mastercard_regular` = COALESCE(NULLIF(@credit_cards_mastercard_regular, ''), 0),
`credit_cards_retail_gas_premium` = COALESCE(NULLIF(@credit_cards_retail_gas_premium, ''), 0),
`credit_cards_retail_gas_regular` = COALESCE(NULLIF(@credit_cards_retail_gas_regular, ''), 0),
`credit_cards_upscale_department_store` = COALESCE(NULLIF(@credit_cards_upscale_department_store, ''), 0),
`credit_cards_visa_gold_premium` = COALESCE(NULLIF(@credit_cards_visa_gold_premium, ''), 0),
`credit_cards_visa_regular` = COALESCE(NULLIF(@credit_cards_visa_regular, ''), 0),
`credit_rating_>800` = COALESCE(NULLIF(@credit_rating_gt800, ''), 0),
`credit_rating_700_799` = COALESCE(NULLIF(@credit_rating_700_799, ''), 0),
`credit_rating_600_699` = COALESCE(NULLIF(@credit_rating_600_699, ''), 0),
`credit_rating_500_599` = COALESCE(NULLIF(@credit_rating_500_599, ''), 0),
`credit_rating_<500` = COALESCE(NULLIF(@credit_rating_lt500, ''), 0),
`seasonal_back_to_school_shoppers` = COALESCE(NULLIF(@seasonal_back_to_school_shoppers, ''), 0),
`seasonal_affluent_back_to_school_shoppers` = COALESCE(NULLIF(@seasonal_affluent_back_to_school_shoppers, ''), 0),
`seasonal_back_to_school_shoppers_with_pre_school_aged_kids` = COALESCE(NULLIF(@seasonal_back_to_school_shoppers_with_pre_school_aged_kids, ''), 0),
`seasonal_back_to_school_shoppers_with_elem_school_aged_kids` = COALESCE(NULLIF(@seasonal_back_to_school_shoppers_with_elem_school_aged_kids, ''), 0),
`seasonal_back_to_school_shoppers_with_middle_school_aged_kids` = COALESCE(NULLIF(@seasonal_back_to_school_shoppers_with_middle_school_aged_kids, ''), 0),
`seasonal_back_to_school_shoppers_with_high_school_aged_kids` = COALESCE(NULLIF(@seasonal_back_to_school_shoppers_with_high_school_aged_kids, ''), 0),
`seasonal_valentines_day_shoppers` = COALESCE(NULLIF(@seasonal_valentines_day_shoppers, ''), 0),
`seasonal_affluent_valentines_day_shoppers` = COALESCE(NULLIF(@seasonal_affluent_valentines_day_shoppers, ''), 0),
`seasonal_presidents_day_shoppers` = COALESCE(NULLIF(@seasonal_presidents_day_shoppers, ''), 0),
`seasonal_easter_shoppers` = COALESCE(NULLIF(@seasonal_easter_shoppers, ''), 0),
`seasonal_mothers_day_shoppers` = COALESCE(NULLIF(@seasonal_mothers_day_shoppers, ''), 0),
`seasonal_fathers_day_shoppers` = COALESCE(NULLIF(@seasonal_fathers_day_shoppers, ''), 0),
`seasonal_independence_day_shoppers` = COALESCE(NULLIF(@seasonal_independence_day_shoppers, ''), 0),
`seasonal_halloween_shoppers` = COALESCE(NULLIF(@seasonal_halloween_shoppers, ''), 0),
`seasonal_thanksgiving_shoppers` = COALESCE(NULLIF(@seasonal_thanksgiving_shoppers, ''), 0),
`seasonal_black_friday_shoppers` = COALESCE(NULLIF(@seasonal_black_friday_shoppers, ''), 0),
`seasonal_cyber_monday_shoppers` = COALESCE(NULLIF(@seasonal_cyber_monday_shoppers, ''), 0),
`seasonal_christmas_and_holiday_shoppers` = COALESCE(NULLIF(@seasonal_christmas_and_holiday_shoppers, ''), 0),
`seasonal_affluent_christmas_and_holiday_shoppers` = COALESCE(NULLIF(@seasonal_affluent_christmas_and_holiday_shoppers, ''), 0),
`seasonal_new_years_shoppers` = COALESCE(NULLIF(@seasonal_new_years_shoppers, ''), 0);


alter table alc_consumer_segments_2018_10_12
add index (email_1)
;



load data local infile 'Netwise_li_urls_hashed.txt'
into table `B2B_23992_insideview_shorthand_name_hash`
fields terminated by '|'
lines terminated by '\n'
(canonical_shorthand_name_hash)
set id=null
;



load data local infile 'isp_ips.csv'
into table `B2B_24113_aberdeen_ips`
fields terminated by '|'
lines terminated by '\n'
ignore 1 lines
(ip_address)
set id=null
;

load data local infile 'aberdeen.tsv.dedup'
into table `B2B_24113_aberdeen_ips_with_emails`
fields terminated by '\t'
lines terminated by '\n'
ignore 1 lines
(ip_address, email)
set id=null
;


select pp.first_name,
    pp.last_name,
    pp.title,
    pc.name AS company_name,
    (SELECT CASE
            WHEN `production_validated_emails_v8.0.2`.status = 'Ok' THEN `production_business_emails_v8.0.0`.email
            WHEN `production_validated_emails_v8.0.2`.status = 'Bad' THEN NULL
            WHEN `production_validated_emails_v8.0.2`.hard_bounce = 1 THEN NULL
            WHEN `production_validated_emails_v8.0.2`.additional_status_info = 'ServerIsCatchAll' THEN `production_business_emails_v8.0.0`.email
            WHEN `production_validated_emails_v8.0.2`.id IS NOT NULL THEN NULL
            WHEN `production_validated_email_domains_v8.0.1`.most_recent_additional_email_status_info IN ('DomainIsInexistent', 'NoMxServersFound') THEN NULL
            WHEN `production_validated_email_domains_v8.0.1`.most_recent_additional_email_status_info = 'ServerIsCatchAll' THEN `production_business_emails_v8.0.0`.email
            WHEN `production_validated_emails_v8.0.2`.id IS NULL THEN NULL
            ELSE NULL
        END
        FROM `production_business_emails_v8.0.0`
        LEFT OUTER JOIN `production_validated_emails_v8.0.2` ON `production_business_emails_v8.0.0`.md5 = `production_validated_emails_v8.0.2`.md5
        LEFT OUTER JOIN `production_validated_email_domains_v8.0.1` ON `production_business_emails_v8.0.0`.domain = `production_validated_email_domains_v8.0.1`.email_domain
        WHERE pp.id = `production_business_emails_v8.0.0`.person_id
        ORDER BY FIELD(`production_validated_emails_v8.0.2`.status,'Ok') DESC,
            `production_business_emails_v8.0.0`.best_guess DESC,
            FIELD(`production_validated_email_domains_v8.0.1`.most_recent_additional_email_status_info,'ServerIsCatchAll') DESC,
            FIELD(`production_validated_emails_v8.0.2`.status,'Bad') ASC,
            `production_validated_emails_v8.0.2`.hard_bounce ASC,
            `production_business_emails_v8.0.0`.guess_confidence DESC,
            FIELD( `production_business_emails_v8.0.0`.`pattern`, 'filn','ln','fn.ln','fnln','lnfi','fnli','fn','fi.ln','fn_ln','fn.li','fi_ln','fn_li' ) ASC,
            `production_business_emails_v8.0.0`.id ASC
        LIMIT 1) as b2b_email,
    a.ip_address,
    pp.company_phone_waterfall,
    pp.linkedin_profile_url
from `production_persons_v8.0.0` pp
inner JOIN `production_companies_v8.0.0` pc on pp.matched_processed_company_id = pc.id
inner join `production_person_consumer_matches_v8.0.0` ppc on pp.id = ppc.person_id
inner join ticket_tables.B2B_24113_aberdeen_ips_with_emails a on ppc.email = a.email
where pp.personal_country = 'United States'
;


------------------------------

create table temp.name_city_state (
object_id char(32),
cleansed_name varchar(1024) distkey,
compiled_city varchar(200),
compiled_state varchar(1024),
domain varchar(400),
source_id INTEGER
)
sortkey (cleansed_name, compiled_city, compiled_state);

insert into temp.name_city_state
select b.object_id,
    b.cleansed_name,
    bl.compiled_city,
    bl.compiled_state,
    bd.domain,
    o.source_id
from staging.objects o
inner join staging.businesses b on o.object_id = b.object_id
inner join staging.business_locations bl on o.object_id = bl.object_id
left outer join staging.business_domains bd on o.object_id = bd.object_id
where b.cleansed_name <> ''
    and bl.compiled_city <> ''
    and bl.compiled_state <> ''
;

CREATE TABLE temp.grouped AS
SELECT min(b.object_id) as object_id,
    b.cleansed_name,
    bl.compiled_city,
    bl.compiled_state,
    bd.domain,
    count(DISTINCT o.source_id) AS num_sources
FROM objects o
INNER JOIN businesses b ON o.object_id = b.object_id
INNER JOIN business_locations bl ON o.object_id = bl.object_id
LEFT OUTER JOIN business_domains bd ON o.object_id = bd.object_id
WHERE b.cleansed_name <> ''
    AND bl.compiled_city <> ''
    AND bl.compiled_state <> ''
--    AND bd.domain <> ''
GROUP BY b.cleansed_name,
    bl.compiled_city,
    bl.compiled_state,
    bd.domain
HAVING (domain IS NOT NULL OR num_sources > 1)
;



CREATE TABLE temp.grouped_best_domain AS
SELECT object_id,
    cleansed_name,
    compiled_city,
    compiled_state,
    domain
FROM (
SELECT object_id,
    cleansed_name,
    compiled_city,
    compiled_state,
    domain,
    row_number() OVER (PARTITION by cleansed_name, compiled_city, compiled_state ORDER BY ISNULL(domain) ASC, num_sources desc) AS row_num
FROM temp.grouped
) t
WHERE row_num = 1
;

CREATE TABLE company_match AS 
SELECT min(b.object_id), 
FROM staging.businesses b
INNER JOIN staging.business_locations bl ON b.object_id = bl.object_id
INNER JOIN temp.grouped_best_domain d ON b.cleansed_name = d.cleansed_name
    AND bl.compiled_city = d.compiled_city
    AND bl.compiled_state = d.compiled_state
-- LEFT OUTER JOIN staging.business_domains bd ON b.object_id = bd.object_id
;


-----------
DROP TABLE if EXISTS temp.name_city_state;
CREATE TABLE temp.name_city_state (
object_id char(32) encode zstd,
cleansed_name varchar(1024) encode raw distkey,
compiled_city varchar(200) encode zstd,
compiled_state varchar(1024) encode zstd,
domain varchar(400) encode zstd,
source_id INTEGER encode zstd
)
sortkey (cleansed_name, compiled_city, compiled_state, domain);

INSERT INTO temp.name_city_state
SELECT b.object_id,
    b.cleansed_name,
    LOWER(bl.compiled_city),
    LOWER(bl.compiled_state),
    bd.domain,
    o.source_id
FROM staging.objects o
INNER JOIN staging.businesses b ON o.object_id = b.object_id
INNER JOIN staging.business_locations bl ON o.object_id = bl.object_id
LEFT OUTER JOIN staging.business_domains bd ON o.object_id = bd.object_id
WHERE b.cleansed_name <> ''
    AND bl.compiled_city <> ''
    AND bl.compiled_state <> ''
;


DROP TABLE if EXISTS temp.grouped;
CREATE TABLE temp.grouped (
match_id char(32) encode zstd,
cleansed_name varchar(1024) encode raw distkey,
compiled_city varchar(200) encode zstd,
compiled_state varchar(1024) encode zstd,
domain varchar(400) encode zstd,
num_sources INTEGER encode zstd
)
sortkey (cleansed_name, compiled_city, compiled_state, domain);

INSERT INTO temp.grouped
SELECT min(object_id) AS match_id,
    cleansed_name,
    compiled_city,
    compiled_state,
    domain,
    count(DISTINCT source_id) AS num_sources
FROM temp.name_city_state ncs
GROUP BY cleansed_name,
    compiled_city,
    compiled_state,
    domain
HAVING (domain IS NOT NULL OR num_sources > 1)
;


DROP TABLE if EXISTS temp.grouped_best_domain;
CREATE TABLE temp.grouped_best_domain (
match_id char(32) encode zstd,
cleansed_name varchar(1024) encode zstd,
compiled_city varchar(200) encode zstd,
compiled_state varchar(1024) encode zstd,
domain varchar(400) encode raw distkey
)
sortkey (domain, cleansed_name, compiled_city, compiled_state);

INSERT INTO temp.grouped_best_domain
SELECT match_id,
    cleansed_name,
    compiled_city,
    compiled_state,
    domain
FROM (
SELECT match_id,
    cleansed_name,
    compiled_city,
    compiled_state,
    domain,
    row_number() OVER (PARTITION by cleansed_name, compiled_city, compiled_state ORDER BY ISNULL(domain) ASC, num_sources desc) AS row_num
FROM temp.grouped
) t
WHERE row_num = 1
;


DROP TABLE if EXISTS temp.grouped_domain_match;
CREATE TABLE temp.grouped_domain_match (
match_id char(32) encode zstd,
cleansed_name varchar(1024) encode raw distkey,
compiled_city varchar(200) encode zstd,
compiled_state varchar(1024) encode zstd,
domain varchar(400) encode zstd
)
sortkey (cleansed_name, compiled_city, compiled_state, domain);

INSERT INTO temp.grouped_domain_match
SELECT min(d2.match_id) AS match_id,
    d.cleansed_name,
    d.compiled_city,
    d.compiled_state,
    d.domain
FROM temp.grouped_best_domain d
INNER JOIN temp.grouped_best_domain d2 ON d.domain = d2.domain
GROUP BY
    d.cleansed_name,
    d.compiled_city,
    d.compiled_state,
    d.domain
;

INSERT INTO temp.grouped_domain_match
SELECT d.match_id,
    d.cleansed_name,
    d.compiled_city,
    d.compiled_state,
    d.domain
FROM temp.grouped_best_domain d
WHERE d.domain IS NULL
;

vacuum temp.grouped_domain_match to 100 percent;

DROP TABLE if EXISTS temp.company_match;
CREATE TABLE temp.company_match (
match_id char(32) encode zstd,
object_id char(32) encode raw distkey
)
sortkey (object_id);

INSERT INTO temp.company_match
SELECT d.match_id, ncs.object_id
FROM temp.name_city_state ncs
INNER JOIN temp.grouped_domain_match d ON ncs.cleansed_name = d.cleansed_name
    AND ncs.compiled_city = d.compiled_city
    AND ncs.compiled_state = d.compiled_state
WHERE ncs.cleansed_name = d.cleansed_name
;


-- add in domain records
DROP TABLE if EXISTS temp.domain;
CREATE TABLE temp.domain (
object_id char(32) encode zstd,
domain varchar(400) encode raw distkey,
source_id INTEGER encode zstd
)
sortkey (domain);

INSERT INTO temp.domain
SELECT bd.object_id,
    bd.domain,
    o.source_id
FROM staging.objects o
INNER JOIN staging.business_domains bd ON o.object_id = bd.object_id
LEFT OUTER JOIN temp.company_match m ON o.object_id = m.object_id
WHERE bd.domain <> ''
    AND m.object_id IS NULL
;

-- add in where domains match
INSERT INTO temp.company_match
SELECT DISTINCT m.match_id, d.object_id
FROM temp.grouped_domain_match m
INNER JOIN temp.domain d ON m.domain = d.domain
;

vacuum temp.company_match TO 100 percent;



TRUNCATE TABLE temp.domain;
INSERT INTO temp.domain
SELECT bd.object_id,
    bd.domain,
    o.source_id
FROM staging.objects o
INNER JOIN staging.business_domains bd ON o.object_id = bd.object_id
LEFT OUTER JOIN temp.company_match m ON o.object_id = m.object_id
WHERE bd.domain <> ''
    AND m.object_id IS NULL
;

-- add in where domains don't match
INSERT INTO temp.company_match
SELECT min(d.object_id) AS match_id, d2.object_id
FROM temp.domain d
INNER JOIN temp.domain d2 ON d.domain = d2.domain
GROUP BY d2.object_id
HAVING count(DISTINCT d.source_id) > 1
;


vacuum temp.company_match TO 100 percent;



UNLOAD ('SELECT * FROM export.athena_contacts')
TO 's3://nwd-athena-production/consolidated/2019_05_07/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
FORMAT AS CSV
NULL AS ''
;



CREATE TABLE temp.emails_to_onboard (
    md5 CHAR(32) encode raw sortkey distkey
)
;

INSERT INTO temp.emails_to_onboard
SELECT DISTINCT MD5(email) FROM staging.emails
WHERE email <> ''
;

VACUUM temp.emails_to_onboard TO 100 PERCENT;
ANALYZE temp.emails_to_onboard;

UNLOAD ('SELECT md5 AS email_md5, md5 AS file_wide_field FROM temp.emails_to_onboard')
TO 's3://nwd-exports/liveramp/matchbacks/20190529/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
FORMAT AS CSV
HEADER
NULL AS '';



SELECT
    COUNT(NULLIF(address1, '')) AS address1,
    COUNT(NULLIF(address1_global, '')) AS address1_global,
    COUNT(NULLIF(address1_usa, '')) AS address1_usa,
    COUNT(NULLIF(address2, '')) AS address2,
    COUNT(NULLIF(address2_global, '')) AS address2_global,
    COUNT(NULLIF(address2_usa, '')) AS address2_usa,
    COUNT(NULLIF(city, '')) AS city,
    COUNT(NULLIF(city_global, '')) AS city_global,
    COUNT(NULLIF(city_usa, '')) AS city_usa,
    COUNT(NULLIF(corporate_entity_type, '')) AS corporate_entity_type,
    COUNT(NULLIF(country, '')) AS country,
    COUNT(NULLIF(country_global, '')) AS country_global,
    COUNT(NULLIF(country_usa, '')) AS country_usa,
    COUNT(NULLIF(description, '')) AS description,
    COUNT(NULLIF(domain, '')) AS domain,
    COUNT(NULLIF(employees, '')) AS employees,
    COUNT(NULLIF(employees_bucket, '')) AS employees_bucket,
    COUNT(home_based_indicator) AS home_based_indicator,
    COUNT(id) AS id,
    COUNT(NULLIF(industries, '')) AS industries,
    COUNT(last_found) AS last_found,
    COUNT(last_processed) AS last_processed,
    COUNT(NULLIF(lat, 0)) AS lat,
    COUNT(NULLIF(lat_global, 0)) AS lat_global,
    COUNT(NULLIF(lat_usa, 0)) AS lat_usa,
    COUNT(NULLIF(lon, 0)) AS lon,
    COUNT(NULLIF(lon_global, 0)) AS lon_global,
    COUNT(NULLIF(lon_usa, 0)) AS lon_usa,
    COUNT(NULLIF(naics_codes, '')) AS naics_codes,
    COUNT(NULLIF(name, '')) AS name,
    COUNT(NULLIF(name_global, '')) AS name_global,
    COUNT(NULLIF(name_normalized, '')) AS name_normalized,
    COUNT(NULLIF(name_usa, '')) AS name_usa,
    COUNT(num_processed_matches) AS num_processed_matches,
    COUNT(NULLIF(persistent_id, '')) AS persistent_id,
    COUNT(NULLIF(persistent_id_type, '')) AS persistent_id_type,
    COUNT(NULLIF(phone, '')) AS phone,
    COUNT(NULLIF(phone_global, '')) AS phone_global,
    COUNT(NULLIF(phone_usa, '')) AS phone_usa,
    COUNT(NULLIF(primary_industry, '')) AS primary_industry,
    COUNT(NULLIF(primary_naics_code, '')) AS primary_naics_code,
    COUNT(NULLIF(primary_sic_code, '')) AS primary_sic_code,
    COUNT(NULLIF(profile_url_facebook, '')) AS profile_url_facebook,
    COUNT(NULLIF(profile_url_google, '')) AS profile_url_google,
    COUNT(NULLIF(profile_url_instagram, '')) AS profile_url_instagram,
    COUNT(NULLIF(profile_url_linkedin, '')) AS profile_url_linkedin,
    COUNT(NULLIF(profile_url_twitter, '')) AS profile_url_twitter,
    COUNT(NULLIF(profile_url_yelp, '')) AS profile_url_yelp,
    COUNT(NULLIF(profile_url_youtube, '')) AS profile_url_youtube,
    COUNT(NULLIF(profile_url_zoominfo, '')) AS profile_url_zoominfo,
    COUNT(NULLIF(provider_id, 0)) AS provider_id,
    COUNT(NULLIF(provider_record_id, '')) AS provider_record_id,
    COUNT(random_tenth_percent) AS random_tenth_percent,
    COUNT(NULLIF(raw_address, '')) AS raw_address,
    COUNT(NULLIF(raw_address_global, '')) AS raw_address_global,
    COUNT(NULLIF(raw_address_usa, '')) AS raw_address_usa,
    COUNT(NULLIF(revenue, '')) AS revenue,
    COUNT(NULLIF(revenue_bucket, '')) AS revenue_bucket,
    COUNT(NULLIF(sic_codes, '')) AS sic_codes,
    COUNT(NULLIF(sole_propriertorship_indicator, 0)) AS sole_propriertorship_indicator,
    COUNT(source_id) AS source_id,
    COUNT(NULLIF(source_record_id, '')) AS source_record_id,
    COUNT(NULLIF(state, '')) AS state,
    COUNT(NULLIF(state_global, '')) AS state_global,
    COUNT(NULLIF(state_short, '')) AS state_short,
    COUNT(NULLIF(state_short_global, '')) AS state_short_global,
    COUNT(NULLIF(state_short_usa, '')) AS state_short_usa,
    COUNT(NULLIF(state_usa, '')) AS state_usa,
    COUNT(NULLIF(website, '')) AS website,
    COUNT(NULLIF(year_company_established, 0)) AS year_company_established,
    COUNT(NULLIF(year_founded, 0)) AS year_founded,
    COUNT(NULLIF(zip, '')) AS zip,
    COUNT(NULLIF(zip4, '')) AS zip4,
    COUNT(NULLIF(zip4_usa, '')) AS zip4_usa,
    COUNT(NULLIF(zip_global, '')) AS zip_global,
    COUNT(NULLIF(zip_usa, '')) AS zip_usa
FROM public.production_companies
;


SELECT
    COUNT(match_id) AS match_id,
    COUNT(persistent_id) AS persistent_id,
    COUNT(persistent_id_type) AS persistent_id_type,
    COUNT(num_processed_matches) AS num_processed_matches,
    COUNT(name) AS name,
    COUNT(name_normalized) AS name_normalized,
    COUNT(description) AS description,
    COUNT(domain) AS domain,
    COUNT(website) AS website,
    COUNT(profile_url_linkedin) AS profile_url_linkedin,
    COUNT(profile_url_linkedin_path) AS profile_url_linkedin_path,
    COUNT(profile_url_google) AS profile_url_google,
    COUNT(profile_url_yelp) AS profile_url_yelp,
    COUNT(profile_url_facebook) AS profile_url_facebook,
    COUNT(profile_url_zoominfo) AS profile_url_zoominfo,
    COUNT(profile_url_twitter) AS profile_url_twitter,
    COUNT(profile_url_instagram) AS profile_url_instagram,
    COUNT(profile_url_youtube) AS profile_url_youtube,
    COUNT(location_id) AS location_id,
    COUNT(phone) AS phone,
    COUNT(address1) AS address1,
    COUNT(address2) AS address2,
    COUNT(city) AS city,
    COUNT(state) AS state,
    COUNT(state_short) AS state_short,
    COUNT(zip) AS zip,
    COUNT(zip4) AS zip4,
    COUNT(country) AS country,
    COUNT(lat) AS lat,
    COUNT(lon) AS lon,
    COUNT(raw_address) AS raw_address,
    COUNT(employees) AS employees,
    COUNT(employees_bucket) AS employees_bucket,
    COUNT(revenue) AS revenue,
    COUNT(revenue_bucket) AS revenue_bucket,
    COUNT(primary_industry) AS primary_industry,
    COUNT(industries) AS industries,
    COUNT(primary_sic_code) AS primary_sic_code,
    COUNT(sic_codes) AS sic_codes,
    COUNT(primary_naics_code) AS primary_naics_code,
    COUNT(naics_codes) AS naics_codes,
    COUNT(year_company_established) AS year_company_established,
    COUNT(corporate_entity_type) AS corporate_entity_type,
    COUNT(last_found) AS last_found,
    COUNT(last_processed) AS last_processed,
    COUNT(random_tenth_percent) AS random_tenth_percent,
FROM production.companies;

TRUNCATE TABLE onboarding.current_matchback;
COPY onboarding.current_matchback
FROM 's3://nwd-imports/liveramp-matchbacks/2019_06_email_only/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
ENCODING UTF8
GZIP
IGNOREHEADER AS 1;


UNLOAD ('SELECT md5 AS email_md5, md5 AS file_wide_field FROM b2b_emails')
TO 's3://nwd-exports/liveramp/matchbacks/20190626/'
CREDENTIALS 'aws_iam_role=arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
FORMAT AS CSV;