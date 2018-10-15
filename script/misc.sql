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


load data local infile 'ALC_Aggregated.psv'
into table `alc_consumer_segments_2018_10_12`
fields terminated by '|' escaped by '\\'
lines terminated by '\r\n'
ignore 1 lines
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

