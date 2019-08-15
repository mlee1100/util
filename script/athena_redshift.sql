SET search_path TO production;

-- DROP TABLE IF EXISTS temp.athena_b2b_emails;
-- CREATE TABLE temp.athena_b2b_emails (
--     person_match_id CHAR(32) encode raw distkey,
--     b2b_email varchar(1020) encode zstd,
--     b2b_email_md5 char(32) encode raw,
--     b2b_email_status varchar(1020) encode zstd,
--     b2b_email_additional_status_info varchar(1020) encode zstd,
--     b2b_domain_status varchar(1020) encode zstd,
--     b2b_email_status_waterfall varchar(1020) encode zstd,
--     b2b_email_vendor_status varchar(1020) encode zstd,
--     b2b_email_query_type varchar(1020) encode zstd,
--     b2b_email_domain_office_365 smallint encode raw
-- )
-- sortkey (person_match_id, b2b_email_md5)
-- ;

-- INSERT INTO temp.athena_b2b_emails
-- SELECT person_match_id,
--     b2b_email,
--     MD5(b2b_email) AS b2b_email_md5,
--     b2b_email_status,
--     b2b_email_additional_status_info,
--     b2b_domain_status,
--     b2b_email_status_waterfall,
--     b2b_email_vendor_status,
--     b2b_email_query_type,
--     b2b_email_domain_office_365
-- FROM (
--     SELECT business_emails.person_match_id,
--         business_emails.email AS b2b_email,
--         COALESCE(validated_emails.status,'Unverified') AS b2b_email_status,
--         COALESCE(validated_emails.additional_status_info,'Unverified') AS b2b_email_additional_status_info,
--         COALESCE(validated_email_domains.most_recent_additional_email_status_info,'Unverified') AS b2b_domain_status,
--         CASE
--             WHEN validated_emails.status = 'Ok' THEN 'Good'
--             WHEN validated_emails.status = 'Bad' THEN 'Bad'
--             WHEN validated_emails.hard_bounce = 1 THEN 'Bad'
--             WHEN validated_emails.additional_status_info = 'ServerIsCatchAll' THEN 'Catch All'
--             WHEN validated_emails.id IS NOT NULL THEN 'Unknown'
--             WHEN validated_email_domains.most_recent_additional_email_status_info IN ('DomainIsInexistent', 'NoMxServersFound') THEN 'Bad'
--             WHEN validated_email_domains.most_recent_additional_email_status_info = 'ServerIsCatchAll' THEN 'Catch All'
--             WHEN validated_emails.id IS NULL THEN 'Unverified'
--             ELSE 'Unknown'
--         END AS b2b_email_status_waterfall,
--         validated_emails.vendor_status AS b2b_email_vendor_status,
--         validated_emails.query_type AS b2b_email_query_type,
--         validated_email_domains.most_recent_office_365 AS b2b_email_domain_office_365,
--         ROW_NUMBER() OVER (PARTITION BY business_emails.person_match_id ORDER BY
--             validated_emails.status = 'Ok' DESC,
--             validated_email_domains.most_recent_server_type <> 'responds' AND business_emails.best_guess = 1 DESC,
--             validated_email_domains.most_recent_additional_email_status_info = 'ServerIsCatchAll' DESC,
--             validated_emails.status = 'Bad' ASC,
--             validated_emails.hard_bounce ASC,
--             business_emails.email_rank ASC) AS row_num
--     FROM persons
--     INNER JOIN business_emails ON persons.match_id = business_emails.person_match_id
--     LEFT OUTER JOIN validated_emails ON business_emails.md5 = validated_emails.md5
--     LEFT OUTER JOIN validated_email_domains ON business_emails.domain = validated_email_domains.email_domain
-- ) t
-- WHERE row_num = 1
-- ;

-- DROP TABLE IF EXISTS temp.athena_personal_emails_raw;
-- CREATE TABLE temp.athena_personal_emails_raw (
--     person_match_id CHAR(32) encode raw distkey sortkey,
--     p2b_email_1 varchar(1020) encode zstd,
--     p2b_email_2 varchar(1020) encode zstd,
--     p2b_email_3 varchar(1020) encode zstd,
--     p2b_email_4 varchar(1020) encode zstd,
--     p2b_email_5 varchar(1020) encode zstd,
--     p2b_email_6 varchar(1020) encode zstd,
--     p2b_email_7 varchar(1020) encode zstd,
--     p2b_email_8 varchar(1020) encode zstd,
--     p2b_email_9 varchar(1020) encode zstd
-- )
-- ;

-- INSERT INTO temp.athena_personal_emails_raw 
-- SELECT person_match_id,
--     SPLIT_PART(grouped_emails, '|', 1) AS p2b_email_1,
--     SPLIT_PART(grouped_emails, '|', 2) AS p2b_email_2,
--     SPLIT_PART(grouped_emails, '|', 3) AS p2b_email_3,
--     SPLIT_PART(grouped_emails, '|', 4) AS p2b_email_4,
--     SPLIT_PART(grouped_emails, '|', 5) AS p2b_email_5,
--     SPLIT_PART(grouped_emails, '|', 6) AS p2b_email_6,
--     SPLIT_PART(grouped_emails, '|', 7) AS p2b_email_7,
--     SPLIT_PART(grouped_emails, '|', 8) AS p2b_email_8,
--     SPLIT_PART(grouped_emails, '|', 9) AS p2b_email_9
-- FROM (
--     SELECT t.person_match_id,
--         LISTAGG(t.email, '|') WITHIN GROUP (ORDER BY
--             t.onboarded DESC
--             -- t.confidence DESC,
--             -- t.id ASC
--             ) || '|||||||||' AS grouped_emails
--     FROM 
--         (
--             SELECT consumer_emails.*,
--                 ROW_NUMBER() OVER (PARTITION BY consumer_emails.person_match_id ORDER BY
--                     consumer_emails.onboarded DESC
--                     -- consumer_emails.confidence DESC,
--                     -- consumer_emails.id ASC
--                     ) AS row_num
--             FROM consumer_emails
--         ) t
--     WHERE row_num <= 9
--     GROUP BY t.person_match_id
-- ) t
-- ;

-- DROP TABLE IF EXISTS temp.athena_personal_emails_md5;
-- CREATE TABLE temp.athena_personal_emails_md5 (
--     person_match_id CHAR(32) encode raw distkey sortkey,
--     p2b_email_1_md5 char(32) encode zstd,
--     p2b_email_2_md5 char(32) encode zstd,
--     p2b_email_3_md5 char(32) encode zstd,
--     p2b_email_4_md5 char(32) encode zstd,
--     p2b_email_5_md5 char(32) encode zstd,
--     p2b_email_6_md5 char(32) encode zstd,
--     p2b_email_7_md5 char(32) encode zstd,
--     p2b_email_8_md5 char(32) encode zstd,
--     p2b_email_9_md5 char(32) encode zstd
-- )
-- ;

-- INSERT INTO temp.athena_personal_emails_md5
-- SELECT person_match_id,
--     SPLIT_PART(grouped_emails, '|', 1) AS p2b_email_1_md5,
--     SPLIT_PART(grouped_emails, '|', 2) AS p2b_email_2_md5,
--     SPLIT_PART(grouped_emails, '|', 3) AS p2b_email_3_md5,
--     SPLIT_PART(grouped_emails, '|', 4) AS p2b_email_4_md5,
--     SPLIT_PART(grouped_emails, '|', 5) AS p2b_email_5_md5,
--     SPLIT_PART(grouped_emails, '|', 6) AS p2b_email_6_md5,
--     SPLIT_PART(grouped_emails, '|', 7) AS p2b_email_7_md5,
--     SPLIT_PART(grouped_emails, '|', 8) AS p2b_email_8_md5,
--     SPLIT_PART(grouped_emails, '|', 9) AS p2b_email_9_md5
-- FROM (
--     SELECT t.person_match_id,
--         LISTAGG(t.md5, '|') WITHIN GROUP (ORDER BY
--             t.onboarded DESC
--             -- t.confidence DESC,
--             -- t.id ASC
--             ) || '|||||||||' AS grouped_emails
--     FROM 
--         (
--             SELECT consumer_emails.*,
--                 ROW_NUMBER() OVER (PARTITION BY consumer_emails.person_match_id ORDER BY
--                     consumer_emails.onboarded DESC
--                     -- consumer_emails.confidence DESC,
--                     -- consumer_emails.id ASC
--                     ) AS row_num
--             FROM consumer_emails
--         ) t
--     WHERE row_num <= 9
--     GROUP BY t.person_match_id
-- ) t
-- ;

-- DROP TABLE IF EXISTS temp.athena_emails_onboarding;
-- CREATE TABLE temp.athena_emails_onboarding (
--     person_match_id CHAR(32) encode raw distkey sortkey,
--     email_onboarding_1 varchar(1020) encode zstd,
--     email_onboarding_2 varchar(1020) encode zstd,
--     email_onboarding_3 varchar(1020) encode zstd,
--     email_onboarding_4 varchar(1020) encode zstd,
--     email_onboarding_5 varchar(1020) encode zstd,
--     email_onboarding_6 varchar(1020) encode zstd,
--     email_onboarding_7 varchar(1020) encode zstd,
--     email_onboarding_8 varchar(1020) encode zstd,
--     email_onboarding_9 varchar(1020) encode zstd
-- )
-- ;

-- INSERT INTO temp.athena_emails_onboarding
-- WITH CTE_PERSONAL AS (
--     SELECT t.person_match_id,
--         LISTAGG(t.email, '|') WITHIN GROUP (ORDER BY
--             t.onboarded DESC
--             -- t.confidence DESC,
--             -- t.id ASC
--             ) AS grouped_emails
--     FROM 
--         (
--             SELECT consumer_emails.*,
--                 ROW_NUMBER() OVER (PARTITION BY consumer_emails.person_match_id ORDER BY
--                     consumer_emails.onboarded DESC
--                     -- consumer_emails.confidence DESC,
--                     -- consumer_emails.id ASC
--                     ) AS row_num
--             FROM consumer_emails
--         ) t
--     WHERE row_num <= 9
--     GROUP BY t.person_match_id
-- ),

-- CTE_B2B AS (
--     SELECT
--         business_emails.person_match_id,
--         LISTAGG(business_emails.email, '|') WITHIN GROUP (ORDER BY
--             validated_emails.status = 'Ok' DESC,
--             validated_email_domains.most_recent_server_type <> 'responds' AND business_emails.best_guess = 1 DESC,
--             validated_email_domains.most_recent_additional_email_status_info = 'ServerIsCatchAll' DESC,
--             validated_emails.status = 'Bad' ASC,
--             validated_emails.hard_bounce ASC,
--             business_emails.email_rank ASC) AS grouped_emails
--     FROM business_emails
--     LEFT OUTER JOIN validated_emails ON business_emails.md5 = validated_emails.md5
--     LEFT OUTER JOIN validated_email_domains ON business_emails.domain = validated_email_domains.email_domain
--     GROUP BY business_emails.person_match_id
-- )

-- SELECT match_id,
--     SPLIT_PART(t.grouped_emails, '|', 1) AS email_onboarding_1,
--     SPLIT_PART(t.grouped_emails, '|', 2) AS email_onboarding_2,
--     SPLIT_PART(t.grouped_emails, '|', 3) AS email_onboarding_3,
--     SPLIT_PART(t.grouped_emails, '|', 4) AS email_onboarding_4,
--     SPLIT_PART(t.grouped_emails, '|', 5) AS email_onboarding_5,
--     SPLIT_PART(t.grouped_emails, '|', 6) AS email_onboarding_6,
--     SPLIT_PART(t.grouped_emails, '|', 7) AS email_onboarding_7,
--     SPLIT_PART(t.grouped_emails, '|', 8) AS email_onboarding_8,
--     SPLIT_PART(t.grouped_emails, '|', 9) AS email_onboarding_9
-- FROM (
--     SELECT persons.match_id,
--         COALESCE(ctep.grouped_emails || '|', '') || COALESCE(cteb.grouped_emails || '|', '') || '|||||||||' AS grouped_emails
--     FROM persons
--     LEFT OUTER JOIN CTE_PERSONAL ctep ON persons.match_id = ctep.person_match_id
--     LEFT OUTER JOIN CTE_B2B cteb ON persons.match_id = cteb.person_match_id
--     WHERE (ctep.person_match_id IS NOT NULL OR cteb.person_match_id IS NOT NULL)
-- ) t
-- ;

DROP TABLE IF EXISTS sandbox.athena_contacts;
CREATE TABLE sandbox.athena_contacts AS 
SELECT
    persons.match_id AS person_id,
    CASE
        WHEN temp.athena_b2b_emails.b2b_email <> ''
            AND persons.first_name NOT IN ('','"','|','\'')
            AND persons.last_name NOT IN ('','"','|','\'')
            AND companies.name NOT IN ('','"','|','\'')
            AND persons.title NOT IN ('','"','|','\'')
            AND company_locations.phone NOT IN ('','"','|','\'')
            AND company_locations.address1 NOT IN ('','"','|','\'')
            AND company_locations.city NOT IN ('','"','|','\'')
            AND company_locations.state NOT IN ('','"','|','\'')
            AND company_locations.zip NOT IN ('','"','|','\'')
            THEN 1
        ELSE 0
    END AS full_b2b_contact,
    persons.first_name,
    persons.last_name,
    persons.title,
    persons.is_digital_only,
    person_segments.b2b_seniority,
    person_segments.b2b_job_function,
    person_segments.b2b_job_sub_function,
    persons.company_match_id AS company_id,
    company_locations.phone AS company_phone_waterfall,
    company_locations.address1 AS company_address1_waterfall,
    company_locations.address2 AS company_address2_waterfall,
    company_locations.city AS company_city_waterfall,
    company_locations.state AS company_state_waterfall,
    company_locations.state_short AS company_state_short_waterfall,
    company_locations.zip AS company_zip_waterfall,
    company_locations.zip4 AS company_zip4_waterfall,
    company_locations.country AS company_country_waterfall,
    company_locations2.phone AS company_phone_headquarters,
    company_locations2.address1 AS company_address1_headquarters,
    company_locations2.address2 AS company_address2_headquarters,
    company_locations2.city AS company_city_headquarters,
    company_locations2.state AS company_state_headquarters,
    company_locations2.state_short AS company_state_short_headquarters,
    company_locations2.zip AS company_zip_headquarters,
    company_locations2.zip4 AS company_zip4_headquarters,
    company_locations2.country AS company_country_headquarters,
    CASE
        WHEN temp.athena_personal_emails_raw.p2b_email_1 <> '' THEN 1
        ELSE 0
    END AS has_raw_personal_email,
    CASE
        WHEN temp.athena_personal_emails_md5.p2b_email_1_md5 <> '' THEN 1
        ELSE 0
    END AS has_personal_email,
    person_locations.phone AS personal_phone,
    person_locations.address1 AS personal_address1,
    person_locations.address2 AS personal_address2,
    person_locations.city AS personal_city,
    person_locations.state AS personal_state,
    person_locations.state_short AS personal_state_short,
    person_locations.zip AS personal_zip,
    person_locations.zip4 AS personal_zip4,
    person_locations.country AS personal_country,
    CASE
        WHEN temp.athena_personal_emails_md5.p2b_email_1_md5 <> ''
            AND person_locations.address1 NOT IN ('','"','|','\'')
            AND person_locations.zip NOT IN ('','"','|','\'')
            THEN 1
        ELSE 0
    END AS full_personal_connection,
    persons.current_position_start_year,
    persons.education_1_graduation_year,
    persons.education_1_institution,
    persons.education_1_level,
    persons.education_1_start_year,
    persons.education_2_graduation_year,
    persons.education_2_institution,
    persons.education_2_level,
    persons.education_2_start_year,
    persons.education_earliest_graduation_year,
    persons.education_earliest_level,
    persons.language1,
    persons.language2,
    persons.language3,
    companies.profile_url_linkedin_path AS linkedin_company_url_path,
    persons.previous_position_1_company_name,
    -- persons.previous_position_1_industry,
    persons.previous_position_1_start_year,
    persons.previous_position_1_title,
    persons.previous_position_2_company_name,
    -- persons.previous_position_2_industry,
    persons.previous_position_2_start_year,
    persons.previous_position_2_title,
    persons.skill1,
    persons.skill2,
    persons.skill3,
    persons.skill4,
    persons.skill5,
    persons.profile_url_linkedin AS linkedin_profile_url,
    persons.profile_url_linkedin_path_canonical AS linkedin_profile_url_path,
    persons.number_of_connections,
    companies.num_processed_matches AS company_num_processed_matches,
    companies.name AS company_name,
    companies.domain AS company_domain,
    companies.website AS company_website,
    companies.employees AS company_employees,
    companies.employees_bucket AS company_employees_bucket,
    companies.revenue AS company_revenue,
    companies.revenue_bucket AS company_revenue_bucket,
    companies.primary_industry AS company_primary_industry,
    companies.primary_sic_code AS company_primary_sic_code,
    companies.primary_naics_code AS company_primary_naics_code,
    temp.athena_b2b_emails.b2b_email,
    temp.athena_b2b_emails.b2b_email_md5,
    temp.athena_b2b_emails.b2b_email_status,
    temp.athena_b2b_emails.b2b_email_additional_status_info,
    temp.athena_b2b_emails.b2b_domain_status AS b2b_domain_email_status,
    temp.athena_b2b_emails.b2b_email_status_waterfall,
    temp.athena_b2b_emails.b2b_email_vendor_status,
    temp.athena_b2b_emails.b2b_email_query_type,
    temp.athena_b2b_emails.b2b_email_domain_office_365,
    temp.athena_personal_emails_raw.p2b_email_1,
    temp.athena_personal_emails_raw.p2b_email_2,
    temp.athena_personal_emails_raw.p2b_email_3,
    temp.athena_personal_emails_raw.p2b_email_4,
    temp.athena_personal_emails_md5.p2b_email_1_md5,
    temp.athena_personal_emails_md5.p2b_email_2_md5,
    temp.athena_personal_emails_md5.p2b_email_3_md5,
    temp.athena_personal_emails_md5.p2b_email_4_md5,
    temp.athena_emails_onboarding.email_onboarding_1,
    temp.athena_emails_onboarding.email_onboarding_2,
    temp.athena_emails_onboarding.email_onboarding_3,
    temp.athena_emails_onboarding.email_onboarding_4,
    temp.athena_emails_onboarding.email_onboarding_5,
    temp.athena_emails_onboarding.email_onboarding_6,
    temp.athena_emails_onboarding.email_onboarding_7,
    temp.athena_emails_onboarding.email_onboarding_8,
    temp.athena_emails_onboarding.email_onboarding_9,
    CAST(NULLIF(company_segments.b2b_company_fortune_1000, 0) AS SMALLINT) AS b2b_company_fortune_1000,
    CAST(NULLIF(company_segments.b2b_company_fortune_500, 0) AS SMALLINT) AS b2b_company_fortune_500,
    CAST(NULLIF(company_segments.b2b_company_revenue_large, 0) AS SMALLINT) AS b2b_company_revenue_large,
    CAST(NULLIF(company_segments.b2b_company_revenue_med, 0) AS SMALLINT) AS b2b_company_revenue_med,
    CAST(NULLIF(company_segments.b2b_company_revenue_med_large, 0) AS SMALLINT) AS b2b_company_revenue_med_large,
    CAST(NULLIF(company_segments.b2b_company_revenue_micro, 0) AS SMALLINT) AS b2b_company_revenue_micro,
    CAST(NULLIF(company_segments.b2b_company_revenue_small, 0) AS SMALLINT) AS b2b_company_revenue_small,
    CAST(NULLIF(company_segments.b2b_company_revenue_xlarge, 0) AS SMALLINT) AS b2b_company_revenue_xlarge,
    company_segments.b2b_company_size,
    CAST(NULLIF(company_segments.b2b_company_size_large, 0) AS SMALLINT) AS b2b_company_size_large,
    CAST(NULLIF(company_segments.b2b_company_size_med, 0) AS SMALLINT) AS b2b_company_size_med,
    CAST(NULLIF(company_segments.b2b_company_size_med_large, 0) AS SMALLINT) AS b2b_company_size_med_large,
    CAST(NULLIF(company_segments.b2b_company_size_micro, 0) AS SMALLINT) AS b2b_company_size_micro,
    CAST(NULLIF(company_segments.b2b_company_size_small, 0) AS SMALLINT) AS b2b_company_size_small,
    CAST(NULLIF(company_segments.b2b_company_size_xlarge, 0) AS SMALLINT) AS b2b_company_size_xlarge,
    CAST(NULLIF(person_segments.b2b_engineering_decision_makers, 0) AS SMALLINT) AS b2b_engineering_decision_makers,
    CAST(NULLIF(person_segments.b2b_engineering_executives, 0) AS SMALLINT) AS b2b_engineering_executives,
    CAST(NULLIF(person_segments.b2b_engineering_managers, 0) AS SMALLINT) AS b2b_engineering_managers,
    CAST(NULLIF(person_segments.b2b_fa_admin, 0) AS SMALLINT) AS b2b_fa_admin,
    CAST(NULLIF(person_segments.b2b_fa_admin_administrator, 0) AS SMALLINT) AS b2b_fa_admin_administrator,
    CAST(NULLIF(person_segments.b2b_fa_admin_assistant, 0) AS SMALLINT) AS b2b_fa_admin_assistant,
    CAST(NULLIF(person_segments.b2b_fa_admin_office_manager, 0) AS SMALLINT) AS b2b_fa_admin_office_manager,
    CAST(NULLIF(person_segments.b2b_fa_admin_secretary, 0) AS SMALLINT) AS b2b_fa_admin_secretary,
    CAST(NULLIF(person_segments.b2b_fa_admin_typist, 0) AS SMALLINT) AS b2b_fa_admin_typist,
    CAST(NULLIF(person_segments.b2b_fa_consultants, 0) AS SMALLINT) AS b2b_fa_consultants,
    CAST(NULLIF(person_segments.b2b_fa_education, 0) AS SMALLINT) AS b2b_fa_education,
    CAST(NULLIF(person_segments.b2b_fa_education_admissions, 0) AS SMALLINT) AS b2b_fa_education_admissions,
    CAST(NULLIF(person_segments.b2b_fa_education_librarian, 0) AS SMALLINT) AS b2b_fa_education_librarian,
    CAST(NULLIF(person_segments.b2b_fa_education_professor, 0) AS SMALLINT) AS b2b_fa_education_professor,
    CAST(NULLIF(person_segments.b2b_fa_education_teacher, 0) AS SMALLINT) AS b2b_fa_education_teacher,
    CAST(NULLIF(person_segments.b2b_fa_engineering, 0) AS SMALLINT) AS b2b_fa_engineering,
    CAST(NULLIF(person_segments.b2b_fa_engineering_biomedical, 0) AS SMALLINT) AS b2b_fa_engineering_biomedical,
    CAST(NULLIF(person_segments.b2b_fa_engineering_chemical, 0) AS SMALLINT) AS b2b_fa_engineering_chemical,
    CAST(NULLIF(person_segments.b2b_fa_engineering_civil, 0) AS SMALLINT) AS b2b_fa_engineering_civil,
    CAST(NULLIF(person_segments.b2b_fa_engineering_electrical, 0) AS SMALLINT) AS b2b_fa_engineering_electrical,
    CAST(NULLIF(person_segments.b2b_fa_engineering_electronics, 0) AS SMALLINT) AS b2b_fa_engineering_electronics,
    CAST(NULLIF(person_segments.b2b_fa_engineering_industrial, 0) AS SMALLINT) AS b2b_fa_engineering_industrial,
    CAST(NULLIF(person_segments.b2b_fa_engineering_mechanical, 0) AS SMALLINT) AS b2b_fa_engineering_mechanical,
    CAST(NULLIF(person_segments.b2b_fa_finance, 0) AS SMALLINT) AS b2b_fa_finance,
    CAST(NULLIF(person_segments.b2b_fa_finance_accounting, 0) AS SMALLINT) AS b2b_fa_finance_accounting,
    CAST(NULLIF(person_segments.b2b_fa_finance_analyst, 0) AS SMALLINT) AS b2b_fa_finance_analyst,
    CAST(NULLIF(person_segments.b2b_fa_finance_audit, 0) AS SMALLINT) AS b2b_fa_finance_audit,
    CAST(NULLIF(person_segments.b2b_fa_finance_banking, 0) AS SMALLINT) AS b2b_fa_finance_banking,
    CAST(NULLIF(person_segments.b2b_fa_finance_cfp, 0) AS SMALLINT) AS b2b_fa_finance_cfp,
    CAST(NULLIF(person_segments.b2b_fa_finance_compliance_specialist, 0) AS SMALLINT) AS b2b_fa_finance_compliance_specialist,
    CAST(NULLIF(person_segments.b2b_fa_finance_corporate, 0) AS SMALLINT) AS b2b_fa_finance_corporate,
    CAST(NULLIF(person_segments.b2b_fa_finance_cpa, 0) AS SMALLINT) AS b2b_fa_finance_cpa,
    CAST(NULLIF(person_segments.b2b_fa_finance_economist, 0) AS SMALLINT) AS b2b_fa_finance_economist,
    CAST(NULLIF(person_segments.b2b_fa_finance_investment_banking, 0) AS SMALLINT) AS b2b_fa_finance_investment_banking,
    CAST(NULLIF(person_segments.b2b_fa_finance_mortgage_specialist, 0) AS SMALLINT) AS b2b_fa_finance_mortgage_specialist,
    CAST(NULLIF(person_segments.b2b_fa_finance_risk_management, 0) AS SMALLINT) AS b2b_fa_finance_risk_management,
    CAST(NULLIF(person_segments.b2b_fa_finance_statistician, 0) AS SMALLINT) AS b2b_fa_finance_statistician,
    CAST(NULLIF(person_segments.b2b_fa_finance_tax_specialist, 0) AS SMALLINT) AS b2b_fa_finance_tax_specialist,
    CAST(NULLIF(person_segments.b2b_fa_finance_treasurer, 0) AS SMALLINT) AS b2b_fa_finance_treasurer,
    CAST(NULLIF(person_segments.b2b_fa_finance_wealth_management, 0) AS SMALLINT) AS b2b_fa_finance_wealth_management,
    CAST(NULLIF(person_segments.b2b_fa_general_management, 0) AS SMALLINT) AS b2b_fa_general_management,
    CAST(NULLIF(person_segments.b2b_fa_general_management_board_member, 0) AS SMALLINT) AS b2b_fa_general_management_board_member,
    CAST(NULLIF(person_segments.b2b_fa_general_management_ceo, 0) AS SMALLINT) AS b2b_fa_general_management_ceo,
    CAST(NULLIF(person_segments.b2b_fa_general_management_cfo, 0) AS SMALLINT) AS b2b_fa_general_management_cfo,
    CAST(NULLIF(person_segments.b2b_fa_general_management_cio, 0) AS SMALLINT) AS b2b_fa_general_management_cio,
    CAST(NULLIF(person_segments.b2b_fa_general_management_cmo, 0) AS SMALLINT) AS b2b_fa_general_management_cmo,
    CAST(NULLIF(person_segments.b2b_fa_general_management_coo, 0) AS SMALLINT) AS b2b_fa_general_management_coo,
    CAST(NULLIF(person_segments.b2b_fa_general_management_cto, 0) AS SMALLINT) AS b2b_fa_general_management_cto,
    CAST(NULLIF(person_segments.b2b_fa_general_management_director, 0) AS SMALLINT) AS b2b_fa_general_management_director,
    CAST(NULLIF(person_segments.b2b_fa_general_management_executive_vp, 0) AS SMALLINT) AS b2b_fa_general_management_executive_vp,
    CAST(NULLIF(person_segments.b2b_fa_general_management_founder, 0) AS SMALLINT) AS b2b_fa_general_management_founder,
    CAST(NULLIF(person_segments.b2b_fa_general_management_manager_or_supervisor, 0) AS SMALLINT) AS b2b_fa_general_management_manager_or_supervisor,
    CAST(NULLIF(person_segments.b2b_fa_general_management_owner, 0) AS SMALLINT) AS b2b_fa_general_management_owner,
    CAST(NULLIF(person_segments.b2b_fa_general_management_partner, 0) AS SMALLINT) AS b2b_fa_general_management_partner,
    CAST(NULLIF(person_segments.b2b_fa_general_management_president, 0) AS SMALLINT) AS b2b_fa_general_management_president,
    CAST(NULLIF(person_segments.b2b_fa_general_management_senior_board_member, 0) AS SMALLINT) AS b2b_fa_general_management_senior_board_member,
    CAST(NULLIF(person_segments.b2b_fa_general_management_vp, 0) AS SMALLINT) AS b2b_fa_general_management_vp,
    CAST(NULLIF(person_segments.b2b_fa_government, 0) AS SMALLINT) AS b2b_fa_government,
    CAST(NULLIF(person_segments.b2b_fa_government_elected_officials, 0) AS SMALLINT) AS b2b_fa_government_elected_officials,
    CAST(NULLIF(person_segments.b2b_fa_healthcare, 0) AS SMALLINT) AS b2b_fa_healthcare,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_chiropractor, 0) AS SMALLINT) AS b2b_fa_healthcare_chiropractor,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_dental_assistant, 0) AS SMALLINT) AS b2b_fa_healthcare_dental_assistant,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_dental_hygenist, 0) AS SMALLINT) AS b2b_fa_healthcare_dental_hygenist,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_dentist, 0) AS SMALLINT) AS b2b_fa_healthcare_dentist,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_dietician, 0) AS SMALLINT) AS b2b_fa_healthcare_dietician,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_healthcare_professional, 0) AS SMALLINT) AS b2b_fa_healthcare_healthcare_professional,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_nurse, 0) AS SMALLINT) AS b2b_fa_healthcare_nurse,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_opticians, 0) AS SMALLINT) AS b2b_fa_healthcare_opticians,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_optometrist, 0) AS SMALLINT) AS b2b_fa_healthcare_optometrist,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_paramedic, 0) AS SMALLINT) AS b2b_fa_healthcare_paramedic,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_pharmaceuticals, 0) AS SMALLINT) AS b2b_fa_healthcare_pharmaceuticals,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_pharmacist, 0) AS SMALLINT) AS b2b_fa_healthcare_pharmacist,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_physical_therapist, 0) AS SMALLINT) AS b2b_fa_healthcare_physical_therapist,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_physician_or_doctor, 0) AS SMALLINT) AS b2b_fa_healthcare_physician_or_doctor,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_physicians_assistant, 0) AS SMALLINT) AS b2b_fa_healthcare_physicians_assistant,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_psychologist, 0) AS SMALLINT) AS b2b_fa_healthcare_psychologist,
    CAST(NULLIF(person_segments.b2b_fa_healthcare_therapist, 0) AS SMALLINT) AS b2b_fa_healthcare_therapist,
    CAST(NULLIF(person_segments.b2b_fa_human_resources, 0) AS SMALLINT) AS b2b_fa_human_resources,
    CAST(NULLIF(person_segments.b2b_fa_human_resources_benefits_and_compensation, 0) AS SMALLINT) AS b2b_fa_human_resources_benefits_and_compensation,
    CAST(NULLIF(person_segments.b2b_fa_human_resources_employee_development, 0) AS SMALLINT) AS b2b_fa_human_resources_employee_development,
    CAST(NULLIF(person_segments.b2b_fa_human_resources_payroll_specialist, 0) AS SMALLINT) AS b2b_fa_human_resources_payroll_specialist,
    CAST(NULLIF(person_segments.b2b_fa_human_resources_recruiting, 0) AS SMALLINT) AS b2b_fa_human_resources_recruiting,
    CAST(NULLIF(person_segments.b2b_fa_law_enforcement, 0) AS SMALLINT) AS b2b_fa_law_enforcement,
    CAST(NULLIF(person_segments.b2b_fa_law_enforcement_correctional_workers, 0) AS SMALLINT) AS b2b_fa_law_enforcement_correctional_workers,
    CAST(NULLIF(person_segments.b2b_fa_law_enforcement_parole_officer, 0) AS SMALLINT) AS b2b_fa_law_enforcement_parole_officer,
    CAST(NULLIF(person_segments.b2b_fa_law_enforcement_police, 0) AS SMALLINT) AS b2b_fa_law_enforcement_police,
    CAST(NULLIF(person_segments.b2b_fa_legal, 0) AS SMALLINT) AS b2b_fa_legal,
    CAST(NULLIF(person_segments.b2b_fa_legal_judge, 0) AS SMALLINT) AS b2b_fa_legal_judge,
    CAST(NULLIF(person_segments.b2b_fa_legal_lawyer, 0) AS SMALLINT) AS b2b_fa_legal_lawyer,
    CAST(NULLIF(person_segments.b2b_fa_legal_paralegal, 0) AS SMALLINT) AS b2b_fa_legal_paralegal,
    CAST(NULLIF(person_segments.b2b_fa_legal_prosecutor, 0) AS SMALLINT) AS b2b_fa_legal_prosecutor,
    CAST(NULLIF(person_segments.b2b_fa_marketing, 0) AS SMALLINT) AS b2b_fa_marketing,
    CAST(NULLIF(person_segments.b2b_fa_marketing_advertising, 0) AS SMALLINT) AS b2b_fa_marketing_advertising,
    CAST(NULLIF(person_segments.b2b_fa_marketing_creative, 0) AS SMALLINT) AS b2b_fa_marketing_creative,
    CAST(NULLIF(person_segments.b2b_fa_marketing_events, 0) AS SMALLINT) AS b2b_fa_marketing_events,
    CAST(NULLIF(person_segments.b2b_fa_marketing_marketing_communications, 0) AS SMALLINT) AS b2b_fa_marketing_marketing_communications,
    CAST(NULLIF(person_segments.b2b_fa_military, 0) AS SMALLINT) AS b2b_fa_military,
    CAST(NULLIF(person_segments.b2b_fa_military_air_force, 0) AS SMALLINT) AS b2b_fa_military_air_force,
    CAST(NULLIF(person_segments.b2b_fa_military_army, 0) AS SMALLINT) AS b2b_fa_military_army,
    CAST(NULLIF(person_segments.b2b_fa_military_coast_guard, 0) AS SMALLINT) AS b2b_fa_military_coast_guard,
    CAST(NULLIF(person_segments.b2b_fa_military_marines, 0) AS SMALLINT) AS b2b_fa_military_marines,
    CAST(NULLIF(person_segments.b2b_fa_military_navy, 0) AS SMALLINT) AS b2b_fa_military_navy,
    CAST(NULLIF(person_segments.b2b_fa_operations, 0) AS SMALLINT) AS b2b_fa_operations,
    CAST(NULLIF(person_segments.b2b_fa_operations_environmental, 0) AS SMALLINT) AS b2b_fa_operations_environmental,
    CAST(NULLIF(person_segments.b2b_fa_operations_facility_maintenance, 0) AS SMALLINT) AS b2b_fa_operations_facility_maintenance,
    CAST(NULLIF(person_segments.b2b_fa_operations_food_and_beverage, 0) AS SMALLINT) AS b2b_fa_operations_food_and_beverage,
    CAST(NULLIF(person_segments.b2b_fa_operations_manufacturing, 0) AS SMALLINT) AS b2b_fa_operations_manufacturing,
    CAST(NULLIF(person_segments.b2b_fa_operations_quality_assurance, 0) AS SMALLINT) AS b2b_fa_operations_quality_assurance,
    CAST(NULLIF(person_segments.b2b_fa_operations_records_management, 0) AS SMALLINT) AS b2b_fa_operations_records_management,
    CAST(NULLIF(person_segments.b2b_fa_operations_safety, 0) AS SMALLINT) AS b2b_fa_operations_safety,
    CAST(NULLIF(person_segments.b2b_fa_operations_training, 0) AS SMALLINT) AS b2b_fa_operations_training,
    CAST(NULLIF(person_segments.b2b_fa_photographer, 0) AS SMALLINT) AS b2b_fa_photographer,
    CAST(NULLIF(person_segments.b2b_fa_real_estate, 0) AS SMALLINT) AS b2b_fa_real_estate,
    CAST(NULLIF(person_segments.b2b_fa_real_estate_architect, 0) AS SMALLINT) AS b2b_fa_real_estate_architect,
    CAST(NULLIF(person_segments.b2b_fa_real_estate_builder, 0) AS SMALLINT) AS b2b_fa_real_estate_builder,
    CAST(NULLIF(person_segments.b2b_fa_real_estate_contractor, 0) AS SMALLINT) AS b2b_fa_real_estate_contractor,
    CAST(NULLIF(person_segments.b2b_fa_real_estate_developer, 0) AS SMALLINT) AS b2b_fa_real_estate_developer,
    CAST(NULLIF(person_segments.b2b_fa_real_estate_mortgage_specialist, 0) AS SMALLINT) AS b2b_fa_real_estate_mortgage_specialist,
    CAST(NULLIF(person_segments.b2b_fa_real_estate_real_estate_agent, 0) AS SMALLINT) AS b2b_fa_real_estate_real_estate_agent,
    CAST(NULLIF(person_segments.b2b_fa_religious_leader, 0) AS SMALLINT) AS b2b_fa_religious_leader,
    CAST(NULLIF(person_segments.b2b_fa_research_and_development, 0) AS SMALLINT) AS b2b_fa_research_and_development,
    CAST(NULLIF(person_segments.b2b_fa_research_and_development_product_development, 0) AS SMALLINT) AS b2b_fa_research_and_development_product_development,
    CAST(NULLIF(person_segments.b2b_fa_research_and_development_project_management, 0) AS SMALLINT) AS b2b_fa_research_and_development_project_management,
    CAST(NULLIF(person_segments.b2b_fa_research_and_development_research, 0) AS SMALLINT) AS b2b_fa_research_and_development_research,
    CAST(NULLIF(person_segments.b2b_fa_retired, 0) AS SMALLINT) AS b2b_fa_retired,
    CAST(NULLIF(person_segments.b2b_fa_sales, 0) AS SMALLINT) AS b2b_fa_sales,
    CAST(NULLIF(person_segments.b2b_fa_sales_account_management, 0) AS SMALLINT) AS b2b_fa_sales_account_management,
    CAST(NULLIF(person_segments.b2b_fa_sales_business_development, 0) AS SMALLINT) AS b2b_fa_sales_business_development,
    CAST(NULLIF(person_segments.b2b_fa_sales_community_development, 0) AS SMALLINT) AS b2b_fa_sales_community_development,
    CAST(NULLIF(person_segments.b2b_fa_sales_customer_service, 0) AS SMALLINT) AS b2b_fa_sales_customer_service,
    CAST(NULLIF(person_segments.b2b_fa_science, 0) AS SMALLINT) AS b2b_fa_science,
    CAST(NULLIF(person_segments.b2b_fa_science_chemist, 0) AS SMALLINT) AS b2b_fa_science_chemist,
    CAST(NULLIF(person_segments.b2b_fa_science_geologist, 0) AS SMALLINT) AS b2b_fa_science_geologist,
    CAST(NULLIF(person_segments.b2b_fa_science_scientists, 0) AS SMALLINT) AS b2b_fa_science_scientists,
    CAST(NULLIF(person_segments.b2b_fa_stay_at_home_parent, 0) AS SMALLINT) AS b2b_fa_stay_at_home_parent,
    CAST(NULLIF(person_segments.b2b_fa_student, 0) AS SMALLINT) AS b2b_fa_student,
    CAST(NULLIF(person_segments.b2b_fa_supply_chain, 0) AS SMALLINT) AS b2b_fa_supply_chain,
    CAST(NULLIF(person_segments.b2b_fa_supply_chain_logistics, 0) AS SMALLINT) AS b2b_fa_supply_chain_logistics,
    CAST(NULLIF(person_segments.b2b_fa_supply_chain_procurement, 0) AS SMALLINT) AS b2b_fa_supply_chain_procurement,
    CAST(NULLIF(person_segments.b2b_fa_supply_chain_sourcing, 0) AS SMALLINT) AS b2b_fa_supply_chain_sourcing,
    CAST(NULLIF(person_segments.b2b_fa_technology, 0) AS SMALLINT) AS b2b_fa_technology,
    CAST(NULLIF(person_segments.b2b_fa_technology_computing, 0) AS SMALLINT) AS b2b_fa_technology_computing,
    CAST(NULLIF(person_segments.b2b_fa_technology_data_management, 0) AS SMALLINT) AS b2b_fa_technology_data_management,
    CAST(NULLIF(person_segments.b2b_fa_technology_dba, 0) AS SMALLINT) AS b2b_fa_technology_dba,
    CAST(NULLIF(person_segments.b2b_fa_technology_information_security, 0) AS SMALLINT) AS b2b_fa_technology_information_security,
    CAST(NULLIF(person_segments.b2b_fa_technology_information_technology, 0) AS SMALLINT) AS b2b_fa_technology_information_technology,
    CAST(NULLIF(person_segments.b2b_fa_technology_mis, 0) AS SMALLINT) AS b2b_fa_technology_mis,
    CAST(NULLIF(person_segments.b2b_fa_technology_network_administration, 0) AS SMALLINT) AS b2b_fa_technology_network_administration,
    CAST(NULLIF(person_segments.b2b_fa_technology_quality_assurance, 0) AS SMALLINT) AS b2b_fa_technology_quality_assurance,
    CAST(NULLIF(person_segments.b2b_fa_technology_software, 0) AS SMALLINT) AS b2b_fa_technology_software,
    CAST(NULLIF(person_segments.b2b_fa_technology_systems_admin, 0) AS SMALLINT) AS b2b_fa_technology_systems_admin,
    CAST(NULLIF(person_segments.b2b_fa_technology_technical_support, 0) AS SMALLINT) AS b2b_fa_technology_technical_support,
    CAST(NULLIF(person_segments.b2b_fa_technology_telecom, 0) AS SMALLINT) AS b2b_fa_technology_telecom,
    CAST(NULLIF(person_segments.b2b_fa_technology_web_development, 0) AS SMALLINT) AS b2b_fa_technology_web_development,
    CAST(NULLIF(person_segments.b2b_fa_writer, 0) AS SMALLINT) AS b2b_fa_writer,
    CAST(NULLIF(person_segments.b2b_finance_decision_makers, 0) AS SMALLINT) AS b2b_finance_decision_makers,
    CAST(NULLIF(person_segments.b2b_finance_executives, 0) AS SMALLINT) AS b2b_finance_executives,
    CAST(NULLIF(person_segments.b2b_finance_managers, 0) AS SMALLINT) AS b2b_finance_managers,
    CAST(NULLIF(person_segments.b2b_healthcare_decision_makers, 0) AS SMALLINT) AS b2b_healthcare_decision_makers,
    CAST(NULLIF(person_segments.b2b_healthcare_executives, 0) AS SMALLINT) AS b2b_healthcare_executives,
    CAST(NULLIF(person_segments.b2b_healthcare_managers, 0) AS SMALLINT) AS b2b_healthcare_managers,
    CAST(NULLIF(person_segments.b2b_hr_decision_makers, 0) AS SMALLINT) AS b2b_hr_decision_makers,
    CAST(NULLIF(person_segments.b2b_hr_executives, 0) AS SMALLINT) AS b2b_hr_executives,
    CAST(NULLIF(person_segments.b2b_hr_managers, 0) AS SMALLINT) AS b2b_hr_managers,
    CAST(NULLIF(company_segments.b2b_ind_agriculture, 0) AS SMALLINT) AS b2b_ind_agriculture,
    CAST(NULLIF(company_segments.b2b_ind_apparel, 0) AS SMALLINT) AS b2b_ind_apparel,
    CAST(NULLIF(company_segments.b2b_ind_automotive, 0) AS SMALLINT) AS b2b_ind_automotive,
    CAST(NULLIF(company_segments.b2b_ind_business_services, 0) AS SMALLINT) AS b2b_ind_business_services,
    CAST(NULLIF(company_segments.b2b_ind_business_services_adv_marketing, 0) AS SMALLINT) AS b2b_ind_business_services_adv_marketing,
    CAST(NULLIF(company_segments.b2b_ind_construction, 0) AS SMALLINT) AS b2b_ind_construction,
    CAST(NULLIF(company_segments.b2b_ind_consumer_services, 0) AS SMALLINT) AS b2b_ind_consumer_services,
    CAST(NULLIF(company_segments.b2b_ind_education, 0) AS SMALLINT) AS b2b_ind_education,
    CAST(NULLIF(company_segments.b2b_ind_education_colleges_univ, 0) AS SMALLINT) AS b2b_ind_education_colleges_univ,
    CAST(NULLIF(company_segments.b2b_ind_energy_and_utilities, 0) AS SMALLINT) AS b2b_ind_energy_and_utilities,
    CAST(NULLIF(company_segments.b2b_ind_finance, 0) AS SMALLINT) AS b2b_ind_finance,
    CAST(NULLIF(company_segments.b2b_ind_finance_investment_banking, 0) AS SMALLINT) AS b2b_ind_finance_investment_banking,
    CAST(NULLIF(company_segments.b2b_ind_food_and_beverages, 0) AS SMALLINT) AS b2b_ind_food_and_beverages,
    CAST(NULLIF(company_segments.b2b_ind_government, 0) AS SMALLINT) AS b2b_ind_government,
    CAST(NULLIF(company_segments.b2b_ind_healthcare, 0) AS SMALLINT) AS b2b_ind_healthcare,
    CAST(NULLIF(company_segments.b2b_ind_healthcare_hosp_clinics, 0) AS SMALLINT) AS b2b_ind_healthcare_hosp_clinics,
    CAST(NULLIF(company_segments.b2b_ind_healthcare_insurance, 0) AS SMALLINT) AS b2b_ind_healthcare_insurance,
    CAST(NULLIF(company_segments.b2b_ind_hospitality_and_hotels, 0) AS SMALLINT) AS b2b_ind_hospitality_and_hotels,
    CAST(NULLIF(company_segments.b2b_ind_information_technology, 0) AS SMALLINT) AS b2b_ind_information_technology,
    CAST(NULLIF(company_segments.b2b_ind_insurance, 0) AS SMALLINT) AS b2b_ind_insurance,
    CAST(NULLIF(company_segments.b2b_ind_legal, 0) AS SMALLINT) AS b2b_ind_legal,
    CAST(NULLIF(company_segments.b2b_ind_manufacturing, 0) AS SMALLINT) AS b2b_ind_manufacturing,
    CAST(NULLIF(company_segments.b2b_ind_media, 0) AS SMALLINT) AS b2b_ind_media,
    CAST(NULLIF(company_segments.b2b_ind_media_internet, 0) AS SMALLINT) AS b2b_ind_media_internet,
    CAST(NULLIF(company_segments.b2b_ind_mining, 0) AS SMALLINT) AS b2b_ind_mining,
    CAST(NULLIF(company_segments.b2b_ind_non_profit, 0) AS SMALLINT) AS b2b_ind_non_profit,
    CAST(NULLIF(company_segments.b2b_ind_oil_and_gas, 0) AS SMALLINT) AS b2b_ind_oil_and_gas,
    CAST(NULLIF(company_segments.b2b_ind_pharmaceutical, 0) AS SMALLINT) AS b2b_ind_pharmaceutical,
    CAST(NULLIF(company_segments.b2b_ind_real_estate, 0) AS SMALLINT) AS b2b_ind_real_estate,
    CAST(NULLIF(company_segments.b2b_ind_real_estate_residential, 0) AS SMALLINT) AS b2b_ind_real_estate_residential,
    CAST(NULLIF(company_segments.b2b_ind_religious_services, 0) AS SMALLINT) AS b2b_ind_religious_services,
    CAST(NULLIF(company_segments.b2b_ind_retail, 0) AS SMALLINT) AS b2b_ind_retail,
    CAST(NULLIF(company_segments.b2b_ind_software, 0) AS SMALLINT) AS b2b_ind_software,
    CAST(NULLIF(company_segments.b2b_ind_software_security, 0) AS SMALLINT) AS b2b_ind_software_security,
    CAST(NULLIF(company_segments.b2b_ind_sports_and_rec, 0) AS SMALLINT) AS b2b_ind_sports_and_rec,
    CAST(NULLIF(company_segments.b2b_ind_telecom, 0) AS SMALLINT) AS b2b_ind_telecom,
    CAST(NULLIF(company_segments.b2b_ind_transportation, 0) AS SMALLINT) AS b2b_ind_transportation,
    CAST(NULLIF(company_segments.b2b_ind_travel, 0) AS SMALLINT) AS b2b_ind_travel,
    CAST(NULLIF(company_segments.b2b_ind_waste_services, 0) AS SMALLINT) AS b2b_ind_waste_services,
    CAST(NULLIF(company_segments.b2b_ind_wholesalers, 0) AS SMALLINT) AS b2b_ind_wholesalers,
    CAST(NULLIF(person_segments.b2b_leadership_business_decision_maker, 0) AS SMALLINT) AS b2b_leadership_business_decision_maker,
    CAST(NULLIF(person_segments.b2b_leadership_female_owner, 0) AS SMALLINT) AS b2b_leadership_female_owner,
    CAST(NULLIF(person_segments.b2b_leadership_minority_owner, 0) AS SMALLINT) AS b2b_leadership_minority_owner,
    CAST(NULLIF(person_segments.b2b_leadership_senior_board_members, 0) AS SMALLINT) AS b2b_leadership_senior_board_members,
    CAST(NULLIF(person_segments.b2b_leadership_senior_executives, 0) AS SMALLINT) AS b2b_leadership_senior_executives,
    CAST(NULLIF(person_segments.b2b_leadership_senior_management, 0) AS SMALLINT) AS b2b_leadership_senior_management,
    CAST(NULLIF(person_segments.b2b_leadership_small_biz_owner, 0) AS SMALLINT) AS b2b_leadership_small_biz_owner,
    CAST(NULLIF(person_segments.b2b_leadership_vp_plus, 0) AS SMALLINT) AS b2b_leadership_vp_plus,
    CAST(NULLIF(person_segments.b2b_marketing_decision_makers, 0) AS SMALLINT) AS b2b_marketing_decision_makers,
    CAST(NULLIF(person_segments.b2b_marketing_executives, 0) AS SMALLINT) AS b2b_marketing_executives,
    CAST(NULLIF(person_segments.b2b_marketing_managers, 0) AS SMALLINT) AS b2b_marketing_managers,
    CAST(NULLIF(person_segments.b2b_operations_decision_makers, 0) AS SMALLINT) AS b2b_operations_decision_makers,
    CAST(NULLIF(person_segments.b2b_operations_executives, 0) AS SMALLINT) AS b2b_operations_executives,
    CAST(NULLIF(person_segments.b2b_operations_managers, 0) AS SMALLINT) AS b2b_operations_managers,
    CAST(NULLIF(person_segments.b2b_r_and_d_decision_makers, 0) AS SMALLINT) AS b2b_r_and_d_decision_makers,
    CAST(NULLIF(person_segments.b2b_r_and_d_executives, 0) AS SMALLINT) AS b2b_r_and_d_executives,
    CAST(NULLIF(person_segments.b2b_r_and_d_managers, 0) AS SMALLINT) AS b2b_r_and_d_managers,
    CAST(NULLIF(person_segments.b2b_sales_decision_makers, 0) AS SMALLINT) AS b2b_sales_decision_makers,
    CAST(NULLIF(person_segments.b2b_sales_executives, 0) AS SMALLINT) AS b2b_sales_executives,
    CAST(NULLIF(person_segments.b2b_sales_managers, 0) AS SMALLINT) AS b2b_sales_managers,
    company_segments.b2b_sales_volume,
    CAST(NULLIF(person_segments.b2b_seniority_advisors, 0) AS SMALLINT) AS b2b_seniority_advisors,
    CAST(NULLIF(person_segments.b2b_seniority_board_of_directors, 0) AS SMALLINT) AS b2b_seniority_board_of_directors,
    CAST(NULLIF(person_segments.b2b_seniority_c_level, 0) AS SMALLINT) AS b2b_seniority_c_level,
    CAST(NULLIF(person_segments.b2b_seniority_director, 0) AS SMALLINT) AS b2b_seniority_director,
    CAST(NULLIF(person_segments.b2b_seniority_founder, 0) AS SMALLINT) AS b2b_seniority_founder,
    CAST(NULLIF(person_segments.b2b_seniority_manager, 0) AS SMALLINT) AS b2b_seniority_manager,
    CAST(NULLIF(person_segments.b2b_seniority_non_management, 0) AS SMALLINT) AS b2b_seniority_non_management,
    CAST(NULLIF(person_segments.b2b_seniority_owner_or_partner, 0) AS SMALLINT) AS b2b_seniority_owner_or_partner,
    CAST(NULLIF(person_segments.b2b_seniority_retired, 0) AS SMALLINT) AS b2b_seniority_retired,
    CAST(NULLIF(person_segments.b2b_seniority_staff, 0) AS SMALLINT) AS b2b_seniority_staff,
    CAST(NULLIF(person_segments.b2b_seniority_student, 0) AS SMALLINT) AS b2b_seniority_student,
    CAST(NULLIF(person_segments.b2b_seniority_vice_president, 0) AS SMALLINT) AS b2b_seniority_vice_president,
    CAST(NULLIF(person_segments.b2b_supply_chain_decision_makers, 0) AS SMALLINT) AS b2b_supply_chain_decision_makers,
    CAST(NULLIF(person_segments.b2b_supply_chain_executives, 0) AS SMALLINT) AS b2b_supply_chain_executives,
    CAST(NULLIF(person_segments.b2b_supply_chain_managers, 0) AS SMALLINT) AS b2b_supply_chain_managers,
    CAST(NULLIF(person_segments.b2b_technology_decision_makers, 0) AS SMALLINT) AS b2b_technology_decision_makers,
    CAST(NULLIF(person_segments.b2b_technology_executives, 0) AS SMALLINT) AS b2b_technology_executives,
    CAST(NULLIF(person_segments.b2b_technology_managers, 0) AS SMALLINT) AS b2b_technology_managers,
    CAST(NULLIF(person_segments.cat_social_media, 0) AS SMALLINT) AS cat_social_media,
    CAST(NULLIF(person_segments.demo_age_18_19, 0) AS SMALLINT) AS demo_age_18_19,
    CAST(NULLIF(person_segments.demo_age_20_24, 0) AS SMALLINT) AS demo_age_20_24,
    CAST(NULLIF(person_segments.demo_age_20_29, 0) AS SMALLINT) AS demo_age_20_29,
    CAST(NULLIF(person_segments.demo_age_25_29, 0) AS SMALLINT) AS demo_age_25_29,
    CAST(NULLIF(person_segments.demo_age_30_34, 0) AS SMALLINT) AS demo_age_30_34,
    CAST(NULLIF(person_segments.demo_age_30_39, 0) AS SMALLINT) AS demo_age_30_39,
    CAST(NULLIF(person_segments.demo_age_35_39, 0) AS SMALLINT) AS demo_age_35_39,
    CAST(NULLIF(person_segments.demo_age_40_44, 0) AS SMALLINT) AS demo_age_40_44,
    CAST(NULLIF(person_segments.demo_age_40_49, 0) AS SMALLINT) AS demo_age_40_49,
    CAST(NULLIF(person_segments.demo_age_45_49, 0) AS SMALLINT) AS demo_age_45_49,
    CAST(NULLIF(person_segments.demo_age_50_54, 0) AS SMALLINT) AS demo_age_50_54,
    CAST(NULLIF(person_segments.demo_age_50_59, 0) AS SMALLINT) AS demo_age_50_59,
    CAST(NULLIF(person_segments.demo_age_55_59, 0) AS SMALLINT) AS demo_age_55_59,
    CAST(NULLIF(person_segments.demo_age_60_64, 0) AS SMALLINT) AS demo_age_60_64,
    CAST(NULLIF(person_segments.demo_age_60_Older, 0) AS SMALLINT) AS demo_age_60_Older,
    CAST(NULLIF(person_segments.demo_age_65_69, 0) AS SMALLINT) AS demo_age_65_69,
    CAST(NULLIF(person_segments.demo_age_70_75, 0) AS SMALLINT) AS demo_age_70_75,
    CAST(NULLIF(person_segments.demo_age_70_Older, 0) AS SMALLINT) AS demo_age_70_Older,
    CAST(NULLIF(person_segments.demo_age_baby_boomers, 0) AS SMALLINT) AS demo_age_baby_boomers,
    CAST(NULLIF(person_segments.demo_age_elders, 0) AS SMALLINT) AS demo_age_elders,
    CAST(NULLIF(person_segments.demo_age_gen_x, 0) AS SMALLINT) AS demo_age_gen_x,
    CAST(NULLIF(person_segments.demo_age_gen_y, 0) AS SMALLINT) AS demo_age_gen_y,
    person_segments.demo_age_lifestages,
    person_segments.demo_education,
    CAST(NULLIF(person_segments.demo_education_college, 0) AS SMALLINT) AS demo_education_college,
    CAST(NULLIF(person_segments.demo_education_grad, 0) AS SMALLINT) AS demo_education_grad,
    CAST(NULLIF(person_segments.demo_education_high_school, 0) AS SMALLINT) AS demo_education_high_school,
    CAST(NULLIF(person_segments.demo_education_student, 0) AS SMALLINT) AS demo_education_student,
    CAST(NULLIF(person_segments.demo_education_undergrad, 0) AS SMALLINT) AS demo_education_undergrad,
    CAST(NULLIF(person_segments.demo_education_vocation, 0) AS SMALLINT) AS demo_education_vocation,
    CAST(NULLIF(person_segments.demo_emp_status_employed, 0) AS SMALLINT) AS demo_emp_status_employed,
    CAST(NULLIF(person_segments.demo_ethnicity_hispanic, 0) AS SMALLINT) AS demo_ethnicity_hispanic,
    person_segments.demo_gender,
    CAST(NULLIF(person_segments.demo_gender_female, 0) AS SMALLINT) AS demo_gender_female,
    CAST(NULLIF(person_segments.demo_gender_male, 0) AS SMALLINT) AS demo_gender_male,
    CAST(NULLIF(person_segments.demo_high_net_worth, 0) AS SMALLINT) AS demo_high_net_worth,
    CAST(NULLIF(person_segments.demo_lang_arabic, 0) AS SMALLINT) AS demo_lang_arabic,
    CAST(NULLIF(person_segments.demo_lang_chinese, 0) AS SMALLINT) AS demo_lang_chinese,
    CAST(NULLIF(person_segments.demo_lang_farsi, 0) AS SMALLINT) AS demo_lang_farsi,
    CAST(NULLIF(person_segments.demo_lang_french, 0) AS SMALLINT) AS demo_lang_french,
    CAST(NULLIF(person_segments.demo_lang_german, 0) AS SMALLINT) AS demo_lang_german,
    CAST(NULLIF(person_segments.demo_lang_group_indian, 0) AS SMALLINT) AS demo_lang_group_indian,
    CAST(NULLIF(person_segments.demo_lang_hindi, 0) AS SMALLINT) AS demo_lang_hindi,
    CAST(NULLIF(person_segments.demo_lang_italian, 0) AS SMALLINT) AS demo_lang_italian,
    CAST(NULLIF(person_segments.demo_lang_japanese, 0) AS SMALLINT) AS demo_lang_japanese,
    CAST(NULLIF(person_segments.demo_lang_korean, 0) AS SMALLINT) AS demo_lang_korean,
    CAST(NULLIF(person_segments.demo_lang_portuguese, 0) AS SMALLINT) AS demo_lang_portuguese,
    CAST(NULLIF(person_segments.demo_lang_russian, 0) AS SMALLINT) AS demo_lang_russian,
    CAST(NULLIF(person_segments.demo_lang_spanish, 0) AS SMALLINT) AS demo_lang_spanish,
    CAST(NULLIF(person_segments.demo_lang_urdu, 0) AS SMALLINT) AS demo_lang_urdu,
    CAST(NULLIF(person_segments.int_inter_online_act, 0) AS SMALLINT) AS int_inter_online_act,
    company_segments.company_normalized_industry,
    CAST(NULLIF(company_segments.b2b_diversity_business_type, 0) AS SMALLINT) AS b2b_diversity_business_type,
    CAST(NULLIF(company_segments.b2b_us_federal_gov_approved_supplier, 0) AS SMALLINT) AS b2b_us_federal_gov_approved_supplier,
    CAST(NULLIF(company_segments.mfg_business_services, 0) AS SMALLINT) AS mfg_business_services,
    CAST(NULLIF(company_segments.mfg_business_services_custom_services, 0) AS SMALLINT) AS mfg_business_services_custom_services,
    CAST(NULLIF(company_segments.mfg_business_type_contract_manufacturer, 0) AS SMALLINT) AS mfg_business_type_contract_manufacturer,
    CAST(NULLIF(company_segments.mfg_business_type_distributor, 0) AS SMALLINT) AS mfg_business_type_distributor,
    CAST(NULLIF(company_segments.mfg_business_type_manufacturer, 0) AS SMALLINT) AS mfg_business_type_manufacturer,
    CAST(NULLIF(company_segments.mfg_business_type_supplier, 0) AS SMALLINT) AS mfg_business_type_supplier,
    CAST(NULLIF(company_segments.mfg_certifications_as_9100, 0) AS SMALLINT) AS mfg_certifications_as_9100,
    CAST(NULLIF(company_segments.mfg_certifications_fda, 0) AS SMALLINT) AS mfg_certifications_fda,
    CAST(NULLIF(company_segments.mfg_certifications_iso, 0) AS SMALLINT) AS mfg_certifications_iso,
    CAST(NULLIF(company_segments.mfg_certifications_iso9001, 0) AS SMALLINT) AS mfg_certifications_iso9001,
    CAST(NULLIF(company_segments.mfg_certifications_itar, 0) AS SMALLINT) AS mfg_certifications_itar,
    CAST(NULLIF(company_segments.mfg_certifications_lean_manufacturer, 0) AS SMALLINT) AS mfg_certifications_lean_manufacturer,
    CAST(NULLIF(company_segments.mfg_certifications_leed, 0) AS SMALLINT) AS mfg_certifications_leed,
    CAST(NULLIF(company_segments.mfg_consumer_products, 0) AS SMALLINT) AS mfg_consumer_products,
    CAST(NULLIF(company_segments.mfg_consumer_products_animal_and_pet_products, 0) AS SMALLINT) AS mfg_consumer_products_animal_and_pet_products,
    CAST(NULLIF(company_segments.mfg_consumer_products_apparel_and_textiles, 0) AS SMALLINT) AS mfg_consumer_products_apparel_and_textiles,
    CAST(NULLIF(company_segments.mfg_consumer_products_appliances, 0) AS SMALLINT) AS mfg_consumer_products_appliances,
    CAST(NULLIF(company_segments.mfg_consumer_products_child_and_baby_care_products, 0) AS SMALLINT) AS mfg_consumer_products_child_and_baby_care_products,
    CAST(NULLIF(company_segments.mfg_consumer_products_computers_av_and_peripherals, 0) AS SMALLINT) AS mfg_consumer_products_computers_av_and_peripherals,
    CAST(NULLIF(company_segments.mfg_consumer_products_consumer_medical_products, 0) AS SMALLINT) AS mfg_consumer_products_consumer_medical_products,
    CAST(NULLIF(company_segments.mfg_consumer_products_consumer_vehicles_and_components, 0) AS SMALLINT) AS mfg_consumer_products_consumer_vehicles_and_components,
    CAST(NULLIF(company_segments.mfg_consumer_products_cosmetics, 0) AS SMALLINT) AS mfg_consumer_products_cosmetics,
    CAST(NULLIF(company_segments.mfg_consumer_products_food_and_food_products, 0) AS SMALLINT) AS mfg_consumer_products_food_and_food_products,
    CAST(NULLIF(company_segments.mfg_consumer_products_household_products, 0) AS SMALLINT) AS mfg_consumer_products_household_products,
    CAST(NULLIF(company_segments.mfg_consumer_products_jewelry, 0) AS SMALLINT) AS mfg_consumer_products_jewelry,
    CAST(NULLIF(company_segments.mfg_consumer_products_marine_products, 0) AS SMALLINT) AS mfg_consumer_products_marine_products,
    CAST(NULLIF(company_segments.mfg_consumer_products_photography, 0) AS SMALLINT) AS mfg_consumer_products_photography,
    CAST(NULLIF(company_segments.mfg_consumer_products_recreation_and_sports_equipment, 0) AS SMALLINT) AS mfg_consumer_products_recreation_and_sports_equipment,
    CAST(NULLIF(company_segments.mfg_defense_and_law_enforcement_aerospace_aircraft, 0) AS SMALLINT) AS mfg_defense_and_law_enforcement_aerospace_aircraft,
    CAST(NULLIF(company_segments.mfg_defense_and_law_enforcement_equipment_and_supplies, 0) AS SMALLINT) AS mfg_defense_and_law_enforcement_equipment_and_supplies,
    CAST(NULLIF(company_segments.mfg_defense_and_law_enforcement_guns_ammunition, 0) AS SMALLINT) AS mfg_defense_and_law_enforcement_guns_ammunition,
    CAST(NULLIF(company_segments.mfg_defense_and_law_enforcement_manufacturing, 0) AS SMALLINT) AS mfg_defense_and_law_enforcement_manufacturing,
    CAST(NULLIF(company_segments.mfg_defense_and_law_enforcement_military_equipment, 0) AS SMALLINT) AS mfg_defense_and_law_enforcement_military_equipment,
    CAST(NULLIF(company_segments.mfg_general_manufacturing, 0) AS SMALLINT) AS mfg_general_manufacturing,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_automation_systems_and_components, 0) AS SMALLINT) AS mfg_general_manufacturing_automation_systems_and_components,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_electrical_and_electronic_components, 0) AS SMALLINT) AS mfg_general_manufacturing_electrical_and_electronic_components,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_facility_equipment_and_supplies, 0) AS SMALLINT) AS mfg_general_manufacturing_facility_equipment_and_supplies,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_fluid_control_and_components, 0) AS SMALLINT) AS mfg_general_manufacturing_fluid_control_and_components,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_hardware_and_fasteners, 0) AS SMALLINT) AS mfg_general_manufacturing_hardware_and_fasteners,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_machinery_tools_and_supplies, 0) AS SMALLINT) AS mfg_general_manufacturing_machinery_tools_and_supplies,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_manufacturing_equipment, 0) AS SMALLINT) AS mfg_general_manufacturing_manufacturing_equipment,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_packaging_and_materials_handling, 0) AS SMALLINT) AS mfg_general_manufacturing_packaging_and_materials_handling,
    CAST(NULLIF(company_segments.mfg_general_manufacturing_test_measurement_and_positioning, 0) AS SMALLINT) AS mfg_general_manufacturing_test_measurement_and_positioning,
    CAST(NULLIF(company_segments.mfg_industries_aerospace, 0) AS SMALLINT) AS mfg_industries_aerospace,
    CAST(NULLIF(company_segments.mfg_industries_automotive, 0) AS SMALLINT) AS mfg_industries_automotive,
    CAST(NULLIF(company_segments.mfg_industries_biomedical, 0) AS SMALLINT) AS mfg_industries_biomedical,
    CAST(NULLIF(company_segments.mfg_industries_construction, 0) AS SMALLINT) AS mfg_industries_construction,
    CAST(NULLIF(company_segments.mfg_industries_defense, 0) AS SMALLINT) AS mfg_industries_defense,
    CAST(NULLIF(company_segments.mfg_industries_electronics, 0) AS SMALLINT) AS mfg_industries_electronics,
    CAST(NULLIF(company_segments.mfg_industries_environmental, 0) AS SMALLINT) AS mfg_industries_environmental,
    CAST(NULLIF(company_segments.mfg_industries_healthcare, 0) AS SMALLINT) AS mfg_industries_healthcare,
    CAST(NULLIF(company_segments.mfg_industries_pharmaceutical, 0) AS SMALLINT) AS mfg_industries_pharmaceutical,
    CAST(NULLIF(company_segments.mfg_industries_renewable_energy, 0) AS SMALLINT) AS mfg_industries_renewable_energy,
    CAST(NULLIF(company_segments.mfg_industries_transportation, 0) AS SMALLINT) AS mfg_industries_transportation,
    CAST(NULLIF(company_segments.mfg_machinery, 0) AS SMALLINT) AS mfg_machinery,
    CAST(NULLIF(company_segments.mfg_machinery_cnc, 0) AS SMALLINT) AS mfg_machinery_cnc,
    CAST(NULLIF(company_segments.mfg_machinery_presses, 0) AS SMALLINT) AS mfg_machinery_presses,
    CAST(NULLIF(company_segments.mfg_machinery_printing, 0) AS SMALLINT) AS mfg_machinery_printing,
    CAST(NULLIF(company_segments.mfg_materials_and_chemicals, 0) AS SMALLINT) AS mfg_materials_and_chemicals,
    CAST(NULLIF(company_segments.mfg_materials_and_chemicals_adhesives_tapes_and_sealants, 0) AS SMALLINT) AS mfg_materials_and_chemicals_adhesives_tapes_and_sealants,
    CAST(NULLIF(company_segments.mfg_materials_and_chemicals_chemicals_and_chemical_products, 0) AS SMALLINT) AS mfg_materials_and_chemicals_chemicals_and_chemical_products,
    CAST(NULLIF(company_segments.mfg_materials_and_chemicals_metals_and_metal_products, 0) AS SMALLINT) AS mfg_materials_and_chemicals_metals_and_metal_products,
    CAST(NULLIF(company_segments.mfg_materials_and_chemicals_polymers_and_polymer_products, 0) AS SMALLINT) AS mfg_materials_and_chemicals_polymers_and_polymer_products,
    CAST(NULLIF(company_segments.tech_inst_cloud_services, 0) AS SMALLINT) AS tech_inst_cloud_services,
    CAST(NULLIF(company_segments.tech_inst_cloud_services_cloud_infrastructure_computing, 0) AS SMALLINT) AS tech_inst_cloud_services_cloud_infrastructure_computing,
    CAST(NULLIF(company_segments.tech_inst_cloud_services_platform_as_a_service, 0) AS SMALLINT) AS tech_inst_cloud_services_platform_as_a_service,
    CAST(NULLIF(company_segments.tech_inst_communications_tech_email, 0) AS SMALLINT) AS tech_inst_communications_tech_email,
    CAST(NULLIF(company_segments.tech_inst_communications_technologies, 0) AS SMALLINT) AS tech_inst_communications_technologies,
    CAST(NULLIF(company_segments.tech_inst_customer_mgmt, 0) AS SMALLINT) AS tech_inst_customer_mgmt,
    CAST(NULLIF(company_segments.tech_inst_customer_mgmt_help_desk, 0) AS SMALLINT) AS tech_inst_customer_mgmt_help_desk,
    CAST(NULLIF(company_segments.tech_inst_data_center, 0) AS SMALLINT) AS tech_inst_data_center,
    CAST(NULLIF(company_segments.tech_inst_data_center_operating_systems_and_computing_languages, 0) AS SMALLINT) AS tech_inst_data_center_operating_systems_and_computing_languages,
    CAST(NULLIF(company_segments.tech_inst_data_center_system_analytics_and_monitoring, 0) AS SMALLINT) AS tech_inst_data_center_system_analytics_and_monitoring,
    CAST(NULLIF(company_segments.tech_inst_enterprise_apps, 0) AS SMALLINT) AS tech_inst_enterprise_apps,
    CAST(NULLIF(company_segments.tech_inst_enterprise_apps_commerce, 0) AS SMALLINT) AS tech_inst_enterprise_apps_commerce,
    CAST(NULLIF(company_segments.tech_inst_enterprise_apps_hr_mgmt_systems, 0) AS SMALLINT) AS tech_inst_enterprise_apps_hr_mgmt_systems,
    CAST(NULLIF(company_segments.tech_inst_enterprise_content, 0) AS SMALLINT) AS tech_inst_enterprise_content,
    CAST(NULLIF(company_segments.tech_inst_enterprise_content_document_mgmt, 0) AS SMALLINT) AS tech_inst_enterprise_content_document_mgmt,
    CAST(NULLIF(company_segments.tech_inst_hardware, 0) AS SMALLINT) AS tech_inst_hardware,
    CAST(NULLIF(company_segments.tech_inst_hardware_consumer_electronics_computers_and_software, 0) AS SMALLINT) AS tech_inst_hardware_consumer_electronics_computers_and_software,
    CAST(NULLIF(company_segments.tech_inst_it_governance, 0) AS SMALLINT) AS tech_inst_it_governance,
    CAST(NULLIF(company_segments.tech_inst_it_governance_application_development_and_mgmt, 0) AS SMALLINT) AS tech_inst_it_governance_application_development_and_mgmt,
    CAST(NULLIF(company_segments.tech_inst_it_governance_change_mgmt, 0) AS SMALLINT) AS tech_inst_it_governance_change_mgmt,
    CAST(NULLIF(company_segments.tech_inst_it_governance_software_configuration_mgmt, 0) AS SMALLINT) AS tech_inst_it_governance_software_configuration_mgmt,
    CAST(NULLIF(company_segments.tech_inst_marketing_performance_mgmt, 0) AS SMALLINT) AS tech_inst_marketing_performance_mgmt,
    CAST(NULLIF(company_segments.tech_inst_marketing_performance_mgmt_business_intelligence, 0) AS SMALLINT) AS tech_inst_marketing_performance_mgmt_business_intelligence,
    CAST(NULLIF(company_segments.tech_inst_marketing_performance_mgmt_measurement, 0) AS SMALLINT) AS tech_inst_marketing_performance_mgmt_measurement,
    CAST(NULLIF(company_segments.tech_inst_network_computing, 0) AS SMALLINT) AS tech_inst_network_computing,
    CAST(NULLIF(company_segments.tech_inst_network_computing_network_mgmt_hardware, 0) AS SMALLINT) AS tech_inst_network_computing_network_mgmt_hardware,
    CAST(NULLIF(company_segments.tech_inst_network_computing_network_mgmt_software, 0) AS SMALLINT) AS tech_inst_network_computing_network_mgmt_software,
    CAST(NULLIF(company_segments.tech_inst_productivity_solutions, 0) AS SMALLINT) AS tech_inst_productivity_solutions,
    CAST(NULLIF(company_segments.tech_inst_productivity_solutions_collaboration, 0) AS SMALLINT) AS tech_inst_productivity_solutions_collaboration,
    CAST(NULLIF(company_segments.tech_inst_software, 0) AS SMALLINT) AS tech_inst_software,
    CAST(NULLIF(company_segments.tech_inst_software_search_engine, 0) AS SMALLINT) AS tech_inst_software_search_engine,
    CAST(NULLIF(company_segments.tech_inst_software_server_technologies, 0) AS SMALLINT) AS tech_inst_software_server_technologies,
    CAST(NULLIF(company_segments.tech_inst_vertical_markets, 0) AS SMALLINT) AS tech_inst_vertical_markets,
    CAST(NULLIF(company_segments.tech_inst_vertical_markets_academic_and_education_mgmt_software, 0) AS SMALLINT) AS tech_inst_vertical_markets_academic_and_education_mgmt_software,
    CAST(NULLIF(company_segments.tech_inst_web_oriented_arch_online_video_platform, 0) AS SMALLINT) AS tech_inst_web_oriented_arch_online_video_platform,
    CAST(NULLIF(company_segments.tech_inst_web_oriented_arch_remote_server_solutions, 0) AS SMALLINT) AS tech_inst_web_oriented_arch_remote_server_solutions,
    CAST(NULLIF(company_segments.tech_inst_web_oriented_arch_social_media_systems, 0) AS SMALLINT) AS tech_inst_web_oriented_arch_social_media_systems,
    CAST(NULLIF(company_segments.tech_inst_web_oriented_arch_virtualization_data_center, 0) AS SMALLINT) AS tech_inst_web_oriented_arch_virtualization_data_center,
    CAST(NULLIF(company_segments.tech_inst_web_oriented_arch_virtualization_platform_mgmt, 0) AS SMALLINT) AS tech_inst_web_oriented_arch_virtualization_platform_mgmt,
    CAST(NULLIF(company_segments.tech_inst_web_oriented_arch_web_and_portal_technology, 0) AS SMALLINT) AS tech_inst_web_oriented_arch_web_and_portal_technology,
    CAST(NULLIF(company_segments.tech_inst_web_oriented_arch_web_content_mgmt_system, 0) AS SMALLINT) AS tech_inst_web_oriented_arch_web_content_mgmt_system,
    CAST(NULLIF(company_segments.tech_inst_web_oriented_architecture, 0) AS SMALLINT) AS tech_inst_web_oriented_architecture,
    persons.random_tenth_percent
FROM persons
LEFT OUTER JOIN temp.athena_b2b_emails ON persons.match_id = temp.athena_b2b_emails.person_match_id
LEFT OUTER JOIN temp.athena_personal_emails_raw ON persons.match_id = temp.athena_personal_emails_raw.person_match_id
LEFT OUTER JOIN temp.athena_personal_emails_md5 ON persons.match_id = temp.athena_personal_emails_md5.person_match_id
LEFT OUTER JOIN temp.athena_emails_onboarding ON persons.match_id = temp.athena_emails_onboarding.person_match_id
LEFT OUTER JOIN companies ON persons.company_match_id = companies.match_id
LEFT OUTER JOIN company_locations ON persons.company_match_id = company_locations.match_id AND persons.company_location_id = company_locations.location_id
LEFT OUTER JOIN person_segments ON persons.match_id = person_segments.person_match_id
LEFT OUTER JOIN company_segments ON companies.match_id = company_segments.company_match_id
LEFT OUTER JOIN company_locations company_locations2 ON companies.match_id = company_locations.match_id AND companies.location_id = company_locations2.location_id
LEFT OUTER JOIN person_locations ON persons.match_id = person_locations.match_id AND persons.location_id = person_locations.location_id
;

-- DROP TABLE temp.athena_b2b_emails;
-- DROP TABLE temp.athena_personal_emails_raw;
-- DROP TABLE temp.athena_personal_emails_md5;
-- DROP TABLE temp.athena_emails_onboarding;