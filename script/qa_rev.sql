-- "count","count"
-- 133871008,127591527
SELECT count(*),
    count(nullif(title, ''))
FROM vx.production_persons
WHERE matched_processed_company_id <> 0
;

-- "count","count"
-- 154006222,124746486
SELECT count(*),
    count(nullif(title, ''))
FROM production.persons
WHERE company_match_id <> 0
;

-- "count","count"
-- 78707871,74592424
SELECT count(*),
    count(nullif(title, ''))
FROM vx.production_persons
WHERE matched_processed_company_id <> 0
AND lower(personal_country) = 'united states'
;

-- "count","count"
-- 67027279,63622846
SELECT count(*),
    count(nullif(title, ''))
FROM production.persons p
LEFT OUTER JOIN production.person_locations pl ON p.match_id = pl.match_id AND pl.location_id = p.location_id
WHERE company_match_id <> ''
AND lower(country) = 'united states'
;

-- "count","count"
-- 74004534,69991953
SELECT count(*),
    count(nullif(title, ''))
FROM vx.production_persons
WHERE matched_processed_company_id <> 0
AND lower(personal_country) = 'united states'
AND lower(company_country_waterfall) = 'united states'
;

-- "count","count"
-- 63007189,59724536
SELECT count(*),
    count(nullif(title, ''))
FROM production.persons p
INNER JOIN production.person_locations pl ON p.match_id = pl.match_id AND pl.location_id = p.location_id
INNER JOIN production.company_locations cl ON p.company_match_id = cl.match_id AND p.company_location_id = cl.location_id
WHERE company_match_id <> ''
AND lower(pl.country) = 'united states'
AND lower(cl.country) = 'united states'
;


CREATE TABLE temp.object_is_digital_only (
object_id char(32) encode raw sortkey distkey,
is_digital_only SMALLINT encode zstd
);

INSERT INTO temp.object_is_digital_only
SELECT object_id,
    MIN(is_digital_only) AS is_digital_only
FROM (
    SELECT o.object_id,
        CASE
            WHEN so.name = 'Panscient Company Web Crawl' AND pr.name = 'Panscient'
                AND e.is_generic = False
                THEN 0
            WHEN so.name = 'CrimeTime Public Records (see data_sources_crime_time_2018_10_26)' AND pr.name = 'CrimeTime'
                AND p.first_name <> ''
                AND p.last_name <> ''
                AND p.title <> ''
                AND e.email <> ''
                AND b.name <> ''
                AND bl.compiled_address1 <> ''
                AND bl.compiled_city <> ''
                AND bl.compiled_state <> ''
                THEN 0
            WHEN so.name = 'Linkedin Persons' AND pr.name = 'OxyLeads'
                AND COALESCE(p.number_of_connections, 1) > 0
                THEN 0
            ELSE 1
        END AS is_digital_only
    FROM staging.objects o
    INNER JOIN staging.sources so ON o.source_id = so.id
    INNER JOIN staging.providers pr ON o.provider_id = pr.id
    LEFT OUTER JOIN staging.persons p ON o.object_id = p.object_id
    LEFT OUTER JOIN staging.emails e ON o.object_id = e.object_id
    LEFT OUTER JOIN staging.businesses b ON o.object_id = b.object_id
    LEFT OUTER JOIN staging.business_locations bl ON o.object_id = bl.object_id
)
GROUP BY object_id
;

CREATE TABLE temp.match_is_digital_only (
match_id char(32) encode raw sortkey distkey,
is_digital_only SMALLINT encode zstd
);

INSERT INTO temp.match_is_digital_only
SELECT pt.match_id, MIN(d.is_digital_only) AS is_digital_only
FROM production.person_traceback pt
INNER JOIN temp.object_is_digital_only d ON pt.object_id = d.object_id
GROUP BY pt.match_id;

CREATE TABLE production.persons2 (LIKE production.persons);
ALTER TABLE production.persons2 ADD COLUMN is_digital_only smallint;

INSERT INTO production.persons2
SELECT p.*, d.is_digital_only
FROM production.persons p
INNER JOIN temp.match_is_digital_only d ON p.match_id = d.match_id
;

DROP TABLE production.persons;
ALTER TABLE production.persons2 rename TO persons;



cloud tech - always 1
panscient - 1 if non-generic email
crimetime - 1 if first_name, last_name, title, email, company_name, company_address1, company_city, company_state
localblox linked persons - always 1
oxyleads li persons - always 0
all others - always 1


if number_of_connections = 0 then 1, if null then 0 - this only makes things digital_only, it will NOT change something from digital_only to offline (just a filter for junk LI profiles)

if any record rolled in is 0 for is digital only, make 0