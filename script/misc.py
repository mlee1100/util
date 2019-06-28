import sys
reload(sys)
sys.setdefaultencoding('utf8')
infile = 'DSG_AutoFin_FEB2018fx.csv'
outfile = 'DSG_AutoFin_FEB2018fx2.csv'
with open(infile, 'rb') as ifile, open(outfile, 'wb') as ofile:
    for line in ifile:
        ofile.write(line.rstrip().rstrip(',,,') + '\n')




import sys
reload(sys)
sys.setdefaultencoding('utf8')
infile = 'DSG_AutoFin_FEB2018fx2.csv'
with open(infile, 'rb') as ifile:
    for line in ifile:
        if line.count(',') > 23:
            print line




import csv
f = 'email_hippo_initial_file.csv'
o = 'email_hippo_initial_file_dedup.csv'
email_set = set()
with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.DictReader(ifile, delimiter=',', fieldnames=['person_id', 'email'])
    ocsv = csv.DictWriter(ofile, delimiter=',', fieldnames=['person_id', 'email'])
    for line in icsv:
        if line['email'] not in email_set:
            email_set.add(line['email'])
            ocsv.writerow(line)



import csv
f = 'email_hippo_initial_file.csv'
email_set = set()
with open(f, 'rb') as ifile:
    icsv = csv.DictReader(ifile, delimiter=',', fieldnames=['person_id', 'email'])
    for line in icsv:
        domain = line['email'].split('@')[-1]
        if domain not in email_set:
            email_set.add(domain)
        else:
            print domain

print len(email_set)



import json
f = 'zillow_profiles_complete.jl'
counts = dict()

with open(f, 'rb') as ifile:
    for line in ifile:
        if 'linkedin.com' in line.lower():
            counts['linkedin_found'] = counts.get('linkedin_found', 0) + 1
        jline = json.loads(line)
        if jline.get('links', dict()).get('linkedin', ''):
            counts['linkedin_parsed'] = counts.get('linkedin_parsed', 0) + 1


print counts



import json
f = 'zillow_profiles_complete.jl'
keys = set()
with open(f, 'rb') as ifile:
    for i, line in enumerate(ifile):
        j = json.loads(line)
        key = j.get('_key', None)
        if key:
            keys.add(key)


print '{} records'.format(i+1)
print '{} unique keys'.format(len(keys))



import csv
f = 'DOC_Netwise_20180507_ascii.txt'
source_dict = dict()
counter = 1
with open(f, 'rb') as ifile:
    icsv = csv.DictReader(ifile, quoting=csv.QUOTE_ALL, delimiter='|')
    for i, line in enumerate(icsv):
        counter += 1
        if counter == 1000000:
            print i
            counter = 0
        if line['Source'] in source_dict:
            source_dict[line['Source']]['count'] += 1
            if line['DOBNew']:
                source_dict[line['Source']]['dobs'] += 1
        else:
            source_dict[line['Source']] = dict(count=1)
            if line['DOBNew']:
                source_dict[line['Source']]['dobs'] = 1
            else:
                source_dict[line['Source']]['dobs'] = 0


for sc in sorted(source_dict.keys()):
    print '{}|{:,}|{:,}'.format(sc, source_dict[sc]['count'], source_dict[sc]['dobs'])





# ----------

import json
import os
import hashlib
files = [
    'people_with_email_TN_additional_verified_20180613.json',
    'people_with_email_TN_unverified_20180613.json',
    'people_with_email_TN_verified_20180613.json',
    ]

for file in files:
    outfile = os.path.splitext(file)[0] + '.csv'
    with open(file, 'rb') as ifile, open(outfile, 'wb') as ofile:
        for i, line in enumerate(ifile):
            emails = set()
            j = json.loads(line)
            if 'mainEmail' in j:
                emails.add(j['mainEmail'])
            if 'otherEmails' in j:
                emails.update(j['otherEmails'])
            for email in emails:
                ofile.write(hashlib.md5(email.strip().lower()).hexdigest() + '\n')
            # if i % 1000000 == 0:
            #     print i


# ------

import json
import os
import hashlib
files = [
    'people_with_email_TN_additional_verified_20180613.json',
    # 'people_with_email_TN_unverified_20180613.json',
    # 'people_with_email_TN_verified_20180613.json',
    ]

for file in files:
    with open(file, 'rb') as ifile:
        for i, line in enumerate(ifile):
            j = json.loads(line)
            if 'mainEmail' not in j and 'otherEmails' not in j:
                print line
                break




# ------
f = 'liveramp_matchback_20180621.csv'
o = 'liveramp_matchback_20180621.formatted.csv'
with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    ofile.write('email_MD5|file_wide_field\n')
    for line in ifile:
        ofile.write(line.strip() + '|1\n')



# -----

import csv
import os
import hashlib
f = '53860_netwise_uscon_test.txt'
with open(f, 'rb') as ifile, open(os.path.splitext(f)[0] + '.csv', 'wb') as ofile:
    icsv = csv.DictReader(ifile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    for line in icsv:
        for i in range(3):
            if line['email_address_{}'.format(i+1)].strip():
                ofile.write(hashlib.md5(line['email_address_{}'.format(i+1)].strip().lower()).hexdigest() + '\n')


# -----

import csv
infile = 'premover-v4-20180717.tab'
outfile = 'premover-v4-20180717.csv'

insettings = dict(
    delimiter = '\t',
    quoting = csv.QUOTE_NONE,
    escapechar = '\\',
    doublequote = False,
    )

outsettings = dict(
    delimiter = ',',
    quoting = csv.QUOTE_ALL,
    escapechar = '\\',
    doublequote = False,
    )

zips = set([
    '30309',
    '45414',
    '55443',
    '35487',
    '95476',
    '33427',
    '07040',
    '89106',
    '85034',
    '90001',
    '60606',
    '10128',
    '75201',
    '48201',
    '98101',
    ])

zip_counts = dict()
status_counts = dict()

with open(infile, 'rb') as ifile, open(outfile, 'wb') as ofile:
    icsv = csv.DictReader(ifile, **insettings)
    ocsv = csv.DictWriter(ofile, fieldnames=icsv.fieldnames, **outsettings)
    ocsv.writeheader()
    for i, line in enumerate(icsv):
        if i % 100000 == 0:
            print i
        if line['zip'] in zips:
            # record_counts[line['zip']] = record_counts.get(line['zip'], 0) + 1
            # status_counts[line['status']] = status_counts.get(line['status'], 0) + 1
            ocsv.writerow(line)


print record_counts
print status_counts







import json

files = [
    'nw_li_comp_01.json',
    'nw_li_comp_02.json',
    'nw_li_comp_03.json',
    'nw_li_comp_04.json',
    'nw_li_comp_05.json',
    'nw_li_comp_06.json',
    'nw_li_comp_07.json',
    'nw_li_comp_08.json',
    'nw_li_comp_09.json',
    'nw_li_comp_10.json',
    'nw_li_comp_10.json',
    'nw_li_comp_11.json',
    'nw_li_comp_12.json',
    'nw_li_comp_13.json',
    'nw_li_comp_14.json',
    'nw_li_comp_15.json',
    'nw_li_comp_16.json',
    ]


records = 0
founded = 0

for file in files:
    with open(file, 'rb') as ifile:
        for line in ifile:
            j = json.loads(line)
            records += 1
            # if int(j.get('founded', 0)):
            if 1500 <= int(j.get('founded', 0)) <= 2018:
                founded += 1
    print '{:,} / {:,}'.format(founded, records)


print founded
print records






import csv
import tldextract

def get_domain(string):
    if not string:
        return None
    parsed_url = tldextract.extract(string.strip().lower())
    if parsed_url.domain and parsed_url.suffix:
        return '.'.join([parsed_url.domain, parsed_url.suffix])
    print string
    return None

isp_file = 'bombora_ip_domain_append_v2.csv'
isp_outfile = 'db_bombora_ip_domain_append_v2.csv'

fieldnames = (
    'isISP',
    'ipAddress',
    'probability',
    'companyName',
    'websiteURL',
    'hqAddress1',
    'hqAddress2',
    'hqCity',
    'hqStateProvReg',
    'hqZip',
    'hqCountry',
    'parentCompany',
    'parentAddress',
    'parentCity',
    'parentStateProvReg',
    'parentZIP',
    'parentCountry',
    'phone',
    'industry',
    'orgWatch',
    'revenue',
    'employees',
    'stockExchange',
    'tickerSymbol',
    'domain',
    'provider_record_id',
    )



icsv_settings = dict(
    delimiter = ',',
    quoting = csv.QUOTE_MINIMAL,
    )

ocsv_settings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # escapechar = '\\',
    escapechar = None,
    doublequote = True,
    fieldnames = fieldnames,
    restval = None,
    lineterminator = '\n'
    )

def escape(dict_):
    return {k: v.replace('\\', '\\\\') for k, v in dict_.iteritems()}

with open(isp_file, 'rb') as ifile, open(isp_outfile, 'wb') as ofile:
    icsv = csv.DictReader(ifile, **icsv_settings)
    ocsv = csv.DictWriter(ofile, **ocsv_settings)
    ocsv.writeheader()
    for line in icsv:
        # line = escape(line)
        try:
            line['domain'] = get_domain(line['websiteURL'])
            line['isISP'] = (1 if line['isISP'] == 'TRUE' else 0)
            if line['probability']:
                line['probability'] = float(line['probability'])
            else:
                line['probability'] = None
            ocsv.writerow(line)
        except:
            print line
            raise





import csv

infile = 'nwd_liveramp_2018_06_25_13_37_42.psv'
outfile = 'nwd_liveramp_2018_06_25_13_37_42.min.psv'

iheader = [
    'person_id',
    'first_name',
    'last_name',
    'md5_1',
    'md5_2',
    'md5_3',
    'md5_4',
    'md5_5',
    'md5_6',
    'md5_7',
    'md5_8',
    'company_name',
    'domain',
    'title',
    'phone',
    'address1',
    'address2',
    'city',
    'state',
    'zip',
    'country',
    'b2b_company_size',
    'b2b_sales_volume',
    'b2b_seniority',
    'b2b_job_function',
    'b2b_job_sub_function',
    'b2b_company_size_micro',
    'b2b_company_size_small',
    'b2b_company_size_med',
    'b2b_company_size_med_large',
    'b2b_company_size_large',
    'b2b_company_size_xlarge',
    'b2b_company_revenue_micro',
    'b2b_company_revenue_small',
    'b2b_company_revenue_med',
    'b2b_company_revenue_med_large',
    'b2b_company_revenue_large',
    'b2b_company_revenue_xlarge',
    'b2b_seniority_advisors',
    'b2b_seniority_board_of_directors',
    'b2b_seniority_c_level',
    'b2b_seniority_director',
    'b2b_seniority_founder',
    'b2b_seniority_manager',
    'b2b_seniority_non_management',
    'b2b_seniority_owner_or_partner',
    'b2b_seniority_retired',
    'b2b_seniority_staff',
    'b2b_seniority_student',
    'b2b_seniority_vice_president',
    'b2b_leadership_senior_board_members',
    'b2b_leadership_business_decision_maker',
    'b2b_leadership_senior_executives',
    'b2b_leadership_senior_management',
    'b2b_leadership_vp_plus',
    'b2b_engineering_decision_makers',
    'b2b_engineering_executives',
    'b2b_engineering_managers',
    'b2b_finance_decision_makers',
    'b2b_finance_executives',
    'b2b_finance_managers',
    'b2b_healthcare_decision_makers',
    'b2b_healthcare_executives',
    'b2b_healthcare_managers',
    'b2b_hr_decision_makers',
    'b2b_hr_executives',
    'b2b_hr_managers',
    'b2b_marketing_decision_makers',
    'b2b_marketing_executives',
    'b2b_marketing_managers',
    'b2b_operations_decision_makers',
    'b2b_operations_executives',
    'b2b_operations_managers',
    'b2b_r_and_d_decision_makers',
    'b2b_r_and_d_executives',
    'b2b_r_and_d_managers',
    'b2b_sales_decision_makers',
    'b2b_sales_executives',
    'b2b_sales_managers',
    'b2b_supply_chain_decision_makers',
    'b2b_supply_chain_executives',
    'b2b_supply_chain_managers',
    'b2b_technology_decision_makers',
    'b2b_technology_executives',
    'b2b_technology_managers',
    'b2b_fa_admin',
    'b2b_fa_admin_administrator',
    'b2b_fa_admin_assistant',
    'b2b_fa_admin_office_manager',
    'b2b_fa_admin_secretary',
    'b2b_fa_admin_typist',
    'b2b_fa_consultants',
    'b2b_fa_education',
    'b2b_fa_education_admissions',
    'b2b_fa_education_librarian',
    'b2b_fa_education_professor',
    'b2b_fa_education_teacher',
    'b2b_fa_engineering',
    'b2b_fa_engineering_biomedical',
    'b2b_fa_engineering_chemical',
    'b2b_fa_engineering_civil',
    'b2b_fa_engineering_electrical',
    'b2b_fa_engineering_electronics',
    'b2b_fa_engineering_industrial',
    'b2b_fa_engineering_mechanical',
    'b2b_fa_finance',
    'b2b_fa_finance_accounting',
    'b2b_fa_finance_analyst',
    'b2b_fa_finance_audit',
    'b2b_fa_finance_banking',
    'b2b_fa_finance_cfp',
    'b2b_fa_finance_compliance_specialist',
    'b2b_fa_finance_corporate',
    'b2b_fa_finance_cpa',
    'b2b_fa_finance_economist',
    'b2b_fa_finance_investment_banking',
    'b2b_fa_finance_mortgage_specialist',
    'b2b_fa_finance_risk_management',
    'b2b_fa_finance_statistician',
    'b2b_fa_finance_tax_specialist',
    'b2b_fa_finance_treasurer',
    'b2b_fa_finance_wealth_management',
    'b2b_fa_general_management',
    'b2b_fa_general_management_board_member',
    'b2b_fa_general_management_ceo',
    'b2b_fa_general_management_cfo',
    'b2b_fa_general_management_cio',
    'b2b_fa_general_management_cmo',
    'b2b_fa_general_management_coo',
    'b2b_fa_general_management_cto',
    'b2b_fa_general_management_director',
    'b2b_fa_general_management_executive_vp',
    'b2b_fa_general_management_founder',
    'b2b_fa_general_management_manager_or_supervisor',
    'b2b_fa_general_management_owner',
    'b2b_fa_general_management_partner',
    'b2b_fa_general_management_president',
    'b2b_fa_general_management_senior_board_member',
    'b2b_fa_general_management_vp',
    'b2b_fa_government',
    'b2b_fa_government_elected_officials',
    'b2b_fa_healthcare',
    'b2b_fa_healthcare_chiropractor',
    'b2b_fa_healthcare_dental_assistant',
    'b2b_fa_healthcare_dental_hygenist',
    'b2b_fa_healthcare_dentist',
    'b2b_fa_healthcare_dietician',
    'b2b_fa_healthcare_healthcare_professional',
    'b2b_fa_healthcare_nurse',
    'b2b_fa_healthcare_opticians',
    'b2b_fa_healthcare_optometrist',
    'b2b_fa_healthcare_paramedic',
    'b2b_fa_healthcare_pharmaceuticals',
    'b2b_fa_healthcare_pharmacist',
    'b2b_fa_healthcare_physical_therapist',
    'b2b_fa_healthcare_physician_or_doctor',
    'b2b_fa_healthcare_physicians_assistant',
    'b2b_fa_healthcare_psychologist',
    'b2b_fa_healthcare_therapist',
    'b2b_fa_human_resources',
    'b2b_fa_human_resources_benefits_and_compensation',
    'b2b_fa_human_resources_employee_development',
    'b2b_fa_human_resources_payroll_specialist',
    'b2b_fa_human_resources_recruiting',
    'b2b_fa_law_enforcement',
    'b2b_fa_law_enforcement_correctional_workers',
    'b2b_fa_law_enforcement_parole_officer',
    'b2b_fa_law_enforcement_police',
    'b2b_fa_legal',
    'b2b_fa_legal_judge',
    'b2b_fa_legal_lawyer',
    'b2b_fa_legal_paralegal',
    'b2b_fa_legal_prosecutor',
    'b2b_fa_marketing',
    'b2b_fa_marketing_advertising',
    'b2b_fa_marketing_creative',
    'b2b_fa_marketing_events',
    'b2b_fa_marketing_marketing_communications',
    'b2b_fa_military',
    'b2b_fa_military_air_force',
    'b2b_fa_military_army',
    'b2b_fa_military_coast_guard',
    'b2b_fa_military_marines',
    'b2b_fa_military_navy',
    'b2b_fa_operations',
    'b2b_fa_operations_environmental',
    'b2b_fa_operations_facility_maintenance',
    'b2b_fa_operations_food_and_beverage',
    'b2b_fa_operations_manufacturing',
    'b2b_fa_operations_quality_assurance',
    'b2b_fa_operations_records_management',
    'b2b_fa_operations_safety',
    'b2b_fa_operations_training',
    'b2b_fa_photographer',
    'b2b_fa_real_estate',
    'b2b_fa_real_estate_architect',
    'b2b_fa_real_estate_builder',
    'b2b_fa_real_estate_contractor',
    'b2b_fa_real_estate_developer',
    'b2b_fa_real_estate_mortgage_specialist',
    'b2b_fa_real_estate_real_estate_agent',
    'b2b_fa_religious_leader',
    'b2b_fa_research_and_development',
    'b2b_fa_research_and_development_product_development',
    'b2b_fa_research_and_development_project_management',
    'b2b_fa_research_and_development_research',
    'b2b_fa_retired',
    'b2b_fa_sales',
    'b2b_fa_sales_account_management',
    'b2b_fa_sales_business_development',
    'b2b_fa_sales_community_development',
    'b2b_fa_sales_customer_service',
    'b2b_fa_science',
    'b2b_fa_science_chemist',
    'b2b_fa_science_geologist',
    'b2b_fa_science_scientists',
    'b2b_fa_stay_at_home_parent',
    'b2b_fa_student',
    'b2b_fa_supply_chain',
    'b2b_fa_supply_chain_logistics',
    'b2b_fa_supply_chain_procurement',
    'b2b_fa_supply_chain_sourcing',
    'b2b_fa_technology',
    'b2b_fa_technology_computing',
    'b2b_fa_technology_data_management',
    'b2b_fa_technology_dba',
    'b2b_fa_technology_information_security',
    'b2b_fa_technology_information_technology',
    'b2b_fa_technology_mis',
    'b2b_fa_technology_network_administration',
    'b2b_fa_technology_quality_assurance',
    'b2b_fa_technology_software',
    'b2b_fa_technology_systems_admin',
    'b2b_fa_technology_technical_support',
    'b2b_fa_technology_telecom',
    'b2b_fa_technology_web_development',
    'b2b_fa_writer',
    'b2b_ind_business_services',
    'b2b_ind_business_services_adv_marketing',
    'b2b_ind_construction',
    'b2b_ind_education',
    'b2b_ind_education_colleges_univ',
    'b2b_ind_finance',
    'b2b_ind_finance_investment_banking',
    'b2b_ind_government',
    'b2b_ind_healthcare',
    'b2b_ind_healthcare_hosp_clinics',
    'b2b_ind_healthcare_insurance',
    'b2b_ind_insurance',
    'b2b_ind_manufacturing',
    'b2b_ind_media_internet',
    'b2b_ind_non_profit',
    'b2b_ind_real_estate',
    'b2b_ind_real_estate_residential',
    'b2b_ind_retail',
    'b2b_ind_software',
    'b2b_ind_software_security',
    'b2b_ind_telecom',
    'demo_emp_status_employed',
    'b2b_us_federal_gov_approved_supplier',
    'b2b_diversity_business_type',
    'mfg_general_manufacturing',
    'mfg_general_manufacturing_automation_systems_and_components',
    'mfg_general_manufacturing_electrical_and_electronic_components',
    'mfg_general_manufacturing_facility_equipment_and_supplies',
    'mfg_general_manufacturing_fluid_control_and_components',
    'mfg_general_manufacturing_hardware_and_fasteners',
    'mfg_general_manufacturing_machinery_tools_and_supplies',
    'mfg_general_manufacturing_manufacturing_equipment',
    'mfg_general_manufacturing_packaging_and_materials_handling',
    'mfg_general_manufacturing_test_measurement_and_positioning',
    'mfg_materials_and_chemicals',
    'mfg_materials_and_chemicals_adhesives_tapes_and_sealants',
    'mfg_materials_and_chemicals_chemicals_and_chemical_products',
    'mfg_materials_and_chemicals_metals_and_metal_products',
    'mfg_materials_and_chemicals_polymers_and_polymer_products',
    'mfg_business_services',
    'mfg_business_services_custom_services',
    'mfg_consumer_products',
    'mfg_consumer_products_animal_and_pet_products',
    'mfg_consumer_products_apparel_and_textiles',
    'mfg_consumer_products_appliances',
    'mfg_consumer_products_child_and_baby_care_products',
    'mfg_consumer_products_computers_av_and_peripherals',
    'mfg_consumer_products_consumer_medical_products',
    'mfg_consumer_products_consumer_vehicles_and_components',
    'mfg_consumer_products_cosmetics',
    'mfg_consumer_products_food_and_food_products',
    'mfg_consumer_products_household_products',
    'mfg_consumer_products_jewelry',
    'mfg_consumer_products_marine_products',
    'mfg_consumer_products_photography',
    'mfg_consumer_products_recreation_and_sports_equipment',
    'mfg_defense_and_law_enforcement_manufacturing',
    'mfg_defense_and_law_enforcement_aerospace_aircraft',
    'mfg_defense_and_law_enforcement_guns_ammunition',
    'mfg_defense_and_law_enforcement_equipment_and_supplies',
    'mfg_defense_and_law_enforcement_military_equipment',
    'mfg_certifications_iso',
    'mfg_certifications_iso9001',
    'mfg_certifications_itar',
    'mfg_certifications_fda',
    'mfg_certifications_as_9100',
    'mfg_certifications_leed',
    'mfg_certifications_lean_manufacturer',
    'mfg_industries_aerospace',
    'mfg_industries_automotive',
    'mfg_industries_biomedical',
    'mfg_industries_construction',
    'mfg_industries_defense',
    'mfg_industries_electronics',
    'mfg_industries_environmental',
    'mfg_industries_healthcare',
    'mfg_industries_pharmaceutical',
    'mfg_industries_renewable_energy',
    'mfg_industries_transportation',
    'mfg_machinery',
    'mfg_machinery_cnc',
    'mfg_machinery_presses',
    'mfg_machinery_printing',
    'mfg_business_type_manufacturer',
    'mfg_business_type_contract_manufacturer',
    'mfg_business_type_distributor',
    'mfg_business_type_supplier',
    'tech_inst_cloud_services',
    'tech_inst_communications_technologies',
    'tech_inst_customer_mgmt',
    'tech_inst_data_center',
    'tech_inst_enterprise_apps',
    'tech_inst_enterprise_content',
    'tech_inst_hardware',
    'tech_inst_it_governance',
    'tech_inst_marketing_performance_mgmt',
    'tech_inst_network_computing',
    'tech_inst_productivity_solutions',
    'tech_inst_software',
    'tech_inst_vertical_markets',
    'tech_inst_web_oriented_architecture',
    'tech_inst_cloud_services_cloud_infrastructure_computing',
    'tech_inst_cloud_services_platform_as_a_service',
    'tech_inst_communications_tech_email',
    'tech_inst_customer_mgmt_help_desk',
    'tech_inst_data_center_operating_systems_and_computing_languages',
    'tech_inst_data_center_system_analytics_and_monitoring',
    'tech_inst_enterprise_apps_commerce',
    'tech_inst_enterprise_apps_hr_mgmt_systems',
    'tech_inst_enterprise_content_document_mgmt',
    'tech_inst_hardware_consumer_electronics_computers_and_software',
    'tech_inst_it_governance_application_development_and_mgmt',
    'tech_inst_it_governance_change_mgmt',
    'tech_inst_it_governance_software_configuration_mgmt',
    'tech_inst_marketing_performance_mgmt_business_intelligence',
    'tech_inst_marketing_performance_mgmt_measurement',
    'tech_inst_network_computing_network_mgmt_hardware',
    'tech_inst_network_computing_network_mgmt_software',
    'tech_inst_productivity_solutions_collaboration',
    'tech_inst_software_search_engine',
    'tech_inst_software_server_technologies',
    'tech_inst_vertical_markets_academic_and_education_mgmt_software',
    'tech_inst_web_oriented_arch_online_video_platform',
    'tech_inst_web_oriented_arch_remote_server_solutions',
    'tech_inst_web_oriented_arch_social_media_systems',
    'tech_inst_web_oriented_arch_virtualization_platform_mgmt',
    'tech_inst_web_oriented_arch_virtualization_data_center',
    'tech_inst_web_oriented_arch_web_and_portal_technology',
    'tech_inst_web_oriented_arch_web_content_mgmt_system',
    'b2b_leadership_small_biz_owner',
    'b2b_leadership_female_owner',
    'b2b_leadership_minority_owner',
    'b2b_company_fortune_500',
    'b2b_company_fortune_1000',
    'b2b_ind_agriculture',
    'b2b_ind_apparel',
    'b2b_ind_automotive',
    'b2b_ind_consumer_services',
    'b2b_ind_energy_and_utilities',
    'b2b_ind_food_and_beverages',
    'b2b_ind_hospitality_and_hotels',
    'b2b_ind_information_technology',
    'b2b_ind_legal',
    'b2b_ind_media',
    'b2b_ind_mining',
    'b2b_ind_oil_and_gas',
    'b2b_ind_pharmaceutical',
    'b2b_ind_religious_services',
    'b2b_ind_sports_and_rec',
    'b2b_ind_transportation',
    'b2b_ind_travel',
    'b2b_ind_waste_services',
    'b2b_ind_wholesalers',
    'demo_high_net_worth',
    'int_inter_online_act',
    'cat_social_media',
    'demo_age_lifestages',
    'demo_age_18_19',
    'demo_age_20_29',
    'demo_age_20_24',
    'demo_age_25_29',
    'demo_age_30_39',
    'demo_age_30_34',
    'demo_age_35_39',
    'demo_age_40_49',
    'demo_age_40_44',
    'demo_age_45_49',
    'demo_age_50_59',
    'demo_age_50_54',
    'demo_age_55_59',
    'demo_age_60_64',
    'demo_age_60_Older',
    'demo_age_65_69',
    'demo_age_70_75',
    'demo_age_70_Older',
    'demo_age_baby_boomers',
    'demo_age_elders',
    'demo_age_gen_x',
    'demo_age_gen_y',
    'demo_education',
    'demo_education_student',
    'demo_education_high_school',
    'demo_education_college',
    'demo_education_undergrad',
    'demo_education_grad',
    'demo_education_vocation',
    'demo_ethnicity_hispanic',
    'demo_gender',
    'demo_gender_male',
    'demo_gender_female',
    'demo_lang_arabic',
    'demo_lang_chinese',
    'demo_lang_farsi',
    'demo_lang_french',
    'demo_lang_german',
    'demo_lang_hindi',
    'demo_lang_italian',
    'demo_lang_japanese',
    'demo_lang_korean',
    'demo_lang_portuguese',
    'demo_lang_russian',
    'demo_lang_spanish',
    'demo_lang_urdu',
    'demo_lang_group_indian',
    'adsquare - mobile aaid',
    'adsquare - mobile idfa',
    'appnexus - mobile aaid',
    'appnexus - cookie',
    'appnexus - mobile idfa',
    'bluekai - mobile aaid',
    'bluekai - cookie',
    'bluekai - mobile idfa',
    'bombora - cookie',
    'centro (sitescout) - mobile aaid',
    'centro (sitescout) - cookie',
    'centro (sitescout) - mobile idfa',
    'cross pixel - cookie',
    'demandbase - cookie',
    'exelate - cookie',
    'eyeota - cookie',
    'lotame - mobile aaid',
    'lotame - cookie',
    'lotame - mobile idfa',
    'mediamath - cookie',
    'quantcast - cookie',
    'tapfwd - mobile aaid',
    'tapfwd - mobile idfa',
    'tru optik - cookie',
    'the trade desk - cookie',
    'zeotap - mobile aaid',
    'zeotap - mobile idfa',
    'any_destination',
    ]

oheader = [
    'person_id',
    'version',
    'adsquare - mobile aaid',
    'adsquare - mobile idfa',
    'appnexus - mobile aaid',
    'appnexus - cookie',
    'appnexus - mobile idfa',
    'bluekai - mobile aaid',
    'bluekai - cookie',
    'bluekai - mobile idfa',
    'bombora - cookie',
    'centro (sitescout) - mobile aaid',
    'centro (sitescout) - cookie',
    'centro (sitescout) - mobile idfa',
    'cross pixel - cookie',
    'demandbase - cookie',
    'exelate - cookie',
    'eyeota - cookie',
    'lotame - mobile aaid',
    'lotame - cookie',
    'lotame - mobile idfa',
    'mediamath - cookie',
    'quantcast - cookie',
    'tapfwd - mobile aaid',
    'tapfwd - mobile idfa',
    'tru optik - cookie',
    'the trade desk - cookie',
    'zeotap - mobile aaid',
    'zeotap - mobile idfa',
    'any_destination',
    ]

isettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    fieldnames = iheader,
    )

osettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    fieldnames = oheader,
    extrasaction = 'ignore',
    )

fields_to_boolize = [v for v in oheader if v not in ['person_id', 'version',]]

def boolize_dict_strings(dict_):
    if dict_['person_id'].startswith('v'):
        dict_['person_id'] = dict_['person_id'].lstrip('v')
        dict_['version'] = 5
    else:
        dict_['version'] = 6
    return dict(person_id=dict_['person_id'], version=dict_['version'], **{v: (1 if dict_[v] == 'true' else 0) for v in fields_to_boolize})

with open(infile, 'rb') as ifile, open(outfile, 'wb') as ofile:
    icsv = csv.DictReader(ifile, **isettings)
    ocsv = csv.DictWriter(ofile, **osettings)
    _ = icsv.next()
    ocsv.writeheader()
    for line in icsv:
        ocsv.writerow(boolize_dict_strings(line))






# REMOVE KEYS FROM MYSQLDUMP
import os

files = [
    'locations_cities_v3.0.0.sql',
    'locations_countries_v3.0.1.sql',
    'locations_global_cities_v3.0.0.sql',
    'locations_global_states_v3.0.0.sql',
    'locations_states_v3.0.0.sql',
    'locations_zip_v3.0.0.sql',
    'phone_lookup_generated_v20181026.sql',
    'phone_lookup_line_type_20181026.sql',
    'phone_lookup_missing_20181026.sql',
    'phone_lookup_ocn_20181026.sql',
    'phone_lookup_rate_center_20181026.sql',
    'phone_lookup_telco_data_20181026.sql',
    'phone_lookup_telco_name_20181026.sql',
    'phone_lookup_tollfree_20181026.sql',
    'phone_lookup_voip_20181026.sql',
    ]

def remove_key_from_dump(input_file):
    dir_and_name, ext = os.path.splitext(input_file)
    output_file = dir_and_name + '.nokeys' + ext
    with open(input_file, 'rb') as ifile, open(output_file, 'wb') as ofile:
        for i, line in enumerate(ifile):
            if i < 200:
                if line.strip().startswith('KEY '):
                    new_line = '-- ' + line
                elif line.strip().startswith('UNIQUE KEY '):
                    new_line = '-- ' + line
                elif line.strip().startswith(') ENGINE=InnoDB'):
                    new_line = ') ENGINE=InnoDB COLLATE=utf8_unicode_ci;\n'
                elif line.strip().startswith('PRIMARY KEY ('):
                    new_line = line.strip().rstrip(',')
                else:
                    new_line = line
                if line != new_line:
                    # print new_line
                    line = new_line
            # else:
            #     break
            ofile.write(line)


for infile in files:
    remove_key_from_dump(infile)





from pyathenajdbc import connect

conn = connect(s3_staging_dir='s3://nwd-exports/consumer-data/Coreg_2018_09_18/',
               region_name='us-west-2')
try:
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT * FROM many_rows LIMIT 10
        """)
        for row in cursor:
            print(row)
finally:
    conn.close()




f = 'IT_LinkedInHandles_for_Netwise.txt'
o = 'IT_LinkedInHandles_for_Netwise.paths.txt'

counter_yes = 0
counter_no = 0
with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    ofile.write(ifile.next())
    for i, line in enumerate(ifile):
        if line.startswith('http://www.linkedin.com'):
            ofile.write(line.partition('http://www.linkedin.com')[2])
        else:
            print repr(line)



import csv
f = 'query_result.csv'
o = 'IT_LinkedInHandles_for_Netwise_Appended.csv'

isettings = dict(
    delimiter = ',',
    quoting = csv.QUOTE_NONNUMERIC,
    escapechar = '\\',
    )
osettings = dict(
    delimiter = ',',
    quoting = csv.QUOTE_ALL,
    escapechar = '\\',
    )
with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.DictReader(ifile, **isettings)
    ocsv = csv.DictWriter(ofile, fieldnames=icsv.fieldnames, **osettings)
    ocsv.writeheader()
    for line in icsv:
        line['Matched'] = int(line['Matched'])
        ocsv.writerow(line)





import csv
import hashlib

f = 'ALC_Aggregated.psv'
o = 'ALC_Aggregated_SHA256.psv'

settings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    escapechar = '\\',
    doublequote = False,
    )

hashkeys = ['email_{}'.format(i+1) for i in range(8)]

def hash_line(line):
    for key in hashkeys:
        if line[key]:
            line[key] = hashlib.sha256(line[key]).hexdigest()
    return line

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.DictReader(ifile, **settings)
    ocsv = csv.DictWriter(ofile, icsv.fieldnames, **settings)
    for line in icsv:
        try:
            ocsv.writerow(hash_line(line))
        except:
            print line
            raise






import gzip
with gzip.open('2018-09-12T08:23:12.jl.gz', 'rb') as ifile:
    try:
        for i, line in enumerate(ifile):
            pass
    except:
        pass
    print i




import gzip
counter = 0
with gzip.open('2018-09-13T11:10:47.jl.gz', 'rb') as ifile, open('test.jl', 'wb') as ofile:
    while True:
        try:
            ofile.write(ifile.next())
            counter += 1
        except IOError:
            print 'io error at {}'.format(counter)
            print ifile.tell()
            raise
        except StopIteration:
            print 'actually completed to {}'.format(counter)
            break
        except:
            raise




with open('2018-09-13T11:10:47.jl.gz', 'rb') as ifile:
    for line in ifile:
        pass
    print ifile.tell()



# --------

import hashlib
import csv
f = 'coreg_emails_2016_06_01.psv_Matchback'
o = '2016-06-01.psv'

date = '2016-06-01'
has_header = True

isettings = dict(
    delimiter = ',',
    quoting = csv.QUOTE_MINIMAL,
    # fieldnames = ['email', 'onboarded']
    )

osettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # fieldnames = ['md5', 'onboarded_date', ]
    )

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.reader(ifile, **isettings)
    ocsv = csv.writer(ofile, **osettings)
    if has_header:
        _ = icsv.next()
    ocsv.writerow(['md5', 'onboarded_date'])
    for line in icsv:
        if line[1] == 'true':
            ocsv.writerow([hashlib.md5(line[0].strip().lower()).hexdigest(), date,])



# --------

import hashlib
import csv
f = 'nwd_email_md5_file_2018_01_18.csv_Matchback'
o = '2018-01-18.psv'

date = '2018-01-18'
has_header = True

isettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    # fieldnames = ['md5', 'file_wide_field', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'onboarded']
    )

osettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # fieldnames = ['md5', 'onboarded_date', ]
    )

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.reader(ifile, **isettings)
    ocsv = csv.writer(ofile, **osettings)
    if has_header:
        _ = icsv.next()
    ocsv.writerow(['md5', 'onboarded_date'])
    for line in icsv:
        if line[11] == 'true':
            ocsv.writerow([line[0], date,])



# --------

import hashlib
import csv
f = 'nwd_email_md5_file_2017_06_22.csv_Matchback'
o = '2017-06-22.psv'

date = '2017-06-22'
has_header = True

isettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    # fieldnames = ['md5', 'file_wide_field', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'onboarded']
    )

osettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # fieldnames = ['md5', 'onboarded_date', ]
    )

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.reader(ifile, **isettings)
    ocsv = csv.writer(ofile, **osettings)
    if has_header:
        _ = icsv.next()
        _ = icsv.next()
    ocsv.writerow(['md5', 'onboarded_date'])
    for line in icsv:
        if line[9] == 'true':
            ocsv.writerow([line[0], date,])





# --------

import hashlib
import csv
f = 'md5s_deduped_2016_12_08.csv_Matchback'
o = '2016-12-08.psv'

date = '2016-12-08'
has_header = True

isettings = dict(
    delimiter = ',',
    quoting = csv.QUOTE_NONE,
    # fieldnames = ['md5', 'file_wide_field', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'onboarded']
    )

osettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # fieldnames = ['md5', 'onboarded_date', ]
    )

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.reader(ifile, **isettings)
    ocsv = csv.writer(ofile, **osettings)
    if has_header:
        _ = icsv.next()
    ocsv.writerow(['md5', 'onboarded_date'])
    for line in icsv:
        if line[8] == 'true':
            ocsv.writerow([line[0], date,])



# --------

import hashlib
import csv
f = 'nwd_email_md5_file_2017_07_25.csv_Matchback'
o = '2017-07-25.psv'

date = '2017-07-25'
has_header = True

isettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    # fieldnames = ['md5', 'file_wide_field', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'onboarded']
    )

osettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # fieldnames = ['md5', 'onboarded_date', ]
    )

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.reader(ifile, **isettings)
    ocsv = csv.writer(ofile, **osettings)
    if has_header:
        _ = icsv.next()
        _ = icsv.next()
    ocsv.writerow(['md5', 'onboarded_date'])
    for line in icsv:
        if line[2] == 'true':
            ocsv.writerow([line[0], date,])



# --------

import hashlib
import csv
f = 'nwd_md5s_2017_02_07.csv_Matchback'
o = '2017-02-07.psv'

date = '2017-02-07'
has_header = True

isettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    # fieldnames = ['md5', 'file_wide_field', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'onboarded']
    )

osettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # fieldnames = ['md5', 'onboarded_date', ]
    )

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.reader(ifile, **isettings)
    ocsv = csv.writer(ofile, **osettings)
    if has_header:
        _ = icsv.next()
    ocsv.writerow(['md5', 'onboarded_date'])
    for line in icsv:
        if line[9] == 'true':
            ocsv.writerow([line[0], date,])



# --------

import hashlib
import csv
f = 'liveramp_email_specific_matchback_032816.psv'
o = '2016-03-28.psv'

date = '2016-03-28'
has_header = True

isettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    # fieldnames = ['md5', 'file_wide_field', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'onboarded']
    )

osettings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_ALL,
    # fieldnames = ['md5', 'onboarded_date', ]
    )

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.reader(ifile, **isettings)
    ocsv = csv.writer(ofile, **osettings)
    if has_header:
        _ = icsv.next()
    ocsv.writerow(['md5', 'onboarded_date'])
    for line in icsv:
        if line[180] == 'true':
            ocsv.writerow([line[8], date,])






import MySQLdb
import os

with MySQLdb.connect(host = os.getenv('SQL_CONTACTSV2PROD_HOST'), user = os.getenv('SQL_CONTACTSV2PROD_USER'), passwd = os.getenv('SQL_CONTACTSV2PROD_PASSWORD'), db = os.getenv('SQL_CONTACTSV2PROD_DATABASE_DEFAULT'),) as cursor:
    cursor.execute('SELECT * FROM `production_persons_v7.0.0` LIMIT 1')
    r = cursor.fetchone()

for v in r:
    print type((v))






import csv
import json
f = 'Risk_2018_10_17_sample.psv'
o = 'Risk_2018_10_17.jl'
isettings = dict(
    delimiter = '|'
    )

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    icsv = csv.DictReader(ifile, **isettings)
    for i, line in enumerate(icsv):
        if i % 100 == 0:
            before = 'g'
        else:
            before = ''
        ofile.write(before + json.dumps(line) + '\n')




import json
import unicodecsv as csv

f = 'us_all_sample'
o = 'us_all_sample.csv'

fieldnames = [
    'linkedin_profile_url',
    'raw_full_name',
    'raw_headline',
    'raw_locality',
    'n_short2_title',
    'n_short2_company_name',
    'n_short2_personal_city',
    'n_short2_personal_state',
    'company_from_header',
    'companies',
    'experience_name',
    'experience_title',
    'skills',
    'last_crawled',
    'last_html_update',
    'last_updated',
    ]

with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    ocsv = csv.DictWriter(ofile, fieldnames=fieldnames)
    ocsv.writeheader()
    for line in ifile:
        jline = json.loads(line.decode('utf-8'))
        d = {f: jline.get(f, None) for f in fieldnames}
        d['skills'] = ','.join(d['skills'])
        d['companies'] = ','.join(d['companies'])
        if 'experience' in jline:
            if jline['experience']:
                d['experience_title'] = ','.join([j.get('title', None) for j in jline['experience'] if j.get('title', None)])
                d['experience_name'] = ','.join([j.get('name', None) for j in jline['experience'] if j.get('name', None)])
        ocsv.writerow(d)



fieldnames = [
    'person_id',
    'full_b2b_contact',
    'first_name',
    'last_name',
    'title',
    'is_digital_only',
    'b2b_seniority',
    'b2b_job_function',
    'b2b_job_sub_function',
    'production_company_id',
    'staging_company_id',
    'company_phone_waterfall',
    'company_address1_waterfall',
    'company_address2_waterfall',
    'company_city_waterfall',
    'company_state_waterfall',
    'company_state_short_waterfall',
    'company_zip_waterfall',
    'company_zip4_waterfall',
    'company_country_waterfall',
    'company_phone_local',
    'company_address1_local',
    'company_address2_local',
    'company_city_local',
    'company_state_local',
    'company_state_short_local',
    'company_zip_local',
    'company_zip4_local',
    'company_country_local',
    'company_phone_headquarters',
    'company_address1_headquarters',
    'company_address2_headquarters',
    'company_city_headquarters',
    'company_state_headquarters',
    'company_state_short_headquarters',
    'company_zip_headquarters',
    'company_zip4_headquarters',
    'company_country_headquarters',
    'has_raw_personal_email',
    'has_personal_email',
    'personal_phone',
    'personal_address1',
    'personal_address2',
    'personal_city',
    'personal_state',
    'personal_state_short',
    'personal_zip',
    'personal_zip4',
    'personal_country',
    'full_personal_connection',
    'current_position_start_year',
    'declared_city',
    'declared_company_city',
    'declared_company_name',
    'declared_company_state',
    'declared_country',
    'declared_state',
    'education_1_graduation_year',
    'education_1_institution',
    'education_1_level',
    'education_1_start_year',
    'education_2_graduation_year',
    'education_2_institution',
    'education_2_level',
    'education_2_start_year',
    'education_earliest_graduation_year',
    'education_earliest_level',
    'language1',
    'language2',
    'language3',
    'last_processed',
    'last_updated',
    'linkedin_company_id',
    'linkedin_company_url_path',
    'previous_position_1_company_name',
    'previous_position_1_industry',
    'previous_position_1_start_year',
    'previous_position_1_title',
    'previous_position_2_company_name',
    'previous_position_2_industry',
    'previous_position_2_start_year',
    'previous_position_2_title',
    'provider_id',
    'provider_record_id',
    'skill1',
    'skill2',
    'skill3',
    'skill4',
    'skill5',
    'source_id',
    'source_record_id',
    'source_url',
    'linkedin_profile_url',
    'linkedin_profile_url_path',
    'company_source_id',
    'company_provider_id',
    'number_of_connections',
    'company_num_processed_matches',
    'company_name',
    'company_domain',
    'company_website',
    'company_employees',
    'company_employees_bucket',
    'company_revenue',
    'company_revenue_bucket',
    'company_primary_industry',
    'company_primary_sic_code',
    'company_primary_naics_code',
    'b2b_email',
    'b2b_email_md5',
    'b2b_email_status',
    'b2b_email_additional_status_info',
    'b2b_domain_email_status',
    'b2b_email_status_waterfall',
    'b2b_email_vendor_status',
    'b2b_email_query_type',
    'b2b_email_domain_office_365',
    'p2b_email_1',
    'p2b_email_2',
    'p2b_email_3',
    'p2b_email_4',
    'p2b_email_1_md5',
    'p2b_email_2_md5',
    'p2b_email_3_md5',
    'p2b_email_4_md5',
    'b2b_company_fortune_1000',
    'b2b_company_fortune_500',
    'b2b_company_revenue_large',
    'b2b_company_revenue_med',
    'b2b_company_revenue_med_large',
    'b2b_company_revenue_micro',
    'b2b_company_revenue_small',
    'b2b_company_revenue_xlarge',
    'b2b_company_size',
    'b2b_company_size_large',
    'b2b_company_size_med',
    'b2b_company_size_med_large',
    'b2b_company_size_micro',
    'b2b_company_size_small',
    'b2b_company_size_xlarge',
    'b2b_engineering_decision_makers',
    'b2b_engineering_executives',
    'b2b_engineering_managers',
    'b2b_fa_admin',
    'b2b_fa_admin_administrator',
    'b2b_fa_admin_assistant',
    'b2b_fa_admin_office_manager',
    'b2b_fa_admin_secretary',
    'b2b_fa_admin_typist',
    'b2b_fa_consultants',
    'b2b_fa_education',
    'b2b_fa_education_admissions',
    'b2b_fa_education_librarian',
    'b2b_fa_education_professor',
    'b2b_fa_education_teacher',
    'b2b_fa_engineering',
    'b2b_fa_engineering_biomedical',
    'b2b_fa_engineering_chemical',
    'b2b_fa_engineering_civil',
    'b2b_fa_engineering_electrical',
    'b2b_fa_engineering_electronics',
    'b2b_fa_engineering_industrial',
    'b2b_fa_engineering_mechanical',
    'b2b_fa_finance',
    'b2b_fa_finance_accounting',
    'b2b_fa_finance_analyst',
    'b2b_fa_finance_audit',
    'b2b_fa_finance_banking',
    'b2b_fa_finance_cfp',
    'b2b_fa_finance_compliance_specialist',
    'b2b_fa_finance_corporate',
    'b2b_fa_finance_cpa',
    'b2b_fa_finance_economist',
    'b2b_fa_finance_investment_banking',
    'b2b_fa_finance_mortgage_specialist',
    'b2b_fa_finance_risk_management',
    'b2b_fa_finance_statistician',
    'b2b_fa_finance_tax_specialist',
    'b2b_fa_finance_treasurer',
    'b2b_fa_finance_wealth_management',
    'b2b_fa_general_management',
    'b2b_fa_general_management_board_member',
    'b2b_fa_general_management_ceo',
    'b2b_fa_general_management_cfo',
    'b2b_fa_general_management_cio',
    'b2b_fa_general_management_cmo',
    'b2b_fa_general_management_coo',
    'b2b_fa_general_management_cto',
    'b2b_fa_general_management_director',
    'b2b_fa_general_management_executive_vp',
    'b2b_fa_general_management_founder',
    'b2b_fa_general_management_manager_or_supervisor',
    'b2b_fa_general_management_owner',
    'b2b_fa_general_management_partner',
    'b2b_fa_general_management_president',
    'b2b_fa_general_management_senior_board_member',
    'b2b_fa_general_management_vp',
    'b2b_fa_government',
    'b2b_fa_government_elected_officials',
    'b2b_fa_healthcare',
    'b2b_fa_healthcare_chiropractor',
    'b2b_fa_healthcare_dental_assistant',
    'b2b_fa_healthcare_dental_hygenist',
    'b2b_fa_healthcare_dentist',
    'b2b_fa_healthcare_dietician',
    'b2b_fa_healthcare_healthcare_professional',
    'b2b_fa_healthcare_nurse',
    'b2b_fa_healthcare_opticians',
    'b2b_fa_healthcare_optometrist',
    'b2b_fa_healthcare_paramedic',
    'b2b_fa_healthcare_pharmaceuticals',
    'b2b_fa_healthcare_pharmacist',
    'b2b_fa_healthcare_physical_therapist',
    'b2b_fa_healthcare_physician_or_doctor',
    'b2b_fa_healthcare_physicians_assistant',
    'b2b_fa_healthcare_psychologist',
    'b2b_fa_healthcare_therapist',
    'b2b_fa_human_resources',
    'b2b_fa_human_resources_benefits_and_compensation',
    'b2b_fa_human_resources_employee_development',
    'b2b_fa_human_resources_payroll_specialist',
    'b2b_fa_human_resources_recruiting',
    'b2b_fa_law_enforcement',
    'b2b_fa_law_enforcement_correctional_workers',
    'b2b_fa_law_enforcement_parole_officer',
    'b2b_fa_law_enforcement_police',
    'b2b_fa_legal',
    'b2b_fa_legal_judge',
    'b2b_fa_legal_lawyer',
    'b2b_fa_legal_paralegal',
    'b2b_fa_legal_prosecutor',
    'b2b_fa_marketing',
    'b2b_fa_marketing_advertising',
    'b2b_fa_marketing_creative',
    'b2b_fa_marketing_events',
    'b2b_fa_marketing_marketing_communications',
    'b2b_fa_military',
    'b2b_fa_military_air_force',
    'b2b_fa_military_army',
    'b2b_fa_military_coast_guard',
    'b2b_fa_military_marines',
    'b2b_fa_military_navy',
    'b2b_fa_operations',
    'b2b_fa_operations_environmental',
    'b2b_fa_operations_facility_maintenance',
    'b2b_fa_operations_food_and_beverage',
    'b2b_fa_operations_manufacturing',
    'b2b_fa_operations_quality_assurance',
    'b2b_fa_operations_records_management',
    'b2b_fa_operations_safety',
    'b2b_fa_operations_training',
    'b2b_fa_photographer',
    'b2b_fa_real_estate',
    'b2b_fa_real_estate_architect',
    'b2b_fa_real_estate_builder',
    'b2b_fa_real_estate_contractor',
    'b2b_fa_real_estate_developer',
    'b2b_fa_real_estate_mortgage_specialist',
    'b2b_fa_real_estate_real_estate_agent',
    'b2b_fa_religious_leader',
    'b2b_fa_research_and_development',
    'b2b_fa_research_and_development_product_development',
    'b2b_fa_research_and_development_project_management',
    'b2b_fa_research_and_development_research',
    'b2b_fa_retired',
    'b2b_fa_sales',
    'b2b_fa_sales_account_management',
    'b2b_fa_sales_business_development',
    'b2b_fa_sales_community_development',
    'b2b_fa_sales_customer_service',
    'b2b_fa_science',
    'b2b_fa_science_chemist',
    'b2b_fa_science_geologist',
    'b2b_fa_science_scientists',
    'b2b_fa_stay_at_home_parent',
    'b2b_fa_student',
    'b2b_fa_supply_chain',
    'b2b_fa_supply_chain_logistics',
    'b2b_fa_supply_chain_procurement',
    'b2b_fa_supply_chain_sourcing',
    'b2b_fa_technology',
    'b2b_fa_technology_computing',
    'b2b_fa_technology_data_management',
    'b2b_fa_technology_dba',
    'b2b_fa_technology_information_security',
    'b2b_fa_technology_information_technology',
    'b2b_fa_technology_mis',
    'b2b_fa_technology_network_administration',
    'b2b_fa_technology_quality_assurance',
    'b2b_fa_technology_software',
    'b2b_fa_technology_systems_admin',
    'b2b_fa_technology_technical_support',
    'b2b_fa_technology_telecom',
    'b2b_fa_technology_web_development',
    'b2b_fa_writer',
    'b2b_finance_decision_makers',
    'b2b_finance_executives',
    'b2b_finance_managers',
    'b2b_healthcare_decision_makers',
    'b2b_healthcare_executives',
    'b2b_healthcare_managers',
    'b2b_hr_decision_makers',
    'b2b_hr_executives',
    'b2b_hr_managers',
    'b2b_ind_agriculture',
    'b2b_ind_apparel',
    'b2b_ind_automotive',
    'b2b_ind_business_services',
    'b2b_ind_business_services_adv_marketing',
    'b2b_ind_construction',
    'b2b_ind_consumer_services',
    'b2b_ind_education',
    'b2b_ind_education_colleges_univ',
    'b2b_ind_energy_and_utilities',
    'b2b_ind_finance',
    'b2b_ind_finance_investment_banking',
    'b2b_ind_food_and_beverages',
    'b2b_ind_government',
    'b2b_ind_healthcare',
    'b2b_ind_healthcare_hosp_clinics',
    'b2b_ind_healthcare_insurance',
    'b2b_ind_hospitality_and_hotels',
    'b2b_ind_information_technology',
    'b2b_ind_insurance',
    'b2b_ind_legal',
    'b2b_ind_manufacturing',
    'b2b_ind_media',
    'b2b_ind_media_internet',
    'b2b_ind_mining',
    'b2b_ind_non_profit',
    'b2b_ind_oil_and_gas',
    'b2b_ind_pharmaceutical',
    'b2b_ind_real_estate',
    'b2b_ind_real_estate_residential',
    'b2b_ind_religious_services',
    'b2b_ind_retail',
    'b2b_ind_software',
    'b2b_ind_software_security',
    'b2b_ind_sports_and_rec',
    'b2b_ind_telecom',
    'b2b_ind_transportation',
    'b2b_ind_travel',
    'b2b_ind_waste_services',
    'b2b_ind_wholesalers',
    'b2b_leadership_business_decision_maker',
    'b2b_leadership_female_owner',
    'b2b_leadership_minority_owner',
    'b2b_leadership_senior_board_members',
    'b2b_leadership_senior_executives',
    'b2b_leadership_senior_management',
    'b2b_leadership_small_biz_owner',
    'b2b_leadership_vp_plus',
    'b2b_marketing_decision_makers',
    'b2b_marketing_executives',
    'b2b_marketing_managers',
    'b2b_operations_decision_makers',
    'b2b_operations_executives',
    'b2b_operations_managers',
    'b2b_r_and_d_decision_makers',
    'b2b_r_and_d_executives',
    'b2b_r_and_d_managers',
    'b2b_sales_decision_makers',
    'b2b_sales_executives',
    'b2b_sales_managers',
    'b2b_sales_volume',
    'b2b_seniority_advisors',
    'b2b_seniority_board_of_directors',
    'b2b_seniority_c_level',
    'b2b_seniority_director',
    'b2b_seniority_founder',
    'b2b_seniority_manager',
    'b2b_seniority_non_management',
    'b2b_seniority_owner_or_partner',
    'b2b_seniority_retired',
    'b2b_seniority_staff',
    'b2b_seniority_student',
    'b2b_seniority_vice_president',
    'b2b_supply_chain_decision_makers',
    'b2b_supply_chain_executives',
    'b2b_supply_chain_managers',
    'b2b_technology_decision_makers',
    'b2b_technology_executives',
    'b2b_technology_managers',
    'cat_social_media',
    'demo_age_18_19',
    'demo_age_20_24',
    'demo_age_20_29',
    'demo_age_25_29',
    'demo_age_30_34',
    'demo_age_30_39',
    'demo_age_35_39',
    'demo_age_40_44',
    'demo_age_40_49',
    'demo_age_45_49',
    'demo_age_50_54',
    'demo_age_50_59',
    'demo_age_55_59',
    'demo_age_60_64',
    'demo_age_60_Older',
    'demo_age_65_69',
    'demo_age_70_75',
    'demo_age_70_Older',
    'demo_age_baby_boomers',
    'demo_age_elders',
    'demo_age_gen_x',
    'demo_age_gen_y',
    'demo_age_lifestages',
    'demo_education',
    'demo_education_college',
    'demo_education_grad',
    'demo_education_high_school',
    'demo_education_student',
    'demo_education_undergrad',
    'demo_education_vocation',
    'demo_emp_status_employed',
    'demo_ethnicity_hispanic',
    'demo_gender',
    'demo_gender_female',
    'demo_gender_male',
    'demo_high_net_worth',
    'demo_lang_arabic',
    'demo_lang_chinese',
    'demo_lang_farsi',
    'demo_lang_french',
    'demo_lang_german',
    'demo_lang_group_indian',
    'demo_lang_hindi',
    'demo_lang_italian',
    'demo_lang_japanese',
    'demo_lang_korean',
    'demo_lang_portuguese',
    'demo_lang_russian',
    'demo_lang_spanish',
    'demo_lang_urdu',
    'int_inter_online_act',
    'company_normalized_industry',
    'b2b_diversity_business_type',
    'b2b_us_federal_gov_approved_supplier',
    'mfg_business_services',
    'mfg_business_services_custom_services',
    'mfg_business_type_contract_manufacturer',
    'mfg_business_type_distributor',
    'mfg_business_type_manufacturer',
    'mfg_business_type_supplier',
    'mfg_certifications_as_9100',
    'mfg_certifications_fda',
    'mfg_certifications_iso',
    'mfg_certifications_iso9001',
    'mfg_certifications_itar',
    'mfg_certifications_lean_manufacturer',
    'mfg_certifications_leed',
    'mfg_consumer_products',
    'mfg_consumer_products_animal_and_pet_products',
    'mfg_consumer_products_apparel_and_textiles',
    'mfg_consumer_products_appliances',
    'mfg_consumer_products_child_and_baby_care_products',
    'mfg_consumer_products_computers_av_and_peripherals',
    'mfg_consumer_products_consumer_medical_products',
    'mfg_consumer_products_consumer_vehicles_and_components',
    'mfg_consumer_products_cosmetics',
    'mfg_consumer_products_food_and_food_products',
    'mfg_consumer_products_household_products',
    'mfg_consumer_products_jewelry',
    'mfg_consumer_products_marine_products',
    'mfg_consumer_products_photography',
    'mfg_consumer_products_recreation_and_sports_equipment',
    'mfg_defense_and_law_enforcement_aerospace_aircraft',
    'mfg_defense_and_law_enforcement_equipment_and_supplies',
    'mfg_defense_and_law_enforcement_guns_ammunition',
    'mfg_defense_and_law_enforcement_manufacturing',
    'mfg_defense_and_law_enforcement_military_equipment',
    'mfg_general_manufacturing',
    'mfg_general_manufacturing_automation_systems_and_components',
    'mfg_general_manufacturing_electrical_and_electronic_components',
    'mfg_general_manufacturing_facility_equipment_and_supplies',
    'mfg_general_manufacturing_fluid_control_and_components',
    'mfg_general_manufacturing_hardware_and_fasteners',
    'mfg_general_manufacturing_machinery_tools_and_supplies',
    'mfg_general_manufacturing_manufacturing_equipment',
    'mfg_general_manufacturing_packaging_and_materials_handling',
    'mfg_general_manufacturing_test_measurement_and_positioning',
    'mfg_industries_aerospace',
    'mfg_industries_automotive',
    'mfg_industries_biomedical',
    'mfg_industries_construction',
    'mfg_industries_defense',
    'mfg_industries_electronics',
    'mfg_industries_environmental',
    'mfg_industries_healthcare',
    'mfg_industries_pharmaceutical',
    'mfg_industries_renewable_energy',
    'mfg_industries_transportation',
    'mfg_machinery',
    'mfg_machinery_cnc',
    'mfg_machinery_presses',
    'mfg_machinery_printing',
    'mfg_materials_and_chemicals',
    'mfg_materials_and_chemicals_adhesives_tapes_and_sealants',
    'mfg_materials_and_chemicals_chemicals_and_chemical_products',
    'mfg_materials_and_chemicals_metals_and_metal_products',
    'mfg_materials_and_chemicals_polymers_and_polymer_products',
    'tech_inst_cloud_services',
    'tech_inst_cloud_services_cloud_infrastructure_computing',
    'tech_inst_cloud_services_platform_as_a_service',
    'tech_inst_communications_tech_email',
    'tech_inst_communications_technologies',
    'tech_inst_customer_mgmt',
    'tech_inst_customer_mgmt_help_desk',
    'tech_inst_data_center',
    'tech_inst_data_center_operating_systems_and_computing_languages',
    'tech_inst_data_center_system_analytics_and_monitoring',
    'tech_inst_enterprise_apps',
    'tech_inst_enterprise_apps_commerce',
    'tech_inst_enterprise_apps_hr_mgmt_systems',
    'tech_inst_enterprise_content',
    'tech_inst_enterprise_content_document_mgmt',
    'tech_inst_hardware',
    'tech_inst_hardware_consumer_electronics_computers_and_software',
    'tech_inst_it_governance',
    'tech_inst_it_governance_application_development_and_mgmt',
    'tech_inst_it_governance_change_mgmt',
    'tech_inst_it_governance_software_configuration_mgmt',
    'tech_inst_marketing_performance_mgmt',
    'tech_inst_marketing_performance_mgmt_business_intelligence',
    'tech_inst_marketing_performance_mgmt_measurement',
    'tech_inst_network_computing',
    'tech_inst_network_computing_network_mgmt_hardware',
    'tech_inst_network_computing_network_mgmt_software',
    'tech_inst_productivity_solutions',
    'tech_inst_productivity_solutions_collaboration',
    'tech_inst_software',
    'tech_inst_software_search_engine',
    'tech_inst_software_server_technologies',
    'tech_inst_vertical_markets',
    'tech_inst_vertical_markets_academic_and_education_mgmt_software',
    'tech_inst_web_oriented_arch_online_video_platform',
    'tech_inst_web_oriented_arch_remote_server_solutions',
    'tech_inst_web_oriented_arch_social_media_systems',
    'tech_inst_web_oriented_arch_virtualization_data_center',
    'tech_inst_web_oriented_arch_virtualization_platform_mgmt',
    'tech_inst_web_oriented_arch_web_and_portal_technology',
    'tech_inst_web_oriented_arch_web_content_mgmt_system',
    'tech_inst_web_oriented_architecture',
    'random_tenth_percent',
]

import csv
import gzip
import os

writers = dict()

f = 'NWD_Athena_2019_01_10.psv.gz'

settings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    escapechar = '\\',
    doublequote = False,
    )

personal_country = fieldnames.index('personal_country')

basedir = '/home/ec2-user/temp/athena/output/'

with gzip.open(f, 'rb') as ifile:
    icsv = csv.reader(ifile, **settings)
    for i, line in enumerate(icsv):
        if line[personal_country] in writers:
            writers[line[personal_country]].writerow(line)
        else:
            new_dir = os.path.join(basedir, 'personal_country={}'.format(line[personal_country]))
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)
            path = os.path.join(new_dir, 'NWD_Athena_2019_01_10.psv')
            writers[line[personal_country]] = csv.writer(open(path, 'wb'), **settings)
            writers[line[personal_country]].writerow(line)




import psycopg2
s3_loc = 's3://nwd-exports/redshift/production_persons_2019_01_25/production_persons_2019_01_25.psv'
arn = 'arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
con=psycopg2.connect(dbname= 'dev', host='b2b-data-redshift-cluster.ce0dlxdyinbt.us-west-2.redshift.amazonaws.com', port= 5439, user= 'root', password= 'masterAWS123!')
cur = con.cursor()

drop_persons = 'DROP TABLE IF EXISTS "production_persons_v8.0.0"'
create_persons = '''
CREATE TABLE IF NOT EXISTS "production_persons_v8.0.0" (
  "id" bigint DISTKEY,
  "persistent_id" char(32),
  "persistent_id_type" varchar(1020) DEFAULT '',
  "provider_id" integer,
  "provider_record_id" varchar(40) DEFAULT '',
  "source_id" integer,
  "source_record_id" varchar(40) DEFAULT '',
  "source_url" varchar(1020) DEFAULT '',
  "linkedin_profile_url" varchar(1020) DEFAULT '',
  "linkedin_profile_url_path" varchar(1020) DEFAULT '',
  "image_url" varchar(1020) DEFAULT '',
  "is_sparse_record" smallint DEFAULT '0',
  "is_digital_only" smallint DEFAULT '0',
  "is_duplicate" smallint DEFAULT '0',
  "duplicate_parent_id" bigint,
  "first_name" varchar(400) DEFAULT '',
  "middle_name" varchar(400),
  "middle_initial" char(1) DEFAULT '',
  "last_name" varchar(400) DEFAULT '',
  "title" varchar(400),
  "declared_city" varchar(400) DEFAULT '',
  "declared_state" varchar(400) DEFAULT '',
  "declared_country" varchar(400) DEFAULT '',
  "declared_company_name" varchar(1020),
  "declared_company_domain" varchar(1020),
  "declared_company_city" varchar(400),
  "declared_company_state" varchar(400),
  "linkedin_company_id" numeric(20, 0),
  "linkedin_company_url_path" varchar(1020) DEFAULT '',
  "matched_processed_company_id" integer SORTKEY,
  "matched_staging_company_id" integer,
  "company_match_type" varchar(1020) DEFAULT '',
  "personal_provider_id" integer,
  "personal_source_id" integer,
  "personal_phone" varchar(100) DEFAULT '',
  "personal_phone_is_wireless" smallint,
  "personal_address1" varchar(1020) DEFAULT '',
  "personal_address2" varchar(1020) DEFAULT '',
  "personal_city" varchar(1020) DEFAULT '',
  "personal_state" varchar(1020) DEFAULT '',
  "personal_state_short" char(2) DEFAULT '',
  "personal_zip" varchar(64) DEFAULT '',
  "personal_zip4" char(16) DEFAULT '',
  "personal_country" varchar(1020) DEFAULT '',
  "personal_lat" decimal(10,8),
  "personal_lon" decimal(11,8),
  "company_phone_local" varchar(100) DEFAULT '',
  "company_address1_local" varchar(1020) DEFAULT '',
  "company_address2_local" varchar(1020) DEFAULT '',
  "company_city_local" varchar(1020) DEFAULT '',
  "company_state_local" varchar(1020) DEFAULT '',
  "company_state_short_local" char(2) DEFAULT '',
  "company_zip_local" varchar(64) DEFAULT '',
  "company_zip4_local" char(16) DEFAULT '',
  "company_country_local" varchar(1020) DEFAULT '',
  "company_lat_local" decimal(10,8),
  "company_lon_local" decimal(11,8),
  "company_phone_waterfall" varchar(100),
  "company_address1_waterfall" varchar(1020),
  "company_address2_waterfall" varchar(1020),
  "company_city_waterfall" varchar(1020),
  "company_state_waterfall" varchar(1020),
  "company_state_short_waterfall" char(2),
  "company_zip_waterfall" varchar(64),
  "company_zip4_waterfall" char(16),
  "company_country_waterfall" varchar(1020),
  "company_lat_waterfall" decimal(10,8),
  "company_lon_waterfall" decimal(11,8),
  "current_position_start_year" smallint,
  "previous_position_1_company_name" varchar(400) DEFAULT '',
  "previous_position_1_title" varchar(400) DEFAULT '',
  "previous_position_1_industry" varchar(400) DEFAULT '',
  "previous_position_1_start_year" smallint,
  "previous_position_1_end_year" smallint,
  "previous_position_2_company_name" varchar(400) DEFAULT '',
  "previous_position_2_title" varchar(400) DEFAULT '',
  "previous_position_2_industry" varchar(400) DEFAULT '',
  "previous_position_2_start_year" smallint,
  "previous_position_2_end_year" smallint,
  "education_1_level" varchar(400) DEFAULT '',
  "education_1_institution" varchar(400) DEFAULT '',
  "education_1_graduation_year" smallint,
  "education_1_start_year" smallint,
  "education_2_level" varchar(400) DEFAULT '',
  "education_2_institution" varchar(400) DEFAULT '',
  "education_2_graduation_year" smallint,
  "education_2_start_year" smallint,
  "education_earliest_graduation_year" smallint,
  "education_earliest_level" varchar(400) DEFAULT '',
  "skill1" varchar(400) DEFAULT '',
  "skill2" varchar(400) DEFAULT '',
  "skill3" varchar(400) DEFAULT '',
  "skill4" varchar(400) DEFAULT '',
  "skill5" varchar(400) DEFAULT '',
  "number_of_connections" integer,
  "language1" varchar(400) DEFAULT '',
  "language2" varchar(400) DEFAULT '',
  "language3" varchar(400) DEFAULT '',
  "functional_area" text,
  "last_updated" datetime,
  "last_processed" datetime,
  "random_sample" smallint DEFAULT '0',
  "random_tenth_percent" smallint
) 
'''
cur.execute(drop_persons)
con.commit()

cur.execute(create_persons)
con.commit()

cur.execute('COPY "production_persons_v8.0.0" FROM \'{}\' CREDENTIALS \'aws_iam_role={}\' DELIMITER \'|\' ESCAPE IGNOREHEADER 1 NULL AS \'\\\\\\\\\\\\\\N\' ACCEPTINVCHARS AS \'?\''.format(s3_loc, arn))

cur.execute('select count(*) from "production_persons_v8.0.0"')
cur.fetchall()





import psycopg2
s3_loc = 's3://nwd-exports/redshift/production_companies_2019_01_25/production_companies_2019_01_25.psv'
arn = 'arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
con=psycopg2.connect(dbname= 'dev', host='b2b-data-redshift-cluster.ce0dlxdyinbt.us-west-2.redshift.amazonaws.com', port= 5439, user= 'root', password= 'masterAWS123!')
cur = con.cursor()

drop_companies = 'DROP TABLE IF EXISTS "production_companies_v8.0.0"'
create_companies = '''
CREATE TABLE IF NOT EXISTS "production_companies_v8.0.0" (
  "id" integer sortkey,
  "persistent_id" char(32) ,
  "persistent_id_type" varchar(1020)  DEFAULT '',
  "num_processed_matches" integer,
  "provider_id" smallint,
  "provider_record_id" varchar(40)  DEFAULT NULL,
  "source_id" smallint DEFAULT NULL,
  "source_record_id" varchar(40)  DEFAULT NULL,
  "name" varchar(1020)  DEFAULT '',
  "name_normalized" varchar(1020) ,
  "name_usa" varchar(1020) ,
  "name_global" varchar(1020) ,
  "description" varchar(max) ,
  "domain" varchar(1020)  DEFAULT '' distkey,
  "website" varchar(1020)  DEFAULT '',
  "profile_url_linkedin" varchar(max) ,
  "profile_url_google" varchar(max) ,
  "profile_url_yelp" varchar(max) ,
  "profile_url_facebook" varchar(max) ,
  "profile_url_zoominfo" varchar(max) ,
  "profile_url_twitter" varchar(max) ,
  "profile_url_instagram" varchar(max) ,
  "profile_url_youtube" varchar(max) ,
  "phone" varchar(100)  DEFAULT '',
  "address1" varchar(1020)  DEFAULT '',
  "address2" varchar(1020)  DEFAULT '',
  "city" varchar(1020)  DEFAULT '',
  "state" varchar(1020)  DEFAULT '',
  "state_short" char(2)  DEFAULT '',
  "zip" varchar(64)  DEFAULT '',
  "zip4" char(4)  DEFAULT '',
  "country" varchar(1020)  DEFAULT '',
  "lat" decimal(10,8),
  "lon" decimal(11,8),
  "raw_address" varchar(1020)  DEFAULT '',
  "phone_usa" varchar(100)  DEFAULT '',
  "address1_usa" varchar(1020)  DEFAULT '',
  "address2_usa" varchar(1020)  DEFAULT '',
  "city_usa" varchar(1020)  DEFAULT '',
  "state_usa" varchar(1020)  DEFAULT '',
  "state_short_usa" char(2)  DEFAULT '',
  "zip_usa" varchar(64)  DEFAULT '',
  "zip4_usa" char(4)  DEFAULT '',
  "country_usa" varchar(1020)  DEFAULT '',
  "lat_usa" decimal(10,8),
  "lon_usa" decimal(11,8),
  "raw_address_usa" varchar(1020) ,
  "phone_global" varchar(100)  DEFAULT '',
  "address1_global" varchar(1020)  DEFAULT '',
  "address2_global" varchar(1020)  DEFAULT '',
  "city_global" varchar(1020)  DEFAULT '',
  "state_global" varchar(1020)  DEFAULT '',
  "state_short_global" char(2)  DEFAULT '',
  "zip_global" varchar(64)  DEFAULT '',
  "country_global" varchar(1020)  DEFAULT '',
  "lat_global" decimal(10,8),
  "lon_global" decimal(11,8),
  "raw_address_global" varchar(1020) ,
  "year_founded" integer DEFAULT NULL,
  "employees" varchar(11)  DEFAULT '',
  "employees_bucket" varchar(50)  DEFAULT '',
  "revenue" varchar(20)  DEFAULT '',
  "revenue_bucket" varchar(50) ,
  "primary_industry" varchar(400)  DEFAULT '',
  "industries" varchar(max) ,
  "primary_sic_code" char(8)  DEFAULT '',
  "sic_codes" varchar(max) ,
  "primary_naics_code" char(8)  DEFAULT '',
  "naics_codes" varchar(max) ,
  "year_company_established" integer DEFAULT NULL,
  "corporate_entity_type" varchar(400)  DEFAULT NULL,
  "home_based_indicator" smallint DEFAULT '0',
  "sole_propriertorship_indicator" smallint DEFAULT '0',
  "last_found" datetime,
  "last_processed" datetime,
  "random_tenth_percent" smallint
)'''

cur.execute(drop_companies)
con.commit()
cur.execute(create_companies)
con.commit()
cur.execute('COPY "production_companies_v8.0.0" FROM \'{}\' CREDENTIALS \'aws_iam_role={}\' DELIMITER \'|\' ESCAPE IGNOREHEADER 1 NULL AS \'\\\\\\\\\\\\\\N\' ACCEPTINVCHARS AS \'?\''.format(s3_loc, arn))

con.commit()
cur.execute('select count(*) from "production_companies_v8.0.0"')
cur.fetchall()
cur.execute('select count(distinct name) from "production_companies_v8.0.0"')
cur.fetchall()

# --------
import psycopg2
s3_loc = 's3://nwd-exports/redshift/production_business_emails_2019_01_25/production_business_emails_2019_01_25.psv'
arn = 'arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
con=psycopg2.connect(dbname= 'dev', host='b2b-data-redshift-cluster.ce0dlxdyinbt.us-west-2.redshift.amazonaws.com', port= 5439, user= 'root', password= 'masterAWS123!')
cur = con.cursor()

drop_business_emails = 'DROP TABLE IF EXISTS "production_business_emails_v8.0.0"'
create_business_emails = '''
CREATE TABLE "production_business_emails_v8.0.0" (
  "id" BIGINT ,
  "person_id" bigint DISTKEY,
  "email" varchar(1020),
  "domain" varchar(1020),
  "best_guess" smallint DEFAULT '0',
  "guess_confidence" smallint,
  "guess_confidence_num_found" integer,
  "pattern" varchar(40) DEFAULT '',
  "md5" char(32) SORTKEY,
  "last_modified" datetime
)'''

cur.execute(drop_business_emails)
con.commit()
cur.execute(create_business_emails)
con.commit()
cur.execute('COPY "production_business_emails_v8.0.0" FROM \'{}\' CREDENTIALS \'aws_iam_role={}\' DELIMITER \'|\' ESCAPE IGNOREHEADER 1 NULL AS \'\\\\\\\\\\\\\\N\' ACCEPTINVCHARS AS \'?\''.format(s3_loc, arn))

con.commit()
cur.execute('select count(*) from "production_business_emails_v8.0.0"')
cur.fetchall()
cur.execute('select count(distinct md5) from "production_business_emails_v8.0.0"')
cur.fetchall()


import psycopg2
s3_loc = 's3://nwd-exports/redshift/production_business_emails_2019_01_25/production_business_emails_2019_01_25.psv'
arn = 'arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
con=psycopg2.connect(dbname= 'dev', host='b2b-data-redshift-cluster.ce0dlxdyinbt.us-west-2.redshift.amazonaws.com', port= 5439, user= 'root', password= 'masterAWS123!')
con.set_session(autocommit=True)
cur = con.cursor()
query = '''
SELECT
    id,
    firstname,
    lastname,
    title,
    SPLIT_PART(b2b_email_list, '|', 1) AS email,
    phone1,
    phone2,
    companyID,
    company,
    address,
    address2,
    city,
    state,
    zip,
    zip4,
    country,
    companyurl,
    employeesize,
    revenue,
    industry,
    SIC8,
    SIC8Description,
    function,
    level,
    linkedin,
    SPLIT_PART(b2b_email_list, '|', 2) AS emailaccuracy,
    personal_email_md5
    FROM (
    SELECT
    "production_persons_v8.0.0".id AS id,
    "production_persons_v8.0.0".first_name AS firstname,
    "production_persons_v8.0.0".last_name AS lastname,
    "production_persons_v8.0.0".title,
    (SELECT 
    "production_business_emails_v8.0.0".email || '|' ||
    CASE
    WHEN "production_validated_emails_v8.0.2".status = 'Bad' THEN NULL
    WHEN "production_validated_emails_v8.0.2".hard_bounce = 1 THEN NULL
    WHEN "production_validated_emails_v8.0.2".status = 'Ok' THEN 'good'
    WHEN "production_validated_emails_v8.0.2".additional_status_info = 'ServerIsCatchAll' THEN 'catch all'
    WHEN "production_validated_emails_v8.0.2".id IS NOT NULL THEN 'unknown'
    WHEN "production_validated_email_domains_v8.0.1".most_recent_additional_email_status_info IN ('DomainIsInexistent', 'NoMxServersFound') THEN NULL
    WHEN "production_validated_email_domains_v8.0.1".most_recent_additional_email_status_info = 'ServerIsCatchAll' THEN 'catch all'
    WHEN "production_validated_emails_v8.0.2".id IS NULL THEN 'unverified'
    ELSE 'unknown'
    END AS b2b_email_list
    FROM "production_business_emails_v8.0.0"
    LEFT OUTER JOIN "production_validated_emails_v8.0.2" ON "production_business_emails_v8.0.0".md5 = "production_validated_emails_v8.0.2".md5
    LEFT OUTER JOIN "production_validated_email_domains_v8.0.1" ON "production_business_emails_v8.0.0".domain = "production_validated_email_domains_v8.0.1".email_domain
    WHERE "production_persons_v8.0.0".id = "production_business_emails_v8.0.0".person_id
    ORDER BY POSITION(',' || "production_validated_emails_v8.0.2".status || ',' in ',Ok,') DESC,
    CASE "production_validated_email_domains_v8.0.1".most_recent_server_type WHEN 'responds' THEN 0 ELSE "production_business_emails_v8.0.0".best_guess END DESC,
    POSITION(',' || "production_validated_email_domains_v8.0.1".most_recent_additional_email_status_info || ',' in ',ServerIsCatchAll,') DESC,
    POSITION(',' || "production_validated_emails_v8.0.2".status || ',' in ',Bad,') ASC,
    POSITION(',' || "production_validated_emails_v8.0.2".hard_bounce || ',' in ',1,') ASC,
    "production_business_emails_v8.0.0".best_guess DESC,
    "production_business_emails_v8.0.0".guess_confidence DESC,
    POSITION( ',' || "production_business_emails_v8.0.0"."pattern" || ',' in ',fn_li,fi_ln,fn.li,fn_ln,fi.ln,fn,fnli,lnfi,fnln,fn.ln,ln,filn,' ) DESC,
    "production_business_emails_v8.0.0".id ASC
    LIMIT 1) AS b2b_email_list,
    "production_persons_v8.0.0".company_phone_local AS phone1,
    "production_companies_v8.0.0".phone AS phone2,
    "production_companies_v8.0.0".id AS companyID,
    "production_companies_v8.0.0".name AS company,
    "production_persons_v8.0.0".company_address1_waterfall AS address,
    "production_persons_v8.0.0".company_address2_waterfall AS address2,
    "production_persons_v8.0.0".company_city_waterfall AS city,
    "production_persons_v8.0.0".company_state_short_waterfall AS state,
    "production_persons_v8.0.0".company_zip_waterfall AS zip,
    "production_persons_v8.0.0".company_zip4_waterfall AS zip4,
    "production_persons_v8.0.0".company_country_waterfall AS country,
    'http://www.' || NULLIF("production_companies_v8.0.0".domain, '') AS companyurl,
    "production_companies_v8.0.0".employees_bucket AS employeesize,
    "production_companies_v8.0.0".revenue_bucket AS revenue,
    "production_companies_v8.0.0".primary_industry AS industry,
    "production_companies_v8.0.0".primary_sic_code AS SIC8,
    "production_companies_v8.0.0".primary_industry AS SIC8Description,
    "production_segments_v8.0.1".b2b_job_function AS function,
    "production_segments_v8.0.1".b2b_seniority AS level,
    'http://www.linkedin.com' || NULLIF("production_persons_v8.0.0".linkedin_company_url_path, '') AS linkedin,
    (SELECT "production_person_consumer_matches".md5
    FROM "production_person_consumer_matches"
    WHERE "production_person_consumer_matches".person_id = "production_persons_v8.0.0".id
    ORDER BY
    "production_person_consumer_matches".onboarded DESC,
    "production_person_consumer_matches".confidence DESC,
    "production_person_consumer_matches".id ASC
    LIMIT 1
    ) AS personal_email_md5
    FROM "production_persons_v8.0.0"
    INNER JOIN "production_companies_v8.0.0" ON "production_persons_v8.0.0".matched_processed_company_id = "production_companies_v8.0.0".id
    LEFT OUTER JOIN "production_segments_v8.0.1" ON "production_persons_v8.0.0".id = "production_segments_v8.0.1".person_id
    WHERE "production_persons_v8.0.0".is_digital_only = 0
    AND "production_persons_v8.0.0".first_name <> ''
    AND "production_persons_v8.0.0".last_name <> ''
    AND "production_persons_v8.0.0".title <> ''
    AND "production_companies_v8.0.0".name <> ''
    AND "production_companies_v8.0.0".revenue_bucket <> ''
    AND "production_companies_v8.0.0".employees_bucket <> ''
    AND "production_companies_v8.0.0".website <> ''
    AND "production_persons_v8.0.0".personal_country = 'United States'
    AND "production_persons_v8.0.0".number_of_connections >= 1
    AND "production_persons_v8.0.0".company_country_waterfall = 'United States'
    ) t
WHERE b2b_email_list IS NOT NULL
'''

cur.execute('''UNLOAD ('{}') TO 's3://nwd-exports/redshift/unload/' CREDENTIALS 'aws_iam_role={}' '''.format(query.replace("'", "\\'"), arn))


# -----------------
import psycopg2
s3_loc = 's3://nwd-exports/redshift/production_business_emails_2019_01_25/production_business_emails_2019_01_25.psv'
arn = 'arn:aws:iam::759086516457:role/nwd-redshift-s3-access'
con=psycopg2.connect(dbname= 'dev', host='b2b-data-redshift-cluster.ce0dlxdyinbt.us-west-2.redshift.amazonaws.com', port= 5439, user= 'root', password= 'masterAWS123!')
con.set_session(autocommit=True)
cur = con.cursor()
cur.execute('ANALYZE COMPRESSION "{}"'.format('production_companies_v8.0.0'))
cur.fetchall()
con.close()




# -------------
import multiprocessing
import csv
import os

header = ['id','provider_id','provider_record_id','source_id','source_record_id','source_url_path','source_country','json','last_found','random_tenth_percent',]
json_index = header.index('json')

settings = dict(
    delimiter = '|',
    quoting = csv.QUOTE_NONE,
    escapechar = '\\',
    doublequote = '"',
    )

def get_max_json_len(path):
    long_lines = 0
    try:
        with open(path, 'rb') as ifile:
            icsv = csv.reader(ifile, **settings)
            for line in icsv:
                json_len = len(line[json_index])
                if json_len > 65535:
                    long_lines += 1
    except:
        long_lines += 1
        raise
    print '{}: {}'.format(path, long_lines)
    return long_lines

def get_files(directory):
    return sorted([os.path.join(directory, f) for f in os.listdir(directory)])


pool = multiprocessing.Pool(multiprocessing.cpu_count())
print sum(pool.map(get_max_json_len, get_files('/home/ec2-user/temp/redshift/tables/'), 1))





from url_parser import url_parse
import hashlib
f = '/home/ec2-user/temp/insideview/Netwise_li_urls.txt'
o = '/home/ec2-user/temp/insideview/Netwise_li_urls_hashed.txt'

with open(f, 'rt', encoding='utf8') as ifile, open(o, 'wt', encoding='utf8') as ofile:
    for line in ifile:
        parsed = url_parse(line)
        assert parsed.path.startswith('/in/')
        _ = ofile.write(hashlib.md5(parsed.path[4:].encode('utf8')).hexdigest() + '\n')






import MySQLdb
import os
import random

num_results = 10000
result_ids = set()
result_ips = set()
outfile = '/home/ec2-user/temp/orb_ips.csv'


def get_randint():
    return random.randint(1, 434237550)


query = '''select requesting_ip
from `staging_consumer_persons_vQ1_2019` c
inner join `unique_onboarded_md5s_vQ4_2018` o on c.md5 = o.md5
where o.id = %s
and requesting_ip <> ''
limit 1
'''


with open(outfile, 'wb') as ofile:
    with MySQLdb.connect(host = os.getenv('SQL_CONSUMER_HOST'), user = os.getenv('SQL_CONSUMER_USER'), passwd = os.getenv('SQL_CONSUMER_PASSWORD'), db = os.getenv('SQL_CONSUMER_DATABASE_DEFAULT'),) as cursor:
        while len(result_ips) < num_results:
            while True:
                newint = get_randint()
                if newint not in result_ids:
                    result_ids.add(newint)
                    break
            _ = cursor.execute(query, [newint,])
            result = cursor.fetchall()
            if result:
                newip = result[0][0]
                if newip.count('.') == 3 and newip not in ('0.0.0.0', '1.1.1.1',) and newip not in result_ips:
                    _ = ofile.write(newip + '\n')
                    result_ips.add(newip)
                    if len(result_ips) % 1000 == 0:
                        print len(result_ips)




import json
employees = set()
files = [
    'li_comp_01.json',
    'li_comp_02.json',
    'li_comp_03.json',
    'li_comp_04.json',
    'li_comp_05.json',
    'li_comp_06.json',
    'li_comp_07.json',
    'li_comp_08.json',
    'li_comp_09.json',
    'li_comp_10.json',
    'li_comp_11.json',
    'li_comp_12.json',
    'li_comp_13.json',
    ]

for file in files:
    with open(file, 'rt', encoding='utf-8') as ifile:
        for line in ifile:
            jline = json.loads(line)
            if jline.get('employment_range', None):
                employees.add(jline['employment_range'])


print(employees)








import re
f = 'sic_naics.txt'
o = 'sic_naics_fixed.txt'
valid = re.compile(r'\b[0-9]{4}\b')
dig4 = re.compile(r'\b[0-9]{4}\b')
dig6 = re.compile(r'\b[0-9]{6}\b')
bar = re.compile(r'\|\s+|\s+\|')
with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    inf = ifile.read().replace('\n', ' ')
    s = 0
    start_points = [m.start(0) for m in re.finditer(valid, inf)]
    for point in start_points:
        p = dig4.sub(lambda mo: '{}|'.format(mo.group(0)), inf[s:point])
        p = dig6.sub(lambda me: '|{}|'.format(me.group(0)), p)
        ofile.write(bar.sub('|', p) + '\n')
        s = int(point)




f = 'loglines_sorted.txt'
results = dict()
with open(f, 'rb') as ifile:
    cline = None
    counter = 0
    for line in ifile:
        if line != cline:
            results[cline] = counter
            cline = line
            counter = 1
        else:
            counter += 1


print sorted(results.items(), key=lambda x: x[1], reverse=True)



import csv
from loaders.businesses_loader import BusinessesModel
from loaders.business_domains_loader import BusinessDomainsModel

f = '/home/ec2-user/temp/cleansing/Matching File_Jan2019.csv'
o = '/home/ec2-user/temp/cleansing/Matching File_Jan2019 Appended.csv'

with open(f, 'rt', encoding='latin1') as ifile, open(o, 'wt', encoding='utf-8') as ofile:
    icsv = csv.DictReader(ifile, delimiter=',')
    ocsv = csv.DictWriter(ofile, delimiter=',', fieldnames=['company_name_aws', 'company_website_aws', 'company_name_nwd', 'company_name_normalized_nwd', 'company_domain_nwd', 'company_website_nwd'])
    ocsv.writeheader()
    for line in icsv:
        od = dict(line)
        businesses_model = BusinessesModel(name=line['company_name_aws'])
        od['company_name_nwd'] = businesses_model.name
        od['company_name_normalized_nwd'] = businesses_model.cleansed_name
        business_domains_model = BusinessDomainsModel(url=line['company_website_aws'])
        od['company_domain_nwd'] = business_domains_model.domain
        od['company_website_nwd'] = business_domains_model.website
        _ = ocsv.writerow(od)





import csv
from loaders.businesses_loader import BusinessesModel
from loaders.business_domains_loader import BusinessDomainsModel

f = '/home/ec2-user/temp/override/business_override_name_to_domain_000'
o = '/home/ec2-user/temp/override/business_override_name_to_domain.csv'

with open(f, 'rt', encoding='utf-8') as ifile, open(o, 'wt', encoding='utf-8') as ofile:
    icsv = csv.DictReader(ifile, delimiter=',')
    ocsv = csv.DictWriter(ofile, delimiter=',', fieldnames=['name_from', 'name_normalized_from', 'subdomain_to', 'domain_to', 'website_to'])
    ocsv.writeheader()
    dup_set = set()
    for line in icsv:
        od = dict(line)
        businesses_model = BusinessesModel(name=line['name_from'])
        business_domains_model = BusinessDomainsModel(url=line['domain_to'])
        if businesses_model.cleansed_name and businesses_model.name not in dup_set:
            dup_set.add(businesses_model.name)
            od['name_from'] = businesses_model.name
            od['name_normalized_from'] = businesses_model.cleansed_name
            od['subdomain_to'] = business_domains_model.subdomain
            od['domain_to'] = business_domains_model.domain
            od['website_to'] = business_domains_model.website
            _ = ocsv.writerow(od)



def ltr(l):
    max_len = max(map(lambda x: len(x), l))
    for i, item in enumerate(l):
        length = len(item)
        spaces = max_len - length
        if i == 0:
            print(f'{" "*19}\'\\\\b\' || \'{item.lower()}\'{" "*(spaces+2)} || \'\\\\b\'')
        else:
            print(f'{" "*12}\'|\' || \'\\\\b\' || \'{item.lower()}\'{" "*(spaces+2)} || \'\\\\b\'')

def str(l):
    max_len = max(map(lambda x: len(x), l))
    for i, item in enumerate(l):
        length = len(item)
        spaces = max_len - length
        print(f'OR public.fn_segp_{item}($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) = 1')

