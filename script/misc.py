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
f = 'risk_main.sql'
o = 'risk_main.nokeys.sql'
with open(f, 'rb') as ifile, open(o, 'wb') as ofile:
    for i, line in enumerate(ifile):
        if i < 200:
            if line.strip().startswith('KEY '):
                new_line = '-- ' + line
            elif line.strip().startswith(') ENGINE=InnoDB'):
                new_line = ') ENGINE=InnoDB COLLATE=utf8_unicode_ci;\n'
            elif line.strip().startswith('PRIMARY KEY ('):
                new_line = '  PRIMARY KEY (`id`)\n'
            else:
                new_line = line
            if line != new_line:
                # print new_line
                line = new_line
        # else:
        #     break
        ofile.write(line)





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

['sha256', 'last_name', 'company', 'first_name', 'title_base64', 'sha512', 'zip4', 'city', 'md5', 'zip', 'company_base64', 'nwd_id', 'domain_base64', 'state', 'sha1', 'demo_age_lifestages', 'demo_gender', 'demo_education', 'b2b_company_size', 'demo_education_grad', 'b2b_prof_groups_crim_jus_prof_corr_fac_hcps', 'demo_age_baby_boomers', 'demo_ethnicity_hispanic', 'b2b_ind_insurance', 'demo_emp_occ_health_dentist', 'b2b_company_size_med', 'b2b_high_net_worth', 'b2b_fa_med_dentist', 'b2b_ind_manufacturing', 'demo_age_20_24', 'demo_emp_occ_health_optician', 'demo_age_60_64', 'demo_age_20_29', 'demo_emp_occ_const_arch_builder', 'demo_emp_occ_services_counselor', 'b2b_fa_engineer_civil', 'demo_age_gen_y', 'b2b_ind_media_internet', 'b2b_ind_healthcare_hosp_clinics', 'demo_emp_occ_const_arch_paralegal', 'demo_emp_occ_services_funeral_director', 'demo_emp_occ_const_arch_contractor', 'b2b_ind_healthcare', 'b2b_ind_non_profit', 'b2b_fa_education_professor', 'demo_lang_japanese', 'demo_lang_french', 'b2b_ind_software_security', 'b2b_prof_groups_crim_jus_prof_sheriffs', 'b2b_fa_education', 'cat_government_politics_military', 'b2b_fa_sales', 'b2b_ind_finance', 'b2b_fa_engineer_electronics', 'demo_age_70_75', 'demo_education_college', 'demo_emp_occ_health_physical_therapist', 'demo_gender_female', 'b2b_fa_it_software', 'demo_emp_occ_health_nurse', 'demo_emp_occ_sales_real_estate', 'demo_emp_occ_health_dietician', 'b2b_business_decision_maker', 'cat_social_media', 'cat_government_politics_military_marines', 'demo_emp_roles_management', 'demo_age_18_19', 'demo_emp_occ_const_arch_architect', 'b2b_fa_engineer_chemical', 'demo_education_student', 'demo_age_elders', 'b2b_fa_medical', 'demo_education_undergrad', 'demo_emp_occ_science_engineer_statistician', 'demo_emp_occ_health_dental_hygienist', 'b2b_seniority_executives', 'b2b_fa_it_database', 'demo_age_50_59', 'demo_emp_status_employed', 'b2b_ind_real_estate', 'demo_emp_occ_argiculture_nat_resources_vet', 'demo_lang_italian', 'demo_age_50_54', 'b2b_ind_retail', 'b2b_fa_finance_investment_banking', 'demo_age_65_69', 'demo_age_40_44', 'b2b_ind_government', 'b2b_company_size_xlarge', 'demo_age_35_39', 'demo_age_70_Older', 'b2b_company_size_med_large', 'demo_age_60_Older', 'b2b_ind_education', 'b2b_fa_finance_wealth_management', 'demo_emp_occ_argiculture_nat_resources_farmer', 'demo_lang_spanish', 'b2b_fa_hr', 'demo_emp_occ_business_admin_secretary', 'b2b_fa_consultants', 'demo_emp_occ_finance_accountant', 'demo_emp_occ_it_comp', 'cat_government_politics_military_coast_guard', 'demo_lang_arabic', 'demo_age_40_49', 'demo_lang_urdu', 'b2b_fa_engineer_industrial', 'demo_lang_chinese', 'demo_education_vocation', 'demo_gender_male', 'b2b_fa_scientists', 'b2b_fa_it', 'demo_emp_occ_edu_librarian', 'demo_lang_german', 'b2b_ind_finance_investment_banking', 'cat_government_politics_military_air_force', 'demo_emp_occ_services_religious', 'demo_emp_occ_health_orderly', 'demo_emp_occ_business_admin_typist', 'b2b_ind_telecom', 'b2b_prof_groups_crim_jus_prof_parole_probation_off', 'demo_age_30_34', 'b2b_fa_legal', 'b2b_prof_groups_crim_jus_prof_judges', 'b2b_fa_it_qa', 'demo_age_30_39', 'b2b_ind_business_services', 'b2b_ind_business_services_adv_marketing', 'b2b_ind_construction', 'b2b_fa_c-suite', 'b2b_manag_mark_medic_directors', 'demo_emp_occ_health_medical_secretary', 'b2b_fa_marketing', 'demo_emp_occ_science_engineer_chemist', 'demo_age_55_59', 'demo_education_high_school', 'demo_emp_occ_health_paramedic', 'demo_emp_occ_const_arch_attorney', 'b2b_fa_it_web_development', 'demo_lang_russian', 'int_inter_online_act', 'demo_age_25_29', 'b2b_fa_engineer', 'b2b_fa_finance', 'b2b_fa_government', 'b2b_ind_real_estate_residential', 'b2b_prof_groups_crim_jus_prof_prosecutors', 'demo_age_45_49', 'demo_emp_occ_health_pharmacist', 'demo_emp_occ_science_engineer_engineer', 'demo_emp_occ_science_engineer_geologist', 'demo_emp_occ_edu_teacher', 'b2b_company_size_small', 'b2b_seniority_board_members', 'b2b_fa_finance_banking', 'demo_emp_occ_health_psychologist', 'demo_emp_roles_business_owner', 'b2b_ind_software', 'demo_emp_occ_health_medical_assistant', 'b2b_fa_med_pharma', 'demo_emp_occ_health_therapist', 'demo_emp_occ_health_chiro', 'demo_emp_occ_science_engineer_scientist', 'demo_lang_farsi', 'b2b_company_size_micro', 'b2b_ind_education_colleges_univ', 'b2b_fa_engineer_mechanical', 'demo_emp_occ_health_dental_assistant', 'b2b_company_size_large', 'demo_emp_occ_services_religious_pastor', 'b2b_fa_gov_elected_officials', 'b2b_fa_board_members', 'b2b_manag_mark_pharma_directors', 'b2b_fa_engineer_biomedical', 'b2b_fa_engineer_electrical', 'demo_emp_occ_health_optometrist', 'cat_government_politics_military_navy', 'b2b_fa_finance_accounting', 'cat_government_politics_military_army', 'demo_age_gen_x', 'false']