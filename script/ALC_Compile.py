import os
import csv
import multiprocessing
import traceback
import signal

class Processor(object):

    directory = '/home/ec2-user/temp/alc/'

    agg_header = (
        'person_id',
        'first_name',
        'middle_initial',
        'last_name',
        'address1',
        'address2',
        'city',
        'state',
        'zip',
        'zip4',
        'email_1',
        'email_2',
        'email_3',
        'email_4',
        'email_5',
        'email_6',
        'email_7',
        'email_8',
        'phone',
        'affluent_dads',
        'affluent_empty_nesters',
        'affluent_families_with_children',
        'affluent_moms',
        'luxury_travelers',
        'cruise_travel',
        'domestic_travel',
        'international_travel',
        'travel_purchase_online',
        'travel_purchase_retail',
        'families_with_children_0_2',
        'families_with_children_3_5',
        'families_with_children_6_10',
        'families_with_children_11_15',
        'families_with_children_16_17',
        'families_with_children_single_moms',
        'families_with_children_single_dads',
        'families_with_children_grandparents',
        'childrens_product_purchases_baby_care',
        'childrens_product_purchases_general',
        'childrens_product_purchases_apparel',
        'childrens_product_purchases_back_to_school',
        'childrens_product_purchases_learning_and_activity_toys',
        'purchases_antiques',
        'purchases_apparel_childrens',
        'purchases_apparel_mens',
        'purchases_apparel_womens',
        'purchases_art',
        'purchases_baby_care',
        'purchases_books',
        'purchases_books_audio',
        'purchases_childrens_back_to_school',
        'purchases_childrens_general',
        'purchases_childrens_learning_and_activity_toys',
        'purchases_club_membership',
        'purchases_collectibles',
        'purchases_computing_home_office',
        'purchases_crafts',
        'purchases_dvds_videos',
        'purchases_female_oriented_products_services',
        'purchases_food_products_services',
        'purchases_garden_products_services',
        'purchases_gifts',
        'purchases_health_and_beauty',
        'purchases_high_end_appliances',
        'purchases_hunting',
        'purchases_jewelry',
        'purchases_luggage',
        'purchases_magazines',
        'purchases_magazines_cooking',
        'purchases_magazines_do_it_yourself',
        'purchases_magazines_family',
        'purchases_magazines_female_oriented',
        'purchases_magazines_financial',
        'purchases_magazines_gardening',
        'purchases_magazines_health',
        'purchases_magazines_male_oriented',
        'purchases_magazines_photography',
        'purchases_magazines_money',
        'purchases_male_oriented_products_services',
        'purchases_military_momorabilia_weaponry',
        'purchases_musical_instruments',
        'purchases_pets',
        'purchases_photography_video_equipment',
        'purchases_upscale_products_services',
        'purchases_travel',
        'houseshold_income_<75000',
        'houseshold_income_75000_99999',
        'houseshold_income_100000_149999',
        'houseshold_income_150000_199999',
        'houseshold_income_200000_249999',
        'houseshold_income_>250000',
        'affluent_consumers',
        'home_owners',
        'renters',
        'home_value_<100000',
        'home_value_100000_199999',
        'home_value_200000_299999',
        'home_value_300000_399999',
        'home_value_400000_499999',
        'home_value_500000_749999',
        'home_value_750000_999999',
        'home_value_>1000000',
        'credit_cards_american_express_gold_premium',
        'credit_cards_american_express_regular',
        'credit_cards_discover_gold_premium',
        'credit_cards_discover_regular',
        'credit_cards_mastercard_gold_premium',
        'credit_cards_mastercard_regular',
        'credit_cards_retail_gas_premium',
        'credit_cards_retail_gas_regular',
        'credit_cards_upscale_department_store',
        'credit_cards_visa_gold_premium',
        'credit_cards_visa_regular',
        'credit_rating_>800',
        'credit_rating_700_799',
        'credit_rating_600_699',
        'credit_rating_500_599',
        'credit_rating_<500',
        'seasonal_back_to_school_shoppers',
        'seasonal_affluent_back_to_school_shoppers',
        'seasonal_back_to_school_shoppers_with_pre_school_aged_kids',
        'seasonal_back_to_school_shoppers_with_elementary_school_aged_kids',
        'seasonal_back_to_school_shoppers_with_middle_school_aged_kids',
        'seasonal_back_to_school_shoppers_with_high_school_aged_kids',
        'seasonal_valentines_day_shoppers',
        'seasonal_affluent_valentines_day_shoppers',
        'seasonal_presidents_day_shoppers',
        'seasonal_easter_shoppers',
        'seasonal_mothers_day_shoppers',
        'seasonal_fathers_day_shoppers',
        'seasonal_independence_day_shoppers',
        'seasonal_halloween_shoppers',
        'seasonal_thanksgiving_shoppers',
        'seasonal_black_friday_shoppers',
        'seasonal_cyber_monday_shoppers',
        'seasonal_christmas_and_holiday_shoppers',
        'seasonal_affluent_christmas_and_holiday_shoppers',
        'seasonal_new_years_shoppers',
        )

    header_map = {
        'ALC_Netwise_B2S_Aug_2018.csv': ('person_id', 'first_name', 'middle_initial', 'last_name', 'address1', 'address2', 'city', 'state', 'zip', 'zip4', 'IGNORE1', 'IGNORE2', 'phone', 'email_1', 'email_2', 'email_3', 'email_4', 'email_5', 'email_6', 'email_7', 'email_8', 'email_9', 'email_10', 'email_11', 'email_12', 'email_13', 'seasonal_back_to_school_shoppers', 'seasonal_affluent_back_to_school_shoppers', 'seasonal_back_to_school_shoppers_with_pre_school_aged_kids', 'seasonal_back_to_school_shoppers_with_elementary_school_aged_kids', 'seasonal_back_to_school_shoppers_with_middle_school_aged_kids', 'seasonal_back_to_school_shoppers_with_high_school_aged_kids'),
        'ALC_Netwise_Consumer_Aug_2018.csv': ('person_id', 'first_name', 'middle_initial', 'last_name', 'address1', 'address2', 'city', 'state', 'zip', 'zip4', 'IGNORE1', 'email_1', 'email_2', 'email_3', 'email_4', 'email_5', 'email_6', 'email_7', 'email_8', 'email_9', 'email_10', 'email_11', 'email_12', 'email_13', 'IGNORE2', 'phone', 'families_with_children_0_2', 'families_with_children_3_5', 'families_with_children_6_10', 'families_with_children_11_15', 'families_with_children_16_17', 'families_with_children_single_moms', 'families_with_children_single_dads', 'families_with_children_grandparents', 'childrens_product_purchases_baby_care', 'childrens_product_purchases_general', 'childrens_product_purchases_apparel', 'childrens_product_purchases_back_to_school', 'childrens_product_purchases_learning_and_activity_toys', 'purchases_antiques', 'purchases_apparel_childrens', 'purchases_apparel_mens', 'purchases_apparel_womens', 'purchases_art', 'purchases_baby_care', 'purchases_books', 'purchases_books_audio', 'purchases_childrens_back_to_school', 'purchases_childrens_general', 'purchases_childrens_learning_and_activity_toys', 'purchases_club_membership', 'purchases_collectibles', 'purchases_computing_home_office', 'purchases_crafts', 'purchases_dvds_videos', 'purchases_female_oriented_products_services', 'purchases_food_products_services', 'purchases_garden_products_services', 'purchases_gifts', 'purchases_health_and_beauty', 'purchases_high_end_appliances', 'purchases_hunting', 'purchases_jewelry', 'purchases_luggage', 'purchases_magazines', 'purchases_magazines_cooking', 'purchases_magazines_do_it_yourself', 'purchases_magazines_family', 'purchases_magazines_female_oriented', 'purchases_magazines_financial', 'purchases_magazines_gardening', 'purchases_magazines_health', 'purchases_magazines_male_oriented', 'purchases_magazines_photography', 'purchases_magazines_money', 'purchases_male_oriented_products_services', 'purchases_military_momorabilia_weaponry', 'purchases_musical_instruments', 'purchases_pets', 'purchases_photography_video_equipment', 'purchases_upscale_products_services', 'purchases_travel', 'houseshold_income_<75000', 'houseshold_income_75000_99999', 'houseshold_income_100000_149999', 'houseshold_income_150000_199999', 'houseshold_income_200000_249999', 'houseshold_income_>250000', 'affluent_consumers', 'home_owners', 'renters', 'home_value_<100000', 'home_value_100000_199999', 'home_value_200000_299999', 'home_value_300000_399999', 'home_value_400000_499999', 'home_value_500000_749999', 'home_value_750000_999999', 'home_value_>1000000', 'credit_cards_american_express_gold_premium', 'credit_cards_american_express_regular', 'credit_cards_discover_gold_premium', 'credit_cards_discover_regular', 'credit_cards_mastercard_gold_premium', 'credit_cards_mastercard_regular', 'credit_cards_retail_gas_premium', 'credit_cards_retail_gas_regular', 'credit_cards_upscale_department_store', 'credit_cards_visa_gold_premium', 'credit_cards_visa_regular', 'credit_rating_>800', 'credit_rating_700_799', 'credit_rating_600_699', 'credit_rating_500_599', 'credit_rating_<500', 'IGNORE3', 'IGNORE4', 'IGNORE5', 'IGNORE6', 'IGNORE7', 'IGNORE8', 'IGNORE9', 'IGNORE10', 'IGNORE11', 'IGNORE12'),
        'ALC_Netwise_Holiday_Aug_2018.csv': ('person_id', 'first_name', 'middle_initial', 'last_name', 'address1', 'address2', 'city', 'state', 'zip', 'zip4', 'IGNORE1', 'email_1', 'email_2', 'email_3', 'email_4', 'email_5', 'email_6', 'email_7', 'email_8', 'email_9', 'email_10', 'email_11', 'email_12', 'email_13', 'IGNORE2', 'phone', 'seasonal_halloween_shoppers', 'seasonal_thanksgiving_shoppers', 'seasonal_black_friday_shoppers', 'seasonal_cyber_monday_shoppers', 'seasonal_christmas_and_holiday_shoppers', 'seasonal_affluent_christmas_and_holiday_shoppers', 'seasonal_new_years_shoppers'),
        'ALC_Netwise_Seasonal_Aug_2018.csv': ('person_id', 'first_name', 'middle_initial', 'last_name', 'address1', 'address2', 'city', 'state', 'zip', 'zip4', 'IGNORE1', 'email_1', 'email_2', 'email_3', 'email_4', 'email_5', 'email_6', 'email_7', 'email_8', 'email_9', 'email_10', 'email_11', 'email_12', 'email_13', 'IGNORE2', 'phone', 'seasonal_valentines_day_shoppers', 'seasonal_affluent_valentines_day_shoppers', 'seasonal_presidents_day_shoppers', 'seasonal_easter_shoppers', 'seasonal_mothers_day_shoppers', 'seasonal_fathers_day_shoppers', 'seasonal_independence_day_shoppers'),
        'ALC_Netwise_Wealth_Window_Aug_2018.csv': ('person_id', 'first_name', 'last_name', 'address1', 'address2', 'city', 'state', 'zip', 'zip4', 'phone', 'email_1', 'email_2', 'email_3', 'email_4', 'email_5', 'email_6', 'email_7', 'email_8', 'email_9', 'email_10', 'affluent_dads', 'affluent_empty_nesters', 'affluent_families_with_children', 'affluent_moms', 'luxury_travelers', 'cruise_travel', 'domestic_travel', 'international_travel', 'travel_purchase_online', 'travel_purchase_retail'),
        }

    isettings = dict(
        delimiter = ',',
        )
    osettings = dict(
        delimiter = '|',
        quoting = csv.QUOTE_NONE,
        escapechar = '\\',
        extrasaction = 'ignore',
        )

    input_emails = 13
    output_emails = 8
    input_email_iterable = range(input_emails)
    output_email_iterable = range(output_emails)

    def __init__(self):
        pass

    def test_headers(self):
        for file_name, file_header in self.header_map.iteritems():
            for column in file_header:
                if (not column.startswith('IGNORE')) and (column not in ['email_{}'.format(i+1) for i in self.input_email_iterable if i >= 8]):
                    assert column in self.agg_header, '{}: {} not in agg_header'.format(file_name, column)
            assert len(self.get_file_header(os.path.join(self.directory, file_name))) == len(file_header), '{}: column length different'.format(file_name)

    def test_fn(self):
        l = dict(name='mike', email_1=' ', email_2='', email_3='mlee1100@gmail.com', email_4='michael.lee@netwisedata.com', email_5='', email_6=' ', email_7='mlee1100@gmail.com')
        consolidated = self.consolidate_emails(l)
        assert consolidated == dict(name='mike', email_1='mlee1100@gmail.com', email_2='michael.lee@netwisedata.com', email_3=None, email_4=None, email_5=None, email_6=None, email_7=None, email_8=None)

    def get_file_header(self, file_path):
        with open(file_path, 'rb') as ifile:
            icsv = csv.DictReader(ifile, **self.isettings)
            return icsv.fieldnames

    def _dedup_list(self, l):
        found = set()
        ol = list()
        for item in l:
            if item not in found:
                ol.append(item)
                found.add(item)

        return ol

    def consolidate_emails(self, line):
        emails = self._dedup_list([line.get('email_{}'.format(i+1), None) for i in self.input_email_iterable if line.get('email_{}'.format(i+1), '').strip()])
        emails_count = len(emails)
        for i in range(self.output_emails):
            if i < emails_count:
                line['email_{}'.format(i+1)] = emails[i].strip().lower()
            else:
                line['email_{}'.format(i+1)] = None
        return line

    def process(self, file_name):
        try:
            name, ext = os.path.splitext(file_name)
            infile_path = os.path.join(self.directory, file_name)
            outfile_path = os.path.join(self.directory, '.'.join([name, 'psv']))
            assert infile_path != outfile_path, 'input and output file paths are the same'
            with open(infile_path, 'rb') as ifile, open(outfile_path, 'wb') as ofile:
                icsv = csv.DictReader(ifile, fieldnames=self.header_map[file_name], **self.isettings)
                icsv.next()
                ocsv = csv.DictWriter(ofile, fieldnames=self.agg_header, **self.osettings)
                ocsv.writeheader()
                for line in icsv:
                    line = self.consolidate_emails(line)
                    ocsv.writerow(line)
                    # break
        except KeyboardInterrupt:
            raise KeyboardInterruptError()
        except:
            traceback.print_exc()
            raise
        else:
            print '{} COMPLETED'.format(file_name)

    def ordered_files(self):
        return [os.path.split(f)[1] for f in sorted([os.path.join(self.directory, n) for n in self.header_map.keys()], key=os.path.getsize, reverse=True)]

def process(file_name):
    p = Processor()
    p.process(file_name)
    return file_name


class KeyboardInterruptError(Exception): pass


if __name__ == '__main__':
    proc = Processor()
    proc.test_headers()
    proc.test_fn()
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    try:
        res = pool.map_async(process, proc.ordered_files(), 1)
    except KeyboardInterrupt:
        pool.terminate()
    else:
        # pool.wait()
        pool.close()
    pool.join()