from cmath import exp
import unittest
from unittest.mock import patch
import sys
from datetime import date
from faker import Faker
from freezegun import freeze_time
import json
import os
import numpy as np
import random as random_test

from datagen_customer import Customer, make_age_gender_dict, make_cities
from main_config import MainConfig
from profile_weights import Profile


class FakerMock():
    def md5(raw_output):
        return '12345'


profile_list = [
    "adults_50up_male_urban.json",
	"adults_50up_female_urban.json",
	"adults_50up_male_rural.json",
	"adults_50up_female_rural.json",
	"adults_2550_male_urban.json",
	"adults_2550_female_urban.json",
	"adults_2550_male_rural.json",
	"adults_2550_female_rural.json",
	"young_adults_male_urban.json",
	"young_adults_female_urban.json",
	"young_adults_male_rural.json",
	"young_adults_female_rural.json"
]


main = 'profiles/main_config.json'

class TestCustomer(unittest.TestCase):
    @freeze_time("2022-01-01")
    # @patch("numpy.random")
    @patch("datagen_customer.random")
    @patch("faker.Faker")
    @patch("datagen_customer.cities", make_cities())
    @patch("datagen_customer.age_gender", make_age_gender_dict())
    def test_init_male(self, faker_constructor, random_mock):
        fake = faker_constructor.return_value
        fake.ssn.return_value = '123-45-678'
        fake.last_name.return_value = 'Smith'
        fake.first_name_male.return_value = 'John'
        fake.first_name_female.return_value = 'Mary'
        fake.street_address.return_value = '123 Park Ave'
        fake.job.return_value = 'Engineer'
        fake.credit_card_number.return_value = '1234567890101234'
        fake.email.return_value = 'john.smith@example.com'
        fake.random_number.return_value = '123456789011'
        fake.date_time_this_century.return_value = date(1970, 1, 4)
        random_mock.side_effect = [0.421, 0.333]
        
        c = Customer(config=main)
        c.fake = fake
        customer_data = c.generate_customer()

        self.assertEqual(customer_data[0], '123-45-678')
        self.assertEqual(customer_data[1], '1234567890101234')
        self.assertEqual(customer_data[2], 'John')
        self.assertEqual(customer_data[3], 'Smith')
        self.assertEqual(customer_data[4], 'M')
        self.assertEqual(customer_data[5], '123 Park Ave')
        self.assertEqual(customer_data[6:12], 'Fairbanks|AK|99701|64.644|-147.5221|63999'.split("|"))
        self.assertEqual(customer_data[12], 'Engineer')
        self.assertEqual(customer_data[13], date(1976, 1, 4).strftime('%Y-%m-%d'))
        self.assertEqual(customer_data[14], '123456789011')
        self.assertEqual(customer_data[15], 'adults_2550_male_urban.json')

    @freeze_time("2022-01-01")
    # @patch("numpy.random")
    @patch("datagen_customer.random")
    @patch("faker.Faker")
    @patch("datagen_customer.cities", make_cities())
    @patch("datagen_customer.age_gender", make_age_gender_dict())
    def test_init_female(self, faker_constructor, random_mock):
        fake = faker_constructor.return_value
        fake.ssn.return_value = '123-45-678'
        fake.last_name.return_value = 'Smith'
        fake.first_name_male.return_value = 'John'
        fake.first_name_female.return_value = 'Mary'
        fake.street_address.return_value = '123 Park Ave'
        fake.job.return_value = 'Engineer'
        fake.credit_card_number.return_value = '1234567890101234'
        fake.email.return_value = 'john.smith@example.com'
        fake.random_number.return_value = '123456789011'
        fake.date_time_this_century.return_value = date(1970, 1, 4)
        # random_mock.random.return_value = 0.2
        random_mock.side_effect = [0.2, 0.333]

        c = Customer(config=main)
        c.fake = fake
        customer_data = c.generate_customer()

        self.assertEqual(customer_data[0], '123-45-678')
        self.assertEqual(customer_data[1], '1234567890101234')
        self.assertEqual(customer_data[2], 'Mary')
        self.assertEqual(customer_data[3], 'Smith')
        self.assertEqual(customer_data[4], 'F')
        self.assertEqual(customer_data[5], '123 Park Ave')
        self.assertEqual(customer_data[6:12], 'Fairbanks|AK|99701|64.644|-147.5221|63999'.split("|"))
        self.assertEqual(customer_data[12], 'Engineer')
        self.assertEqual(customer_data[13], date(1990, 1, 4).strftime('%Y-%m-%d'))
        self.assertEqual(customer_data[14], '123456789011')
        self.assertEqual(customer_data[15], 'adults_2550_female_urban.json')


    @freeze_time("2022-01-01")
    # @patch("numpy.random")
    @patch("datagen_customer.random")
    @patch("faker.Faker")
    @patch("datagen_customer.cities", make_cities())
    @patch("datagen_customer.age_gender", make_age_gender_dict())
    def test_generate_age_gender(self, faker_constructor, random_mock):

        fake = faker_constructor.return_value
        random_mock.return_value = 0.1
        fake.date_time_this_century.return_value = date(1970, 1, 4)
        c = Customer(config=main)
        c.fake = fake
        c.generate_customer()

        results = [
             (0.01, ('M', date(2002, 1, 4).strftime("%Y-%m-%d"), 19)) ,
            (0.02, ('F', date(2002, 1, 4).strftime("%Y-%m-%d"), 19)) ,
            (0.03, ('F', date(2001, 1, 4).strftime("%Y-%m-%d"), 20)) ,
            (0.04, ('M', date(2000, 1, 4).strftime("%Y-%m-%d"), 21)) ,
            (0.05, ('M', date(1999, 1, 4).strftime("%Y-%m-%d"), 22)) ,
            (0.06, ('F', date(1999, 1, 4).strftime("%Y-%m-%d"), 22)) ,
            (0.07, ('F', date(1998, 1, 4).strftime("%Y-%m-%d"), 23)) ,
            (0.08, ('M', date(1997, 1, 4).strftime("%Y-%m-%d"), 24)) ,
            (0.09, ('F', date(1997, 1, 4).strftime("%Y-%m-%d"), 24)) ,
            (0.1, ('F', date(1996, 1, 4).strftime("%Y-%m-%d"), 25)) ,
            (0.11, ('M', date(1995, 1, 4).strftime("%Y-%m-%d"), 26)) ,
            (0.12, ('F', date(1995, 1, 4).strftime("%Y-%m-%d"), 26)) ,
            (0.13, ('M', date(1994, 1, 4).strftime("%Y-%m-%d"), 27)) ,
            (0.14, ('M', date(1993, 1, 4).strftime("%Y-%m-%d"), 28)) ,
            (0.15, ('F', date(1993, 1, 4).strftime("%Y-%m-%d"), 28)) ,
            (0.16, ('M', date(1992, 1, 4).strftime("%Y-%m-%d"), 29)) ,
            (0.17, ('F', date(1992, 1, 4).strftime("%Y-%m-%d"), 29)) ,
            (0.18, ('M', date(1991, 1, 4).strftime("%Y-%m-%d"), 30)) ,
            (0.19, ('M', date(1990, 1, 4).strftime("%Y-%m-%d"), 31)) ,
            (0.2, ('F', date(1990, 1, 4).strftime("%Y-%m-%d"), 31)) ,
            (0.21, ('M', date(1989, 1, 4).strftime("%Y-%m-%d"), 32)) ,
            (0.22, ('F', date(1989, 1, 4).strftime("%Y-%m-%d"), 32)) ,
            (0.23, ('M', date(1988, 1, 4).strftime("%Y-%m-%d"), 33)) ,
            (0.24, ('M', date(1987, 1, 4).strftime("%Y-%m-%d"), 34)) ,
            (0.25, ('F', date(1987, 1, 4).strftime("%Y-%m-%d"), 34)) ,
            (0.26, ('M', date(1986, 1, 4).strftime("%Y-%m-%d"), 35)) ,
            (0.27, ('F', date(1986, 1, 4).strftime("%Y-%m-%d"), 35)) ,
            (0.28, ('F', date(1985, 1, 4).strftime("%Y-%m-%d"), 36)) ,
            (0.29, ('M', date(1984, 1, 4).strftime("%Y-%m-%d"), 37)) ,
            (0.3, ('F', date(1984, 1, 4).strftime("%Y-%m-%d"), 37)) ,
            (0.31, ('F', date(1983, 1, 4).strftime("%Y-%m-%d"), 38)) ,
            (0.32, ('M', date(1982, 1, 4).strftime("%Y-%m-%d"), 39)) ,
            (0.33, ('F', date(1982, 1, 4).strftime("%Y-%m-%d"), 39)) ,
            (0.34, ('M', date(1981, 1, 4).strftime("%Y-%m-%d"), 40)) ,
            (0.35, ('M', date(1980, 1, 4).strftime("%Y-%m-%d"), 41)) ,
            (0.36, ('F', date(1980, 1, 4).strftime("%Y-%m-%d"), 41)) ,
            (0.37, ('M', date(1979, 1, 4).strftime("%Y-%m-%d"), 42)) ,
            (0.38, ('F', date(1979, 1, 4).strftime("%Y-%m-%d"), 42)) ,
            (0.39, ('M', date(1978, 1, 4).strftime("%Y-%m-%d"), 43)) ,
            (0.4, ('M', date(1977, 1, 4).strftime("%Y-%m-%d"), 44)) ,
            (0.41, ('F', date(1977, 1, 4).strftime("%Y-%m-%d"), 44)) ,
            (0.42, ('M', date(1976, 1, 4).strftime("%Y-%m-%d"), 45)) ,
            (0.43, ('F', date(1976, 1, 4).strftime("%Y-%m-%d"), 45)) ,
            (0.44, ('M', date(1975, 1, 4).strftime("%Y-%m-%d"), 46)) ,
            (0.45, ('F', date(1975, 1, 4).strftime("%Y-%m-%d"), 46)) ,
            (0.46, ('M', date(1974, 1, 4).strftime("%Y-%m-%d"), 47)) ,
            (0.47, ('M', date(1973, 1, 4).strftime("%Y-%m-%d"), 48)) ,
            (0.48, ('F', date(1973, 1, 4).strftime("%Y-%m-%d"), 48)) ,
            (0.49, ('M', date(1972, 1, 4).strftime("%Y-%m-%d"), 49)) ,
            (0.5, ('F', date(1972, 1, 4).strftime("%Y-%m-%d"), 49)) ,
            (0.51, ('M', date(1971, 1, 4).strftime("%Y-%m-%d"), 50)) ,
            (0.52, ('F', date(1971, 1, 4).strftime("%Y-%m-%d"), 50)) ,
            (0.53, ('M', date(1970, 1, 4).strftime("%Y-%m-%d"), 51)) ,
            (0.54, ('F', date(1970, 1, 4).strftime("%Y-%m-%d"), 51)) ,
            (0.55, ('M', date(1969, 1, 4).strftime("%Y-%m-%d"), 52)) ,
            (0.56, ('F', date(1969, 1, 4).strftime("%Y-%m-%d"), 52)) ,
            (0.57, ('M', date(1968, 1, 4).strftime("%Y-%m-%d"), 53)) ,
            (0.58, ('F', date(1968, 1, 4).strftime("%Y-%m-%d"), 53)) ,
            (0.59, ('M', date(1967, 1, 4).strftime("%Y-%m-%d"), 54)) ,
            (0.6, ('M', date(1966, 1, 4).strftime("%Y-%m-%d"), 55)) ,
            (0.61, ('F', date(1966, 1, 4).strftime("%Y-%m-%d"), 55)) ,
            (0.62, ('M', date(1965, 1, 4).strftime("%Y-%m-%d"), 56)) ,
            (0.63, ('F', date(1965, 1, 4).strftime("%Y-%m-%d"), 56)) ,
            (0.64, ('M', date(1964, 1, 4).strftime("%Y-%m-%d"), 57)) ,
            (0.65, ('F', date(1964, 1, 4).strftime("%Y-%m-%d"), 57)) ,
            (0.66, ('M', date(1963, 1, 4).strftime("%Y-%m-%d"), 58)) ,
            (0.67, ('F', date(1963, 1, 4).strftime("%Y-%m-%d"), 58)) ,
            (0.68, ('M', date(1962, 1, 4).strftime("%Y-%m-%d"), 59)) ,
            (0.69, ('F', date(1962, 1, 4).strftime("%Y-%m-%d"), 59)) ,
            (0.7, ('M', date(1961, 1, 4).strftime("%Y-%m-%d"), 60)) ,
            (0.71, ('F', date(1961, 1, 4).strftime("%Y-%m-%d"), 60)) ,
            (0.72, ('F', date(1960, 1, 4).strftime("%Y-%m-%d"), 61)) ,
            (0.73, ('M', date(1959, 1, 4).strftime("%Y-%m-%d"), 62)) ,
            (0.74, ('F', date(1959, 1, 4).strftime("%Y-%m-%d"), 62)) ,
            (0.75, ('M', date(1958, 1, 4).strftime("%Y-%m-%d"), 63)) ,
            (0.76, ('M', date(1957, 1, 4).strftime("%Y-%m-%d"), 64)) ,
            (0.77, ('F', date(1957, 1, 4).strftime("%Y-%m-%d"), 64)) ,
            (0.78, ('M', date(1956, 1, 4).strftime("%Y-%m-%d"), 65)) ,
            (0.79, ('M', date(1955, 1, 4).strftime("%Y-%m-%d"), 66)) ,
            (0.8, ('F', date(1955, 1, 4).strftime("%Y-%m-%d"), 66)) ,
            (0.81, ('F', date(1954, 1, 4).strftime("%Y-%m-%d"), 67)) ,
            (0.82, ('M', date(1953, 1, 4).strftime("%Y-%m-%d"), 68)) ,
            (0.83, ('M', date(1952, 1, 4).strftime("%Y-%m-%d"), 69)) ,
            (0.84, ('F', date(1952, 1, 4).strftime("%Y-%m-%d"), 69)) ,
            (0.85, ('M', date(1950, 1, 4).strftime("%Y-%m-%d"), 71)) ,
            (0.86, ('M', date(1949, 1, 4).strftime("%Y-%m-%d"), 72)) ,
            (0.87, ('M', date(1948, 1, 4).strftime("%Y-%m-%d"), 73)) ,
            (0.88, ('M', date(1947, 1, 4).strftime("%Y-%m-%d"), 74)) ,
            (0.89, ('F', date(1946, 1, 4).strftime("%Y-%m-%d"), 75)) ,
            (0.9, ('F', date(1945, 1, 4).strftime("%Y-%m-%d"), 76)) ,
            (0.91, ('M', date(1943, 1, 4).strftime("%Y-%m-%d"), 78)) ,
            (0.92, ('F', date(1942, 1, 4).strftime("%Y-%m-%d"), 79)) ,
            (0.93, ('F', date(1940, 1, 4).strftime("%Y-%m-%d"), 81)) ,
            (0.94, ('F', date(1938, 1, 4).strftime("%Y-%m-%d"), 83)) ,
            (0.95, ('M', date(1936, 1, 4).strftime("%Y-%m-%d"), 85)) ,
            (0.96, ('F', date(1934, 1, 4).strftime("%Y-%m-%d"), 87)) ,
            (0.97, ('F', date(1932, 1, 4).strftime("%Y-%m-%d"), 89)) ,
            (0.98, ('F', date(1930, 1, 4).strftime("%Y-%m-%d"), 91)) ,
            (0.99, ('F', date(1928, 1, 4).strftime("%Y-%m-%d"), 93)) 
        ]

        for i in range(len(results)):
            random_mock.return_value = results[i][0]
            gender, dob, age = c.generate_age_gender()
            self.assertEqual(results[i][1], (gender, dob, age))

    @freeze_time("2022-01-01")
    # @patch("numpy.random")
    @patch("datagen_customer.random")
    @patch("faker.Faker")
    @patch("datagen_customer.cities", make_cities())
    @patch("datagen_customer.age_gender", make_age_gender_dict())
    def test_get_random_location(self, faker_constructor, random_mock):
        fake = faker_constructor.return_value
        fake.ssn.return_value = '123-45-678'
        fake.last_name.return_value = 'Smith'
        fake.first_name_male.return_value = 'John'
        fake.first_name_female.return_value = 'Mary'
        fake.street_address.return_value = '123 Park Ave'
        fake.job.return_value = 'Engineer'
        fake.credit_card_number.return_value = '1234567890101234'
        fake.email.return_value = 'john.smith@example.com'
        fake.random_number.return_value = '123456789011'
        fake.date_time_this_century.return_value = date(1970, 1, 4)
        fake.date_time_this_century.return_value = date(1970, 1, 4)

        random_mock.return_value = 0.1
        c = Customer(config=main)
        c.fake = fake
        c.generate_customer()

        results = [
            (0.0, 'Woodland Hills|CA|91371|33.7866|-118.2987|65351') ,
            (0.01, 'Jekyll Island|GA|31527|31.074|-81.4128|805') ,
            (0.02, 'Valier|MT|59486|48.2795|-112.3033|1328') ,
            (0.03, 'Duck|WV|25063|38.5647|-80.9764|1797') ,
            (0.04, 'Shullsburg|WI|53586|42.5786|-90.2266|2284') ,
            (0.05, 'Charleston|WV|25301|38.349|-81.6306|116525') ,
            (0.06, 'Joaquin|TX|75954|31.944|-94.0608|3280') ,
            (0.07, 'Unionville|MO|63565|40.4815|-92.9951|3805') ,
            (0.08, 'Alamo|GA|30411|32.133|-82.7944|4371') ,
            (0.09, 'Hustonville|KY|40437|37.4595|-84.8528|4932') ,
            (0.1, 'Longview|TX|75603|32.4264|-94.7117|102348') ,
            (0.11, 'Saint Gabriel|LA|70776|30.2528|-91.0857|6043') ,
            (0.12, 'Weed|CA|96094|41.6661|-122.4086|6630') ,
            (0.13, 'Pinconning|MI|48650|43.8491|-84.0082|7253') ,
            (0.14, 'Montvale|NJ|07645|41.0495|-74.0384|7832') ,
            (0.15, 'Viroqua|WI|54665|43.5442|-90.8939|8427') ,
            (0.16, 'Township Of Washington|NJ|07676|40.9883|-74.0635|9075') ,
            (0.17, 'North Manchester|IN|46962|40.9986|-85.7842|9728') ,
            (0.18, 'Galena|OH|43021|40.2011|-82.8749|10301') ,
            (0.19, 'Kershaw|SC|29067|34.5578|-80.5546|10949') ,
            (0.2, 'Steubenville|OH|43953|40.3524|-80.6781|30603') ,
            (0.21, 'Benton|LA|71006|32.6976|-93.691|12158') ,
            (0.22, 'Rayville|LA|71269|32.4456|-91.7433|12757') ,
            (0.23, 'Greenbrier|TN|37073|36.4229|-86.7914|13350') ,
            (0.24, 'Pella|IA|50219|41.4082|-92.9172|13939') ,
            (0.25, 'North Augusta|SC|29860|33.6028|-81.9748|46944') ,
            (0.26, 'Saint Petersburg|FL|33701|27.7723|-82.6386|341043') ,
            (0.27, 'Oneonta|AL|35121|33.9259|-86.4741|15570') ,
            (0.28, 'Wixom|MI|48393|42.534|-83.5285|16111') ,
            (0.29, 'Sarasota|FL|34233|27.2866|-82.477|232625') ,
            (0.3, 'Sturgeon Bay|WI|54235|44.8438|-87.3753|17203') ,
            (0.31, 'Easthampton|MA|01027|42.3683|-72.7688|17660') ,
            (0.32, 'Lorain|OH|44053|41.432|-82.2038|67952') ,
            (0.33, 'Denver|CO|80203|39.7313|-104.9811|990452') ,
            (0.34, 'Palm Springs|CA|92264|33.8018|-116.517|45562') ,
            (0.35, 'Baraboo|WI|53913|43.4825|-89.7462|19881') ,
            (0.36, 'Louisville|OH|44641|40.8477|-81.2595|20349') ,
            (0.37, 'Valley Stream|NY|11581|40.6523|-73.7118|60957') ,
            (0.38, 'Warsaw|IN|46580|41.2438|-85.8508|33716') ,
            (0.39, 'Fort Smith|AR|72904|35.4051|-94.3872|90141') ,
            (0.4, 'Gulf Breeze|FL|32563|30.3962|-87.0274|30305') ,
            (0.41, 'Phoenix|AZ|85024|33.6617|-112.037|1312922') ,
            (0.42, 'Omaha|NE|68111|41.2962|-95.965|518429') ,
            (0.43, 'Carthage|MO|64836|37.1597|-94.3112|23940') ,
            (0.44, 'Edgewood|MD|21040|39.4277|-76.3056|24420') ,
            (0.45, 'Dunn|NC|28334|35.3165|-78.6151|24908') ,
            (0.46, 'Los Angeles|CA|90064|34.0353|-118.4259|2383912') ,
            (0.47, 'Bremerton|WA|98311|47.6271|-122.6373|84812') ,
            (0.48, 'Lafayette|CO|80026|39.998|-105.0963|26378') ,
            (0.49, 'Puyallup|WA|98375|47.1037|-122.3235|129412') ,
            (0.5, 'Memphis|TN|38106|35.1021|-90.033|697385') ,
            (0.51, 'Las Cruces|NM|88011|32.3244|-106.6683|143900') ,
            (0.52, 'Burlington|VT|05401|44.484|-73.2199|42705') ,
            (0.53, 'Yorktown Heights|NY|10598|41.2999|-73.7924|28647') ,
            (0.54, 'Bourbonnais|IL|60914|41.1661|-87.879|29107') ,
            (0.55, 'Everett|WA|98201|47.9884|-122.2006|168159') ,
            (0.56, 'Lynnwood|WA|98087|47.862|-122.2532|92865') ,
            (0.57, 'Frankfort|IL|60423|41.5094|-87.8248|30423') ,
            (0.58, 'West Covina|CA|91792|34.0229|-117.8975|108175') ,
            (0.59, 'Ogden|UT|84405|41.1739|-111.9809|186422') ,
            (0.6, 'Atlanta|GA|30341|33.8879|-84.2905|900273') ,
            (0.61, 'Alexandria|VA|22306|38.7589|-77.0873|321490') ,
            (0.62, 'Key West|FL|33040|24.6557|-81.3824|32891') ,
            (0.63, 'Falls Church|VA|22042|38.8635|-77.1939|116155') ,
            (0.64, 'Albany|OR|97322|44.626|-123.057|58967') ,
            (0.65, 'Windsor Mill|MD|21244|39.3331|-76.7849|34611') ,
            (0.66, 'Chester|PA|19013|39.8498|-75.3747|35130') ,
            (0.67, 'Saint George|UT|84790|37.0831|-113.5581|74901') ,
            (0.68, 'Muskegon|MI|49441|43.1962|-86.2738|128715') ,
            (0.69, 'Chandler|AZ|85249|33.2414|-111.7745|255641') ,
            (0.7, 'Silver Spring|MD|20910|38.9982|-77.0338|282095') ,
            (0.71, 'Ellicott City|MD|21042|39.2726|-76.8614|80322') ,
            (0.72, 'Gonzales|LA|70737|30.2473|-90.918|38643') ,
            (0.73, 'Bristol|TN|37620|36.5686|-82.1819|39102') ,
            (0.74, 'Portland|OR|97230|45.5472|-122.5001|841711') ,
            (0.75, 'Phoenix|AZ|85051|33.5591|-112.1332|1312922') ,
            (0.76, 'Phoenix|AZ|85042|33.3794|-112.0283|1312922') ,
            (0.77, 'Vancouver|WA|98661|45.6418|-122.6251|299480') ,
            (0.78, 'Carol Stream|IL|60188|41.9178|-88.137|42656') ,
            (0.79, 'Vineland|NJ|08360|39.4818|-75.0091|60707') ,
            (0.8, 'Endicott|NY|13760|42.1506|-76.0551|44264') ,
            (0.81, 'San Diego|CA|92111|32.7972|-117.1708|1241364') ,
            (0.82, 'Olive Branch|MS|38654|34.9441|-89.8544|45956') ,
            (0.83, 'Broomfield|CO|80020|39.9245|-105.0609|92337') ,
            (0.84, 'Vista|CA|92084|33.2131|-117.2243|112033') ,
            (0.85, 'Taunton|MA|02780|41.905|-71.1026|49036') ,
            (0.86, 'Brooklyn|NY|11237|40.7006|-73.918|2504700') ,
            (0.87, 'New York City|NY|10011|40.7402|-73.9996|1577385') ,
            (0.88, 'Greensboro|NC|27410|36.1032|-79.8794|305168') ,
            (0.89, 'Humble|TX|77346|30.0042|-95.1728|194500') ,
            (0.9, 'Laredo|TX|78045|27.6357|-99.5923|248858') ,
            (0.91, 'Fort Lauderdale|FL|33313|26.1487|-80.2075|711693') ,
            (0.92, 'Los Angeles|CA|90034|34.029|-118.4005|2383912') ,
            (0.93, 'San Pablo|CA|94806|37.9724|-122.3369|59861') ,
            (0.94, 'Lancaster|PA|17603|40.0091|-76.3671|164596') ,
            (0.95, 'Los Angeles|CA|90019|34.0482|-118.3343|2383912') ,
            (0.96, 'Pasco|WA|99301|46.2492|-119.1044|68191') ,
            (0.97, 'Temecula|CA|92592|33.4983|-117.0958|114424') ,
            (0.98, 'Brooklyn|NY|11223|40.5979|-73.9743|2504700') ,
            (0.99, 'Los Angeles|CA|90044|33.9551|-118.2901|2383912')        ]
        for i in range(len(results)):
            random_mock.return_value = results[i][0]
            self.assertEqual(results[i][1].split("|"), c.get_random_location())


    @freeze_time("2022-01-01")
    @patch("datagen_customer.random")
    @patch("faker.Faker")
    @patch("datagen_customer.cities", make_cities())
    @patch("datagen_customer.age_gender", make_age_gender_dict())
    def test_find_profile(self, faker_constructor, random_mock):
        fake = faker_constructor.return_value
        fake.ssn.return_value = '123-45-678'
        fake.last_name.return_value = 'Smith'
        fake.first_name_male.return_value = 'John'
        fake.first_name_female.return_value = 'Mary'
        fake.street_address.return_value = '123 Park Ave'
        fake.job.return_value = 'Engineer'
        fake.credit_card_number.return_value = '1234567890101234'
        fake.email.return_value = 'john.smith@example.com'
        fake.random_number.return_value = '123456789011'

        fake.date_time_this_century.return_value = date(1960, 1, 4)

        results = [
            (0.0, 'young_adults_male_urban.json') ,
            (0.01, 'young_adults_male_urban.json') ,
            (0.02, 'young_adults_female_urban.json') ,
            (0.03, 'young_adults_female_urban.json') ,
            (0.04, 'young_adults_male_urban.json') ,
            (0.05, 'young_adults_male_urban.json') ,
            (0.06, 'young_adults_female_urban.json') ,
            (0.07, 'young_adults_female_urban.json') ,
            (0.08, 'young_adults_male_urban.json') ,
            (0.09, 'young_adults_female_urban.json') ,
            (0.1, 'adults_2550_female_urban.json') ,
            (0.11, 'adults_2550_male_urban.json') ,
            (0.12, 'adults_2550_female_urban.json') ,
            (0.13, 'adults_2550_male_urban.json') ,
            (0.14, 'adults_2550_male_urban.json') ,
            (0.15, 'adults_2550_female_urban.json') ,
            (0.16, 'adults_2550_male_urban.json') ,
            (0.17, 'adults_2550_female_urban.json') ,
            (0.18, 'adults_2550_male_urban.json') ,
            (0.19, 'adults_2550_male_urban.json') ,
            (0.2, 'adults_2550_female_urban.json') ,
            (0.21, 'adults_2550_male_urban.json') ,
            (0.22, 'adults_2550_female_urban.json') ,
            (0.23, 'adults_2550_male_urban.json') ,
            (0.24, 'adults_2550_male_urban.json') ,
            (0.25, 'adults_2550_female_urban.json') ,
            (0.26, 'adults_2550_male_urban.json') ,
            (0.27, 'adults_2550_female_urban.json') ,
            (0.28, 'adults_2550_female_urban.json') ,
            (0.29, 'adults_2550_male_urban.json') ,
            (0.3, 'adults_2550_female_urban.json') ,
            (0.31, 'adults_2550_female_urban.json') ,
            (0.32, 'adults_2550_male_urban.json') ,
            (0.33, 'adults_2550_female_urban.json') ,
            (0.34, 'adults_2550_male_urban.json') ,
            (0.35, 'adults_2550_male_urban.json') ,
            (0.36, 'adults_2550_female_urban.json') ,
            (0.37, 'adults_2550_male_urban.json') ,
            (0.38, 'adults_2550_female_urban.json') ,
            (0.39, 'adults_2550_male_urban.json') ,
            (0.4, 'adults_2550_male_urban.json') ,
            (0.41, 'adults_2550_female_urban.json') ,
            (0.42, 'adults_2550_male_urban.json') ,
            (0.43, 'adults_2550_female_urban.json') ,
            (0.44, 'adults_2550_male_urban.json') ,
            (0.45, 'adults_2550_female_urban.json') ,
            (0.46, 'adults_2550_male_urban.json') ,
            (0.47, 'adults_2550_male_urban.json') ,
            (0.48, 'adults_2550_female_urban.json') ,
            (0.49, 'adults_2550_male_urban.json') ,
            (0.5, 'adults_2550_female_urban.json') ,
            (0.51, 'adults_50up_male_urban.json') ,
            (0.52, 'adults_50up_female_urban.json') ,
            (0.53, 'adults_50up_male_urban.json') ,
            (0.54, 'adults_50up_female_urban.json') ,
            (0.55, 'adults_50up_male_urban.json') ,
            (0.56, 'adults_50up_female_urban.json') ,
            (0.57, 'adults_50up_male_urban.json') ,
            (0.58, 'adults_50up_female_urban.json') ,
            (0.59, 'adults_50up_male_urban.json') ,
            (0.6, 'adults_50up_male_urban.json') ,
            (0.61, 'adults_50up_female_urban.json') ,
            (0.62, 'adults_50up_male_urban.json') ,
            (0.63, 'adults_50up_female_urban.json') ,
            (0.64, 'adults_50up_male_urban.json') ,
            (0.65, 'adults_50up_female_urban.json') ,
            (0.66, 'adults_50up_male_urban.json') ,
            (0.67, 'adults_50up_female_urban.json') ,
            (0.68, 'adults_50up_male_urban.json') ,
            (0.69, 'adults_50up_female_urban.json') ,
            (0.7, 'adults_50up_male_urban.json') ,
            (0.71, 'adults_50up_female_urban.json') ,
            (0.72, 'adults_50up_female_urban.json') ,
            (0.73, 'adults_50up_male_urban.json') ,
            (0.74, 'adults_50up_female_urban.json') ,
            (0.75, 'adults_50up_male_urban.json') ,
            (0.76, 'adults_50up_male_urban.json') ,
            (0.77, 'adults_50up_female_urban.json') ,
            (0.78, 'adults_50up_male_urban.json') ,
            (0.79, 'adults_50up_male_urban.json') ,
            (0.8, 'adults_50up_female_urban.json') ,
            (0.81, 'adults_50up_female_urban.json') ,
            (0.82, 'adults_50up_male_urban.json') ,
            (0.83, 'adults_50up_male_urban.json') ,
            (0.84, 'adults_50up_female_urban.json') ,
            (0.85, 'adults_50up_male_urban.json') ,
            (0.86, 'adults_50up_male_urban.json') ,
            (0.87, 'adults_50up_male_urban.json') ,
            (0.88, 'adults_50up_male_urban.json') ,
            (0.89, 'adults_50up_female_urban.json') ,
            (0.9, 'adults_50up_female_urban.json') ,
            (0.91, 'adults_50up_male_urban.json') ,
            (0.92, 'adults_50up_female_urban.json') ,
            (0.93, 'adults_50up_female_urban.json') ,
            (0.94, 'adults_50up_female_urban.json') ,
            (0.95, 'adults_50up_male_urban.json') ,
            (0.96, 'adults_50up_female_urban.json') ,
            (0.97, 'adults_50up_female_urban.json') ,
            (0.98, 'adults_50up_female_urban.json') ,
            (0.99, 'adults_50up_female_urban.json')
        ]
        for i in range(len(results)):
            # set random value that affects age_gender
            random_mock.side_effect = [results[i][0], 0.1, results[i][0]]
            c = Customer(config=main)
            c.fake = fake
            c.generate_customer()
            self.assertEqual((results[i][0], results[i][1]), (results[i][0], c.find_profile()))

class TestProfileWeights(unittest.TestCase):
    @patch("random.randint")
    def test_sample_time(self, random_randint_mock):

        profile_file = os.path.join('profiles', 'adults_2550_female_urban.json')
        with open(profile_file, 'r') as f:
            profile_obj = json.load(f)

            p = Profile(profile_obj)

            # AM no fraud
            for i in range(100):
                ts = p.sample_time('AM', 0)
                hr, mn, sec = ts
                self.assertLess(int(hr), 12)

            # PM no fraud
            for i in range(100):
                ts = p.sample_time('PM', 0)
                hr, mn, sec = ts
                self.assertGreaterEqual(int(hr), 12)

            # AM fraud
            for i in range(100):
                random_randint_mock.return_value = i
                ts = p.sample_time('AM', 1)
                hr, mn, sec = ts
                if i <= 20:
                    self.assertLess(int(hr), 12)
                else:
                    self.assertLess(int(hr), 4)

            # PM fraud
            for i in range(100):
                random_randint_mock.return_value = i
                ts = p.sample_time('PM', 1)
                hr, mn, sec = ts
                if i <= 20:
                    self.assertGreaterEqual(int(hr), 12)
                else:
                    self.assertGreaterEqual(int(hr), 22)

    def test_profile_values(self):
        for p_file in profile_list:
            profile_file = os.path.join('profiles', p_file)
            with open(profile_file, 'r') as f:
                profile_obj = json.load(f)

                p = Profile(profile_obj)
                p.set_date_range(date(2012,1,1), date(2012, 12,31))

                # to save the profiles as test data use the following:
                # del p.proportions['date_wt']
                # with open(f"./tests/data/{p_file}", 'w') as f:
                #     json.dump(p.proportions, f, indent=4, sort_keys=True, default=str)

                # test that the results are matching the saved data
                del p.proportions['date_wt']
                p_dump = json.loads(json.dumps(p.proportions, default=str))
                with open(os.path.join('tests', 'data', p_file), 'r') as f:
                    p_test = json.load(f)
                self.assertDictEqual(p_dump['categories_wt'], p_test['categories_wt'])
                self.assertDictEqual(p_dump['shopping_time'], p_test['shopping_time'])
                self.assertDictEqual(p_dump['date_prop'], p_test['date_wt'])


    @patch("numpy.random", np.random)
    @patch("random.randrange", random_test.randrange)
    @patch("random.randint", random_test.randint)
    def test_sample_from(self):
        self.maxDiff = None

        # set the seed so we always get the same random numbers
        # note with 0, we end up with 1 profile without transactions, but enough transactions to 
        # make sure we have not regression in the code.
        np.random.seed(0)
        random_test.seed(0)

        for p_file in profile_list:
            profile_file = os.path.join('profiles', p_file)
            with open(profile_file, 'r') as f:
                profile_obj = json.load(f)

                p = Profile(profile_obj)
                p.set_date_range(date(2012,1,1), date(2012, 1,31))

                # no fraud
                output, is_traveling, travel_max, fraud_dates = p.sample_from(0)

                # # to save the test data, use the following
                # with open(os.path.join('tests','data', p_file.replace('.json', '.csv')), 'w') as f:
                #     for row in output:
                #         f.write("|".join([row, str(int(is_traveling)), str(travel_max)] + fraud_dates) + '\n')

                with open(os.path.join('tests','data', p_file.replace('.json', '.csv')), 'r') as f:
                    rows = f.readlines()
                    self.assertEqual(len(rows), len(output))
                    for i in range(len(output)):
                        row = rows[i].strip().split('|')
                        expected_output = row[:6]
                        test_row = output[i]
                        for j, o in enumerate(expected_output):
                            # skip md5
                            if j == 0:
                                continue
                            self.assertEqual(o, test_row[j])
                        
                        self.assertEqual(int(is_traveling), int(row[7]))
                        self.assertEqual(travel_max, int(row[8]))
                        

                # fraud
                output, is_traveling, travel_max, fraud_dates = p.sample_from(1)

                # # to save the test data, use the following
                # with open(os.path.join('tests','data', p_file.replace('.json', '_fraud.csv')), 'w') as f:
                #     for row in output:
                #         f.write("|".join([row, str(int(is_traveling)), str(travel_max)] + fraud_dates) + '\n')

                with open(os.path.join('tests','data', p_file.replace('.json', '_fraud.csv')), 'r') as f:
                    rows = f.readlines()
                    self.assertEqual(len(rows), len(output))
                    for i in range(len(output)):
                        row = rows[i].strip().split('|')
                        expected_output = row[:6]
                        test_row = output[i]
                        for j, o in enumerate(expected_output):
                            # skip md5
                            if j == 0:
                                continue
                            self.assertEqual(o, test_row[j])

                        self.assertEqual(int(is_traveling), int(row[7]))
                        self.assertEqual(travel_max, int(row[8]))
                        if len(fraud_dates) > 0:
                            self.assertEqual(fraud_dates, row[9:])


if __name__ == '__main__':
    unittest.main()