"""A fake for each of the engine's providers, to name a column's values by: `sd.generate({'email': sd.fakes.email}, rows=10)`.

Each draws text, and takes `nulls=` when called: `sd.fakes.email(nulls=0.1)`. `email` is at reserved `example` domains, so no
generated address reaches anyone, and `zip_code` is five digits. `icd10cm` and `hcpcs_level2` draw a code in the code set's
shape, which may name no real code.
"""

from doppeldata.inline_columns import Fake

email = Fake('email')
free_email = Fake('free_email')
username = Fake('username')
ipv4 = Fake('ipv4')
ipv6 = Fake('ipv6')
mac_address = Fake('mac_address')
user_agent = Fake('user_agent')
name = Fake('name')
first_name = Fake('first_name')
last_name = Fake('last_name')
phone_number = Fake('phone_number')
cell_number = Fake('cell_number')
company = Fake('company')
job_title = Fake('job_title')
street_address = Fake('street_address')
street_name = Fake('street_name')
building_number = Fake('building_number')
city = Fake('city')
state = Fake('state')
state_abbr = Fake('state_abbr')
zip_code = Fake('zip_code')
post_code = Fake('post_code')
country = Fake('country')
country_code = Fake('country_code')
credit_card = Fake('credit_card')
uuid = Fake('uuid')
icd10cm = Fake('icd10cm')
hcpcs_level2 = Fake('hcpcs_level2')
