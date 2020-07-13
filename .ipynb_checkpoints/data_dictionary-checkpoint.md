## immigration
cicid: unique key within a month
arr_city_port_code: port code of entry
arr_date: arrival date of immigrant. It uses sas date presentation
arr_mode: mode of arrival
arr_state_code: state of immigrant in US
departure_date: departure date of immigrant. It uses sas date presentation
file_date: dates in the format YYYYMMDD
airline: Airline of entry for immigrant
flight_num: flight number of immigrant
arrival_flag: one-letter arrival code
departure_flag: one-letter departure code
update_flag: one-letter update code
match_flag: M if the arrival and departure records match


## immigrant_info
cicid: unique key within a month
i94bir: immigrant's age
gender: M, F, X, U
birthyear: year of birth
country_citizen_code: country of citizenship
country_residence_code: country of residence
visa_category:  Visa codes collapsed into three categories 1 = Business 2 = Pleasure 3 = Student
visa_issue_dep: codes corresponding to where visa was issued
visa_type: visa codes
occupation: occupation in US of immigration. Mostly STU for student, also many OTH for other
date_admitted: date to which admitted to U.S. (allowed to stay until) 
insnum: Immigration and Naturalization Services number
admnum: admission number

## city_temperature
city: city
date: date
average_temperature: average temperature of the date
average_temperature_uncertainty: the 95% confidence interval around the average
latitude: latitude of the city
longitude: longitude of the city

## city_demographic
city: city
state: state
median_age: the median of population age
male_population: the number of city male 
female_population: the number of city female 
total_population: the number of the total city 
veteran_num: the number of veterans
foreign-born: the number of the foreign-born
average_household_size: the average size of city household
state_code: state code
race: race
count: count

## city_airport
ident: identity code
type: type of the airport
name: the name of the airport
elevation_ft: elevation by feet
iso_region: region of the country
municipality: city
gps_code: gps code
iata_code: iata code
local_code: local code
coordinates: coordiantes of the airport