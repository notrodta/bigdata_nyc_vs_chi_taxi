from pyspark import SparkContext
from pyspark.sql import SparkSession

from functools import reduce

def get_chi_taxi_trip_sec(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[3] != "":
            yield row[3]


def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

#get all the trip miles
def get_chi_taxi_trip_miles(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[4] != "":
            yield row[4]


def get_chi_taxi_trip_avg_sec():
    l = chi_taxi_sec.take(chi_taxi.count())
    return reduce(lambda x, y: float(x) + float(y), l) / len(l)

def get_chi_taxi_avg_mile():
    l = chi_taxi_tripmiles.take(chi_taxi.count())
    return reduce(lambda x, y: float(x) + float(y), l) / len(l)


def get_avg_mile_per_hour_chi():
    avg_trip_sec = get_chi_taxi_trip_avg_sec()
    avg_trip_mile = get_chi_taxi_avg_mile()

    return avg_trip_mile * (3600 / avg_trip_sec)


def get_time_traveled_bracket_chi():
    dict = {'1': 0, '5': 0, '10': 0, '20': 0, '30': 0, '60': 0, '100': 0}
    for sec in chi_taxi_sec.collect():
        sec = int(sec)
        if sec > 0 and sec < 60:  # 1 min
            dict['1'] += 1
        elif sec >= 60 and sec < 300:  # 5min
            dict['5'] += 1
        elif sec >= 300 and sec < 600:  # 10min
            dict['10'] += 1
        elif sec >= 600 and sec < 1200:  # 20min
            dict['20'] += 1
        elif sec >= 1200 and sec < 1800:  # 30min
            dict['30'] += 1
        elif sec >= 1800 and sec < 3600:  # 1hr
            dict['60'] += 1
        elif sec >= 3600:  # more than 1 hour
            dict['100'] += 1

    l = []

    for key, value in dict.items():
        l.append((int(key), int(value)))

    l = sorted(l, key=lambda x: int(x[0]))

    return l


def get_dist_traveled_bracket_chi():

    # dict = {'1': 0, '2':0, '3':0, '4':0, '5':0, '6':0, '7':0, '8':0, '9':0, '10':0, '11':0, '12':0, '13':0, '14':0, '15':0, '16':0, '17':0, '18':0, '19':0, '20':0 }
    dict = {'1': 0, '2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0}

    for miles in chi_taxi_tripmiles.collect():
        miles = float(miles)
        if miles < 1:  # 1 min
            dict['1'] += 1
        elif miles >= 1 and miles < 2:  # 5min
            dict['2'] += 1
        elif miles >= 2 and miles < 3:  # 10min
            dict['3'] += 1
        elif miles >= 4 and miles < 5:  # 10min
            dict['4'] += 1
        elif miles >= 5 and miles < 6:  # 10min
            dict['5'] += 1
        elif miles >= 6 and miles < 7:  # 5min
            dict['6'] += 1
        elif miles >= 7 and miles < 8:  # 10min
            dict['7'] += 1
        elif miles >= 8 and miles < 9:  # 10min
            dict['8'] += 1
        elif miles >= 9 and miles < 10:  # 10min
            dict['9'] += 1
        elif miles >= 10 and miles < 35:
            dict['10'] += 1

    l = []

    for key, value in dict.items():
        l.append((int(key), int(value)))

    l = sorted(l, key=lambda x: int(x[0]))

    return l


def get_distinct_companies_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[15] != "":
            yield row[15]


def get_company_business_record_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[15] != "":
            yield row[15]

def get_company_business_bracket():
    company_business_bracket = {}
    for d in companies_business.collect():
        if d not in company_business_bracket:
            company_business_bracket[d] = 0
        company_business_bracket[d] += 1

    return company_business_bracket


def get_trip_start_time_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[1] != "":
            yield row[1][-8:-6]


def get_taxi_time_start_bracket_chi():
    # time_dict = {'1':0, '2':0, '3':0, '4':0, '5':0, '6':0, '7':0, '8':0, '9':0, '10':0, '11':0, '12':0, '13':0,
    #              '14:'0, '15':0, '16':0, '17':0, '18':0, '19':0, '20':0, '21':0, '22':0, '23':0, '24':0}

    time_dict = {}

    for time in start_time.collect():
        if time not in time_dict:
            time_dict[time] = 0
        time_dict[time] += 1


    l = []
    for key, value in time_dict.items():
        if key != 'im':
            l.append((key, value))

    l = sorted(l, key=lambda x: int(x[0]))
    return l


def get_trip_per_month_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        try:
            if row[1] != "":
                yield row[1].split()[0].split('-')[1]
        except:
            pass


def get_trip_per_month_bracket_chi():
    d = {}

    for trip in trip_per_month.collect():
        if trip not in d:
            d[trip] = 0
        d[trip] += 1

    # print time_dict

    l = []
    for key, value in d.items():
        l.append((key, value))

    l = sorted(l, key=lambda x: int(x[0]))
    return l


#-------
def get_trip_date_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[1] != "":
            try:
                yield row[1].split()[0].split('-')[1] + '-' + row[1].split()[0].split('-')[2]
            except:
                pass


def get_highest_trip_day_chi(num_of_days):
    trip_dates = {}
    for trip in trip_date.collect():
        if trip not in trip_dates:
            trip_dates[trip] = 0
        trip_dates[trip] += 1

    l = []
    for key, value in trip_dates.items():
        l.append((key, value))

    # l = sorted(l,key=itemgetter(1),reverse=True)
    l = sorted(l, key=lambda x: int(x[1]), reverse=True)
    return l[:num_of_days]


def get_lowest_trip_day_chi(num_of_days):
    trip_dates = {}
    for trip in trip_date.collect():
        if trip not in trip_dates:
            trip_dates[trip] = 0
        trip_dates[trip] += 1

    l = []
    for key, value in trip_dates.items():
        l.append((key, value))

    l = sorted(l, key=lambda x: int(x[1]))
    return l[:num_of_days]




if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    sc = SparkContext()

    spark = SparkSession(sc)

    #chi_taxi = sc.textFile('chicago_taxi_trips_2016_01.csv')
    chi_taxi = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_01.csv')

    chi_taxi_sec = chi_taxi.mapPartitionsWithIndex(get_chi_taxi_trip_sec).cache()
    chi_taxi_sec = chi_taxi_sec.filter(lambda x: x.isdigit())
    chi_taxi_sec.take(10)

    chi_taxi_tripmiles = chi_taxi.mapPartitionsWithIndex(get_chi_taxi_trip_miles).cache()
    chi_taxi_tripmiles = chi_taxi_tripmiles.filter(lambda x: is_number(x))
    chi_taxi_tripmiles.take(10)

    print(get_chi_taxi_trip_avg_sec())
    print(get_chi_taxi_avg_mile())
    print(get_avg_mile_per_hour_chi())
    print(get_time_traveled_bracket_chi())
    print(get_dist_traveled_bracket_chi())

    companies = chi_taxi.mapPartitionsWithIndex(get_distinct_companies_chi).cache()
    companies = companies.distinct()
    print(companies.count())
    companies.take(10)

    companies_business = chi_taxi.mapPartitionsWithIndex(get_company_business_record_chi).cache()
    print(companies_business.count())
    companies_business.take(10)

    print(get_company_business_bracket())

    start_time = chi_taxi.mapPartitionsWithIndex(get_trip_start_time_chi).cache()
    # active_drivers = active_drivers.filter(lambda x: x == '2017')
    print(start_time.count())
    print(type(start_time.take(10)[0]))
    start_time.take(10)

    print(get_taxi_time_start_bracket_chi())

    trip_per_month = chi_taxi.mapPartitionsWithIndex(get_trip_per_month_chi).cache()
    trip_per_month.take(10)

    print(get_trip_per_month_bracket_chi())

    average_trip_a_day = chi_taxi.count() / 366
    print(average_trip_a_day)

    trip_date = chi_taxi.mapPartitionsWithIndex(get_trip_date_chi).cache()
    trip_date.take(10)

    print(get_highest_trip_day_chi(5))
    print(get_lowest_trip_day_chi(5))

    print("success")


