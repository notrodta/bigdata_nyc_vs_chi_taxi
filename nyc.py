import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession

from functools import reduce
from operator import add


def convert_to_military_time_NoMonthAndYear(time):
    from datetime import datetime
    return datetime.strptime(time, '%I:%M:%S %p').strftime('%H')

def convert_to_military_time(time):
    from datetime import datetime
    return datetime.strptime(time, '%m/%d/%Y %I:%M:%S %p').strftime('%H')

def convert_to_military_time_Min_Sec(time):
    from datetime import datetime
    return datetime.strptime(time, '%I:%M:%S %p').strftime('%H:%M:%S')


def get_sec_diff(_time1, _time2):
    from datetime import datetime
    import time

    time1 = convert_to_military_time_Min_Sec(_time1)
    time2 = convert_to_military_time_Min_Sec(_time2)
    t1 = time.mktime(time.strptime(time1, "%H:%M:%S"))
    t2 = time.mktime(time.strptime(time2, "%H:%M:%S"))
    diff = t2 - t1
    return diff


def get_nyc_taxi_trip_datetime(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[1] != "" and row[2] != "":

            time1 = row[1].split()[1] + " " + row[1].split()[2]
            time2 = row[2].split()[1] + " " + row[2].split()[2]
            yield get_sec_diff(time1, time2)


def get_trip_start_time_nyc(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[1] != "":
            try:
                time = convert_to_military_time_NoMonthAndYear(row[1][-11:])
                yield time
            except:
                pass


def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def get_nyc_taxi_trip_miles(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[10] != "":
            try:
                yield row[10]
            except:
                pass


def get_nyc_avg_mile():
    return (nyc_taxi_tripmiles.reduce(lambda accum, n: float(accum) + float(n))) / 16385533



def time_bracket_bracket(sec):
    sec = int(sec)
    if sec > 0 and sec < 60:  # 1 min
        sec = 1
    elif sec >= 60 and sec < 300:  # 5min
        sec = 5
    elif sec >= 300 and sec < 600:  # 10min
        sec = 10
    elif sec >= 600 and sec < 1200:  # 20min
        sec = 20
    elif sec >= 1200 and sec < 1800:  # 30min
        sec = 30
    elif sec >= 1800 and sec < 3600:  # 1hr
        sec = 60
    elif sec >= 3600:  # more than 1 hour
        sec = 100

    return sec


def get_time_traveled_bracket_nyc():
    dates = nyc_taxi_sec.map(lambda row: (time_bracket_bracket(row), 1)).reduceByKey(add)
    dates = dates.collect()
    dates = filter(lambda x: x[0].isdigit(), dates)
    dates = sorted(dates, key=lambda x: int(x[0]))
    return dates



def dist_traveled_bracket(m):
    miles = float(m)
    if miles < 1:  # 1 min
        miles = 1
    elif miles >= 1 and miles < 2:  # 5min
        miles = 2
    elif miles >= 2 and miles < 3:  # 10min
        miles = 3
    elif miles >= 4 and miles < 5:  # 10min
        miles = 4
    elif miles >= 5 and miles < 6:  # 10min
        miles = 5
    elif miles >= 6 and miles < 7:  # 5min
        miles = 6
    elif miles >= 7 and miles < 8:  # 10min
        miles = 7
    elif miles >= 8 and miles < 9:  # 10min
        miles = 8
    elif miles >= 9 and miles < 10:  # 10min
        miles = 9
    elif miles >= 10 and miles < 35:
        miles = 10
    else:
        miles = 9999

    return int(miles)


def get_dist_traveled_bracket_nyc():
    dates = nyc_taxi_tripmiles.map(lambda row: (dist_traveled_bracket(row), 1)).reduceByKey(add)
    dates = dates.collect()
    dates = filter(lambda x: x[0].isdigit(), dates)
    dates = sorted(dates, key=lambda x: int(x[0]))
    return dates


    # from operator import itemgetter
    #
    # # dict = {'1': 0, '2':0, '3':0, '4':0, '5':0, '6':0, '7':0, '8':0, '9':0, '10':0, '11':0, '12':0, '13':0, '14':0, '15':0, '16':0, '17':0, '18':0, '19':0, '20':0 }
    # mydict = {'1': 0, '2': 0, '3': 0, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0}
    #
    # for miles in nyc_taxi_tripmiles.collect():
    #     miles = float(miles)
    #     if miles < 1:  # 1 min
    #         mydict['1'] += 1
    #     elif miles >= 1 and miles < 2:  # 5min
    #         mydict['2'] += 1
    #     elif miles >= 2 and miles < 3:  # 10min
    #         mydict['3'] += 1
    #     elif miles >= 4 and miles < 5:  # 10min
    #         mydict['4'] += 1
    #     elif miles >= 5 and miles < 6:  # 10min
    #         mydict['5'] += 1
    #     elif miles >= 6 and miles < 7:  # 5min
    #         mydict['6'] += 1
    #     elif miles >= 7 and miles < 8:  # 10min
    #         mydict['7'] += 1
    #     elif miles >= 8 and miles < 9:  # 10min
    #         mydict['8'] += 1
    #     elif miles >= 9 and miles < 10:  # 10min
    #         mydict['9'] += 1
    #     elif miles >= 10 and miles < 35:
    #         mydict['10'] += 1
    #
    # l = []
    #
    # for key, value in mydict.items():
    #     l.append((int(key), int(value)))
    #
    # l = sorted(l, key=itemgetter(0))
    #
    # return l


def get_taxi_time_start_bracket_nyc():
    # time_dict = {'1':0, '2':0, '3':0, '4':0, '5':0, '6':0, '7':0, '8':0, '9':0, '10':0, '11':0, '12':0, '13':0,
    #              '14:'0, '15':0, '16':0, '17':0, '18':0, '19':0, '20':0, '21':0, '22':0, '23':0, '24':0}

    dates = start_time_NYC.map(lambda row: (row, 1)).reduceByKey(add)
    dates = dates.collect()
    dates = filter(lambda x: x[0].isdigit(), dates)
    dates = sorted(dates, key=lambda x: int(x[0]))
    return dates

    #
    # time_dict = {}
    #
    # for time in start_time_NYC.collect():
    #     if time not in time_dict:
    #         time_dict[time] = 0
    #     time_dict[time] += 1
    #
    #
    # l = []
    # for key, value in time_dict.items():
    #     l.append((key, value))
    #
    # # l = sorted(l,key=itemgetter(0))
    # l = sorted(l, key=lambda x: int(x[0]))
    # return l


def get_trip_per_month_nyc(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[1] != "":
            try:
                yield int(row[1].split()[0].split('/')[0])
            except:
                pass


def get_trip_per_month_bracket_nyc():
    trip_per_month = trip_per_month_nyc.map(lambda row: (row, 1)).reduceByKey(add)
    trip_per_month = trip_per_month.collect()
    trip_per_month = filter(lambda x: x[0].isdigit(), trip_per_month)
    trip_per_month = sorted(trip_per_month, key=lambda x: int(x[0]))
    return trip_per_month

    # from operator import itemgetter
    # d = {}
    #
    # for trip in trip_per_month_nyc.collect():
    #     if trip not in d:
    #         d[trip] = 0
    #     d[trip] += 1
    #
    #
    # l = []
    # for key, value in d.items():
    #     l.append((key, value))
    #
    # l = sorted(l, key=lambda x: int(x[0]))  # sorted(l2, key=lambda x: int(x[0]))sorted(l,key=itemgetter(0))
    # return l


def get_trip_date_nyc(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[1] != "":
            try:
                yield str(int(row[1].split()[0].split('/')[0])) + '-' + str(int(row[1].split()[0].split('/')[1]))
            except:
                pass


def get_highest_trip_day_nyc(num_of_days):
    dates = trip_date_nyc.map(lambda row: (row, 1)).reduceByKey(add)
    dates = dates.collect()
    dates = filter(lambda x: x[0].isdigit(), dates)
    dates = sorted(dates, key=lambda x: int(x[1]), reverse=True)
    return dates


    # trip_date = {}
    # for trip in trip_date_nyc.collect():
    #     if trip not in trip_date:
    #         trip_date[trip] = 0
    #     trip_date[trip] += 1
    #
    # l = []
    # for key, value in trip_date.items():
    #     l.append((key, value))
    #
    # # l = sorted(l,key=itemgetter(1),reverse=True)
    # l = sorted(l, key=lambda x: int(x[1]), reverse=True)
    # return l[:num_of_days]


def get_lowest_trip_day_nyc(num_of_days):
    dates = trip_date_nyc.map(lambda row: (row, 1)).reduceByKey(add)
    dates = dates.collect()
    dates = filter(lambda x: x[0].isdigit(), dates)
    dates = sorted(dates, key=lambda x: int(x[1]))
    return dates


    # trip_date = {}
    # for trip in trip_date_nyc.collect():
    #     if trip not in trip_date:
    #         trip_date[trip] = 0
    #     trip_date[trip] += 1
    #
    # l = []
    # for key, value in trip_date.items():
    #     l.append((key, value))
    #
    # l = sorted(l, key=lambda x: int(x[1]))
    # return l[:num_of_days]



if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    sc = SparkContext()

    spark = SparkSession(sc)

    #nyc_taxi = sc.textFile('1000lines.csv').cache()
    nyc_taxi = sc.textFile('/user/tlee000/2016_Green_Taxi_Trip_Data.csv')


    nyc_taxi_sec = nyc_taxi.mapPartitionsWithIndex(get_nyc_taxi_trip_datetime).cache()
    nyc_taxi_sec.take(10)

    start_time_NYC = nyc_taxi.mapPartitionsWithIndex(get_trip_start_time_nyc).cache()
    start_time_NYC.take(10)

    nyc_taxi_tripmiles = nyc_taxi.mapPartitionsWithIndex(get_nyc_taxi_trip_miles)
    nyc_taxi_tripmiles = nyc_taxi_tripmiles.filter(lambda x: is_number(x))
    nyc_taxi_tripmiles.take(10)

    print("nyc avg miles: ", get_nyc_avg_mile())

    time_traveled_info_nyc = get_time_traveled_bracket_nyc()
    print("time traveled info: ", time_traveled_info_nyc)

    nyc_taxi_tripmiles_nyc = get_dist_traveled_bracket_nyc()
    print(nyc_taxi_tripmiles_nyc)

    nyc_time_bracket = start_time_NYC.map(lambda row: (row, 1)).reduceByKey(add)
    nyc_time_bracket.take(1)

    print(get_taxi_time_start_bracket_nyc())

    trip_per_month_nyc = nyc_taxi.mapPartitionsWithIndex(get_trip_per_month_nyc).cache()
    trip_per_month_nyc.take(10)

    print(get_trip_per_month_bracket_nyc())

    print("average_trip_a_day_chi = ", nyc_taxi.count()/366)

    trip_date_nyc = nyc_taxi.mapPartitionsWithIndex(get_trip_date_nyc).cache()
    trip_date_nyc.take(10)

    print(get_highest_trip_day_nyc(5))

    print(get_lowest_trip_day_nyc(5))

    print("success")