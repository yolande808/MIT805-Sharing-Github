from mrjob.job import MRJob
#import pandas as pd
#import locale
#locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


class MRPassengerAvgByMonth(MRJob):

    def mapper(self, _, line):
        (tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,pick_up_date_time,pick_up_month,pick_up_day,pick_up_hour,drop_off_date_time,trip_duration) = line.split(',')
    
        yield pick_up_month, float(passenger_count)

    def reducer(self, pick_up_month, passenger_count):
        total = 0
        numElements = 0
        for x in passenger_count:
            total += x
            numElements += 1
            
        yield pick_up_month, total / numElements


if __name__ == '__main__':
    MRPassengerAvgByMonth.run()

