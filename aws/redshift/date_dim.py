#!/usr/bin/env python
from __future__ import print_function
import argparse
from datetime import date, timedelta

def date_dim(start_year, num_years):
    d = date(start_year, 1, 1)
    one_day = timedelta(days=1)

    while d.year < start_year + num_years:
        date_dim_row(d)
        d += one_day

def date_dim_row(d):
    date_key    = d.strftime("%Y%m%d")
    full_date   = d.strftime("%Y-%m-%d")
    year_month  = d.strftime("%Y-%m")
    year        = d.year
    month       = d.month
    month_name  = d.strftime("%B")
    month_abbr  = d.strftime("%b")
    day         = d.day
    day_name    = d.strftime("%A")
    day_abbr    = d.strftime("%a")
    day_of_week = d.isoweekday()
    day_of_year = d.timetuple().tm_yday
    week_number = d.isocalendar()[1]

    print(date_key, full_date, year_month, year, month, month_name, month_abbr, day, day_name, day_abbr, day_of_week, day_of_year, week_number, sep='\t')

def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', '--start-year', type=int, required=True, help='')
    parser.add_argument('-n', '--num-years', type=int, required=True, help='')
    args = parser.parse_args()

    date_dim(args.start_year, args.num_years)

if __name__ == "__main__":
    main()
