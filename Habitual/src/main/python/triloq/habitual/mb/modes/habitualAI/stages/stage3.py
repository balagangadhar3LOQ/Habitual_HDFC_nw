import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta


def get_datelist(start_dt, end_dt, train_period, test_period):
    try:
        if (type(train_period) != int) or (type(test_period) != int):
            raise Exception("Invalid train period and test period type")
        elif test_period > train_period:
            raise Exception(" Test period is higher than train period ")
    except Exception as e:
        print(e)
    dt_list, ymonth_list = [], []
    start_dt = datetime.datetime.strptime(start_dt, '%Y-%m-%d')
    end_dt = datetime.datetime.strptime(end_dt, '%Y-%m-%d')
    temp_end_dt = start_dt + relativedelta(months=+(train_period + test_period - 1))
    temp_end_dt = temp_end_dt + relativedelta(day=31)

    while temp_end_dt <= end_dt:
        ymonth_temp = start_dt + relativedelta(months=+(train_period - 1))
        ymonth_temp = ymonth_temp + relativedelta(day=31)
        dt_list.append(
            (start_dt.strftime('%Y-%m-%d'), ymonth_temp.strftime('%Y-%m-%d'), temp_end_dt.strftime('%Y-%m-%d')))
        start_dt = start_dt + relativedelta(months=+1)
        temp_end_dt = start_dt + relativedelta(months=+train_period + 1)
        temp_end_dt = temp_end_dt + relativedelta(day=31)
    for i in dt_list:
        print(i)


#    return dt_list

def get_dummies(df: pd.DataFrame, count):
    df = pd.get_dummies(df, columns=['category'], prefix='frame' + str(count))
    return df


def frame(df, config):
    try:
        if 'txn_dt' not in df.columns.tolist():
            raise Exception("[stage3.py] txn date not found in dataframe")
    except Exception as e:
        print(e)

    start_dt, end_dt = config["dateWindow"]['windowStart'], config["dateWindow"]['windowEnd']
    train_period, test_period = config['dateWindow']['training_months'], config['dateWindow']['testing_months']
    start_dt = datetime.datetime.strptime(start_dt, '%Y-%m-%d')
    end_dt = datetime.datetime.strptime(end_dt, '%Y-%m-%d')
    date_list = get_datelist(start_dt, end_dt, int(train_period), int(test_period))
    df_list = []
    count = 1
    for i in date_list:
        min_date, max_date = i[0], i[1]
        train_date = i[2]
        tempdf = df[df['txn_dt'] >= min_date & df['txn_dt'] <= max_date]
        y_df = df[df['txn_dt'] >= max_date & df['txn_dt'] <= train_date]
        y_df_copy = y_df.copy()
        train_cat_list = tempdf['category'].unique().tolist()
        y_train_cat = y_df['category'].unique().tolist()
        ynew = []
        for i in train_cat_list:
            if i not in y_train_cat:
                ynew.append(i)
        for i in ynew:
            y_df = y_df[y_df['category'] == str(i)]

        tempdf = get_dummies(tempdf, count)
        count += 1
        df_list.append((tempdf, y_df))

        return df_list, ynew
