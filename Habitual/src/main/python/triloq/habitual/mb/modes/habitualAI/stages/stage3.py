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
    print(start_dt, type(start_dt), end_dt, type(end_dt))
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
        temp_end_dt = start_dt + relativedelta(months=+train_period + (test_period-1))
        temp_end_dt = temp_end_dt + relativedelta(day=31)
    print("fetched dates \n\n", dt_list)
    return dt_list


def get_dummies(df: pd.DataFrame, count):
    df = pd.get_dummies(df, columns=['txn_category'], prefix='frame' + str(count))
    return df


def write_data(df: pd.DataFrame, path, format_type):
    if format_type == 'csv':
        df.to_csv(path, index=False)
    elif format_type == "parquet":
        df.to_parquet(path, index=False, engine="fastparquet", compression='gzip')
    else:
        raise Exception("[stage3.py] Invalid file format for saving data")


def frame(df, config):
    try:
        if 'txn_dt' not in df.columns.tolist():
            raise Exception("[stage3.py] txn date not found in dataframe")
    except Exception as e:
        print(e)

    start_dt, end_dt = config["dateWindow"]['windowStart'], config["dateWindow"]['windowEnd']
    train_period, test_period = config['dateWindow']['training_months'], config['dateWindow']['testing_months']
    # start_dt = datetime.datetime.strptime(start_dt, '%Y-%m-%d')
    # end_dt = datetime.datetime.strptime(end_dt, '%Y-%m-%d')
    date_list = get_datelist(start_dt, end_dt, int(train_period), int(test_period))
    df_list = []
    count = 0
    print("Inside frame")
    for i in date_list:
        min_date, max_date = i[0], i[1]
        train_date = i[2]

        df['txn_dt'] = pd.to_datetime(df['txn_dt'], format="%Y-%m-%d")
        df['txn_dt'] = df['txn_dt'].dt.strftime('%Y-%m-%d')

        tempdf = df[df['txn_dt'] >= min_date]
        tempdf = tempdf[tempdf['txn_dt'] <= max_date]

        y_df = df[df['txn_dt'] >= max_date]
        y_df = y_df[y_df['txn_dt'] <= train_date]


        y_df_copy = y_df.copy()
        train_cat_list = tempdf['txn_category'].unique().tolist()
        y_train_cat = y_df['txn_category'].unique().tolist()
        ynew = []
        for i in train_cat_list:
            if i not in y_train_cat:
                ynew.append(i)

        yf_user_list = tempdf[~tempdf['txn_category'].isin(ynew)].user_id.tolist()

        tempdf = tempdf[~tempdf['user_id'].isin(yf_user_list)]

        tempdf = get_dummies(tempdf, count)
        count += 1
        df_list.append((tempdf, y_df))

        path = config['file_output_path']['dataframe_output']
        path = path[:-4]+'_'+str(count)+path[-4:]
        format_type = config['file_output_path']['dataframe_output_format']
        write_data(tempdf, path, format_type)

    return df_list, ynew
