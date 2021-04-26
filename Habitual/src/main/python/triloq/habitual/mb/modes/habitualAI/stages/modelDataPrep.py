import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from src.main.python.triloq.habitual.mb.modes.habitualAI.stages.log_writer import write_log


def get_datelist(config):
    write_log("Getting Dates Frame...")
    start_dt, end_dt = config["dateWindow"]['windowStart'], config["dateWindow"]['windowEnd']
    train_period, test_period = int(config['dateWindow']['training_months']), int(
        config['dateWindow']['testing_months'])
    try:
        if (type(train_period) != int) or (type(test_period) != int):
            raise Exception("Invalid train period and test period type")
        elif test_period > train_period:
            raise Exception(" Test period is higher than train period ")
    except Exception as e:
        write_log(e)
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
        temp_end_dt = start_dt + relativedelta(months=+train_period + (test_period - 1))
        temp_end_dt = temp_end_dt + relativedelta(day=31)
    write_log("fetched dates \n\n" + str(dt_list))
    return dt_list


def get_dummies(df: pd.DataFrame, count):
    df = pd.get_dummies(df, columns=['txn_category'], prefix='frame' + str(count))
    return df


def write_data(df: pd.DataFrame, path, format_type):
    try:
        if format_type == 'csv':
            df.to_csv(path, index=False)
        elif format_type == "parquet":
            df.to_parquet(path, index=False, engine="fastparquet", compression='gzip')
        else:
            raise Exception("[stage3.py] Invalid file format for saving data")
    except Exception as e:
        write_log(e)
        print(e)


def get_frame_datasets(df, config):
    date_list = get_datelist(config)
    tempdf_list: list = []
    ydf_list: list = []

    for i in date_list:
        min_date, max_date = i[0], i[1]
        train_date = i[2]
        after_start_date = df["txn_dt"] >= min_date
        before_end_date = df["txn_dt"] <= max_date
        between_two_dates = after_start_date & before_end_date
        tempdf = df.loc[between_two_dates]

        y_after_start_date = df["txn_dt"] > max_date
        y_before_end_date = df["txn_dt"] <= train_date
        y_between_two_dates = y_after_start_date & y_before_end_date
        y_df = df.loc[y_between_two_dates]

        tempdf['total_txn_count'] = tempdf.groupby(['user_id', ])['txn_amt'].transform('count')
        tempdf['total_txn_amt'] = tempdf.groupby(['user_id', ])['txn_amt'].transform('sum')
        tempdf['txn_cat_list'] = tempdf.groupby(['user_id', 'txn_dt'])['txn_category'].transform(lambda x: ','.join(x))

        y_df['total_txn_count'] = y_df.groupby(['user_id', ])['txn_amt'].transform('count')
        y_df['total_txn_amt'] = y_df.groupby(['user_id', ])['txn_amt'].transform('sum')
        y_df['txn_cat_list'] = y_df.groupby(['user_id', 'txn_dt'])['txn_category'].transform(lambda x: ','.join(x))

        print(tempdf.head(2))
        tempdf['txn_cat_list'] = tempdf['txn_cat_list'].astype(str)
        tempdf['x_cat_split'] = tempdf['txn_cat_list'].str.split(",")
        tempdf['x_cat_n_unique'] = tempdf['x_cat_split'].apply(lambda x: len(set(x)))

        y_df['txn_cat_list'] = y_df['txn_cat_list'].astype(str)
        y_df['y_cat_split'] = y_df['txn_cat_list'].str.split(",")
        y_df['y_cat_n_unique'] = y_df['y_cat_split'].apply(lambda x: len(set(x)))

        tempdf = tempdf.drop(columns=['txn_cat_list'])
        y_df = y_df.drop(columns=['txn_cat_list'])

        tempdf = tempdf.drop_duplicates(subset=['user_id'])
        y_df = y_df.drop_duplicates(subset=['user_id'])

        tempdf_list.append(tempdf)
        ydf_list.append(y_df)
    return tempdf_list, ydf_list


def get_active(df: pd.DataFrame, config):
    active_cat = list(config['target_billpay_category'])
    df = df[df.txn_category.isin(active_cat)]
    return df


def model_data_prep(df: pd.DataFrame, config):
    write_log("Model Data Preparation... ")
    try:
        if 'txn_dt' not in df.columns.tolist():
            raise Exception("[stage3.py] txn date not found in dataframe")
    except Exception as e:
        write_log(e)
        print(e)

    # Uncomment below line when the active or bill-pay categories are defined
    # df = get_active(df, config)

    tempdf_list, ydf_list = get_frame_datasets(df, config)
    tempdf_final = []

    def newcat(x):
        x_catlist = x.x_cat_split
        y_catlist = x.y_cat_split
        templist = []
        for i in y_catlist:
            if i not in x_catlist:
                templist.append(i)
        if len(templist) == 0:
            return np.nan
        return list(templist)

    for i in range(len(tempdf_list)):
        print("[modelDataPrep.py] creating attributes...")
        ydf_list[i].rename(columns={"cat_n_unique": "y_cat_n_unique", "cat_split": "y_cat_split"}, inplace=True)
        tempdf = pd.merge(tempdf_list[i], ydf_list[i][["user_id", 'y_cat_n_unique', 'y_cat_split']], on='user_id',
                          how='left')
        tempdf = tempdf.dropna(subset=['y_cat_split', 'x_cat_split'], axis=0)
        tempdf["new_cat"] = tempdf.apply(lambda x: newcat(x), axis=1)
        # tempdf = tempdf.dropna(subset=['new_cat'],axis=0)
        tempdf['new_cat_available'] = tempdf['new_cat'].apply(lambda x: 1 if x is not np.nan else 0)
        tempdf['new_cat_count'] = tempdf['new_cat'].apply(lambda x: len(x) if type(x) == list else 0)
        tempdf = tempdf.explode('new_cat').reset_index(drop=True)
        tempdf = tempdf[['user_id', 'txn_dt', 'txn_amt', 'txn_category',
                         'txn_subcategory', 'ri', 'si', 'total_txn_count', 'total_txn_amt',
                         'txn_cat_list', 'cat_split', 'cat_n_unique', 'y_cat_n_unique',
                         'y_cat_split', 'new_cat', 'new_cat_available', 'new_cat_count']]
        tempdf_final.append(tempdf)

    write_log("End of Model Data Preparation Stage...")
    return tempdf_final

# def frame(df: pd.DataFrame, config):
#     try:
#         if 'txn_dt' not in df.columns.tolist():
#             raise Exception("[stage3.py] txn date not found in dataframe")
#     except Exception as e:
#         print(e)
#
#     start_dt, end_dt = config["dateWindow"]['windowStart'], config["dateWindow"]['windowEnd']
#     train_period, test_period = config['dateWindow']['training_months'], config['dateWindow']['testing_months']
#     # start_dt = datetime.datetime.strptime(start_dt, '%Y-%m-%d')
#     # end_dt = datetime.datetime.strptime(end_dt, '%Y-%m-%d')
#     date_list = get_datelist(start_dt, end_dt, int(train_period), int(test_period))
#     df_list = []
#     count = 0
#     print("Inside frame")
#     for i in date_list:
#         min_date, max_date = i[0], i[1]
#         train_date = i[2]
#
#         df['txn_dt'] = pd.to_datetime(df['txn_dt'], format="%Y-%m-%d")
#         df['txn_dt'] = df['txn_dt'].dt.strftime('%Y-%m-%d')
#
#         # print("TEMPDF IN STAGE3 \n", df.head(2))
#
#         min_date = datetime.datetime.strptime(min_date, "%Y-%m-%d")
#         max_date = datetime.datetime.strptime(max_date, "%Y-%m-%d")
#         train_date = datetime.datetime.strptime(train_date, "%Y-%m-%d")
#
#         print(type(min_date))
#         tempdf = df[df['txn_dt'] >= min_date]
#         tempdf = tempdf[tempdf['txn_dt'] <= max_date]
#
#         y_df = df[df['txn_dt'] >= max_date]
#         y_df = y_df[y_df['txn_dt'] <= train_date]
#
#         print("TEMPDF in 3 \n", tempdf.head(2))
#         y_df_copy = y_df.copy()
#         train_cat_list = tempdf['txn_category'].unique().tolist()
#         y_train_cat = y_df['txn_category'].unique().tolist()
#         ynew = []
#         for i in train_cat_list:
#             if i not in y_train_cat:
#                 ynew.append(i)
#
#         yf_user_list = tempdf[~tempdf['txn_category'].isin(ynew)].user_id.tolist()
#
#         tempdf = tempdf[~tempdf['user_id'].isin(yf_user_list)]
#
#         tempdf = get_dummies(tempdf, count)
#         print("TEMPDF in D \n", tempdf.head(2))
#
#         count += 1
#         df_list.append((tempdf, y_df))
#
#         path = config['file_output_path']['dataframe_output']
#         path = path[:-4] + '_' + str(count) + path[-4:]
#         format_type = config['file_output_path']['dataframe_output_format']
#
#         write_data(tempdf, path, format_type)
#
#     return df_list, ynew
