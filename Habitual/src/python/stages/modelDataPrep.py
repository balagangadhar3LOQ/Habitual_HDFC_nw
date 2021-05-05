import sys

import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from src.python.stages.log_writer import write_log
import warnings

warnings.filterwarnings(action="ignore")


def get_datelist(config):
    write_log("Getting Dates Frame...")
    start_dt, end_dt = config["dateWindow"]['windowStart'], config["dateWindow"]['windowEnd']
    start_dt = start_dt[:-2] + '01'
    if end_dt[5:-3] in ['01', '03', '05', '07', '08', '10', '12']:
        end_dt = end_dt[:8] + '31'
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
    print(dt_list)
    return dt_list


def newcat(x):
    x_catlist = x['uniq_cat']
    y_catlist = x['y_uniq_cat']
    templist = []
    for i in y_catlist:
        if i not in x_catlist:
            templist.append(i)
    if len(templist) == 0:
        return np.nan
    return list(templist)


def get_dummies(df: pd.DataFrame, count):
    df = pd.get_dummies(df, columns=['txn_category'], prefix='frame' + str(count))
    return df


def get_active(df: pd.DataFrame, config):
    active_cat = list(config['target_billpay_category'])
    df = df[df.txn_category.isin(active_cat)]
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
    import sys
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

        #special functionality
        sorted_list = sorted(set(tempdf.txn_dt.str[:7].to_list()))
        months = ['m' + str(i + 1) for i in range(len(sorted_list))]
        my_dict = dict(zip(sorted_list, months))
        tempdf["month"] = tempdf["txn_dt"].str[:7].apply(lambda x: my_dict.get(x))
        tempdf.sort_values(by=['month'], inplace=True)
        tempdf['month_cat'] = tempdf['month'] + '_' + tempdf['txn_category']

        # tempdf['total_txn_count'] = tempdf.groupby(['user_id', ])['txn_amt'].transform('count')
        # tempdf['total_txn_amt'] = tempdf.groupby(['user_id', ])['txn_amt'].transform('sum')
        tempdf['txn_cat_list'] = tempdf.groupby(['user_id', 'txn_dt'])['txn_category'].transform(lambda x: ','.join(x))
        tempdf['month_txn_cat_list'] = tempdf.groupby(['user_id', 'txn_dt'])['month_cat'].transform(lambda x: ','.join(x))


        # y_df['total_txn_count'] = y_df.groupby(['user_id', ])['txn_amt'].transform('count')
        # y_df['total_txn_amt'] = y_df.groupby(['user_id', ])['txn_amt'].transform('sum')
        y_df['txn_cat_list'] = y_df.groupby(['user_id', 'txn_dt'])['txn_category'].transform(lambda x: ','.join(x))

        tempdf['txn_cat_list'] = tempdf['txn_cat_list'].astype(str)
        tempdf['x_cat_split'] = tempdf['txn_cat_list'].str.split(",")
        tempdf['x_cat_n_unique'] = tempdf['x_cat_split'].apply(lambda x: len(set(x)))
        tempdf['active'] = 1

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


def get_x_data(df: pd.DataFrame):
    write_log("Getting X Data...")
    df = df[['user_id', 'txn_dt', 'txn_amt', 'txn_category', 'txn_subcategory', 'ri', 'si', 'active', 'x_cat_split', 'month_txn_cat_list', 'month_cat']]

    # sorted_list = sorted(set(df.txn_dt.str[:7].to_list()))
    # months = ['m' + str(i + 1) for i in range(len(sorted_list))]
    # my_dict = dict(zip(sorted_list, months))
    # df["month"] = df["txn_dt"].str[:7].apply(lambda x: my_dict.get(x))
    # df.sort_values(by=['month'],inplace=True)
    # df['month_cat'] = df['month'] + '_' + df['txn_category']

    df['x_cat_split'] = df['x_cat_split'].astype(str)
    df['uniq_cat'] = df['x_cat_split'].apply(
        lambda x: list(set(x.replace("['", '').replace("']", '').replace("'", "").split(','))))
    df['uniq_cat'] = df['uniq_cat'].apply(lambda x: list(set([i.strip() for i in x])))
    df.drop(columns=['x_cat_split'], inplace=True)

    df['month_txn_cat_list'] = df['month_txn_cat_list'].astype(str)
    df['month_uniq_cat'] = df['month_txn_cat_list'].apply(
        lambda x: list(set(x.replace("['", '').replace("']", '').replace("'", "").split(','))))
    df['month_uniq_cat'] = df['month_uniq_cat'].apply(lambda x: list(set([i.strip() for i in x])))
    df.drop(columns=['month_txn_cat_list'], inplace=True)



    return df


def get_y_data(df: pd.DataFrame, ds2: pd.DataFrame):
    write_log("MDP Gettting Y Data")
    df['y_cat_split'] = df['y_cat_split'].astype(str)
    ds2['uniq_cat'] = ds2['uniq_cat'].astype(str)
    df['y_cat_split'] = df['y_cat_split'].apply(
        lambda x: list(set(x.replace("['", '').replace("']", '').replace("'", "").split(','))))
    ds2['uniq_cat'] = ds2['uniq_cat'].apply(
        lambda x: list(set(x.replace("['", '').replace("']", '').replace("'", "").split(','))))
    ds2['uniq_cat'] = ds2['uniq_cat'].apply(lambda x: list(set([i.strip() for i in x])))
    df['y_uniq_cat'] = df['y_cat_split'].apply(lambda x: list(set([i.strip() for i in x])))
    semi_df = pd.merge(ds2, df[['user_id', 'y_uniq_cat']], on='user_id', how='left')
    semi_df.dropna(inplace=True)  # removing null values after merging
    semi_df['new_cat'] = semi_df[['uniq_cat','y_uniq_cat']].apply(lambda x: newcat(x), axis=1)
    semi_df.dropna(inplace=True)  # removing null values for no new categories
    write_log("Removing temp variable for space...")
    del df, ds2
    return semi_df


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

    for i in range(len(tempdf_list)):
        write_log("[modelDataPrep.py] creating attributes...")

        df = get_x_data(tempdf_list[i])
        tempdf_ds2 = df[['user_id', 'uniq_cat']]  # required for y_lable

        # for verifying category_binarization
        xs = set()
        xs_temp = df['uniq_cat'].apply(lambda x: [xs.add(i) for i in x])
        write_log(f"[ModelDataPrep]. {i + 1} No of Categories Available are :" + str(len(xs)))

        # category binarization
        from sklearn.preprocessing import MultiLabelBinarizer
        mlb = MultiLabelBinarizer(sparse_output=True)
        dfs = df.join(
            pd.DataFrame.sparse.from_spmatrix(
                mlb.fit_transform(df.pop('month_uniq_cat')),
                index=df.index,
                columns=mlb.classes_))
        dfs.drop_duplicates(subset=['user_id'], inplace=True)
        # verifying category_binarization
        write_log(f"[ModelDataPrep]. {i+1} Shape of binary Categorization " + str(dfs.shape))
        write_data(tempdf_ds2, f'output/datagen/DataSource2_{i}.csv', 'csv')
        write_data(dfs, f'output/feature_dataset/Features_Dataset_{i}.csv', "csv")

        write_log("Deleting the df for space...")
        del df

        ydf = get_y_data(ydf_list[i], tempdf_ds2)
        write_data(ydf, f"output/ylabel_data/YLabel_{i}.csv", "csv")

        tempdf = pd.merge(dfs, ydf[['user_id', 'new_cat']], on='user_id', how='left')

        write_log("Total Number of customers " + str(tempdf.shape))
        tempdf.dropna(axis=0, inplace=True)
        write_log("After Removing customers" + str(tempdf.shape))

        tempdf['new_cat'] = tempdf['new_cat'].astype(str)
        tempdf['new_cat'] = tempdf['new_cat'].apply(lambda x: list(set(x.replace("['", '').replace("']", '').replace("'", "").split(','))))
        tempdf['new_cat'] = tempdf['new_cat'].apply(lambda x: list(set([i.strip() for i in x])))
        tempdf = tempdf.explode('new_cat')
        write_data(tempdf,f"output/stage3/MDP_part_{i}","csv")
        tempdf.to_csv("/home/bala/Desktop/Temp.csv",index=False)
        tempdf_final.append(tempdf)
    write_log("End of Model Data Preparation Stage...")
    return tempdf_final
