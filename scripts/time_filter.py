'''
将input_file中的视频数据按照时间分类，time_col_name为时间列名
输入输出文件路径、命名待修改
'''
import pandas as pd
import os

input_file = ''

time_col_name = '发表时间'


df = pd.read_csv(input_file)

df[time_col_name] = df[time_col_name].astype(str)

for date in df[time_col_name].unique():
    filtered_df = df[df[time_col_name] == date]

    output_file = f'{date}.csv'

    filtered_df.to_csv(output_file, index=False)
