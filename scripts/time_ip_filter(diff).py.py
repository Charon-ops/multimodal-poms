'''
时间数据在input_file中，ip数据在user_info_file中，此情况下将视频数据按时间和ip分类
time_col_name为时间信息列名，ip_col_name为ip信息列名，user_col_name为两文件共同列名（用户id）
输入输出文件路径、命名待修改
'''

import pandas as pd
import os

input_file = ''
user_info_file = ''

time_col_name = '发表时间'
user_col_name = '用户id'
ip_col_name = 'IP属地'


user_info_df = pd.read_csv(user_info_file)

df = pd.read_csv(input_file)

df[time_col_name] = df[time_col_name].astype(str)
df[user_col_name] = df[user_col_name].astype(str)

for date in df[time_col_name].unique():
    filtered_df = df[df[time_col_name] == date]

    merged_df = filtered_df.merge(user_info_df[[user_col_name, ip_col_name]], on=user_col_name, how='left')

    for ip_location in merged_df[ip_col_name].unique():
        ip_filtered_df = merged_df[merged_df[ip_col_name] == ip_location]

        output_file = f'{date}_{ip_location}.csv'

        ip_filtered_df.to_csv(output_file, index=False)
