'''
时间数据和ip数据都在input_file中，此情况下将视频数据按时间和ip分类
time_col_name为时间信息列名，ip_col_name为ip信息列名
输入输出文件路径、命名待修改
'''

import pandas as pd
import os

input_file = ''

time_col_name = '发表时间'
ip_col_name = 'IP属地'


df = pd.read_csv(input_file)

df[time_col_name] = pd.to_datetime(df[time_col_name], format='%Y.%m.%d')

grouped = df.groupby([df[time_col_name].dt.strftime('%Y.%m.%d'), ip_col_name])

for (time, ip_location), group in grouped:
    filename = f"{time}_{ip_location}.csv"
    output_path = filename
    group.to_csv(output_path, index=False)
