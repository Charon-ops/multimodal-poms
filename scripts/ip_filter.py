'''
将input_file中的视频数据按照ip分类，ip_col_name为ip信息列名
输入输出文件路径、命名待修改
'''

import pandas as pd
import os

input_file = ''

ip_col_name = 'IP属地'


df = pd.read_csv(input_file)

grouped = df.groupby(ip_col_name)

for ip_location, group in grouped:
    sanitized_ip = ip_location.replace('/', '_').replace('\\', '_').replace(':', '_')
    filename = f"{sanitized_ip}.csv"
    output_path = filename
    group.to_csv(output_path, index=False)
