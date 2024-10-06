'''
除ip之外的数据在input_file中，ip数据在user_info_file中，此情况下将视频数据按ip分类，user_col_name为两文件共同列名（用户id），ip_col_name为ip信息列名
输入输出文件路径、命名待修改
'''
import pandas as pd
import os

input_file = ''
user_info_file = ''

user_col_name = '用户id'
ip_col_name = 'IP属地'

data_df = pd.read_csv(input_file)
user_infos_df = pd.read_csv(user_info_file)

if user_col_name not in data_df.columns or user_col_name not in user_infos_df.columns:
    raise ValueError(f'两份CSV文件中必须同时包含 {user_col_name} 列')

if ip_col_name not in user_infos_df.columns:
    raise ValueError(f'{user_info_file} 中必须包含 {ip_col_name} 列')

merged_df = pd.merge(data_df, user_infos_df[[user_col_name, ip_col_name]], on=user_col_name, how='left')

grouped = merged_df.groupby(ip_col_name)

for ip_location, group in grouped:
    sanitized_ip = ip_location.replace('/', '_').replace('\\', '_').replace(':', '_')
    filename = f"{sanitized_ip}.csv"
    output_path = filename
    group.to_csv(output_path, index=False)
