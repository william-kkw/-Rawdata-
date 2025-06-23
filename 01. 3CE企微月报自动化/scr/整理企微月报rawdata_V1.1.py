import pandas as pd
import os
from colorama import init, Fore, Style
import sys

init(autoreset=True)

'''
 1. 运行前先调整下面的参数
 2. input_path的路径为含有BA List信息的Excel文件路径, 运行完毕后会把输出结果放到这个路径所在的文件夹;
 3. raw_data_excel的路径为数厂出数的.xlsx文件的路径;
'''
##-------------------------------------------运行前需要手工调整的参数-------------------------------------------## 
input_path = r""
raw_data_excel = r""
p = os.path.dirname(input_path)
output_path = os.path.join(p,
                            '3CE企微Monthly Report_去重名单.xlsx'
                            )
# 以下是BA List文件里包含的信息, 与柜台相关放在Counters, 与BA相关放在BA_Codes, 与中智Code相关放在af_BA_Codes
Counters = ["CNT_LOCALNAME", "CNT_CODE", "CNT_DEAL_NO", "门店习惯称呼", "所属区域", "区域主管", "城市", "培训老师"]
BA_Codes = ["BA_name", "Counter_name", "BA_code", "Em_status"]
af_BA_Codes = ["af_ba_name", "af_ba_code"]
##------------------------------------------------------------------------------------------------------------##


# Step1: 读取BA_List
type_dict = dict(CNT_CODE=str, CNT_DEAL_NO=str, BA_code=str, af_ba_code=str)
df_BA_List = pd.read_excel(input_path,
                           dtype=type_dict)

try:
    df_Counters = df_BA_List[Counters].dropna(how="all")
    df_BA_Codes = df_BA_List[BA_Codes].dropna(how="all")
    df_af_BA_Codes = df_BA_List[af_BA_Codes].dropna(how="all")

    df_t_to_m = df_Counters[["CNT_CODE", "CNT_DEAL_NO"]]
    dict_t_to_m = df_t_to_m.set_index('CNT_DEAL_NO')["CNT_CODE"].to_dict()

    df_counternames = df_Counters[["CNT_CODE", "门店习惯称呼"]]
    dict_c_to_name = df_counternames.set_index("CNT_CODE")["门店习惯称呼"].to_dict()

    df_counter_regions = df_Counters[["CNT_CODE", "所属区域"]]
    dict_c_to_region = df_counter_regions.set_index("CNT_CODE")["所属区域"].to_dict()

    df_region_directors = df_Counters[["所属区域", "区域主管"]]
    dict_c_to_dir = df_region_directors.set_index("所属区域")["区域主管"].to_dict()
except Exception as e:
    print(f"{Fore.YELLOW}Error occured:")
    cols_needed = list(set(Counters).union(set(BA_Codes), set(af_BA_Codes)))
    cols = df_BA_List.columns
    diff = [item for item in cols_needed if item not in cols]
    if len(diff) > 1:
        print(f'{Fore.RED}Following elements are missing:{Style.RESET_ALL}:\n  {diff}')
    elif len(diff) == 1:
        print(f'{Fore.RED}Following element is missing:{Style.RESET_ALL}:\n  {diff}')
    else:
        print(f'{Fore.RED}{e}')
    sys.exit(1)


# Step2: 读取原始数据
mod_cols = ['region_name_cn', 'director', 'terminal_code', 'store_name', 'e_code', 'ba_name']
mod_col_type = {key: str for key in mod_cols}
dict_df_raw = pd.read_excel(raw_data_excel,
                    sheet_name=None,
                    dtype = mod_col_type)

# Step3: 开始替换
dict_dfs = {}
try:
    for key in dict_df_raw.keys():
        df1 = dict_df_raw[key]
        df2 = df1.copy()
        df2['terminal_code'] = df2['terminal_code'].map(dict_t_to_m)
        df2['store_name'] = df2['terminal_code'].map(dict_c_to_name)
        # condition1 = (df2['director'] != "TTL") & (df2['region_name_cn'].isin(dict_c_to_region))
        # df2.loc[condition1,'region_name_cn'] = df2.loc[condition1, "region_name_cn"].map(dict_c_to_region)
        df2['region_name_cn'] = df2["region_name_cn"].map(dict_c_to_region)
        df2['director'] = df2['region_name_cn'].map(dict_c_to_dir)
        dict1 = {key: df2}
        dict_dfs.update(dict1)

except Exception as e:
    # 检查每张Sheet是否缺少字段
    print(f'{Fore.YELLOW}Error occured:')
    dict_df = pd.read_excel(raw_data_excel,
                            sheet_name=None)
    for k in dict_df.keys():
        df_inspect = dict_df[k]
        raw_col_name = df_inspect.columns
        diff = [item for item in mod_cols if item not in raw_col_name]
        if len(diff) > 1:
            print(f'{Fore.RED}Following elements are missing in {Fore.CYAN}{k}{Style.RESET_ALL}:\n  {diff}')
        elif len(diff) == 1:
            print(f'{Fore.RED}Following element is missing in {Fore.CYAN}{k}{Style.RESET_ALL}:\n  {diff}')
        else:
            print(f'{Fore.RED}{e}')
    sys.exit(1)

# Step4: 合并去重
merged_df = pd.concat(dict_dfs.values(), ignore_index=True)
merged_df_unique = merged_df[mod_cols].drop_duplicates()

# Step5: 整理排序
nat = ['全国']
regions = ['华东', '西北', '南部']

df_nat = merged_df_unique.loc[merged_df_unique['region_name_cn'].isin(nat)]
df_region = merged_df_unique.loc[(merged_df_unique['region_name_cn'].isin(regions)) & (merged_df_unique['terminal_code'] == 'TTL')]
df_counter = merged_df_unique.loc[(merged_df_unique['region_name_cn'].isin(regions)) & (merged_df_unique['terminal_code'] != 'TTL') & (merged_df_unique['e_code'] == 'TTL')]
df_BA = merged_df_unique.loc[(merged_df_unique['region_name_cn'].isin(regions)) & (merged_df_unique['terminal_code'] != 'TTL') & (merged_df_unique['e_code'] != 'TTL')]

# 以下代码并未对区域为空的行进行处理，请留意
dfs = []
dfs.append(df_nat)
for r in regions:
    df3 = df_region.loc[df_region['region_name_cn'] == r]
    df_stores = df_counter.loc[df_counter['region_name_cn'] == r]
    stores_by_region = df_stores['store_name'].unique()
    dfs.append(df3)
    for store in stores_by_region:
        df4 = df_counter[df_counter['store_name'] == store]
        df5 = df_BA[df_BA['store_name'] == store]
        df5.sort_values(by='e_code', inplace=True)
        df6 = pd.concat([df4, df5], ignore_index=True)
        dfs.append(df6)
res = pd.concat(dfs,ignore_index=True)

# Step6: 导出
res.to_excel(output_path,sheet_name="Sheet1",index=False)
print(f"🎉{Fore.GREEN}File has successfully been output to {p}")