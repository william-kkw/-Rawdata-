import pandas as pd

####

input_path = r''
output_path = r''
raw_data_excel = r''

# Step1: 读取BA_List
type_dict = dict(CNT_CODE=str, CNT_DEAL_NO=str, BA_code=str, af_ba_code=str)
df_BA_List = pd.read_excel(input_path,
                           dtype=type_dict)

Counters = ["CNT_LOCALNAME", "CNT_CODE", "CNT_DEAL_NO", "门店习惯称呼", "所属区域", "区域主管", "城市", "培训老师"]
BA_Codes = ["BA_name", "Counter_name", "BA_code", "Em_status"]
af_BA_Codes = ["af_ba_name", "af_ba_code"]

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


# Step2: 读取原始数据
mod_cols = ['region_name_cn', 'director', 'terminal_code', 'store_name', 'e_code', 'ba_name']
mod_col_type = {key: str for key in mod_cols}
dict_df_raw = pd.read_excel(raw_data_excel,
                       sheet_name=None,
                       dtype = mod_col_type)

# Step3: 开始替换
dict_dfs = {}
for key in dict_df_raw.keys():
    df1 = dict_df_raw[key]
    df2 = df1.copy()
    df2['terminal_code'] = df2['terminal_code'].map(dict_t_to_m)
    df2['store_name'] = df2['terminal_code'].map(dict_c_to_name)
    # condition1 = (df2['director'] != "TTL") & (df2['region_name_cn'].isin(dict_c_to_region))
    # df2.loc[condition1,'region_name_cn'] = df2.loc[condition1, "region_name_cn"].map(dict_c_to_region)
    df2['region_name_cn'] = df2["region_name_cn"].map(dict_c_to_region)
    df2['director'] = df2['region_name_cn'].map(dict_c_to_dir)
    # print(df2)
    dict1 = {key: df2}
    dict_dfs.update(dict1)

# Step4: 合并去重
merged_df = pd.concat(dict_dfs.values(), ignore_index=True)
merged_df_unique = merged_df[mod_cols].drop_duplicates()
# print(merged_df_unique)

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
        df5.sort_values(by='e_code')
        df6 = pd.concat([df4, df5], ignore_index=True)
        dfs.append(df6)
res = pd.concat(dfs,ignore_index=True)
# print(res)

# Step6: 导出
res.to_excel(output_path,sheet_name="Sheet1",index=False)
