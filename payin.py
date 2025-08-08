import pandas as pd
import time
import hashlib
# import bytedtos  
# import requests
import json


# tos config initialization
# tos = bytedtos.Client("pipo-tianshu-exchange-boe","KAI3MXASXEEZFFIS701B")
# file_path = 'pipo/payin_bill/202508/'

#loading template file
filename1 = 'payin_template.csv'
df =  pd.read_csv(filename1)
columns_1 = df.columns.tolist()[0]
columns = columns_1.split("|")
row1 = df.loc[0].values[0].split("|")
df_deal = pd.DataFrame(columns=columns,data=[row1])

columns_save = [columns_1]
success_file_col = 'Total Count|Amount Micro|test'

row_save = []

# filename
seq = 6
env = 'pre'
account_date = '20250804'
#environment change account
if env == 'pre':
    savefilename = '{}|11202105SaEss_payin|USD|{}|{}|V0|normal.csv'.format(account_date,account_date,seq)
    success_file_name = savefilename.split('.')[0]+'|success.csv'
else:
    savefilename = '{}|05815SGPGrFAowfl4RSO|USD|{}|{}|V0|normal.csv'.format(account_date,account_date,seq)
    success_file_name = savefilename.split('.')[0] + '|success.csv'

# 9|V0|writeoff.csv
regular = {"Settled":{"Debit":0,"Credit":500000},}
batch = 10000
payin_type = ["Settled","Refunded","Chargeback","Chargeback Reverse","CashOut","Adjustment","ManualAdjustment","ManualAddition","TP settlement","Allocate","Refund","Charge refund"]
# payin_type = ["Refunded","Chargeback","Chargeback Reverse","Refund","Charge refund"]
# payin_type = ["Refunded"]
test_currency = ["USD"]
#
date = ['2024-08-14 07:09:51']
first_flag = True


#Item quantity
num = 1867
#Build summary dataframe for success files
totalamount = num*(int(0 if df_deal.loc[0]['Net Credit (NC)']=='' else df_deal.loc[0]['Net Credit (NC)'])-int(0 if df_deal.loc[0]['Net Debit (NC)']=='' else df_deal.loc[0]['Net Debit (NC)']))
payin_success_data = str(num)+'|0|'+str(totalamount)
df_success = pd.DataFrame(columns=[success_file_col],data=[payin_success_data])
for i in range(num):
    timestamp = str(time.time())
    md5str = hashlib.md5(timestamp.encode("utf-8")).hexdigest()
    df_deal.loc[0]["Modification Reference"] = md5str
    df_deal.loc[0]["Bill Id"] = md5str
    df_deal.loc[0]["Gross Credit (GC)"] = str(429000000) #Transaction Amount
    df_deal.loc[0]["Gross Currency"] = test_currency[i%len(test_currency)]
    df_deal.loc[0]["Type"] = payin_type[i%len(payin_type)]
    df_deal.loc[0]["Record Type"] = "1"
    df_deal.loc[0]["Creation Date"] = date[i%len(date)]
    df_deal.loc[0]["Account Date"] = date[i %len(date)]
    df_deal.loc[0]["Merchant Name"] = "213"
    # print(df_deal.loc[0].values.tolist())
    df_deal_row_save = "|".join(df_deal.loc[0].values.tolist())
    if (i+1)%batch==0:
        df_save = pd.DataFrame(columns=columns_save, data=row_save)
        if first_flag:
            df_save.to_csv(savefilename, index=False)
            first_flag = False
        else:
            df_save.to_csv(savefilename, index=False, mode='a', header=None)
        row_save = []
    if i%1000==0:
        print("当前条数：{}".format(i))
    row_save.append(df_deal_row_save)
df_save = pd.DataFrame(columns=columns_save, data=row_save)
if not first_flag:
    df_save.to_csv(savefilename,index=False, mode='a', header=None)
else:
    df_save.to_csv(savefilename, index=False)

df_success.to_csv(success_file_name,index=False)
#
#
#
# upload tos
# uploadFile = file_path+savefilename
# uploadSuccessFile = file_path+success_file_name
# #
# tos.put_object(uploadFile, open("{}".format(savefilename), "r"))
# print(tos.get_object(uploadFile).data)
#
# tos.put_object(uploadSuccessFile, open("{}".format(success_file_name), "r"))
# print(tos.get_object(uploadSuccessFile).data)
#
# # with open(os.path.join("tt.csv"), "wb") as f:
# #     f.write(tos.get_object(uploadSuccessFile).data)
#
