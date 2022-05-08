#importing stuffsss
from pickle import FALSE
import pandas as pd
import json
import os
import gc
from googletrans import Translator 


translator = Translator()
df = pd.read_json('/mnt/c/Users/vatsa/Desktop/Projects/misinfoproject/factcheck.json')

with open('/mnt/c/Users/vatsa/Desktop/Projects/misinfoproject/factcheck.json','r') as myfile:
    data=myfile.read()

obj = json.loads(data)
column_names = ["num","datepublished"]
column_namesclaim = ["num","claim"]
column_namessource = ["num","source","source_type"]
column_namesrating = ["num","reviewratingtype","reviewrating"]

rows = []
rowsclaim = []
rowssource = []
rowrating = []

#for i in range(0,len(obj["dataFeedElement"])):
for i in range(0,300):
	x = str(i)+" "+ "of " + str(len(obj["dataFeedElement"])) + "done"
	if obj["dataFeedElement"][i]["item"] is not None:
		for j in range(0, len(obj["dataFeedElement"][i]["item"])):
			##DatePublished
			if "datePublished" in obj["dataFeedElement"][i]["item"][j]:
				row = [str(i),obj["dataFeedElement"][i]["item"][j]["datePublished"]]
			else:
				row = [str(i),"NA"]
			rows.append(row)
			##Claim
			if "claimReviewed" in obj["dataFeedElement"][i]["item"][j]:
				row = [str(i),obj["dataFeedElement"][i]["item"][j]["claimReviewed"],]
			else:
				row = [str(i),"NA"]
			rowsclaim.append(row)
			##Source
			if "itemReviewed" in obj["dataFeedElement"][i]["item"][j]:
				if "appearance" in obj["dataFeedElement"][i]["item"][j]["itemReviewed"]:
					for k in range(0, len(obj["dataFeedElement"][i]["item"][j]["itemReviewed"]["appearance"])):
						row = [str(i),obj["dataFeedElement"][i]["item"][j]["itemReviewed"]["appearance"][k]["url"],obj["dataFeedElement"][i]["item"][j]["itemReviewed"]["appearance"][k]["@type"]]
				else:
					row = [str(i),"NA","Social Media"]
				rowssource.append(row)
			else:
				row = [str(i),"NA","NA"]
			rowssource.append(row)
			##Rating & rating type
			if "reviewRating" in obj["dataFeedElement"][i]["item"][j]:
				if "alternateName" in obj["dataFeedElement"][i]["item"][j]["reviewRating"]:
					row = [str(i),obj["dataFeedElement"][i]["item"][j]["reviewRating"]["@type"],obj["dataFeedElement"][i]["item"][j]["reviewRating"]["alternateName"]]
				else:
					row = [str(i),"NA","NA"]
			else:
				row = [str(i),"NA","NA"]
			
			rowrating.append(row)
			
	print(x)
	del x

df = pd.DataFrame(rows,columns = column_names)
df2 = pd.DataFrame(rowsclaim,columns = column_namesclaim)
df3 = pd.DataFrame(rowssource,columns = column_namessource)
df4 = pd.DataFrame(rowrating,columns = column_namesrating)

df_temp_1 = pd.merge(df,df2,how="left", on = ["num"])
df_temp_2 = pd.merge(df_temp_1,df3,how="left", on = ["num"])
df_temp_3 = pd.merge(df_temp_2,df4,how="left", on = ["num"])
df_temp_3 = df_temp_3.reset_index(drop=True)
df_temp_3 = df_temp_3.drop_duplicates()


print(df_temp_3.head())

#df_temp_3.to_csv("/mnt/c/Users/vatsa/Desktop/Projects/misinfoproject/database_translated.csv",sep='\t',encoding='utf-8',index=False)
#print(os.getcwd())