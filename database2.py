#importing stuff
import pandas as pd
import json
import os
import gc

df = pd.read_json('factcheck.json')

with open('factcheck.json','r') as myfile:
    data=myfile.read()

obj = json.loads(data)
column_names = ["index","datepublished"]
column_namesclaim = ["index","claim"]
column_namessource = ["index","source","source_type"]
column_namesrating = ["index","reviewratingtype","reviewrating"]
#column_fact = ["index","factcheck source","factcheck_aggregator_url"]

rows = []
rowsclaim = []
rowssource = []
rowrating = []
#rowfactcheck = []
for i in range(0,len(obj["dataFeedElement"])):
#for i in range(0,30000):
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
			
#			if "sdPublisher" in obj["dataFeedElement"][i]["item"][j]:
#				row = [str(i),obj["dataFeedElement"][i]["item"][j]["url"],obj["dataFeedElement"][i]["item"][j]["sdPublisher"]["url"]]
#			else:
#				row = [str(i),"NA","NA"]
#			rowfactcheck.append(row)
	print(x)
	del x

df = pd.DataFrame(rows,columns = column_names)
df2 = pd.DataFrame(rowsclaim,columns = column_namesclaim)
df3 = pd.DataFrame(rowssource,columns = column_namessource)
df4 = pd.DataFrame(rowrating,columns = column_namesrating)
#df5 = pd.DataFrame(rowfactcheck,columns = column_fact)

df_temp_1 = pd.merge(df,df2,how="left", on = ["index"])
df_temp_2 = pd.merge(df_temp_1,df3,how="left", on = ["index"])
df_temp_3 = pd.merge(df_temp_2,df4,how="left", on = ["index"])
#df_temp_4 = pd.merge(df_temp_3,df5,how="left", on = ["index"])

#print(df_temp_4)
df_temp_3.to_csv("database.csv",sep='\t',encoding='utf-8')
print(os.getcwd())