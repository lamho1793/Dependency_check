import json
from pickle import FALSE
from sys import displayhook
import warnings
import networkx as nx
import pandas as pd
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import scipy as sp
import timeit
import pymysql

def get_important_task():
    
    sql="""select a.name as flow, t_node_name as node, t_key as value, t_relationship as depandence, a.user_id as username from indata.nifi_stream_node
           Left join indata.nifi_stream a on indata.nifi_stream_node.t_stream_id = a.id
           order by t_stream_id asc, t_relationship desc"""
    connection = pymysql.connect(
                             host='10.250.50.139',
                             port=3306,
                             user='BDO',
                             password='Cmhk1@345',
                             cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            data=pd.DataFrame.from_dict(result)       
    
    cursor.close()
    return data

warnings.filterwarnings('ignore')
def Path_identification(data):
    start = timeit.default_timer()  
    data.columns=['Flow','Node', 'RandomNum','Precondition',"User_name"]

    data['Precondition'] = data['Precondition'].str.replace("&&", ",").fillna(data['Precondition'])
    data['Precondition'] = data['Precondition'].str.replace("(", "").fillna(data['Precondition'])
    data['Precondition'] = data['Precondition'].str.replace(")", "").fillna(data['Precondition'])
    data['Flow'] = data['Flow'].fillna('NULL')
    
    count=0
    for i in range(len(data.Flow)):
        if data.iloc[i]['Flow']=='NULL':
            data.at[i,'Flow']=f'''NULL{count}'''
        if data.iloc[i-1]['Flow']==f'''NULL{count}''' and data.iloc[i+1]['Flow']!='NULL':
            count+=1
        else:
            continue

    data=data.sort_values(by=['Flow', 'Precondition'])
    
    # SQOOP_L_TO_POST_PROD_STA_853_D

    data=data.assign(Precondition=data["Precondition"].str.split(',')).explode('Precondition')
    data["Precondition"] = pd.to_numeric(data['Precondition'], errors='coerce').fillna(0).astype(int)

    unique_flows=data['Flow'].unique()

    data0=data.groupby(by="Flow")
    dic = {}
    for name,DataFrame in data0:
        dic[name] = data.loc[data["Flow"] == name]


    dic_random_list={}
    for i in range(len(unique_flows)):
        dic_random_list[unique_flows[i]] = list(dic[unique_flows[i]].RandomNum)
        
    for i in range(len(unique_flows)):
        for j in range(len(dic[unique_flows[i]])):

            if dic[unique_flows[i]]["Precondition"].iloc[j] not in dic_random_list[unique_flows[i]]:
                dic[unique_flows[i]]["Precondition"].iloc[j] = 0
    
    dic_randomNum_Node={}
    for i in range(len(unique_flows)):
        dic_randomNum_Node[unique_flows[i]] = (dic[unique_flows[i]][["RandomNum","Node"]])
    
        
    dic_list={}
    for i in range(len(unique_flows)):
        dic_list[unique_flows[i]]=dict((dic_randomNum_Node[unique_flows[i]]).values)

    
    for i in range(len(unique_flows)):
        dic[unique_flows[i]] = dic[unique_flows[i]][dic[unique_flows[i]]["Precondition"] != 0]
        
    for i in range(len(unique_flows)):
        for j in range(len(dic[unique_flows[i]])):
            (dic[unique_flows[i]])["RandomNum"].iloc[j]=(dic_list[unique_flows[i]][(dic[unique_flows[i]])["RandomNum"].iloc[j]])
            (dic[unique_flows[i]])["Precondition"].iloc[j]=(dic_list[unique_flows[i]][(dic[unique_flows[i]])["Precondition"].iloc[j]])
   
    Frame=[]

    for i in range(len(dic)):
        Frame.append(dic[unique_flows[i]])
    DF=pd.concat(Frame)


    DF=DF.drop(DF[(DF['Precondition']=="Time")|(DF['Precondition']=="modelData")].index)
    DF=DF.drop(DF[(DF['RandomNum']=="And")|(DF['RandomNum']=="Or")|(DF['RandomNum']=="Start")|(DF['RandomNum']=="End")].index)

    DF.to_csv('DF_del.csv',encoding='utf-8-sig', index=False)

    G=nx.from_pandas_edgelist(DF, source='Precondition', target='RandomNum',create_using=nx.DiGraph)
    G2=nx.from_pandas_edgelist(DF, source='RandomNum', target='Precondition',create_using=nx.DiGraph)
    
    with open("eee.json", "w+", encoding='utf-8-sig') as outfile:
            outfile.write(json.dumps(list(G2.nodes))) 

    nx.write_graphml_lxml(G, "Forward.graphml")
    nx.write_graphml_lxml(G2, "Backward.graphml")

    stop = timeit.default_timer()
    print('Time: ', stop - start) 
#---------------------------------------------------------------------------------------------------------------------------------------#
sql_data=get_important_task()
graph=Path_identification(sql_data)

