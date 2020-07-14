import pandas
import numpy
import os
import glob


def pivotTables_DataAggr(input_path,output_path):
    os.chdir(input_path)
    df=pandas.read_csv("Merged_Combined_File.csv")
    df=df.replace(-9999,numpy.nan)
    df["air_temp"]=df["air_temp"]/1.0
    pivot_table=pandas.pivot_table(df,index=["station_id"],columns="year",values="air_temp")
    pivot_table.to_csv("aggr_info"+".csv")


pivotTables_DataAggr("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/WriteModifiedFiles","")