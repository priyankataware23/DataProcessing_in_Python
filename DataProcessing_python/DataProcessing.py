import os
from ftplib import FTP, error_perm

import pandas
import patoolib
import xlrd
import glob

"""
#create ftp object to access methods in FTP class
ftp=FTP("ftp.pyclass.com")

#login to ftp server, provide uername, password
ftp.login("student@pyclass.com","student123")

#Display all contents on ftp server
print(ftp.nlst())

os is used to change directories on local
cwd : change working directory on ftp server
open method is used to open file, with 'wb' -- write & binary access is given to file, and stored as file object

RETR : is the function in ftb to retrieve file with provided file name as : ftp.retrbinary('RETR filename.pft',file.write) -- here we retrive the file, and write the file 

to avoid hard-coding on filename, filename can be passed as a parameter to function.
in RETR function -- it accepts 'RETR filename.pdf' in same quotes, hence we can pass param directly in quotes, as it will treated as a string. 
SO we use: 'RETR %s' %filename  ; where %s will be replaced by variable right after it; in this case it will be filename
"""


def ftpFileDownloader(filename, host="ftp.pyclass.com", user="student@pyclass.com", password="student123"):
    ftp = FTP(host)
    ftp.login(user, password)
    print(ftp.nlst())
    print("---")
    ftp.cwd("Data")
    os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads")
    with open(filename, 'wb') as file:
        ftp.retrbinary('RETR %s' % filename, file.write)
    ftp.close()


# ftpFileDownloader("isd-lite-format.pdf")

'''download multiple files

1. connect to ftp server, using username and passwd
2. Create directory on local machine/server to download files. 
3. Since downloading multiple files in a loop : identify:
        on remote host: ftp://ftp.pyclass.com/Data/1901/ --> files are present in dir Data--> year --> stationid-year.gz

        for eg: 
        construct 
            fullFilePath=/Data/year/stationId-year.gz ; this can be written as below
            fullFilePath='/Data/%s/%s-%s.gz' %(year,stationId,year)
            fileName=os.path.basename(fullFilePath)

            open file on local and retiver from FTP
            with open (filename,'wb') as file:
                ftp.retrbinary('RETR %s' fullFilePath,file.write)


'''


def ftpDownloadMultipleFiles(stationId, startYear, endYear, host="ftp.pyclass.com", username="student@pyclass.com",
                             password="student123"):
    ftp = FTP(host)
    ftp.login(username, password)
    print("Successful Connection to Host: ", host)
    print("Directories on Host: ", ftp.nlst())
    # creating folder on local to download files from server
    if not os.path.exists(
            "/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/MultiplefileDownload"):
        os.makedirs(
            "/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/MultiplefileDownload")
    os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/MultiplefileDownload")

    for year in range(startYear, endYear + 1):
        print ("=============")
        fullPath = '/Data/%s/%s-%s.gz' % (year, stationId, year)
        print("fullPath: ", fullPath)
        fileName = os.path.basename(fullPath)
        print("fileName: ", fileName)
        try:
            with open(fileName, 'wb') as file:
                ftp.retrbinary('RETR %s' % fullPath, file.write)
                print ("file : %s Successfully Downloaded " % fileName)
        except error_perm:
            print ("file : %s Not available for Download " % fileName)
            os.remove(fileName)
    ftp.close()


# ftpDownloadMultipleFiles("029070-99999",1901,1920)
# ftpDownloadMultipleFiles("029500-99999",1901,1920)
# ftpDownloadMultipleFiles("029600-99999",1901,1920)
# ftpDownloadMultipleFiles("029700-99999",1901,1920)
# ftpDownloadMultipleFiles("029720-99999",1901,1920)
# ftpDownloadMultipleFiles("029810-99999",1901,1920)
# ftpDownloadMultipleFiles("029820-99999",1901,1920)
# ftpDownloadMultipleFiles("029170-99999",1901,1920)
# ftpDownloadMultipleFiles("029920-99999",1901,1920)
# ftpDownloadMultipleFiles("029440-99999",1901,1920)
# ftpDownloadMultipleFiles("029170-99999",1901,1920)


'''
Extract archived Files using patoolib  
'''


def extractArchivedFiles(compressed_filePath, uncompressed_filePath):
    # change dir to input where zipped files exists
    os.chdir(compressed_filePath)

    # create empty list, to hold filenames of files to be unzipped/extracted. Using glob or os.walk (), method read files from dir, with a specified extensions
    fileList = []
    for root, dirs, files in os.walk(compressed_filePath):
        for file in files:
            if file.endswith('.gz') or file.endswith('.zip') or file.endswith('.rar') or file.endswith(
                    '.tar') or file.endswith('.txt') or file.endswith('.csv' or file.endswith('.pdf')):
                print file
                fileList.append(file)

    # check if output path already exists, if not create output path

    if not os.path.exists(uncompressed_filePath):
        os.makedirs(uncompressed_filePath)

    # check already existing files that are extracted, below helps in avoiding re-extraction of already extracted file

    existing_files_inuncompressed_filePath = os.listdir(uncompressed_filePath)

    for item in fileList:
        print("------------")
        print ("item: ", item)
        print("outdir:", uncompressed_filePath)
        if item[: -3] not in existing_files_inuncompressed_filePath:
            patoolib.extract_archive('%s' % item, outdir='%s' % uncompressed_filePath)


# extractArchivedFiles("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/MultiplefileDownload","/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/ExtractData")


#  using pandas


os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/ExtractData")

# below technique can be used to append headers tp a file
col_Names = ["year", "month", "day", "hour", "air_temp", "dp_temp", "sea_lvl_pressure", "wind_direction",
             "wind_speed_rate", "sky_cnd_code", "lpd_1hr", "lpd_6hr"]

df1 = pandas.read_csv("029070-99999-1901", sep='\s+', names=col_Names)
# df1.reset_index(drop=True, inplace=True)

# write DF to file -- csv,txt,html, etc
os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads")
if not os.path.exists(
        "/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/CreateDataFiles"):
    os.makedirs("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/CreateDataFiles")
os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/CreateDataFiles")

df1.to_csv("comma_seperated_file.csv", sep=',', index=None)
df1.to_csv("colon_seperated_file.txt", sep=':', header=None, index=None)
df1.to_html("df.html", index=None)

# df1.to_excel('output1.xlsx', engine='xlsxwriter')
os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads")
df2 = pandas.read_excel("original.xls", skiprows=[0, 1, 2], index=None)
df2.to_csv("excel_to_csv.csv", index=None)

# extracting data from csv
os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads")
df = pandas.read_csv("SmallFile.csv", index_col="ID")
print (df.head())
print("------------")

print("Min", df["Temp"].min())
print("max", df["Temp"].max())
print("abs", df["Temp"].abs())
print("unique", df["Temp"].unique())
print("sum", df["Temp"].sum())
print("count", df["Temp"].count())

# extract certain rows, columns from df
dc = df.loc["Day 1":"Day 5", "Year":"DewTemp"]
print(dc)

print("+++++++++++++++++++++")


# Read data from file in a loop

def readDataFromFile(input_path, output_path):
    if input_path == None:
        print("Incorrect Input Path")
    os.chdir(input_path)
    filelist = glob.glob("*")
    for file in filelist:
        os.chdir(input_path)
        df = pandas.read_csv(file, sep='\s+', index_col=None, header=None)
        df["station"] = [file.rsplit("-", 1)[0]] * df.shape[0]
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        os.chdir(output_path)
        try:
            df.to_csv(file + '.csv', index=None, header=None)
            print("File Created Successfully: " + file + '.csv')
        except error_perm:
            print ("File Creation Unsuccessful: " + file + '.csv')


# readDataFromFile("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/ExtractData","/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/WriteModifiedFiles")


print("+++++++++++++++++++++")
# data Analysis exercise
os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads")
df = pandas.read_csv("excel_to_csv.csv")
df["euro"] = df["1984"] * 0.88
df["bristish"] = df["1984"] * 0.44
df.to_csv("cols_added_excel_to_csv" + ".csv", index=None)

print("+++++++++++++++++++++")


def concatMultipleFiles(input_path, output_path):
    os.chdir(input_path)
    fileList = glob.glob("*.csv")
    dfList = []
    colNames = ["year", "month", "day", "hour", "air_temp", "dp_temp", "sea_lvl_pressure", "wind_direction",
                "wind_speed_rate", "sky_cnd_code", "lpd_1hr", "lpd_6hr", "station_id"]
    for file in fileList:
        print(file)
        df = pandas.read_csv(file, header=None, index_col=None)
        dfList.append(df)
    os.chdir(output_path)
    dfConcat = pandas.concat(dfList, axis=0)
    dfConcat.columns = colNames
    dfConcat.to_csv("CombinedFile" + ".csv", index=None)


# concatMultipleFiles("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/WriteModifiedFiles","/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/WriteModifiedFiles")


# DataAnalytics Exercise -- merge files horizontally & remove duplicate columns
os.chdir(
    "/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/DataAnalyticsExercise/Income-By-State/")
'''
fileList=glob.glob("*.xls")
dfList=[]
for file in fileList:
    print("filename: "+file )
    df=pandas.read_excel(file,skiprows=[0,1,2],index=None)
    dfList.append(df)
dfConcat=pandas.concat(dfList,axis=1)
dfConcat.to_csv("CombinedFile"+".csv",index=None)
'''
df1 = pandas.read_csv("CombinedFile.csv", index_col=None)
df1 = df1.T.drop_duplicates().T
df1.to_csv("removed_dups.csv", index=None)


## Merge files or join 2 files on common columns
def mergeFiles(input_path, output_path):
    os.chdir(input_path)
    left_df = pandas.read_csv("CombinedFile.csv", index_col=None)
    right_df = pandas.read_fwf("station-info.txt", converters={"USAF": str, "WBAN": str})
    #right_df["USAF_WBAN"] = right_df["USAF"] + "-" + ["WBAN"]
    right_df["USAF_WBAN"] = right_df["USAF"] + "-" + right_df["WBAN"]
    mergedDf = pandas.merge(left_df, right_df.ix[:, ["USAF_WBAN", "STATION NAME", "LAT", "LON"]], left_on="station_id",
                            right_on="USAF_WBAN")
    os.chdir(output_path)
    mergedDf.to_csv("Merged_Combined_File" + ".csv",index=None)


#mergeFiles("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/WriteModifiedFiles","/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/WriteModifiedFiles")

#Data Analytics exercise Merge 2 files on common column
os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/DataAnalyticsExercise/Income-By-State/")
left_df=pandas.read_csv("removed_dups.csv")
os.chdir("/Users/ptaware/PycharmProjects/DataProcessing_python/DataProcess_File_Downloads/DataAnalyticsExercise/Two-Files-Inside/")
right_dt=pandas.read_csv("Geoids_and_states.csv")
merge_df=pandas.merge(left_df,right_dt,left_on="GEOID",right_on="GEOID")
merge_df.set_index(["GEOID" , "State"],inplace=True)
merge_df.to_csv("merged_income_by_state.csv")