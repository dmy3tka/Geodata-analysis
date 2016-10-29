# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 15:40:28 2015

@author: Dmitry
"""

from ftplib import FTP, error_perm
import os
import glob
import patoolib
import pandas
import numpy
import simplekml
import seaborn as sns

def ftpDownloader(stationId,startYear,endYear,url="ftp.pyclass.com",user="student@pyclass.com",passwd="student123"):
    ftp=FTP(url)
    ftp.login(user,passwd)
    if not os.path.exists("D:\\in"):
        os.makedirs("D:\\in")
    os.chdir("D:\\in")
    for year in range(startYear,endYear+1):
        fullpath='/Data/%s/%s-%s.gz' % (year,stationId,year)
        filename=os.path.basename(fullpath)
        try:
            with open(filename,'wb') as file:
                ftp.retrbinary('RETR %s' % fullpath,file.write)
                print("% successfully downloaded" % filename)
        except error_perm:
            print('%s is not available' % filename)
            os.remove(filename)
    ftp.close()


def extractfiles(indir="D:\\in",out="D:\\in\Extracted"):
   os.chdir(indir)
   archives=glob.glob("*.gz")
   if not os.path.exists(out):
       os.makedirs(out)
   files=os.listdir("Extracted")
   for archive in archives:
       if archive[:-3] not in files:
           patoolib.extract_archive(archive,outdir=out)
           
def addField(indir="D:\\in\Extracted"):
    os.chdir(indir)
    fileList=glob.glob("*")
    for filename in fileList:
        df=pandas.read_csv(filename,sep='\s+',header=None)
        df["Station"]=[filename.rsplit("-",1)[0]]*df.shape[0]
        df.to_csv(filename+".csv",index=None,header=None)
        os.remove(filename)
        
def concatenate(indir="D:\\in\Extracted",outfile="D:\\in\Concatenated.csv"):
    os.chdir(indir)
    fileList=glob.glob("*.csv")
    dfList=[]
    colnames=["Year","Month","Day","Hour","Temp","DewTemp","Pressure","WindDir","WindSpeed","Sky","Precip1","Precip6","ID"]
    for filename in fileList:
        print(filename)
        df=pandas.read_csv(filename,header=None)
        dfList.append(df)
    concatDf=pandas.concat(dfList,axis=0)
    concatDf.columns=colnames
    concatDf.to_csv(outfile,index=None)
    
def merge(left="D:\\in\Concatenated.csv",right="D:\\in\station-info.txt",output="D:\\in\Concatenated-Merged.csv"):
    leftDf=pandas.read_csv(left)
    rightDf=pandas.read_fwf(right,converters={"USAF":str,"WBAN":str})
    rightDf["USAF_WBAN"]=rightDf["USAF"]+"-"+rightDf["WBAN"]
    mergeDf=pandas.merge(leftDf,rightDf.ix[:,["USAF_WBAN","STATION NAME","LAT","LON"]],left_on="ID",right_on="USAF_WBAN")
    mergeDf.to_csv(output)
    
def pivot(infile="D:\\in\Concatenated-Merged.csv",outfile="D:\\in\Pivoted.csv"):
    df=pandas.read_csv(infile)
    df=df.replace(-9999,numpy.nan)
    df["Temp"]=df["Temp"]/10.0
    table=pandas.pivot_table(df,index=["ID","LON","LAT","STATION NAME"],columns="Year",values="Temp")
    table.to_csv(outfile)
    return table
    
def plot(outfigure="D:\\in\Ploted.png"):
    df=pivot()
    df.T.plot(subplots=True,kind='bar')
    sns.plt.savefig(outfigure,dpi=300)
    
def kml(input="D:\\in\\Pivoted.csv",out="D:\\in\\Weather.kml"):
    kml=simplekml.Kml()
    df=pandas.read_csv(input,index_col=["ID","LON","LAT","STATION NAME"])
    for lon,lat,name in zip(df.index.get_level_values("LON"),df.index.get_level_values("LAT"),df.index.get_level_values("STATION NAME")):
        kml.newpoint(name=name,coords=[(lon,lat)])
        kml.save(out)

if __name__=="__main__":    
    stationsIdString=input("Enter station names divided by comas: ")
    startingYear=int(input("Enter the starting year of the data: "))
    endingYear=int(input("Enter the ending year of the data: "))
    stationIdList=stationsIdString.split(',')
    
    for station in stationIdList:
        ftpDownloader(station,startingYear,endingYear)
        
    extractfiles()
    addField()
    concatenate()
    merge()
    pivot()
    kml()
    plot()
