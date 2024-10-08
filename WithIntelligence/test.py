#!/usr/local/bin/python3

#############################################################
#Scripted by Syamal Reddy                                   #
#Date: 01/04/2024                                           #
#                                                           #
#                                                           #
#                   Purpose of the script                   #
#-----------------------------------------------------------#
#This function runs SRI ASP files comes thru STFP.          #
#We are downloading the .zip file with proper file name such#
#as ON.SRI11302023-07.zip... After we download we are unzip #
#each file. Then we are looking for .csv file which contains#
#details of the each file. First we are loading this .csv   #
#file to database. Then, based on with this information we  #
#proccesing Data files and Attachment files. We are loading #
#the files to database. Also we are loading this files to   #
#Azure bucket for archiving. After load them all, we move   #
#them to SAS location. Finally, we are sending notification #
#emails to given users.                                     #
#############################################################
import ora
import cx_Oracle
from openpyxl import load_workbook
import openpyxl
import pysftp
import sys
import os
import itertools
import json
import botocore.session as s
import zipfile
import fnmatch
import gc
from datetime import date,datetime
import xlrd
import xlsxwriter
import saspy
import csv_loader as ldr
import xls_loader as xld
import logging
import re
import etl_notification as email
import shutil
import pandas as pd
import csv
import os.path

#############################################################
#           Pre configuration for all Pyhon Tasks           #
#############################################################

#Change Language to UTF-8
os.environ["NLS_LANG"] = "American_America.UTF8"

#Change environment to current location
Script_Location = os.path.dirname(os.path.realpath(__file__))
os.chdir(Script_Location)

#Setup Todays date for future use
Today_Date = datetime.now().strftime('%Y-%m-%d')

#Set Test mode True
Test_Mode = True

#Set the environment if it is DEV or PROD
ENV = 'DEV'  #'PROD'

#If the code runs in Production, test mode is False
Env_Test = os.getenv('TEST')
if Env_Test == "False":
  Test_Mode = False


#Create the connection.
connection= ora.connect()
cursor = connection.cursor()

if ENV == 'DEV':
  Parent_Location = "/home/pythonadm/Syamal/SRI"
  import azure_blob_dev as azure_blob
else:
  Parent_Location = "/data/ih_data/infa_shared/SrcFiles/ANALYTICS/SRI/"
  import azure_blob_prod as azure_blob
 
 


#Depends on the Env_Test variable, set the future use variables and show it to developer
if Test_Mode:
  Sftp_Folder = '/BCS/' #connet to UAT SFTP
  Today_File=str("ON.SRI"+datetime.now().strftime("%m%d%Y")+"-15.zip")
  Files_Location = Parent_Location + "/data/" + Today_File[:-4] + '/MINQ/'
  Azure_Container='sri-test'
  Azure_Folder=str("ON.SRI"+datetime.now().strftime("%m%d%Y")+"-15/")+'SRI/MINQ/'
  Attachment_Table_Name = 'HDCS_FILE_ATTACHMENT_T'
  MetaInformation_Table_Name = 'HDCS_METAINFO_T'
  COMMUNITY_QUARTLY_REPORT_1_Table_Name = 'HDCS_COMMUNITY_QUARTLY_REPORT_1_T'
  COMMUNITY_QUARTLY_REPORT_2_Table_Name = 'HDCS_COMMUNITY_QUARTLY_REPORT_2_T'
  COMMUNITY_QUARTLY_REPORT_3_Table_Name = 'HDCS_COMMUNITY_QUARTLY_REPORT_3_T'
  COMMUNITY_QUARTLY_REPORT_4_Table_Name = 'HDCS_COMMUNITY_QUARTLY_REPORT_4_T'  
  Schema = "UAT"
else:
  Sftp_Folder = 'BCS' #connet to PROD SFTP
  Today_File=str("ON.SRI"+datetime.now().strftime("%m%d%Y")+"-15.zip")
  Files_Location = Parent_Location + "/data/" + Today_File[:-4] + '/HOSPQ/'
  Azure_Container='sri'
  Azure_Folder=str("ON.SRI"+datetime.now().strftime("%m%d%Y")+"-15/")+'SRI/MINQ/'
  Attachment_Table_Name = 'HDCS_FILE_ATTACHMENT'
  MetaInformation_Table_Name = 'HDCS_METAINFO'
  COMMUNITY_QUARTLY_REPORT_1_Table_Name = 'HDCS_COMMUNITY_QUARTLY_REPORT_1'
  COMMUNITY_QUARTLY_REPORT_2_Table_Name = 'HDCS_COMMUNITY_QUARTLY_REPORT_2'
  COMMUNITY_QUARTLY_REPORT_3_Table_Name = 'HDCS_COMMUNITY_QUARTLY_REPORT_3'
  COMMUNITY_QUARTLY_REPORT_4_Table_Name = 'HDCS_COMMUNITY_QUARTLY_REPORT_4'
  Schema = "SRI"
 
Notifications_Error_Type = 0
Table_Names_List = [COMMUNITY_QUARTLY_REPORT_1_Table_Name, COMMUNITY_QUARTLY_REPORT_2_Table_Name, COMMUNITY_QUARTLY_REPORT_3_Table_Name, COMMUNITY_QUARTLY_REPORT_4_Table_Name, Attachment_Table_Name,MetaInformation_Table_Name]


def Process_HDCS_COMMUNITY_QUARTLY_REPORT_1(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,each_sheetname,Excel_File_Data):
  global Notifications_Error_Type
  global Files_Location
 
  #First check is it resubmission. If so, we have to remove the old data
  Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,TYPE,"Data",COMMUNITY_QUARTLY_REPORT_1_Table_Name)
 
  Temp_Error = False
  p = {'file_name' : Files_Location + File_Name,
        'table_name' : COMMUNITY_QUARTLY_REPORT_1_Table_Name,
        'map_columns' : "by_name",
        'header_pos'  : 0,
        'skiprows' : 0,
        'sheet_name' : each_sheetname,
        'truncate_table' : False,
        'charset' : 'American_America.UTF8',
        'date_format' : "mm/dd/yyyy",
        'quoting' : 'all',
        'quotechar': '"'
      }
  try:
    l = xld.xls_loader(p)
    l.ignore_column('INST')
    l.ignore_column('TYPE')
    l.ignore_column('CENSUSDATE')
    l.ignore_column('QUARTER')
    l.ignore_column('STATUSID')
    l.ignore_column('STATUS')
    l.ignore_column('FISCALYEAR')
    l.ignore_column('MONTH')
    l.map_column('SERV_PROVIDER_NAME',"Service Provider Name (Self-Reported)")
    l.map_column('SERV_PROVIDER_LEGAL_NAME',"Service Provider Legal Name (Self-Reported)")
    l.map_column('ADDRESS1',"Address 1")
    l.map_column('ADDRESS2',"Address 2")
    l.map_column('CITY',"City")
    l.map_column('POSTAL_CODE',"Postal Code")
    l.map_column('HCCSS_NAME',"HCCSS")
    l.map_column('IFIS_NO',"IFIS #")
    l.map_column('DESCRIPTION',"Description")
    l.map_column('NAME',"Name")
    l.map_column('POSITION_NAME',"Position Name")
    l.map_column('TELEPHONE',"Telephone")
    l.map_column('EMAIL',"Email")
    l.ignore_column('CREATEDATE')
    l.ignore_column('CREATEDBYUSER')
    l.ignore_column('LASTMODIFIEDUSER')
    l.ignore_column('LASTMODIFIEDDATE')
    l.ignore_column('SUBMITDATE')
    l.ignore_column('UPLOAD_FILENAME')
    l.ignore_column('ETL_LOAD_DATE')
    l.ignore_column('ETL_UPDATE_DATE')
    l.process_rows()
    connection.commit()
  except Exception as e:
    logging.info("There was an error while loading worksheet name " + each_sheetname + " in " + File_Name +  " file. The error is: " + str(e))
    Notifications_Error_Type = 1
    Temp_Error = True
  if Temp_Error == False:
    if l.get_rows_rejected() > 0:
      logging.info (File_Name + " : Processing Done (" + str(l.get_rows_loaded())+ " rows loaded," + str(l.get_rows_rejected()) + " rows rejected).")
      for each_table in Table_Names_List:
        Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,TYPE,"Data",each_table)
      Notifications_Error_Type = 1
    else:
      logging.info (File_Name + " : Processing Done (" + str(l.get_rows_loaded())+ " rows loaded," + str(l.get_rows_rejected()) + " rows rejected).")
 
  Update_MINQ_Table (COMMUNITY_QUARTLY_REPORT_1_Table_Name,Instution_Number,TYPE,STATUSID,FISCALYEAR,Quarter,Census_Date,MONTH,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,File_Name)

def Process_HDCS_COMMUNITY_QUARTLY_REPORT_2(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,each_sheetname,Excel_File_Data):
  global Notifications_Error_Type
  global Files_Location
 
  #First check is it resubmission. If so, we have to remove the old data
  Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,TYPE,"Data",COMMUNITY_QUARTLY_REPORT_2_Table_Name)
 
  Temp_Error = False
  p = {'file_name' : Files_Location + File_Name,
        'table_name' : COMMUNITY_QUARTLY_REPORT_2_Table_Name,
        'map_columns' : "by_name",
        'header_pos'  : 0,
        'skiprows' : 0,
        'sheet_name' : each_sheetname,
        'truncate_table' : False,
        'charset' : 'American_America.UTF8',
        'date_format' : "mm/dd/yyyy",
        'quoting' : 'all',
        'quotechar': '"'
      }
  try:
    l = xld.xls_loader(p)
    l.ignore_column('INST')
    l.ignore_column('TYPE')
    l.ignore_column('CENSUSDATE')
    l.ignore_column('QUARTER')    
    l.ignore_column('STATUSID')
    l.ignore_column('STATUS')
    l.ignore_column('FISCALYEAR')
    l.ignore_column('MONTH')
    l.map_column('TPBE',"TPBE")
    l.map_column('SECTION',"Section")
    l.map_column('CATEGORY_DESC',"Category")
    l.map_column('BUDGET',"Budget")
    l.map_column('BUDGET_ADJUSTMENT',"Budget Adjustments")
    l.map_column('TOT_UPDATED_BUDGET',"Total Updated Budget")
    l.map_column('YTD_ACTUAL',"YTD Actual")
    l.map_column('YTD_VAR_TO_BUDGET_AMT',"YTD $ Variance to Budget")
    l.map_column('YTD_VAR_TO_BUDGET_PERC',"YTD % Variance to Budget")
    l.map_column('COMMENTS',"Comments")
    l.ignore_column('CREATEDATE')
    l.ignore_column('CREATEDBYUSER')
    l.ignore_column('LASTMODIFIEDUSER')
    l.ignore_column('LASTMODIFIEDDATE')
    l.ignore_column('SUBMITDATE')
    l.ignore_column('UPLOAD_FILENAME')
    l.ignore_column('ETL_LOAD_DATE')
    l.ignore_column('ETL_UPDATE_DATE')
    l.process_rows()
    connection.commit()
  except Exception as e:
    logging.info("There was an error while loading worksheet name " + each_sheetname + " in " + File_Name +  " file. The error is: " + str(e))
    Notifications_Error_Type = 1
    Temp_Error = True
  if Temp_Error == False:
    if l.get_rows_rejected() > 0:
      logging.info (File_Name + " : Processing Done (" + str(l.get_rows_loaded())+ " rows loaded," + str(l.get_rows_rejected()) + " rows rejected).")
      for each_table in Table_Names_List:
        Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,TYPE,"Data",each_table)
      Notifications_Error_Type = 1
    else:
      logging.info (File_Name + " : Processing Done (" + str(l.get_rows_loaded())+ " rows loaded," + str(l.get_rows_rejected()) + " rows rejected).")
 
  Update_MINQ_Table (COMMUNITY_QUARTLY_REPORT_2_Table_Name,Instution_Number,TYPE,STATUSID,FISCALYEAR,Quarter,Census_Date,MONTH,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,File_Name)

def Process_HDCS_COMMUNITY_QUARTLY_REPORT_3(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,each_sheetname,Excel_File_Data):
  global Notifications_Error_Type
  global Files_Location
 
  #First check is it resubmission. If so, we have to remove the old data
  Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,TYPE,"Data",COMMUNITY_QUARTLY_REPORT_3_Table_Name)
 
  Temp_Error = False
  p = {'file_name' : Files_Location + File_Name,
        'table_name' : COMMUNITY_QUARTLY_REPORT_3_Table_Name,
        'map_columns' : "by_name",
        'header_pos'  : 0,
        'skiprows' : 0,
        'sheet_name' : each_sheetname,
        'truncate_table' : False,
        'charset' : 'American_America.UTF8',
        'date_format' : "mm/dd/yyyy",
        'quoting' : 'all',
        'quotechar': '"'
      }
  try:
    l = xld.xls_loader(p)
    l.ignore_column('INST')
    l.ignore_column('TYPE')
    l.ignore_column('CENSUSDATE')
    l.ignore_column('QUARTER')    
    l.ignore_column('STATUSID')
    l.ignore_column('FISCALYEAR')
    l.ignore_column('MONTH')
    l.map_column('CATEGORY_DESC',"Category")
    l.map_column('DESCRIPTION',"Description")
    l.map_column('OHRS',"OHRS")
    l.map_column('VALUE',"Value")
    l.ignore_column('CREATEDATE')
    l.ignore_column('CREATEDBYUSER')
    l.ignore_column('LASTMODIFIEDUSER')
    l.ignore_column('LASTMODIFIEDDATE')
    l.ignore_column('SUBMITDATE')
    l.ignore_column('UPLOAD_FILENAME')
    l.ignore_column('ETL_LOAD_DATE')
    l.ignore_column('ETL_UPDATE_DATE')
    l.process_rows()
    connection.commit()
  except Exception as e:
    logging.info("There was an error while loading worksheet name " + each_sheetname + " in " + File_Name +  " file. The error is: " + str(e))
    Notifications_Error_Type = 1
    Temp_Error = True
  if Temp_Error == False:
    if l.get_rows_rejected() > 0:
      logging.info (File_Name + " : Processing Done (" + str(l.get_rows_loaded())+ " rows loaded," + str(l.get_rows_rejected()) + " rows rejected).")
      for each_table in Table_Names_List:
        Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,TYPE,"Data",each_table)
      Notifications_Error_Type = 1
    else:
      logging.info (File_Name + " : Processing Done (" + str(l.get_rows_loaded())+ " rows loaded," + str(l.get_rows_rejected()) + " rows rejected).")
 
  Update_MINQ_Table(COMMUNITY_QUARTLY_REPORT_3_Table_Name,Instution_Number,TYPE,STATUSID,FISCALYEAR,Quarter,Census_Date,MONTH,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,File_Name)

def Process_HDCS_COMMUNITY_QUARTLY_REPORT_4(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,each_sheetname,Excel_File_Data):
  global Notifications_Error_Type
  global Files_Location
 
  #First check is it resubmission. If so, we have to remove the old data
  Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,TYPE,"Data",COMMUNITY_QUARTLY_REPORT_4_Table_Name)
 
  Temp_Error = False
  p = {'file_name' : Files_Location + File_Name,
        'table_name' : COMMUNITY_QUARTLY_REPORT_4_Table_Name,
        'map_columns' : "by_name",
        'header_pos'  : 0,
        'skiprows' : 0,
        'sheet_name' : each_sheetname,
        'truncate_table' : False,
        'charset' : 'American_America.UTF8',
        'date_format' : "mm/dd/yyyy",
        'quoting' : 'all',
        'quotechar': '"'
      }
  try:
    l = xld.xls_loader(p)
    l.ignore_column('INST')
    l.ignore_column('TYPE')
    l.ignore_column('CENSUSDATE')
    l.ignore_column('QUARTER')    
    l.ignore_column('STATUSID')
    l.ignore_column('FISCALYEAR')
    l.ignore_column('MONTH')
    l.map_column('TPBE',"TPBE")    
    l.map_column('EDIT_CHECKS',"Edit Checks")
    l.map_column('VALUE',"Value")
    l.ignore_column('CREATEDATE')
    l.ignore_column('CREATEDBYUSER')
    l.ignore_column('LASTMODIFIEDUSER')
    l.ignore_column('LASTMODIFIEDDATE')
    l.ignore_column('SUBMITDATE')
    l.ignore_column('UPLOAD_FILENAME')
    l.ignore_column('ETL_LOAD_DATE')
    l.ignore_column('ETL_UPDATE_DATE')
    l.process_rows()
    connection.commit()
  except Exception as e:
    logging.info("There was an error while loading worksheet name " + each_sheetname + " in " + File_Name +  " file. The error is: " + str(e))
    Notifications_Error_Type = 1
    Temp_Error = True
  if Temp_Error == False:
    if l.get_rows_rejected() > 0:
      logging.info (File_Name + " : Processing Done (" + str(l.get_rows_loaded())+ " rows loaded," + str(l.get_rows_rejected()) + " rows rejected).")
      for each_table in Table_Names_List:
        Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,TYPE,"Data",each_table)
      Notifications_Error_Type = 1
    else:
      logging.info (File_Name + " : Processing Done (" + str(l.get_rows_loaded())+ " rows loaded," + str(l.get_rows_rejected()) + " rows rejected).")
     
  Update_MINQ_Table(COMMUNITY_QUARTLY_REPORT_4_Table_Name,Instution_Number,TYPE,STATUSID,FISCALYEAR,Quarter,Census_Date,MONTH,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,File_Name)

def Process_MINQ_File(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate):
  global Files_Location
  global Notifications_Error_Type
  Excel_File = pd.ExcelFile(Files_Location + File_Name)
  Excel_File_SheetNames = Excel_File.sheet_names
  Excel_File_Data = ''
 
  for each_sheetname in Excel_File_SheetNames:
    if "upload1" == each_sheetname.replace(" ","").lower():
      logging.info("Processing : " + str(each_sheetname))
      Process_HDCS_COMMUNITY_QUARTLY_REPORT_1(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,each_sheetname,Excel_File_Data)
    elif "upload2" == each_sheetname.replace(" ","").lower():
      logging.info("Processing : " + str(each_sheetname))
      Process_HDCS_COMMUNITY_QUARTLY_REPORT_2(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,each_sheetname,Excel_File_Data)
    elif "upload3" == each_sheetname.replace(" ","").lower():
      logging.info("Processing : " + str(each_sheetname))
      Process_HDCS_COMMUNITY_QUARTLY_REPORT_3(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,each_sheetname,Excel_File_Data)
    elif "upload4" == each_sheetname.replace(" ","").lower():
      logging.info("Processing : " + str(each_sheetname))
      Process_HDCS_COMMUNITY_QUARTLY_REPORT_4(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,each_sheetname,Excel_File_Data)
    else:
      continue
     
def Update_MINQ_Table (Table_Name,Instution_Number,TYPE,STATUSID,FISCALYEAR,Quarter,Census_Date,MONTH,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate,File_Name):
  global Notifications_Error_Type
  if str(CreateDate) == "None":
    CreateDate = "1900-01-01"
  if str(LastModifiedDate) == "None":
    LastModifiedDate = "1900-01-01"
  if str(SubmitDate) == "None":
    SubmitDate = "1900-01-01"
   
  logging.info(Table_Name + " being updated..")
  Update_Data_Table_Query = (""" Update {data_table} t1 SET INST = '{instutuion_number}' ,  TYPE = '{Type}', CENSUSDATE = to_timestamp('{censusdate}','YYYY-MM-DD HH24:MI:SS.ff'), QUARTER = '{quarter}',
  STATUSID = '{StatusID}',MONTH = '{Month}', FISCALYEAR = '{fiscalyear}', CREATEDATE = to_timestamp('{createddate}','YYYY-MM-DD HH24:MI:SS.ff'), CREATEDBYUSER = '{createdbyuser}',
  LASTMODIFIEDDATE = to_timestamp('{lastmodifieddate}','YYYY-MM-DD HH24:MI:SS.ff'), LASTMODIFIEDUSER = '{lastmodifieduser}', SUBMITDATE = to_timestamp('{submitdate}','YYYY-MM-DD HH24:MI:SS.ff'),
  Upload_FileName = '{FileName}' where t1.INST is null and t1.TYPE is null and t1.CENSUSDATE is null""".format(data_table = Table_Name, instutuion_number = Instution_Number, quarter = Quarter,
  Type = TYPE, censusdate = str(Census_Date)[:30], StatusID = STATUSID, Month = MONTH, fiscalyear = FISCALYEAR, createddate = str(CreateDate)[:30], createdbyuser = CreatedByUser,
  lastmodifieduser = LastModifiedUser, lastmodifieddate = str(LastModifiedDate)[:30], submitdate = str(SubmitDate)[:30], FileName = File_Name))
  try:
    cursor.execute(Update_Data_Table_Query)
    connection.commit()
  except Exception as e:
    logging.info ("Updating the ETL_LOAD_DATE,INST,TYPE,CENSUSDATE,STATUSID,MONTH,FISCALYEAR,SUBMISSION_FILE_NAME,CREATEDATE,CREATEDBYUSER,LastModifiedDate, " + \
    "LASTMODIFIEDUSER and SUBMITDATE fields of " + Table_Name + " was unsucessful. The error is " +  str(e) + ". The query is " + str(Update_Data_Table_Query))
    Notifications_Error_Type = 1
   
  logging.info("Data tables being clared to apply project requirements...")
  if Table_Name == Process_HDCS_COMMUNITY_QUARTLY_REPORT_2:  
    logging.info(Table_Name + " being cleared..")
    # Do not upload rows where [TPBE] is in list of [TPBE] where SUM(Columns D:I)=0 AND [Category] = "TOTAL REVENUE FUND TYPE 2"
    # This DELETE statement is removing rows from the specified table if the following conditions are met: 1. The category of the row matches one of the categories that have no 'X' values across the entire table.
    # 2. The sum of the values in columns D through I is zero. So, for each category where the sum of D to I is zero and there's no 'X' in any row's value,all the rows of that category will be deleted.
    Clear_Data_Table_Query = ("""delete from {data_table} where TPBE IN (SELECT TPBE FROM {data_table}
                              WHERE [Category] = 'TOTAL REVENUE FUND TYPE 2' AND ([D] + [E] + [F] + [G] + [H] + [I]) = 0)""".format(data_table = Table_Name))
    try:
      cursor.execute(Clear_Data_Table_Query)
      connection.commit()
    except Exception as e:
      logging.info ("Clearing the rows in " + Table_Name + " was unsucessful. The error is " +  str(e) +
      ". The query is " + str(Clear_Data_Table_Query))
      Notifications_Error_Type = 1
  elif Table_Name == Process_HDCS_COMMUNITY_QUARTLY_REPORT_3:
    logging.info(Table_Name + " being cleared..")
    # Do not upload rows where [Category] is in list of [Category] where count of [Value]='X' is 0.
    # The DELETE statement below is used to remove rows from the table where there are no entries with the value 'X' in the [Value] column for each category.
    # In other words, if a category doesn't have any 'X' values at all, all rows of that category will be deleted from the table.
    Clear_Data_Table_Query = ("""delete from {data_table} where category in (SELECT Category FROM {data_table}
                              GROUP BY Category HAVING SUM(CASE WHEN [Value] = 'X' THEN 1 ELSE 0 END) = 0)""".format(data_table = Table_Name))
    try:
      cursor.execute(Clear_Data_Table_Query)
      connection.commit()
    except Exception as e:
      logging.info ("Clearing the rows in " + Table_Name + " was unsucessful. The error is " +  str(e) +
      ". The query is " + str(Clear_Data_Table_Query))
      Notifications_Error_Type = 1
  elif Table_Name == Process_HDCS_COMMUNITY_QUARTLY_REPORT_4:
    logging.info(Table_Name + " being cleared..")
    # Do not upload rows where [TPBE] not in list of [TPBE]s to upload from UPLOAD2
    # Using the pattern from Process_HDCS_COMMUNITY_QUARTLY_REPORT_3 as a basis, here we're negating the condition.
    # Instead of checking for a count of 'X' being 0, we're interested in TPBEs which should not be uploaded according to REPORT_2's logic.
    # This translates to deleting TPBEs where they are NOT found in the list of TPBEs that would have been uploaded from REPORT_2.
    Clear_Data_Table_Query = (""" DELETE FROM {data_table} WHERE TPBE NOT IN (SELECT TPBE FROM {data_table}
                              WHERE Category = 'TOTAL REVENUE FUND TYPE 2' AND (ColumnD + ColumnE + ColumnF + ColumnG + ColumnH + ColumnI) = 0)""".format(data_table=Table_Name))
    try:
      cursor.execute(Clear_Data_Table_Query)
      connection.commit()
    except Exception as e:
      logging.info ("Clearing the rows in " + Table_Name + " was unsucessful. The error is " +  str(e) +
      ". The query is " + str(Clear_Data_Table_Query))
      Notifications_Error_Type = 1
               
#This function help us to Process Meta Information File which ends with .csv. We are loading to db without changing anyting.
def Process_MetaInformation_File(each_file):
  global Notifications_Error_Type
   
  metainfo_df = pd.read_csv(Files_Location + each_file,sep='|')
  if len(metainfo_df) == 0:
    return 0
  logging.info("Checking and removing duplications from " + MetaInformation_Table_Name + " table.")
  for row_index,row in metainfo_df.iterrows():
    Remove_Duplications_From_Database('',row['Inst'],row['Type'],row['CensusDate'],'Meta',MetaInformation_Table_Name)
  metainfo_df = ''
  logging.info("Loading " + each_file + " file into " + MetaInformation_Table_Name + " table.")
  cp = {'file_name' : Files_Location + each_file,
        'table_name' : MetaInformation_Table_Name,
        'quoting' : 'none',
        'delimiter' : "|",
        'has_header' : True,
        'map_columns' : "by_position",
        'date_format' : "yyyy-mm-dd hh24:mi:ss.ff",
        'truncate_table' : False,
        'truncate_input' : True
       }
  cl = ldr.csv_loader(cp)
  cl.process_rows()
 
  if cl.get_rows_rejected() > 0:
    logging.info ("There is an error while loading data into " + MetaInformation_Table_Name + " table. "+ str(cl.get_rows_loaded()) + " rows loaded," + str(cl.get_rows_rejected()) + " rows rejected.")
    Notifications_Error_Type =  1
  else:
    logging.info ("Done (" + str(cl.get_rows_loaded()) + " rows loaded)")
  return 1

#This function help us to retrieve which MetaInformations not processed yet by using ETL_LOAD_DATE column.  
def Get_Latest_MetaInformation_Load():
  global Notifications_Error_Type
  logging.info ("Reading latest data from " + MetaInformation_Table_Name + " table.")
  Select_Query_MetaInformation = ("""Select * from {delta_table} where etl_load_date is null""".format(delta_table = MetaInformation_Table_Name))
  try:
    cursor.execute(Select_Query_MetaInformation)
    Select_Query_MetaInformation_Result = cursor.fetchall()
    connection.commit()
  except Exception as e:
    error = e.args
    logging.info("There is an error while retrieveing MetaInformation file from " + MetaInformation_Table_Name +" table. System quitted. The error is: " + str(error.message))
    Notifications_Error_Type =  2
    return 2
  return Select_Query_MetaInformation_Result

#This is the function which finds duplications from given tables with given information. If there are any, we are deleting records from table.  
def Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,File_Type,Duplication_Type,Table_Name):
  global Notifications_Error_Type
  #Prepare Queries to use in the function
  Select_Query_in_Table = ("select * from " + Table_Name + " where INST = '" + str(Instution_Number) + "' and TYPE='" + str(File_Type) + "' and CENSUSDATE = to_timestamp('" +
    str(Census_Date)[:30] + "','YYYY-MM-DD HH24:MI:SS.ff')")
  Delete_Query_in_Table = ("delete from " + Table_Name + " where INST = '" + str(Instution_Number) + "' and TYPE='" + str(File_Type) + "' and CENSUSDATE = to_timestamp('" +
    str(Census_Date)[:30] + "','YYYY-MM-DD HH24:MI:SS.ff')")

  if Table_Name == Attachment_Table_Name:
    Select_Query_in_Table = Select_Query_in_Table + " and url like '%" + File_Name + "%'"
    Delete_Query_in_Table = Delete_Query_in_Table + " and url like '%" + File_Name + "%'"
   
  #Run the queries to find duplication records.
  try:
    cursor.execute(Select_Query_in_Table)
    Select_Query_Result = cursor.fetchall()
    connection.commit()
  except Exception as e:
    logging.info("There is an error while checking the duplication of a " + str(Duplication_Type).lower() + " file. The file name is " + File_Name + " The error is " + str(e))
    Notifications_Error_Type = 1
    os.chdir(Files_Location)
    return 1
  os.chdir(Files_Location)
 
  #If there are any duplication record found, we are deleting....
  if len(Select_Query_Result) > 0:
    try:
      cursor.execute(Delete_Query_in_Table)
      connection.commit()
      if Table_Name != MetaInformation_Table_Name:
        logging.info (str(cursor.rowcount) + " rows removed from table to avoid duplications.")
    except Exception as e:
      logging.info("There is an error while deleting the duplication of a " + str(Duplication_Type).lower() + " file. The file name is " + File_Name)
      Notifications_Error_Type = 1

#To avoid duplications on Azure container, we are searching the files and remove them...  
def Remove_Duplications_From_Azure(File_Name, container_name):
  global Notifications_Error_Type
  try:
    i=0
    #disable logging temporarily by increasing the level to critical
    logging.disable(logging.CRITICAL)
   
    for each_file in azure_blob.list_blob(container_name):
      if str(Azure_Folder + File_Name) == each_file:
        logging.info ("Removing the " + File_Name + " from Azure location to avoid duplicates.")
        azure_blob.delete_blob(container_name,Azure_Folder+File_Name)
        i = i + 1
  except Exception as e:
    logging.info ("Removing the " + File_Name + " from " + Azure_Folder + " location was unsucessful. The error is: " + str(e))
    Notifications_Error_Type = 1
  #enable logging
  logging.disable(logging.NOTSET)  
 
#This is the main function of loading files to Azure Container bucket based on their name....
def Upload_File_to_Azure(File_Name):
  global Notifications_Error_Type

  try:
    #disable logging temporarily by increasing the level to critical
    logging.disable(logging.CRITICAL)
    retain_days = 3650
    File_URL = azure_blob.upload_blob(Azure_Container,Files_Location+File_Name,Azure_Folder + File_Name,retain_days)
  except Exception as e:
    logging.info("There is an error while loading the " + File_Name + " file to Azure " + Azure_Container + ". The error is " + str(e))
    Notifications_Error_Type = 1
    return Notifications_Error_Type
 
  #enable logging
  logging.disable(logging.NOTSET)  
  return File_URL
 
#This is the main function of procesing Attachment files.
def Process_Attachment_Files(File_Name,Instution_Number,FISCALYEAR,Census_Date,File_Type):
  global Notifications_Error_Type
 
  #First check is it resubmission. If so, we have to remove the old data  
  Remove_Duplications_From_Database(File_Name,Instution_Number,Census_Date,File_Type,"Attachment",Attachment_Table_Name)
  Remove_Duplications_From_Azure(File_Name,Azure_Container)
 
  #Preparing the variable to use next lines.
  os.chdir(Files_Location)
 
  #Calling the Upload_File_to_Azure function to upload new files to Azure container...  
  File_URL = Upload_File_to_Azure(File_Name)
 
  if File_URL == Notifications_Error_Type:
    return Notifications_Error_Type
 
  #Also we are uploading the the files with Azure URL to database.
  Insert_Statement = """insert into {attachment_table} (INST, TYPE, FISCALYEAR, CENSUSDATE, URL,TOTAL_FILES)
  values
  ('{inst}','{Type}','{fiscalyear}',to_timestamp('{CensusDate}','YYYY-MM-DD HH24:MI:SS.ff'),'{file_url}',{attach_file_cnt})
  """.format(attachment_table=Attachment_Table_Name, inst=Instution_Number,
  Type=File_Type, fiscalyear=FISCALYEAR,CensusDate=str(Census_Date)[:30], file_url=File_URL,attach_file_cnt= 1)
  try:
    cursor.execute(Insert_Statement)
    connection.commit()
  except Exception as e:
    logging.info (File_Name + " couldnt loaded to database " + Attachment_Table_Name + " table. The error is " + str(e))
    Notifications_Error_Type = 1
   
  #If we are not in test mode, we dont have to keep attachment files in our hdd
  if Test_Mode == False:
    os.remove(Files_Location + File_Name)
   
#This function help us to update Total_Files field to how many files... We are using Group By function using INST,TYPE and CENSUSDATE columns.
def Update_Attachment_Table(Attachment_Count,Instution_Number,Census_Date,TYPE,Quarter):
  global Notifications_Error_Type
  logging.info ("Updating the " + Attachment_Table_Name + " for TOTAL_FILES Column")
 
  Update_Attachment_Table = """ Update {attachment_table} t1 SET TOTAL_FILES = {total_count},ETL_LOAD_DATE = to_timestamp('{today_date}','YYYY-MM-DD HH24:MI:SS.ff'),QUARTER = '{quarter}'
  where t1.INST = '{inst}' and ETL_LOAD_DATE is NULL and t1.TYPE = '{type1}' and t1.CENSUSDATE = to_timestamp('{CensusDate}','YYYY-MM-DD HH24:MI:SS.ff')""".format(attachment_table = Attachment_Table_Name,
  total_count = Attachment_Count,inst=Instution_Number,type1=TYPE,CensusDate=str(Census_Date)[:30].replace("-",""),today_date = str(datetime.now())[:30],quarter=Quarter)
  try:
    cursor.execute(Update_Attachment_Table)
    connection.commit()
  except Exception as e:
    logging.info ("Updating the Total Numbers of files unsucessful. The error is " +  str(e))
    Notifications_Error_Type = 1
     
#This function Updates Submission table to populate ETL_LOAD_DATE. This population help us to flag which submissions have been processed.    
def Update_MetaInformation_Table():
  global Notifications_Error_Type
  logging.info(MetaInformation_Table_Name + " being updated..")
  Update_MetaInformation_Table_Query = (""" Update {MetaInformation_table} t1 SET ETL_LOAD_DATE = to_timestamp('{today_date}','YYYY-MM-DD HH24:MI:SS.ff')
  where t1.ETL_LOAD_DATE is null""".format(MetaInformation_table = MetaInformation_Table_Name,today_date = str(datetime.now())[:30]))
  try:
    cursor.execute(Update_MetaInformation_Table_Query)
    connection.commit()
  except Exception as e:
    logging.info ("Updating the ETL_LOAD_DATE of " + MetaInformation_Table_Name + " was unsucessful. The error is " +  str(e))
    Notifications_Error_Type = 1
   
#This function moves all loaded database tables to SAS.
def Move_Bida_to_Sas():
  global Notifications_Error_Type
  #We are starting with Data_Table to move
  logging.info("Starting move tables to " + Schema)
  for each_table in Table_Names_List:
    #Before we move, we have to drop it first.
    try:
      print("Truncate old " + str(each_table) + " from schema of " + str(Schema))
      cursor.callproc('SASADMIN.truncate_table', [Schema, str(each_table)])
      try:
        logging.info(str(each_table) + " is being moved")
        cursor.callproc('SASADMIN.append_table', [Schema,str(each_table), '_mybida', str(each_table)])
        connection.commit()
      except cx_Oracle.DatabaseError as e:
        # there was an Oracle error
        error, = e.args
        connection.rollback()
        connection.close()
        print ("ERROR: Unexpected Oracle error.")
        print (error.message)
        logging.info("There was an error while moving " + str(each_table) + " to " + Schema + " . The error is: " + str(error.message))
        Notifications_Error_Type = 1
    except cx_Oracle.DatabaseError as e:
        # there was an Oracle error
        error = e.args[0]
        connection.rollback()
        logging.info("There was an error while dropping " + str(each_table) + " from " + Schema + " . The error is: " + str(error.message))
        Notifications_Error_Type = 1
     
def main(Test_Mode):
  global Notifications_Error_Type
  global Files_Location

  #Searchin Detail file which is .csv... We assume that there is file name convention which ends with metainformation.csv
  MetaInformation_Detail_File_Found =  False
  for each_file in sorted(os.listdir(Files_Location)):
    if "metainformation.csv" in each_file.lower():
      MetaInfo_Length = Process_MetaInformation_File(each_file)
      if MetaInfo_Length == 0:
        logging.info("There is no data to process in " + each_file)
        return 0
      MetaInformation_Detail_File_Found = True
     
  #If the MetaInformation file not found. We wont process anything.
  if MetaInformation_Detail_File_Found == False:
    logging.info("There is no MetaInformation data file found with MetaInformation.csv in name")
    Notifications_Error_Type = 2
    return 2
   
  #After Loading Todays MetaInformation, start to process them
  #We are calling the data from MetaInformation table which has ETL_LOAD_DATE is empty.
  Latest_MetaInformation_Load = Get_Latest_MetaInformation_Load()
 
  if Notifications_Error_Type == Latest_MetaInformation_Load:
    return Notifications_Error_Type
   
  #Based on the return location we know that which file is attachment and which one is the Data file.
  for each_load in Latest_MetaInformation_Load:
    Instution_Number = str(each_load[0])
    TYPE = str(each_load[1])
    Census_Date = each_load[2]
    STATUSID = str(each_load[3])
    FISCALYEAR = str(each_load[5])
    MONTH = str(each_load[4])
    File_Name = str(each_load[6])
    if 1 <= Census_Date.month <= 3:
      Quarter = "Q4"
    elif 4 <= Census_Date.month <= 6:
      Quarter = "Q1"
    elif 7 <= Census_Date.month <= 9:
      Quarter = "Q2"
    elif 10 <= Census_Date.month <= 12:
      Quarter = "Q3"
    Attachment_1 = str(each_load[8])
    Attachment_2 = str(each_load[9])
    Attachment_3 = str(each_load[10])
    Attachment_4 = str(each_load[11])
    Attachment_5 = str(each_load[12])
    CreateDate = each_load[23]
    CreatedByUser = str(each_load[24])
    LastModifiedUser = str(each_load[25])
    LastModifiedDate =  each_load[26]
    SubmitDate = each_load[27]
   
    #Process Attachment files
    #First we check that is there any attachment file or not.file or not.
    Attachment_Count = 0
    if Attachment_1 == "None" and Attachment_2 == "None" and Attachment_3 == "None" and Attachment_4 == "None" and Attachment_5 == "None":
      logging.info ("There is no attachment for INST = " + str(Instution_Number) + " CensusDate = " + str(Census_Date)[:10] + " on SubmitDate = " + str(SubmitDate)[:10])
    else:
      if Attachment_1 != "None":
        Attachment_Count += 1
        if os.path.isfile(Files_Location + Attachment_1):
          logging.info (Attachment_1 + " : started to processing...")
          Process_Attachment_Files(Attachment_1,Instution_Number,FISCALYEAR,Census_Date,TYPE)
        else:
          logging.info(Attachment_1 + " is not a proper file to process.. Please check")
          Notifications_Error_Type = 1
      if Attachment_2 != "None":
        Attachment_Count += 1
        if os.path.isfile(Files_Location + Attachment_2):
          logging.info (Attachment_2 + " : started to processing...")
          Process_Attachment_Files(Attachment_2,Instution_Number,FISCALYEAR,Census_Date,TYPE)
        else:
          logging.info(Attachment_2 + " is not a proper file to process.. Please check")
          Notifications_Error_Type = 1
      if Attachment_3 != "None":
        Attachment_Count += 1
        if os.path.isfile(Files_Location + Attachment_3):
          logging.info (Attachment_3 + " : started to processing...")
          Process_Attachment_Files(Attachment_3,Instution_Number,FISCALYEAR,Census_Date,TYPE)
        else:
          logging.info(Attachment_3 + " is not a proper file to process.. Please check")
          Notifications_Error_Type = 1
      if Attachment_4 != "None":
        Attachment_Count += 1
        if os.path.isfile(Files_Location + Attachment_4):
          logging.info (Attachment_4 + " : started to processing...")
          Process_Attachment_Files(Attachment_4,Instution_Number,FISCALYEAR,Census_Date,TYPE)
        else:
          logging.info(Attachment_4 + " is not a proper file to process.. Please check")
          Notifications_Error_Type = 1
      if Attachment_5 != "None":
        Attachment_Count += 1
        if os.path.isfile(Files_Location + Attachment_5):
          logging.info (Attachment_5 + " : started to processing...")
          Process_Attachment_Files(Attachment_5,Instution_Number,FISCALYEAR,Census_Date,TYPE)
        else:
          logging.info(Attachment_5 + " is not a proper file to process.. Please check")
          Notifications_Error_Type = 1
         
    #Process Data file
    if File_Name != "None" and STATUSID == "2":
      logging.info (File_Name + " : started to processing...")
      os.chdir(Files_Location)
     
      #Call the function to populate database table with MetaInformation Files data.
      Process_MINQ_File(Instution_Number,TYPE,Census_Date,Quarter,STATUSID,FISCALYEAR,MONTH,File_Name,CreateDate,CreatedByUser,LastModifiedUser,LastModifiedDate,SubmitDate)
    else:
      logging.info ("There is no data file for INST = " + str(Instution_Number) + " CensusDate = " + str(Census_Date)[:10] + " on SubmitDate = " + str(SubmitDate)[:10])

    #Update Attachment table to populate how many files for each attachment by using Institution Number, Census Date and File Type.
    Update_Attachment_Table(Attachment_Count,Instution_Number,Census_Date,TYPE,Quarter)
  #Updateding MetaInformation table to populate ETL LOAD DATE. We are going to flag which MetaInformations have been processed already.
  Update_MetaInformation_Table()
  #Moving Bida tables to SAS environment
  Move_Bida_to_Sas()
  return Notifications_Error_Type
 
#Set the log configuation and call the main function
if __name__ == "__main__":
  print("Test mode: " + str(Test_Mode))
  print('today date: ', Today_Date)
  #print('Sftp Folder: ', Sftp_Folder)
 
  Log_File = Parent_Location+"/logs/MINQ/HDCS_MINQ_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')+ ".log"
  logging.basicConfig(filename=Log_File, filemode='w',format='%(asctime)s%(message)s',level=logging.INFO,datefmt='%d/%m/%Y %I:%M:%S %p:')
 
  logging.info("Script: " + Script_Location)

  result = main( Test_Mode)
 
  if Test_Mode:
    Email_Recipients = ['syamal.reddy@ontario.ca']
  else:
    Email_Recipients = ['syamal.reddy@ontario.ca']

  print('Email Recipients: ' + str(Email_Recipients))
  print('Notification Type: ' + str(Notifications_Error_Type))
  #Send notifications based on Notifications_Error_Type variable
  if result == 0:
    subject = "SRI HOSPQ processed successfully"
  elif result == 1:
    subject = "SRI HOSPQ processed with some errors"
  elif result == 2:
    subject = "SRI HOSPQ process failed"

  os.chdir(Parent_Location + "/LOGS/")
  Log_File = str(Log_File).rsplit("/",1)[1]
  email.etl_notification(Email_Recipients,{},subject, '',msg_file = Log_File)