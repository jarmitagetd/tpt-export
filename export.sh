#!/bin/bash
#-------------------------------------------------------------------------------#
#  Project:     TPT Export Project                                              #
#  Description: Transaction data extraction using Teradata Parralel Transporter #
#  Version:     1.0                                                             #
#  Release      Optimised TPT job - multiple instances of operators in one job  #
#  Style:       https://google.github.io/styleguide/shell.xml                   #
#  Github:      https://github.com/jarmitagetd/tpt-export                       #
#  Date:        05/09/2017                                                      #
#  Author:      James Armitage                                                  #
#  Company:     Teradata                                                        #
#  Email:       james.armitage@teradata.com                                     # 
#-------------------------------------------------------------------------------#


#-------------------------------------------------------------------------------#
# STEP 1                                                                        #
# start logging clear screen for interactive terminal session                   #
# declare environmental variables, constants and initial variables              #
# set present working directory for execution and put the Linux PID             #
# (process id) into the file pid                                                #
#-------------------------------------------------------------------------------#

clear
# add tpt to PATH
PATH=$PATH:/opt/teradata/client/15.10/bin/tbuild

# echo pid to file
echo $$ > pid

# cd to pwd
cd "$(dirname "$0")"

# declare constants
readonly PRM="vtpt"
readonly FAC="objects.txt"
readonly LOG="log/"
readonly SQL="sql/"
readonly REP="report/"
readonly ARC="archive/"
readonly EQL="="
readonly JLG="job.log"

# assign values from tpt.ini to variables
tdp=$(cat $PRM | grep TdpId | awk '{print $3}')
usr=$(cat $PRM | grep UserName | awk '{print $3}')
pas=$(cat $PRM | grep UserPassword | awk '{print $3}')
dlm=$(cat $PRM | grep Delimiter | awk '{print $3}')
qtd=$(cat $PRM | grep Quotes | awk '{print $3}')
out=$(cat $PRM | grep DirectoryPath | awk '{print $3}')
epl=$(cat $PRM | grep EXPrivateLogName | awk '{print $3}')
fpl=$(cat $PRM | grep FWPrivateLogName | awk '{print $3}')
ans=$(cat $PRM | grep DateForm | awk '{print $3}')
ind=$(cat $PRM | grep IndicatorMode | awk '{print $3}')
fmt=$(cat $PRM | grep Format | awk '{print $3}')
mxs=$(cat $PRM | grep MaxSessions | awk '{print $3}')
mns=$(cat $PRM | grep MinSessions | awk '{print $3}')

# check if max and min sessions have a value and if so add '='
if ! [ -z "$mxs" ] && ! [ -z "$mns" ]
then
  mxs=$(echo = "$mxs") 
  mns=$(echo = "$mns")
fi

# declare local variables
# current date and time
function datetime 
{
  dat=$(date +%Y%m%d | tr -d /)
  tme=$(date +%H:%M:%S| tr -d :)  
}

#-------------------------------------------------------------------------------#
# STEP 2                                                                        #
# Set up to_do and done file process for bash scipt checkpoint for objects      #
# read in databases, objects, start and end date from file objects.txt          #
# calculate dates between start and end date for loop                           #
# build tpt scripts - for transactions process all transaction days for 1 month #
# in one script.  This is an optimal approach as removes overhead incurred by   #
# tpt job instantiation                                                         #
#-------------------------------------------------------------------------------#

# remove previously generated tpt script if it exists
if [[ -f tpt ]]
then
  rm tpt
fi

# copy objects.txt to to_do.txt file if the to_do.txt file does not exist /
# if it exists diff with done.txt to get updated to_do list
# tpt handles the checkpoint restart if the object job falls over mid run
if [ -f to_do.txt ] && [ -f done.txt ]; then
   # append to log file 
   exec &> >(tee -a job.log)
   # assign diff output to variable z
   z=$(diff "to_do.txt" "done.txt")
   # using awk to pipe the object names to to do file
   echo "$z" | grep "<" | awk '{print $2}' > to_do.txt
else
   # start logging - new log file
   exec &> >(tee job.log)
   # copy all reference data objects from refs.txt into to to_do.txt
   cp "$FAC" "to_do.txt"
   # create rowcount file
   echo "DATE,JOBNAME,ROWCOUNT" > rowcount.txt
   # create file for export operator start times
   echo START > jobstart.txt
   # create file for export operator end times
   echo END > jobend.txt
fi

# assign to_do.txt to variable
to_do=to_do.txt

# read file in ($fli) to get database,object name
while read -r str
do
  # test $str has a value - if not exit read loop 
  if [[ -z "$str" ]]
  then
    exit
  fi
  # get database from $str
  dbs=$(echo "$str" | awk -F "|" '{print $1}')
  # get tablename from $str
  tbl=$(echo "$str" | awk -F "|" '{print $2}')
  # get ppi flag from $str
  #ppi=$(echo "$str" | awk -F "|" '{print $3}')
  # get sql for tablename from sql file and replace databasename token
  sql=$(cat "$SQL$tbl.sql" | sed -e 's/{DATABASENAME}/'$dbs'/g')
  # get integer start date for transactional exports from $str (objects.txt)
  sdt=$(echo "$str" | awk -F "|" '{print $3}'| tr -d -)
  # get ANSI formated start date for transactional exports from $str (objects.txt)
  sdf=$(echo "$str" | awk -F "|" '{print $3}')
  # get integer end date for transactional exports from $str (objects.txt)
  edt=$(echo "$str" | awk -F "|" '{print $4}'| tr -d -)

  # calculate start and end date difference in days
  let ddf=(`date +%s -u -d "$edt"`-`date +%s -u -d "$sdt"`)/86400
  
  # assign min and max values for loop based on number of days between start /
  # and end dates in tpt.ini
  min=0
  max=$ddf
  
  # if a start and end date is not specified then assume reference data and /
  # do not loop through dates. Reset variable for no loop
  if [[ -z "$sdt" ]]
  then
    max=0
    datetime
    sdt="$dat"
  fi
  
  # generate export job defintion (file header)
  printf "DEFINE JOB EXPORT_%s_TO_FILE\n" "$tbl" >> tpt
  printf "DESCRIPTION 'EXPORT %s TO A FILE'\n" "$tbl" >> tpt 
  printf "(\n" >> tpt
  printf "\n" >> tpt
  # generate tpt schema operator defintion
  printf "DEFINE SCHEMA %s FROM SELECT OF OPERATOR EXPORT_OPERATOR_%s_%s();\n" "$tbl" "$tbl" "$sdt" >> tpt
  #printf   "DEFINE SCHEMA %s FROM TABLE '%s.%s';\n" "$tbl" "$dbs" "$tbl" >> tpt         
  printf "\n" >> tpt
  # use for loop to generate date sequence
  for i in `seq $min $max`;
  do
    # increment dates by seq number using date function
    dte=$(date -u -d "$sdf   $i days" +%Y-%m-%d)
    dti=$(echo $dte | tr -d '-') 
    
    #  test for ppi column to identify transactions
    if [[ -z "$sdt" ]]
    then
      dti="$dat"
    fi
    
    # get sql for tablename from sql file and replace databasename token
    sql=$(cat "$SQL$tbl.sql" | sed -e 's/{DATABASENAME}/'$dbs'/g')
    
    # get sql and if there is a date token substitute with date $dte
    sql=$(echo "$sql" | sed -e 's/{DATE}/'\'$dte\''/g')
    
    # using printf for string substitution generate TPT job operators#
    # define export operator
    printf   "DEFINE OPERATOR EXPORT_OPERATOR_%s_%s()\n" "$tbl" "$dti" >> tpt 
    printf   "DESCRIPTION 'TERADATA PARALLEL TRANSPORTER EXPORT OPERATOR'\n" >> tpt
    printf   "TYPE EXPORT\n"   >> tpt
    printf   "SCHEMA %s\n" "$tbl"   >> tpt
    printf   "ATTRIBUTES\n"   >> tpt
    printf   "(\n" >> tpt
    printf    "VARCHAR PrivateLogName = '%s',\n" "$epl" >> tpt
    printf    "INTEGER MaxSessions %s,\n" "$mxs" >> tpt
    printf    "INTEGER MinSessions =  1,\n" >> tpt
    printf    "VARCHAR TdpId = '%s',\n" "$tdp" >> tpt
    printf    "VARCHAR UserName = 'james.armitage',\n" >> tpt
    printf    "VARCHAR UserPassword = '%s',\n" "$pas" >> tpt
    printf    "VARCHAR AccountId,\n" >> tpt
    printf    "VARCHAR DateForm = '%s',\n" "$ans" >> tpt
    printf    "VARCHAR SelectStmt = '$sql;'\n" >> tpt
    printf    ");\n"   >> tpt
    printf    "\n" >> tpt
    # define file writer operator
    printf  "DEFINE OPERATOR FILE_WRITER_%s_%s()\n" "$tbl" "$dti"  >> tpt
    printf  "DESCRIPTION 'TERADATA PARALLEL TRANSPORTER DATA CONNECTOR OPERATOR'\n"   >> tpt
    printf  "TYPE DATACONNECTOR CONSUMER\n"   >> tpt
    printf  "SCHEMA *\n"   >> tpt
    printf  "ATTRIBUTES\n"   >> tpt
    printf   "(\n"   >> tpt
    printf    "VARCHAR PrivateLogName = '%s',\n" "$fpl"  >> tpt
    printf    "VARCHAR FileName = '%s_%s.csv',\n" "$tbl" "$dti" >> tpt
    printf    "VARCHAR DirectoryPath = '%s',\n" "$out"  >> tpt
    printf    "VARCHAR IndicatorMode = '%s',\n" "$ind"   >> tpt
    printf    "VARCHAR OpenMode = 'Write',\n"   >> tpt
    printf    "VARCHAR Format = '%s',\n" "$fmt"   >> tpt
    printf    "VARCHAR TextDelimiter = '%s',\n" "$dlm"   >> tpt
    printf    "VARCHAR QuotedData = '%s'\n" "$qtd"  >> tpt
    printf   ");\n"   >> tpt
    printf  "\n" >> tpt
    # define step operator
    printf  "STEP EXPORT_TO_FILE_%s_%s\n" "$tbl" "$dti"  >> tpt
    printf   "(\n"   >> tpt
    printf    "APPLY TO OPERATOR (FILE_WRITER_%s_%s() )\n" "$tbl" "$dti"  >> tpt
    printf    "SELECT * FROM OPERATOR (EXPORT_OPERATOR_%s_%s() [1] );\n" "$tbl" "$dti"  >> tpt
    printf  ");\n"   >> tpt
    printf  "\n" >> tpt
  done
  # generate job close bracket (footer)
  printf ");" >> tpt
  # execute tpt script
  tbuild -f tpt
  # write out completed object to done file
  echo "$dbs"'|'"$tbl"'|'"$sdt"'|'"$edt" >> "done.txt"
  mv tpt "$tbl""_""$std""_""$edt".tpt
done < $to_do

#-------------------------------------------------------------------------------#
# STEP 3                                                                        #
# 1) build csv file for rowcounts - file imports into excel for quick           #
# reconciliation.                                                               #
# 2) build report file in csv format for rowcounts and additional metadata      #
#-------------------------------------------------------------------------------#

# call datetime function to get system date and time for archive file names
datetime

rpf="$REP"report"$dat"_"$tme".csv
acd="$ARC""tpt_""$dat""_""$tme"

# get rowcount from log file
cat job.log | grep "Total Rows Exported" \
| sed -e 's/ //g; s/^/'$dat':/g; s/:/,/g' \
| awk -F "," '{print $1,$2,$4}' \
| sed -e 's/ /,/g' >> rowcount.txt

# get job start and end times
cat job.log | grep "Start :" | awk '{print $7}' >> jobstart.txt
cat job.log | grep "End   :" | awk '{print $7}' >> jobend.txt
paste -d ',' rowcount.txt jobstart.txt jobend.txt >> $rpf

#-------------------------------------------------------------------------------#
# STEP 4                                                                        #
# 1) move all files excluding objects.txt to the job folder in archive          #
#                                                                               #
# 2) copy report file and objects.txt to the job folder                         #
#-------------------------------------------------------------------------------#

# if the job directory in archive does not exist create it
if [ ! -d "$acd" ]; then
  mkdir "$acd" 
fi

# move and copy the job files into archived job folder
mv *.log "$acd"
mv rowcount.txt jobstart.txt jobend.txt to_do.txt done.txt "$acd"
mv *.tpt "$acd"
mv pid "$acd"
cp "$rpf" "$acd"
