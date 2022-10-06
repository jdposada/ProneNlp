# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of ProneNlp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Load libraries
install.packages("tidyr")
library(dplyr)
library(stringr)

install.packages("DatabaseConnector")
library(DatabaseConnector)

library(SqlRender)
library(usethis)

install.packages("DBI")
library(DBI)

#Github credentials (and adds itself to .gitignore), if needed.
#source("githubCreds.R")

working_directory <- "/workdir/workdir/"
setwd(working_directory)

nlp_admission_summary <- "nlp_admission_summary"

nlp_output_filename_t1 <- "nlp_output_leo_t1.csv"
nlp_output_filename_t2 <- "nlp_output_leo_t2.csv"
nlp_output_filename_t3 <- "nlp_output_leo_t3.csv"
nlp_table_leo_output_t1 <- "leo_output_t1"
nlp_table_leo_output_t2 <- "leo_output_t2"
nlp_table_leo_output_t3 <- "leo_output_t3"
nlp_admission_summary_t1 <- "nlp_summary_t1"
nlp_admission_summary_t2 <- "nlp_summary_t2"
nlp_admission_summary_t3 <- "nlp_summary_t3"
target_cohort_id_t1 <- "141"
target_cohort_id_t2 <- "142"
target_cohort_id_t3 <- "143"
subset_table_name_t1 <- "note_t1"
subset_table_name_t2 <- "note_t2"
subset_table_name_t3 <- "note_t3"

leo_nlp_output_folder <- "/workdir/workdir/NLP_Results/"

jsonPath <- "/workdir/gcloud/application_default_credentials.json"
bqDriverPath <- "/workdir/workdir/BQDriver/"
project_id <- "som-nero-nigam-starr"
dataset_id <- "prone_nlp"


#cdm_database_schema <- ""
#defines cdm_database_schema and adds itself to .gitignore
source("/workdir/workdir/ProneNlp/cdmDatabaseSchema.R")
vocabulary_database_schema <- cdm_database_schema
target_database_schema <- "som-nero-nigam-starr.prone_nlp"
target_cohort_table <- "cohort"

# for BQ uploading
Sys.setenv(GOOGLE_APPLICATION_CREDENTIALS = jsonPath)
bigrquery::bq_auth(path=jsonPath)
Sys.setenv(GCLOUD_PROJECT = project_id)
gargle::credentials_app_default()

# Connect to Database

connectionString <-  BQJdbcConnectionStringR::createBQConnectionString(projectId = project_id,
                                                                       defaultDataset = dataset_id,
                                                                       authType = 2,
                                                                       jsonCredentialsPath = jsonPath)

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms="bigquery",
                                                                connectionString=connectionString,
                                                                user="",
                                                                password='',
                                                                pathToDriver = bqDriverPath)
# # Create a test connection
# connection <- DatabaseConnector::connect(connectionDetails)
# 
# sql <- "
# SELECT
#  COUNT(1) as counts
# FROM
#  `bigquery-public-data.cms_synthetic_patient_data_omop.care_site`
# "
# 
# counts <- DatabaseConnector::querySql(connection, sql)
# 
# print(counts)
DatabaseConnector::disconnect(connection)

# Create Cohort Table
renderedSql <- SqlRender::render(SqlRender::readSql("ProneNlp/inst/sql/sql_server/create_cohort_table.sql"),
                                 cdmDatabaseSchema=target_database_schema,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)


con = DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=con,
                              sql=translatedSql)
DatabaseConnector::disconnect(con)

# Cohort 1:

renderedSql <- SqlRender::render(SqlRender::readSql("ProneNlp/inst/sql/sql_server/target/covid_prone_T1_Baseline_Hosp_TestPos_OR_Condn.sql"),
                                 cdm_database_schema=cdm_database_schema,
                                 vocabulary_database_schema=vocabulary_database_schema,
                                 target_database_schema=target_database_schema,
                                 target_cohort_table=target_cohort_table,
                                 target_cohort_id=target_cohort_id_t1,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)


con = DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=con,
                              sql=translatedSql)
DatabaseConnector::disconnect(con)


# Cohort 2:
renderedSql <- SqlRender::render(SqlRender::readSql("ProneNlp/inst/sql/sql_server/target/covid_prone_T2_Proc_Excl.sql"),
                                 cdm_database_schema=cdm_database_schema,
                                 vocabulary_database_schema=vocabulary_database_schema,
                                 target_database_schema=target_database_schema,
                                 target_cohort_table=target_cohort_table,
                                 target_cohort_id=target_cohort_id_t2,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)


con = DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=con,
                              sql=translatedSql)
DatabaseConnector::disconnect(con)


# Cohort 3:
renderedSql <- SqlRender::render(SqlRender::readSql("ProneNlp/inst/sql/sql_server/target/covid_prone_T3_Proc_Excl_NewDex_032021.sql"),
                                 cdm_database_schema=cdm_database_schema,
                                 vocabulary_database_schema=vocabulary_database_schema,
                                 target_database_schema=target_database_schema,
                                 target_cohort_table=target_cohort_table,
                                 target_cohort_id=target_cohort_id_t3,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)


con = DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=con,
                              sql=translatedSql)
DatabaseConnector::disconnect(con)


# Subset the NOTE table to download only the notes for each cohort


subsetByPersonIdAndDate <- function(cdmTable, cohortId, cohortTable, cdmDatabaseSchema, 
                                  resultDatabaseSchema, subsetTableName, connectionDetails) {
  
  "
  Subset the CDM table by using person_id and cohort start and end date
  "
  setwd(working_directory)
  # get the appropriate date column
  dateColumn <- "note_date"
  
  renderedSql <- SqlRender::render(SqlRender::readSql("ProneNlp/inst/sql/sql_server/byPersonAndDate.sql"),
                                   resultDatabaseSchema=resultDatabaseSchema,
                                   cdmDatabaseSchema=cdmDatabaseSchema,
                                   cdmTable=cdmTable,
                                   cohortTable=cohortTable,
                                   cohortId=cohortId,
                                   dateColumn=dateColumn,
                                   subsetTableName=subsetTableName,
                                   warnOnMissingParameters = TRUE)
  
  translatedSql <- SqlRender::translate(sql=renderedSql,
                                        targetDialect = connectionDetails$dbms,
                                        tempEmulationSchema = target_database_schema)
  
  con = DatabaseConnector::connect(connectionDetails)
  DatabaseConnector::executeSql(connection = con, sql = translatedSql)
  DatabaseConnector::disconnect(con)
  
}


subsetByPersonIdAndDate(cdmTable="note",
                        cohortId=target_cohort_id_t1,
                        cohortTable=target_cohort_table,
                        cdmDatabaseSchema=cdm_database_schema, 
                        resultDatabaseSchema=target_database_schema,
                        subsetTableName=subset_table_name_t1,
                        connectionDetails=connectionDetails)

subsetByPersonIdAndDate(cdmTable="note",
                        cohortId=target_cohort_id_t2,
                        cohortTable=target_cohort_table,
                        cdmDatabaseSchema=cdm_database_schema, 
                        resultDatabaseSchema=target_database_schema,
                        subsetTableName=subset_table_name_t2,
                        connectionDetails=connectionDetails)


subsetByPersonIdAndDate(cdmTable="note",
                        cohortId=target_cohort_id_t3,
                        cohortTable=target_cohort_table,
                        cdmDatabaseSchema=cdm_database_schema, 
                        resultDatabaseSchema=target_database_schema,
                        subsetTableName=subset_table_name_t3,
                        connectionDetails=connectionDetails)



# Download the clinical notes from the subset

## Function to write notes on disk file by file

write_notes <- function(x, notes_folder) {
  note_id <- x['NOTE_ID']
  person_id <- x['PERSON_ID']
  note_text <- x['NOTE_TEXT']
  fileName <- paste0(notes_folder,"/",person_id, "_", note_id, ".txt")
  cat(note_text, file=fileName , append = F, fill = F)
}

## Cohort 1
# Output folder for notes
notes_folder_t1 <- paste0(working_directory, subset_table_name_t1)
system(paste0("mkdir ", notes_folder_t1))

con = DatabaseConnector::connect(connectionDetails)

renderedSql <- SqlRender::render("SELECT * FROM @resultDatabaseSchema.@subsetTableName",
                                 resultDatabaseSchema=target_database_schema,
                                 subsetTableName=subset_table_name_t1,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

notes_df = DatabaseConnector::querySql(connection = con, sql = translatedSql)
DatabaseConnector::disconnect(con)

# Write the notes in individual files
apply(notes_df, 1, write_notes, notes_folder=notes_folder_t1)

## Cohort 2

# Output folder for notes
notes_folder_t2 <- paste0(working_directory, subset_table_name_t2)
system(paste0("mkdir ", notes_folder_t2))

con = DatabaseConnector::connect(connectionDetails)

renderedSql <- SqlRender::render("SELECT * FROM @resultDatabaseSchema.@subsetTableName",
                                 resultDatabaseSchema=target_database_schema,
                                 subsetTableName=subset_table_name_t2,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

notes_df = DatabaseConnector::querySql(connection = con, sql = translatedSql)
DatabaseConnector::disconnect(con)
 
# Write the notes in individual files
apply(notes_df, 1, write_notes, notes_folder=notes_folder_t2)

## Cohort 3

# Output folder for notes
notes_folder_t3 <- paste0(working_directory, subset_table_name_t3)
system(paste0("mkdir ", notes_folder_t3))

con = DatabaseConnector::connect(connectionDetails)

renderedSql <- SqlRender::render("SELECT * FROM @resultDatabaseSchema.@subsetTableName",
                                 resultDatabaseSchema=target_database_schema,
                                 subsetTableName=subset_table_name_t3,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

notes_df = DatabaseConnector::querySql(connection = con, sql = translatedSql)
DatabaseConnector::disconnect(con)

# Write the notes in individual files
apply(notes_df, 1, write_notes, notes_folder=notes_folder_t3)
 
 
##########################################################################################
# Run NLP algorithm manually
# The name of the file should be the same as the one declared on nlp_output_filename
##########################################################################################

# Upload the file to the Database. Assumes results are in 
# leo_nlp_output_folder under sub folders T1, T2, T3
##########################################################################################


# Cohort 1

#load data from csv and split DocID into patient_id and note_id 
output_leo_t1_df = read.csv(paste0(leo_nlp_output_folder,"T1/",nlp_output_filename_t1)) %>% 
  tidyr::separate(.,col=DocID, into=c("person_id","note_id"),sep="_", remove=FALSE) %>%
  tidyr::separate(., col=note_id, into=c("note_id"), sep=".txt",remove=TRUE)

#upload into BigQuery  
bigrquery::bq_table(project_id, dataset_id, table = nlp_table_leo_output_t1) %>%
  bigrquery::bq_table_upload(.,output_leo_t1_df)

### uploading for other DBMS:
# connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
# DatabaseConnector::insertTable(connection = connection,
#                                databaseSchema = target_database_schema,
#                                tableName=nlp_table_leo_output_t1,
#                                data=output_leo_t1_df_subset,
#                                dropTableIfExists = TRUE,
#                                createTable = TRUE,
#                                tempTable = FALSE,
#                                oracleTempSchema = NULL,
#                                progressBar = TRUE,
#                                camelCaseToSnakeCase = FALSE
#                               )
# 
# DatabaseConnector::disconnect(connection)

rm(output_leo_t1_df)

# Cohort 2

output_leo_t2_df = read.csv(paste0(leo_nlp_output_folder,"T2/",nlp_output_filename_t2)) %>%
  tidyr::separate(.,col=DocID, into=c("person_id","note_id"),sep="_", remove=FALSE) %>%
  tidyr::separate(., col=note_id, into=c("note_id"), sep=".txt",remove=TRUE)

#upload into BigQuery  
bigrquery::bq_table(project_id, dataset_id, table = nlp_table_leo_output_t2) %>%
  bigrquery::bq_table_upload(.,output_leo_t2_df)

##for other DBMS
# DatabaseConnector::insertTable(connection = connection,
#                                databaseSchema = target_database_schema,
#                                tableName=nlp_table_leo_output_t2,
#                                data=output_leo_t2_df,
#                                dropTableIfExists = TRUE,
#                                createTable = TRUE,
#                                tempTable = FALSE,
#                                oracleTempSchema = NULL,
#                                progressBar = TRUE,
#                                camelCaseToSnakeCase = FALSE
#                               )

rm(output_leo_t2_df)

# Cohort 3
output_leo_t3_df = read.csv(paste0(leo_nlp_output_folder,"T3/",nlp_output_filename_t3))  %>%
  tidyr::separate(.,col=DocID, into=c("person_id","note_id"),sep="_", remove=FALSE) %>%
  tidyr::separate(., col=note_id, into=c("note_id"), sep=".txt",remove=TRUE)

#upload into BigQuery  
bigrquery::bq_table(project_id, dataset_id, table = nlp_table_leo_output_t3) %>%
  bigrquery::bq_table_upload(.,output_leo_t3_df)

## for other DBMS
# DatabaseConnector::insertTable(connection = connection,
#                                databaseSchema = target_database_schema,
#                                tableName=nlp_table_leo_output_t3,
#                                data=output_leo_t3_df,
#                                dropTableIfExists = TRUE,
#                                createTable = TRUE,
#                                tempTable = FALSE,
#                                oracleTempSchema = NULL,
#                                progressBar = TRUE,
#                                camelCaseToSnakeCase = FALSE
#                               )

rm(output_leo_t3_df)

# Execute the Rollup Logic per each table

# The resulting table should have the following schema
# person_id: INT
# treated: [1, 0] 
# intent: [1, 0]
# notTreated: [1, 0]
# treated_count: INT
# notTreated_count: INT
# intent_count: INT
# proneTreatment: [treated, intent, notTreated, noDocumentation]


# Cohort 1

renderedSql <- SqlRender::render(SqlRender::readSql("ProneNlp/inst/sql/sql_server/nlp_rollup_logic.sql"),
                                 result_schema=target_database_schema,
                                 nlp_admission_summary=nlp_admission_summary_t1,
                                 target_cohort=target_cohort_table,
                                 cohortId=target_cohort_id_t1,
                                 nlp_raw_output=nlp_table_leo_output_t1,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

DatabaseConnector::executeSql(connection=DatabaseConnector::connect(connectionDetails),
                              sql=translatedSql)

# Cohort 2

renderedSql <- SqlRender::render(SqlRender::readSql("ProneNlp/inst/sql/sql_server/nlp_rollup_logic.sql"),
                                 result_schema=target_database_schema,
                                 nlp_admission_summary=nlp_admission_summary_t2,
                                 target_cohort=target_cohort_table,
                                 cohortId=target_cohort_id_t2,
                                 nlp_raw_output=nlp_table_leo_output_t2,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

DatabaseConnector::executeSql(connection=DatabaseConnector::connect(connectionDetails),
                              sql=translatedSql)

# Cohort 3

renderedSql <- SqlRender::render(SqlRender::readSql("ProneNlp/inst/sql/sql_server/nlp_rollup_logic.sql"),
                                 result_schema=target_database_schema,
                                 nlp_admission_summary=nlp_admission_summary_t3,
                                 target_cohort=target_cohort_table,
                                 cohortId=target_cohort_id_t3,
                                 nlp_raw_output=nlp_table_leo_output_t3,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

DatabaseConnector::executeSql(connection=DatabaseConnector::connect(connectionDetails),
                              sql=translatedSql)

#####################################################################
# Compute the incidence rates
#####################################################################


