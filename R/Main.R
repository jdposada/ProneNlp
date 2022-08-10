# Copyright 2021 Observational Health Data Sciences and Informatics
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



working_directory <- "/workdir/workdir"
package_directory <- "/workdir/workdir/OHDSI_prone_eminty/"
setwd(working_directory)
r_env_cache_folder <- "/workdir/renv_cache"
renv_package_version <- '0.13.2'
renv_vesion <- "v5"
r_version <- "R-4.0"
linux_version <- "x86_64-pc-linux-gnu"


jsonPath <- ""
bqDriverPath <- "/workdir/workdir/BQDriver/"
project_id <- "som-nero-nigam-starr"
dataset_id <- "prone_nlp"
#output folder for notes
notes_folder <- paste0(working_directory,"/ProneNotes_NoProc/")
system(paste0("mkdir ", notes_folder))

cdm_database_schema <- ""
vocabulary_database_schema <- cdm_database_schema
target_database_schema <- "`som-nero-nigam-starr.prone_nlp`"


nlp_admission_summary <- "nlp_admission_summary"
nlp_raw_output <- "nlp_raw_output"
nlp_output_filename <- "nlp_output_leo.csv"

renv_final_path <- paste(r_env_cache_folder,
                         renv_vesion,
                         r_version,
                         linux_version,
                         sep="/")

.libPaths(renv_final_path)


# Load libraries
library(dplyr)
library(stringr)


# Connecto to Database

connectionString <-  BQJdbcConnectionStringR::createBQConnectionString(projectId = project_id,
                                                                       defaultDataset = dataset_id,
                                                                       authType = 2,
                                                                       jsonCredentialsPath = jsonPath)

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms="bigquery",
                                                                connectionString=connectionString,
                                                                user="",
                                                                password='',
                                                                pathToDriver = bqDriverPath)


# Create Cohort Table
renderedSql <- SqlRender::render(SqlRender::readSql("inst/sql/sql_server/create_cohort_table.sql"),
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
target_cohort_id <- "141"
renderedSql <- SqlRender::render(SqlRender::readSql("inst/sql/sql_server/target/covid_prone_T_Proc_Excl.sql"),
                                 cdm_database_schema=cdm_database_schema,
                                 vocabulary_database_schema=vocabulary_database_schema,
                                 target_database_schema=target_database_schema,
                                 target_cohort_table=target_cohort_table,
                                 target_cohort_id=target_cohort_id,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)


con = DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=con,
                              sql=translatedSql)
DatabaseConnector::disconnect(con)


# Cohort 2:
target_cohort_id <- "142"
renderedSql <- SqlRender::render(SqlRender::readSql("inst/sql/sql_server/target/covid_prone_T2_Proc_Excl.sql"),
                                 cdm_database_schema=cdm_database_schema,
                                 vocabulary_database_schema=vocabulary_database_schema,
                                 target_database_schema=target_database_schema,
                                 target_cohort_table=target_cohort_table,
                                 target_cohort_id=target_cohort_id,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)


con = DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=con,
                              sql=translatedSql)
DatabaseConnector::disconnect(con)


# Cohort 3:
target_cohort_id <- "143"
renderedSql <- SqlRender::render(SqlRender::readSql("inst/sql/sql_server/target/covid_prone_T3_Proc_Excl_OnDex.sql"),
                                 cdm_database_schema=cdm_database_schema,
                                 vocabulary_database_schema=vocabulary_database_schema,
                                 target_database_schema=target_database_schema,
                                 target_cohort_table=target_cohort_table,
                                 target_cohort_id=target_cohort_id,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)


con = DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=con,
                              sql=translatedSql)
DatabaseConnector::disconnect(con)


# Download the clinical notes from the subset
setwd(working_directory)

con = DatabaseConnector::connect(connectionDetails)

renderedSql <- SqlRender::render("SELECT * FROM @resultDatabaseSchema.note",
                                 resultDatabaseSchema=target_database_schema,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

all_prone_notes_no_proc = DatabaseConnector::querySql(connection = con, sql = translatedSql)
DatabaseConnector::disconnect(con)


# Write the notes in individual files
write_notes <- function(x, notes_folder) {
  note_id <- x['NOTE_ID']
  person_id <- x['PERSON_ID']
  note_text <- x['NOTE_TEXT']
  fileName <- paste0(notes_folder, person_id, "_", note_id, ".txt")
  cat(note_text, file=fileName , append = F, fill = F)
}

 apply(all_prone_notes_no_proc, 1, write_notes, notes_folder=notes_folder)

 
 ###############################################################################
 # Copy / rename cdm tables and result tables to denote procedure exlcusion cohort
 # and allow running of Dex cohort.  
 ###############################################################################
 
 # TODO: Make SQL render compliant
 
 # cdm tables in these files would appear to be implied in the analysis
 # not all of them are output in the result schema (see cdmSubset.R)
 
 personIdTables = read.csv(paste0(package_directory,"/inst/settings/personIdTables.csv"))
 dateTables = read.csv((paste0(package_directory,"/inst/settings/dateTables.csv")))
 
 # get list of cdm tables possibly implied in analysis.
 cdmTablesInAnalysis <- unique(c(personIdTables[,1], dateTables[,1]))
 
 # get list of cdm tables in the target DB schema
 con = DatabaseConnector::connect(connectionDetails)
 tablesInTargetSchema <- DatabaseConnector::getTableNames(con,dataset_id)
 DatabaseConnector::disconnect(con)
 
 # define list of tables to rename as the intersect of the two lists above.
 tablesToRename <- tolower(subset(tablesInTargetSchema, tolower(tablesInTargetSchema) %in% cdmTablesInAnalysis))
 
 # add a table suffix to denote the tables from the procedure exclusion cohort. 
 
 tableSuffix = "_proc_excl"
 con = DatabaseConnector::connect(connectionDetails)
 
 for (i in 1:length(tablesToRename)){
   renameSQL <-paste0("ALTER TABLE ", target_database_schema, "." , tablesToRename[i], " RENAME TO ", tablesToRename[i],
                      tableSuffix)
   DatabaseConnector::executeSql(connection=con,sql=renameSQL)
 }
 
 
 DatabaseConnector::disconnect(con)
 

##########################################################################################
# Run NLP algorithm manually
# The name of the file should be the same as the one declared on nlp_output_filename

# Upload the file to Database. it is assumed that the file lives within the same folder the notes are
##########################################################################################
nlp_output_leo_proc_excl = read.csv(paste0(notes_folder, nlp_output_filename))

DatabaseConnector::insertTable(connection = connection,
                               databaseSchema = target_database_schema,
                               tableName=nlp_raw_output,
                               data=nlp_output_leo,
                               dropTableIfExists = TRUE,
                               createTable = TRUE,
                               tempTable = FALSE,
                               oracleTempSchema = NULL,
                               progressBar = TRUE,
                               camelCaseToSnakeCase = FALSE
                              )

# Rollup Logic
# The resulting table should have the following schema
# person_id: INT
# treated: [1, 0] 
# intent: [1, 0]
# notTreated: [1, 0]
# treated_count: INT
# notTreated_count: INT
# intent_count: INT
# proneTreatment: [treated, intent, notTreated, noDocumentation]

renderedSql <- SqlRender::render(SqlRender::readSql("inst/sql/sql_server/nlp_rollup_logic.sql"),
                                 result_schema=target_database_schema,
                                 nlp_admission_summary=nlp_admission_summary,
                                 target_cohort=target_cohort_table,
                                 nlp_raw_output=nlp_raw_output,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

DatabaseConnector::executeSql(connection=DatabaseConnector::connect(connectionDetails),
                              sql=translatedSql)


############################################################################
#Dexamethasone cohort. 
############################################################################
#output folder for notes
setwd(working_directory)
system("mkdir ProneNotes_OnDex")
notes_folder = paste0(working_directory,"/ProneNotes_OnDex/")

# Names and IDs of cohort tables to be created
target_cohort_table <- "covid_hosp_on_dex_cohort"
target_cohort_id <- "142"

# Create Cohort Table for Prone Dexamethasone Cohort 
setwd(package_directory)
renderedSql <- SqlRender::render(SqlRender::readSql("inst/sql/sql_server/CreateNoProcOnDexCohortTable.sql"),
                                 cdmDatabaseSchema=target_database_schema,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

con = DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=DatabaseConnector::connect(connectionDetails),
                              sql=translatedSql)
DatabaseConnector::disconnect(con)


# Run the on Dex cohort
# Copied from T_Cohort folder in repo (May 2022) to inst/sql/sql_server/exposure/covid_prone_T_Proc_Excl_OnDex.sql

setwd(package_directory)
renderedSql <- SqlRender::render(SqlRender::readSql("inst/sql/sql_server/exposure/covid_prone_T_Proc_Excl_OnDex.sql"),
                                 cdm_database_schema=cdm_database_schema,
                                 vocabulary_database_schema=vocabulary_database_schema,
                                 target_database_schema=target_database_schema,
                                 target_cohort_table=target_cohort_table,
                                 target_cohort_id=target_cohort_id,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

con =DatabaseConnector::connect(connectionDetails)
DatabaseConnector::executeSql(connection=con,sql=translatedSql)
DatabaseConnector::disconnect(con)


# Subset the CDM for the procedure exclusion cohort. 
setwd(package_directory)
source("R/cdmSubset.R")

con = DatabaseConnector::connect(connectionDetails)

subsetCDM(cohortId=target_cohort_id,
          cohortTable=target_cohort_table,
          cdmDatabaseSchema=cdm_database_schema, 
          resultDatabaseSchema=target_database_schema,
          connectionDetails=connectionDetails)                               

DatabaseConnector::disconnect(con)

# Download the clinical notes from the subset

setwd(working_directory)
con = DatabaseConnector::connect(connectionDetails)

renderedSql <- SqlRender::render("SELECT * FROM @resultDatabaseSchema.note",
                                 resultDatabaseSchema=target_database_schema,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

all_prone_notes_on_dex = DatabaseConnector::querySql(connection = con, sql = translatedSql)
DatabaseConnector::disconnect(con)

# Write the notes in individual files

write_notes <- function(x, notes_folder) {
  note_id <- x['NOTE_ID']
  person_id <- x['PERSON_ID']
  note_text <- x['NOTE_TEXT']
  fileName <- paste0(notes_folder, person_id, "_", note_id, ".txt")
  cat(note_text, file=fileName , append = F, fill = F)
}

apply(all_prone_notes_on_dex, 1, write_notes, notes_folder=notes_folder)

###############################################################################
# rename cdm tables and result tables to denote dexamethasone cohort
###############################################################################

# TODO: Make SQL render compliant

# cdm tables in these files would appear to be implied in the analysis
# not all of them are output in the result schema (see cdmSubset.R)

personIdTables = read.csv(paste0(package_directory,"/inst/settings/personIdTables.csv"))
dateTables = read.csv((paste0(package_directory,"/inst/settings/dateTables.csv")))

# get list of cdm tables possibly implied in analysis.
cdmTablesInAnalysis <- unique(c(personIdTables[,1], dateTables[,1]))

# get list of cdm tables in the target DB schema
con = DatabaseConnector::connect(connectionDetails)
tablesInTargetSchema <- DatabaseConnector::getTableNames(con,dataset_id)
DatabaseConnector::disconnect(con)

# define list of tables to rename as the intersect of the two lists above.
tablesToRename <- tolower(subset(tablesInTargetSchema, tolower(tablesInTargetSchema) %in% cdmTablesInAnalysis))

# add a table suffix to denote the tables from the procedure exclusion cohort. 

tableSuffix = "_on_dex"
con = DatabaseConnector::connect(connectionDetails)

for (i in 1:length(tablesToRename)){
  renameSQL <-paste0("ALTER TABLE ", target_database_schema, "." , tablesToRename[i], " RENAME TO ", tablesToRename[i],
                     tableSuffix)
  DatabaseConnector::executeSql(connection=con,sql=renameSQL)
}


DatabaseConnector::disconnect(con)

################################################################################################
# Run NLP algorithm manually
# The name of the file should be the same as the one declared on nlp_output_filename

# Upload the file to Database. it is assumed that the file lives within the same folder the notes are
#################################################################################################
nlp_output_leo_onDex = read.csv(paste0(notes_folder, nlp_output_filename))

DatabaseConnector::insertTable(connection = connection,
                               databaseSchema = target_database_schema,
                               tableName=nlp_raw_output,
                               data=nlp_output_leo,
                               dropTableIfExists = TRUE,
                               createTable = TRUE,
                               tempTable = FALSE,
                               oracleTempSchema = NULL,
                               progressBar = TRUE,
                               camelCaseToSnakeCase = FALSE
)

# Rollup Logic
# The resulting table should have the following schema
# person_id: INT
# treated: [1, 0] 
# intent: [1, 0]
# notTreated: [1, 0]
# treated_count: INT
# notTreated_count: INT
# intent_count: INT
# proneTreatment: [treated, intent, notTreated, noDocumentation]

renderedSql <- SqlRender::render(SqlRender::readSql("inst/sql/sql_server/nlp_rollup_logic.sql"),
                                 result_schema=target_database_schema,
                                 nlp_admission_summary=nlp_admission_summary,
                                 target_cohort=target_cohort_table,
                                 nlp_raw_output=nlp_raw_output,
                                 warnOnMissingParameters = TRUE)

translatedSql <- SqlRender::translate(sql=renderedSql,
                                      targetDialect = connectionDetails$dbms,
                                      tempEmulationSchema = target_database_schema)

DatabaseConnector::executeSql(connection=DatabaseConnector::connect(connectionDetails),
                              sql=translatedSql)

#####################################################################
# Compute the incidence rates
#####################################################################











#################################
#ATLAS failure clean up. 
sessionHash <- "x23as9em"

con = DatabaseConnector::connect(connectionDetails)
tablesInTargetSchema <- tolower(DatabaseConnector::getTableNames(con,dataset_id))

tablesToDrop <- subset(tablesInTargetSchema, str_detect(tablesInTargetSchema,sessionHash))

for (i in 1:length(tablesToDrop)){
  dropTableSQL <-paste0("drop table ", target_database_schema, "." , tablesToDrop[i])
  DatabaseConnector::executeSql(connection=con,sql=dropTableSQL)
}
                      
DatabaseConnector::disconnect(con)
