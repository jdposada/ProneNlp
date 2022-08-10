CREATE TABLE 
 @resultDatabaseSchema.@subsetTableName AS
  SELECT
    table_.*
  FROM
    @cdmDatabaseSchema.@cdmTable AS table_
  JOIN
    @resultDatabaseSchema.@cohortTable AS cohort
  ON
    table_.person_id = cohort.subject_id
  WHERE
    cohort_definition_id = @cohortId
    AND table_.@dateColumn BETWEEN cohort.cohort_start_date AND cohort.cohort_end_date
