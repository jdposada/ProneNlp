/*
OHDSI COVID Prone Implimentation: Post Processing Patient Rollup
Author: Patrick Alba
Date: 2022-03-15
Requires: 
  Initial Cohort Reesult Schema:  @result_schema.@target_cohort
  Output From NLP:  @result_schema.@nlp_raw_output
*/


----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Aggregate and combine output to a patient/admission-level
----------------------------------------------------------------------------------------------------------------------------------------------------------

drop table if exists @result_schema.@nlp_admission_summary;

create table
 @result_schema.@nlp_admission_summary as
with all_covid_admissions as (
    select 
     subject_id as person_id,
     cohort_start_date,
     cohort_end_date
    from 
     @result_schema.@target_cohort
    where
     cohort_definition_id = @cohortId
),
NLP_labels_all as (
    Select person_id >,
        count(*) as ProneTerms,
        sum(
            case
                when Prone_Status like 'Treated' then 1
                else 0
            end
        ) as Treated -- All instances of a patient being in a prone position
,
        sum(
            case
                when Prone_Status like 'NotTreated' then 1
                else 0
            end
        ) as NotTreated,
        sum(
            case
                when Prone_Status like 'Intent' then 1
                else 0
            end
        ) as Intent
    from 
     @result_schema.@nlp_raw_output
    group by person_id
),
rolled_up_summary as (
    Select distinct coh.person_id,
        case
            when Treated >= 1 then 1
            else 0
        end as Treated,
        case
            when Intent >= 1 then 1
            else 0
        end as Intent,
        case
            when NotTreated >= 1 then 1
            else 0
        end as NotTreated,
        Treated as Treated_Count,
        NotTreated as NotTreated_Count,
        Intent as Intent_count
    from 
     all_covid_admissions coh
    left join 
     NLP_labels_all nlp 
    on 
     coh.person_id = nlp.person_id
)
--Single label per admission
select 
 rolled_up_summary.*,
    case
        when Treated >= 1 then 'Treated'
        when Intent >= 1
        and Treated = 0
        and NotTreated >= 0 then 'Intent' --Setting intent to override when negation occurs
        When NotTreated >= 1 then 'NotTreated'
        Else 'NoDocumentation'
    end as ProneTreatment 
from 
 rolled_up_summary
