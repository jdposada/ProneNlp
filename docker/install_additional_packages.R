# enviroment paths
working_directory <- "/workdir"

setwd(working_directory)

# required for Bigquery
install.packages('devtools')

devtools::install_github("jdposada/BQJdbcConnectionStringR", 
                         upgrade="never")