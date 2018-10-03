# Test connection with bucket
system("aws s3 ls s3://datalake-eu-central-1/N1vA-polkproc/")

library(dplyr)
library(data.table)
library(doMC)
registerDoMC(cores = 16)

# tire_trans  200 KB
# geography     2 MB
# make_model    9 MB
# vehicle_count 6 GB

# Build path to data files in S3 bucket
path = "s3://datalake-eu-central-1/N1vA-polkproc/"
vc_file = paste0(path, "db.vehicle_count.csv")
mm_file = paste0(path, "db.make_model.csv")
geo_file = paste0(path, "db.geography.csv")
tt_file = paste0(path, "db.conti_tire_trans.csv")
# pp_file = paste0(path, "polk_processed.csv") # waiting for Juergen's answer, R or aws problem?

# Load data into Rstudio Environment for manipulation
vc <- aws.s3::s3read_using(fread, object = vc_file)
mm <- aws.s3::s3read_using(read.csv, object = mm_file)
geo <- aws.s3::s3read_using(read.csv, object = geo_file)
tt <- aws.s3::s3read_using(read.csv, object = tt_file)

# Perform joins, output processed data
polkproc <- vc %>% 
  inner_join(., mm, by = "MAKE_MODEL_CDE") %>% 
  inner_join(., geo, by = c("STATE_CDE", "COUNTY_CDE", "ZIP_CODE")) %>% 
  left_join(., tt, by = "SIZEDESC")

# Write .csv to local disk
fwrite(polkproc, file = "polk_processed.csv")

# Copy processed data to S3 bucket
system("aws s3 cp polk_processed.csv s3://datalake-eu-central-1/N1vA-polkproc/")

write.csv(polkproc[1:100,], file = "polkproc_head.csv")

# # Write processed data output to S3 bucket as a .csv file
# path = "s3://datalake-eu-central-1/N1vA-polkproc/"
# pp_file = paste0(path, "db.polk_data_processed.csv")
# aws.s3::s3write_using(polkproc, fwrite, object = pp_file)
