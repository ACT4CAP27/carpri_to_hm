# init.R
# Project initialization script

# List of required packages
required_packages <- c(
  "data.table",
  "dplyr",
  "readxl",
  "tidyverse",
  "ggplot2",
  "ggpubr",
  "stringr",
  "gamstransfer",
  "countrycode",
  "parallel",
  "future",
  "future.apply"
)

# Install missing packages
installed_packages <- rownames(installed.packages())
missing_packages <- setdiff(required_packages, installed_packages)

if (length(missing_packages) > 0) {
  install.packages(missing_packages, dependencies = TRUE)
}

# Load libraries
invisible(lapply(required_packages, library, character.only = TRUE))

# Optional: set parallel plan
library(future)
plan(multisession)

cat("Environment initialized successfully.\n")