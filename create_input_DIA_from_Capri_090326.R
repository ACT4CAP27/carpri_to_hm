## Create Health Module (DIA/CRA) input data

# input folder
input_dir <- "mapping_input"

# create output folder if it doesn't exist
dir.create("mapping_output", showWarnings = FALSE)

# mapping file name
mapping_file <- "Mapping_scheme_DIA_CAPRI.xlsx"

#data with waste or without (INHA and INHA_nowaste)
waste <- "INHA"

library(data.table)
#library(dplyr)
library(dtplyr)
library(dplyr, warn.conflicts = FALSE)
library(readxl)
library(tidyverse)
library(gamstransfer)
library(countrycode)

################################################################################
# 1. Modify and filter the CAPRI data
################################################################################
#_______________________________________________________________________________
# FOLDER and FILES ####
#_______________________________________________________________________________

## Directories ----
project_dir <- getwd()
gdx_dir     <- file.path(input_dir, "0_gdx")

## Results files ----
scenario_files <- list.files(
  path       = gdx_dir,
  pattern    = "\\.gdx$",
  full.names = FALSE
)

# adjust scenario names: 
## (!!!) Scenario names ----
clean_name <- function(x) {
  x |>
    gsub("res_2_1730gtMINPRI_scenario", "", x = _) |>
    gsub("defaultA\\.gdx$", "", x = _) |>
    trimws()
}

scenario_names <- clean_name(scenario_files)
scenario_names <- unname(scenario_names)

#_______________________________________________________________________________
# MAPPINGS ####
#_______________________________________________________________________________

## Mapping file ----
utilities_file <- file.path(input_dir, mapping_file)

## CAPRI sets ----
CAPRI_c     <- read_xlsx(path = utilities_file, sheet = "CAPRI_c")
CAPRI_r     <- read_xlsx(path = utilities_file, sheet = "CAPRI_r")

## CAPRI variables ----
EXT_c_var <- read_xlsx(path = utilities_file, sheet = "EXT_c_var")

#_______________________________________________________________________________
# FUNCTIONS ####
#_______________________________________________________________________________

unit_map <- setNames(as.character(EXT_c_var$UNIT_LVL1), EXT_c_var$CODE_LVL1)

extract_scenario <- function(scen_file, scen_name) {
  gdx_path <- file.path(input_dir, "0_gdx", scen_file)
  
  dataOut <- readGDX(gdx_path)$dataOut$records %>%
    filter(uni_2 == "",
           uni_3 %in% EXT_c_var$CODE_LVL1,
           uni_4 %in% CAPRI_c$CODE_LVL1) %>%
    mutate(Unit = unname(unit_map[as.character(uni_3)])) %>%
    transmute(
      Module   = "MARKET",
      Scenario = scen_name,
      Region   = uni_1,
      Item     = uni_4,
      Variable = uni_3,
      Year     = uni_5,
      Unit,
      Value    = value
    )
}

df_results <- bind_rows(
  mapply(extract_scenario, scenario_files, scenario_names, SIMPLIFY = FALSE)
)

#_______________________________________________________________________________
# AGGREGATION ####
#_______________________________________________________________________________

## Regions ----
mapping_regions_LVL0 <- setNames(CAPRI_r$CODE_LVL1, CAPRI_r$CODE_LVL1)
mapping_regions_LVL1 <- setNames(CAPRI_r$CODE_LVL2, CAPRI_r$CODE_LVL1)
mapping_regions_LVL2 <- setNames(CAPRI_r$CODE_LVL3, CAPRI_r$CODE_LVL1)
mapping_regions_LVL3 <- setNames(CAPRI_r$CODE_LVL4, CAPRI_r$CODE_LVL1)
mapping_regions_LVL4 <- setNames(CAPRI_r$CODE_LVL5, CAPRI_r$CODE_LVL1)
mapping_regions <- list(
  mapping_regions_LVL0,
  mapping_regions_LVL1,
  mapping_regions_LVL2,
  mapping_regions_LVL3,
  mapping_regions_LVL4
)

## Commodities ----
mapping_commodities_LVL0 <- setNames(CAPRI_c$CODE_LVL1, CAPRI_c$CODE_LVL1)
mapping_commodities_LVL1 <- setNames(CAPRI_c$CODE_LVL2, CAPRI_c$CODE_LVL1)
mapping_commodities_LVL2 <- setNames(CAPRI_c$CODE_LVL3, CAPRI_c$CODE_LVL1)
mapping_commodities_LVL3 <- setNames(CAPRI_c$CODE_LVL4, CAPRI_c$CODE_LVL1)
mapping_commodities <- list(
  mapping_commodities_LVL0,
  mapping_commodities_LVL1,
  mapping_commodities_LVL2,
  mapping_commodities_LVL3
)

# helper: apply a named-vector mapping to one column (keeps originals when not mapped)
apply_map <- function(df, col, map) {
  col <- rlang::ensym(col)
  x <- df %>% pull(!!col)
  new <- unname(map[as.character(x)])
  df %>%
    mutate(!!col := new) %>%        # no coalesce
    filter(!is.na(!!col))
}

#_______________________________________________________________________________ 
# MARKET IND ####
#_______________________________________________________________________________

df_market1 <- df_results %>%
  filter(Module == "MARKET", Region %in% CAPRI_r$CODE_LVL1)

df_inha_nowaste <- df_market1 %>%
  filter(Variable %in% c("LOSCsh", "INHA")) %>%
  select(!Unit) %>%
  pivot_wider(
    names_from = Variable,
    values_from = Value
  ) %>%
  filter(!is.na(INHA)) %>%
  mutate(
    Value = INHA / (1 - LOSCsh),
    Variable = "INHA_nowaste",
    Unit = "[kg/p/y]"
  ) %>%
  select(Module, Scenario, Region, Item, Variable, Year, Unit, Value)

df_market1 <- bind_rows(df_market1,df_inha_nowaste)

df_regions <- map_dfr(mapping_regions, ~ apply_map(df_market1, Region, .x)) %>% filter(!is.na(Region)) %>%
  distinct()

df_commodities <- map_dfr(mapping_commodities, ~ apply_map(df_regions, Item, .x)) %>% filter(!is.na(Item)) %>%
  distinct()

## (!!!) indicators ---- 
agg_funs <- list(
  N_CAL           = function(x) sum(x, na.rm = TRUE),
  INHA            = function(x) sum(x, na.rm = TRUE),
  INHA_nowaste    = function(x) sum(x, na.rm = TRUE)
)

# df_market2 <- imap_dfr(agg_funs, function(fun, var) { 
#   df_commodities %>% filter(Variable == var) %>% 
#     group_by(Module, Scenario, Region, Item, Variable, Year, Unit) %>% 
#     summarise(Value = fun(Value), .groups = "drop") 
# }
# )

df_market2 <- imap_dfr(agg_funs, function(fun, var) { 
  df_commodities %>% filter(Variable == var) %>% 
    group_by(Module, Scenario, Region, Item, Variable, Year, Unit) %>% 
    summarise(Value = fun(Value), .groups = "drop") 
}
)

df_aggregation <- bind_rows(df_market1, df_market2) %>% distinct()

#_______________________________________________________________________________ 
# SAVED CSV ####
#_______________________________________________________________________________

# Final filter before saving
df_accelerator <- df_aggregation %>%
  lazy_dt() %>%
  filter(
    Region %in% CAPRI_r$CODE_LVL1,
    (Variable == "INHA"  & Item %in% CAPRI_c$CODE_LVL1) |
      (Variable == "INHA_nowaste"  & Item %in% CAPRI_c$CODE_LVL1) |
      (Variable == "N_CAL" & Item %in% CAPRI_c$CODE_LVL1)
  ) %>%
  mutate(
    Value = if_else(Variable %in% c("INHA","INHA_nowaste"), Value / 365, Value),
    Unit  = if_else(Variable %in% c("INHA","INHA_nowaste"), "[kg/p/d]", Unit)
  ) %>%
  as_tibble()


df_inha <- df_aggregation %>%
  filter(
    Region %in% CAPRI_r$CODE_LVL1,
    Variable %in% c("INHA", "INHA_nowaste"),
    Item %in% CAPRI_c$CODE_LVL1
  ) %>%
  mutate(
    Value = Value / 365,
    Unit  = "[kg/p/d]"
  )

df_ncal <- df_aggregation %>%
  filter(
    Region %in% CAPRI_r$CODE_LVL1,
    Variable == "N_CAL",
    Item %in% CAPRI_c$CODE_LVL1
  ) %>%
  group_by(Module, Scenario, Region, Variable, Year, Unit) %>%
  summarise(Value = sum(Value, na.rm = TRUE), .groups = "drop") %>%
  mutate(Item = "AGR")  # label the summed row

df_accelerator <- bind_rows(df_inha, df_ncal)

# results_file_name <- "Accelerator_results.csv"
# write.csv(df_accelerator, file = file.path(input_dir, results_file_name),   row.names = FALSE)

# capri results (data which shall be analysed in terms of health impacts)
capri_results <- df_accelerator

################################################################################
# 2. Map regions
################################################################################

#_______________________________________________________________________________
# DATA ####
#_______________________________________________________________________________

# regions in Capri and their naming
regions_capri <- read_excel(file.path(input_dir, mapping_file), 
                            sheet = "rg_map")

# regions and their mapping to ISO3 regions (DIA)
map_to_dia_rg <- read_excel(file.path(input_dir, mapping_file), 
                            sheet = "rg_map_iso3")

#_______________________________________________________________________________
# Region Mapping to ISO3 Standard groups ####
#_______________________________________________________________________________

# 1. Get relevant regions from THE CAPRI RESULTS
unique_rgs_results <- unique(capri_results$Region)
unique_rgs_df_results <- data.frame(Region = unique_rgs_results)

# 2. Extract the CAPRI CODES and REGION NAMES from the overall mapping file
region_mapping_unique <- regions_capri %>%
  select(CODE_LVL1, NAME_LVL1) %>%
  distinct()

# 3. Now combine the relevant regions form the current results with the 
# respective mapped ISO3 codes from the mapping file

map_to_dia_rg_df <- map_to_dia_rg %>%
  select(iso_3, capri) %>%
  distinct()

map_to_dia_rg_df_join <- region_mapping_unique %>%
  left_join(map_to_dia_rg_df, by = c("CODE_LVL1" = "capri"))

# rename for comprehension
regions_capri_dia_map <- map_to_dia_rg_df_join %>%
  rename("Code_Capri" = CODE_LVL1,
         "Name Capri" = NAME_LVL1,
         "ISO3" = iso_3)

#_______________________________________________________________________________
# # Note: The region mapping needs some modifications. 
# Although the DIA has an internal mapping for aggregates, we need to identify
# the "member" states of every aggregate clearly for the required mapping 
# template
# furthermore, within the analysis, some wrong ISO3 codes have been identified
# the original ISO3 code will be maintained (reproducability of the research)
#_______________________________________________________________________________

# 1. check double and single assignment to identify aggregate regions
# single and double 

iso3_counts <- regions_capri_dia_map %>%
  group_by(ISO3) %>%
  mutate(count_iso3 = n()) %>%
  ungroup() %>%
  group_by(Code_Capri) %>%
  mutate(count_capri = n()) %>%
  ungroup()

# 2. Map the regions, which are clearly defined (single assignment)

# Single assignment (both unique )
single_assignment <- iso3_counts %>%
  filter(count_iso3 == 1 & count_capri == 1) %>%
  select(-count_iso3, -count_capri) %>%
  mutate(ISO3_correct = countrycode(`Name Capri`, 
                                    origin = "country.name", 
                                    destination = "iso3c"))

# Multiple assignment (either name or ISO code appears more than once)
multiple_assignment <- iso3_counts %>%
  filter(count_iso3 > 1 | count_capri > 1) %>%
  select(-count_iso3, -count_capri)

# Move unmatched from single assignemt (eg. REU) to multiple_assignment
multiple_assignment <- bind_rows(
  multiple_assignment,
  single_assignment %>% filter(is.na(ISO3_correct))
)

# Keep only successfully mapped in single_assignment, these regions are fully 
# mapped
single_assignment <- single_assignment %>%
  filter(!is.na(ISO3_correct))

# 3. Map the regions, which are NOT clearly defined (mutiple assignment)
# aggregates or unclear matches, various issues:
# BLT → Estonia, Lithuania, Latvia (individual countries, just wrong code)
# OBN → Croatia, Bosnia, Serbia, Kosovo, Macedonia, Montenegro (individual countries, just wrong code)
# ITP → Italy, Malta (individual countries, just wrong code)
# FSU → ARM, AZE, GEO, KGZ, TJK, TKM, UZB (already have correct ISO3!)
# package countrycode helps with assigning the correct ISO3

# countries of aggregates get assigned an ISO3 code based on the name in CAPRI
# other ISO3 codes get corrected, also KOSOVO gets added manually 
multiple_assignment_fixed <- multiple_assignment %>%
  mutate(ISO3_correct = case_when(
    Code_Capri == "KO000000" ~ "XKX",
    !ISO3 %in% c("BLT", "OBN", "ITP", "CHM", "REU") ~ ISO3,
    TRUE ~ countrycode(`Name Capri`, 
                       origin = "country.name", 
                       destination = "iso3c")
  ))

# Fix REU countries - input for countries from provided mapping file 'world_map'
REU_countries <- read_xlsx(path = file.path(input_dir, mapping_file), sheet = "world_map") %>%
  as.data.frame() %>%
  filter(CAPRI_r == "REU") %>%
  mutate(ISO3_correct = countrycode(region, 
                                    origin = "country.name", 
                                    destination = "iso3c"),
         Code_Capri = "REU",
         `Name Capri` = "Rest of Europe",      # CAPRI aggregate name
         Country_Name_Official = region) %>%   # keep individual country name here
  select(Code_Capri, `Name Capri`, ISO3_correct, Country_Name_Official)

# Merge REU into multiple_assignment_fixed
multiple_assignment_fixed <- bind_rows(
  multiple_assignment_fixed %>% filter(Code_Capri != "REU"),
  REU_countries
)

# Final merge and add official country name as last column for reference
capri_rgs_corrected <- bind_rows(
  single_assignment,
  multiple_assignment_fixed
) %>%
  arrange(Code_Capri) %>%
  mutate(Country_Name_Official = countrycode(ISO3_correct,
                                             origin = "iso3c",
                                             destination = "country.name"),
         # Kosovo exception since countrycode doesn't recognize XKX
         Country_Name_Official = ifelse(ISO3_correct == "XKX", 
                                        "Kosovo", 
                                        Country_Name_Official))

# 4. Do safety checks eg. if a region could not be matched
# this is mainly designed to detect missing indications in the provided
# mapping csv

# Safety check - flag any unmatched rows
unmatched_final <- capri_rgs_corrected %>%
  filter(is.na(ISO3_correct) | is.na(Country_Name_Official))
if(nrow(unmatched_final) > 0) {
  warning(paste("WARNING:", nrow(unmatched_final), "rows could not be matched!"))
  print(unmatched_final)
} else {
  message("All rows successfully matched!")
}

# Safety check - flag any duplicate ISO3_correct
duplicate_iso3 <- capri_rgs_corrected %>%
  filter(duplicated(ISO3_correct) | duplicated(ISO3_correct, fromLast = TRUE))
if(nrow(duplicate_iso3) > 0) {
  warning(paste("WARNING:", nrow(duplicate_iso3), "rows have duplicate ISO3_correct values!"))
  print(duplicate_iso3)
} else {
  message("All ISO3_correct values are unique!")
}

#5. Write the region mapping csv for the DIA model (adjusted to template format)
# This is one of two files need to be provided to run the health module
# as said, the health module has various options on how to account for the
# scaling of the consumption for the aggregates

template_rgs_map <- capri_rgs_corrected %>% 
  select(ISO3_correct, Code_Capri) %>%
  rename(r_iso3 = ISO3_correct, r_m = Code_Capri)

write.csv(template_rgs_map,
          file = "mapping_output/map_reg.csv",
          row.names = FALSE)

################################################################################
#3. Map foods
################################################################################

#_______________________________________________________________________________
# DATA ####
#_______________________________________________________________________________

# commodities in Capri
commodities_capri <- read_excel(file.path(input_dir, mapping_file), 
                                sheet = "fg_capri")

# commodities and their mapping to DIA food groups
map_to_dia_fg <- read_excel(file.path(input_dir, mapping_file), 
                            sheet = "fg_map")

# please indicate the following
# is your data waste adjusted ? y = intake, n = availability
# (desired in g/d usually as intake and kcal/day as availability)
parameter <- "intake"

#measure (< note if value is provided as an absolute value ("abs") or as 
#percentage change compared to the reference scenario in the same year ("pct"))
measure <- "abs"

#unit (< unit of absolute value; please use "g/d" for grams per person per day 
#and "kcal/d" for kilocalories per person per day)
units_extracted <- unique(capri_results$Unit)
print(units_extracted)

capri_results <- capri_results %>%
  mutate(Value = case_when(
    Unit == "[kg/p/d]" ~ Value * 1000,  # kg/p/d to g/p/d
    TRUE ~ Value                          # keep kcal as is
  ),
  Unit = case_when(
    Unit == "[kg/p/d]"   ~ "g/d",
    Unit == "[kcal/p/d]" ~ "kcal/d",
    TRUE ~ Unit
  )) %>%
  filter (
    Variable == waste | Variable == "N_CAL",
  )

#_______________________________________________________________________________
# Identify relevant commodities from the results file ####
#_______________________________________________________________________________

# 1. get only unique CODE_LVL1 and NAME_LVL1 pairs to create generalized mapping 
# procedure
commodities_capri <- commodities_capri %>%
  select(CODE_LVL1, NAME_LVL1) %>%
  distinct()

# 2. add the AGR exception for total kcal per dar (included in results file), 
# necessary for the health model link
agr_exception <- data.frame(CODE_LVL1 = "AGR", 
                            NAME_LVL1 = "Total calories per day per person")
commodities_capri <- bind_rows(commodities_capri, agr_exception)

# 3. identify commodities existing in the presented Capri results file 
unique_commodities_results <- unique(capri_results$Item)
unique_commodities_df_results <- data.frame(Commodity = unique_commodities_results)
unique_commodities_df_results <- unique_commodities_df_results %>%
  left_join(commodities_capri, by = c("Commodity" = "CODE_LVL1"))

#_______________________________________________________________________________
# Generate the commodity mapping to DIA ####
#_______________________________________________________________________________

# 1. Create dataframe from excel sheet (manually created)
map_to_dia_fg_df <- map_to_dia_fg  %>%
  select(CAPRI_c, fg_p, CRA) %>%
  distinct()

# 2. add again the total kcal consumption in kcal/c/day (provided in results)
agr_exception_fg <- data.frame(
  CAPRI_c = "AGR",
  fg_p = "total",
  CRA = "total"
)
map_to_dia_fg_with_kcal <- bind_rows(map_to_dia_fg_df, agr_exception_fg)

commodities_capri_dia_map <- unique_commodities_df_results %>%
  left_join(map_to_dia_fg_with_kcal, by = c("Commodity" = "CAPRI_c"))


commodities_capri_dia_map <- commodities_capri_dia_map %>%
  filter(!( CRA == "NA"))

commodities_capri_dia_map_1to1 <- commodities_capri_dia_map %>%
  arrange(Commodity) %>% 
  distinct(Commodity, .keep_all = TRUE)

capri_results_mapped <- capri_results %>%
  left_join(
    commodities_capri_dia_map_1to1 %>% select(Commodity, CRA),
    by = c("Item" = "Commodity"),
    relationship = "many-to-one"
  ) %>%
  filter(!is.na(CRA), CRA != "NA")


#_______________________________________________________________________________
# Handle the OFRU problem ####
#_______________________________________________________________________________
# 

# 1. Load OFRU splitting ratios from Excel
ofru_ratios_raw <- read_xlsx(file.path(input_dir, mapping_file), sheet = "OFRU_Splits")

ofru_ratios <- ofru_ratios_raw %>%
  filter(fg_m == "OFRU") %>%
  select(Region, fg, value) %>%
  pivot_wider(
    names_from = fg,
    values_from = value,
    names_prefix = "ratio_"
  )
# Now we have: Region, ratio_fruits, ratio_nuts

# 2. Split OFRU into fruits and nuts_seeds
capri_results_ofru <- capri_results_mapped %>%
  filter(CRA == "OFRU") %>%
  left_join(ofru_ratios, by = "Region") %>%
  # Create two values
  mutate(
    fruits_value = Value * ratio_fruits,
    nuts_seeds_value = Value * ratio_nuts
  ) %>%
  select(-Value, -ratio_fruits, -ratio_nuts) %>%
  # Pivot to long format
  pivot_longer(
    cols = c(fruits_value, nuts_seeds_value),
    names_to = "food_group",
    values_to = "Value"
  ) %>%
  mutate(
    CRA = case_when(
      food_group == "fruits_value" ~ "fruits",
      food_group == "nuts_seeds_value" ~ "nuts_seeds"
    )
  ) %>%
  select(-food_group)

# 3. Combine with non-OFRU data
capri_results_final <- bind_rows(
  capri_results_mapped %>% filter(CRA != "OFRU"),
  capri_results_ofru
)

# just checking if we have any duplicate consumption values
capri_results_final %>%
  count(Scenario, Year, Value) %>%
  filter(n > 1) %>%
  arrange(desc(n))

mapped_to_template_commodities <- capri_results_final %>%
  group_by(Region, Scenario, Year, Unit, CRA) %>%
  summarise(Value = sum(Value, na.rm = TRUE), .groups = "drop") %>%
  transmute(
    region     = Region,
    parameter  = parameter,   
    measure    = measure,
    unit       = Unit,                
    year       = Year,
    scenario   = Scenario,     
    food_group = CRA,
    value      = Value
  )

ref_duplicates <- mapped_to_template_commodities %>%
  filter(str_detect(scenario, "_REF\\.gdx$")) %>%
  mutate(scenario = "REF")

mapped_to_template_commodities_final <- bind_rows(
  mapped_to_template_commodities,
  ref_duplicates
) %>%
  arrange(year, scenario, region, food_group)

# check
mapped_to_template_commodities_final %>%
  distinct(year, scenario) %>%
  arrange(year, scenario) %>%
  print()

write.csv(
  mapped_to_template_commodities_final,
  file = "mapping_output/map_fg.csv",
  row.names = FALSE
)