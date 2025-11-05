

# Main processing function
process_lqas_data <- function(input_file) {
  # Load required packages
  library(tidyverse)
  library(lubridate)
  library(readxl)
  library(qs)
  
  # Set locale to French for month names
  Sys.setlocale("LC_TIME", "French_France.1252")  # Windows
  # Sys.setlocale("LC_TIME", "fr_FR.UTF-8")       # Linux/macOS
  
  # Define input and output paths
  input_file <- "C:/Users/TOURE/Documents/PADACORD/LQAS/272.rds"
  output_file <- "C:/Users/TOURE/Documents/REPOSITORIES/LQAS_raw_data/LQAS_level1/272.csv"
  prep_data_file <- "C:/Users/TOURE/Documents/REPOSITORIES/LQAS_raw_data/harmonized_date/data (5).xlsx"
  
  # Helper functions
  clean_binary_var <- function(x) {
    case_when(
      tolower(x) %in% c("yes", "y", "1") ~ 1,
      tolower(x) %in% c("no", "n", "0") ~ 0,
      TRUE ~ NA_real_
    )
  }
  
  clean_sex_var <- function(x) {
    case_when(
      toupper(x) == "F" ~ 1,
      toupper(x) == "M" ~ 0,
      TRUE ~ NA_real_
    )
  }
  
  df <- qread(input_file) |>
    mutate(country = "NIE") |>
    mutate(Cluster = ifelse(!is.na(Cluster), 1, NA)) |>
    mutate(across(starts_with("Children_Seen_"), as.numeric)) |>
    select(
      country, Region = states, District = lgas, Cluster, today,
      matches("Children_seen_h[1-9]|Children_Seen_h10"),
      matches("Sex_Child[1-9]|Sex_Child10"),
      matches("FM_Child[1-9]|FM_Child10"),
      matches("Reason_Not_FM[1-9]|Reason_Not_FM10"),
      matches("Caregiver_Aware_h[1-9]|Caregiver_Aware_h10")
    )
  
  # Parse and extract date info
  df <- df |>
    mutate(
      today = parse_date_time(today, orders = c("ymd", "dmy", "mdy"), quiet = TRUE),
      year = year(today),
      month = format(today, "%b")  # e.g., "janv.", "f\xe9vr."
    )
  
  # Compute households visited
  df <- df |>
    mutate(across(matches("Children_seen_h[1-9]|Children_Seen_h10"),
                  ~ ifelse(!is.na(.), 1, 0),
                  .names = "h_{.col}")) |>
    mutate(tot_hh_visited = rowSums(across(starts_with("h_Children_Seen_")), na.rm = TRUE))
  
  # Clean binary variables
  df <- df |>
    mutate(across(matches("Sex_Child[1-9]|Sex_Child10"), clean_sex_var),
           across(matches("FM_Child[1-9]|FM_Child10"), clean_binary_var),
           across(matches("Caregiver_Aware_h[1-9]|Caregiver_Aware_h10"), clean_binary_var))
  
  # Process reasons
  reason_cols <- c("childnotborn", "childabsent", "noncompliance", "housenotvisited", "security")
  for (r in reason_cols) {
    for (i in 1:10) {
      col_in <- paste0("Reason_Not_FM", i)
      col_out <- paste0("R_", r, i)
      if (col_in %in% names(df)) {
        df[[col_out]] <- ifelse(tolower(df[[col_in]]) == r, 1, 0)
      }
    }
  }
  
  # Summary stats
  df <- df |>
    mutate(
      female_sampled = rowSums(across(matches("Sex_Child[1-9]|Sex_Child10")), na.rm = TRUE),
      male_sampled = tot_hh_visited - female_sampled,
      total_vaccinated = rowSums(across(matches("FM_Child[1-9]|FM_Child10")), na.rm = TRUE),
      missed_child = tot_hh_visited - total_vaccinated
    ) |>
    rowwise() |>
    mutate(
      female_vaccinated = sum(
        unlist(across(matches("Sex_Child[1-9]|Sex_Child10"))) == 1 &
          unlist(across(matches("FM_Child[1-9]|FM_Child10"))) == 1,
        na.rm = TRUE
      ),
      male_vaccinated = total_vaccinated - female_vaccinated
    ) |>
    ungroup()
  
  # Aggregate reasons
  df <- df |>
    mutate(
      R_House_not_visited = rowSums(across(matches("R_housenotvisited[1-9]|R_housenotvisited10")), na.rm = TRUE),
      R_childabsent = rowSums(across(matches("R_childabsent[1-9]|R_childabsent10")), na.rm = TRUE),
      R_Non_Compliance = rowSums(across(matches("R_noncompliance[1-9]|R_noncompliance10")), na.rm = TRUE),
      R_childnotborn = rowSums(across(matches("R_childnotborn[1-9]|R_childnotborn10")), na.rm = TRUE),
      R_security = rowSums(across(matches("R_security[1-9]|R_security10")), na.rm = TRUE),
      Care_Giver_Informed_SIA = rowSums(across(matches("Caregiver_Aware_h[1-9]|Caregiver_Aware_h10")), na.rm = TRUE)
    )
  
  # Round and response
  df <- df |>
    mutate(
      today = as_date(today),
      month = format(today, "%b"),  # ex: "janv.", "f\xe9vr.", etc.
      roundNumber = case_when(
        str_detect(month, "janv.") ~ "Rnd1",
        str_detect(month, "f\xe9vr.") ~ "Rnd2",
        str_detect(month, "mars") ~ "Rnd3",
        str_detect(month, "avr.")  ~ "Rnd4",
        str_detect(month, "mai")  ~ "Rnd5",
        str_detect(month, "juin") ~ "Rnd6",
        str_detect(month, "juil.") ~ "Rnd7",
        str_detect(month, "ao\xfbt") ~ "Rnd8",
        str_detect(month, "sept.") ~ "Rnd9",
        str_detect(month, "oct.")  ~ "Rnd10",
        str_detect(month, "nov.")  ~ "Rnd11",
        str_detect(month, "d\xe9c.")  ~ "Rnd12",
        TRUE ~ NA_character_
      )
    )
  
  df <- df |>
    mutate(
      roundNumber = case_when(
        year == 2025 & month %in% c("oct.", "nov.") ~ "Rnd1",
        year == 2025 & month %in% c("ao\xfbt", "sept.") ~ "Rnd1",
        year == 2025 & month %in% c("juin", "juil.") ~ "Rnd2",
        year == 2025 & month %in% c("janv.", "f\xe9vr.", "mars", "avr.", "mai") ~ "Rnd1",
        year == 2024 & month %in% c("f\xe9vr.", "mars") ~ "Rnd1",
        year == 2024 & month %in% c("avr.", "mai", "juin","ao\xfbt") ~ "Rnd2",
        year == 2024 & month %in% c("sept.", "oct.") ~ "Rnd3",
        year == 2024 & month == "nov." ~ "Rnd4",
        year == 2024 & month == "d\xe9c." ~ "Rnd5",
        year == 2023 & month %in% c("janv.", "mai") ~ "Rnd1",
        year == 2023 & month %in% c("juin", "juil.", "ao\xfbt") ~ "Rnd2",
        year == 2023 & month %in% c("sept.", "oct.") ~ "Rnd3",
        year == 2023 & month == "nov." ~ "Rnd4",
        year == 2023 & month == "d\xe9c." ~ "Rnd5",
        TRUE ~ roundNumber
      ),
      total_sampled = tot_hh_visited,
      vaccine.type = case_when(
        year == 2025 & month %in% c("oct.", "nov.") ~ "nOPV2",
        year == 2025 & month %in% c("ao\xfbt", "sept.") ~ "nOPV2 & bOPV",
        year == 2025 & month %in% c("juin", "juil.") ~ "nOPV2",
        year == 2025 & month %in% c("janv.", "avr.") ~ "nOPV2",
        year == 2024 ~ "nOPV2",
        year == 2023 & month == "mai" ~ "fIPV+nOPV2",
        year == 2023 & month == "juil." ~ "fIPV+nOPV2",
        year == 2023 & month %in% c("ao\xfbt", "oct.", "nov.", "d\xe9c.") ~ "nOPV2",
        year == 2023 & month == "sept." ~ "fIPV+nOPV2",
        year %in% 2020:2022 ~ "bOPV",
        TRUE ~ "nOPV2"
      ),
      response = case_when(
        year == 2025 & month %in% c("oct.", "nov.") ~ "NIE-2025-10-01_nOPV_sNID",
        year == 2025 & month %in% c("ao\xfbt", "sept.") ~ "NIE-2025-08-01_n-bOPV_sNID",
        year == 2025 & month %in% c("juin", "juil.") ~ "NIE-2025-04-01_nOPV_NIDs",
        year == 2025 & month %in% c("avr.", "mai") ~ "NIE-2025-04-01_nOPV_NIDs", 
        year == 2024 ~ "NIE-2024-nOPV2",
        year == 2023 & month %in% c("mai", "juin") ~ "NIE-2023-04-02_nOPV",
        year == 2023 & month %in% c("juil.", "ao\xfbt", "sept.", "oct.", "nov.", "d\xe9c.") ~ "NIE-2023-07-03_nOPV",
        year == 2020 ~ "NGA-20DS-01-2020",
        year == 2021 & month %in% c("mars", "avr.") ~ "NGA-2021-013-1",
        year == 2021 & month %in% c("avr.", "mai") ~ "NGA-2021-011-1",
        year == 2021 & month == "juin" ~ "NGA-2021-016-1",
        year == 2021 & month %in% c("juil.", "ao\xfbt") ~ "NGA-2021-014-1",
        year == 2021 & month == "sept." ~ "NGA-2021-020-2",
        TRUE ~ "OBR_name"
      )
    )
  
  # vaccine.type = "nOPV2",
  # response = paste0("NIE-", year, "-nOPV2"),
  # total_sampled = tot_hh_visited
  # )
  
  # Aggregate to cluster level
  df <- df |>
    filter(year > 2019) |>
    group_by(country, Region, District, response, vaccine.type, roundNumber) |>
    summarise(
      start_date = min(today),
      end_date = max(today),
      year = year(start_date),
      numbercluster = sum(Cluster, na.rm = TRUE),
      male_sampled = sum(male_sampled, na.rm = TRUE),
      female_sampled = sum(female_sampled, na.rm = TRUE),
      total_sampled = sum(total_sampled, na.rm = TRUE),
      male_vaccinated = sum(male_vaccinated, na.rm = TRUE),
      female_vaccinated = sum(female_vaccinated, na.rm = TRUE),
      total_vaccinated = sum(total_vaccinated, na.rm = TRUE),
      missed_child = sum(missed_child, na.rm = TRUE),
      r_Non_Compliance = sum(R_Non_Compliance, na.rm = TRUE),
      r_House_not_visited = sum(R_House_not_visited, na.rm = TRUE),
      r_childabsent = sum(R_childabsent, na.rm = TRUE),
      # r_Child_is_a_visitor = sum(R_Child_is_a_visitor, na.rm = TRUE),
      r_security = sum(R_security, na.rm = TRUE),
      r_childnotborn = sum(R_childnotborn, na.rm = TRUE),
      Care_Giver_Informed_SIA = sum(Care_Giver_Informed_SIA, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      percent_care_Giver_Informed_SIA = ifelse(total_sampled > 0, round(Care_Giver_Informed_SIA / total_sampled * 100, 2), 0),
      total_missed = ifelse(total_sampled < 60, 60 - total_sampled + missed_child, missed_child),
      status = ifelse(total_missed <= 3, "Pass", "Fail"),
      performance = case_when(
        total_missed < 4 ~ "high",
        total_missed < 9 ~ "moderate",
        total_missed < 20 ~ "poor",
        TRUE ~ "very poor"
      ),
      tot_r = r_Non_Compliance + r_House_not_visited + r_childabsent + r_security + r_childnotborn,
      other_r = pmax(total_missed - tot_r, 0),
      prct_r_Non_Compliance = ifelse(total_missed > 0, round(r_Non_Compliance / total_missed * 100, 2), 0),
      prct_r_House_not_visited = ifelse(total_missed > 0, round(r_House_not_visited / total_missed * 100, 2), 0),
      prct_r_childabsent = ifelse(total_missed > 0, round(r_childabsent / total_missed * 100, 2), 0),
      prct_r_childnotborn = ifelse(total_missed > 0, round(r_childnotborn / total_missed * 100, 2), 0),
      prct_r_security = ifelse(total_missed > 0, round(r_security / total_missed * 100, 2), 0),
      prct_other_r = ifelse(total_missed > 0, round(other_r / total_missed * 100, 2), 0)
    ) |> 
    mutate(
      start_date = case_when(
        response == "OBR_name" & 
          year(start_date) == 2025 & 
          month(start_date) == 2 &  
          roundNumber == "Rnd1" ~ as_date("2025-01-20"),
        
        response == "OBR_name" & 
          year(start_date) == 2025 & 
          month(start_date) == 3 &  
          roundNumber == "Rnd1" ~ as_date("2025-01-20"),
        
        response == "NIE-2025-04-01_nOPV_NIDs" & 
          year(start_date) == 2025 & 
          month(start_date) == 5 &  
          roundNumber == "Rnd1" ~ as_date("2025-04-26"),
        
        response == "NIE-2025-04-01_nOPV_NIDs" & 
          year(start_date) == 2025 & 
          month(start_date) == 7 &  
          roundNumber == "Rnd2" ~ as_date("2025-06-14"),
        
        TRUE ~ start_date
      )
    )
    
  ##########################################################"
  ##########################################################
  
  ##########################################################
  ##########################################################
  # Load preparedness data and join
  prep_data <- read_excel(prep_data_file) |>
    filter(Country == "NIGERIA") |>
    mutate(
      `Round Number` = case_when(
        `Round Number` == "Round 0" ~ "Rnd0",
        `Round Number` == "Round 1" ~ "Rnd1",
        `Round Number` == "Round 2" ~ "Rnd2",
        `Round Number` == "Round 3" ~ "Rnd3",
        `Round Number` == "Round 4" ~ "Rnd4",
        `Round Number` == "Round 5" ~ "Rnd5",
        `Round Number` == "Round 6" ~ "Rnd6",
        TRUE ~ `Round Number`
      ))
  
  # Prepare the lookup table
  prep_data <- prep_data |> 
    rename(
      response = `OBR Name`,        
      vaccine.type = Vaccines,      
      roundNumber = `Round Number`  
    ) |> 
    mutate(
      round_start_date = as_date(`Round Start Date`),  
      start_date = round_start_date + 4,               
      end_date = as_date(start_date) + 1)
  
  # convert to tibble
  # Prepare the lookup table with necessary columns
  prep_data <- prep_data |> 
    select(response, vaccine.type, roundNumber, round_start_date, start_date, end_date)
  
  lookup_table <- as_tibble(prep_data) |> 
    mutate(
      start_date = as_date(start_date),
      end_date = as_date(end_date),
      round_start_date = as_date(round_start_date)
    )
  
  # Join the lookup table with the original data `G`
  F1 <- df |> 
    left_join(lookup_table, by = c("response", "vaccine.type", "roundNumber")) |> 
    mutate(
      start_date = coalesce(start_date.y, as_date(start_date.x)),  # Replace missing start_date
      # lqas_end_date = coalesce(end_date.y, as_date(end_date.x)),  # Replace missing lqas_end_date
      end_date = as_date(start_date) + 1,
      # Handle `round_start_date`: fallback to `start_date - 4` days if missing
      round_start_date = coalesce(round_start_date, start_date - days(4))
    ) |> 
    select(-start_date.x, -start_date.y, -end_date.x, -end_date.y) |> 
    filter(!is.na(District))
  
  
  district6 <- c(
    "YUSUFARI",
    "GURI",
    "BIRINIWA",
    "KIRI KASAMA",
    "NGURU",
    "MACHINA",
    "KARASUWA",
    "BARDE"
  )
  
  district10 <- c(
    "BIU", "MUBI NORTH", "MUBI SOUTH", "MONGUNO", "BAYO", "BEBEJI", "BICHI",
    "BIRINIWA", "BIRNIN KUDU", "MADOBI", "MALAM MADURI", "MARTE", "MAYO-BELWA",
    "MICHIKA", "MIGA", "MISAU", "MOBBAR", "ABADAM", "NASSARAWA", "NGALA",
    "NGANZAI", "NINGI", "AJINGI", "ALBASU", "ALKALERI", "ASKIRA/UBA", "AUYO",
    "BABURA", "BAGWAI", "BAMA", "BARDE", "BAUCHI", "KURA", "KWAYA KUSAR",
    "LARMURDE", "MACHINA", "MADAGALI", "MAFA", "MAGUMERI", "MAIDUGURI",
    "MAIGATARI", "MAIHA", "MAKODA", "MINJIBIR", "NGURU", "NUMAN", "BOGORO",
    "BORSARI", "GARUM MALLAM", "NANGERE", "BUJI", "BUNKURE", "POTISKUM",
    "CHIBOK", "RANO", "DALA", "DAMATURU", "DAMBAN", "DAMBATTA", "DAMBOA",
    "RIMIN GADO", "ROGO", "RINGIM", "TORO", "FUNE", "GARKI", "GABASAWA",
    "GAGARAWA", "GAMAWA", "GANJUWA", "GANYE", "WARJI", "JAMA'ARE", "JERE",
    "KABO", "KATAGUM", "KAUGAMA", "KAZAURE", "KIBIYA", "KIRFI", "KIRI KASAMA",
    "KIRU", "KIYAWA", "KONDUGA", "KUKAWA", "KUMBOTSO", "KUNCHI", "RONI",
    "DARAZO", "DASS", "DEMSA", "DIKWA", "DOGUWA", "SHANI", "SHANONO",
    "SHELLENG", "DUTSE", "SHIRA", "SONG", "SULE TANKAKAR", "SUMAILA",
    "TAFAWA-BALEWA", "TAKAI", "TARAUNI", "TARMUA", "TAURA", "TEUNGO", "TOFA",
    "TSANYAWA", "TUDUN WADA", "JAKUSKO", "DAWAKIN KUDU", "KAGA", "KALA/BALGE",
    "KANO MUNICIPAL", "KARASUWA", "KARAYE", "ZAKI", "FAGGE", "UNGONGO", "FIKA",
    "FUFORE", "WARAWA", "JAHUN", "KAFIN HAUSA", "DAWAKIN TOFA", "GARKO",
    "YOLA NORTH", "YOLA SOUTH", "GUJBA", "GUMEL", "GULANI", "JADA", "GEZAWA",
    "GEIDAM", "GURI", "GIADE", "GAYA", "WUDIL", "GOMBI", "YANKWASHI", "YUNUSARI",
    "GIRIE", "GWOZA", "HADEJIA", "GUBIO", "GWIWA", "ITAS/GADAU", "HAWUL",
    "HONG", "GWARAM", "GWARZO", "GWALE", "YUSUFARI", "GUZAMALA", "GUYUK"
  )
  
  
  F1<-F1 |> 
    mutate(vaccine.type = case_when(
      District %in% district6 & response == "NIE-2025-04-01_nOPV_NIDs" ~ "nOPV2 & bOPV",
      District %in% district10 & response == "NIE-2025-10-01_nOPV_sNID" ~ "nOPV2 & bOPV",
      TRUE ~ vaccine.type
    )) |> 
    select(
          country, province =Region, district=District, response, vaccine.type,
          roundNumber, numbercluster, round_start_date, start_date, end_date, year, male_sampled, female_sampled,
          total_sampled, male_vaccinated, female_vaccinated, total_vaccinated, missed_child,
          r_Non_Compliance, r_House_not_visited, r_childabsent, r_security, r_childnotborn,
          Care_Giver_Informed_SIA, percent_care_Giver_Informed_SIA, total_missed, status, performance,
          tot_r, other_r, prct_r_Non_Compliance, prct_r_House_not_visited, prct_r_childabsent,
          prct_r_childnotborn, prct_r_security, prct_other_r 
        )
  
  return(F1)
}

# Execute
final_data <- process_lqas_data(input_file)

final_data

# write_csv(final_data, output_file)

write.csv(
  final_data,
  file = "C:/Users/TOURE/Documents/REPOSITORIES/LQAS_raw_data/LQAS_level1/272.csv",
  row.names = FALSE
)


