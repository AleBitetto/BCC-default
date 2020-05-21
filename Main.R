
library(readxl)
library(haven)
library(fBasics)
library(lubridate)
library(data.table)
library(dplyr)
library(tidyverse)


# check files and perimeter
reload_file_list = T
reload_file_rows = T
{
  # folders and files list
  if (reload_file_list == F){
    file_list = data.frame(path = list.files(path = './Data', all.files = T,
                                             full.names = FALSE, recursive = T), stringsAsFactors = F) %>%
      separate(path, c('main_folder', 'file'), sep= '/', remove = F) %>%
      mutate(unique_file_name = sub('.*\\d', '', file)) %>%
      rowwise() %>%
      mutate(date_filename = gsub(main_folder, '', file)) %>%
      filter(main_folder != 'FCVWDWF32')
    saveRDS(file_list, './Checkpoints/file_list.rds')
  }
  file_list = readRDS('./Checkpoints/file_list.rds')
  
  # check chiave comune
  ABI_folder = read_excel("Coding_tables/CSD_contenuto/FileAssociazioniAbi_Cartelle.xlsx", 
                          col_types = c("numeric", "text", "text", "text", "text")) %>%
    filter(Cartelle != 'FCVWDWF32') %>%
    select(-...5) %>%
    mutate(Folder_directory = ifelse(Cartelle %in% unique(file_list$main_folder), 'PRESENT', 'MISSING'))
  if (nrow(ABI_folder) != uniqueN(ABI_folder$Abi)){cat('\n\n ###### error in ABI_folder')}
  unavailable_folders = ABI_folder %>%
    filter(Folder_directory == 'MISSING') %>%
    pull(Abi)
  cat('\n -- Folders unavailable from FileAssociazioniAbi_Cartelle:', length(unavailable_folders), 'out of', nrow(ABI_folder))
  
  chiave_comune = read_dta("Coding_tables/chiave_comune.dta") %>%
    as.data.frame(stringsAsFactors = F) %>%
    mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
    mutate(abi = as.character(abi),
           chiave_comune = as.character(chiave_comune)) %>%
    left_join(ABI_folder %>% select(Abi, Cartelle, Folder_directory), by = c('abi' = 'Abi'))
  if (nrow(chiave_comune) != chiave_comune %>% select(-chiave_comune) %>% unique() %>% nrow()){cat('\n\n ###### error in chiave_comune')}
  unique_chiave = uniqueN(chiave_comune$chiave_comune)
  unique_chiave_accounts = nrow(chiave_comune)
  unique_ABI = uniqueN(chiave_comune$abi)
  cat('\n -- Total unique Chiave Comune:', format(unique_chiave, big.mark=","), '(', format(unique_chiave_accounts, big.mark=","),
      'total accounts in', unique_ABI, 'ABI )')

  ABI_missing_in_folder = setdiff(chiave_comune$abi, ABI_folder$Abi)
  ABI_missing_in_chiave_available = setdiff(ABI_folder %>% filter(Folder_directory == 'PRESENT') %>% pull(Abi), chiave_comune$abi)
  ABI_missing_in_chiave_unavailable = setdiff(ABI_folder %>% filter(Folder_directory == 'MISSING') %>% pull(Abi), chiave_comune$abi)
  cat('\n -- ABI missing in ALL folders but present in chiave_comune:\n', paste0(ABI_missing_in_folder, sep='\n'))
  cat('\n -- ABI missing in chiave_comune but present in AVAILABLE folders:\n', paste0(ABI_missing_in_chiave_available, sep='\n'))
  cat('\n -- ABI missing in chiave_comune but present in UNAVAILABLE folders:\n', paste0(ABI_missing_in_chiave_unavailable, sep='\n'))
  
  chiave_comune = chiave_comune %>%
    filter(!is.na(Cartelle)) %>%
    filter(Folder_directory == 'PRESENT')
  unique_chiave_filter = uniqueN(chiave_comune$chiave_comune)
  unique_chiave_accounts_filter = nrow(chiave_comune)
  unique_ABI_filter = uniqueN(chiave_comune$abi)
  if (unique_ABI_filter != uniqueN(chiave_comune$Cartelle)){cat('\n\n ###### error in chiave_comune filter')}
  cat('\n -- Total unique Chiave Comune after matching folders:',
      format(unique_chiave_filter, big.mark=","), '(', format(unique_chiave_accounts_filter, big.mark=","),
      'total accounts in', unique_ABI_filter, 'ABI )')
  
  summary_chiave_comune = data.frame(
    A = c('Total unique chiave_comune', 'ABI missing in ALL folders but present in chiave_comune',
          'ABI missing in chiave_comune but present in AVAILABLE folders', 'ABI missing in chiave_comune but present in UNAVAILABLE folders',
          'Folders unavailable from FileAssociazioniAbi_Cartelle', 'Total unique Chiave Comune after matching folders'),
    B = c(paste0(format(unique_chiave, big.mark=","), ' (', format(unique_chiave_accounts, big.mark=","),
                 ' total accounts in ', unique_ABI, ' ABI)'),
          paste0(length(ABI_missing_in_folder), ': ', paste0(ABI_missing_in_folder, collapse = ', ')),
          paste0(length(ABI_missing_in_chiave_available), ': ', paste0(ABI_missing_in_chiave_available, collapse = ', ')),
          paste0(length(ABI_missing_in_chiave_unavailable), ifelse(length(ABI_missing_in_chiave_unavailable) > 0, ': ', ''),
                 paste0(ABI_missing_in_chiave_unavailable, collapse = ', ')),
          paste0(length(unavailable_folders), ifelse(length(unavailable_folders) > 0, ': ', ''),
                 paste0(unavailable_folders, collapse = ', ')),
          paste0(format(unique_chiave_filter, big.mark=","), ' (', format(unique_chiave_accounts_filter, big.mark=","),
                 ' total accounts in ', unique_ABI_filter, ' ABI)')),
    c = c('A', 'B', 'C', 'D', 'E', 'A - C - D - E'),
    stringsAsFactors = F
  )
  write.table(summary_chiave_comune, './Stats/00_Summary_chiave_comune.csv', sep = ';', row.names = F, col.names = F, append = F)

  # check folder contents
  matched_folder = unique(chiave_comune$Cartelle)
  expected_files = unique(file_list$unique_file_name)
  expected_date_filenames = unique(file_list$date_filename)
  
  summary_folder_content = file_list %>%
    group_by(main_folder) %>%
    summarise(total_files = n()) %>%
    ungroup() %>%
    mutate(matched_folder = ifelse(main_folder %in% matched_folder, 'YES', 'NO')) %>%
    left_join(file_list %>%
                group_by(main_folder) %>%
                summarise(missing = length(setdiff(expected_date_filenames, date_filename)),
                          missing_files = substr(paste0(setdiff(expected_date_filenames, date_filename), collapse = ' , '), 1, 60)),
              by = "main_folder") %>%
    left_join(ABI_folder %>% select(Cartelle, Abi), by = c('main_folder' = 'Cartelle')) %>%
    select(main_folder, Abi, matched_folder, everything()) %>%
    arrange(desc(matched_folder), missing)
  write.table(summary_folder_content, './Stats/00_summary_folder_content.csv', sep = ';', row.names = F, append = F)
  
  
  row_count = function(x){
    length(count.fields(x, skip = 1))
  }
  if (reload_file_rows == F){
  summary_file_rows = file_list %>%
    left_join(summary_folder_content %>% select(main_folder, matched_folder), by = "main_folder") %>%
    rowwise() %>% 
    mutate(total_rows = possibly(row_count, otherwise = NA_real_)(paste0('./Data/', path))) %>%
    mutate(EOF_warning = ifelse(is.na(total_rows), 'YES', 'NO')) %>%
    mutate(total_rows = ifelse(is.na(total_rows), read.csv(paste0('./Data/', path), sep=';') %>% nrow(), total_rows)) %>%
    select(main_folder, matched_folder, file, total_rows, EOF_warning)
  saveRDS(summary_file_rows, './Checkpoints/summary_file_rows.rds')
  }
  summary_file_rows = readRDS('./Checkpoints/summary_file_rows.rds')
  write.table(summary_file_rows, './Stats/00_summary_file_rows.csv', sep = ';', row.names = F, append = F)
  
  
  # start_time <- Sys.time()
  # aa= summary_file_rows[500:700,] %>%
  #   rowwise() %>% 
  #   mutate(total_rows = possibly(row_count, otherwise = NA_real_)(paste0('./Data/', path))) %>%
  #   # mutate(EOF_warning = ifelse(is.na(total_rows), 'YES', 'NO')) %>%
  #   # mutate(t1 = fread(paste0('./Data/', path), sep=';') %>% nrow()) %>%
  #   # mutate(t2 = row_count2(paste0('./Data/', path)))
  #   mutate(total_rows = ifelse(is.na(total_rows), fread(paste0('./Data/', path), sep=';') %>% nrow(), total_rows))
  # Sys.time() - start_time
  # 
  # start_time <- Sys.time()
  # aa1= summary_file_rows[500:700,] %>%
  #   rowwise() %>% 
  #   # mutate(total_rows = possibly(row_count, otherwise = NA_real_)(paste0('./Data/', path))) %>%
  #   # mutate(EOF_warning = ifelse(is.na(total_rows), 'YES', 'NO')) %>%
  #   # mutate(t1 = fread(paste0('./Data/', path), sep=';') %>% nrow()) %>%
  #   mutate(t2 = row_count2(paste0('./Data/', path)))
  # # mutate(total_rows = ifelse(is.na(total_rows), fread(paste0('./Data/', path), sep=';') %>% nrow(), total_rows))
  # Sys.time() - start_time
  # 
  # 
  # 
  # row_count2 <- function(x){
  #   shell(paste("wc -l", x), intern = TRUE)
  #   # as.numeric(strsplit(out, ' ')[[1]][1]) - 1
  # }
  # 
  # x = paste0('./Data/','CSDAL011/CSDAL011COLL.CSV')
  # row_count2(x)
  # length(count.fields(paste0('./Data/','CSDAL011/CSDAL011COLL.CSV'), skip = 1))
  # 
  # for (i in 1:nrow(summary_file_rows)){
  # 
  # m=length(count.fields(paste0('./Data/','CSDAL011/CSDAL011COLL.CSV'), skip = 1))
  # }
  # a=read.csv(paste0('./Data/',summary_file_rows$path[542]), sep=';', stringsAsFactors = F)

}