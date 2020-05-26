
library(readxl)
library(haven)
library(fBasics)
library(lubridate)
library(data.table)
library(dplyr)
library(tidyverse)

source('./Help.R')
options(scipen=999)  # disable scientific notation

# check files and perimeter
reload_file_list = T    # get list of files in Data
reload_chiave_comune = T    # main reference for perimeter
reload_summary_banks_clients = T  # count total ABI and NDG
reload_file_rows = T    # evaluate number of rows and other stats in each file - takes 1 hour
reload_BILDATI_column_list = T    # column description for all BILDATI.CSV
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
  ABI_nome = read.csv('./Coding_tables/ABI_nome_banca.csv', sep=';', stringsAsFactors = F, colClasses = 'character') %>%
    setNames(c('Abi', 'Banca'))
  ABI_folder = read_excel("Coding_tables/CSD_contenuto/FileAssociazioniAbi_Cartelle.xlsx", 
                          col_types = c("numeric", "text", "text", "text", "text")) %>%
    filter(Cartelle != 'FCVWDWF32') %>%
    select(-...5) %>%
    mutate(Folder_directory = ifelse(Cartelle %in% unique(file_list$main_folder), 'PRESENT', 'MISSING')) %>%
    left_join(ABI_nome, by = "Abi")
  if (sum(is.na(ABI_folder$Banca)) > 0){cat('\n\n ###### missing bank name in ABI_nome:\n',
                                            paste0(ABI_folder %>% filter(is.na(Banca)) %>% pull(Abi), collapse = '\n'))}
  if (nrow(ABI_folder) != uniqueN(ABI_folder$Abi)){cat('\n\n ###### error in ABI_folder')}
  if (reload_chiave_comune == F){
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
    saveRDS(chiave_comune, './Checkpoints/chiave_comune.rds')
  }
  chiave_comune = readRDS('./Checkpoints/chiave_comune.rds')
  total_chiave_comune = uniqueN(chiave_comune$chiave_comune)
  
  # brief summary of banks by clients (chiave_comune) and number of accounts
  if (reload_summary_banks_clients == F){
    summary_banks = chiave_comune %>%
      group_by(abi) %>%
      summarize(Total_chiave_comune = uniqueN(chiave_comune),
                Total_NDG = uniqueN(ndg)) %>%
      arrange(desc(Total_NDG)) %>%
      mutate(`% NDG` = round(Total_NDG / nrow(chiave_comune) * 100, 2)) %>%
      left_join(ABI_nome, by = c('abi' = 'Abi'))
    if (sum(summary_banks$Total_NDG) != nrow(chiave_comune)){
      cat('\n\n ###### error in summary_banks')
    } else {
      saveRDS(summary_banks, './Checkpoints/summary_banks.rds')
    }
    
    summary_clients = chiave_comune %>%
      group_by(chiave_comune) %>%
      summarize(Total_ABI = uniqueN(abi),
                Total_NDG = uniqueN(ndg)) %>%
      ungroup() %>%
      group_by(Total_ABI, Total_NDG) %>%
      summarize(Count_chiave_comune = n(),
                chiave_comune = paste0(chiave_comune[1:min(c(10, n()))], collapse = '|')) %>%
      ungroup()
    if (sum(summary_clients$Count_chiave_comune) != total_chiave_comune){
      cat('\n\n ###### error in summary_clients')
    } else {
      saveRDS(summary_clients, './Checkpoints/summary_clients.rds')
    }
  }
  summary_banks = readRDS('./Checkpoints/summary_banks.rds')
  write.table(summary_banks, './Stats/00_Summary_banks.csv', sep = ';', row.names = F, append = F)
  
  summary_clients = readRDS('./Checkpoints/summary_clients.rds')
  summary_clients_by_ABI = summary_clients %>%
    group_by(Total_ABI) %>%
    summarize(Count_chiave_comune = sum(Count_chiave_comune)) %>%
    mutate(`%` = round(Count_chiave_comune / total_chiave_comune * 100, 2)) %>%
    arrange(desc(`%`))
  if (sum(summary_clients_by_ABI$Count_chiave_comune) != total_chiave_comune){
    cat('\n\n ###### error in summary_clients_by_ABI')
  } else {
    write.table(summary_clients_by_ABI, './Stats/00_Summary_clients_by_ABI.csv', sep = ';', row.names = F, append = F)
  }
  
  # check folder contents
  matched_folder = unique(chiave_comune$Cartelle)
  expected_files = unique(file_list$unique_file_name)
  expected_date_filenames = unique(file_list$date_filename)
  
  summary_folder_content =suppressWarnings(
    file_list %>%
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
      left_join(ABI_nome, by = "Abi") %>%
      select(main_folder, Abi, matched_folder, everything()) %>%
      arrange(desc(matched_folder), missing)
  )
  write.table(summary_folder_content, './Stats/00_summary_folder_content.csv', sep = ';', row.names = F, append = F)
  
  # count rows in each file
  {
    ######     too slow     ######
    # run git bash the following command. To run the git bash, right click on folder and "Git bash here"
    # be sure the log file is deleted, results will be appended otherwise
    # symbolic link doesn't
    # find "D:\UniPV data\BCC-default\Data" -type f -name "*.CSV" -exec wc -l "{}" >> "C:\Users\Alessandro Bitetto\Downloads\UniPV\BCC-default\Checkpoints\rows_log.txt" \;
    
    ######     this one counts only rows     ######
    # row_count = function(x){
    #   length(count.fields(x, skip = 1))
    # }
    # summary_file_rows = file_list %>%
    #   left_join(summary_folder_content %>% select(main_folder, matched_folder), by = "main_folder") %>%
    #   rowwise() %>% 
    #   mutate(total_rows = possibly(row_count, otherwise = NA_real_)(paste0('./Data/', path))) %>%
    #   mutate(EOF_warning = ifelse(is.na(total_rows), 'YES', 'NO')) %>%
    #   mutate(total_rows = ifelse(is.na(total_rows), read.csv(paste0('./Data/', path), sep=';') %>% nrow(), total_rows)) %>%
    #   select(main_folder, matched_folder, file, total_rows, EOF_warning)
    # saveRDS(summary_file_rows, './Checkpoints/summary_file_rows.rds')
  }   # alternatives
  if (reload_file_rows == F){
    summary_file_rows = file_list %>%
      left_join(summary_folder_content %>% select(main_folder, matched_folder, Abi, Banca), by = "main_folder") %>%
      do(data.frame(., file_stats(paste0('./Data/', .$path)), stringsAsFactors = F)) %>%
      select(-path, -date_filename) %>%
      select(main_folder, matched_folder, unique_file_name, file, everything()) %>%
      select(-Abi, - Banca, Abi, Banca) %>%
      rename(`% missing` = missing_perc,
             `% blank` = blank_perc)
    saveRDS(summary_file_rows, './Checkpoints/summary_file_rows.rds')
  }
  summary_file_rows = readRDS('./Checkpoints/summary_file_rows.rds')
  write.table(summary_file_rows, './Stats/00_summary_file_rows.csv', sep = ';', row.names = F, append = F)
  
  
  
  
  
  
  # check files with same unique_file_name but different number of columns
  check_file_rows = summary_file_rows %>%
    as.data.frame() %>%
    group_by(unique_file_name) %>%
    summarize(cols_number = uniqueN(cols)) %>%
    filter(cols_number > 1)
  if (nrow(check_file_rows) > 0){cat('\n\n ###### warning: different number of columns for file:\n', 
                                     paste0(check_file_rows$unique_file_name, collapse = '\n'))}
  
  # BILDATI.CSV check column format to correctly feed data_loader()
  {
    bilanci_list = summary_file_rows %>%
      filter(unique_file_name == 'BILDATI.CSV') %>%
      filter(matched_folder == 'YES') %>%
      mutate(path = paste0('./Data/', main_folder, '/', file))
    
    if (reload_BILDATI_column_list == F){
      BILDATI_column_list = c()
      for (f in bilanci_list$path){
        file = read.csv(f, sep=';', stringsAsFactors = F)
        f_name = strsplit(f, '/')[[1]][4]
        rows = nrow(file)
        tt = c()
        for (col in colnames(file)){
          t_col = file[ ,col]
          full_NA = ifelse(sum(is.na(t_col)) == rows, T, F)
          full_blank = ifelse(sum(t_col == '', na.rm = T) == rows, T, F)
          if (full_NA | full_blank){
            comma_ind = dot_ind = c()
          } else {
            comma_ind = grepl(',', t_col)
            dot_ind = grepl('\\.', t_col)
          }
          comma_sample = t_col[comma_ind][1]
          dot_sample = t_col[dot_ind][1]
          col_format = 
            tt = tt %>% bind_rows(
              data.frame(file = f_name,
                         rows = rows,
                         variable = col,
                         empty = full_NA | full_blank,
                         format = class(t_col),
                         comma = sum(comma_ind) > 0,
                         dot = sum(dot_ind) > 0,
                         comma_sample = ifelse(is.na(comma_sample), '', comma_sample),
                         dot_sample = ifelse(is.na(dot_sample), '', as.character(dot_sample)),
                         stringsAsFactors = F)
            )
        } # col
        BILDATI_column_list = BILDATI_column_list %>% bind_rows(tt)
      } # f
      saveRDS(BILDATI_column_list, './Checkpoints/BILDATI_column_list.rds')
    }
    BILDATI_column_list = readRDS('./Checkpoints/BILDATI_column_list.rds')
    
    BILDATI_summary = BILDATI_column_list %>%
      mutate(format = ifelse(empty == T, '', format)) %>%    # logical occurs only when column is empty
      group_by(variable) %>%
      summarize(number_of_files = n(),
                always_empty = sum(empty) == n(),
                multiple_format = ifelse(uniqueN(setdiff(format, '')) > 1, 'YES', 'NO'),
                format = paste0(unique(setdiff(format, '')), collapse = ' - '),
                comma = sum(comma > 0),
                dot = sum(dot > 0),
                comma_sample = paste0(unique(comma_sample), collapse = '|'),
                dot_sample = paste0(unique(dot_sample), collapse = '|')
      )
    if (uniqueN(BILDATI_summary$variable) != nrow(BILDATI_summary)){
      cat('\n\n ###### error in BILDATI_summary')
    } else {
      write.table(BILDATI_summary, './Checks/00_BILDATI_summary.csv', sep = ';', row.names = F, append = F)
    }
    
    BILDATI_always_empty = BILDATI_summary %>% filter(always_empty)
    BILDATI_multiple_format = BILDATI_summary %>% filter(multiple_format == 'YES')
    cat('\n -- Statistics on BILDATI files:\n')
    if (nrow(BILDATI_always_empty) > 0){
    cat('\n     -', nrow(BILDATI_always_empty), 'variables always empty in', min(BILDATI_always_empty$number_of_files), 'to',
        max(BILDATI_always_empty$number_of_files), 'files (different ABI)')}
    if (nrow(BILDATI_multiple_format) > 0){
      cat('\n     -', nrow(BILDATI_multiple_format), 'variables have multiple formats:\n')
      print(BILDATI_multiple_format %>% select(variable, format, number_of_files) %>% as.data.frame())}
    
  }
  
  
}



print(sort(unique(file_list$unique_file_name)))


# read csv and format columns according to file type (unique_file_name) from file_list or summary_file_rows
data_loader = function(file_path){}


unique_file_name = sub('.*\\d', '', strsplit(file_path, '/')[[1]][4])

if (unique_file_name == "AI_ANFA.CSV"){
  col_class = c()
} else if (unique_file_name == ""){
  
} else if (unique_file_name == ""){
  
} else if (unique_file_name == ""){
  
} else if (unique_file_name == ""){
  
} else if (unique_file_name == ""){
  
} else if (unique_file_name == ""){
  
} else if (unique_file_name == ""){
  
} else if (unique_file_name == ""){
  
} else if (unique_file_name == ""){
  
} else {
  cat('\n\n ###### error:', unique_file_name, 'not found for', file_path)
}

file = read.csv(file_path, sep=';', stringsAsFactors = F)


# todo: una volta capiti come sono fatti i file crea un "loader" che, in base al nome del file, formatti le colonne del file da leggere
#       e faccia le eventuali trasformazioni (tipo sostituire , con . per i decimali). Magari puoi mettere un switch per capire solo il formato
#       atteso delle colonne così da avere una descrizione precisa nella funzione file_stats -> in realtà è superfluo perché se leggi già il file formattato
#       con le trasformazioni tutte le statistiche vengono già calcolate correttamente.
# todo: puoi anche ritornare il numero di colonne scartate (ad esempio abi = ***) così da aggiungere l'info in file_stats


# todo: controlla 3599 se ha molti file vuoti

# todo: capisci cosa fare con i file che hanno 0 righe. Si può calcolare quante banche sono coinvolte ed eventualmente se ci sono mesi particolari
#       come nella pivot che avevo creato

