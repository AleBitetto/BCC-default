
utils::memory.limit(64000)
library(readxl)
library(haven)
library(lubridate)
library(stringr)
library(data.table)
library(dplyr)
library(tidyverse)

source('./Help.R')
options(scipen=999)  # disable scientific notation

# compile functions
{
  library(compiler)
  enableJIT(3)
  setCompilerOptions(optimize=3)
  setCompilerOptions(suppressAll = TRUE)
  funlist=lsf.str()
  for (i in c(1:length(funlist))){
    comfun=cmpfun(eval(parse(text=funlist[i])),options=list(suppressUndefined=T))
    assign(funlist[i],comfun)
  }
}

### check files and perimeter
reload_file_list = T    # get list of files in Data
reload_chiave_comune = T    # main reference for perimeter
reload_summary_banks_clients = T  # count total ABI and NDG
reload_file_rows = T    # evaluate number of rows and other stats in each file - takes 1 hour
reload_BILDATI_column_list = T    # column description for all BILDATI.CSV
reload_censimento = T   # reload censimento of NDG from folders
reload_collegamento = T   # reload check for COLLEGAMENTO
reload_censimento_CRIF = T   # reload censimento of NDG from folders CRIF
{
  # folders and files list
  if (reload_file_list == F){
    file_list = data.frame(path = list.files(path = './Data/CSD/', all.files = T,
                                             full.names = FALSE, recursive = T), stringsAsFactors = F) %>%
      separate(path, c('main_folder', 'file'), sep= '/', remove = F) %>%
      mutate(unique_file_name = sub('.*\\d', '', file)) %>%
      rowwise() %>%
      mutate(date_filename = gsub(main_folder, '', file)) %>%
      mutate(date = sub(unique_file_name, '', date_filename)) %>%
      filter(main_folder != 'FCVWDWF32')
    saveRDS(file_list, './Checkpoints/file_list.rds')
  }
  file_list = readRDS('./Checkpoints/file_list.rds')
  
  # check chiave comune
  {
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
      
      chiave_comune = read_dta("./Data/CRIF/chiave_comune.dta") %>%
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
  }
  chiave_comune = readRDS('./Checkpoints/chiave_comune.rds')
  total_chiave_comune = uniqueN(chiave_comune$chiave_comune)
  
  # brief summary of banks by clients (chiave_comune) and number of accounts
  {
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
    write.table(summary_banks, './Stats/01_Summary_banks.csv', sep = ';', row.names = F, append = F)
    
    summary_clients = readRDS('./Checkpoints/summary_clients.rds')
    summary_clients_by_ABI = summary_clients %>%
      group_by(Total_ABI) %>%
      summarize(Count_chiave_comune = sum(Count_chiave_comune)) %>%
      mutate(`%` = round(Count_chiave_comune / total_chiave_comune * 100, 2)) %>%
      arrange(desc(`%`))
    if (sum(summary_clients_by_ABI$Count_chiave_comune) != total_chiave_comune){
      cat('\n\n ###### error in summary_clients_by_ABI')
    } else {
      write.table(summary_clients_by_ABI, './Stats/01_Summary_clients_by_ABI.csv', sep = ';', row.names = F, append = F)
    }
  }
  
  # check NDG with a single chiave_comune
  {
    ndg_count_chiave_comune = chiave_comune %>%
      select(-Cartelle, -Folder_directory) %>%
      left_join(chiave_comune %>%
                  group_by(chiave_comune) %>%
                  summarize(Tot_NDG = n()))
    
    check_chiave_comune_single_NDG_list = ndg_count_chiave_comune %>%
      filter(Tot_NDG == 1) %>%
      arrange(abi) %>%
      select(-Tot_NDG)
    summary_chiave_comune_single_NDG = check_chiave_comune_single_NDG_list %>%
      group_by(abi) %>%
      summarize(Total_NDG = n()) %>%
      arrange(desc(Total_NDG)) %>%
      left_join(ABI_nome, by = c('abi' = 'Abi'))
    if (sum(ndg_count_chiave_comune$Tot_NDG == 1) != sum(summary_chiave_comune_single_NDG$Total_NDG)){
      cat('\n\n ###### error in summary_chiave_comune_single_ABI')
    } else {
      write.table(summary_chiave_comune_single_NDG, './Checks/01_chiave_comune_single_NDG_summary.csv', sep = ';', row.names = F, append = F)
      write.table(check_chiave_comune_single_NDG_list %>%
                    mutate(abi = paste0('\'', abi),
                           ndg = paste0('\'', ndg),
                           chiave_comune = paste0('\'', chiave_comune)), './Checks/01_chiave_comune_single_NDG_list.csv', sep = ';', row.names = F, append = F)
    }
  }
  
  # check folders content
  {
    matched_folder = unique(chiave_comune$Cartelle)
    expected_files = unique(file_list$unique_file_name)
    expected_date_filenames = unique(file_list$date_filename)
    
    summary_folder_content = suppressWarnings(
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
    write.table(summary_folder_content, './Stats/02_Summary_folder_content.csv', sep = ';', row.names = F, append = F)
  }
  
  # count rows in each file and check files with 0 rows
  {
    {
      ######     too slow     ######
      # run git bash the following command. To run the git bash, right click on folder and "Git bash here"
      # be sure the log file is deleted, results will be appended otherwise
      # symbolic link doesn't
      # find "D:\UniPV data\BCC-default\Data\CSD\" -type f -name "*.CSV" -exec wc -l "{}" >> "C:\Users\Alessandro Bitetto\Downloads\UniPV\BCC-default\Checkpoints\rows_log.txt" \;
      
      ######     this one counts only rows     ######
      # row_count = function(x){
      #   length(count.fields(x, skip = 1))
      # }
      # summary_file_rows = file_list %>%
      #   left_join(summary_folder_content %>% select(main_folder, matched_folder), by = "main_folder") %>%
      #   rowwise() %>% 
      #   mutate(total_rows = possibly(row_count, otherwise = NA_real_)(paste0('./Data/CSD/', path))) %>%
      #   mutate(EOF_warning = ifelse(is.na(total_rows), 'YES', 'NO')) %>%
      #   mutate(total_rows = ifelse(is.na(total_rows), read.csv(paste0('./Data/CSD/', path), sep=';') %>% nrow(), total_rows)) %>%
      #   select(main_folder, matched_folder, file, total_rows, EOF_warning)
      # saveRDS(summary_file_rows, './Checkpoints/summary_file_rows.rds')
    }   # alternatives
    if (reload_file_rows == F){
      summary_file_rows = file_list %>%
        left_join(summary_folder_content %>% select(main_folder, matched_folder, Abi, Banca), by = "main_folder") %>%
        do(data.frame(., file_stats(paste0('./Data/CSD/', .$path)), stringsAsFactors = F)) %>%
        select(-path, -date_filename) %>%
        select(main_folder, matched_folder, unique_file_name, file, date, everything()) %>%
        select(-Abi, - Banca, Abi, Banca) %>%
        rename(`% missing` = missing_perc,
               `% blank` = blank_perc) %>%
        as.data.frame()
      saveRDS(summary_file_rows, './Checkpoints/summary_file_rows.rds')
    }
    summary_file_rows = readRDS('./Checkpoints/summary_file_rows.rds')
    write.table(summary_file_rows, './Stats/02_Summary_file_rows.csv', sep = ';', row.names = F, append = F)
    
    check_file_always_empty = summary_file_rows %>%
      group_by(unique_file_name) %>%
      summarize(always_empty = sum(rows == 0) == n()) %>%
      filter(always_empty)
    if (nrow(check_file_always_empty) > 0){cat('\n\n -- file always empty:\n', paste0(check_file_always_empty$unique_file_name, collapse = '\n'))}
    summary_0_rows = summary_file_rows %>%
      filter(rows == 0) %>%
      filter(matched_folder == 'YES') %>%
      rowwise()  %>%
      mutate(date = sub(unique_file_name, '', file)) %>%
      mutate(date = sub(main_folder, '', date))
    summary_0_rows_by_ABI = summary_0_rows %>%
      as.data.frame() %>%
      group_by(Abi, Banca) %>%
      summarise(different_files = uniqueN(unique_file_name),
                total_files = n(),
                min_date = min(date),
                max_date = max(date),
                files = paste0(unique(unique_file_name), collapse = ' | ')) %>%
      mutate(min_date = paste0(c(substr(min_date, 5, 6), substr(min_date, 1, 4)), collapse = '-')) %>%
      mutate(max_date = paste0(c(substr(max_date, 5, 6), substr(max_date, 1, 4)), collapse = '-')) %>%
      arrange(desc(total_files))
    summary_0_rows_by_file = summary_0_rows %>%
      mutate(always_empty = unique_file_name %in% check_file_always_empty$unique_file_name) %>%
      as.data.frame() %>%
      group_by(unique_file_name, always_empty) %>%
      summarise(different_ABI = uniqueN(Abi),
                total_files = n(),
                min_date = min(date),
                max_date = max(date),
                ABI = paste0(unique(Abi), collapse = ' | ')) %>%
      mutate(min_date = paste0(c(substr(min_date, 5, 6), substr(min_date, 1, 4)), collapse = '-')) %>%
      mutate(max_date = paste0(c(substr(max_date, 5, 6), substr(max_date, 1, 4)), collapse = '-')) %>%
      mutate(always_empty = ifelse(always_empty, 'YES', 'NO')) %>%
      arrange(desc(total_files))
    write.table(summary_0_rows_by_ABI, './Stats/02_Summary_0_rows_by_ABI.csv', sep = ';', row.names = F, append = F)
    write.table(summary_0_rows_by_file, './Stats/02_Summary_0_rows_by_file.csv', sep = ';', row.names = F, append = F)
  }
  
  # check files with same unique_file_name but different number of columns
  check_file_rows = summary_file_rows %>%
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
      mutate(path = paste0('./Data/CSD/', main_folder, '/', file))
    
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
      ) %>%
      mutate(always_empty = ifelse(always_empty, 'YES', 'NO'))
    if (uniqueN(BILDATI_summary$variable) != nrow(BILDATI_summary)){
      cat('\n\n ###### error in BILDATI_summary')
    } else {
      write.table(BILDATI_summary, './Checks/00_BILDATI_summary.csv', sep = ';', row.names = F, append = F)
    }
    
    BILDATI_always_empty = BILDATI_summary %>% filter(always_empty == 'YES')
    BILDATI_multiple_format = BILDATI_summary %>% filter(multiple_format == 'YES')
    cat('\n\n -- Statistics on BILDATI files:\n')
    if (nrow(BILDATI_always_empty) > 0){
      cat('\n     -', nrow(BILDATI_always_empty), 'variables always empty in', min(BILDATI_always_empty$number_of_files), 'to',
          max(BILDATI_always_empty$number_of_files), 'files (different ABI)')}
    if (nrow(BILDATI_multiple_format) > 0){
      cat('\n     -', nrow(BILDATI_multiple_format), 'variables have multiple formats:\n')
      print(BILDATI_multiple_format %>% select(variable, format, number_of_files) %>% as.data.frame())}
    
  }
  
  # check and recover anagrafica from original CRIF files
  {
    CRIF_anag = read_dta("./Data/CRIF/anag_imp.dta") %>%
      as.data.frame(stringsAsFactors = F) %>%
      mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
      mutate(Source = 'anag_imp') %>%
      bind_rows(read_dta("./Data/CRIF/anag_poe.dta") %>%
                  as.data.frame(stringsAsFactors = F) %>%
                  mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
                  mutate(Source = 'anag_poe')) %>%
      bind_rows(read_dta("./Data/CRIF/anag_prv.dta") %>%
                  as.data.frame(stringsAsFactors = F) %>%
                  mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
                  mutate(Source = 'anag_prv')) %>%
      bind_rows(read_dta("./Data/CRIF/anag_sb.dta") %>%
                  as.data.frame(stringsAsFactors = F) %>%
                  mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
                  mutate(Source = 'anag_sb')) %>%
      mutate(abi = as.character(as.numeric(abi)))
    missing_ABI_in_CRIF = setdiff(unique(summary_banks$abi), unique(CRIF_anag$abi))
    missing_ABI_in_CSD = setdiff(unique(CRIF_anag$abi), unique(summary_banks$abi))
    if (length(missing_ABI_in_CRIF) + length(missing_ABI_in_CSD) > 0){
      cat('\n -- Missing ABI in CRIF anagrafica:', missing_ABI_in_CRIF, '\n    Missing ABI in CSD:', missing_ABI_in_CSD)
      CRIF_anag_report = data.frame(A = c(rep('Missing ABI in CRIF', length(missing_ABI_in_CRIF)), rep('Missing ABI in CSD', length(missing_ABI_in_CSD))),
                                    Abi = c(missing_ABI_in_CRIF, missing_ABI_in_CSD), stringsAsFactors = F) %>%
        left_join(ABI_nome, by = "Abi")
      write.table(CRIF_anag_report, './Stats/03_Missing_ABI_CRIF_anag.csv', sep = ';', row.names = F, col.names = F, append = F)
    }
    
    summary_CRIF_anag_match = chiave_comune %>%
      select(abi, ndg) %>%
      unique() %>%
      left_join(CRIF_anag %>%
                  select(abi, ndg, Source) %>%
                  unique() %>%
                  mutate(check = 1), by = c("abi", "ndg")) %>%
      group_by(abi) %>%
      summarise(Total_match = sum(check, na.rm = T),
                # Total_NA = sum(is.na(check)),
                # Total_row = n(),
                `% match` = round(sum(check, na.rm = T) / n() * 100, 1),
                from_anag_imp = sum(Source == 'anag_imp', na.rm = T),
                from_anag_poe = sum(Source == 'anag_poe', na.rm = T),
                from_anag_prv = sum(Source == 'anag_prv', na.rm = T),
                from_anag_sb = sum(Source == 'anag_sb', na.rm = T)) %>%
      left_join(ABI_nome, by = c('abi' = 'Abi')) %>%
      arrange(desc(`% match`))
    write.table(summary_CRIF_anag_match, './Stats/03_Summary_ABI_CRIF_anag_match.csv', sep = ';', row.names = F, col.names = T, append = F)
    
    segmento_list_CRIF = CRIF_anag %>%
      select(abi, ndg, segmento) %>%
      unique()
    
    check_segmento_list_CRIF = segmento_list_CRIF %>%
      group_by(abi, ndg) %>%
      summarize(Count = n())
    
    if (max(check_segmento_list_CRIF$Count) > 1){
      cat('\n\n ###### error in segmento_list_CRIF')
    } else {
      saveRDS(segmento_list_CRIF, './Checkpoints/segmento_list_CRIF.rds')
    }
  }
  segmento_list_CRIF = readRDS('./Checkpoints/segmento_list_CRIF.rds')
  
  # recover anagrafica (only segmento) from CSD files
  {
    DA_file_list = summary_file_rows %>%
      filter(unique_file_name == 'DA.CSV')
    
    segmento_list_CSD = c()
    
    for (i in 1:nrow(DA_file_list)){
      
      cat(i, '/', nrow(DA_file_list), end = '\r')
      abi = DA_file_list$Abi[i]
      main_folder = DA_file_list$main_folder[i]
      file = DA_file_list$file[i]
      data = read.csv(paste0('./Data/CSD/', main_folder, '/', file), sep=';', stringsAsFactors = F) %>%
        select(COD_NAG, DATA_RIFERIMENTO, SEGMENTO_RISCHIO) %>%
        rename(ndg = COD_NAG,
               segmento = SEGMENTO_RISCHIO) %>%
        unique() %>%
        mutate(abi = abi,
               ndg = sub('\'', '', ndg),
               segmento = sub('\'', '', segmento),
               DATA_RIFERIMENTO = sub('\'', '', DATA_RIFERIMENTO))
      
      if (nrow(data) > 0){
        data = data %>%
          group_by(abi, ndg) %>%
          arrange(desc(DATA_RIFERIMENTO)) %>%
          slice(1:1) %>%
          ungroup() %>%
          select(-DATA_RIFERIMENTO)
        segmento_list_CSD = segmento_list_CSD %>%
          bind_rows(data)
      }
      if (i %% 300 == 0){
        segmento_list_CSD = segmento_list_CSD %>%
          unique()
      }
    }
    
    segmento_list_CSD = segmento_list_CSD %>%
      unique() %>%
      group_by(abi, ndg) %>%
      slice(1:1) %>%
      ungroup()
    
    check_segmento_list_CSD = segmento_list_CSD %>%
      group_by(abi, ndg) %>%
      summarize(Count = n())
    
    if (max(check_segmento_list_CSD$Count) > 1){
      cat('\n\n ###### error in segmento_list_CSD')
    } else {
      saveRDS(segmento_list_CSD, './Checkpoints/segmento_list_CSD.rds')
    }
  }
  segmento_list_CSD = readRDS('./Checkpoints/segmento_list_CSD.rds')
  
  # check censimento of common chiave_comune within each folder among all different unique_file_name from CSD
  {
    if (reload_censimento == F){
      perimeter_banks = summary_file_rows %>%
        filter(matched_folder == 'YES') %>%
        pull(Abi) %>%
        unique()
      
      NDG_censimento_CSD = c()
      
      for (bank_abi in perimeter_banks){
        
        cat('\n bank:', bank_abi, which(perimeter_banks == bank_abi), '/', length(perimeter_banks))
        bank_set = summary_file_rows %>%
          filter(Abi == bank_abi) %>%
          filter(rows > 0)
        main_folder = unique(bank_set$main_folder)
        
        start=Sys.time()
        NDG_list = c()
        for (file_name in unique(bank_set$unique_file_name)){
          
          if (!file_name %in% c("DECO.CSV", "AI_LEGENDA.CSV", "COLL.CSV")){
            
            load_list = bank_set %>%
              filter(unique_file_name == file_name)
            
            ndg = c()
            for (file in load_list$file){ # loop same file for different dates (if any)
              
              data = read.csv(paste0('./Data/CSD/', main_folder, '/', file), sep=';', stringsAsFactors = F)
              if (file_name != "BILANA.CSV"){
                ndg = c(ndg, unique(data$COD_NAG))
                # cat('\n', file, '  ', length(data$COD_NAG))
              } else {
                ndg = c(ndg, unique(data$CATEGORIA))
                # cat('\n', file, '  ', length(data$CATEGORIA))
              }
              rm(data)
              ndg = unique(ndg)
            } # file
            
            NDG_list = c(NDG_list, ndg)
            # NDG_list = NDG_list %>%
            #   bind_rows(data.frame(file = file_name, ndg = ndg, stringsAsFactors = F))
          } # if file_name
        } # file_name
        cat('   - total NDG:', format(length(NDG_list), big.mark=","))
        tot_diff=seconds_to_period(difftime(Sys.time(),start, units='secs'))
        cat(' - Read time: ', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
        
        bind_start = Sys.time()
        NDG_censimento_CSD = NDG_censimento_CSD %>%
          bind_rows(data.frame(abi = bank_abi, ndg = unique(NDG_list), stringsAsFactors = F))
        cat(' - Normalized bind time:', round(as.numeric(round(difftime(Sys.time(),bind_start, units='secs'))) / length(NDG_list) * 1e6, 1))
      } # bank
      
      NDG_censimento_CSD = NDG_censimento_CSD %>%
        mutate(ndg = sub('\'', '', ndg))
      saveRDS(NDG_censimento_CSD, './Checkpoints/NDG_censimento_CSD.rds')
    }
    NDG_censimento_CSD = readRDS('./Checkpoints/NDG_censimento_CSD.rds')
    
    # check with chiave comune
    check_censimento = NDG_censimento_CSD %>%
      left_join(chiave_comune %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg"))
    summary_censimento = data.frame(Censimento = c('matched', 'not matched'),
                                    Chiave_comune = c(sum(!is.na(check_censimento$chiave_comune)), sum(is.na(check_censimento$chiave_comune))),
                                    Unique_chiave_comune = c(uniqueN(check_censimento$chiave_comune), 0),
                                    stringsAsFactors = F) %>%
      mutate(C = paste0(' (', round(Chiave_comune / sum(Chiave_comune) * 100), '%)'),
             E = paste0(' (', round(Unique_chiave_comune / total_chiave_comune * 100), '%)'),
             D = paste0(round(Chiave_comune / nrow(chiave_comune) * 100), '%'),
             Chiave_comune = format(Chiave_comune, big.mark=","), 
             Unique_chiave_comune = format(Unique_chiave_comune, big.mark=",")) %>%
      mutate(D = c(D[1], ''),
             E = c(E[1], ''),
             Unique_chiave_comune = c(Unique_chiave_comune[1], '')) %>%
      bind_rows(data.frame(Censimento = 'Total', Chiave_comune = paste0(format(nrow(check_censimento), big.mark=","), '      '), C = '',
                           E = '', Unique_chiave_comune = paste0(format(total_chiave_comune, big.mark=","), '      '),
                           D = format(nrow(chiave_comune), big.mark=","), stringsAsFactors = F)) %>%
      mutate(Chiave_comune = paste0(Chiave_comune, C),
             Unique_chiave_comune = paste0(Unique_chiave_comune, E)) %>%
      rename(`NDG in censimento` = Chiave_comune,
             `Unique chiave comune` = Unique_chiave_comune,
             `% of NDG in chiave_comune` = D) %>%
      select(-C, -E) %>%
      left_join(
        check_censimento %>%
          left_join(segmento_list_CSD, by = c("abi", "ndg")) %>%
          mutate(segmento = ifelse(is.na(segmento), 'not available', segmento)) %>%
          group_by(segmento) %>%
          summarize(matched = sum(!is.na(chiave_comune)),
                    not_matched = sum(is.na(chiave_comune))) %>%
          ungroup() %>%
          # rename(Censimento = segmento) %>%
          mutate(Total = matched + not_matched) %>%
          bind_rows(data.frame(matched = sum(.['matched']), not_matched = sum(.['not_matched']), Total = sum(.['Total']),
                               segmento = 'Total', stringsAsFactors = F)) %>%
          mutate(matched = paste0(format(matched, big.mark=","), ' (', round(matched / nrow(check_censimento) * 100), '%)'),
                 not_matched = paste0(format(not_matched, big.mark=","), ' (', round(not_matched / nrow(check_censimento) * 100), '%)'),
                 Total = paste0(format(Total, big.mark=","), ' (', round(Total / nrow(check_censimento) * 100), '%)')) %>%
          t() %>%
          as.data.frame(stringsAsFactors = F) %>%
          setNames(.[1,]) %>%
          mutate(Censimento = rownames(.)) %>%
          filter(Censimento != 'segmento') %>%
          select(-`not available`, `not available`, -Total, Total) %>%
          mutate(Censimento = gsub('_', ' ', Censimento)), by = "Censimento"
      )
    
    segmento_coding = data.frame(code = c('CLARG', 'CNOCL', 'CPMI', 'CSSMAL', 'PPRIV','PPROD'),
                                 label = c('Corporate large',	'Corporate non classificati',	'Piccole Medie Imprese', 'Corporate Small',
                                           'Famiglie consumatrici',	'Famiglie produttrici'), stringsAsFactors = F)
    for (i in 1:nrow(segmento_coding)){
      colnames(summary_censimento) = gsub(segmento_coding$code[i], segmento_coding$label[i], colnames(summary_censimento))
    }
    
    if (as.numeric(gsub(',', '', summary_censimento$`NDG in censimento`[3])) != nrow(check_censimento)){
      cat('\n\n ###### error in summary_censimento')
    } else {
      write.table(summary_censimento, './Stats/04_NDG_censimento.csv', sep = ';', row.names = F, append = F)
    }
  }
  
  # check matching NDG in COLLEGAMENTO
  {
    if (reload_collegamento == F){
      coll_list = summary_file_rows %>%
        filter(matched_folder == 'YES') %>%
        filter(unique_file_name =='COLL.CSV')
      
      check_COLLEGAMENTO = check_COLLEGAMENTO_list = DESCRIZIONE_list = c()
      for (i in 1:nrow(coll_list)){
        
        cat(i, '/', nrow(coll_list), end='\r')
        bank_abi = coll_list$Abi[i]
        
        coll_coding = read.csv(paste0('./Data/CSD/', coll_list$main_folder[i], '/', coll_list$main_folder[i], 'DECO.CSV'), sep=';', stringsAsFactors = F) %>%
          filter(COD_TABELLA == '\'COLL') %>%
          select(COD_ELEMENTO, DESCRIZIONE) %>%
          rename(COD_TIPO_COLLEGAMENTO = COD_ELEMENTO) %>%
          mutate(COD_TIPO_COLLEGAMENTO = gsub(' ', '', COD_TIPO_COLLEGAMENTO))
        DESCRIZIONE_list = DESCRIZIONE_list %>%
          bind_rows(coll_coding) %>% unique()
        
        match_NDG = NDG_censimento_CSD %>% filter(abi == bank_abi) %>%
          left_join(chiave_comune %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg")) %>%
          mutate(chiave_comune = replace(chiave_comune, is.na(chiave_comune), 'NA'))
        
        data = read.csv(paste0('./Data/CSD/', coll_list$main_folder[i], '/', coll_list$file[i]), sep=';', stringsAsFactors = F) %>%
          mutate(COD_ABI = bank_abi,
                 COD_NAG = sub('\'', '', COD_NAG),
                 COD_NAG_COLLEGATO = sub('\'', '', COD_NAG_COLLEGATO),
                 COD_TIPO_COLLEGAMENTO = gsub(' ', '', COD_TIPO_COLLEGAMENTO)) %>%
          left_join(coll_coding, by = "COD_TIPO_COLLEGAMENTO") %>%
          left_join(match_NDG %>% rename(COD_NAG_chiave_comune = chiave_comune), by = c('COD_ABI' = 'abi', 'COD_NAG' = 'ndg')) %>%
          left_join(match_NDG %>% rename(COD_NAG_COLLEGATO_chiave_comune = chiave_comune), by = c('COD_ABI' = 'abi', 'COD_NAG_COLLEGATO' = 'ndg'))
        
        NA_descrizione = data %>%
          filter(is.na(DESCRIZIONE))
        NA_COD_NAG = data %>%
          filter(is.na(COD_NAG_chiave_comune))
        NA_COD_NAG_COLLEGATO = data %>%
          filter(is.na(COD_NAG_COLLEGATO_chiave_comune))
        different_linked_chiave_comune = data %>%
          filter(!is.na(COD_NAG_chiave_comune)) %>%
          filter(!is.na(COD_NAG_COLLEGATO_chiave_comune)) %>%
          filter(COD_NAG_chiave_comune == COD_NAG_COLLEGATO_chiave_comune) %>%
          filter(COD_NAG_chiave_comune != 'NA') %>%
          filter(COD_NAG != COD_NAG_COLLEGATO)
        
        check_COLLEGAMENTO = check_COLLEGAMENTO %>%
          bind_rows(data.frame(abi = bank_abi, folder = coll_list$main_folder[i],
                               missing_DESCRIZIONE = paste0(unique(NA_descrizione$COD_TIPO_COLLEGAMENTO), collapse = ', '),
                               missing_COD_NAG_in_censimento = nrow(NA_COD_NAG), missing_COD_NAG_COLLEGATO_in_censimento = nrow(NA_COD_NAG_COLLEGATO),
                               different_linked_chiave_comune = nrow(different_linked_chiave_comune), stringsAsFactors = F))
        
        if (nrow(different_linked_chiave_comune)){
          check_COLLEGAMENTO_list = check_COLLEGAMENTO_list %>%
            bind_rows(different_linked_chiave_comune)
        }
        if (sum(is.na(data)) != nrow(NA_descrizione) + nrow(NA_COD_NAG) + nrow(NA_COD_NAG_COLLEGATO)){cat('\n --', bank_abi, 'unexpected NAs')}
      } # i
      saveRDS(check_COLLEGAMENTO, './Checkpoints/check_COLLEGAMENTO.rds')
      saveRDS(check_COLLEGAMENTO_list, './Checkpoints/check_COLLEGAMENTO_list.rds')
      saveRDS(DESCRIZIONE_list, './Checkpoints/DESCRIZIONE_list.rds')
    }
    check_COLLEGAMENTO = readRDS('./Checkpoints/check_COLLEGAMENTO.rds')
    check_COLLEGAMENTO_list = readRDS('./Checkpoints/check_COLLEGAMENTO_list.rds')
    DESCRIZIONE_list = readRDS('./Checkpoints/DESCRIZIONE_list.rds') %>% arrange(COD_TIPO_COLLEGAMENTO)
    if (sum(check_COLLEGAMENTO$different_linked_chiave_comune) != nrow(check_COLLEGAMENTO_list)){
      cat('\n\n ###### error in check_COLLEGAMENTO')
    } else {
      write.table(check_COLLEGAMENTO, './Stats/04_NDG_COLLEGAMENTO.csv', sep = ';', row.names = F, append = F)
      write.table(check_COLLEGAMENTO_list, './Stats/04_NDG_COLLEGAMENTO_list_same_chiave_comune.csv', sep = ';', row.names = F, append = F)
    }
  }
  
  # check censimento of common chiave_comune within each folder among all different files from CRIF
  {
    if (reload_censimento_CRIF == F){
      file_list_CRIF = data.frame(path = list.files(path = './Data/CRIF/', pattern = 'dta', all.files = T,
                                                    full.names = FALSE, recursive = T), stringsAsFactors = F) %>%
        filter(!grepl('collegamenti|ditte_individuali|decodifiche|chiave_comune|bilana', path))
      
      NDG_censimento_CRIF = c()
      date_list_CRIF = c()
      for (file in file_list_CRIF$path){
        
        cat('\n -', file, '-', which(file == file_list_CRIF$path), '/', nrow(file_list_CRIF))
        if (file %in% c('ai_facocede.dta', 'ai_pofi.dta', 'ai_facoti.dta', 'ai_facogepa.dta')){
          data = read_dta(paste0('./Data/CRIF/', file), col_select = c('COD_ABI', 'COD_NAG', 'DATA_RIFERIMENTO')) %>%
            as.data.frame(stringsAsFactors = F) %>%
            mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
            rename(abi = COD_ABI,
                   ndg = COD_NAG,
                   data_rif = DATA_RIFERIMENTO) %>%
            mutate(abi = sub('\'', '', abi),
                   ndg = sub('\'', '', ndg),
                   data_rif = sub('\'', '', data_rif))
          data_rif = sort(unique(data$data_rif))
        } else if (file %in% c('bildati_cebi.dta', 'bildati_noncebi.dta')){
          data = read_dta(paste0('./Data/CRIF/', file), col_select = c('cod_abi', 'cod_nag', 'anno')) %>%
            as.data.frame(stringsAsFactors = F) %>%
            mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
            rename(abi = cod_abi,
                   ndg = cod_nag,
                   data_rif = anno)
          data_rif = sort(unique(data$data_rif))
        } else {
          data = read_dta(paste0('./Data/CRIF/', file), col_select = c('abi', 'ndg', 'data_rif')) %>%
            select(abi, ndg, data_rif) %>%
            as.data.frame(stringsAsFactors = F) %>%
            mutate_all(function(x) { attributes(x) <- NULL; x })
          
          data_rif = as.Date(unique(data$data_rif), origin = "1960-01-01")
        }
        if (nrow(data) == 0){cat('  - empty file')}
        data = data  %>%
          mutate(abi = as.character(as.numeric(abi)))
        
        NDG_censimento_CRIF = NDG_censimento_CRIF %>%
          bind_rows(data %>% select(-data_rif) %>% unique())
        
        date_list_CRIF = date_list_CRIF %>%
          bind_rows(data.frame(file = file, dates = paste0(data_rif, collapse = ' - '), stringsAsFactors = F))
      }
      NDG_censimento_CRIF = NDG_censimento_CRIF %>%
        unique()
      saveRDS(NDG_censimento_CRIF, './Checkpoints/NDG_censimento_CRIF.rds')
      saveRDS(date_list_CRIF, './Checkpoints/date_list_CRIF.rds')
    }
    NDG_censimento_CRIF = readRDS('./Checkpoints/NDG_censimento_CRIF.rds')
    date_list_CRIF = readRDS('./Checkpoints/date_list_CRIF.rds')
    
    abi_CRIF = unique(NDG_censimento_CRIF$abi)
    abi_CSD = unique(summary_file_rows %>% filter(matched_folder == 'YES') %>% pull(Abi))
    abi_CRIF_not_CSD = setdiff(abi_CRIF, abi_CSD)
    abi_CSD_not_CRIF = setdiff(abi_CSD, abi_CRIF)
    {
      cat('\n - Total ABI found in censimento CSD:', length(abi_CSD))
      cat('\n - Total ABI found in censimento CRIF:', length(abi_CRIF))
      if (length(abi_CRIF_not_CSD) > 0){cat('\n\n - ABI in CRIF not present in CSD:\n   ', paste0(abi_CRIF_not_CSD, collapse = '\n    '))}
      if (length(abi_CSD_not_CRIF) > 0){cat('\n\n - ABI in CSD not present in CRIF:\n   ', paste0(abi_CSD_not_CRIF, collapse = '\n    '))}
      
    }
    
    write.table(date_list_CRIF, './Stats/04b_NDG_censimento_CRIF_date.csv', sep = ';', row.names = F, append = F)
    
    check_censimento_CRIF = NDG_censimento_CRIF %>%
      left_join(chiave_comune %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg"))
    summary_censimento_CRIF = data.frame(Censimento = c('matched', 'not matched'),
                                         Chiave_comune = c(sum(!is.na(check_censimento_CRIF$chiave_comune)), sum(is.na(check_censimento_CRIF$chiave_comune))),
                                         Unique_chiave_comune = c(uniqueN(check_censimento_CRIF$chiave_comune), 0),
                                         stringsAsFactors = F) %>%
      mutate(C = paste0(' (', round(Chiave_comune / sum(Chiave_comune) * 100), '%)'),
             E = paste0(' (', round(Unique_chiave_comune / total_chiave_comune * 100), '%)'),
             D = paste0(round(Chiave_comune / nrow(chiave_comune) * 100), '%'),
             Chiave_comune = format(Chiave_comune, big.mark=","), 
             Unique_chiave_comune = format(Unique_chiave_comune, big.mark=",")) %>%
      mutate(D = c(D[1], ''),
             E = c(E[1], ''),
             Unique_chiave_comune = c(Unique_chiave_comune[1], '')) %>%
      bind_rows(data.frame(Censimento = 'Total', Chiave_comune = paste0(format(nrow(check_censimento_CRIF), big.mark=","), '      '), C = '',
                           E = '', Unique_chiave_comune = paste0(format(total_chiave_comune, big.mark=","), '      '),
                           D = format(nrow(chiave_comune), big.mark=","), stringsAsFactors = F)) %>%
      mutate(Chiave_comune = paste0(Chiave_comune, C),
             Unique_chiave_comune = paste0(Unique_chiave_comune, E)) %>%
      rename(`NDG in censimento` = Chiave_comune,
             `Unique chiave comune` = Unique_chiave_comune,
             `% of NDG in chiave_comune` = D) %>%
      select(-C, -E) %>%
      left_join(
        check_censimento_CRIF %>%
          left_join(segmento_list_CRIF, by = c("abi", "ndg")) %>%
          mutate(segmento = ifelse(is.na(segmento), 'not available', segmento)) %>%
          group_by(segmento) %>%
          summarize(matched = sum(!is.na(chiave_comune)),
                    not_matched = sum(is.na(chiave_comune))) %>%
          ungroup() %>%
          # rename(Censimento = segmento) %>%
          mutate(Total = matched + not_matched) %>%
          bind_rows(data.frame(matched = sum(.['matched']), not_matched = sum(.['not_matched']), Total = sum(.['Total']),
                               segmento = 'Total', stringsAsFactors = F)) %>%
          mutate(matched = paste0(format(matched, big.mark=","), ' (', round(matched / nrow(check_censimento_CRIF) * 100), '%)'),
                 not_matched = paste0(format(not_matched, big.mark=","), ' (', round(not_matched / nrow(check_censimento_CRIF) * 100), '%)'),
                 Total = paste0(format(Total, big.mark=","), ' (', round(Total / nrow(check_censimento_CRIF) * 100), '%)')) %>%
          t() %>%
          as.data.frame(stringsAsFactors = F) %>%
          setNames(.[1,]) %>%
          mutate(Censimento = rownames(.)) %>%
          filter(Censimento != 'segmento') %>%
          select(-`not available`, `not available`, -Total, Total) %>%
          mutate(Censimento = gsub('_', ' ', Censimento)), by = "Censimento"
      )
    if (as.numeric(gsub(',', '', summary_censimento_CRIF$`NDG in censimento`[3])) != nrow(check_censimento_CRIF)){
      cat('\n\n ###### error in summary_censimento_CRIF')
    } else {
      write.table(summary_censimento_CRIF, './Stats/04b_NDG_censimento_CRIF.csv', sep = ';', row.names = F, append = F)
    }
  }
  
  # match NDG_censimento_CSD and NDG_censimento_CRIF
  {
    summary_common_NDG = NDG_censimento_CRIF %>%
      left_join(NDG_censimento_CSD %>% mutate(Database = 'matched'), by = c("abi", "ndg")) %>%
      mutate(Database = ifelse(is.na(Database), 'not matched', Database)) %>%
      group_by(Database) %>%
      summarize(CRIF = n()) %>%
      ungroup() %>%
      t() %>%
      as.data.frame(stringsAsFactors = F) %>%
      mutate(Database = rownames(.)) %>%
      setNames(.[1,]) %>%
      filter(Database != 'Database') %>%
      mutate(matched = as.numeric(matched),
             `not matched` = as.numeric(`not matched`)) %>%
      mutate(Total = matched + `not matched`) %>%
      mutate(matched = paste0(format(matched, big.mark=","), ' (', round(matched / Total * 100), '%)'),
             `not matched` = paste0(format(`not matched`, big.mark=","), ' (', round(`not matched` / Total * 100), '%)'),
             Total = format(Total, big.mark=",")) %>%
      bind_rows(
        NDG_censimento_CSD %>%
          left_join(NDG_censimento_CRIF %>% mutate(Database = 'matched'), by = c("abi", "ndg")) %>%
          mutate(Database = ifelse(is.na(Database), 'not matched', Database)) %>%
          group_by(Database) %>%
          summarize(CSD = n()) %>%
          ungroup() %>%
          t() %>%
          as.data.frame(stringsAsFactors = F) %>%
          mutate(Database = rownames(.)) %>%
          setNames(.[1,]) %>%
          filter(Database != 'Database') %>%
          mutate(matched = as.numeric(matched),
                 `not matched` = as.numeric(`not matched`)) %>%
          mutate(Total = matched + `not matched`) %>%
          mutate(matched = paste0(format(matched, big.mark=","), ' (', round(matched / Total * 100), '%)'),
                 `not matched` = paste0(format(`not matched`, big.mark=","), ' (', round(`not matched` / Total * 100), '%)'),
                 Total = format(Total, big.mark=","))
        
      ) %>%
      select(Database, everything())
    if (sum(as.numeric(gsub(',', '', summary_common_NDG$Total))) != nrow(NDG_censimento_CSD) + nrow(NDG_censimento_CRIF)){
      cat('\n\n ###### error in summary_common_NDG')
    } else {
      write.table(summary_common_NDG, './Stats/04c_NDG_censimento_CRIF_vs_CSD.csv', sep = ';', row.names = F, append = F)
    }
  }
  
}


### create final dataset
reload_perimeter = T  # reload main perimeter
skip_data_extraction = T  # skip data extraction from every CSV
reload_prepare = T   # reload prepared dataset
reload_final_combination = T   # reload df_final_reference_combination before joining all datasets
{
  # define perimeter
  {
    if (reload_perimeter == F){
      chiave_comune = readRDS('./Checkpoints/chiave_comune.rds')
      segmento_list_CSD = readRDS('./Checkpoints/segmento_list_CSD.rds')
      segmento_list_CRIF = readRDS('./Checkpoints/segmento_list_CRIF.rds')
      NDG_censimento_CSD = readRDS('./Checkpoints/NDG_censimento_CSD.rds')
      NDG_censimento_CRIF = readRDS('./Checkpoints/NDG_censimento_CRIF.rds')
      
      # define common NDG - 1,217,717
      common_NDG = NDG_censimento_CSD %>%
        left_join(NDG_censimento_CRIF %>% mutate(matched = 'YES'), by = c("abi", "ndg")) %>%
        filter(matched == 'YES') %>%
        select(-matched)
      rm(NDG_censimento_CSD, NDG_censimento_CRIF)
      
      # define chiave_comune filling missing with "singleNew_" (we assume they are mono-account clients)
      # and "singleOld_" for clients with a single entry and chiave_comune
      # and "singleCen_" for clients with multiple accounts that have been censored because of common_NDG and became mono-account
      singleOld = read.csv("./Checks/01_chiave_comune_single_NDG_list.csv", sep=";", stringsAsFactors=FALSE) %>%
        mutate(abi = sub('\'', '', abi),
               ndg = sub('\'', '', ndg),
               chiave_comune = sub('\'', '', chiave_comune)) %>%
        left_join(common_NDG %>% mutate(matched = 'YES'), by = c("abi", "ndg")) %>%
        filter(matched == 'YES') %>%
        select(-chiave_comune, -matched) %>%
        left_join(chiave_comune %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg")) %>%
        mutate(chiave_comuneSingle = paste0('singleOld_', chiave_comune)) %>%
        select(-chiave_comune)
      if (sum(singleOld$chiave_comuneSingle == 'singleOld_NA') > 0){cat('\n\n ###### error in singleOld')}
      
      chiave_comune_perimeter = common_NDG %>%
        left_join(chiave_comune %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg")) %>%
        left_join(singleOld, by = c("abi", "ndg")) %>%
        mutate(chiave_comune = ifelse(is.na(chiave_comuneSingle), chiave_comune, chiave_comuneSingle)) %>%
        arrange(desc(is.na(chiave_comune))) %>%
        mutate(chiave_comuneSingle = paste0('singleNew_', 1:n())) %>%
        mutate(chiave_comune = ifelse(is.na(chiave_comune), chiave_comuneSingle, chiave_comune)) %>%
        select(-chiave_comuneSingle)
      
      censored_chiave_comune =  chiave_comune_perimeter %>%
        filter(!grepl("single", chiave_comune)) %>%
        group_by(chiave_comune) %>%
        summarize(count = n()) %>%
        filter(count == 1) %>%
        pull(chiave_comune)
      
      chiave_comune_perimeter = chiave_comune_perimeter %>%
        mutate(chiave_comune = ifelse(chiave_comune %in% censored_chiave_comune, paste0('singleCen_', chiave_comune), chiave_comune))
      
      multiple_chiave_comune =  chiave_comune_perimeter %>%
        filter(!grepl("single", chiave_comune)) %>%
        group_by(chiave_comune) %>%
        summarize(count = n()) %>%
        filter(count > 1) %>%
        pull(count) %>%
        table() %>%
        as.data.frame() %>%
        setNames(c('count', 'freq')) %>%
        mutate(perc = floor(freq / sum(freq) * 100)) %>%
        mutate(label = paste0(count, ' (', perc, '%)'),
               filter = ifelse(perc > 0, 1, 0))
      multiple_chiave_comune_count_label = paste0(paste0(multiple_chiave_comune %>% filter(filter == 1) %>% pull(label), collapse = ' - '),
                                                  ' ... ', multiple_chiave_comune$label[nrow(multiple_chiave_comune)])
      multiple_chiave_comune_ABI =  chiave_comune_perimeter %>%
        filter(!grepl("single", chiave_comune)) %>%
        group_by(chiave_comune) %>%
        summarize(count = uniqueN(abi)) %>%
        filter(count > 1) %>%
        pull(count) %>%
        table() %>%
        as.data.frame() %>%
        setNames(c('count', 'freq')) %>%
        mutate(perc = floor(freq / sum(freq) * 100)) %>%
        mutate(label = paste0(count, ' (', perc, '%)'),
               filter = ifelse(perc > 0, 1, 0))
      multiple_chiave_comune_ABI_count_label = paste0(paste0(multiple_chiave_comune_ABI %>% filter(filter == 1) %>% pull(label), collapse = ' - '),
                                                      ' ... ', multiple_chiave_comune_ABI$label[nrow(multiple_chiave_comune_ABI)])
      
      multiple_ABI_count = chiave_comune_perimeter %>%
        filter(!grepl("single", chiave_comune)) %>%
        group_by(chiave_comune) %>%
        mutate(count_chiave = n()) %>%
        ungroup() %>%
        group_by(count_chiave, abi) %>%
        summarize(count_ABI = n()) %>%
        ungroup() %>%
        group_by(count_chiave) %>%
        arrange(desc(count_ABI)) %>%
        mutate(perc = floor(count_ABI / sum(count_ABI) * 100)) %>%
        mutate(label = paste0(count_ABI, ' (', perc, '%)'),
               filter = ifelse(perc > 2, 1, 0)) %>%
        group_by(count_chiave) %>%
        summarize(Total_ABI = sum(count_ABI),
                  Top_ABI = paste0(label[filter == 1], collapse = ' - ')) %>%
        
        bind_rows(data.frame(count_chiave = NA, Total_ABI = sum(.$Total_ABI), Top_ABI = '', stringsAsFactors = F)) %>%
        mutate(Total_ABI = format(Total_ABI, big.mark=","),
               count_chiave = as.character(count_chiave)) %>%
        replace(is.na(.), 'Total') %>%
        rename(`Multiple chiave_comune` = count_chiave,
               `Total ABI` = Total_ABI,
               `Top ABI` = Top_ABI)
      write.table(multiple_ABI_count, './Stats/05_NDG_perimeter_summary_multiple_ABI.csv', sep = ';', row.names = F, append = F)
      
      singleOld_count = nrow(chiave_comune_perimeter %>% filter(grepl("singleOld", chiave_comune)))
      singleNew_count = nrow(chiave_comune_perimeter %>% filter(grepl("singleNew", chiave_comune)))
      singleCen_count = length(censored_chiave_comune)
      multiple_count = sum(multiple_chiave_comune$freq)
      unique_chiave_comune = uniqueN(chiave_comune_perimeter$chiave_comune)
      if (sum(is.na(chiave_comune_perimeter)) > 0){cat('\n\n ###### error in chiave_comune_perimeter: NA found')}
      if (singleOld_count != nrow(singleOld)){cat('\n\n ###### error in chiave_comune_perimeter: singleOld')}
      if (singleNew_count != nrow(common_NDG %>%
                                  left_join(chiave_comune %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg")) %>%
                                  filter(is.na(chiave_comune)))){cat('\n\n ###### error in chiave_comune_perimeter: singleNew')}
      if (singleCen_count != nrow(chiave_comune_perimeter %>% filter(grepl("singleCen", chiave_comune)))){
        cat('\n\n ###### error in chiave_comune_perimeter: singleCen')}
      if (unique_chiave_comune != singleOld_count + singleNew_count + singleCen_count + multiple_count){
        cat('\n\n ###### error in chiave_comune_perimeter: count of chiave_comune')}
      cat('\n -- Total NDG:', format(nrow(chiave_comune_perimeter), big.mark=","))
      cat('\n       - single account with chiave_comune (saved as singleOld_):', format(singleOld_count, big.mark=","))
      cat('\n       - single account with missing chiave_comune (saved as singleNew_):', format(singleNew_count, big.mark=","))
      cat('\n       - single account with censored chiave_comune, now single (saved as singleCen_):', format(singleCen_count, big.mark=","))
      cat('\n       - multiple accounts:', format(nrow(chiave_comune_perimeter) - singleOld_count - singleNew_count - singleCen_count, big.mark=","))
      summary_perimeter = data.frame(Split = c('single account with chiave_comune (saved as singleOld_)',
                                               'single account with missing chiave_comune (saved as singleNew_)',
                                               'censored single account (saved as singleCen_)',
                                               'multiple accounts', 'Total'),
                                     NDG = c(format(singleOld_count, big.mark=","), format(singleNew_count, big.mark=","),
                                             format(singleCen_count, big.mark=","),
                                             format(nrow(chiave_comune_perimeter) - singleOld_count - singleNew_count - singleCen_count, big.mark=","),
                                             format(nrow(chiave_comune_perimeter), big.mark=",")),
                                     Chiave_comune = c(format(singleOld_count, big.mark=","), format(singleNew_count, big.mark=","),
                                                       format(singleCen_count, big.mark=","), paste0(format(multiple_count, big.mark=","), ' | ', multiple_chiave_comune_count_label),
                                                       format(unique_chiave_comune, big.mark=",")), stringsAsFactors = F)
      write.table(summary_perimeter, './Stats/05_NDG_perimeter_summary.csv', sep = ';', row.names = F, append = F)
      
      # check how segmento from CSD maps into segmento from CRIF and viceversa
      common_segmento = common_NDG %>%
        left_join(segmento_list_CSD %>% rename(segmento_CSD = segmento), by = c("abi", "ndg")) %>%
        left_join(segmento_list_CRIF %>% rename(segmento_CRIF = segmento), by = c("abi", "ndg")) %>%
        replace(is.na(.), 'not available')
      
      segmento_map = as.data.frame(table(factor(common_segmento$segmento_CSD, levels = c(unique(segmento_list_CSD$segmento), 'not available')), 
                                         factor(common_segmento$segmento_CRIF, levels = c(unique(segmento_list_CRIF$segmento), 'not available')))) %>%
        spread(Var2, Freq) %>%
        rename(`CSD\\CRIF` = Var1) %>%
        mutate_all(list(~format(., big.mark=",")))
      write.table(segmento_map, './Stats/05_NDG_perimeter_segmento_mapping.csv', sep = ';', row.names = F, append = F)
      
      # final perimeter with segmento and multi-bank flag
      coding_segmento_CSD = data.frame(segmento_CSD_COD = c('CLARG', 'CNOCL', 'CPMI', 'CSMAL', 'PPRIV','PPROD', 'not available'),
                                       segmento_CSD = c('Corporate large',	'Corporate non classificati',	'Piccole Medie Imprese', 'Corporate Small',
                                                        'Famiglie consumatrici',	'Famiglie produttrici', 'not available'), stringsAsFactors = F)
      
      df_final_reference = chiave_comune_perimeter %>%
        left_join(common_segmento, by = c("abi", "ndg")) %>%
        group_by(chiave_comune) %>%
        mutate(FLAG_Multibank = ifelse(uniqueN(abi) == 1, 0, 1)) %>%
        ungroup() %>%
        as.data.frame() %>%
        mutate(segmento_CSD = gsub(' ', '', segmento_CSD)) %>%
        mutate(segmento_CSD = gsub('notavailable', 'not available', segmento_CSD)) %>%
        rename(segmento_CSD_COD = segmento_CSD) %>%
        left_join(coding_segmento_CSD, by = 'segmento_CSD_COD') %>%
        select(-segmento_CSD_COD) %>%
        select(abi, ndg, chiave_comune, segmento_CSD, segmento_CRIF, FLAG_Multibank)
      if (sum(is.na(df_final_reference)) != 0){
        cat('\n\n ####### NA in df_final_reference')
      } else {
        saveRDS(df_final_reference, './Checkpoints/df_final_reference.rds')
      }
      rm(segmento_list_CRIF, segmento_list_CSD, common_NDG, coding_segmento_CSD)
    }
  }
  df_final_reference = readRDS('./Checkpoints/df_final_reference.rds')
  cat('\n -- Total NDG:', format(nrow(df_final_reference), big.mark=","))
  cat('\n -- Total chiave_comune:', format(uniqueN(df_final_reference$chiave_comune), big.mark=","), '\n')
  
  # extract data
  {
    if (skip_data_extraction == FALSE){
      # add anagrafica from CRIF - yearly
      {
        decodifiche = read_dta("D:/UniPV data/BCC-default/Data/CRIF/decodifiche.dta") %>%
          as.data.frame(stringsAsFactors = F) %>%
          mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
          mutate(abi = as.character(as.numeric(abi))) %>%
          filter(cod_tipo_ndg != '') %>%
          select(abi, cod_tipo_ndg, tipo_ndg) %>%
          rename(tipo_ndg_RECOVER = tipo_ndg) %>%
          mutate(tipo_ndg_RECOVER = ifelse(tipo_ndg_RECOVER == "------------------------------", '', tipo_ndg_RECOVER)) %>%
          unique()
        
        CRIF_anag = read_dta("./Data/CRIF/anag_imp.dta") %>%
          as.data.frame(stringsAsFactors = F) %>%
          mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
          mutate(Source = 'anag_imp') %>%
          bind_rows(read_dta("./Data/CRIF/anag_poe.dta") %>%
                      as.data.frame(stringsAsFactors = F) %>%
                      mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
                      mutate(Source = 'anag_poe')) %>%
          bind_rows(read_dta("./Data/CRIF/anag_prv.dta") %>%
                      as.data.frame(stringsAsFactors = F) %>%
                      mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
                      mutate(Source = 'anag_prv')) %>%
          bind_rows(read_dta("./Data/CRIF/anag_sb.dta") %>%
                      as.data.frame(stringsAsFactors = F) %>%
                      mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
                      mutate(Source = 'anag_sb')) %>%
          mutate(abi = as.character(as.numeric(abi)))
        
        df_anag = df_final_reference %>%
          left_join(CRIF_anag %>% mutate(join_ref = 'YES'), by = c("abi", "ndg")) %>%
          filter(join_ref == 'YES') %>%
          select(-segm_rischio_bcc, -Source, -join_ref) %>%
          mutate(data_rif = as.Date(data_rif, origin = "1960-01-01"),
                 data_cliente = as.Date(data_cliente, origin = "1960-01-01")) %>%
          left_join(decodifiche, by = c("abi", "cod_tipo_ndg")) %>%
          mutate(year = year(data_rif)) %>%
          select(all_of(colnames(df_final_reference)), year, everything())
        
        # check multiple entries
        check_df_anag_multiple = df_anag %>%
          group_by(abi, ndg) %>%
          summarize(Rows = n(),
                    years = paste0(year, collapse = '|')) %>%
          ungroup()
        table(check_df_anag_multiple$Rows)
        # check_mult_entries = check_df_anag_multiple %>%
        #   filter(Rows > 1) %>%
        #   left_join(df_anag, by = c("abi", "ndg")) %>%
        #   mutate(data_cliente = as.character(data_cliente)) %>%
        #   select(-chiave_comune, -Rows, -segmento_CRIF, -segmento_CSD, -segmento, -tipo_ndg_RECOVER, -data_rif, -gg_sconf,
        #          -acc_cassa, -acc_firma, -accordato, -util_cassa, -util_firma, -utilizzato, -starts_with('flag_def')) %>%
        #   replace(is.na(.), 'NA_NA_NA') %>%
        #   group_by(abi, ndg) %>%
        #   summarise_all(funs(uniqueN(.)))
        
        # add prefix ANAG_ to columns
        for (cn in colnames(df_anag)){
          if (!cn %in% c(colnames(df_final_reference), 'data_rif', 'year')){
            df_anag = df_anag %>%
              rename(!!sym(paste0('ANAG_', cn)) := !!sym(cn))
          }
        } # cn
        
        rm(CRIF_anag)
        saveRDS(df_anag, './Checkpoints/compact_data/df_anag.rds')
      }
      df_anag = readRDS('./Checkpoints/compact_data/df_anag.rds')
      
      # add bilanci from CRIF - yearly
      {
        bilana = read_dta("D:/UniPV data/BCC-default/Data/CRIF/bilana.dta") %>%
          as.data.frame(stringsAsFactors = F) %>%
          mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
          mutate(cod_abi = as.character(as.numeric(cod_abi)),
                 anno = as.numeric(anno)) %>%
          rename(abi = cod_abi,
                 ndg = categoria,
                 year = anno) %>%
          select(abi, ndg, cod_de, stato) %>%
          unique()
        
        CRIF_bilanci = read_dta('./Data/CRIF/bilanci.dta') %>%
          as.data.frame(stringsAsFactors = F) %>%
          mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
          select(-conta_pratiche) %>%
          bind_rows(
            read_dta('./Data/CRIF/bilanci_201412.dta') %>%
              as.data.frame(stringsAsFactors = F) %>%
              mutate_all(function(x) { attributes(x) <- NULL; x })
          ) %>%
          mutate(abi = as.character(as.numeric(abi)))
        
        df_bilanci = df_final_reference %>%
          left_join(CRIF_bilanci %>% mutate(join_ref = 'YES'), by = c("abi", "ndg")) %>%
          filter(join_ref == 'YES') %>%
          # select(-COD_DE, -join_ref) %>%
          mutate(data_rif = as.Date(data_rif, origin = "1960-01-01")) %>%
          mutate(year = year(data_rif)) %>%
          select(all_of(colnames(df_final_reference)), year, everything())
        # left_join(bilana, by = c("abi", "ndg"))
        
        # check for duplicates merging bilana
        if (nrow(df_bilanci %>%
                 select(abi, ndg) %>%
                 left_join(bilana %>% select(abi, ndg), by = c("abi", "ndg")) %>%
                 unique()) != 
            nrow(df_bilanci %>%
                 select(abi, ndg) %>%
                 unique())){cat('\n\n ###### error in df_bilanci: duplicates in bilana')}
        
        # check multiple entries
        check_df_bilanci_multiple = df_bilanci %>%
          group_by(abi, ndg) %>%
          summarize(Rows = n(),
                    years = paste0(year, collapse = '|')) %>%
          ungroup()
        table(check_df_bilanci_multiple$Rows)
        
        # add prefix BILA_ to columns
        for (cn in colnames(df_bilanci)){
          if (!cn %in% c(colnames(df_final_reference), 'data_rif', 'year')){
            df_bilanci = df_bilanci %>%
              rename(!!sym(paste0('BILA_', cn)) := !!sym(cn))
          }
        } # cn
        
        rm(CRIF_bilanci, bilana)
        saveRDS(df_bilanci, './Checkpoints/compact_data/df_bilanci.rds')
      }
      df_bilanci = readRDS('./Checkpoints/compact_data/df_bilanci.rds')
      
      # add files from CSD - monthly and create report
      {
        file_cat = c('AI_CC', 'AI_PCSBF', 'AI_ANFA', 'AI_POFI', 'AI_MURA', 'AI_APSE', 'AI_CRFI', 'RISCHIO', 'CR')
        
        sink(paste0('./Log/CSD_aggregation_', format(Sys.time(), "%Y-%m-%dT%H%M"), '.txt'), append = F, split = T, type = 'output')
        cat("\n\n+++++++++++++++++++++", format(Sys.time(), "%Y-%m-%d - %H:%M:%S"),"+++++++++++++++++++++\n\n")
        for (file_type in file_cat){
          create_aggregated_data(df_final_reference, file_type, save_report = F)
        }
        cat("\n\n+++++++++++++++++++++", format(Sys.time(), "%Y-%m-%d - %H:%M:%S"),"+++++++++++++++++++++\n\n")
        sink()
      }
      
      # check report from all CSD file for Forme Tecniche
      {
        file_cat = c('AI_CC', 'AI_PCSBF', 'AI_ANFA', 'AI_POFI', 'AI_MURA', 'AI_APSE', 'AI_CRFI', 'AI_FACOCEDE', 'AI_FACOLIRINO', 'AI_FACOTI', 'RISCHIO')
        forme_tecniche = read.csv2('./Coding_tables/Forme_tecniche.csv', stringsAsFactors=FALSE) %>%
          mutate(COD_FT_RAPPORTO = paste0('\'', COD_FT_RAPPORTO))
        
        report_FT = c()
        for (f in file_cat){
          tt = readRDS(paste0('./Checkpoints/report_data/report_', f ,'.rds')) %>%
            mutate(Perc = round(Total_NDG / sum(Total_NDG) * 100, 1))
          if (f == 'RISCHIO'){
            tt$COD_FT_RAPPORTO = NA
          }
          report_FT = report_FT %>%
            bind_rows(tt)
        }
        report_FT = report_FT %>%
          group_by(File) %>%
          mutate(Total_FT = uniqueN(COD_FT_RAPPORTO)) %>%
          ungroup() %>%
          left_join(forme_tecniche, by = "COD_FT_RAPPORTO") %>%
          group_by(File, Total_FT, COD_FT_RAPPORTO, FT_RAPPORTO, Total_rows) %>%
          arrange(desc(Perc)) %>%
          summarise(Rows_count = paste0(Rows, ' (', Perc, '%)', collapse = ' - '),
                    FT_rows_Perc = round(sum(Total_NDG * Rows) / unique(Total_rows) * 100, 1), .groups = 'drop') %>%
          ungroup() %>%
          arrange(File, desc(FT_rows_Perc)) %>%
          relocate(FT_rows_Perc, .after = FT_RAPPORTO)
        
        write.table(report_FT, './Stats/06_Forme_tecniche_AI_multiple_rows.csv', sep = ';', row.names = F, append = F)
      }
      
      # Check decodifica and matching for CR COD_DATO and COD_PRODOTTO
      {
        summary_file_rows = readRDS('./Checkpoints/summary_file_rows.rds')
        CR_cod_dato = read.csv("./Coding_tables/CR_COD_DATO.csv", sep=";", stringsAsFactors=FALSE) %>%
          mutate(CLS_DATO = as.character(CLS_DATO),
                 COD_DATO = paste0('\'', COD_DATO))
        CR_cod_prodotto = read.csv("./Coding_tables/CR_COD_PRODOTTO.csv", sep=";", stringsAsFactors=FALSE) %>%
          mutate_all(list(as.character)) %>%
          mutate(CODICE = ifelse(CODICE == 'CR/CS', 'CR', CODICE)) %>%
          mutate(CODICE = gsub(' ', '', CODICE)) %>%
          rename(COD_PRODOTTO = CODICE)
        
        file_to_load = summary_file_rows %>%
          filter(unique_file_name == 'CR.CSV') %>%
          filter(matched_folder == 'YES') %>%
          filter(rows > 0) %>%
          mutate(path = paste0('./Data/CSD/', main_folder, '/', file))
        if (max(file_to_load$cols) != min(file_to_load$cols)){cat('\n\n ###### error in', unique(file_to_load$unique_file_name), ': columns mismatch')}
        
        {
          report_CR = c()
          row_count = 0
          for (i in 1:nrow(file_to_load)){
            
            cat(i, '/', nrow(file_to_load), end = '\r')
            path = file_to_load$path[i]
            bank_abi = file_to_load$Abi[i]
            file = file_to_load$file[i]
            
            data = df_final_reference %>%
              select(abi, ndg) %>%
              filter(abi == bank_abi) %>%
              left_join(
                read.csv2(path, stringsAsFactors=FALSE) %>%
                  select(-COD_ABI) %>%
                  mutate(COD_NAG = gsub('\'', '', COD_NAG),
                         abi = bank_abi) %>%
                  rename(ndg = COD_NAG), by = c("abi", "ndg"))
            
            report = data %>%
              group_by(ndg, COD_DATO, COD_PRODOTTO) %>%
              summarize(Rows = n(), .groups = 'drop') %>%
              group_by(COD_DATO, COD_PRODOTTO) %>%
              summarize(Total_combination = n(), .groups = 'drop')
            
            report_CR = report_CR %>%
              bind_rows(report) %>%
              group_by(COD_DATO, COD_PRODOTTO) %>%
              summarize(Total_combination = sum(Total_combination), .groups = 'drop')
            
            row_count = row_count + nrow(data)
          } # i
          
          report_CR = report_CR %>%
            unique() %>%
            mutate(COD_PRODOTTO = gsub(' ', '', COD_PRODOTTO)) %>%
            mutate(COD_PRODOTTO = gsub('\'', '', COD_PRODOTTO))
          
          if (sum(report_CR$Total_combination) != row_count){
            cat('\n\n ###### error in report_CR')
          } else {
            saveRDS(report_CR, './Checkpoints/report_data/report_CR.rds')
          }
        }
        report_CR = readRDS('./Checkpoints/report_data/report_CR.rds')
        
        # check unique combination for missing
        report_CR_unique = report_CR %>%
          select(COD_DATO, COD_PRODOTTO) %>%
          unique()
        
        missing_COD_DATO = setdiff(unique(report_CR_unique$COD_DATO), unique(CR_cod_dato$COD_DATO))
        missing_COD_PRODOTTO = setdiff(unique(report_CR_unique$COD_PRODOTTO), unique(CR_cod_prodotto$COD_PRODOTTO))
        if (length(missing_COD_DATO) > 0){cat('\n\n ###### missing COD_DATO in CR:\n', paste0(missing_COD_DATO, collapse = '\n'))}
        if (length(missing_COD_PRODOTTO) > 0){cat('\n\n ###### missing COD_PRODOTTO in CR:\n', paste0(missing_COD_PRODOTTO, collapse = '\n'))}
        if (nrow(CR_cod_prodotto %>% select(COD_PRODOTTO, CATEGORIA_CENSIMENTO) %>% unique()) != nrow(CR_cod_prodotto)){
          cat('\n\n ###### duplicates in CR_cod_prodotto')}
        
        check_COD_PRODOTTO = report_CR %>%
          mutate(Perc_Occurence = round(Total_combination / sum(Total_combination) * 100, 1)) %>%
          left_join(CR_cod_dato, by = "COD_DATO") %>%
          mutate(COD_PRODOTTO = gsub('\'', '', COD_PRODOTTO)) %>%
          left_join(CR_cod_prodotto %>%
                      select(COD_PRODOTTO, CATEGORIA_CENSIMENTO) %>%
                      mutate(check_join = 'YES'), by = c("COD_PRODOTTO", "CATEGORIA_CENSIMENTO")) %>%
          mutate(missing_COD_PRODOTTO = ifelse(COD_PRODOTTO %in% missing_COD_PRODOTTO, 'Missing COD_PRODOTTO', 'Mismatch with CATEGORIA_CENSIMENTO')) %>%
          mutate(missing_COD_PRODOTTO = ifelse(!is.na(check_join), 'Matched', missing_COD_PRODOTTO)) %>%
          mutate(missing_COD_PRODOTTO = ifelse(COD_PRODOTTO == 'CR', 'Matched', missing_COD_PRODOTTO)) %>%
          # filter(is.na(check_join)) %>%
          # filter(COD_PRODOTTO != 'CR') %>%
          select(-check_join, -Total_combination) %>%
          arrange(desc(Perc_Occurence)) %>%
          mutate(Perc_Occurence = paste0(Perc_Occurence, ' %'))
        
        write.table(check_COD_PRODOTTO, './Stats/07_COD_PRODOTTO_CR_matched.csv', sep = ';', row.names = F, append = F)
        
        # check_unmatched_COD_PRODOTTO = CR_cod_prodotto %>%
        #   filter(COD_PRODOTTO %in% (check_COD_PRODOTTO %>%
        #            filter(missing_COD_PRODOTTO == '') %>%
        #            pull(COD_PRODOTTO) %>%
        #            unique()))
        
        
        
      }
      
      # check impact of removed ndg and forme_tecniche for AI_CRFI and AI_MURA
      {
        # AI_CRFI
        {
          compact_AI_CRFI = readRDS(paste0('./Checkpoints/compact_data/compact_AI_CRFI.rds')) %>%
            left_join(df_final_reference %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg"))
          summary_FT = compact_AI_CRFI %>%
            group_by(COD_FT_RAPPORTO) %>%
            summarize(TOT_ROW = n(), .groups = 'drop')
          chiave_comune_to_remove = compact_AI_CRFI %>%
            filter(COD_FT_RAPPORTO != "'99CF01") %>%
            pull(chiave_comune) %>%
            unique()
          chiave_comune_to_remove_check = compact_AI_CRFI %>%
            filter(chiave_comune %in% chiave_comune_to_remove) %>%
            group_by(chiave_comune) %>%
            summarize(FT_keep = ifelse("'99CF01" %in% COD_FT_RAPPORTO, 1, 0), .groups = 'drop') %>%
            group_by(FT_keep) %>%
            summarize(TOT_chiave_comune = n(), .groups = 'drop')
        }
        
        # AI_MURA - remove all clients with more than 2 rows, remove all rows and columns with OTHER (no IPO or CHIRO)
        {
          compact_AI_MURA = readRDS(paste0('./Checkpoints/compact_data/compact_AI_MURA.rds')) %>%
            left_join(df_final_reference %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg"))
          multiple_rows_check = compact_AI_MURA %>%
            group_by(chiave_comune) %>%
            summarise(ONE_ROW = sum(TOTAL_ROW == 1) > 0,
                      MORE_ROW = sum(TOTAL_ROW > 1) > 0, .groups = 'drop') %>%
            group_by(ONE_ROW, MORE_ROW) %>%
            summarise(COUNT = n(), .groups = 'drop')
          chiave_comune_to_remove_MURA = compact_AI_MURA %>%
            filter(TOTAL_ROW > 1) %>%
            filter(MACRO_KEEP == "KEEP") %>%
            select(chiave_comune, abi, ndg) %>%
            unique()
          saveRDS(chiave_comune_to_remove_MURA, './Checkpoints/compact_data/chiave_comune_to_remove_MURA.rds')
        }
      }
      
      # create a single DECO.CSV file, looping all files
      {
        summary_file_rows = readRDS('./Checkpoints/summary_file_rows.rds')
        file_to_load = summary_file_rows %>%
          filter(unique_file_name == 'DECO.CSV') %>%
          filter(matched_folder == 'YES') %>%
          filter(rows > 0) %>%
          mutate(path = paste0('./Data/CSD/', main_folder, '/', file))
        
        df_AI_DECO = c()
        for (i in 1:nrow(file_to_load)){
          df_AI_DECO = df_AI_DECO %>% bind_rows(
            read.csv2(file_to_load$path[i], stringsAsFactors=FALSE) %>%
              select(-COD_ABI) %>%
              mutate(abi = file_to_load$Abi[i],
                     COD_ELEMENTO = gsub(' ', '', COD_ELEMENTO),
                     DESCRIZIONE = ifelse(DESCRIZIONE == "------------------------------                                                  ", '', DESCRIZIONE),
                     DESCRIZIONE = ifelse(DESCRIZIONE == "'------------------------------                                                  ", '', DESCRIZIONE)) %>%
              
              unique())
        }
        saveRDS(df_AI_DECO, './Coding_tables/df_AI_DECO.rds')
      }
    }
  }
  
  # prepare dataset to merge
  {
    chiave_comune_to_remove_MURA = readRDS('./Checkpoints/compact_data/chiave_comune_to_remove_MURA.rds')
    # df_AI_DECO = readRDS('./Coding_tables/df_AI_DECO.rds')
    
    if (reload_prepare == FALSE){
      # AI_CRFI - keep only COD_FT_RAPPORTO == '99CF01' and add dummy for multiple Forme Tecniche
      cat('\nReloading AI_CRFI...')
      compact_AI_CRFI = readRDS('./Checkpoints/compact_data/compact_AI_CRFI.rds')
      multiple_FT = compact_AI_CRFI %>%
        filter(COD_FT_RAPPORTO != "'99CF01") %>%
        select(abi, ndg, year, month) %>%
        unique() %>%
        mutate(DUMMY_CRFI_MULTIPLE_FT = 1)
      df_AI_CRFI = compact_AI_CRFI %>%
        filter(COD_FT_RAPPORTO == "'99CF01") %>%
        left_join(multiple_FT, by = c("abi", "ndg", "year", "month")) %>%
        mutate(DUMMY_CRFI_MULTIPLE_FT = ifelse(is.na(DUMMY_CRFI_MULTIPLE_FT), 0, 1)) %>%
        select(-COD_FT_RAPPORTO)
      rm(compact_AI_CRFI, multiple_FT)
      if (df_AI_CRFI %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_AI_CRFI)){
        cat('\n\n ###### error in unique records in df_AI_CRFI')
      }
      if (sum(is.na(df_AI_CRFI)) > 0){cat('\n\n ###### NA in df_AI_CRFI')}
      cat('Done')
      
      # AI_MURA - remove all clients with more than 2 rows, remove all rows and columns with OTHER (no IPO or CHIRO)
      cat('\nReloading AI_MURA...')
      compact_AI_MURA = readRDS('./Checkpoints/compact_data/compact_AI_MURA.rds')
      df_AI_MURA = compact_AI_MURA %>%
        filter(TOTAL_ROW == 1) %>%
        filter(MACRO_KEEP == "KEEP") %>%
        select(-ends_with('_OTHER'), -MACRO_KEEP, -TOTAL_ROW)
      rm(compact_AI_MURA)
      if (df_AI_MURA %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_AI_MURA)){
        cat('\n\n ###### error in unique records in df_AI_MURA')
      }
      if (sum(is.na(df_AI_MURA)) > 0){cat('\n\n ###### NA in df_AI_MURA')}
      cat('Done')
      
      # AI_POFI - just create a check for dummy
      cat('\nReloading AI_POFI...')
      df_AI_POFI = readRDS('./Checkpoints/compact_data/compact_AI_POFI.rds')
      dummy_POFI_check = df_AI_POFI %>%
        mutate(abi_ndg = paste0(abi, ndg)) %>%
        group_by(DUMMY_POFI_AGR, DUMMY_POFI_FIN, DUMMY_POFI_ART) %>%
        summarise(ABI_NDG = uniqueN(abi_ndg), .groups = 'drop')
      write.table(dummy_POFI_check, './Checks/02_POFI_DUMMY.csv', sep = ';', row.names = F, append = F)
      rm(dummy_POFI_check)
      if (df_AI_POFI %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_AI_POFI)){
        cat('\n\n ###### error in unique records in df_AI_POFI')
      }
      if (sum(is.na(df_AI_POFI)) > 0){cat('\n\n ###### NA in df_AI_POFI')}
      cat('Done')
      
      # AI_CC
      cat('\nReloading AI_CC...')
      df_AI_CC = readRDS('./Checkpoints/compact_data/compact_AI_CC.rds')
      if (df_AI_CC %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_AI_CC)){
        cat('\n\n ###### error in unique records in df_AI_CC')
      }
      if (sum(is.na(df_AI_CC)) > 0){cat('\n\n ###### NA in df_AI_CC')}
      cat('Done')
      
      # AI_PCSBF
      cat('\nReloading AI_PCSBF...')
      df_AI_PCSBF = readRDS('./Checkpoints/compact_data/compact_AI_PCSBF.rds')
      if (df_AI_PCSBF %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_AI_PCSBF)){
        cat('\n\n ###### error in unique records in df_AI_PCSBF')
      }
      if (sum(is.na(df_AI_PCSBF)) > 0){cat('\n\n ###### NA in df_AI_PCSBF')}
      cat('Done')
      
      # AI_ANFA
      cat('\nReloading AI_ANFA...')
      df_AI_ANFA = readRDS('./Checkpoints/compact_data/compact_AI_ANFA.rds')
      if (df_AI_ANFA %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_AI_ANFA)){
        cat('\n\n ###### error in unique records in df_AI_ANFA')
      }
      if (sum(is.na(df_AI_ANFA)) > 0){cat('\n\n ###### NA in df_AI_ANFA')}
      cat('Done')
      
      # AI_APSE
      cat('\nReloading AI_APSE...')
      df_AI_APSE = readRDS('./Checkpoints/compact_data/compact_AI_APSE.rds')
      if (df_AI_APSE %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_AI_APSE)){
        cat('\n\n ###### error in unique records in df_AI_APSE')
      }
      if (sum(is.na(df_AI_APSE)) > 0){cat('\n\n ###### NA in df_AI_APSE')}
      cat('Done')
      
      # RISCHIO
      cat('\nReloading RISCHIO...')
      df_RISCHIO = readRDS('./Checkpoints/compact_data/compact_RISCHIO.rds')
      if (df_RISCHIO %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_RISCHIO)){
        cat('\n\n ###### error in unique records in df_RISCHIO')
      }
      if (sum(is.na(df_RISCHIO)) > 0){cat('\n\n ###### NA in df_RISCHIO')}
      cat('Done')
      
      # CR
      cat('\nReloading CR...')
      df_CR = readRDS('./Checkpoints/compact_data/compact_CR.rds')
      if (df_CR %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(df_CR)){
        cat('\n\n ###### error in unique records in df_CR')
      }
      # if (sum(is.na(df_CR)) > 0){cat('\n\n ###### NA in df_CR')}
      cat('Done')
      
      # evaluate perimeter for abi, ndg, year (to be used to assess bilanci and anagrafica missing)
      # df_reference_available_years = c()
      # for (df_t in c('AI_CC', 'AI_PCSBF', 'AI_ANFA', 'AI_POFI', 'AI_MURA', 'AI_APSE', 'AI_CRFI', 'RISCHIO', 'CR')){
      #   eval(parse(text = paste0('df = df_', df_t)))
      #   df_reference_available_years = df_reference_available_years %>%
      #     bind_rows(df %>%
      #                 select(abi, ndg, year) %>%
      #                 unique())
      #   rm(df)
      # }
      # df_reference_available_years = df_reference_available_years %>% unique()
      # saveRDS(df_reference_available_years, './Checkpoints/df_reference_available_years.rds')
      df_reference_available_years = readRDS('./Checkpoints/df_reference_available_years.rds')
      
      # anagrafica CRIF
      {
        cat('\nReloading ANAGRAFICA CRIF...')
        coding_COD_TIPO_CLIENTE = data.frame(ANAG_cod_tipo_cliente = as.character(c(0:4)),
                                             ANAG_tipo_cliente = c('Indiretto', 'Diretto', 'Potenziale', 'Ex cliente', 'Collegato'), stringsAsFactors = F)
        coding_SOCIO = data.frame(ANAG_socio_NUM = c('S', 'N', 'G', 'X'),
                                  ANAG_socio = c('Socio', 'Non Socio', 'Non socio garantito da socio', ''), stringsAsFactors = F)
        df_CRIF_anag = readRDS('./Checkpoints/compact_data/df_anag.rds') %>%
          mutate(ANAG_comune = ifelse(is.na(ANAG_comune), '', ANAG_comune)) %>%
          rename(ANAG_socio_NUM = ANAG_socio) %>%
          left_join(coding_COD_TIPO_CLIENTE, by = 'ANAG_cod_tipo_cliente') %>%
          left_join(coding_SOCIO, by = 'ANAG_socio_NUM') %>%
          select(-data_rif, -ANAG_data_cliente, -ANAG_flag_def_calib, -ANAG_flag_def_calib_notec, -chiave_comune, -ANAG_cod_tipo_cliente,
                 -ANAG_socio_NUM, -starts_with('segmento_'), -ANAG_segmento, -ANAG_tipo_ndg, -ANAG_cod_tipo_ndg, -FLAG_Multibank) %>%
          rename(ANAG_tipo_ndg = ANAG_tipo_ndg_RECOVER) %>%
          select(abi, ndg, year, everything())
        if (df_CRIF_anag %>% select(abi, ndg, year) %>% uniqueN() != nrow(df_CRIF_anag)){
          cat('\n\n ###### error in unique records in df_CRIF_anag')
        } else {
          write.table(basicStatistics(df_CRIF_anag), './Stats/08_df_CRIF_anag_preJoin_stats.csv', sep = ';', row.names = F, append = F)
        }
        
        # list_abi_ndg_year_CRIF_anag = df_CRIF_anag %>%
        #   left_join(df_final_reference %>% select(abi, ndg, segmento_CSD, segmento_CRIF), by = c("abi", "ndg"))
        # list_abi_ndg_year_CRIF_anag = list_abi_ndg_year_CRIF_anag %>%
        #   select(segmento_CSD, abi, ndg, year) %>%
        #   rename(Segmento = segmento_CSD) %>%
        #   group_by(Segmento, abi, ndg) %>%
        #   summarise(YEAR = uniqueN(year),
        #             MISSING = paste0(setdiff(c(2012:2014), year), collapse = '|'), .groups = 'drop') %>%
        #   mutate(Fonte = 'CSD') %>%
        #   bind_rows(list_abi_ndg_year_CRIF_anag %>%
        #               select(segmento_CRIF, abi, ndg, year) %>%
        #               rename(Segmento = segmento_CRIF) %>%
        #               group_by(Segmento, abi, ndg) %>%
        #               summarise(YEAR = uniqueN(year),
        #                         MISSING = paste0(setdiff(c(2012:2014), year), collapse = '|'), .groups = 'drop') %>%
        #               mutate(Fonte = 'CRIF'))
        # saveRDS(list_abi_ndg_year_CRIF_anag, './Checkpoints/list_abi_ndg_year_CRIF_anag.rds')
        list_abi_ndg_year_CRIF_anag = readRDS('./Checkpoints/list_abi_ndg_year_CRIF_anag.rds')
        summary_list_abi_ndg_year_CRIF_anag = c()
        for (fonte in c('CSD', 'CRIF')){
          summary_list_abi_ndg_year_CRIF_anag = summary_list_abi_ndg_year_CRIF_anag %>% bind_rows(
            list_abi_ndg_year_CRIF_anag %>%
              filter(Fonte == fonte) %>%
              group_by(Fonte, Segmento, YEAR) %>%
              summarise(Tot_ABI_NDG = n(),
                        Missing_2012 = sum(MISSING == '2012'),
                        Missing_2013 = sum(MISSING == '2013'),
                        Missing_2014 = sum(MISSING == '2014'),
                        Missing_2012_13 = sum(MISSING == '2012|2013'),
                        Missing_2012_14 = sum(MISSING == '2012|2014'),
                        Missing_2013_14 = sum(MISSING == '2013|2014'), .groups = 'drop') %>%
              group_by(Fonte, Segmento) %>%
              mutate(Segmento_Missing_Perc = sum(Tot_ABI_NDG[YEAR != 3])) %>%
              ungroup() %>%
              mutate('Tot_ABI_NDG_Perc' = paste0(round(Tot_ABI_NDG / sum(Tot_ABI_NDG) * 100), '%'),
                     'Segmento_Missing_Perc' = paste0(round(Segmento_Missing_Perc / sum(Tot_ABI_NDG) * 100), '%')) %>%
              rename(Available_years = YEAR) %>%
              mutate_if(is.numeric, ~format(., big.mark=",")) %>%
              mutate(N = 1:n(),
                     Segmento = ifelse(N %% 3 != 1, '', Segmento),
                     Segmento_Missing_Perc = ifelse(N %% 3 != 1, '', Segmento_Missing_Perc))
          ) %>%
            select(Fonte, Segmento, Segmento_Missing_Perc, Available_years, Tot_ABI_NDG, Tot_ABI_NDG_Perc, everything(), -N)
        }
        write.table(summary_list_abi_ndg_year_CRIF_anag, './Checks/03_CRIF_anagrafica_YEAR.csv', sep = ';', row.names = F, append = F)
        
        list_abi_ndg_year_CRIF_anag = list_abi_ndg_year_CRIF_anag %>%
          select(-Segmento, -Fonte) %>%
          unique()
        
        # replicate available data for all years
        df_CRIF_anag_1year = list_abi_ndg_year_CRIF_anag %>%
          filter(YEAR == 1) %>%
          select(-YEAR, -MISSING) %>%
          crossing(data.frame(year = c(2012, 2013, 2014))) %>%
          left_join(df_CRIF_anag %>% select(-year), by = c("abi", "ndg"))
        
        df_CRIF_anag_2year_2012_abi_ndg = list_abi_ndg_year_CRIF_anag %>%
          filter(YEAR == 2) %>%
          filter(MISSING == '2012') %>%
          select(abi, ndg)
        df_CRIF_anag_2year_2012 = df_CRIF_anag_2year_2012_abi_ndg %>%
          crossing(data.frame(year = c(2012, 2013))) %>%
          left_join(df_CRIF_anag %>% filter(year == 2013) %>% select(-year), by = c("abi", "ndg")) %>%
          bind_rows(df_CRIF_anag_2year_2012_abi_ndg %>%
                      mutate(year = 2014) %>%
                      left_join(df_CRIF_anag, by = c("abi", "ndg", 'year')))
        if (nrow(df_CRIF_anag_2year_2012_abi_ndg) * 3 != nrow(df_CRIF_anag_2year_2012)){cat('\n\n ###### error in df_CRIF_anag_2year_2012')}
        
        df_CRIF_anag_2year_2013_abi_ndg = list_abi_ndg_year_CRIF_anag %>%
          filter(YEAR == 2) %>%
          filter(MISSING == '2013') %>%
          select(abi, ndg)
        df_CRIF_anag_2year_2013 = df_CRIF_anag_2year_2013_abi_ndg %>%
          crossing(data.frame(year = c(2013, 2014))) %>%
          left_join(df_CRIF_anag %>% filter(year == 2014) %>% select(-year), by = c("abi", "ndg")) %>%
          bind_rows(df_CRIF_anag_2year_2013_abi_ndg %>%
                      mutate(year = 2012) %>%
                      left_join(df_CRIF_anag, by = c("abi", "ndg", 'year')))
        if (nrow(df_CRIF_anag_2year_2013_abi_ndg) * 3 != nrow(df_CRIF_anag_2year_2013)){cat('\n\n ###### error in df_CRIF_anag_2year_2013')}
        
        df_CRIF_anag_2year_2014_abi_ndg = list_abi_ndg_year_CRIF_anag %>%
          filter(YEAR == 2) %>%
          filter(MISSING == '2014') %>%
          select(abi, ndg)
        df_CRIF_anag_2year_2014 = df_CRIF_anag_2year_2014_abi_ndg %>%
          crossing(data.frame(year = c(2013, 2014))) %>%
          left_join(df_CRIF_anag %>% filter(year == 2013) %>% select(-year), by = c("abi", "ndg")) %>%
          bind_rows(df_CRIF_anag_2year_2014_abi_ndg %>%
                      mutate(year = 2012) %>%
                      left_join(df_CRIF_anag, by = c("abi", "ndg", 'year')))
        if (nrow(df_CRIF_anag_2year_2014_abi_ndg) * 3 != nrow(df_CRIF_anag_2year_2014)){cat('\n\n ###### error in df_CRIF_anag_2year_2014')}
        
        
        df_CRIF_anag = df_CRIF_anag_1year %>%
          bind_rows(df_CRIF_anag_2year_2012, df_CRIF_anag_2year_2013, df_CRIF_anag_2year_2014) %>%
          bind_rows(list_abi_ndg_year_CRIF_anag %>%
                      filter(YEAR == 3) %>%
                      select(abi, ndg) %>%
                      left_join(df_CRIF_anag, by = c("abi", "ndg")))
        if (nrow(list_abi_ndg_year_CRIF_anag) * 3 != nrow(df_CRIF_anag)){
          cat('\n\n ###### error in df_CRIF_anag with expanded years')
        }
        rm(coding_COD_TIPO_CLIENTE, coding_SOCIO, summary_list_abi_ndg_year_CRIF_anag, list_abi_ndg_year_CRIF_anag,
           df_CRIF_anag_1year, df_CRIF_anag_2year_2012, df_CRIF_anag_2year_2013, df_CRIF_anag_2year_2014,
           df_CRIF_anag_2year_2012_abi_ndg, df_CRIF_anag_2year_2013_abi_ndg, df_CRIF_anag_2year_2014_abi_ndg)
        cat('Done')
      }
      
      # bilanci CRIF
      {
        cat('\nReloading BILANCI CRIF...')
        # check common column with anagrafica
        df_anag = readRDS('./Checkpoints/compact_data/df_anag.rds')
        df_bila = readRDS('./Checkpoints/compact_data/df_bilanci.rds')
        common_column = c('sae', 'rae', 'ateco', 'provincia', 'socio', 'tipo_ndg')
        common_abi_ndg = df_bila %>% select(abi, ndg, year) %>%
          left_join(df_anag %>% select(abi, ndg, year) %>% mutate(JOIN = 'YES'), by = c("abi", "ndg", "year")) %>%
          filter(!is.na(JOIN)) %>%
          select(-JOIN)
        cat('\nDifference between Anagrafica and Bilanci common columns:')
        for (col in common_column){
          df_t = common_abi_ndg %>%
            left_join(df_anag %>% select(abi, ndg, year, paste0('ANAG_', col)), by = c("abi", "ndg", "year")) %>%
            left_join(df_bila %>% select(abi, ndg, year, paste0('BILA_', col)), by = c("abi", "ndg", "year")) %>%
            setNames(c('abi', 'ndg', 'year', 'ANAG', 'BILA')) %>%
            mutate(DIFF = ANAG != BILA) %>%
            filter(DIFF == TRUE)
          cat('\n', col, ':', nrow(df_t))
        }
        cat('\n')
        rm(df_anag, df_bila, df_t, common_abi_ndg)
        
        coding_strutbil = data.frame(BILA_strutbil_NUM = c('00',	'01',	'02',	'03',	'04',	'05',	'06',	'07',	'10',	'20',	'50',	'54'),
                                     BILA_strutbil = c('BILANCIO CON CONTROLLI', 'BILANCIO RILEVAZIONE LIBERA',	'TRIMESTRALE SOCIETARIA',
                                                       'SEMESTRALE SOCIETARIA',	'BILANCIO CONSOLIDATO',	'BILANCIO CONSOLIDATO RILEVAZIONE LIBERA',
                                                       'CONSOLIDATO TRIMESTRALE',	'CONSOLIDATO SEMESTRALE',	'BILANCIO CONSOLIDATO STIMATO',
                                                       'MODELLI FISCALI',	'BIL. SOCIETARIO ESTERO', 'BIL. CONSOLIDATO ESTERO'), stringsAsFactors = F)
        coding_natura = data.frame(BILA_natura_NUM = c('0',	'1', '2', '3', '4', '5', '6', '7'),
                                   BILA_natura = c('D\'ESERCIZIO', 'BOZZA DI BILANCIO',	'FUSIONE/INCORPORAZIONE',	'SCORPORO/SCISSIONE/TRASFORMAZIONE',
                                                   'BILANCIO ESERCIZIO SOC.IN LIQUIDAZIONE', 'BILANCIO FINALE SOC.IN LIQUIDAZIONE',
                                                   'BILANCIO DI INIZIO SERIE STORICA', 'BILANCIO DI FONTE CERVED'), stringsAsFactors = F)
        coding_schemaril = data.frame(BILA_schemaril_NUM = c('01', '02',	'03',	'04',	'05',	'06',	'07',	'08',	'09'),
                                      BILA_schemaril = c('INDUSTRIALE',	'COMMERCIALE', 'PRODUZIONE PLURIENNALE', 'SERVIZI',	'IMMOBILIARE',
                                                         'FINANZIARIA',	'FACTORING', 'HOLDING',	'LEASING'), stringsAsFactors = F)
        
        df_CRIF_bila = readRDS('./Checkpoints/compact_data/df_bilanci.rds') %>%
          select(-chiave_comune, -starts_with('segmento_'), -data_rif, -FLAG_Multibank, -BILA_join_ref, -BILA_segmento,
                 -BILA_flag_presenza_bilancio, -BILA_flag_presenza_bilancio_ok, -BILA_flag_presenza_bilancio_dataqual, -BILA_flag_esclusione_primo_bil,
                 -all_of(paste0('BILA_', common_column))) %>%
          mutate(BILA_flag_esclusione_secondo_bil = ifelse(is.na(BILA_flag_esclusione_secondo_bil), '', BILA_flag_esclusione_secondo_bil),
                 BILA_flag_esclusione_terzo_bil = ifelse(is.na(BILA_flag_esclusione_terzo_bil), '', BILA_flag_esclusione_terzo_bil)) %>%
          rename(BILA_strutbil_NUM = BILA_strutbil,
                 BILA_natura_NUM = BILA_natura,
                 BILA_schemaril_NUM = BILA_schemaril) %>%
          left_join(coding_strutbil, by = "BILA_strutbil_NUM") %>%
          left_join(coding_natura, by = "BILA_natura_NUM") %>%
          left_join(coding_schemaril, by = "BILA_schemaril_NUM") %>%
          mutate(BILA_strutbil = ifelse(is.na(BILA_strutbil), '', BILA_strutbil),
                 BILA_natura = ifelse(is.na(BILA_natura), '', BILA_natura),
                 BILA_schemaril = ifelse(is.na(BILA_schemaril), '', BILA_schemaril)) %>%
          select(-BILA_strutbil_NUM, -BILA_natura_NUM, -BILA_schemaril_NUM)
        rm(coding_strutbil, coding_natura, coding_schemaril, common_column)
        
        # check availability of bilanci
        # list_abi_ndg_year_CRIF_bila = df_CRIF_bila %>%
        #   select(abi, ndg, year) %>%
        #   left_join(df_final_reference %>% select(abi, ndg, segmento_CSD, segmento_CRIF), by = c("abi", "ndg")) %>%
        #   left_join(df_reference_available_years %>%
        #               group_by(abi, ndg) %>%
        #               summarize(Expected_available_years = n(),
        #                         Expected_available_years_val = paste0(sort(year), collapse = '|'), .groups = 'drop'), by = c("abi", "ndg")) %>%
        #   filter(!is.na(Expected_available_years))  # abi+ndg in df_final_reference and df_CRIF_bila but with no other values in al df_AI_*, etc (~100)
        # list_abi_ndg_year_CRIF_bila = list_abi_ndg_year_CRIF_bila %>%
        #   rename(Segmento = segmento_CSD) %>%
        #   group_by(Segmento, abi, ndg) %>%
        #   summarise(Bilanci_available_years = uniqueN(year),
        #             Expected_available_years = unique(Expected_available_years),
        #             MISSING = paste0(setdiff(strsplit(Expected_available_years_val, '\\|')[[1]], year), collapse = '|'),.groups = 'drop') %>%
        #   mutate(Fonte = 'CSD') %>%
        #   bind_rows(list_abi_ndg_year_CRIF_bila %>%
        #               rename(Segmento = segmento_CRIF) %>%
        #               group_by(Segmento, abi, ndg) %>%
        #               summarise(Bilanci_available_years = uniqueN(year),
        #                         Expected_available_years = unique(Expected_available_years),
        #                         MISSING = paste0(setdiff(strsplit(Expected_available_years_val, '\\|')[[1]], year), collapse = '|'),.groups = 'drop') %>%
        #               mutate(Fonte = 'CRIF')) %>%
        #   mutate(match = Expected_available_years == Bilanci_available_years) %>%
        #   mutate(Bilanci_available_years = ifelse(match, 'Matched', Bilanci_available_years),
        #          Expected_available_years = ifelse(match, 'Matched', Expected_available_years))
        # saveRDS(list_abi_ndg_year_CRIF_bila, './Checkpoints/list_abi_ndg_year_CRIF_bila.rds')
        list_abi_ndg_year_CRIF_bila = readRDS('./Checkpoints/list_abi_ndg_year_CRIF_bila.rds')
        summary_list_abi_ndg_year_CRIF_bila = c()
        for (fonte in c('CRIF', 'CSD')){
          summary_list_abi_ndg_year_CRIF_bila = summary_list_abi_ndg_year_CRIF_bila %>% bind_rows(
            list_abi_ndg_year_CRIF_bila %>%
              filter(Fonte == fonte) %>%
              group_by(Fonte, Segmento, Expected_available_years, Bilanci_available_years) %>%
              mutate(Expected_available_years = gsub('Matched', '0Matched', Expected_available_years)) %>%
              summarise(Tot_ABI_NDG = n(),
                        Missing_2012 = sum(MISSING == '2012'),
                        Missing_2013 = sum(MISSING == '2013'),
                        Missing_2014 = sum(MISSING == '2014'),
                        Missing_2012_13 = sum(MISSING == '2012|2013'),
                        Missing_2012_14 = sum(MISSING == '2012|2014'),
                        Missing_2013_14 = sum(MISSING == '2013|2014'), .groups = 'drop') %>%
              mutate(Expected_available_years = gsub('0Matched', 'Matched', Expected_available_years)) %>%
              group_by(Fonte, Segmento) %>%
              mutate(Segmento_Missing_Perc = sum(Tot_ABI_NDG[Expected_available_years != Bilanci_available_years])) %>%
              ungroup() %>%
              mutate('Tot_ABI_NDG_Perc' = paste0(round(Tot_ABI_NDG / sum(Tot_ABI_NDG) * 100), '%'),
                     'Segmento_Missing_Perc' = paste0(round(Segmento_Missing_Perc / sum(Tot_ABI_NDG) * 100), '%')) %>%
              mutate_if(is.numeric, ~format(., big.mark=",")) %>%
              group_by(Fonte, Segmento) %>%
              mutate(N = 1:n()) %>%
              mutate(Segmento = ifelse(N != 1, '', Segmento),
                     Segmento_Missing_Perc = ifelse(N != 1, '', Segmento_Missing_Perc))
          ) %>%
            select(Fonte, Segmento, Segmento_Missing_Perc, Expected_available_years, Bilanci_available_years, Tot_ABI_NDG, Tot_ABI_NDG_Perc, everything(), -N)
        }
        write.table(summary_list_abi_ndg_year_CRIF_bila, './Checks/03_CRIF_bilanci_YEAR.csv', sep = ';', row.names = F, append = F)
        rm(list_abi_ndg_year_CRIF_bila, summary_list_abi_ndg_year_CRIF_bila)
        if (df_CRIF_bila %>% select(abi, ndg, year) %>% uniqueN() != nrow(df_CRIF_bila)){
          cat('\n\n ###### error in unique records in df_CRIF_bila')
        } else {
          write.table(basicStatistics(df_CRIF_bila), './Stats/08_df_CRIF_bila_preJoin_stats.csv', sep = ';', row.names = F, append = F)
        }
        cat('Done')
      }
      
      # Tot_Attivo, Tot_Equity and Tot_Valore_Produzione from additional bilanci CRIF
      {
        # abi+ndg+year with both variable available only
        # Value available for 1 year only  are repeated for all year
        # value available for >=2 year are both averaged or linearly extrapolated
        # 2 version of "filled" variables are provided -> interpolation creates negative values, discarded
        
        cat('\nTot_Attivo and Tot_Valore_Produzione from BILANCI CRIF...')
        
        # take varT012 and varT051 from cebi_indicatori_2008_2014.dta and keep only abi+ndg with both available.
        df_bilanci_CEBI = read_dta("./Distance_to_Default/Data/cebi_indicatori_2008_2014.dta") %>%
          as.data.frame(stringsAsFactors = F) %>%
          mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
          mutate(abi = as.character(as.numeric(cod_abi)),
                 cod_nag = str_pad(cod_nag, 16, pad = "0"),
                 anno = as.numeric(anno)) %>%
          rename(ndg = cod_nag,
                 year = anno,
                 Tot_Valore_Produzione = varT051,
                 Tot_Attivo = varT012,
                 Tot_Equity = varT021) %>%
          mutate(Tot_Attivo = Tot_Attivo * 1000,
                 Tot_Equity = Tot_Equity * 1000,
                 Tot_Valore_Produzione = Tot_Valore_Produzione * 1000) %>%
          select(abi, ndg, year, Tot_Attivo, Tot_Equity, Tot_Valore_Produzione, tipo_bil, data_chiusura) %>%
          group_by(abi, ndg, year) %>%
          mutate(dup = n()) %>%
          ungroup()
        
        tot_abi_ndg = df_bilanci_CEBI %>% select(abi, ndg) %>% uniqueN()
        # for multiple tipo_bil, take '00', for single tipo_bil take last available data_chiusura
        duplicated_year = df_bilanci_CEBI %>%
          filter(dup > 1) %>%
          group_by(abi, ndg, year) %>%
          mutate(dup = uniqueN(tipo_bil))
        solved_duplicates = duplicated_year %>%
          filter(dup == 2) %>%
          filter(tipo_bil == '00') %>%
          ungroup() %>% bind_rows(
            duplicated_year %>%
              filter(dup == 1) %>%
              group_by(abi, ndg, year) %>%
              arrange(desc(data_chiusura)) %>%
              filter(row_number() == 1) %>%
              ungroup()
          )
        
        df_bilanci_CEBI = df_bilanci_CEBI %>%
          filter(dup == 1) %>%
          bind_rows(
            solved_duplicates
          ) %>%
          select(-tipo_bil, -data_chiusura, -dup)
        
        if (df_bilanci_CEBI %>% select(abi, ndg) %>% uniqueN() != tot_abi_ndg){cat('\n\n ###### something wrong in duplicates of df_bilanci_CEBI')}
        df_bilanci_CEBI = df_bilanci_CEBI %>%
          group_by(abi, ndg) %>%
          mutate(avail_Tot_Attivo = sum(!is.na(Tot_Attivo)),
                 avail_Tot_Equity = sum(!is.na(Tot_Equity)),
                 avail_Tot_Valore_Produzione = sum(!is.na(Tot_Valore_Produzione))) %>%
          filter(avail_Tot_Attivo != 0) %>%
          filter(avail_Tot_Equity != 0) %>%
          filter(avail_Tot_Valore_Produzione != 0) %>%
          select(-avail_Tot_Attivo, -avail_Tot_Equity, -avail_Tot_Valore_Produzione)
        if (df_bilanci_CEBI %>% select(abi, ndg, year) %>% uniqueN() != nrow(df_bilanci_CEBI)){cat('\n\n ###### duplicates in df_bilanci_CEBI')}
        
        # take values for NON-CEBI from bildati_noncebi.dta
        df_bilanci_NONCEBI_ref = read_dta("./Data/CRIF/bildati_noncebi.dta") %>%
          as.data.frame(stringsAsFactors = F) %>%
          mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
          mutate(abi = as.character(as.numeric(cod_abi)),
                 cod_nag = str_pad(cod_nag, 16, pad = "0"),
                 anno = as.numeric(anno)) %>%
          rename(ndg = cod_nag,
                 year = anno) %>%
          filter(cod_de %in% c('CIBA', 'TANA', 'TSEM', 'TSEA', 'TSES')) %>%
          select(abi, ndg, year, attributo, cod_de, k007, k283, d205, d044, s200, s012, d206, d019, k293, d053, s013, d047) %>%
          mutate(tot_na = apply(., 1, function(x) sum(is.na(x)))) %>%
          filter(tot_na < 12) %>%   # should be the same number of variable above k007, k283, etc.
          group_by(abi, ndg, year) %>%
          mutate(dup = n()) %>%
          ungroup()
        if (df_bilanci_NONCEBI_ref %>% select(abi, ndg, year) %>% uniqueN() != nrow(df_bilanci_NONCEBI_ref)){
          cat('\n\n ###### duplicates in df_bilanci_NONCEBI_ref')}
        
        variable_mapping = read.csv('./Coding_tables/bilanci_NonCEBI.csv', sep=';', stringsAsFactors = F)
        df_bilanci_NONCEBI = c()
        Tot_Attivo_check = Tot_Equity_check = Tot_Prod_check = 0
        for (ind in 1:nrow(variable_mapping)){
          var_attivo = variable_mapping$Tot_Attivo[ind]
          var_equity = variable_mapping$Tot_Equity[ind]
          var_prod = variable_mapping$Tot_Valore_Produzione[ind]
          df_bilanci_NONCEBI = df_bilanci_NONCEBI %>% bind_rows(
            df_bilanci_NONCEBI_ref %>%
              filter(cod_de == variable_mapping$cod_de[ind]) %>%
              group_by(abi, ndg, year, cod_de) %>%
              summarize(Tot_Attivo = ifelse(length((!!sym(var_attivo))[!is.na(!!sym(var_attivo))]) > 0, (!!sym(var_attivo))[!is.na(!!sym(var_attivo))], NA),
                        Tot_Equity = ifelse(length((!!sym(var_equity))[!is.na(!!sym(var_equity))]) > 0, (!!sym(var_equity))[!is.na(!!sym(var_equity))], NA),
                        Tot_Valore_Produzione = ifelse(length((!!sym(var_prod))[!is.na(!!sym(var_prod))]) > 0, (!!sym(var_prod))[!is.na(!!sym(var_prod))], NA), .groups = 'drop')
          )
          Tot_Attivo_check = Tot_Attivo_check + df_bilanci_NONCEBI_ref %>% filter(cod_de == variable_mapping$cod_de[ind]) %>% select(all_of(var_attivo)) %>% sum(na.rm = T)
          Tot_Equity_check = Tot_Equity_check + df_bilanci_NONCEBI_ref %>% filter(cod_de == variable_mapping$cod_de[ind]) %>% select(all_of(var_equity)) %>% sum(na.rm = T)
          Tot_Prod_check = Tot_Prod_check + df_bilanci_NONCEBI_ref %>% filter(cod_de == variable_mapping$cod_de[ind]) %>% select(all_of(var_prod)) %>% sum(na.rm = T)
        }
        if (df_bilanci_NONCEBI %>% select(abi, ndg, year) %>% uniqueN() != df_bilanci_NONCEBI_ref %>% select(abi, ndg, year) %>% uniqueN()){
          cat('\n\n ###### mismatch in df_bilanci_NONCEBI')}
        if (Tot_Attivo_check != sum(df_bilanci_NONCEBI$Tot_Attivo, na.rm=T)){cat('\n\n ###### Tot_Attivo mismatch in df_bilanci_NONCEBI')}
        if (Tot_Equity_check != sum(df_bilanci_NONCEBI$Tot_Equity, na.rm=T)){cat('\n\n ###### Tot_Equity mismatch in df_bilanci_NONCEBI')}
        if (Tot_Prod_check != sum(df_bilanci_NONCEBI$Tot_Valore_Produzione, na.rm=T)){cat('\n\n ###### Tot_Valore_Produzione mismatch in df_bilanci_NONCEBI')}
        df_bilanci_NONCEBI = df_bilanci_NONCEBI %>%
          group_by(abi, ndg) %>%
          mutate(avail_Tot_Attivo = sum(!is.na(Tot_Attivo)),
                 avail_Tot_Equity = sum(!is.na(Tot_Equity)),
                 avail_Tot_Valore_Produzione = sum(!is.na(Tot_Valore_Produzione))) %>%
          filter(avail_Tot_Attivo != 0) %>%
          filter(avail_Tot_Equity != 0) %>%
          filter(avail_Tot_Valore_Produzione != 0) %>%
          select(-avail_Tot_Attivo, -avail_Tot_Equity, -avail_Tot_Valore_Produzione, -cod_de)
        
        # merge CEBI and NONCEBI
        df_bilanci_merge = df_bilanci_CEBI %>%
          bind_rows(df_bilanci_NONCEBI) %>%
          filter(year <= 2014)
        
        # keep 1,000 <= Tot_Attivo, Tot_Valore_Produzione <= 500,000,000
        abi_ndg_to_remove = df_bilanci_merge %>%
          filter(Tot_Attivo < 1000 | Tot_Attivo > 5e8 | Tot_Valore_Produzione < 1000 | Tot_Valore_Produzione > 5e8) %>%
          select(abi, ndg) %>%
          unique() %>%
          mutate(mm = paste0(abi, ndg)) %>%
          pull(mm)
        df_bilanci_merge = df_bilanci_merge %>%
          mutate(mm = paste0(abi, ndg)) %>%
          left_join(data.frame(mm = abi_ndg_to_remove, remove = 'YES', stringsAsFactors = F), by = "mm") %>%
          filter(is.na(remove)) %>%
          select(-mm, -remove)
        
        # evaluate average or linear interpolation
        interp_data = df_bilanci_merge %>%
          group_by(abi, ndg) %>%
          summarize(min_year = min(c(2012, year)),
                    max_year = max(c(year, 2014)),.groups = 'drop') %>%
          nest(year = c(min_year, max_year)) %>%
          mutate(year = map(year, ~seq(unique(.x$min_year), unique(.x$max_year), 1))) %>%
          unnest(year) %>%
          left_join(df_bilanci_merge, by = c("abi", "ndg", "year")) %>%
          group_by(abi, ndg) %>%
          mutate(avail_Tot_Attivo = sum(!is.na(Tot_Attivo)),
                 avail_Tot_Equity = sum(!is.na(Tot_Equity)),
                 avail_Tot_Valore_Produzione = sum(!is.na(Tot_Valore_Produzione))) %>%
          group_by(abi, ndg) %>%
          mutate(Tot_Attivo_2 = case_when(
            avail_Tot_Attivo == 1 ~ max(Tot_Attivo, na.rm = T),
            TRUE ~ Tot_Attivo
          )) %>%
          group_by(abi, ndg) %>%
          mutate(Tot_Equity_2 = case_when(
            avail_Tot_Equity == 1 ~ max(Tot_Equity, na.rm = T),
            TRUE ~ Tot_Equity
          )) %>%
          group_by(abi, ndg) %>%
          mutate(Tot_Valore_Produzione_2 = case_when(
            avail_Tot_Valore_Produzione == 1 ~ max(Tot_Valore_Produzione, na.rm = T),
            TRUE ~ Tot_Valore_Produzione
          )) %>%
          
          group_by(abi, ndg) %>%
          mutate(Tot_Attivo_avg = mean(Tot_Attivo, na.rm = T),
                 Tot_Equity_avg = mean(Tot_Equity, na.rm = T),
                 Tot_Valore_Produzione_avg = mean(Tot_Valore_Produzione, na.rm = T)) %>%
          ungroup() %>%
          mutate(Tot_Attivo_avg = ifelse(is.na(Tot_Attivo), Tot_Attivo_avg, Tot_Attivo),
                 Tot_Equity_avg = ifelse(is.na(Tot_Equity), Tot_Equity_avg, Tot_Equity),
                 Tot_Valore_Produzione_avg = ifelse(is.na(Tot_Valore_Produzione), Tot_Valore_Produzione_avg, Tot_Valore_Produzione)) %>%
          
          group_by(abi, ndg) %>%
          arrange(year) %>%
          mutate(Tot_Attivo_interp = Hmisc::approxExtrap(x=year[!is.na(Tot_Attivo_2)],
                                                         y=Tot_Attivo_2[!is.na(Tot_Attivo_2)], xout=year)$y) %>%
          group_by(abi, ndg) %>%
          arrange(year) %>%
          mutate(Tot_Equity_interp = Hmisc::approxExtrap(x=year[!is.na(Tot_Equity_2)],
                                                         y=Tot_Equity_2[!is.na(Tot_Equity_2)], xout=year)$y) %>%
          group_by(abi, ndg) %>%
          arrange(year) %>%
          mutate(Tot_Valore_Produzione_interp = Hmisc::approxExtrap(x=year[!is.na(Tot_Valore_Produzione_2)],
                                                                    y=Tot_Valore_Produzione_2[!is.na(Tot_Valore_Produzione_2)], xout=year)$y) %>%
          ungroup()
        
        df_CRIF_bilanci_Attivo_Produzione = interp_data %>%
          filter(year >= 2012) %>%
          select(abi, ndg, year, Tot_Attivo_avg, Tot_Equity_avg, Tot_Valore_Produzione_avg, Tot_Attivo_interp, Tot_Equity_interp, Tot_Valore_Produzione_interp)
        
        # check differences between average and interpolation approach
        plot_tot_diff = df_CRIF_bilanci_Attivo_Produzione %>%
          mutate(perc_diff = (Tot_Attivo_interp - Tot_Attivo_avg) / abs(Tot_Attivo_avg),
                 Variable = 'Tot_Attivo') %>%
          select(year, Variable, perc_diff) %>%
          bind_rows(
            df_CRIF_bilanci_Attivo_Produzione %>%
              mutate(perc_diff = (Tot_Equity_interp - Tot_Equity_avg) / abs(Tot_Equity_avg),
                     Variable = 'Tot_Equity') %>%
              select(year, Variable, perc_diff),
            df_CRIF_bilanci_Attivo_Produzione %>%
              mutate(perc_diff = (Tot_Valore_Produzione_interp - Tot_Valore_Produzione_avg) / abs(Tot_Valore_Produzione_avg),
                     Variable = 'Tot_Valore_Produzione') %>%
              select(year, Variable, perc_diff)
          ) %>%
          filter(perc_diff != 0)
        png(paste0('./Checks/Distribution_percentage_diff_Average_vs_Interp.png'), width = 10, height = 7, units = 'in', res=300)
        plot(ggplot(plot_tot_diff, aes(x=perc_diff, fill=year)) +
               geom_density(alpha=.3) +
               facet_grid(Variable ~ ., scales = 'free') +
               theme(legend.position="bottom") +
               ggtitle('Distribution of (Interpolation - Average) / Average for Tot_Attivo and Tot_Valore_Produzione'))
        dev.off()
        
        error_flag = FALSE
        if (sum(is.na(df_CRIF_bilanci_Attivo_Produzione)) > 0){cat('\n\n ###### df_CRIF_bilanci_Attivo_Produzione has NA');error_flag = TRUE}
        if (df_CRIF_bilanci_Attivo_Produzione %>% select(abi, ndg, year) %>% uniqueN() != nrow(df_CRIF_bilanci_Attivo_Produzione)){
          cat('\n\n ###### df_CRIF_bilanci_Attivo_Produzione has duplicated rows for same year');error_flag = TRUE
        }
        
        # keep only average
        df_CRIF_bilanci_Attivo_Produzione = df_CRIF_bilanci_Attivo_Produzione %>%
          select(-ends_with('_interp')) %>%
          setNames(gsub('_avg', '', names(.))) %>%
          mutate(Tot_Attivo = round(Tot_Attivo),
                 Tot_Equity = round(Tot_Equity),
                 Tot_Valore_Produzione = round(Tot_Valore_Produzione))
        
        # add log10(Tot_Attivo) and Dimensione_Impresa
        df_CRIF_bilanci_Attivo_Produzione = df_CRIF_bilanci_Attivo_Produzione %>%
          mutate(Tot_Attivo_log10 = log10(Tot_Attivo)) %>%
          left_join(
            df_CRIF_bilanci_Attivo_Produzione %>%
              group_by(abi, ndg) %>%
              summarize(Tot_Attivo = max(Tot_Attivo),
                        Tot_Valore_Produzione = max(Tot_Valore_Produzione), .groups = 'drop') %>%
              mutate(Dimensione_Impresa = ifelse(Tot_Attivo < 2e6 | Tot_Valore_Produzione < 2e6, 'MICRO', '')) %>%
              mutate(Dimensione_Impresa = ifelse((Tot_Attivo >= 2e6 & Tot_Attivo < 10e6) | (Tot_Valore_Produzione >= 2e6 & Tot_Valore_Produzione < 10e6),
                                                 'SMALL', Dimensione_Impresa)) %>%
              mutate(Dimensione_Impresa = ifelse((Tot_Attivo >= 10e6 & Tot_Attivo < 50e6) | (Tot_Valore_Produzione >= 10e6 & Tot_Valore_Produzione < 43e6),
                                                 'MEDIUM', Dimensione_Impresa)) %>%
              mutate(Dimensione_Impresa = ifelse(Tot_Attivo > 50e6 | Tot_Valore_Produzione > 43e6, 'LARGE', Dimensione_Impresa)) %>%
              select(-Tot_Attivo, -Tot_Valore_Produzione),
            by = c("abi", "ndg"))
        
        if (error_flag == FALSE){
          saveRDS(df_CRIF_bilanci_Attivo_Produzione, './Checkpoints/compact_data/compact_CRIF_bilanci_Attivo_Produzione.rds')
        }
        
        rm(variable_mapping, df_bilanci_CEBI, df_bilanci_NONCEBI, df_bilanci_NONCEBI_ref, df_bilanci_merge, df_reference_available_years,
           solved_duplicates, duplicated_year, interp_data, abi_ndg_to_remove, plot_tot_diff, df_CRIF_bilanci_Attivo_Produzione)
        cat('Done')
      }
      
      # check impact of chiave_comune_to_remove_MURA
      {
        df_to_check = c(paste0('df_', c('AI_CC', 'AI_PCSBF', 'AI_ANFA', 'AI_POFI', 'AI_APSE',
                                        'AI_MURA_edit', 'AI_CRFI_edit',
                                        'RISCHIO', 'CR',
                                        'CRIF_anag', 'CRIF_bila')))
        
        cat('\n\nEvaluating impact of chiave_comune to remove from MURA...')
        summary_remove_MURA = c()
        for (df_lab in df_to_check){
          df = readRDS(paste0('./Checkpoints/compact_data/compact_', gsub('df_', '', df_lab), '.rds'))
          df = df %>%
            select(abi, ndg) %>%
            left_join(df_final_reference %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg")) %>%
            left_join(chiave_comune_to_remove_MURA %>% select(abi, ndg) %>% mutate(REMOVE_ABI_NDG = 'YES'), by = c("abi", "ndg")) %>%
            left_join(chiave_comune_to_remove_MURA %>% select(chiave_comune) %>% mutate(REMOVE_CHIAVE_COMUNE = 'YES'), by = "chiave_comune") %>%
            mutate(REMOVE_ABI_NDG = ifelse(is.na(REMOVE_ABI_NDG), 'NO', REMOVE_ABI_NDG)) %>%
            mutate(REMOVE_CHIAVE_COMUNE = ifelse(is.na(REMOVE_CHIAVE_COMUNE), 'NO', REMOVE_CHIAVE_COMUNE))
          total_abi_ndg = df %>%
            select(abi, ndg) %>%
            uniqueN()
          total_abi_ndg_to_remove = df %>%
            filter(REMOVE_ABI_NDG == 'YES') %>%
            select(abi, ndg) %>%
            uniqueN()
          total_chiave_comune = df %>%
            select(chiave_comune) %>%
            uniqueN()
          total_chiave_comune_to_remove = df %>%
            filter(REMOVE_CHIAVE_COMUNE == 'YES') %>%
            select(chiave_comune) %>%
            uniqueN()
          total_row = nrow(df)
          total_row_to_remove_abi_ndg = df %>%
            filter(REMOVE_ABI_NDG == 'YES') %>%
            nrow()
          total_row_to_remove_chiave_comnue = df %>%
            filter(REMOVE_CHIAVE_COMUNE == 'YES') %>%
            nrow()
          
          summary_remove_MURA = summary_remove_MURA %>% bind_rows(
            data.frame(Dataset = gsub('df_', '', df_lab),
                       CHIAVE_COMUNE_TO_REMOVE = paste0(format(total_chiave_comune_to_remove, big.mark=","),
                                                        ' (', round(total_chiave_comune_to_remove / total_chiave_comune * 100),'%)'),
                       ABI_NDG_TO_REMOVE = paste0(format(total_abi_ndg_to_remove, big.mark=","),
                                                  ' (', round(total_abi_ndg_to_remove / total_abi_ndg * 100),'%)'),
                       CHIAVE_COMUNE_ROWS = paste0(format(total_row_to_remove_chiave_comnue, big.mark=","),
                                                   ' (', round(total_row_to_remove_chiave_comnue / total_row * 100),'%)'),
                       ABI_NDG_ROWS = paste0(format(total_row_to_remove_abi_ndg, big.mark=","),
                                             ' (', round(total_row_to_remove_abi_ndg / total_row * 100),'%)'),
                       stringsAsFactors = F))
          rm(df)
        }
        write.table(summary_remove_MURA, './Stats/09_remove_from_AI_MURA_impact.csv', sep = ';', row.names = F, append = F)
        rm(summary_remove_MURA)
        cat('Done')
      }
      
      # save dataset different from compact_*
      cat('\n\nSaving rds...')
      saveRDS(df_AI_CRFI, './Checkpoints/compact_data/compact_AI_CRFI_edit.rds')
      saveRDS(df_AI_MURA, './Checkpoints/compact_data/compact_AI_MURA_edit.rds')
      saveRDS(df_CRIF_anag, './Checkpoints/compact_data/compact_CRIF_anag.rds')
      saveRDS(df_CRIF_bila, './Checkpoints/compact_data/compact_CRIF_bila.rds')
      rm(df_AI_CRFI, df_AI_POFI, df_AI_CC, df_AI_PCSBF, df_AI_ANFA, df_AI_APSE, df_AI_MURA, df_RISCHIO, df_CR,
         df_CRIF_anag, df_CRIF_bila, chiave_comune_to_remove_MURA, forme_tecniche)
      cat('Done')
      
      # create statistics for AI, CR and RISCHIO
      {
        df_to_run = c(paste0('df_', c('AI_CC', 'AI_PCSBF', 'AI_ANFA', 'AI_POFI', 'AI_APSE',
                                      'AI_MURA_edit', 'AI_CRFI_edit',
                                      'RISCHIO', 'CR')))
        cat('\n\nEvaluating statistics...')
        var_descr = read.csv2('./Coding_tables/Variable_Description.csv', stringsAsFactors=FALSE)
        forme_tecniche = read.csv2('./Coding_tables/Forme_tecniche.csv', stringsAsFactors=FALSE)
        stats_all = read.csv2(paste0('./Stats/08_df_CRIF_anag_preJoin_stats.csv'), stringsAsFactors=FALSE) %>%
          bind_rows(read.csv2(paste0('./Stats/08_df_CRIF_bila_preJoin_stats.csv'), stringsAsFactors=FALSE)) %>%
          mutate_all(as.character)
        for (df_lab in df_to_run){
          stats_all = stats_all %>% bind_rows(
            basicStatistics(readRDS(paste0('./Checkpoints/compact_data/compact_', gsub('df_', '', df_lab), '.rds'))) %>%
              rowwise() %>%
              mutate(VAR = strsplit(VARIABLE, '_')[[1]][2],
                     COD_FT_RAPPORTO = strsplit(VARIABLE, '_')[[1]][3]) %>%
              left_join(var_descr, by = "VAR") %>%
              left_join(forme_tecniche, by = "COD_FT_RAPPORTO") %>%
              mutate(DESCRIPTION = ifelse(!is.na(FT_RAPPORTO), paste0(DESCRIPTION, ' - ', FT_RAPPORTO), DESCRIPTION)) %>%
              select(VARIABLE, DESCRIPTION, everything(), -VAR, -COD_FT_RAPPORTO, -FT_RAPPORTO) %>%
              mutate_all(as.character) %>%
              replace(is.na(.), ''))
        }
        write.table(stats_all %>% replace(is.na(.), '') %>% select(VARIABLE, DESCRIPTION, everything()),
                    './Stats/08_All_preJoin_stats.csv', sep = ';', row.names = F, append = F)
        a=file.remove('./Stats/08_df_CRIF_anag_preJoin_stats.csv')
        a=file.remove('./Stats/08_df_CRIF_bila_preJoin_stats.csv')
        rm(stats_all, var_descr)
        cat('Done')
      }
    }
  }
  
  # define new perimeter with available bilanci of segmento_CRIF in Imprese, POE and Small Business and only with all 3 years
  {
    list_abi_ndg_year_CRIF_bila = readRDS('./Checkpoints/list_abi_ndg_year_CRIF_bila.rds')
    df_reference_new_perimeter = list_abi_ndg_year_CRIF_bila %>%
      filter(Fonte == 'CRIF') %>%
      unique() %>%
      filter(Segmento %in% c('Small Business', 'Imprese', 'POE')) %>%
      filter(match) %>%
      select(-Fonte, -match, -Segmento, -Expected_available_years, -Bilanci_available_years, -MISSING) %>%
      left_join(df_final_reference %>% select(abi, ndg, chiave_comune, segmento_CRIF, FLAG_Multibank), by = c("abi", "ndg"))
    
    # update FLAG_Multibank
    new_count = df_reference_new_perimeter %>%
      group_by(FLAG_Multibank, chiave_comune) %>%
      summarise(COUNT = n(), .groups = 'drop')
    if (new_count %>% filter(FLAG_Multibank == 0) %>% pull(COUNT) %>% unique() != 1){cat('\n\n ###### wrong FLAG_Multibank == 0 in df_reference_new_perimeter')}
    new_mono_bank = new_count %>%
      filter(FLAG_Multibank == 1 & COUNT == 1) %>%
      pull(chiave_comune)
    df_reference_new_perimeter = df_reference_new_perimeter %>%
      mutate(chiave_comune = ifelse(chiave_comune %in% new_mono_bank, paste0('singleCenPerim_', chiave_comune), chiave_comune)) %>%
      mutate(FLAG_Multibank = ifelse(chiave_comune %in% new_mono_bank, 0, FLAG_Multibank))
    new_count = df_reference_new_perimeter %>%
      group_by(FLAG_Multibank, chiave_comune) %>%
      summarise(COUNT = n(), .groups = 'drop')
    if (new_count %>% filter(FLAG_Multibank == 0) %>% pull(COUNT) %>% unique() != 1){cat('\n\n ###### wrong FLAG_Multibank == 0 in df_reference_new_perimeter final')}
    if (new_count %>% filter(FLAG_Multibank == 1) %>% pull(COUNT) %>% unique() %>% min() == 0){cat('\n\n ###### wrong FLAG_Multibank == 1 in df_reference_new_perimeter final')}
    rm(new_count, list_abi_ndg_year_CRIF_bila)
    saveRDS(df_reference_new_perimeter, './Checkpoints/df_reference_new_perimeter.rds')
  }
  df_reference_new_perimeter = readRDS('./Checkpoints/df_reference_new_perimeter.rds')
  cat('\n\n New perimeter')
  cat('\n -- Total NDG:', format(nrow(df_reference_new_perimeter), big.mark=","))
  cat('\n -- Total chiave_comune:', format(uniqueN(df_reference_new_perimeter$chiave_comune), big.mark=","), '\n')
  
  # create final dataset
  {
    var_descr = read.csv2('./Coding_tables/Variable_Description.csv', stringsAsFactors=FALSE)
    forme_tecniche = read.csv2('./Coding_tables/Forme_tecniche.csv', stringsAsFactors=FALSE)
    
    # prepare abi+ndg+year+month reference
    {
      chiave_comune_to_remove_MURA = readRDS('./Checkpoints/compact_data/chiave_comune_to_remove_MURA.rds')
      df_to_add = c(paste0('df_', c('AI_CC', 'AI_PCSBF', 'AI_ANFA', 'AI_POFI', 'AI_APSE',
                                    'AI_MURA_edit', 'AI_CRFI_edit',
                                    'RISCHIO', 'CR',
                                    'CRIF_anag', 'CRIF_bila')))
      summary_remove = data.frame(Perimeter = 'New Perimeter', Tot_chiave_comune = format(uniqueN(df_reference_new_perimeter$chiave_comune), big.mark=","),
                                  Tot_ABI_NDG=format(nrow(df_reference_new_perimeter), big.mark=","), stringsAsFactors = F)
      
      # prepare reference with combination of year and month
      if (reload_final_combination == FALSE){
        # loop all dataset to create a final reference (with all year combination)
        df_final_reference_combination = df_reference_new_perimeter %>%
          crossing(data.frame(year = c(2012, 2013, 2014))) %>%
          crossing(data.frame(month = c(1:12))) %>%
          select(abi, ndg, chiave_comune, year, month, everything())
        full_combination_row = nrow(df_final_reference_combination)
        if (nrow(df_reference_new_perimeter) * 36 != full_combination_row){cat('\n\n ###### wrong number of rows in df_final_reference_combination')}
        cat('\n\nCreating final reference with month/year combination:')
        for (df_lab in df_to_add){
          
          cat('\n', df_lab, nrow(df), '...')
          start_time = Sys.time()
          df = readRDS(paste0('./Checkpoints/compact_data/compact_', gsub('df_', '', df_lab), '.rds'))
          
          if ('month' %in% colnames(df)){
            if (!'JOIN' %in% colnames(df_final_reference_combination)){
              df_final_reference_combination = df_final_reference_combination %>%
                left_join(df %>%
                            select(abi, ndg, year, month) %>%
                            mutate(JOIN = 'YES'), by = c("abi", "ndg", "year", "month"))
            } else {
              df_final_reference_combination = df_final_reference_combination %>%
                left_join(df %>%
                            select(abi, ndg, year, month) %>%
                            mutate(JOIN_NEW = 'YES'), by = c("abi", "ndg", "year", "month")) %>%
                mutate(JOIN = ifelse(is.na(JOIN), JOIN_NEW, JOIN)) %>%
                select(-JOIN_NEW)
            }
            tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
            cat('Done in:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
          } else {cat('Skipped')}
          rm(df)
        }
        df_final_reference_combination = df_final_reference_combination %>%
          filter(JOIN == 'YES') %>%
          select(-JOIN)
        final_rows = nrow(df_final_reference_combination)
        cat('\nTotal rows:', format(final_rows, big.mark=","), ' - ', format(full_combination_row - final_rows, big.mark=","), 'removed')
        saveRDS(df_final_reference_combination, './Checkpoints/df_final_reference_combination.rds')
      }
      df_final_reference_combination = readRDS('./Checkpoints/df_final_reference_combination.rds')
      initial_ndg = nrow(df_reference_new_perimeter)
      initial_chiave_comune = uniqueN(df_reference_new_perimeter$chiave_comune)
      new_ndg = df_final_reference_combination %>% select(abi, ndg) %>% unique() %>% nrow()
      new_chiave_comune = uniqueN(df_final_reference_combination$chiave_comune)
      cat('\n\nEvaluating matching perimeter with datasets')
      cat('\n -- Total NDG:', format(new_ndg, big.mark=","), ' - ', format(initial_ndg - new_ndg, big.mark=","), 'removed')
      cat('\n -- Total chiave_comune:', format(new_chiave_comune, big.mark=","), ' - ', format(initial_chiave_comune - new_chiave_comune, big.mark=","), 'removed')
      summary_remove = summary_remove %>% bind_rows(data.frame('Matching with datasets', format(new_chiave_comune, big.mark=","),
                                                               format(new_ndg, big.mark=","), stringsAsFactors = F) %>% setNames(colnames(summary_remove)))
      
      # remove full chiave_comune from MURA
      final_rows = nrow(df_final_reference_combination)
      df_final_reference_combination = df_final_reference_combination %>% 
        # left_join(chiave_comune_to_remove_MURA %>% select(abi, ndg) %>% mutate(REMOVE = 'YES'), by = c("abi", "ndg")) %>%
        left_join(chiave_comune_to_remove_MURA %>% select(chiave_comune) %>% mutate(REMOVE = 'YES'), by = "chiave_comune") %>%
        filter(is.na(REMOVE)) %>%
        select(-REMOVE)
      new_final_rows = nrow(df_final_reference_combination)
      cat('\n\nRemoving chiave_comune from MURA')
      cat('\nTotal rows after removing chiave_comune from MURA:', format(new_final_rows, big.mark=","), ' - ', format(final_rows - new_final_rows, big.mark=","), 'removed')
      new_ndg_MURA = df_final_reference_combination %>% select(abi, ndg) %>% unique() %>% nrow()
      new_chiave_comune_MURA = uniqueN(df_final_reference_combination$chiave_comune)
      cat('\n -- Total NDG:', format(new_ndg_MURA, big.mark=","), ' - ', format(new_ndg - new_ndg_MURA, big.mark=","), 'removed')
      cat('\n -- Total chiave_comune:', format(new_chiave_comune_MURA, big.mark=","), ' - ', format(new_chiave_comune - new_chiave_comune_MURA, big.mark=","), 'removed')
      rm(chiave_comune_to_remove_MURA)
      summary_remove = summary_remove %>% bind_rows(data.frame('Removing chiave_comune from MURA', format(new_chiave_comune_MURA, big.mark=","),
                                                               format(new_ndg_MURA, big.mark=","), stringsAsFactors = F) %>% setNames(colnames(summary_remove)))
    }
    
    # join all dataset
    {
      df_final = df_final_reference_combination
      abi_ndg_to_keep = df_final %>% select(abi, ndg) %>% unique() %>% mutate(abi_ndg = paste0(abi, '_', ndg)) %>% pull(abi_ndg)
      col_not_to_check = colnames(df_final)
      total_cols = ncol(df_final)
      total_rowS = nrow(df_final)
      save_flag = TRUE
      cat('\n\nJoining all dataset:')
      for (df_lab in df_to_add){
        
        cat('\n', df_lab, nrow(df), '...')
        start_time = Sys.time()
        # filter only rows to keep (for correct tot_elements_df)
        df = readRDS(paste0('./Checkpoints/compact_data/compact_', gsub('df_', '', df_lab), '.rds')) %>%
          left_join(df_final_reference %>% select(abi, ndg), by = c("abi", "ndg")) %>%
          mutate(abi_ndg = paste0(abi, '_', ndg)) %>%
          filter(abi_ndg %in% abi_ndg_to_keep) %>%
          select(-abi_ndg)
        if ('month' %in% colnames(df)){
          col_to_check = setdiff(colnames(df), col_not_to_check)
          total_cols = total_cols + length(col_to_check)
          tot_elements_df = sum(!is.na(df %>% select(all_of(col_to_check))))
          duplicated_cols = intersect(colnames(df_final), col_to_check)
          if (length(duplicated_cols) > 0){cat('\nDuplicated columns found:\n--', paste0(duplicated_cols, collapse = '\n-- '));save_flag = F}
          
          df_final = df_final %>%
            left_join(df, by = c("abi", "ndg", "year", "month"))
          tot_elements_df_after_join = sum(!is.na(df_final %>% select(all_of(col_to_check))))
          if (tot_elements_df != tot_elements_df_after_join){cat('\n-- total non-NA elements mismatch');save_flag = F}
          tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
          cat('Done in:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
        } else {cat('Skipped by month')}
        rm(df)
      }
      
      # add CRIF_anag and CRIF_bila
      for (add in c('CRIF_anag', 'CRIF_bila')){
        cat('\n', add, 'by year...')
        start_time = Sys.time()
        df = readRDS(paste0('./Checkpoints/compact_data/compact_', add, '.rds'))
        col_to_check = setdiff(colnames(df), col_not_to_check)
        total_cols = total_cols + length(col_to_check)
        df_final = df_final %>%
          left_join(df, by = c("abi", "ndg", "year"))
        tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
        rm(df)
        cat('Done in:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
      }
      
      # add Tot_Attivo, Tot_Equity and Tot_Valore_Produzione
      compact_CRIF_bilanci_Attivo_Produzione = readRDS('./Checkpoints/compact_data/compact_CRIF_bilanci_Attivo_Produzione.rds')
      df_final = df_final %>%
        left_join(compact_CRIF_bilanci_Attivo_Produzione, by = c("abi", "ndg", "year"))
      total_cols = total_cols + ncol(compact_CRIF_bilanci_Attivo_Produzione) - 3
      rm(compact_CRIF_bilanci_Attivo_Produzione)
      if (total_rowS != nrow(df_final)){cat('\n\n ###### wrong number of rows:', nrow(df_final), '- expected:', total_rowS);save_flag = F}
      
      # check percentage of matching abi+ndg for Tot_Attivo, Tot_Equity and Tot_Valore_Produzione
      summary_df_bilanci = df_final %>%
        select(abi, ndg, year, segmento_CRIF) %>%
        left_join(
          df_final %>%
            select(abi, ndg, BILA_tipo_dataentry_fin) %>%
            group_by(abi, ndg) %>%
            summarise(Tipo_Bilancio = paste0(sort(unique(BILA_tipo_dataentry_fin)), collapse = '|'), .groups = 'drop'), by = c("abi", "ndg")) %>%
        left_join(readRDS('./Checkpoints/compact_data/compact_CRIF_bilanci_Attivo_Produzione.rds'), by = c("abi", "ndg", "year")) %>%
        unique() %>%
        group_by(Tipo_Bilancio, abi, ndg) %>%
        summarize(Expected_years = n(),#uniqueN(year),
                  Available_years = sum(!is.na(Tot_Attivo)), .groups = 'drop') %>%
        mutate(Expected_years = ifelse(Available_years == 0, '1,2,3', Expected_years),
               Available_years = ifelse(Available_years == 0, 'Missing', Available_years)) %>%
        mutate(match = Expected_years == Available_years) %>%
        mutate(Expected_years = ifelse(match, '1,2,3', Expected_years),
               Available_years = ifelse(match, 'Matched', Available_years)) %>%
        group_by(Tipo_Bilancio, Expected_years, Available_years) %>%
        summarize(Total_abi_ndg = n(), .groups = 'drop')
      if (sum(summary_df_bilanci$Total_abi_ndg) != df_final %>% select(abi, ndg) %>% uniqueN()){cat('\n\n ###### abi+ndg mismatch in summary_df_bilanci')}
      summary_df_bilanci = summary_df_bilanci %>% 
        bind_rows(data.frame(t(c('---', '', '', NA)), stringsAsFactors = F) %>%
                    setNames(colnames(summary_df_bilanci)) %>%
                    mutate(Total_abi_ndg = as.numeric(Total_abi_ndg)),
                  summary_df_bilanci %>%
                    mutate(Tipo_Bilancio = ifelse(str_detect(Tipo_Bilancio, 'noncebi_'), 'TOTAL_NONCEBI', 'TOTAL_CEBI')) %>%
                    group_by(Tipo_Bilancio, Expected_years, Available_years) %>%
                    summarize(Total_abi_ndg = sum(Total_abi_ndg), .groups = 'drop')
        ) %>%
        mutate(Total_abi_ndg_perc = paste0(round(Total_abi_ndg / (sum(Total_abi_ndg, na.rm = T) / 2) * 100, 1), '%'),
               Total_abi_ndg = format(Total_abi_ndg, big.mark = ',')) %>%
        replace(. == "    NA" | . == "NA%", '')
      write.table(summary_df_bilanci, './Stats/09_df_bilanci_matching_by_year.csv', sep = ';', row.names = F, append = F)
      
      # remove abi+ndg with missing Tot_Attivo, Tot_Equity and Tot_Valore_Produzione
      cat('\n\nRemoving missing Tot_Attivo and Tot_Valore_Produzione')
      final_rows = nrow(df_final)
      final_abi_ndg = df_final %>% select(abi, ndg) %>% uniqueN()
      final_chiave_comune = df_final %>% select(chiave_comune) %>% uniqueN()
      abi_ndg_to_remove = df_final %>%
        filter(is.na(Tot_Attivo)) %>%
        select(abi, ndg) %>%
        unique() %>%
        mutate(mm = paste0(abi, ndg)) %>%
        pull(mm)
      abi_ndg_to_keep = df_final %>%
        filter(!is.na(Tot_Attivo)) %>%
        select(abi, ndg) %>%
        unique() %>%
        mutate(mm = paste0(abi, ndg)) %>%
        pull(mm)
      if (length(intersect(abi_ndg_to_remove, abi_ndg_to_keep)) > 0){cat('\n\n ###### abi+ndg partially removed for Tot_Attivo and Tot_Valore_Produzione')}
      df_final = df_final %>%
        filter(!is.na(Tot_Attivo))
      if (sum(is.na(df_final$Tot_Attivo)) > 0){cat('\n\n ###### missing Tot_Attivo still present')}
      if (sum(is.na(df_final$Tot_Equity)) > 0){cat('\n\n ###### missing Tot_Equity still present')}
      if (sum(is.na(df_final$Tot_Valore_Produzione)) > 0){cat('\n\n ###### missing Tot_Valore_Produzione still present')}
      rm(abi_ndg_to_remove, abi_ndg_to_keep)
      
      new_abi_ndg = df_final %>% select(abi, ndg) %>% uniqueN()
      nwe_chiave_comune = df_final %>% select(chiave_comune) %>% uniqueN()
      cat('\nTotal rows after removing missing Tot_Attivo and Tot_Valore_Produzione:', format(nrow(df_final), big.mark=","), ' - ', format(final_rows - nrow(df_final), big.mark=","), 'removed')
      cat('\n -- Total NDG:', format(new_abi_ndg, big.mark=","), ' - ', format(final_abi_ndg - new_abi_ndg, big.mark=","), 'removed')
      cat('\n -- Total chiave_comune:', format(nwe_chiave_comune, big.mark=","), ' - ', format(final_chiave_comune - nwe_chiave_comune, big.mark=","), 'removed')
      summary_remove = summary_remove %>% bind_rows(data.frame('Removing missing Tot_Attivo and Tot_Valore_Produzione', format(nwe_chiave_comune, big.mark=","),
                                                               format(new_abi_ndg, big.mark=","), stringsAsFactors = F) %>% setNames(colnames(summary_remove)))
      
      # remove abi = '3599' - Cassa Centrale Banca
      final_rows = nrow(df_final)
      final_abi_ndg = df_final %>% select(abi, ndg) %>% uniqueN()
      final_chiave_comune = df_final %>% select(chiave_comune) %>% uniqueN()
      
      df_final = df_final %>%
        filter(abi != '3599')
      
      new_abi_ndg = df_final %>% select(abi, ndg) %>% uniqueN()
      nwe_chiave_comune = df_final %>% select(chiave_comune) %>% uniqueN()
      cat('\nTotal rows after removing abi=3599:', format(nrow(df_final), big.mark=","), ' - ', format(final_rows - nrow(df_final), big.mark=","), 'removed')
      cat('\n -- Total NDG:', format(new_abi_ndg, big.mark=","), ' - ', format(final_abi_ndg - new_abi_ndg, big.mark=","), 'removed')
      cat('\n -- Total chiave_comune:', format(nwe_chiave_comune, big.mark=","), ' - ', format(final_chiave_comune - nwe_chiave_comune, big.mark=","), 'removed')
      summary_remove = summary_remove %>% bind_rows(data.frame('Removing abi 3599', format(nwe_chiave_comune, big.mark=","),
                                                               format(new_abi_ndg, big.mark=","), stringsAsFactors = F) %>% setNames(colnames(summary_remove)))
      
      # remove missing in BILA_tipo_dataentry_fin (77 rows) -> 125 rows because of same abi+ndg
      final_rows = nrow(df_final)
      final_abi_ndg = df_final %>% select(abi, ndg) %>% uniqueN()
      final_chiave_comune = df_final %>% select(chiave_comune) %>% uniqueN()
      
      abi_ndg_to_remove = df_final %>%
        filter(is.na(BILA_tipo_dataentry_fin)) %>%
        select(abi, ndg) %>%
        unique() %>%
        mutate(abi_ndg = paste0(abi, ndg))
      # total_NA = sum(is.na(df_final$BILA_tipo_dataentry_fin))
      # 
      # check_remove = df_final %>% select(abi, ndg) %>%
      #   mutate(abi_ndg = paste0(abi, ndg)) %>%
      #   left_join(abi_ndg_to_remove %>% select(abi_ndg) %>% mutate(REMOVE = 'YES'), by = "abi_ndg")
      # total_NA == sum(!is.na(check_remove$REMOVE))
      df_final = df_final %>%
        left_join(abi_ndg_to_remove %>% select(abi, ndg) %>% mutate(REMOVE = 'YES'), by = c("abi", "ndg")) %>%
        filter(is.na(REMOVE)) %>%
        select(-REMOVE)
	  rm(abi_ndg_to_remove)
      new_abi_ndg = df_final %>% select(abi, ndg) %>% uniqueN()
      nwe_chiave_comune = df_final %>% select(chiave_comune) %>% uniqueN()
      cat('\nTotal rows after removing missing BILA_tipo_dataentry_fin:', format(nrow(df_final), big.mark=","), ' - ', format(final_rows - nrow(df_final), big.mark=","), 'removed')
      cat('\n -- Total NDG:', format(new_abi_ndg, big.mark=","), ' - ', format(final_abi_ndg - new_abi_ndg, big.mark=","), 'removed')
      cat('\n -- Total chiave_comune:', format(nwe_chiave_comune, big.mark=","), ' - ', format(final_chiave_comune - nwe_chiave_comune, big.mark=","), 'removed')
      summary_remove = summary_remove %>% bind_rows(data.frame('Removing missing tipo bilancio', format(nwe_chiave_comune, big.mark=","),
                                                               format(new_abi_ndg, big.mark=","), stringsAsFactors = F) %>% setNames(colnames(summary_remove)))
      
      # check full missing rows
      check_missing = df_final %>%
        select(-abi, -ndg, -chiave_comune, -year, -month, -segmento_CRIF, -FLAG_Multibank) %>%
        mutate(NA_count = rowSums(is.na(.)))
      na_full = sum(check_missing$NA_count == (ncol(check_missing) - 1))
      if (na_full > 0){cat('\n\n ###### full missing rows:', na_full)}
      
      if (total_cols != ncol(df_final)){cat('\n\n ###### wrong number of columns:', ncol(df_final), '- expected:', total_cols);save_flag = F}
      write.table(summary_remove, './Stats/10_df_final_perimeter_summary.csv', sep = ';', row.names = F, append = F)
      rm(df_final_reference, check_missing, df_reference_new_perimeter, summary_remove, df_final_reference_combination, summary_df_bilanci)
      
      # add flag CEBI vs NON_CEBI
      df_final = df_final %>%
        mutate(FLAG_BILANCIO = ifelse(str_detect(BILA_tipo_dataentry_fin, 'noncebi_'), 'NONCEBI', 'CEBI'))
      if (df_final %>% group_by(abi, ndg) %>% summarize(COUNT = uniqueN(FLAG_BILANCIO), .groups = 'drop') %>% pull(COUNT) %>% max() > 1){
        cat('\n\n ###### abi+ndg with multiple FLAG_BILANCIO found')}
      
      # add Regione and Regione_Macro
      mapping_regione = read.csv2('./Coding_tables/Mapping_regione.csv', stringsAsFactors=FALSE, colClasses = 'character', na.strings = 'ccc') %>%
        select(-PROVINCIA, -REGIONE)
      mapping_regione_missing = read.csv2('./Coding_tables/Mapping_regione_missing.csv', stringsAsFactors=FALSE, colClasses = 'character')
      df_final = df_final %>%
        left_join(mapping_regione, by = "ANAG_cod_provincia") %>%
        left_join(mapping_regione_missing, by = "abi") %>%
        mutate(Regione = ifelse(Regione == 'OTH', Regione_recover, Regione)) %>%
        mutate(
          Regione_Macro = case_when(
            Regione %in% c("PIE", "LIG", "LOM", "VDA") ~ "NORD-OVEST",
            Regione %in% c("TAA", "VEN", "FVG", "EMR") ~ "NORD-EST",
            Regione %in% c("TOS", "UMB", "MAR", "LAZ") ~ "CENTRO",
            Regione %in% c("ABR", "CAM", "PUG", "BAS", "CAL", "MOL") ~ "SUD",
            Regione %in% c("SIC", "SAR") ~ "ISOLE")
        ) %>%
        select(-Regione_recover)
      rm(mapping_regione, mapping_regione_missing)
      
      # evaluate statistics for df_final
      write.table(basicStatistics(df_final) %>%
                    rowwise() %>%
                    mutate(VAR = strsplit(VARIABLE, '_')[[1]][2],
                           COD_FT_RAPPORTO = strsplit(VARIABLE, '_')[[1]][3]) %>%
                    left_join(var_descr, by = "VAR") %>%
                    left_join(forme_tecniche, by = "COD_FT_RAPPORTO") %>%
                    mutate(DESCRIPTION = ifelse(!is.na(FT_RAPPORTO), paste0(DESCRIPTION, ' - ', FT_RAPPORTO), DESCRIPTION)) %>%
                    select(VARIABLE, DESCRIPTION, everything(), -VAR, -COD_FT_RAPPORTO, -FT_RAPPORTO) %>%
                    mutate_all(as.character) %>%
                    replace(is.na(.), ''),
                  './Stats/10_df_final_stats.csv', sep = ';', row.names = F, append = F)
      
    }
    
    # remove some variables and anomalous values
    {
      var_to_remove = c('ANAG_professione',	'AVG_APSE002_D11910',	'AVG_APSE002_D11916',	'AVG_APSE002_E11910',	'AVG_APSE002_E11916',
                        'AVG_APSE003_D11910',	'AVG_APSE003_D11916',	'AVG_APSE003_E11910',	'AVG_APSE003_E11916',	'AVG_APSE004_D11910',
                        'AVG_APSE004_E11910',	'AVG_APSE004_EST050',	'AVG_APSE004_EST054',	'AVG_APSE1854_D11910',	'AVG_APSE1854_E11910',
                        'AVG_APSE1854_EST050',	'AVG_APSE1854_EST054',	'RAW_CR9789',	'RAW_CR9790',	'RAW_CR9791',	'RAW_CR9792',	'RAW_CR9793',
                        'RAW_CR9794',	'RAW_CR9795',	'RAW_CR9796',	'RAW_CR9797',	'RAW_CR9798',	'RAW_CR9820',	'RAW_CR9821',	'RAW_CR9822',
                        'RAW_CR9823',	'RAW_CR9824',	'RAW_CR9825',	'RAW_CR9852',	'SUM_ANFA010',	'SUM_ANFA011',	'SUM_ANFA012',	'SUM_ANFA013',
                        'SUM_ANFA014',	'SUM_ANFA015',	'SUM_ANFA016',	'SUM_ANFA017',	'SUM_APSE000_D11910',	'SUM_APSE000_D11916',
                        'SUM_APSE000_E11910',	'SUM_APSE000_E11916',	'SUM_APSE001_D11910',	'SUM_APSE001_D11916',	'SUM_APSE001_E11910',
                        'SUM_APSE001_E11916',	'SUM_APSE005_EXPORT',	'SUM_APSE005_IMPORT',	'SUM_APSE006_EXPORT',	'SUM_APSE006_IMPORT',
                        'SUM_APSE007_EXPORT',	'SUM_APSE007_IMPORT',	'SUM_APSE008_EXPORT',	'SUM_APSE008_IMPORT',	'SUM_APSE009_EXPORT',
                        'SUM_APSE009_IMPORT',	'SUM_APSE010_EXPORT',	'SUM_APSE010_IMPORT',	'SUM_APSE011_EXPORT',	'SUM_APSE011_IMPORT',
                        'SUM_APSE012_EXPORT',	'SUM_APSE012_IMPORT',	'SUM_APSE013_EXPORT',	'SUM_APSE013_IMPORT',	'SUM_APSE014_EXPORT',
                        'SUM_APSE014_IMPORT',	'SUM_APSE015_EXPORT',	'SUM_APSE015_IMPORT',	'SUM_APSE016_EXPORT',	'SUM_APSE016_IMPORT',
                        'SUM_PCSBF010',	'SUM_PCSBF011',	'SUM_PCSBF012',	'SUM_PCSBF013',	'SUM_PCSBF014',	'SUM_PCSBF015',	'SUM_PCSBF016',
                        'SUM_PCSBF017',	'SUM_POFI000',	'SUM_POFI001',	'SUM_POFI002',	'SUM_POFI003')
      
      df_final = df_final %>%
        select(-all_of(var_to_remove))
      
      # check for strange values with high frequency (like 99996)
      # check_values = c()
      # for (var in colnames(df_final)){
      #   aa = data.frame(table(df_final %>% pull(var))) %>%
      #     arrange(desc(Freq)) %>%
      #     mutate(label = paste0(Var1, ' (', Freq, ')')) %>%
      #     filter(row_number() <= 10) %>%
      #     pull(label) %>%
      #     paste0(collapse = ', ')
      #   check_values = check_values %>%
      #     bind_rows(data.frame(Variable = var, Values = aa, stringsAsFactors = F))
      # }
      # write.table(check_values, './Checks/04_df_final_anomalous_values.csv', sep = ';', row.names = F, append = F)
      
      # remove anomalous values
      df_final = df_final %>%
        mutate_if(is.numeric, ~replace(., . == 99996, NA)) %>%
        mutate_if(is.numeric, ~replace(., . == 99995, NA))
      
      # evaluate statistics for df_final
      write.table(basicStatistics(df_final) %>%
                    rowwise() %>%
                    mutate(VAR = strsplit(VARIABLE, '_')[[1]][2],
                           COD_FT_RAPPORTO = strsplit(VARIABLE, '_')[[1]][3]) %>%
                    left_join(var_descr, by = "VAR") %>%
                    left_join(forme_tecniche, by = "COD_FT_RAPPORTO") %>%
                    mutate(DESCRIPTION = ifelse(!is.na(FT_RAPPORTO), paste0(DESCRIPTION, ' - ', FT_RAPPORTO), DESCRIPTION)) %>%
                    select(VARIABLE, DESCRIPTION, everything(), -VAR, -COD_FT_RAPPORTO, -FT_RAPPORTO) %>%
                    mutate_all(as.character) %>%
                    replace(is.na(.), ''),
                  './Stats/11_df_final_stats_cleaning.csv', sep = ';', row.names = F, append = F)
      
    }
    
    # update FLAG_Multibank
    {
      # update FLAG_Multibank
      new_count = df_final %>%
        select(FLAG_Multibank, chiave_comune, abi, ndg) %>%
        unique() %>%
        group_by(FLAG_Multibank, chiave_comune) %>%
        summarise(COUNT = n(), .groups = 'drop')
      if (new_count %>% filter(FLAG_Multibank == 0) %>% pull(COUNT) %>% unique() != 1){cat('\n\n ###### wrong FLAG_Multibank == 0 in df_final')}
      new_mono_bank = new_count %>%
        filter(FLAG_Multibank == 1 & COUNT == 1) %>%
        pull(chiave_comune)
      new_chiave_comune = df_final %>%
        select(chiave_comune) %>%
        unique() %>%
        left_join(data.frame(chiave_comune = new_mono_bank, change = 'YES', stringsAsFactors = F ), by = "chiave_comune") %>%
        filter(change == 'YES') %>%
        mutate(split = grepl('_', chiave_comune)) %>%
        rowwise() %>%
        mutate(new_chiave_comune = ifelse(split, strsplit(chiave_comune, '_')[[1]][2], chiave_comune)) %>%
        mutate(new_chiave_comune = paste0('singleCenPerim_', new_chiave_comune)) %>%
        select(-change, -split)
      df_final = df_final %>%
        left_join(new_chiave_comune, by = "chiave_comune") %>%
        mutate(chiave_comune = ifelse(!is.na(new_chiave_comune), new_chiave_comune, chiave_comune)) %>%
        select(-new_chiave_comune) %>%
        group_by(chiave_comune, abi, ndg) %>%
        mutate(FLAG_Multibank = ifelse(n() > 1, 1, 0)) %>%
        ungroup()
      if (sum(is.na(df_final$chiave_comune)) > 0){cat('\n\n ###### missing in chiave_comune df_final')}
      new_count = df_final %>%
        select(FLAG_Multibank, chiave_comune, abi, ndg) %>%
        unique() %>%
        group_by(FLAG_Multibank, chiave_comune) %>%
        summarise(COUNT = n(), .groups = 'drop')
      if (new_count %>% filter(FLAG_Multibank == 0) %>% pull(COUNT) %>% unique() != 1){cat('\n\n ###### wrong FLAG_Multibank == 0 in df_final')}
      if (new_count %>% filter(FLAG_Multibank == 1) %>% pull(COUNT) %>% unique() %>% min() == 0){cat('\n\n ###### wrong FLAG_Multibank == 1 in df_final')}
      rm(new_count, new_chiave_comune)
    }
    
    # check percentage of missing by segmento_CRIF
    {
      check_missing_segmento = c()
      for (seg in unique(df_final$segmento_CRIF)){
        stats = suppressWarnings(basicStatistics(df_final %>% filter(segmento_CRIF == seg)))
        cols = stats %>%
          filter(TYPE == 'CHARACTER') %>%
          rename(Missing = BLANKs) %>%
          select(VARIABLE, TYPE, Missing) %>%
          bind_rows(stats %>%
                      filter(TYPE == 'NUMERIC') %>%
                      rename(Missing = NAs) %>%
                      select(VARIABLE, TYPE, Missing)) %>%
          rowwise() %>%
          mutate(Missing = strsplit(Missing, ' ')[[1]][2]) %>%
          mutate(Missing = gsub('[()]', '', Missing)) %>%
          rename(!!sym(paste0('Missing_', seg, '_', stats$NUM_OSS[1],'obs')) := Missing) %>%
          filter(!VARIABLE %in% c('abi', 'ndg', 'chiave_comune', 'segmento_CRIF', 'year', 'month', 'FLAG_Multibank'))
        if (length(check_missing_segmento) == 0){
          check_missing_segmento = cols
        } else {
          check_missing_segmento = check_missing_segmento %>% left_join(cols, by = c("VARIABLE", "TYPE"))
        }
      }
      write.table(check_missing_segmento, './Stats/10_df_final_missing_by_segmento.csv', sep = ';', row.names = F, append = F)
      rm(cols, stats, check_missing_segmento)
    }
    
    # check number of abi+ndg for total years and months
    {
      check_by_years = df_final %>%
        group_by(abi, ndg) %>%
        summarize(Available_Years = uniqueN(year), .groups = 'drop') %>%
        group_by(Available_Years) %>%
        summarize(Total_Abi_Ndg = n(), .groups = 'drop')
      if (sum(check_by_years$Total_Abi_Ndg) != df_final %>% select(abi, ndg) %>% uniqueN()){
        cat('\n\n ###### abi+ndg mismatch in check_by_years')
      } else {
        write.table(check_by_years %>% mutate(Total_Abi_Ndg = format(Total_Abi_Ndg, big.mark = ',')),
                    './Stats/10_df_final_distribution_year.csv', sep = ';', row.names = F, append = F)
      }
      
      check_by_months = df_final %>%
        group_by(abi, ndg) %>%
        summarize(Available_Months = n(), .groups = 'drop') %>%
        group_by(Available_Months) %>%
        summarize(Total_Abi_Ndg = n(), .groups = 'drop')
      if (sum(check_by_months$Total_Abi_Ndg) != df_final %>% select(abi, ndg) %>% uniqueN()){
        cat('\n\n ###### abi+ndg mismatch in check_by_months')
      } else {
        write.table(check_by_months %>% mutate(Total_Abi_Ndg = format(Total_Abi_Ndg, big.mark = ',')),
                    './Stats/10_df_final_distribution_month.csv', sep = ';', row.names = F, append = F)
      }
      rm(check_by_years, check_by_months)
    }
    
    # save distribution by Dimensione_Impresa
    {
      check_Impresa = df_final %>%
        select(abi, ndg, Dimensione_Impresa) %>%
        unique() %>%
        group_by(Dimensione_Impresa) %>%
        summarize(Tot_Abi_Ndg = n(), .groups = 'drop') %>%
        arrange(Tot_Abi_Ndg) %>%
        mutate(Tot_Abi_Ndg = format(Tot_Abi_Ndg, big.mark = ','))
      write.table(check_Impresa, './Stats/10_df_final_Dimensione_Impresa.csv', sep = ';', row.names = F, append = F)
      
      segmento_map = df_final %>%
        select(abi, ndg, segmento_CRIF, Dimensione_Impresa) %>%
        unique()
      segmento_map = as.data.frame(table(factor(segmento_map$Dimensione_Impresa, levels = check_Impresa$Dimensione_Impresa), 
                                         factor(segmento_map$segmento_CRIF, levels = unique(segmento_map$segmento_CRIF)))) %>%
        spread(Var2, Freq) %>%
        rename(`Dimensione_Impresa\\CRIF` = Var1) %>%
        mutate_all(list(~format(., big.mark=",")))
      write.table(segmento_map, './Stats/10_df_final_Dimensione_Impresa_mapping.csv', sep = ';', row.names = F, append = F)
      rm(check_Impresa, segmento_map)
    }
    
    # check missing distribution for BILA by BILA_tipo_dataentry_fin
    {
      var_to_check = c('BILA_onerifinanz_mol', 'BILA_oneri_ricavi', 'BILA_oneri_valagg', 'BILA_rimanenze_attivo', 'BILA_riman_debbreve',
                       'BILA_riman_debml', 'BILA_rimanenze_debtot', 'BILA_rimanenze_debbanche', 'BILA_roi', 'BILA_ros', 'BILA_valprod_riman')
      check_missing = data.frame(Tot_Abi_Ndg = unique(df_final$BILA_tipo_dataentry_fin), stringsAsFactors = F)
      for (v in var_to_check){
        check_missing = check_missing %>%
          left_join(df_final %>%
                      select(abi, ndg, BILA_tipo_dataentry_fin, all_of(v)) %>%
                      mutate(abi_ndg = paste0(abi, ndg)) %>%
                      filter(is.na(!!sym(v))) %>%
                      group_by(BILA_tipo_dataentry_fin) %>%
                      summarize(Count_Abi_Ndg = format(uniqueN(abi_ndg), big.mark = ','), .groups = 'drop') %>%
                      rename(!!sym(v) := Count_Abi_Ndg,
                             Tot_Abi_Ndg = BILA_tipo_dataentry_fin), by = "Tot_Abi_Ndg")
      }
      write.table(check_missing %>% replace(is.na(.), ''), './Checks/06_df_final_missing_BILA.csv', sep = ';', row.names = F, append = F)
      rm(check_missing)
    }

    # replace missing
    {
      # BILA
      {
        replace_0 = c('BILA_ARIC_VPROD', 'BILA_IMM_VPROD', 'BILA_ammor_su_costi', 'BILA_costolavoro_ricavi', 'BILA_costolavoro_valagg',
                      'BILA_costolavoro_valprod', 'BILA_totdebiti_su_passivo', 'BILA_IMM_Patnetto', 'BILA_IMM_attivo', 'BILA_Mat_Patnetto',
                      'BILA_Mat_attivo', 'BILA_debbanche_passivo')
        replace_median = c('BILA_cashflow_valprod', 'BILA_liquid_attivo', 'BILA_VAGG_VPROD', 'BILA_autonomia_finanz', 'BILA_cashflow_ricavi',
                           'BILA_copertura_attivo', 'BILA_copertura_immob', 'BILA_copertura_finanz', 'BILA_crediti_su_attivo', 'BILA_indebitamento_breve',
                           'BILA_indebitamento_ml', 'BILA_dipend_finanz', 'BILA_liquid_imm', 'BILA_rigidita_impieghi', 'BILA_mol_ricavi', 'BILA_oneri_ricavi',
                           'BILA_oneri_valagg', 'BILA_patr_su_patrml', 'BILA_leverage', 'BILA_mol_valprod', 'BILA_rimanenze_attivo', 'BILA_riman_debbreve',
                           'BILA_riman_debml', 'BILA_rimanenze_debtot', 'BILA_rimanenze_debbanche', 'BILA_roa', 'BILA_rod', 'BILA_roe', 'BILA_roi',
                           'BILA_ros', 'BILA_rotazione_capitale', 'BILA_rotazione_circolante', 'BILA_rotazione_magazzino', 'BILA_turnover',
                           'BILA_valagg_fatturato',	'BILA_valprod_riman', 'Tot_Valore_Produzione')
        replace_100 = c('BILA_onerifinanz_mol')
        
        df_final = df_final %>%
          mutate_at(all_of(replace_median), funs(ifelse(is.na(.),median(., na.rm = TRUE),.))) %>%
          mutate_at(all_of(replace_0), funs(ifelse(is.na(.),0,.))) %>%
          mutate_at(all_of(replace_100), funs(ifelse(is.na(.),100,.))) %>%
          mutate(BILA_acid_test = ifelse(is.na(BILA_acid_test) & (BILA_rimanenze_attivo == 0 | BILA_riman_debbreve == 0 | BILA_riman_debml == 0 |
                                                                    BILA_rimanenze_debtot == 0 | BILA_rimanenze_debbanche == 0), BILA_current_ratio, BILA_acid_test)) %>%
          mutate(BILA_durata_scorte = ifelse(is.na(BILA_durata_scorte) & (BILA_rimanenze_attivo == 0 | BILA_riman_debbreve == 0 | BILA_riman_debml == 0 |
                                                                            BILA_rimanenze_debtot == 0 | BILA_rimanenze_debbanche == 0), 0, BILA_durata_scorte)) %>%
          mutate(BILA_patr_su_patrrim = ifelse(is.na(BILA_patr_su_patrrim) & (BILA_rimanenze_attivo == 0 | BILA_riman_debbreve == 0 | BILA_riman_debml == 0 |
                                                                                BILA_rimanenze_debtot == 0 | BILA_rimanenze_debbanche == 0), 100, BILA_patr_su_patrrim)) %>%
          
          mutate(BILA_debitifornit_su_totdebiti = ifelse(is.na(BILA_debitifornit_su_totdebiti) & BILA_totdebiti_su_debitibreve == 0, 100, BILA_debitifornit_su_totdebiti)) %>%
          mutate(BILA_totdebiti_su_debitibreve = ifelse(is.na(BILA_totdebiti_su_debitibreve) & (BILA_debbrevi_debbanche == 0 | BILA_debbrevi_patrim == 0), 100, BILA_totdebiti_su_debitibreve)) %>%
          mutate(BILA_debbanche_circolante = ifelse(is.na(BILA_debbanche_circolante) & BILA_debbanche_passivo == 0 , 0, BILA_debbanche_circolante)) %>%
          mutate(BILA_current_ratio = ifelse(is.na(BILA_current_ratio) & BILA_IMM_attivo == 100 , 0, BILA_current_ratio)) %>%
          mutate(BILA_debbrevi_debbanche = ifelse(is.na(BILA_debbrevi_debbanche) & BILA_debbanche_passivo == 0 , 100, BILA_debbrevi_debbanche)) %>%
          mutate(BILA_debbrevi_patrim = ifelse(is.na(BILA_debbrevi_patrim) & BILA_patr_su_patrml == 0 , 100, BILA_debbrevi_patrim)) %>%
          mutate(BILA_totdebiti_su_patrim = ifelse(is.na(BILA_totdebiti_su_patrim) & BILA_patr_su_patrml == 0 , 100, BILA_totdebiti_su_patrim)) %>%
          mutate(BILA_debitifornit_su_patrim = ifelse(is.na(BILA_debitifornit_su_patrim) & BILA_patr_su_patrml == 0 , 100, BILA_debitifornit_su_patrim))
        
        # replace everything else with median
        BILA_with_missing = df_final %>%
          select(starts_with('BILA_')) %>%
          summarise_all(funs(sum(is.na(.)))) %>%
          t() %>%
          as.data.frame() %>%
          filter(V1 > 0) %>%
          rownames()
        df_final = df_final %>%
          mutate_at(all_of(BILA_with_missing), funs(ifelse(is.na(.),median(., na.rm = TRUE),.)))
      }
    }
    
    # winsorize variables
    {
      winsor_level = 0.05
      var_to_winsorize = c('BILA_ARIC_VPROD', 'BILA_cashflow_valprod', 'BILA_IMM_VPROD', 'BILA_liquid_attivo', 'BILA_VAGG_VPROD', 'BILA_acid_test',
                           'BILA_ammor_su_costi', 'BILA_autonomia_finanz', 'BILA_debbanche_circolante', 'BILA_cashflow_ricavi', 'BILA_copertura_attivo',
                           'BILA_copertura_immob', 'BILA_copertura_finanz', 'BILA_costolavoro_ricavi', 'BILA_costolavoro_valagg', 'BILA_costolavoro_valprod',
                           'BILA_crediti_su_attivo', 'BILA_current_ratio', 'BILA_debbrevi_debbanche', 'BILA_debbrevi_patrim', 'BILA_totdebiti_su_debitibreve',
                           'BILA_totdebiti_su_patrim', 'BILA_debitifornit_su_patrim', 'BILA_debitifornit_su_totdebiti', 'BILA_durata_scorte',
                           'BILA_totdebiti_su_passivo', 'BILA_IMM_Patnetto', 'BILA_IMM_attivo', 'BILA_Mat_Patnetto', 'BILA_Mat_attivo',
                           'BILA_indebitamento_breve', 'BILA_indebitamento_ml', 'BILA_debbanche_passivo', 'BILA_dipend_finanz', 'BILA_liquid_imm',
                           'BILA_onerifinanz_mol', 'BILA_rigidita_impieghi', 'BILA_mol_ricavi', 'BILA_oneri_ricavi', 'BILA_oneri_valagg', 'BILA_patr_su_patrml',
                           'BILA_patr_su_patrrim', 'BILA_leverage', 'BILA_mol_valprod', 'BILA_rimanenze_attivo', 'BILA_riman_debbreve', 'BILA_riman_debml',
                           'BILA_rimanenze_debtot', 'BILA_rimanenze_debbanche', 'BILA_roa', 'BILA_rod', 'BILA_roe', 'BILA_roi', 'BILA_ros',
                           'BILA_rotazione_capitale', 'BILA_rotazione_circolante', 'BILA_rotazione_magazzino', 'BILA_turnover', 'BILA_valagg_fatturato',
                           'BILA_valprod_riman')
      
      winsorize = function(x, alpha){
        quant = quantile(x, c(alpha, 1-alpha))
        x[x < quant[1]] = quant[1]
        x[x > quant[2]] = quant[2]
        return(x) 
      }
      
      df_final = df_final %>%
        mutate_at(all_of(var_to_winsorize), ~winsorize(., winsor_level))
      
    }
    
    # evaluate statistics on recovered missing and winsorized dataset
    {
      write.table(basicStatistics(df_final) %>%
                    rowwise() %>%
                    mutate(VAR = strsplit(VARIABLE, '_')[[1]][2],
                           COD_FT_RAPPORTO = strsplit(VARIABLE, '_')[[1]][3]) %>%
                    left_join(var_descr, by = "VAR") %>%
                    left_join(forme_tecniche, by = "COD_FT_RAPPORTO") %>%
                    mutate(DESCRIPTION = ifelse(!is.na(FT_RAPPORTO), paste0(DESCRIPTION, ' - ', FT_RAPPORTO), DESCRIPTION)) %>%
                    select(VARIABLE, DESCRIPTION, everything(), -VAR, -COD_FT_RAPPORTO, -FT_RAPPORTO) %>%
                    mutate_all(as.character) %>%
                    replace(is.na(.), ''),
                  './Stats/12_df_final_stats_missing_winsor.csv', sep = ';', row.names = F, append = F)
      rm(var_descr, forme_tecniche)
    }
    
    if (save_flag){saveRDS(df_final, './Checkpoints/df_final.rds')}
  }
}
df_final = readRDS('./Checkpoints/df_final.rds')

# check if in CR sum of Banca == Sistema
{
  chiave_comune = readRDS('./Checkpoints/chiave_comune.rds')
  df_final = readRDS('./Checkpoints/df_final.rds')
  
  
  
  
  df_CR = readRDS('./Checkpoints/compact_data/compact_CR.rds')
  chiave_comune = readRDS('./Checkpoints/chiave_comune.rds')
  
  check_tot_chiave_comune = df_CR %>%
    select(abi, ndg) %>%
    unique() %>%
    left_join(chiave_comune %>% select(abi, ndg, chiave_comune), by = c("abi", "ndg")) %>%
    group_by(chiave_comune) %>%
    summarize(Count = n(), .groups = 'drop') %>%
    filter(!is.na(chiave_comune)) %>%
    left_join(chiave_comune %>%
                group_by(chiave_comune) %>%
                summarize(Expected_count = n(), .groups = 'drop'), by = "chiave_comune") %>%
    mutate(Complete_abi_ndg = ifelse(Count == Expected_count, 'YES', 'NO')) %>%
    select(-Count, -Expected_count)
  chiave_comune = chiave_comune %>%
    select(-Cartelle, -Folder_directory) %>%
    left_join(check_tot_chiave_comune, by = "chiave_comune")
  
  check_CR_sistema = df_CR %>%
    left_join(chiave_comune, by = c("abi", "ndg"))
  CR_cod_dato = read.csv("./Coding_tables/CR_COD_DATO.csv", sep=";", stringsAsFactors=FALSE) %>%
    mutate(CLS_DATO = as.character(CLS_DATO),
           DESCR = paste0(DESCRIZIONE, '-', FENOMENO)) %>%
    select(COD_DATO, RILEVAZIONE, DESCR) %>%
    filter(RILEVAZIONE != '') %>%
    filter(!COD_DATO %in% as.character(c(9789:9794, 9522))) %>%    # 9522 is available only for Banca
    mutate(COD_DATO = paste0('CR', COD_DATO))
  avail_CR = data.frame(COD_DATO = gsub('RAW_', '', check_CR_sistema %>% select(starts_with('RAW_')) %>% colnames()), stringsAsFactors = F) %>%
    left_join(CR_cod_dato, by = "COD_DATO") %>%
    group_by(DESCR) %>%
    summarise(COUNT = n(), .groups='drop') %>%
    filter(COUNT == 2)
  CR_cod_dato = CR_cod_dato %>%
    filter(DESCR %in% avail_CR$DESCR)
  if (uniqueN(CR_cod_dato$DESCR) != nrow(CR_cod_dato) / 2){cat('\n\n ######## check check_CR_sistema')}
  rm(chiave_comune, df_CR, check_tot_chiave_comune)

  # check_banca = check_CR_sistema %>%
  #   setNames(gsub('RAW_', '', names(.))) %>%
  #   select(Complete_abi_ndg, chiave_comune, year, month, all_of(CR_cod_dato %>% filter(RILEVAZIONE == 'Banca') %>% pull(COD_DATO))) %>%
  #   replace(is.na(.), 0) %>%
  #   group_by(Complete_abi_ndg, chiave_comune, year, month) %>%
  #   summarise_all(sum) %>%
  #   ungroup()
  # saveRDS(check_banca, './Checkpoints/aa_banca_full.rds')
  check_banca = readRDS('./Checkpoints/aa_banca_full.rds')
  
  
  # check_sistema_count = check_CR_sistema %>%
  #   setNames(gsub('RAW_', '', names(.))) %>%
  #   select(Complete_abi_ndg, chiave_comune, year, month, all_of(CR_cod_dato %>% filter(RILEVAZIONE == 'Sistema') %>% pull(COD_DATO))) %>%
  #   group_by(Complete_abi_ndg, chiave_comune, year, month) %>%
  #   summarise_all(function(x) uniqueN(x, na.rm = T)) %>%
  #   ungroup()
  # saveRDS(check_sistema_count, './Checkpoints/aa_sistema_count_full.rds')
  check_sistema_count = readRDS('./Checkpoints/aa_sistema_count_full.rds')
  if (max(check_sistema_count %>% select(-Complete_abi_ndg, -chiave_comune, -year, -month)) > 1){cat('\n\n ######## check_sistema_count has multiple values for same chiave_comune')}
  max_unique_values = check_sistema_count %>%
    mutate(cc = apply(., 1, function(x) max(x[5:22]))) %>%
    filter(cc > 1)
  max_unique_values %>% select(-Complete_abi_ndg, -chiave_comune, -year, -month) %>% summarise_all(max)
  
  # check_sistema = check_CR_sistema %>%
  #   setNames(gsub('RAW_', '', names(.))) %>%
  #   select(Complete_abi_ndg, chiave_comune, year, month, all_of(CR_cod_dato %>% filter(RILEVAZIONE == 'Sistema') %>% pull(COD_DATO))) %>%
  #   group_by(Complete_abi_ndg, chiave_comune, year, month) %>%
  #   summarise_all(function(x) max(x, na.rm = T)) %>%
  #   replace(. == -Inf, 0) %>%
  #   ungroup()
  # saveRDS(check_sistema, './Checkpoints/aa_sistema.rds')
  check_sistema = readRDS('./Checkpoints/aa_sistema.rds')
  
  
  
    
  
  
  
  
  
  
  
  
    
  check_CR_sistema = df_final %>% select(abi, ndg, chiave_comune, year, month, FLAG_Multibank, starts_with('RAW_CR'))
  CR_cod_dato = read.csv("./Coding_tables/CR_COD_DATO.csv", sep=";", stringsAsFactors=FALSE) %>%
    mutate(CLS_DATO = as.character(CLS_DATO),
           DESCR = paste0(DESCRIZIONE, '-', FENOMENO)) %>%
    select(COD_DATO, RILEVAZIONE, DESCR) %>%
    filter(RILEVAZIONE != '') %>%
    filter(!COD_DATO %in% as.character(c(9789:9794, 9522))) %>%    # 9522 is available only for Banca
    mutate(COD_DATO = paste0('CR', COD_DATO))
  avail_CR = data.frame(COD_DATO = gsub('RAW_', '', check_CR_sistema %>% select(starts_with('RAW_')) %>% colnames()), stringsAsFactors = F) %>%
    left_join(CR_cod_dato, by = "COD_DATO") %>%
    group_by(DESCR) %>%
    summarise(COUNT = n(), .groups='drop') %>%
    filter(COUNT == 2)
  CR_cod_dato = CR_cod_dato %>%
    filter(DESCR %in% avail_CR$DESCR)
  if (uniqueN(CR_cod_dato$DESCR) != nrow(CR_cod_dato) / 2){cat('\n\n ######## check check_CR_sistema')}
  
  
  # check_banca = check_CR_sistema %>%
  #   setNames(gsub('RAW_', '', names(.))) %>%
  #   select(chiave_comune, year, month, all_of(CR_cod_dato %>% filter(RILEVAZIONE == 'Banca') %>% pull(COD_DATO))) %>%
  #   replace(is.na(.), 0) %>%
  #   group_by(chiave_comune, year, month) %>%
  #   summarise_all(sum) %>%
  #   ungroup()
  # saveRDS(check_banca, './Checkpoints/aa_banca.rds')
  check_banca = readRDS('./Checkpoints/aa_banca.rds')
  
  
  # check_sistema_count = check_CR_sistema %>%
  #   setNames(gsub('RAW_', '', names(.))) %>%
  #   select(chiave_comune, year, month, all_of(CR_cod_dato %>% filter(RILEVAZIONE == 'Sistema') %>% pull(COD_DATO))) %>%
  #   group_by(chiave_comune, year, month) %>%
  #   summarise_all(function(x) uniqueN(x, na.rm = T)) %>%
  #   ungroup()
  # saveRDS(check_sistema_count, './Checkpoints/aa_sistema_count.rds')
  check_sistema_count = readRDS('./Checkpoints/aa_sistema_count.rds')
  if (max(check_sistema_count %>% select(-chiave_comune, -year, -month)) > 1){cat('\n\n ######## check_sistema_count has multiple values for same chiave_comune')}
  max_unique_values = check_sistema_count %>%
    mutate(cc = apply(., 1, function(x) max(x[4:22]))) %>%
    filter(cc > 1)
  max_unique_values %>% select(-chiave_comune, -year, -month) %>% summarise_all(max)
  
  # the only variable with more than one single value for each year-month is CR9520 - it is due to RILEVAZIONE switch with CR9521
  example = check_CR_sistema %>%
    select(-FLAG_Multibank) %>%
    filter(chiave_comune == '23657') %>%
    filter(year == 2014) %>%
    filter(month == 10) %>%
    gather(variable, value, -c(chiave_comune, abi, ndg, year, month), factor_key=F) %>%
    mutate(variable = gsub('RAW_', '', variable)) %>%
    left_join(CR_cod_dato, by = c('variable' = 'COD_DATO'))
  write.table(example, './Checks/05_sample_of_CR_multiple_values_sistema.csv', sep = ';', row.names = F, append = F)
  
  
  # check_sistema = check_CR_sistema %>%
  #   setNames(gsub('RAW_', '', names(.))) %>%
  #   select(chiave_comune, year, month, all_of(CR_cod_dato %>% filter(RILEVAZIONE == 'Sistema') %>% pull(COD_DATO))) %>%
  #   group_by(chiave_comune, year, month) %>%
  #   summarise_all(function(x) max(x, na.rm = T)) %>%
  #   replace(. == -Inf, 0) %>%
  #   ungroup()
  # saveRDS(check_sistema, './Checkpoints/aa_sistema.rds')
  check_sistema = readRDS('./Checkpoints/aa_sistema.rds')
  
  
  check_difference = check_banca %>%
    gather(variable_Banca, sum_value_Banca, -c(chiave_comune, year, month), factor_key=F) %>%
    left_join(CR_cod_dato %>% select(-RILEVAZIONE), by = c('variable_Banca' = 'COD_DATO')) %>%
    left_join(
      check_sistema %>%
        gather(variable_Sistema, value_Sistema, -c(chiave_comune, year, month), factor_key=F) %>%
        left_join(CR_cod_dato %>% select(-RILEVAZIONE), by = c('variable_Sistema' = 'COD_DATO')),
      by = c("chiave_comune", "year", "month", "DESCR")
    )
  if (nrow(check_sistema) * (ncol(check_sistema) - 3) != nrow(check_difference)){cat('\n\n ######## check_difference expected rows mismatch')}
  
  # check matching sistema and banca
  check_difference_same = check_difference %>%
    mutate(check = sum_value_Banca == value_Sistema) %>%
    group_by(chiave_comune, year, month) %>%
    summarize(all_same = sum(check) == n(), .groups = 'drop') %>%
    filter(all_same == TRUE)
  
  chiave = '105159'
  a_year = 2014
  a_month = 1
  
  example = check_CR_sistema %>%
    select(-FLAG_Multibank) %>%
    filter(chiave_comune == chiave) %>%
    filter(year == a_year) %>%
    filter(month == a_month) %>%
    gather(variable, value, -c(chiave_comune, abi, ndg, year, month), factor_key=F) %>%
    mutate(variable = gsub('RAW_', '', variable)) %>%
    left_join(CR_cod_dato, by = c('variable' = 'COD_DATO'))
  write.table(example, './Checks/05_sample_of_matching_CR_sistema_banca.csv', sep = ';', row.names = F, append = F)
  
  # check_difference_same_9520 = check_difference %>%
  #   mutate(check = sum_value_Banca == value_Sistema) %>%
  #   left_join(check_difference %>%
  #               filter(variable_Sistema == 'CR9520' & value_Sistema > 0) %>%
  #               filter(sum_value_Banca != value_Sistema) %>%
  #               select(chiave_comune, year, month) %>%
  #               unique() %>%
  #               mutate(keep = 'YES'), by = c("chiave_comune", "year", "month")) %>%
  #   filter(!is.na(keep)) %>%
  #   group_by(chiave_comune, year, month) %>%
  #   summarize(all_same = sum(check) == n(), .groups = 'drop') %>%
  #   filter(all_same == TRUE)
  # 
  # chiave = '105790'
  # a_year = 2014
  # a_month = 3
  # 
  # example = check_CR_sistema %>%
  #   select(-FLAG_Multibank) %>%
  #   filter(chiave_comune == chiave) %>%
  #   filter(year == a_year) %>%
  #   filter(month == a_month) %>%
  #   gather(variable, value, -c(chiave_comune, abi, ndg, year, month), factor_key=F) %>%
  #   mutate(variable = gsub('RAW_', '', variable)) %>%
  #   left_join(CR_cod_dato, by = c('variable' = 'COD_DATO')) %>%
  #   arrange(DESCR)
  
  # check mismatching sistema and banca
  check_difference_diff = check_difference %>%
    filter(sum_value_Banca != value_Sistema)
  summary_difference = check_difference_diff %>%
    group_by(DESCR, variable_Banca, variable_Sistema) %>%
    summarize(Year_month_Count = n(),
              Chiave_comune_Count = uniqueN(chiave_comune), .groups = 'drop')
  write.table(summary_difference, './Checks/05_summary_of_mismatching_CR_sistema_banca.csv', sep = ';', row.names = F, append = F)
  
  
  chiave = '10008'
  a_year = 2012
  a_month = 1
  
  example = check_CR_sistema %>%
    select(-FLAG_Multibank) %>%
    filter(chiave_comune == chiave) %>%
    filter(year == a_year) %>%
    filter(month == a_month) %>%
    gather(variable, value, -c(chiave_comune, abi, ndg, year, month), factor_key=F) %>%
    mutate(variable = gsub('RAW_', '', variable)) %>%
    left_join(CR_cod_dato, by = c('variable' = 'COD_DATO')) %>%
    arrange(DESCR)
  write.table(example, './Checks/05_sample_of_mismatching_CR_sistema_banca.csv', sep = ';', row.names = F, append = F)
  write.table(chiave_comune %>% filter(chiave_comune == chiave), './Checks/05_sample_of_mismatching_CR_sistema_banca_chiave_comune.csv',
              sep = ';', row.names = F, append = F)
}


# export to csv and dta
{
  library(haven)
  for (bila in unique(df_final$FLAG_BILANCIO)){
    tt = df_final %>% filter(FLAG_BILANCIO == bila)
    write_dta(tt, paste0(bila,'_mensile_2012_2014.dta'))
    write.table(tt, paste0(bila,'_mensile_2012_2014.csv'), sep = ',', row.names = F, append = F)
    rm(tt)
  }
  
  for (bila in unique(df_final$FLAG_BILANCIO)){
    tt = df_final %>%
      filter(FLAG_BILANCIO == bila) %>%
      select(abi, ndg, chiave_comune, year, month, segmento_CRIF, FLAG_Multibank, ANAG_ateco, ANAG_sae, ANAG_rae, ANAG_cab_residenza,
             ANAG_cod_provincia, ANAG_comune, ANAG_cab_comune, ANAG_flag_def, ANAG_tipo_ndg, ANAG_socio, Tot_Attivo_log10, Dimensione_Impresa,
             Regione, Regione_Macro, MAX_CC011, MAX_CC012, AVG_CC011, AVG_CC012, SUM_CC000, SUM_CC001, SUM_MURA005_CHIRO, SUM_MURA005_IPO,
             SUM_MURA006_CHIRO, SUM_MURA006_IPO, SUM_MURA008_CHIRO, SUM_MURA008_IPO, SUM_MURA009_CHIRO, SUM_MURA009_IPO, SUM_ANFA000, SUM_ANFA001,
             SUM_PCSBF000, SUM_PCSBF001, RAW_CR9501, RAW_CR9503, RAW_CR9505, RAW_CR9507, RAW_CR9509, RAW_CR9511, RAW_CR9513, RAW_CR9515, RAW_CR9517,
             RAW_CR9525, RAW_CR9527, RAW_CR9528, RAW_CR9529, RAW_I001, RAW_I002, RAW_I003, RAW_I004, RAW_I005, RAW_I006, RAW_I007, RAW_IO,
             RAW_PD_180, RAW_PD_90 , RAW_PR, RAW_SO, RAW_SS, ANAG_gg_sconf, ANAG_acc_cassa , ANAG_acc_firma, ANAG_accordato, ANAG_util_cassa,
             ANAG_util_firma, ANAG_utilizzato) %>%
      filter(year == 2013)
    write.table(tt, paste0(bila,'_mensile_2013.csv'), sep = ',', row.names = F, append = F)
    
    tt = df_final %>%
      filter(FLAG_BILANCIO == bila) %>%
      filter(year != 2014) %>%
      select(abi, ndg, chiave_comune, year, segmento_CRIF, FLAG_Multibank, Tot_Attivo_log10, Dimensione_Impresa, Regione, Regione_Macro,
             starts_with('BILA_')) %>%
      unique()
    write.table(tt, paste0(bila,'_Bilancio_2012-2013.csv'), sep = ',', row.names = F, append = F)
    rm(tt)
  }
  
}









# todo: controllare se gli NDG multipli nella stessa banca sono consecutivi (cambio di NDG nel tempo) o contemporanei. Controlla i file AI_CC
#       oppure possiamo controllare per ogni tipologia di file?

# todo: controlla quante aziende con bilancio 3 anni hanno effettivamente lavorato per i 3 anni con le banche (dagli andamentali interni).
#       si può fare una statistica generica su quanto sono popolati i dati panel per ogni anno



# todo: ha senso controllare se all'interno della stessa cartella gli NDG (o chiave comune) sono presenti in tutti i file o solo in alcuni?
#       magari si può calcolare quanti sono comuni e quanti non lo sono in ciascuna tipologia di file. Forse ha più senso farlo per chiave comune
#       così si può evitare il problema di NDG che cambiano nel tempo. Meglio, si può prendere l'unique di chiavi comuni per ogni tipologia 
#       di file (quindi considerando tutti i mesi insieme) e poi fare il confronto. Mi aspetterei che in anagrafica ci siano sempre tutti, in altri no 

