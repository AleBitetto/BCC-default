
library(readxl)
library(haven)
library(fBasics)
library(lubridate)
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
reload_perimeter = T
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
      df_final_reference = chiave_comune_perimeter %>%
        left_join(common_segmento, by = c("abi", "ndg")) %>%
        group_by(chiave_comune) %>%
        mutate(FLAG_Multibank = ifelse(uniqueN(abi) == 1, 0, 1)) %>%
        ungroup() %>%
        as.data.frame()
      saveRDS(df_final_reference, './Checkpoints/df_final_reference.rds')
      rm(segmento_list_CRIF, segmento_list_CSD, common_NDG)
    }
  }
  df_final_reference = readRDS('./Checkpoints/df_final_reference.rds')
  cat('\n -- Total NDG:', format(nrow(df_final_reference), big.mark=","))
  cat('\n -- Total chiave_comune:', format(uniqueN(df_final_reference$chiave_comune), big.mark=","))
  
  # add anagrafica from CRIF - yearly
  {
    decodifiche = read_dta("D:/UniPV data/BCC-default/Data/CRIF/decodifiche.dta") %>%
      as.data.frame(stringsAsFactors = F) %>%
      mutate_all(function(x) { attributes(x) <- NULL; x }) %>%
      mutate(abi = as.character(as.numeric(abi))) %>%
      filter(cod_tipo_ndg != '') %>%
      select(abi, cod_tipo_ndg, tipo_ndg) %>%
      rename(tipo_ndg_RECOVER = tipo_ndg) %>%
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
    saveRDS(df_anag, './Checkpoints/df_anag.rds')
  }
  df_anag = readRDS('./Checkpoints/df_anag.rds')
  
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
      saveRDS(df_bilanci, './Checkpoints/df_bilanci.rds')
  }
  df_bilanci = readRDS('./Checkpoints/df_bilanci.rds')
  
  # add files from CSD - monthly and create report
  {
    file_cat = c('AI_CC', 'AI_PCSBF', 'AI_ANFA', 'AI_POFI', 'AI_MURA', 'AI_APSE', 'AI_CRFI', 'RISCHIO')
    
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

# todo: controlla in quanti casi ho o solo il NAG o solo il NAG_COLLEGATO con la chiave_comune (potremmo estenderla a quello non censito con la chiave_comune)

# todo: controllare se gli NDG multipli nella stessa banca sono consecutivi (cambio di NDG nel tempo) o contemporanei. Controlla i file AI_CC
#       oppure possiamo controllare per ogni tipologia di file?








# todo: aggancia tutti i dati CRIF evidenziati in 'Contenuto BD Crif...'
#       aggancia tutti i dati CSD evidenziati in 'Legenda flussi AI' e 'CR' (bisogna capire come agganciare la colonna COD_PRODOTTO)




# todo: in bilancio e anagrafica ci sono dei casi in cui l'informazione è diversa in base all'anno e la possiamo agganciare puntualmente.
#       cosa facciamo con quelli che hanno l'informazione solo per un anno? la riportiamo uguale anche per gli anni mancanti?
#       tagliamo fuori il 2012? ciò potrebbe rendere mancanti dei dati. E se ho 2 anni su 3, quale uso per recuperare il mancante?
# todo: per anagrafica, spalma dati disponibili, per bilancio, lascia quelli presenti (magari crea un flag dove per abi+ndg c'è il conteggio degli anni presenti)



# todo: crea report numero di abi+ndg agganciati + numero di anni presenti (per bilancio)

# todo: controlla quante aziende con bilancio 3 anni hanno effettivamente lavorato per i 3 anni con le banche (dagli andamentali interni).
#       si può fare una statistica generica su quanto sono popolati i dati panel per ogni anno

# todo: fai un censimento di tutte le forme tecniche (conteggio totale) per ciascun tipo di file, contando anche quante righe ci sono a parità di abi+ndg






# todo: ha senso controllare se all'interno della stessa cartella gli NDG (o chiave comune) sono presenti in tutti i file o solo in alcuni?
#       magari si può calcolare quanti sono comuni e quanti non lo sono in ciascuna tipologia di file. Forse ha più senso farlo per chiave comune
#       così si può evitare il problema di NDG che cambiano nel tempo. Meglio, si può prendere l'unique di chiavi comuni per ogni tipologia 
#       di file (quindi considerando tutti i mesi insieme) e poi fare il confronto. Mi aspetterei che in anagrafica ci siano sempre tutti, in altri no 

# todo: controlla come agganciare l'anagrafica    <<-------
#       per ogni coppia abi+ndg ci sono più righe relative a date differenti. Prendiamo la più recente? A volte la data più recente ha dei missing rispetto
#       alle altre (e.s. filter(abi == "8354" & ndg == "0000000000000470") )

# le colonne X in BILDATI corrispondono a CEBI (la X la aggiunge R)

# todo: dopo aver creato il data_loader crea lista di tutte le colonne disponibili con minime statistiche (min/max, uniqueN, alcuni esempi)
#       ha senso fare un controllo sugli NDG per confrontarli in chiave_comune?


# todo: 3599 dovrebbe essere esclusa dall'analisi finale
