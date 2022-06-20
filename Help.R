
# file dimension, missing, blanks and columns format
file_stats = function(x){
  file = read.csv(x, sep=';', stringsAsFactors = F)
  dim = dim(file)
  missing = 0
  missing_perc = 0
  blank = 0
  blank_perc = 0
  num_col = 0
  char_col = 0
  if (dim[1] > 0){
    missing = sum(is.na(file))
    missing_perc = round(missing / prod(dim) * 100)
    blank = sum(file == '', na.rm = T)
    blank_perc = round(blank / prod(dim) * 100)
    char_col = length(which(sapply(file, is.character)))
  }
  rm(file)
  
  return(data.frame(rows = dim[1],
                    cols = dim[2],
                    numeric_cols = dim[2] - char_col,
                    string_cols = char_col,
                    missing = missing,
                    blank = blank,
                    missing_perc = missing_perc,
                    blank_perc = blank_perc)
  )
}

# aggregation functions for AI
data_aggregate_CC = function(data){
  
  col_sum = c('CC000', 'CC001',	'CC002', 'CC100',	'CC101', 'CC102',	'CC003', 'CC004',	'CC005', 'CC006',	'CC007', 'CC008',	'CC009', 'CC010')
  col_avg = c('CC011','CC012')
  col_max = c('CC011','CC012')
  
  data_sum = data %>%
    group_by(abi, ndg) %>%
    summarize_at(col_sum, sum, na.rm = T) %>%
    ungroup() %>%
    setNames(c('abi', 'ndg', paste0('SUM_', col_sum)))
  
  data_avg = data %>%
    select(c('abi', 'ndg', all_of(col_avg))) %>%
    mutate_at(col_avg, ~replace(., . == 0, NA)) %>%    # mean on non-zero
    group_by(abi, ndg) %>%
    summarize_at(col_avg, mean, na.rm = T) %>%
    ungroup() %>%
    mutate_at(col_avg, ~replace(., is.na(.), 0)) %>%
    setNames(c('abi', 'ndg', paste0('AVG_', col_avg)))
  
  data_max = data %>%
    group_by(abi, ndg) %>%
    summarize_at(col_max, max, na.rm = T) %>%
    ungroup() %>%
    setNames(c('abi', 'ndg', paste0('MAX_', col_max)))
  
  data_compact = data_sum %>%
    left_join(data_avg, by = c("abi", "ndg")) %>%
    left_join(data_max, by = c("abi", "ndg"))
  
  return(data_compact)
}

data_aggregate_PCSBF = function(data){
  
  col_sum = c('PCSBF000',	'PCSBF001',	'PCSBF002',	'PCSBF003',	'PCSBF004',	'PCSBF005',	'PCSBF006',	'PCSBF007',	'PCSBF008',	'PCSBF009',
              'PCSBF010',	'PCSBF011',	'PCSBF012',	'PCSBF013',	'PCSBF014',	'PCSBF015',	'PCSBF017',	'PCSBF016',	'PCSBF018',	'PCSBF019')
  
  data_compact = data %>%
    group_by(abi, ndg) %>%
    summarize_at(col_sum, sum, na.rm = T) %>%
    ungroup() %>%
    setNames(c('abi', 'ndg', paste0('SUM_', col_sum)))
  
  return(data_compact)
}

data_aggregate_ANFA = function(data){
  
  col_sum = c('ANFA000', 'ANFA001', 'ANFA002', 'ANFA003',	'ANFA004', 'ANFA005', 'ANFA006', 'ANFA007', 'ANFA008', 'ANFA009',
              'ANFA010', 'ANFA011',	'ANFA012', 'ANFA013',	'ANFA014', 'ANFA015',	'ANFA017', 'ANFA016',	'ANFA018', 'ANFA019')
  
  data_compact = data %>%
    group_by(abi, ndg) %>%
    summarize_at(col_sum, sum, na.rm = T) %>%
    ungroup() %>%
    setNames(c('abi', 'ndg', paste0('SUM_', col_sum)))
  
  return(data_compact)
}

data_aggregate_POFI = function(data){
  
  col_sum = c('POFI000', 'POFI001',	'POFI002', 'POFI003',	'POFI004', 'POFI005',	'POFI006', 'POFI007')
  
  data_sum = data %>%
    group_by(abi, ndg) %>%
    summarize_at(col_sum, sum, na.rm = T) %>%
    ungroup() %>%
    setNames(c('abi', 'ndg', paste0('SUM_', col_sum)))
  
  data_FT = data %>%
    mutate(COD_FT_RAPPORTO = gsub('\'', '', COD_FT_RAPPORTO)) %>%
    group_by(abi, ndg) %>%
    summarize(DUMMY_POFI_AGR = ifelse('234140' %in% COD_FT_RAPPORTO, 1, 0),
              DUMMY_POFI_FIN = ifelse('234130' %in% COD_FT_RAPPORTO, 1, 0),
              DUMMY_POFI_ART = ifelse('234142' %in% COD_FT_RAPPORTO, 1, 0), .groups = 'drop') %>%
    ungroup()
  
  data_compact = data_sum %>%
    left_join(data_FT, by = c("abi", "ndg"))
  
  return(data_compact)
}

data_aggregate_APSE = function(data){
  
  col_single_sum = c('APSE000', 'APSE001', 'APSE_NUMRAP')
  col_single_avg = c('APSE002', 'APSE003', 'APSE_1854', 'APSE004')
  col_macro_sum = c('APSE005', 'APSE006',	'APSE007', 'APSE008',	'APSE009', 'APSE010',	'APSE011', 'APSE012',	'APSE013', 'APSE014',	'APSE015', 'APSE016')
  FT = data.frame(FT = c('E11910', 'EST050', 'D11910', 'E11916', 'EST054', 'D11916'),
                  MACRO = c(rep('EXPORT', 3), rep('IMPORT', 3)), stringsAsFactors = F)
  
  data = data %>%
    mutate(COD_FT_RAPPORTO = gsub('\'', '', COD_FT_RAPPORTO)) %>%
    rename(APSE1854 = APSE_1854,
           APSENUMRAP = APSE_NUMRAP)
  
  data_sum = data %>%
    select(abi, ndg) %>%
    unique()
  data_avg = data_sum
  data_macro = data_sum
  for (ft in FT$FT){
    
    data_sum = data_sum %>%
      left_join(
        data %>%
          filter(COD_FT_RAPPORTO == ft) %>%
          group_by(abi, ndg) %>%
          summarize_at(col_single_sum, sum, na.rm = T) %>%
          ungroup() %>%
          setNames(c('abi', 'ndg', paste0('SUM_', col_single_sum, '_', ft))),
        by = c("abi", "ndg")
      )
    
    data_avg = data_avg %>%
      left_join(
        data %>%
          filter(COD_FT_RAPPORTO == ft) %>%
          select(c('abi', 'ndg', all_of(col_single_avg))) %>%
          mutate_at(col_single_avg, ~replace(., . == 0, NA)) %>%    # mean on non-zero
          group_by(abi, ndg) %>%
          summarize_at(col_single_avg, mean, na.rm = T) %>%
          ungroup() %>%
          mutate_at(col_single_avg, ~replace(., is.na(.), 0)) %>%
          setNames(c('abi', 'ndg', paste0('AVG_', col_single_avg, '_', ft))),
        by = c("abi", "ndg")
      )
    
  }
  data_sum = data_sum %>% mutate_all(~replace(., is.na(.), 0))
  data_avg = data_avg %>% mutate_all(~replace(., is.na(.), 0))
  
  for (macro in unique(FT$MACRO)){
    
    data_macro = data_macro %>%
      left_join(
        data %>%
          filter(COD_FT_RAPPORTO %in% (FT %>% filter(MACRO == macro) %>% pull(FT))) %>%
          group_by(abi, ndg) %>%
          summarize_at(col_macro_sum, sum, na.rm = T) %>%
          ungroup() %>%
          setNames(c('abi', 'ndg', paste0('SUM_', col_macro_sum, '_', macro))),
        by = c("abi", "ndg")
      )
  }
  data_macro = data_macro %>% mutate_all(~replace(., is.na(.), 0))
  
  data_compact = data_sum %>%
    left_join(data_avg, by = c("abi", "ndg")) %>%
    left_join(data_macro, by = c("abi", "ndg"))
  
  return(data_compact)
}

data_aggregate_CRFI = function(data){
  
  col_sum = c('CRFI000', 'CRFI001', 'CRFI002', 'CRFI003')
  
  data_compact = data %>%
    group_by(abi, ndg, COD_FT_RAPPORTO) %>%
    summarize_at(col_sum, sum, na.rm = T) %>%
    ungroup() %>%
    setNames(c('abi', 'ndg', 'COD_FT_RAPPORTO', paste0('SUM_', col_sum)))
  
  return(data_compact)
}

data_aggregate_MURA = function(data){
  
  col_sum = c('MURA000', 'MURA001',	'MURA002', 'MURA003',	'MURA004', 'MURA005',	'MURA006', 'MURA007',	'MURA008', 'MURA009')
  FT = data.frame(COD_FT_RAPPORTO = c('99ML04', '99ML03', '99MM02', '99ML02', '99MM01', '99ML01',
                                      '99MC51', '99MC50', '99MB01', '99MB02', '99MC99', '99MM04', '99MM03', '99MB03', '99MB04'),
                  MACRO_FT = c(rep('IPO', 2), rep('CHIRO', 4), rep('OTHER', 9)), stringsAsFactors = F)
  
  data = data %>%
    mutate(COD_FT_RAPPORTO = gsub('\'', '', COD_FT_RAPPORTO)) %>%
    mutate(MURA000 = as.numeric(gsub('\'', '', MURA000))) %>%
    mutate(MURA001 = as.numeric(gsub('\'', '', MURA001))) %>%
    left_join(FT, by = "COD_FT_RAPPORTO") %>%
    mutate(MACRO_KEEP = ifelse(MACRO_FT == 'OTHER', 'REMOVE', 'KEEP'))
  
  data_compact = data %>%
    group_by(abi, ndg, MACRO_KEEP) %>%
    summarize(TOTAL_ROW = n(), .groups = 'drop')
  for (macro in unique(FT$MACRO_FT)){
    
    data_compact = data_compact %>%
      left_join(
        data %>%
          filter(MACRO_FT == macro) %>%
          group_by(abi, ndg, MACRO_KEEP) %>%
          summarize_at(col_sum, sum, na.rm = T) %>%
          ungroup() %>%
          setNames(c('abi', 'ndg', 'MACRO_KEEP', paste0('SUM_', col_sum, '_', macro))),
        by = c("abi", "ndg", "MACRO_KEEP")
      )
  }
  data_compact = data_compact %>% mutate_all(~replace(., is.na(.), 0))
  if (abs(sum(data %>% select(all_of(col_sum)), na.rm=T) - sum(data_compact[,-c(1:4)], na.rm=T)) > 1e-5){cat('\n mismatch in total sum\n')}
  
  return(data_compact)
}

data_aggregate_RISCHIO = function(data){
  
  col_num = c('I001',	'I002',	'I003',	'I004',	'I005',	'I006',	'I007')
  col_char = c('PD_180', 'PD_90', 'IO', 'PR', 'SS', 'SO')
  
  data_compact = data %>%
    mutate_at(col_char, ~replace(.,. == '\'N', 0)) %>%
    mutate_at(col_char, ~replace(.,. == '\'S', 1)) %>%
    mutate_at(col_num, ~replace(., is.na(.), 0)) %>%
    mutate_at(col_char, as.numeric) %>%
    select('abi', 'ndg', all_of(c(col_num, col_char))) %>%
    setNames(c('abi', 'ndg', paste0('RAW_', c(col_num, col_char)))) 
  
  return(data_compact)
}

data_aggregate_CR = function(data){
  
  data_compact = data %>%
    select(-COD_FT_RAPPORTO, -DATA_RIFERIMENTO) %>%
    mutate(COD_DATO = gsub('\'', '', COD_DATO),
           COD_PRODOTTO = gsub(' ', '', COD_PRODOTTO),
           COD_DATO = paste0('RAW_CR', COD_DATO)) %>%
    filter(COD_PRODOTTO == '\'CR') %>%
    select(-COD_PRODOTTO) %>%
    # bind_rows(data.frame(abi='8542', ndg = '0000000000000030', COD_DATO = 'RAW_CR9513', VALORE = -11, stringsAsFactors = )) %>%
    setDT() %>%
    dcast(abi + ndg ~ COD_DATO, value.var='VALORE')
  
  return(data_compact)
}


# create aggregated data and report for CSD
create_aggregated_data = function(df_final_reference, file_type, save_report = F){
  
  cat('\n---------------------------------- ', file_type, ' ----------------------------------\n')
  summary_file_rows = readRDS('./Checkpoints/summary_file_rows.rds')
  forme_tecniche = read.csv2('./Coding_tables/Forme_tecniche.csv', stringsAsFactors=FALSE) %>%
    mutate(COD_FT_RAPPORTO = paste0('\'', COD_FT_RAPPORTO))
  
  file_to_load = summary_file_rows %>%
    filter(unique_file_name == paste0(file_type, '.CSV')) %>%
    filter(matched_folder == 'YES') %>%
    filter(rows > 0) %>%
    mutate(path = paste0('./Data/CSD/', main_folder, '/', file))
  if (max(file_to_load$cols) != min(file_to_load$cols)){cat('\n\n ###### error in', unique(file_to_load$unique_file_name), ': columns mismatch')}
  
  # select aggregation function
  if (file_type == 'AI_CC'){
    aggregation_function = data_aggregate_CC
  } else if (file_type == 'AI_PCSBF'){
    aggregation_function = data_aggregate_PCSBF
  } else if (file_type == 'AI_ANFA'){
    aggregation_function = data_aggregate_ANFA
  } else if (file_type == 'AI_POFI'){
    aggregation_function = data_aggregate_POFI
  } else if (file_type == 'AI_MURA'){
    aggregation_function = data_aggregate_MURA
  } else if (file_type == 'AI_APSE'){
    aggregation_function = data_aggregate_APSE
  } else if (file_type == 'AI_CRFI'){
    aggregation_function = data_aggregate_CRFI
  } else if (file_type == 'RISCHIO'){
    aggregation_function = data_aggregate_RISCHIO
  } else if (file_type == 'CR'){
    aggregation_function = data_aggregate_CR
  }
  
  report_out = c()
  data_compact_list = list()
  data_compact_out = list()
  report_row_count = 0
  list_ind = 0
  compact_row_count = 0
  start_time = Sys.time()
  tot_i = nrow(file_to_load)
  for (i in 1:tot_i){
    
    cat(i, '/', tot_i, end = '\r')
    path = file_to_load$path[i]
    bank_abi = file_to_load$Abi[i]
    file = file_to_load$file[i]
    
    # read and format data
    data = df_final_reference %>%
      select(abi, ndg) %>%
      filter(abi == bank_abi) %>%
      left_join(
        read.csv2(path, stringsAsFactors=FALSE) %>%
          mutate(COD_UO = '.') %>%    # used only for RISCHIO.CSV and CR.CSV
          select(-COD_ABI, -COD_UO) %>%
          mutate(COD_NAG = gsub('\'', '', COD_NAG),
                 abi = bank_abi) %>%
          rename(ndg = COD_NAG), by = c("abi", "ndg")) %>%
      filter(!is.na(DATA_RIFERIMENTO)) %>%
      mutate(DATA_RIFERIMENTO = as.Date(gsub('\'', '', DATA_RIFERIMENTO), format = '%Y%m%d'))
    if (!file_type %in% c('RISCHIO', 'CR')){
      data = data %>% left_join(forme_tecniche, by = "COD_FT_RAPPORTO")
    } else {
      data$COD_FT_RAPPORTO = forme_tecniche$COD_FT_RAPPORTO[1]   # random one - used to run report anyway
      data$FT_RAPPORTO = 'cc'   # random one
    }
    
    # prepare data for report
    if (save_report){
      if (sum(!unique(data$COD_FT_RAPPORTO) %in% forme_tecniche$COD_FT_RAPPORTO) > 0){cat('\n', i, '|', file ,'- some COD_FT_RAPPORTO not found', end = '\n')}
      
      report = data %>%
        group_by(ndg, COD_FT_RAPPORTO) %>%
        summarize(Rows = n(), .groups = 'drop') %>%
        ungroup() %>%
        group_by(COD_FT_RAPPORTO, Rows) %>%
        summarize(Total_NDG = n(), .groups = 'drop')
      
      report_out = report_out %>%
        bind_rows(report)
      report_row_count = report_row_count + nrow(data)
    }
    
    # prepare data to be saved
    data = data %>%
      select(-FT_RAPPORTO)
    ref_date = unique(data$DATA_RIFERIMENTO)
    if (length(ref_date) > 1){cat('\n', i, '|', file ,'- multiple reference date found', end = '\n')}
    
    # aggregate data
    data_compact = aggregation_function(data)
    
    # add year and month and
    data_compact = data_compact %>%
      mutate(year = year(ref_date),
             month = month(ref_date)) %>%
      select(abi, ndg, year, month, everything())
    compact_row_count = compact_row_count + nrow(data_compact)
    list_ind = list_ind + 1
    data_compact_list[[list_ind]] = data_compact
    
    if (i %% 100 == 0 | i == tot_i){
      data_compact_out = rbindlist(c(list(data_compact_out), data_compact_list), fill=TRUE) %>% as.data.frame()
      data_compact_list = list()
      list_ind = 0
    }
    
  } # i
  
  # save aggregated data
  if (data_compact_out %>% select(abi, ndg, year, month) %>% uniqueN() != nrow(data_compact_out)){
    cat('\n\n ###### error in unique records in data_compact_out')
  }
  saveRDS(data_compact_out, paste0('./Checkpoints/compact_data/compact_', file_type, '.rds'))
  
  # save report data
  if (save_report){
    report_out = report_out %>%
      group_by(COD_FT_RAPPORTO, Rows) %>%
      summarize(Total_NDG = sum(Total_NDG), .groups = 'drop') %>%
      mutate(File = file_type) %>%
      mutate(Total_rows = report_row_count) %>%
      select(File, everything())
    
    if (sum(report_out$Rows * report_out$Total_NDG) != report_row_count){
      cat('\n\n ###### error in report_out')
    } else {
      saveRDS(report_out, paste0('./Checkpoints/report_data/report_', file_type, '.rds'))
    }
  }
  
  tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
  cat('\n Total elapsed time:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
}

# Statistical analysis: numerical, data and character columns
basicStatistics = function(data){
  
  data=as.data.frame(data, stringsAsFactors = F)
  
  # Get numerical columns
  nums <- names(which(sapply(data, is.numeric)))
  if (length(nums)>0){
    StatNum = c()
    for (col in nums){
      dd = data[, col]
      perc = quantile(dd, c(0.01, 0.05, 0.95, 0.99), na.rm = T)
      StatNum = StatNum %>% bind_rows(
        data.frame(UNIQUE_VALS = uniqueN(dd),
                   NAs = sum(is.na(dd)),
                   Min = min(dd, na.rm = T),
                   Max = max(dd, na.rm = T),
                   Mean = mean(dd, na.rm = T),
                   StDev = sd(dd, na.rm = T),
                   Median = median(dd, na.rm = T),
                   Perc_1 = perc[1],
                   Perc_5 = perc[2],
                   Perc_95 = perc[3],
                   Perc_99 = perc[4],
                   Sum = sum(dd, na.rm = T), stringsAsFactors = F)
      )
    }
    StatNum = StatNum %>%
      mutate(NAs = paste0(NAs,' (',signif(NAs/nrow(data)*100,digits=2),'%)')) %>%
      mutate_all(as.character) %>%
      `rownames<-`(nums)
  } else {StatNum=data.frame()}
  
  # Get dates columns
  dates <- names(which(sapply(data, lubridate::is.Date)))
  if (length(dates)>0){
    StatDat = c()
    for (col in dates){
      dd = data[, col]
      StatDat = StatDat %>% bind_rows(
        data.frame(UNIQUE_VALS = uniqueN(dd),
                   NAs = sum(is.na(dd)),
                   Min = min(dd, na.rm = T),
                   Max = max(dd, na.rm = T),
                   Median = median(dd, na.rm = T), stringsAsFactors = F) %>%
          bind_cols(
            data.frame(val = sort(dd), stringsAsFactors = F) %>%
              filter(!is.na(val)) %>%
              mutate(POS = 1:n()) %>%
              filter(POS %in% round(n() * c(0.01, 0.05, 0.95, 0.99))) %>%
              mutate(POS = paste0('Perc_', c(1, 5, 95, 99))) %>%
              mutate(group = 1) %>%
              spread(POS, val) %>%
              select(-group)
          )
      )
    }
    StatDat = StatDat %>%
      mutate(NAs = paste0(NAs,' (',signif(NAs/nrow(data)*100,digits=2),'%)')) %>%
      mutate_all(as.character)
    rownames(StatDat) = dates
  } else {StatDat=data.frame()}
  
  # Get characters columns
  chars <- names(which(sapply(data, is.character)))
  if (length(chars)>0){
    StatChar = c()
    for (i in chars){
      cc=data[,i];cc=cc[!is.na(cc)]
      if (length(unique(cc)) <= 50){line = paste(unique(cc), collapse = "|")
      } else {line = paste("> 50 unique values",paste(unique(cc)[1:20], collapse = "|"))}
      StatChar = rbind(StatChar,c(paste0(nrow(data)-length(cc),' (',signif((nrow(data)-length(cc))/nrow(data)*100,digits=2),'%)'),
                                  paste0(sum(cc == ''),' (',signif(sum(cc == '')/nrow(data)*100,digits=2),'%)'),
                                  length(unique(cc)),line))
    }
    if (length(StatChar)>0){StatChar = data.frame(StatChar, stringsAsFactors = F) %>% setNames(c('NAs', 'BLANKs', 'UNIQUE_VALS','VALUES'));rownames(StatChar)=chars}
  } else {StatChar=data.frame()}
  
  Stat=bind_rows(StatNum,StatDat,StatChar)
  
  Stat=cbind(VARIABLE=c(rownames(StatNum),rownames(StatDat),rownames(StatChar)),
             TYPE=c(rep('NUMERIC',length(nums)),rep('DATE',length(dates)),rep('CHARACTER',length(chars))),
             NUM_OSS=nrow(data),Stat)
  col_first = intersect(c('VARIABLE', 'TYPE', 'NUM_OSS', 'UNIQUE_VALS', 'NAs', 'BLANKs'), colnames(Stat))
  Stat = Stat %>% select(all_of(col_first), everything())
  
  final=data.frame(VARIABLE=colnames(data), stringsAsFactors = F)
  final = final %>% left_join(Stat, by = "VARIABLE")
  final = as.matrix(final)
  final[is.na(final)]=''
  final = as.data.frame(final, stringsAsFactors = F)
  
  return(final)
}

# evaluate correlation and partial correlation
evaluate_correlation = function(df, corr_method = 'pearson'){
  # df: data.frame of variables
  
  corr = rcorr(df %>% as.matrix(), type = corr_method)
  corr_v = corr$r
  corr_v[lower.tri(corr_v, diag = T)] = NA
  corr_v = reshape2::melt(corr_v) %>% filter(!is.na(value)) %>% setNames(c('Var1', 'Var2', 'Corr')) %>% mutate_if(is.factor, as.character)
  corr_p = round(corr$P, 8)
  corr_p[lower.tri(corr_p, diag = T)] = NA
  corr_p = reshape2::melt(corr_p) %>% filter(!is.na(value)) %>% setNames(c('Var1', 'Var2', 'Corr_pVal')) %>% mutate_if(is.factor, as.character)
  
  pcorr = suppressWarnings(pcor(df, method = corr_method))
  pcorr_v = pcorr$estimate
  rownames(pcorr_v) = colnames(pcorr_v) = colnames(df)
  pcorr_v[lower.tri(pcorr_v, diag = T)] = NA
  pcorr_v = reshape2::melt(pcorr_v) %>% filter(!is.na(value)) %>% setNames(c('Var1', 'Var2', 'PartCorr')) %>% mutate_if(is.factor, as.character)
  pcorr_p = round(pcorr$p.value, 8)
  rownames(pcorr_p) = colnames(pcorr_p) = colnames(df)
  pcorr_p[lower.tri(pcorr_p, diag = T)] = NA
  pcorr_p = reshape2::melt(pcorr_p) %>% filter(!is.na(value)) %>% setNames(c('Var1', 'Var2', 'PartCorr_pVal')) %>% mutate_if(is.factor, as.character)
  
  correlation_list = corr_v %>%
    left_join(corr_p, by = c("Var1", "Var2")) %>%
    left_join(pcorr_v, by = c("Var1", "Var2")) %>%
    left_join(pcorr_p, by = c("Var1", "Var2")) %>%
    mutate(abs = abs(PartCorr)) %>%
    arrange(desc(abs))
  
  return(correlation_list)
}

# PCA output
pca_fun = function(slice, k_fold, cv, method_title){
  pca = prcomp(slice,
               center = F,
               scale = F)
  
  # find optimal number of PC
  PC_opt = c(PC_CV = ncol(slice))
  if(cv){
    PC_opt = cv_pca(slice, func = pca_fun, k_fold)
  }
  pca$PC_opt = PC_opt
  
  load = pca$rotation
  summ = summary(pca)
  scree_plot = fviz_eig_mod(pca, addlabels = T, pc_to_highlight = min(PC_opt))
  scree_plot$labels$title = paste0(method_title, ' - ', scree_plot$labels$title)
  load_plot = factoextra::fviz_pca_var(pca,
                                       col.var = "contrib", # Color by contributions to the PC
                                       gradient.cols = c("#00AFBB", "#E7B800", "#FC4E07"),
                                       repel = TRUE     # Avoid text overlapping
  )
  load_plot$labels$title = paste0(method_title, ' - ', load_plot$labels$title)
  bi_plot = factoextra::fviz_pca_biplot(pca, repel = TRUE,
                                        col.var = "#2E9FDF", # Variables color
                                        col.ind = "#696969"  # Individuals color
  )
  bi_plot$labels$title = paste0(method_title, ' - ', bi_plot$labels$title)
  load_table = data.frame(variable = rep(rownames(load), ncol(load)),
                          PC = rep(c(1:ncol(load)), rep(nrow(load), ncol(load))),
                          loading = as.numeric(load), stringsAsFactors = F)
  importance_table = data.frame(PC = colnames(summ$importance), stringsAsFactors = F) %>%
    cbind(as.data.frame(t(summ$importance)[, -1], stringsAsFactors = F))
  return(list(pca = pca,
              load_table = load_table,
              importance_table = importance_table,
              scree_plot = scree_plot,
              load_plot = load_plot,
              bi_plot = bi_plot))
}

# PC number selection
cv_pca = function(slice, func, k_fold){
  
  # Wold Cross Validation - https://www.researchgate.net/publication/5638191_Cross-validation_of_component_models_A_critical_look_at_current_methods
  
  n = nrow(slice)
  max_pc = min(dim(slice))
  set.seed(10)
  cv_ind = caret::createFolds(c(1:n), k = k_fold, list = TRUE, returnTrain = FALSE)
  max_pc_fold = min(c(n - max(unlist(lapply(cv_ind,length))), max_pc))
  cv_err = c()
  for (fold in 1:length(cv_ind)){
    cal_set = slice[setdiff(1:n, cv_ind[[fold]]), ] %>% as.matrix()
    val_set = slice[cv_ind[[fold]], ] %>% as.matrix()
    cv_est = func(slice = cal_set, k_fold = NULL, cv = F, method_title = '')
    
    for (pc in 1:(max_pc_fold - 1)){
      load_pc = cv_est$pca$rotation[, 1:pc]
      score_pc = val_set %*% load_pc
      val_set_approx = score_pc %*% t(load_pc)
      cv_err = cv_err %>% rbind(c(FOLD = fold, PC = pc, ERR = sum((val_set - val_set_approx) ^ 2)))
      
    }
  }
  tot_var = sum(slice ^ 2)
  cv_err_avg = cv_err %>% as.data.frame() %>%
    group_by(PC) %>%
    summarise(R = sum(ERR) / tot_var, .groups = "drop") %>%
    filter(R < 1) %>%
    arrange(desc(PC))
  pc_opt_wold = cv_err_avg$PC[1]
  
  
  # Malinowski error (REV - Reduced EigenValue)
  
  F_signif = 0.1
  
  eval_pca = func(slice, k_fold = NULL, cv = F, method_title = '')$pca
  eig = eval_pca$sdev^2
  # rev_l = c()
  # for (pc in 1:(max_pc - 1)){
  #  den = 0
  #  for (m in (pc + 1):max_pc){
  #    den = den + eig[m] / ((max_pc - m + 1) * (n - m + 1))
  #  }
  #  F_ratio = (eig[pc] / ((max_pc - pc + 1) * (n - pc + 1))) / den
  #  F_quant = qf(F_signif, 1, n - pc, lower.tail = TRUE, log.p = FALSE)
  #  rev_l = c(rev_l, ifelse(F_ratio < F_quant, 0, 1))
  # }
  REV = rev_l = c()
  for (pc in 1:(max_pc - 1)){
    REV = c(REV, eig[pc] / ((max_pc - pc + 1) * (n - pc + 1)))
  }
  for (pc in 1:(max_pc - 2)){
    F_ratio = (n - pc) * REV[pc] / sum(REV[(pc + 1):(max_pc - 1)])
    F_quant = qf(F_signif, 1, n - pc, lower.tail = TRUE, log.p = FALSE)
    rev_l = c(rev_l, ifelse(F_ratio < F_quant, 0, 1))
  }
  pc_opt_REV =  suppressWarnings(min(which(rev_l == 0)) - 1)
  if (!is.finite(pc_opt_REV)){pc_opt_REV = max_pc - 1}
  
  
  # Q-stat
  
  Q_signif = 0.1
  
  Q_val = c()
  load = eval_pca$rotation
  for (pc in 1:(max_pc - 1)){
    r = (diag(max_pc) - load[1:max_pc, 1:pc] %*% t(load[1:max_pc, 1:pc])) %*% t(eval_pca$x)
    Q = diag(t(r) %*% r)
    T1 = sum(eig[(pc + 1):max_pc])
    T2 = sum(eig[(pc + 1):max_pc] ^ 2)
    T3 = sum(eig[(pc + 1):max_pc] ^ 3)
    h0 = 1 - 2 * T1 * T2 / (3 * T3 ^ 2)
    Q_lim = T1 * (1 + (h0 * qnorm(Q_signif) * sqrt(2 * T2)) / T1 + (T2 * h0 * (h0 - 1)) / T1 ^ 2) ^ (1 / h0)
    Q_val = c(Q_val, sum(Q > Q_lim))
  }
  pc_opt_Q = which.min(Q_val)
  
  return(c(Wold = pc_opt_wold,
           REV = pc_opt_REV,
           Q = pc_opt_Q))
}

# fviz_eig modified to show suggested number of PC
fviz_eig_mod = function (X, choice = c("variance", "eigenvalue"), geom = c("bar", 
                                                                           "line"), barfill = c("steelblue", 'red'), barcolor = "steelblue", 
                         linecolor = "black", ncp = 10, addlabels = FALSE, hjust = 0, 
                         main = NULL, xlab = NULL, ylab = NULL, ggtheme = theme_minimal(), pc_to_highlight,
                         ...){
  eig <- factoextra::get_eigenvalue(X)
  eig <- eig[1:min(ncp, nrow(eig)), , drop = FALSE]
  choice <- choice[1]
  if (choice == "eigenvalue") {
    eig <- eig[, 1]
    text_labels <- round(eig, 0)
    if (is.null(ylab)) 
      ylab <- "Eigenvalue"
  }
  else if (choice == "variance") {
    eig <- eig[, 2]
    text_labels <- round(eig, 0)
  }
  else stop("Allowed values for the argument choice are : 'variance' or 'eigenvalue'")
  text_labels[5:length(text_labels)] = ""
  if (length(intersect(geom, c("bar", "line"))) == 0) 
    stop("The specified value(s) for the argument geom are not allowed ")
  df.eig <- data.frame(dim = factor(1:length(eig)), eig = eig)
  df.eig$hl = ifelse(df.eig$dim == pc_to_highlight, 1, 0)
  df.eig$hl = factor(df.eig$hl)
  extra_args <- list(...)
  bar_width <- extra_args$bar_width
  linetype <- extra_args$linetype
  if (is.null(linetype)) 
    linetype <- "solid"
  p <- ggplot(df.eig, aes(dim, eig, group = hl))
  if ("bar" %in% geom) 
    p <- p + geom_bar(stat = "identity", aes(fill=hl), 
                      color = barcolor, width = bar_width) +
    scale_fill_manual(values = barfill)
  if ("line" %in% geom) 
    p <- p + geom_line(color = linecolor, linetype = linetype) + 
    geom_point(shape = 19, color = linecolor)
  if (addlabels) 
    p <- p + geom_text(label = text_labels, vjust = -0.4, 
                       hjust = hjust)
  if (is.null(main)) 
    main <- "Scree plot"
  if (is.null(xlab)) 
    xlab <- "Dimensions"
  if (is.null(ylab)) 
    ylab <- "Percentage of explained variances"
  p <- p + labs(title = main, x = xlab, y = ylab) +
    theme(legend.position = "none")
  ggpubr::ggpar(p, ...)
  return(p)
}

# fit Robust PCA and automatically select optimal number of PC
fit_pca = function(pca_input, NA_masking = NULL, force_pc_selection = NULL, RobPCA_method = TRUE){
  
  # NA_masking: value to replace missing in the data
  # force_pc_selection: if integer, force number of PC
  
  # replace missing
  if (!is.null(NA_masking)){
    masking = is.na(pca_input)
    pca_input = pca_input %>%
      replace(is.na(.), NA_masking)
  } else {
    masking = matrix(FALSE, ncol = ncol(pca_input), nrow = nrow(pca_input))
  }
  
  # run RobPCA
  if (RobPCA_method){
    robpca = rpca(as.matrix(pca_input), trace = F, max.iter = 10000)
    
    if (robpca$convergence$converged == F){cat('\n #### RobPCA did not converge')}
    L = robpca$L; colnames(L) = colnames(pca_input); rownames(L) = rownames(pca_input)
    S = robpca$S
  } else {
    L = pca_input %>% as.matrix()
    S = matrix(0, ncol = ncol(pca_input), nrow = nrow(pca_input))
    robpca = NULL
  }
  res_pca = pca_fun(L, k_fold=3, cv = F, method_title = '')
  loadings = res_pca$pca$rotation
  scores = res_pca$pca$x
  
  # select optimal number of PC based on % increase in cumulative expl. var.
  max_increase = 0.05
  if (is.null(force_pc_selection)){
    importance_selection = res_pca$importance_table
    importance_selection = c(0, importance_selection$`Cumulative Proportion`[-1]/importance_selection$`Cumulative Proportion`[-ncol(pca_input)]-1)
    pc_selection = suppressWarnings(which(importance_selection > max_increase) %>% max())
    pc_selection = ifelse(is.infinite(pc_selection), 1, pc_selection)
  } else {
    pc_selection = force_pc_selection
  }
  
  loading_transform = loadings[, 1:pc_selection]
  scores_pc = L %*% loading_transform
  pca_input_reconstr = scores_pc %*% t(loadings[,1:pc_selection]) + S
  
  recRMSE = mean((as.matrix(pca_input) - pca_input_reconstr)^2) %>% sqrt()
  AvgAbsInput = mean(abs(pca_input) %>% as.matrix())
  recRMSE_no_mask = mean((as.matrix(pca_input)[!masking] - pca_input_reconstr[!masking])^2) %>% sqrt()
  AvgAbsInput_no_mask = mean(abs(as.matrix(pca_input))[!masking])
  R2 = round(eval_R2(pca_input, pca_input_reconstr, masking = masking) * 100, 1)
  R2_99 = round(eval_R2(pca_input, pca_input_reconstr, 0.99, masking = masking) * 100, 1)
  R2_95 = round(eval_R2(pca_input, pca_input_reconstr, 0.95, masking = masking) * 100, 1)
  
  return(list(embedding_dim = pc_selection,
              Embedding = scores_pc,
              Embedding_Range = paste0(range(scores_pc) %>% round(2), collapse = ' '),
              ExplCumVar_loading = round(res_pca$importance_table$`Cumulative Proportion`[pc_selection] * 100, 1),
              AvgAbsInput = AvgAbsInput,
              ReconstErrorRMSE = recRMSE,
              ReconstErrorRMSE_no_mask = recRMSE_no_mask,
              AvgAbsInput_no_mask = AvgAbsInput_no_mask,
              R2 = R2,
              R2_99 = R2_99,
              R2_95 = R2_95,
              robpca = robpca,
              res_pca = res_pca,
              loading_transform = loadings[, 1:pc_selection],
              importance_table = res_pca$importance_table,
              options = list(NA_masking = NA_masking,
                             masking = masking),
              report = data.frame(Embedding_Dimension = pc_selection,
                                  PCA_ExplCumVar = round(res_pca$importance_table$`Cumulative Proportion`[pc_selection] * 100, 1),
                                  rr = paste0(round(recRMSE, 4), ' (', round(recRMSE / AvgAbsInput * 100, 1), '%)'),
                                  Reconst_RMSE_no_mask = round(recRMSE_no_mask, 4),
                                  R2 = R2,
                                  R2_99 = R2_99,
                                  R2_95 = R2_95, stringsAsFactors = F) %>%
                rename(`Reconst_RMSE (% of AvgAbsInput)` = rr))
  )
}

# evaluate R^2 and its percentile
eval_R2 = function(original_data, predicted_data, alpha = NULL, masking = NULL){
  # evaluare R2 = 1 - RSS/TSS
  
  if (is.null(masking)){masking = matrix(FALSE, ncol = ncol(original_data), nrow = nrow(original_data))}
  
  TSS_val = (as.matrix(original_data)[!masking] - mean(as.matrix(original_data)[!masking])) ^ 2
  RSS_val = (as.matrix(original_data)[!masking] - as.matrix(predicted_data)[!masking]) ^ 2
  
  if (is.null(alpha)){
    RSS = sum(RSS_val)
    TSS = sum(TSS_val)
  } else {
    ind = RSS_val <= quantile(RSS_val, alpha)
    RSS = sum(RSS_val[ind])
    TSS = sum(TSS_val[ind])
  }
  
  R2 = 1 - RSS / TSS
  
  return(R2)
}

# aggregate data to plot into grid
aggregate_points = function(plot_data, label_values, n_cell = 20, err_lab = ""){
  
  # plot_data: data.frame with Dim1, Dim2, Dim3 (if available) and Label columns.
  #            Label can contain also a single value (i. e. points will be just aggregated in a single group for each cell)
  # label_values: available values to be converted to factor for the final output (to be used as groups in ggplot)
  # n_cell: number of cells on shortest dimension
  # err_lab: additional label to be plotted in case of raised warnings
  
  # return: data.frame with Dim1, Dim2, Dim3 (if available), Label and size columns for evaluated centroid of each Label value
  #         grid_x, grid_y, grid_z with grid points used only to plot the grid
  
  tot_dim = plot_data %>% select(starts_with("Dim")) %>% ncol()
  if (tot_dim == 2){plot_data$Dim3 = 0}
  
  max_x = max(plot_data$Dim1)
  min_x = min(plot_data$Dim1)
  max_y = max(plot_data$Dim2)
  min_y = min(plot_data$Dim2)
  max_z = max(plot_data$Dim3)
  min_z = min(plot_data$Dim3)
  cell_size = min(c((max_x - min_x) / n_cell, (max_y - min_y) / n_cell, ifelse(tot_dim == 3, (max_z - min_z) / n_cell, 1e16)))
  grid_x = sort(unique(c(seq(min_x, max_x, by = cell_size), min_x, max_x)))
  grid_y = sort(unique(c(seq(min_y, max_y, by = cell_size), min_y, max_y)))
  grid_z = sort(unique(c(seq(min_z, max_z, by = cell_size), min_z, max_z)))
  start_k = ifelse(tot_dim == 3, 2, 1)
  
  cell_range = matrix("", nrow = (length(grid_x)-1)*(length(grid_y)-1)*(ifelse(tot_dim == 3, length(grid_z)-1, length(grid_z))), ncol = 2)
  cc = 1
  for (i in 2:length(grid_x)){
    for (j in 2:length(grid_y)){
      for (k in start_k:length(grid_z)){
        x_min = grid_x[i-1]
        x_max = grid_x[i]
        y_min = grid_y[j-1]
        y_max = grid_y[j]
        if (tot_dim == 3){
          z_min = grid_z[k-1]
          z_max = grid_z[k]
        } else {
          z_min = z_max = ""
        }
        cell_range[cc,] = c(cell = paste0('(', i-1, ',', j-1, ifelse(tot_dim == 3, paste0(',', k-1), ''), ')'),
                            cell_range = paste0(x_min, ',', x_max, '|', y_min, ',', y_max, '|', z_min, ',', z_max))
        cc = cc + 1
      } # z
    } # j
  } # i
  cell_range = data.frame(cell_range, stringsAsFactors = F) %>% setNames(c("cell", "cell_range"))
  
  cell_summary = plot_data %>%
    mutate(Dim1_cell = set_bins(Dim1, breaks = grid_x),
           Dim2_cell = set_bins(Dim2, breaks = grid_y),
           Dim3_cell = set_bins(Dim3, breaks = grid_z)) %>%
    mutate(cell = paste0("(", Dim1_cell, ",", Dim2_cell, ",", Dim3_cell, ")")) %>%
    group_by(cell, Label) %>%
    summarize(size = n(), .groups = "drop") %>%
    mutate(Dim1 = 0, Dim2 = 0, Dim3 = 0)
  if (tot_dim == 2){
    cell_summary = cell_summary %>%
      mutate(cell = gsub(",1\\)", ")", cell)) %>%
      select(-Dim3)
  }
  cell_summary = cell_summary  %>%
    left_join(cell_range, by = "cell")
  if (nrow(plot_data) - sum(cell_summary$size) >= round(nrow(plot_data)*0.005)){cat('\n\n ###### missing points in cell_summary', err_lab)}
  if (sum(is.na(cell_summary$cell_range)) > 0){cat('\n\n ###### missing range in cell_summary', err_lab)}
  
  # reshape centroid to be equally spaced within the cell
  final_points = matrix("", ncol=ncol(cell_summary), nrow=nrow(cell_summary))
  start = 1
  end = 0
  for (cel in unique(cell_summary$cell)){
    df_cell = cell_summary %>%
      filter(cell == cel)
    
    points = generate_points(n_points = nrow(df_cell), dimension = tot_dim)
    x_min = strsplit(strsplit(df_cell$cell_range[1], '\\|')[[1]][1], ',')[[1]][1] %>% as.numeric()
    y_min = strsplit(strsplit(df_cell$cell_range[1], '\\|')[[1]][2], ',')[[1]][1] %>% as.numeric()
    z_min = strsplit(strsplit(df_cell$cell_range[1], '\\|')[[1]][3], ',')[[1]][1] %>% as.numeric()
    for (i in 1:nrow(df_cell)){
      df_cell$Dim1[i] = x_min + cell_size * points[i, 1]
      df_cell$Dim2[i] = y_min + cell_size * points[i, 2]
      if (tot_dim == 3){
        df_cell$Dim3[i] = z_min + cell_size * points[i, 3]
      }
    }
    end = start + nrow(df_cell) - 1
    final_points[start:end, ] = df_cell %>% as.matrix()
    start = end + 1
  }
  final_points = data.frame(final_points, stringsAsFactors = F) %>% setNames(colnames(cell_summary)) %>%
    mutate_at(vars(starts_with("Dim"), "size"), as.numeric) %>%
    mutate(Label = factor(Label, levels = label_values))
  if (sum(cell_summary$size) != sum(final_points$size)){cat('\n\n ###### missing points in final_points', err_lab)}
  
  return(list(cell_summary = final_points,
              grid_x = grid_x %>% round(4),
              grid_y = grid_y %>% round(4),
              grid_z = grid_z %>% round(4)))
}

# generate "equally spaced" points in [0,1]^dimension
generate_points = function(n_points, dimension){
  
  # if (n_points == 2){
  #   points = matrix(c(0.25, 0.75, 0.75, 0.25), ncol = 2, byrow = T)
  # } else if (n_points == 3){
  #   points = matrix(c(0.5, 0.75, 0.2, 0.25, 0.8, 0.25), ncol = 2, byrow = T)
  # } else if (n_points == 4){
  #   points = matrix(c(0.2, 0.2, 0.2, 0.8, 0.8, 0.2, 0.8, 0.8), ncol = 2, byrow = T)
  # } else if (n_points == 5){
  #   points = matrix(c(0.2, 0.2, 0.2, 0.8, 0.8, 0.2, 0.8, 0.8, 0.5, 0.5), ncol = 2, byrow = T)
  # } else if (n_points == 6){
  #   points = matrix(c(0.15, 0.4, 0.3, 0.85, 0.5, 0.1, 0.5, 0.5, 0.85, 0.4, 0.7, 0.85), ncol = 2, byrow = T)
  # } else {
  #   points = sobol(n_points, dim = 2, scrambling = 3)
  # }
  
  if (n_points == 1){
    points = rep(0.5, dimension) %>% matrix(nrow=1)
  } else {
    points = LatticeDesign::RSPD(p = dimension, n = n_points, rotation="magic", w=100)$Design
  }
  
  return(points)
}

# evaluate embedding quality (residual variance)
eval_R2_embedding = function(original_dist_mat, embedding_dist_mat){
  d_or = c(original_dist_mat[lower.tri(original_dist_mat, diag = F)], original_dist_mat[upper.tri(original_dist_mat, diag = F)])
  d_em = c(embedding_dist_mat[lower.tri(embedding_dist_mat, diag = F)], embedding_dist_mat[upper.tri(embedding_dist_mat, diag = F)])
  R2emb = cor(d_or, d_em)
  
  return(R2emb)
}

# robust scaler - removes median and scale by interquartile range
robust_scaler = function(data, scale_par = NULL, stringsAsFactors = F){
  
  # scale_par: if not NULL must be a data.frame(col = variable_name, median = variable_median, IQR = variable_IQR)) and will be used to scale input data.
  #            otherwise scale_par is evaluated by the function
  
  data = data.frame(data, stringsAsFactors = stringsAsFactors)
  
  if (is.null(scale_par)){
    scale_par = c()
    for (col in colnames(data)){
      if (is.numeric(data[, col])){
        scale_par = scale_par %>%
          bind_rows(data.frame(col = col, median = median(data[, col], na.rm = T),
                               IQR = diff(quantile(data[, col], c(0.25, 0.75), na.rm = T)), stringsAsFactors = F))
      }
    }
  }
  
  data_out = data
  for (coll in scale_par$col){
    median = scale_par %>%
      filter(col == coll) %>%
      pull(median)
    IQR = scale_par %>%
      filter(col == coll) %>%
      pull(IQR)
    data_out[, coll] = (data_out[, coll] - median) / IQR
  }
  
  return(list(data_out = data_out,
              scale_par = scale_par))
  
  
  
  d1 = data %>%
    mutate_if(is.numeric, function(x) (x - median(x)) / diff(quantile(x, c(0.25, 0.75))))
  
}

# fit UMAP or densMAP to visualize data
evaluate_UMAP = function(input_df, n_components = 3, min_dist_set = c(0, 0.01, 0.05, 0.1, 0.5, 0.99),
                         n_neighbors_set = c(5, 15, 30, 50, 100, 500), metric = "euclidean", init = "agspectral", n_epochs = 400,
                         predict_input = NULL, eval_UMAP = T, eval_densMAP = F){
  
  # eval_UMAP: evaluate base version of UMAP
  # eval_densMAP: evaluate densMAP  https://github.com/hhcho/densvis/tree/master/densmap  -  at the moment predict_input is not supported
  # init: only for UMAP. densMAP will use default = "spectral"
  
  # https://cran.r-project.org/web/packages/uwot/uwot.pdf
  # https://github.com/jlmelville/uwot
  
  if (eval_densMAP & !is.null(predict_input)){stop("predict_input is not supported for densMap")}
  
  output = list()
  
  # scale data
  scaler = robust_scaler(input_df)
  input_df = scaler$data_out
  
  
  for (n_neighbors in n_neighbors_set){
    for (min_dist in min_dist_set){
      
      # base UMAP
      if (eval_UMAP){
        set.seed(66)
        emb_umap <- umap(input_df, n_components = n_components, n_neighbors = n_neighbors, min_dist = min_dist,
                         metric = metric, verbose = FALSE, n_threads = detectCores(), ret_model = T, fast_sgd = TRUE,
                         approx_pow = TRUE, scale = FALSE, init = init, n_epochs = n_epochs)
        
        emb_predict_UMAP = c()
        if (!is.null(predict_input)){
          predict_input_UMAP = robust_scaler(predict_input, scale_par = scaler$scale_par)$data_out
          emb_predict_UMAP = umap_transform(predict_input_UMAP, emb_umap, verbose = F)
        }
        
        output[["UMAP"]][[paste0("n_neig_", n_neighbors, "_min_dist_", min_dist)]] = list(emb_umap_fit = emb_umap,
                                                                                          emb_visual = emb_umap$embedding,
                                                                                          emb_predict_visual = emb_predict_UMAP)
      }
      
      # densMAP
      if (eval_densMAP){
        densMAP <- dm$densMAP("n_components"=as.integer(n_components),  "n_neighbors"=as.integer(n_neighbors), "min_dist"=min_dist,
                              "metric"=metric, "verbose"=0, "n_epochs"=as.integer(n_epochs), 
                              "dens_frac"=.3, "dens_lambda"=2., "final_dens"=FALSE, "var_shift"=.1, "random_state"=as.integer(66))
        
        emb_densmap = densMAP$fit(input_df %>% as.matrix())
        
        emb_predict_densMAP = c()
        if (!is.null(predict_input)){
          predict_input_densMAP = robust_scaler(predict_input, scale_par = scaler$scale_par)$data_out
          emb_predict_densMAP = emb_densmap$transform(predict_input_densMAP %>% as.matrix())
        }
        
        output[["densMAP"]][[paste0("n_neig_", n_neighbors, "_min_dist_", min_dist)]] = list(emb_umap_fit = emb_densmap,
                                                                                             emb_visual = emb_densmap$embedding_,
                                                                                             emb_predict_visual = emb_predict_densMAP)
        
      }
      
    } # min_dist
  } # n_neighbors
  
  return(output)
}

# scale range in desired range
scale_range = function(x, a, b, xmin = NULL, xmax = NULL, mode = 'linear', s = NULL){
  
  # Scale input interval into new range
  # - a, b: new interval range
  # - xmin, xmax: provided if scaling has to be performed from a input range different from [min(x), max(x)]
  # - mode: 'linear' for linear scaling, 'exponential' for exponential scaling
  # - s: if mode == 'exponential' s is used for decay in exponential kernel.
  # The higher s the more spiked the decay (leptokurtic)
  
  if (is.null(xmin)){xmin = min(x)}
  if (is.null(xmax)){xmax = max(x)}
  
  if (mode == "linear"){
    # https://stats.stackexchange.com/questions/281162/scale-a-number-between-a-range
    out = (b - a) * (x - xmin) / (xmax - xmin) + a
  }
  
  if (mode == "exponential"){
    if (is.null(s)){s = 5}
    # https://stackoverflow.com/questions/49184033/converting-a-range-of-integers-exponentially-to-another-range
    r = (x - xmin) / (xmax - xmin)
    C = s ^ (xmax - xmin)
    out = ((b - a) * C ^ r + a * C - b) / (C - 1)
  }
  
  return(out)
}

# make a grid plot with 3D interactive plots (works also for 2D plot)
# render_3d_grid = function(plot_list, n_col, show = "point", single_class_size = 1, legend_cex = 1, plot_legend_index = c(1:length(plot_list)),
#                           point_alpha = 1, additional_points_size = 10, show_additional = "sphere", title_cex = 1,
#                           MIN_SCALE = 1, MAX_SCALE = 5, MODE_SCALE = "linear", MODE_S = NULL){
#   
#   # plot_list: list of data to plot. Each element is a list of:
#   #      - data: data.frame with Label (factor - legend label for each point),
#   #             size (numeric - size of each point. will be scaled in [MIN_SCALE, MAX_SCALE] according to MODE_SCALE and MODE_S of scale_range()),
#   #             Dim1, Dim2, Dim3 (if any) (numeric - points coordinates)
#   #      - show: "point" or "sphere" for data - if not provided is set to show - will override show of the function
#   #      - title: title of each plot
#   #      - title_cex: cex for the title - if not provided is set to title_cex - will override title_cex of the function
#   #      - point_alpha: alpha for points when show = "point" - if not provided is set to point_alpha - will override point_alpha of the function
#   #      - cmap: colormap for data - if not provided is set default (see in the code) - will override default one
#   #      - additional_data: data.frame with Dim1, Dim2, Dim3 (if any) and Label. Additional point to plot. Only single label and single size is allowed.
#   #      - additional_color: color for additional points
#   #      - show_additional: "point" or "sphere" for additional data - if not provided is set to show_additional - will override show_additional of the function
#   # n_col: number of columns in the plot
#   # show: "point" for points or "sphere" for 3D spheres
#   # show_additional: "point" for points or "sphere" for 3D spheres for additional data
#   # single_class_size: size of point/sphere when data has a single label/class
#   # plot_legend_index: select index of plot to add legend (indices flow column by column, top to bottom)
#   # additional_points_size: size of additional points
# 
#   # define colormap
#   cmap = c('dodgerblue3', 'firebrick2', 'chartreuse3', 'cadetblue2', 'gold1', 'darkorange', 'slategray4', 'violet', 'yellow1')
#   
#   n_row = ceiling(length(plot_list) / n_col)
#   open3d()
#   mat <- matrix(1:(n_col*n_row*2), ncol = n_col)   # add spot for title (text3d())
#   layout3d(mat, height = rep(c(1, 2), n_row), widths = rep(1, n_col), sharedMouse = T)  # (1,10) is the proportion between text3d and plot3d space
#   plot_count = 1
#   for (pl_data in plot_list){
#     
#     plot_data = pl_data$data
#     label_values = levels(plot_data$Label)
#     plot_data_add = pl_data$additional_data
#     plot_data_add_color = pl_data$additional_color
#     point_alpha_work = pl_data$point_alpha
#     title_cex_work = pl_data$title_cex
#     show_additional_work = pl_data$show_additional
#     show_work = pl_data$show
#     cmap_work = pl_data$cmap
#     
#     if (is.null(point_alpha_work)){point_alpha_work = point_alpha}
#     if (is.null(title_cex_work)){title_cex_work = title_cex}
#     if (is.null(show_additional_work)){show_additional_work = show_additional}
#     if (is.null(show_work)){show_work = show}
#     if (is.null(cmap_work)){cmap_work = cmap}
#     
#     if (range(plot_data$size) %>% uniqueN() != 1){
#       plot_data$size = scale_range(plot_data$size, MIN_SCALE, MAX_SCALE, mode = MODE_SCALE, s = MODE_S) %>% round(1)
#     } else {
#       plot_data$size = rep(single_class_size, nrow(plot_data))
#     }
#     
#     # check if 2D plot must be used
#     plot2d_flag = FALSE
#     if (!"Dim3" %in% colnames(plot_data)){
#       plot_data = plot_data %>% mutate(Dim3 = 0)
#       plot2d_flag = TRUE
#     }
#     
#     # plot title
#     next3d()
#     empty = ""
#     text3d(0, 0, 0,bquote(.(pl_data$title) ~ .(empty)), cex = title_cex_work)  # didn't find a better workaroud for empty
#     next3d()
#     
# 
#     if (show_work == "point"){
#       for (class_i in 1:length(label_values)){
#         
#         class_label = label_values[class_i]
#         class_color = cmap_work[class_i]
#         class_data = plot_data %>%
#           filter(Label == class_label)
#         
#         for (class_size in unique(class_data$size)){
#           size_data = class_data %>%
#             filter(size == class_size) %>%
#             select(starts_with("Dim")) %>%
#             xyz.coords()
#           points3d(size_data, size = class_size, col = class_color, axes = F, box = T, xlab = "", ylab = "", zlab = "",
#                    alpha = point_alpha_work, point_antialias = TRUE)
#         } # class_size
#       } # class_i
#     }
#     
#     if (show_work == "sphere"){
#       plot3d(xyz.coords(plot_data %>% select(starts_with("Dim"))), col = cmap_work[plot_data$Label], type = "s",
#              size = plot_data$size, axes = F, box = T, xlab = "", ylab = "", zlab = "", point_antialias = TRUE)
#     }
#     
#     # plot additional data (if any)
#     if (!is.null(plot_data_add)){
#       if (show_additional_work == "point"){
#       points3d(xyz.coords(plot_data_add %>% select(starts_with("Dim"))), size = additional_points_size,
#                col = plot_data_add_color, axes = F, box = T, xlab = "", ylab = "", zlab = "", point_antialias = TRUE)
#       }
#       if (show_additional_work == "sphere"){
#         spheres3d(xyz.coords(plot_data_add %>% select(starts_with("Dim"))),  radius = additional_points_size/5,
#                col = plot_data_add_color, axes = F, box = T, xlab = "", ylab = "", zlab = "", point_antialias = TRUE)
#       }
#     }
#     
#     # add box and focus on 2D view (if any)
#     box3d(color = "grey")
#     if (plot2d_flag){
#       rgl.viewpoint( theta = 0, phi = 0, fov = 0, interactive = F)
#     } else {
#       rgl.viewpoint(zoom = 0.6)
#     }
#     
#     # plot legend for selected plot
#     additional_point_legend = ifelse(!is.null(plot_data_add), unique(plot_data_add$Label) %>% as.character(), "")
#     if (plot_count %in% plot_legend_index){
#       legend3d("bottomright", legend = c(label_values, additional_point_legend), cex = legend_cex, pch = 16,
#                col = c(cmap_work[1:length(label_values)], plot_data_add_color))
#     }
# 
#     plot_count = plot_count + 1
#   } # pl_data
#   
#   rglwidget(reuse = FALSE)
# }
render_3d_grid = function(plot_list, n_col, show = "point", single_class_size = 1, legend_cex = 1, plot_legend_index = c(1:length(plot_list)),
                          legend_resize = c(512, 512), point_alpha = 1, additional_points_size = 10, show_additional = "sphere", title_cex = 1,
                          MIN_SCALE = 1, MAX_SCALE = 5, MODE_SCALE = "linear", MODE_S = NULL, add_shared_slider = FALSE){
  
  # plot_list: list of data to plot. Elements are plotted column by column, top to bottom. Each element is a list of:
  #      - data: data.frame with Label (factor - legend label for each point),
  #             size (numeric - size of each point. will be scaled in [MIN_SCALE, MAX_SCALE] according to MODE_SCALE and MODE_S of scale_range()),
  #             Dim1, Dim2, Dim3 (if any) (numeric - points coordinates)
  #      - show: "point" or "sphere" for data - if not provided is set to show - will override show of the function
  #      - title: title of each plot
  #      - title_cex: cex for the title - if not provided is set to title_cex - will override title_cex of the function
  #      - point_alpha: alpha for points when show = "point" - if not provided is set to point_alpha - will override point_alpha of the function
  #      - cmap: colormap for data - if not provided is set default (see in the code) - will override default one
  #      - additional_data: data.frame with Dim1, Dim2, Dim3 (if any) and Label. Additional point to plot. Only single label and single size is allowed.
  #      - additional_color: colormap for additional points - if not provided is set to cmap
  #      - show_additional: "point" or "sphere" for additional data - if not provided is set to show_additional - will override show_additional of the function
  # n_col: number of columns in the plot
  # show: "point" for points or "sphere" for 3D spheres
  # show_additional: "point" for points or "sphere" for 3D spheres for additional data
  # single_class_size: size of point/sphere when data has a single label/class
  # legend_cex: cex for legend
  # plot_legend_index: select index of plot to add legend (indices flow column by column, top to bottom)
  # legend_resize: in order to avoid legend low resolution play with (x,y) resize value and legend_cex
  # additional_points_size: size of additional points
  # add_shared_slider: if TRUE add a shared slider to show all points (both data and additional data) together and class label by class label
  #                    https://stackoverflow.com/questions/66731751/r-rgl-link-multiple-plots-to-a-single-widget
  
  # define colormap
  cmap = c('dodgerblue3', 'firebrick2', 'chartreuse3', 'cadetblue2', 'gold1', 'darkorange', 'slategray4', 'violet', 'yellow1')
  
  n_row = ceiling(length(plot_list) / n_col)
  open3d()
  mat <- matrix(1:(n_col*n_row*2), ncol = n_col)   # add spot for title (text3d())
  layout3d(mat, height = rep(c(1, 2), n_row), widths = rep(1, n_col), sharedMouse = T)  # (1,10) is the proportion between text3d and plot3d space
  subset_control_list = list()  # only if add_shared_slider == T
  plot_count = 1
  for (pl_data in plot_list){
    
    plot_data = pl_data$data
    label_values = levels(plot_data$Label) %>% sort()
    plot_data_add = pl_data$additional_data
    label_values_add = levels(plot_data_add$Label) %>% sort()
    plot_data_add_color = pl_data$additional_color
    point_alpha_work = pl_data$point_alpha
    title_cex_work = pl_data$title_cex
    show_additional_work = pl_data$show_additional
    show_work = pl_data$show
    cmap_work = pl_data$cmap
    
    if (is.null(point_alpha_work)){point_alpha_work = point_alpha}
    if (is.null(title_cex_work)){title_cex_work = title_cex}
    if (is.null(show_additional_work)){show_additional_work = show_additional}
    if (is.null(show_work)){show_work = show}
    if (is.null(cmap_work)){cmap_work = cmap}
    if (is.null(plot_data_add_color)){plot_data_add_color = cmap_work}
    
    # set points size and scale if multiple size values are provided
    if (range(plot_data$size) %>% uniqueN() != 1){
      plot_data$size = scale_range(plot_data$size, MIN_SCALE, MAX_SCALE, mode = MODE_SCALE, s = MODE_S) %>% round(1)
    } else {
      plot_data$size = rep(single_class_size, nrow(plot_data))
    }
    if (!is.null(plot_data_add)){
      if (range(plot_data_add$size) %>% uniqueN() != 1){
        plot_data_add$size = scale_range(plot_data_add$size, MIN_SCALE, MAX_SCALE, mode = MODE_SCALE, s = MODE_S) %>% round(1)
      } else {
        plot_data_add$size = rep(additional_points_size, nrow(plot_data_add))
      }
    }
    
    # check if 2D plot must be used
    plot2d_flag = FALSE
    if (!"Dim3" %in% colnames(plot_data)){
      plot_data = plot_data %>% mutate(Dim3 = 0)
      plot2d_flag = TRUE
    }
    
    # plot title
    next3d()
    empty = ""
    text3d(0, 0, 0,bquote(.(pl_data$title) ~ .(empty)), cex = title_cex_work)  # didn't find a better workaroud for empty
    next3d()
    
    # plot data by class label
    plot_list_work = c()
    for (class_i in 1:length(label_values)){
      
      class_label = label_values[class_i]
      class_color = cmap_work[class_i]
      class_data = plot_data %>%
        filter(Label == class_label)
      
      add_plot = c()
      for (class_size in unique(class_data$size)){
        size_data = class_data %>%
          filter(size == class_size) %>%
          select(starts_with("Dim")) %>%
          xyz.coords()
        
        if (show_work == "point"){   
          add_plot = c(add_plot,
                       points3d(size_data, size = class_size, col = class_color, axes = F, box = T, xlab = "", ylab = "", zlab = "",
                                alpha = point_alpha_work, point_antialias = TRUE))
        } else if (show_work == "sphere"){
          add_plot = c(add_plot,
                       spheres3d(size_data,  radius = class_size/5, col = class_color, axes = F, box = T, xlab = "", ylab = "", zlab = "",
                                 point_antialias = TRUE))
        }
      } # class_size
      plot_list_work = c(plot_list_work, list(add_plot))
    } # class_i
    names(plot_list_work) = label_values
    
    # plot additional data (if any) by class label
    plot_list_work_add = list()
    if (!is.null(plot_data_add)){
      for (class_i in 1:length(label_values_add)){
        
        class_label = label_values_add[class_i]
        class_color = plot_data_add_color[class_i]
        class_data = plot_data_add %>%
          filter(Label == class_label)
        
        add_plot = c()
        for (class_size in unique(class_data$size)){
          size_data = class_data %>%
            filter(size == class_size) %>%
            select(starts_with("Dim")) %>%
            xyz.coords()
          
          if (show_additional_work == "point"){   
            add_plot = c(add_plot,
                         points3d(size_data, size = class_size, col = class_color, axes = F, box = T, xlab = "", ylab = "", zlab = "",
                                  point_antialias = TRUE))
          } else if (show_additional_work == "sphere"){
            add_plot = c(add_plot,
                         spheres3d(size_data,  radius = class_size/5, col = class_color, axes = F, box = T, xlab = "", ylab = "", zlab = "",
                                   point_antialias = TRUE))
          }
        } # class_size
        plot_list_work_add = c(plot_list_work_add, list(add_plot))
      } # class_i
      names(plot_list_work_add) = label_values_add
    }
    
    # add box and focus on 2D view (if any)
    box3d(color = "grey")
    if (plot2d_flag){
      rgl.viewpoint( theta = 0, phi = 0, fov = 0, interactive = F)
    } else {
      rgl.viewpoint(zoom = 0.6)
    }
    
    # plot legend for selected plot
    if (!is.null(plot_data_add)){
      additional_point_legend = setdiff(label_values, label_values_add)
      additional_point_legend_color = plot_data_add_color[match(additional_point_legend, label_values_add)]
    } else {
      additional_point_legend = additional_point_legend_color = c()
    }
    if (plot_count %in% plot_legend_index){
      par3d(windowRect = c(0, 0, legend_resize[1], legend_resize[1]))  # used to avoid low resolution for legend
      legend3d("bottomright", legend = c(label_values, additional_point_legend), cex = legend_cex, pch = 16,
               col = c(cmap_work[1:length(label_values)], additional_point_legend_color))
    }
    
    # save single subscene list (each element is a single subscene to be matched with the movement of the other)
    if (add_shared_slider){
      # concatenate data plot and additional data plot by label names
      merge_list = list()
      for (pl_name in names(plot_list_work)){
        merge_list[[pl_name]] = c(plot_list_work[[pl_name]], plot_list_work_add[[pl_name]])
      }
      merge_list = c(list(All = merge_list %>% unlist()), merge_list)
      
      subset_control_list = c(subset_control_list, list(
        subsetControl(1, subscenes = subsceneInfo()$id, subsets = merge_list)
      ))
    }
    
    plot_count = plot_count + 1
  } # pl_data
  
  if (add_shared_slider){
    
    control_labels = subset_control_list[[1]]$labels
    output = rglwidget(reuse = FALSE) %>%
      playwidget(start = 0, stop = length(control_labels)-1, interval = 1, #height = widget_height,
                 components = c("Play", "Slider", "Label"),
                 labels = control_labels,
                 subset_control_list)
    
    return(output)
  } else {
    rglwidget(reuse = FALSE)
  }
}

# make a 3D interactive plot with a slider to show first all points and then subsets (works also for 2D plot)
render_3d_slider = function(data, show = "sphere", point_size = 2, text_hor_just = 0,
                            text_ver_just = 1.2, text_cex = 1, widget_height = 500, zoom_val = 1){
  
  # data: data.frame with Label (factor - legend label for each subset of the slider),
  #                       text_annotation (optional - string to be plotted close to points)
  #                       Dim1, Dim2, Dim3 (if any) (numeric - points coordinates)
  # show: "point" for points "sphere" for 3D spheres
  # point_size: size of point/sphere
  # text_hor_just, text_ver_just: horizontal and vertical spacing for text annotation
  # text_cex: text cex for annotation
  # widget_height: height of the widget in pixels
  # zoom_val: percentage of zoom. The smaller the closer
  
  label_values = levels(data$Label)
  
  # check if 2D plot must be used
  plot2d_flag = FALSE
  if (!"Dim3" %in% colnames(data)){
    data = data %>% mutate(Dim3 = 0)
    plot2d_flag = TRUE
  }
  
  open3d()
  subset_labels = c("All")
  subset_plot_list = list(data %>%
                            mutate(color = rainbow(length(label_values))[Label]))
  for (label_class in label_values){
    subset_plot_list = c(subset_plot_list,
                         list(subset_plot_list[[1]] %>%
                                filter(Label == label_class)))
    subset_labels = c(subset_labels, label_class)
  }
  names(subset_plot_list) = subset_labels
  
  # plot
  plot_list = list()
  for (pl_name in names(subset_plot_list)){
    
    pl_data = subset_plot_list[[pl_name]]
    
    if (show == "point"){
      add_plot = points3d(xyz.coords(pl_data %>% select(starts_with("Dim"))), col=pl_data$color, size = point_size, point_antialias = TRUE)
    } else if (show == "sphere"){
      add_plot = spheres3d(xyz.coords(pl_data %>% select(starts_with("Dim"))), col=pl_data$color, radius = point_size/5, point_antialias = TRUE)
    }
    
    # add text annotation
    if (pl_name != "All" & !is.null(pl_data$text_annotation)){
      # check too close points and invert vertical spacing
      points_dist = dist(pl_data %>% select(starts_with("Dim"))) %>% as.matrix()
      points_dist[upper.tri(points_dist, diag = T)] = NA
      close_points = points_dist <= 0.5 * max(points_dist, na.rm = T)  # matrix TRUE/FALSE
      close_column = which.max(colSums(close_points, na.rm = T))   # find column with most TRUE
      close_points_index = which(close_points[, close_column])    # find point with TRUE in the selected column
      if (length(close_points_index) == 0){close_points_index = 0}
      
      sub_data = pl_data %>% filter(!row_number() %in% close_points_index)
      add_plot = c(add_plot,
                   text3d(xyz.coords(sub_data %>% select(starts_with("Dim"))), texts = sub_data$text_annotation,
                          adj = c(text_hor_just, text_ver_just), cex = text_cex))
      if (length(close_points_index) > 0){  # use -text_ver_just
        sub_data = pl_data %>% filter(row_number() %in% close_points_index)
        add_plot = c(add_plot,
                     text3d(xyz.coords(sub_data%>% select(starts_with("Dim"))), texts = sub_data$text_annotation,
                            adj = c(text_hor_just, -text_ver_just), cex = text_cex))
      }
    }
    plot_list = c(plot_list, list(add_plot))
    box3d(color = "grey")
  }
  names(plot_list) = subset_labels
  
  output = rglwidget(reuse = FALSE) %>%
    playwidget(start = 0, stop = length(plot_list)-1, interval = 1, height = widget_height,
               components = c("Play", "Slider", "Label"),
               labels = subset_labels,
               subsetControl(1, subsets = plot_list))
  
  # add box and focus on 2D view (if any)
  if (plot2d_flag){
    rgl.viewpoint( theta = 0, phi = 0, fov = 0, interactive = F, zoom = zoom_val)
  } else {
    rgl.viewpoint(zoom = zoom_val)
  }
  
  return(output)
}


# generate string of settings for Rmarkdown .Rmd file
RMarkdown_settings = function(title, help_path){
  
  out = paste0(
    "---
title: ", title, "
output:
  html_document:
    toc: true
    toc_depth: 2
    df_print: paged
    css: C:/Users/Alessandro Bitetto/Downloads/UniPV/BCC-default/Distance_to_Default/Stats/styles.css
---

<style type=\"text/css\">
.main-container {
  max-width: 1800px;
  margin-left: auto;
  margin-right: auto;
}
</style>

```{r, echo=FALSE, include=FALSE}
suppressMessages(suppressWarnings(library(knitr)))
suppressMessages(suppressWarnings(library(kableExtra)))
suppressMessages(suppressWarnings(library(rgl)))
suppressMessages(suppressWarnings(library(data.table)))
suppressMessages(suppressWarnings(library(dplyr)))
suppressMessages(suppressWarnings(library(tidyverse)))
options(rgl.useNULL = TRUE) # Suppress the separate window.
render_3d_grid = function(x){0}
suppressMessages(insertSource('", help_path, "', functions='render_3d_grid'))
scale_range = function(x){0}
suppressMessages(insertSource('", help_path, "', functions='scale_range'))
render_3d_slider = function(x){0}
suppressMessages(insertSource('", help_path, "', functions='render_3d_slider'))
```
")
  
  return(out)
}

# generate string of render_3d_grid for Rmarkdown .Rmd file
RMarkdow_render_3d_grid = function(plot_list_rds, emb_type, n_col, show = "point", single_class_size = 1, legend_cex = 1, plot_legend_index = NULL,
                                   legend_resize = c(512, 512), point_alpha = 1, additional_points_size = 5, show_additional = "sphere", title_cex = 1,
                                   MIN_SCALE = 1, MAX_SCALE = 5, MODE_SCALE = "linear", MODE_S = NULL, add_shared_slider = FALSE,
                                   HTML_fig.height = 5, HTML_fig.width = 5, add_section = TRUE){
  out = ""
  if (add_section){
    out = paste0(out, "\n\n## Embedding: ", emb_type, "\n\n")
  }
  out = paste0(out, "\n",
               "
```{r, echo=FALSE, include=TRUE, fig.height = ", HTML_fig.height, ", fig.width = ", HTML_fig.width, "}
plot_list = readRDS('", plot_list_rds, "')
render_3d_grid(plot_list, n_col = ", n_col, ", show = '", show, "', single_class_size = ", single_class_size, ", legend_cex = ", legend_cex,
               ", plot_legend_index = c(", ifelse(is.null(plot_legend_index), "NULL", paste0(plot_legend_index, collapse = ",")),
               "), legend_resize = c(", paste0(legend_resize, collapse = ","), "), point_alpha = ", point_alpha,
               ", additional_points_size = ", additional_points_size, ", show_additional = '", show_additional, "', title_cex = ", title_cex,
               ", MIN_SCALE = ", MIN_SCALE, ", MAX_SCALE = ", MAX_SCALE,
               ", MODE_SCALE = '", MODE_SCALE, "', MODE_S = ", ifelse(is.null(MODE_S), "NULL", MODE_S), ", add_shared_slider = ", add_shared_slider, ")
```
")
  
  return(out)
}

# generate string of render_3d_grid for Rmarkdown .Rmd file
RMarkdow_render_3d_slider = function(data_rds, emb_type, show = "sphere", point_size = 1, text_hor_just = 0, text_ver_just = 1.2,
                                     text_cex = 1, widget_height = 500, zoom_val = 1, HTML_fig.height = 5, HTML_fig.width = 5, add_section = TRUE){
  out = ""
  if (add_section){
    out = paste0(out, "\n\n## Embedding: ", emb_type, "\n\n")
  }
  out = paste0(out, "\n",
               "
```{r, echo=FALSE, include=TRUE, fig.height = ", HTML_fig.height, ", fig.width = ", HTML_fig.width, "}
data = readRDS('", data_rds, "')
render_3d_slider(data, show = '", show, "', point_size = ", point_size , ", text_hor_just = ", text_hor_just,
               ", text_ver_just = ", text_ver_just, ", text_cex = ", text_cex, ", widget_height = ", widget_height, ", zoom_val = ", zoom_val, ")
```
")
  
  return(out)
}

# function used to map label_type  on peers dataset
map_cluster_on_peers = function(label_type, categorical_variables, df_peers_long, ORBIS_mapping, ORBIS_label){
  
  # function used to map label_type  on peers dataset
  # - label_type: "Regione_Macro" or "roa_Median" or "oneri_ricavi_LowMedHig"
  # - categorical_variables: vector of categorical_variables ("Regione_Macro", "Dummy_inddustry", etc.)
  
  if (label_type %in% categorical_variables){
    label_values = df_peers_long %>% select(all_of(label_type)) %>% unique() %>% pull(label_type) %>%
      lapply(function(x) substr(x,1,15)) %>% unlist()
    df_label = df_peers_long %>%
      filter(year != "2011") %>%
      mutate(Label = substr(!!sym(label_type), 1, 15)) %>%
      mutate(Label = factor(Label, levels = label_values)) %>%
      select(Company_name_Latin_alphabet, Label)
  } else {
    match_label = gsub("_LowMedHig|_Median", "", label_type)
    match_label_fix = ifelse(sum(paste0("BILA_", match_label) %in% ORBIS_mapping$BILA) == 0, match_label, paste0("BILA_", match_label))
    label_version = gsub(match_label, "", label_type) %>% gsub("_", "", .)  # Median or LowMedHig
    peers_label = ORBIS_mapping %>%
      filter(BILA == match_label_fix) %>%
      pull(Single_Variable) %>%
      unique()
    if (label_version == 'Median'){
      df_label = df_peers_long %>%
        # filter(year != "2011") %>%
        mutate(Variable = match_label_fix) %>%
        left_join(ORBIS_label %>% filter(Variable == match_label_fix) %>% select(Variable, Peer_median), by = "Variable") %>%
        mutate(Label = ifelse(!!sym(peers_label) <= Peer_median, 'Down', 'Up')) %>%
        select(Company_name_Latin_alphabet, Variable, Peer_median, Label, all_of(peers_label))
    } else if (label_version == 'LowMedHig'){
      df_label = df_peers_long %>%
        # filter(year != "2011") %>%
        mutate(Variable = match_label_fix) %>%
        left_join(ORBIS_label %>% filter(Variable == match_label_fix) %>% select(Variable, Peer_33rd, Peer_66th), by = "Variable") %>%
        select(Company_name_Latin_alphabet, Variable, Peer_33rd, Peer_66th, all_of(peers_label)) %>%
        mutate(Label = 'Low') %>%
        mutate(Label = ifelse(!!sym(peers_label) > Peer_33rd, 'Medium', Label)) %>%
        mutate(Label = ifelse(!!sym(peers_label) > Peer_66th, 'High', Label))
    }
    df_label = df_label %>%
      mutate(Label = factor(Label, levels = unique(df_label$Label))) %>%
      select(Company_name_Latin_alphabet, Label)
  }
  
  # take median value for multiple label for each
  label_values = levels(df_label$Label)
  df_label = df_label %>%
    mutate(Label = as.numeric(Label)) %>%
    group_by(Company_name_Latin_alphabet) %>%
    summarise(Label = round(median(Label))) %>%
    mutate(Label = label_values[Label]) %>%
    mutate(Label = factor(Label, levels = label_values))
  
  return(df_label)
}

# clever standardization
clever_scale = function(input_df, exclude_var = c()){
  # if variable is constant or included in 'exclude_var', no transformation
  
  output_matrix = c()
  
  for (col in colnames(input_df)){
    val = input_df %>% pull(col)
    if (col %in% exclude_var){
      std_val = val
    } else if (uniqueN(val) == 1){
      std_val = scale(val, center = T, scale = F) %>% as.numeric
    }else {
      std_val = scale(val, center = T, scale = T) %>% as.numeric
    }
    output_matrix = output_matrix %>%
      cbind(std_val)
  }
  output_matrix = output_matrix %>%
    as.data.frame() %>%
    setNames(colnames(input_df)) %>%
    `rownames<-`(rownames(input_df)) %>%
    as.matrix()
  
  return(output_matrix)
}

# create fold for outer and inner Cross-Validation with stratification
create_stratified_fold = function(df_work, inn_cross_val_fold, out_cross_val_fold, out_stratify_columns = NULL, inn_stratify_columns = NULL, 
                                  out_stratify_target = F, inn_stratify_target = F){
  
  # to get only outer folds set inn_cross_val_fold <= 1
  
  if (!is.null(out_stratify_columns)){out_stratify_target = F}   # stratify and stratify.cols are mutually exclusive
  if (!is.null(inn_stratify_columns)){inn_stratify_target = F}
  
  # create mock classification problem to speed up evaluation
  df_fold = df_work %>%
    mutate(TARGET = as.factor(round(runif(nrow(df_work))))) %>%
    select(c('TARGET', unique(c(out_stratify_columns, inn_stratify_columns))))
  
  task = makeClassifTask(data = df_fold,
                         target = 'TARGET')
  learner = makeLearner("classif.lda")
  learner$config = list(on.learner.error = 'warn')
  
  # outer folds - used also in case of non-nested CV
  outer = makeResampleDesc(method = "CV",
                           iters = out_cross_val_fold,
                           predict = "test",
                           stratify.cols = out_stratify_columns,
                           stratify = out_stratify_target)
  set.seed(1, "L'Ecuyer-CMRG")
  res_outer = resample(learner = learner,
                       task = task,
                       resampling = outer,
                       show.info = F,
                       models = F)
  outer_ind = getResamplingIndices(res_outer)
  
  # for each outer test set, perform inner folding according to inn_cross_val_fold (if > 1)
  outer_blocking = data.frame(index = as.character(1:nrow(df_fold)), stringsAsFactors = F)
  inner_blocking = data.frame(index = as.character(1:nrow(df_fold)), stringsAsFactors = F)
  cc = 1
  for (ou in 1:length(outer_ind$test.inds)){
    
    # outer
    out_test = outer_ind$test.inds[[ou]]
    outer_blocking = outer_blocking %>%
      left_join(
        data.frame(index = as.character(out_test), group = ou, stringsAsFactors = F),
        by = 'index'
      )
    
    if (inn_cross_val_fold > 1){
      # inner
      inner_task = makeClassifTask(data = df_fold %>%
                                     filter(row_number() %in% out_test),
                                   target = 'TARGET')
      inner = makeResampleDesc(method = "CV",
                               iters = inn_cross_val_fold,
                               predict = "test",
                               stratify.cols = inn_stratify_columns,
                               stratify = inn_stratify_target)
      set.seed(1, "L'Ecuyer-CMRG")
      res_inner = resample(learner = learner,
                           task = inner_task,
                           resampling = inner,
                           show.info = F,
                           models = F)
      inner_ind = getResamplingIndices(res_inner)
      
      for (inn in 1:length(inner_ind$test.inds)){
        
        inn_test = inner_ind$test.inds[[inn]]
        inner_blocking = inner_blocking %>%
          left_join(
            data.frame(index = as.character(out_test[inn_test]), group = cc, stringsAsFactors = F),
            by = 'index'
          )
        cc = cc + 1
      } # inn
    } else {
      inner_blocking$group = 99
    }
  } # ou
  
  outer_blocking = outer_blocking %>%
    gather('group', 'block', -index) %>%
    filter(!is.na(block)) %>%
    select(-group) %>%
    mutate(index = as.integer(index)) %>%
    arrange(index)
  if (uniqueN(outer_blocking$index) != nrow(df_fold)){cat('\n\n####### duplicates in outer_blocking\n\n')}
  
  inner_blocking = inner_blocking %>%
    gather('group', 'block', -index) %>%
    filter(!is.na(block)) %>%
    select(-group) %>%
    mutate(index = as.integer(index)) %>%
    arrange(index)
  if (uniqueN(inner_blocking$index) != nrow(df_fold)){cat('\n\n####### duplicates in inner_blocking\n\n')}
  
  if (inn_cross_val_fold > 1){
    final_blocking = inner_blocking
  } else {
    final_blocking = outer_blocking
  }
  
  # cat('\nSize of each of the', uniqueN(final_blocking$block), 'blocking subsets:')
  # print(table(final_blocking$block))
  # cat('\n')
  
  # final distribution
  final_distr_data = df_work %>%
    bind_cols(final_blocking %>% arrange(index)) %>%
    select(block, all_of(colnames(df_fold))) %>%
    group_by_all() %>%
    summarise(Count = n(), .groups = "drop") %>%
    group_by(block) %>%
    mutate(perc = round(Count/ sum(Count) * 100, 1)) %>%
    ungroup() %>%
    arrange(block)
  
  final_distr = c()
  for (i in 1:uniqueN(final_distr_data$block)){
    final_distr = final_distr %>%
      bind_rows(final_distr_data %>%
                  filter(block == i) %>%
                  unite("comb", all_of(colnames(df_fold)), remove = T) %>%
                  column_to_rownames("comb") %>%
                  select(-block, -Count) %>%
                  t() %>%
                  as.data.frame() %>%
                  mutate(block = i))
  }
  
  return(list(final_blocking = final_blocking,
              final_distr = final_distr %>% select(block, everything()),
              final_distr_data = final_distr_data %>% unite("Combination", all_of(colnames(df_fold)), remove = F))
  )
}

# fit glmnet and select optimal lambda with cross-validation
fit_cv_glmnet = function(x, y, alpha = 1, standardize = F, intercept = T, parallel = T,
                         type.measure = "auc", lambda_final = "lambda.1se", family = "binomial",
                         fixed_variables = c(), n_fold_cvgmlnet = 10, x_test_predict = NULL){
  
  # x: must have rownames
  # alpha: 1 = LASSO, 0 = RIDGE
  # lambda_final: how to select best lambda. "lambda.1se" or "lambda.min"
  # n_fold_cvgmlnet: number of folds in cv.glmnet. Stratified folds are evaluated by y.
  # x_test_predict: additional data to be predicted (same shape of x)
  
  # create stratified sampling for target variable only for cv.glmnet
  cv_gmlnet_ind_gen = stratified.cv.data.single.class(1:nrow(x), which(y == 1), kk=n_fold_cvgmlnet, seed=23)
  cv_gmlnet_ind = rep(NA, nrow(x))
  for (kk in 1:n_fold_cvgmlnet){
    cv_gmlnet_ind[c(cv_gmlnet_ind_gen$fold.positives[[kk]], cv_gmlnet_ind_gen$fold.negatives[[kk]])] = kk
  }
  if (sum(is.na(cv_gmlnet_ind)) > 0){cat('\n#### missing values in cv_gmlnet_ind')}
  
  # fit glmnet and take best lambda coefficients and predictions
  # penalization factor
  pen_factors = rep(1, ncol(x)) # 0 for variable to always keep, 1 for other
  pen_factors[which(colnames(x) %in% fixed_variables)] = 0
  
  # weights
  fraction_0 <- rep(1 - sum(y == 0) / nrow(x), sum(y == 0))
  fraction_1 <- rep(1 - sum(y == 1) / nrow(x), sum(y == 1))
  weights <- numeric(nrow(x))
  weights[y == 0] <- fraction_0
  weights[y == 1] <- fraction_1
  
  if (parallel){
    cl <- parallel::makeCluster(detectCores() - 2)
    doParallel::registerDoParallel(cl)
  }
  cvfit <- cv.glmnet(x %>% as.matrix(), y, weights = weights, parallel = parallel, nfolds = n_fold_cvgmlnet, family = family, alpha = alpha,
                     standardize = standardize, intercept = intercept, penalty.factor = pen_factors,  
                     alignment = "lambda", type.measure = type.measure, foldid = cv_gmlnet_ind)
  if (parallel){parallel::stopCluster(cl)}
  
  lambda_opt = cvfit[[lambda_final]]
  # "response" for probab., "link" for pre-sigmoid (i.e. the predicted logit), "coefficients" for coefficients, "class" for final class (with thresh=0.5)
  coeff = predict(cvfit, newx = x, s = lambda_opt, family = family, type="coefficients") %>%
    as.matrix() %>%
    data.frame() %>%
    setNames("Coeff") %>%
    rownames_to_column("Variable")
  pred_prob_train = predict(cvfit, newx = x %>% as.matrix(), s = lambda_opt, family = family, type="response") %>%
    data.frame() %>%
    setNames("Prob") %>%
    `rownames<-`(rownames(x))
  
  if (!is.null(x_test_predict)){
    pred_prob_test = predict(cvfit, newx = x_test_predict %>% as.matrix(), s = lambda_opt, family = family, type="response") %>%
      data.frame() %>%
      setNames("Prob") %>%
      `rownames<-`(rownames(x_test_predict))
  } else {
    pred_prob_test = NULL
  }
  
  return(list(fit = cvfit,
              lambda_opt = lambda_opt,
              coeff = coeff,
              pred_prob_train = pred_prob_train,
              pred_prob_test = pred_prob_test,
              options = list(weights = weights,
                             pen_factors = pen_factors,
                             alpha = alpha,
                             type.measure = type.measure,
                             foldid = cv_gmlnet_ind,
                             lambda_final = lambda_final,
                             family = family,
                             standardize = standardize)))
}


# find optimal threshold for classification task
find_best_threshold = function(df_pred, prob_thresh_perf = "F1", prob_thresh_perf_minimize = F, thresh_set = NULL){
  
  # pf_pred: data.frame with "Prob" and "y_true" [character] columns
  # prob_thresh_perf: performance metric to be used to tune threshold if prob_thresh = "best". Only "F1", "Precision" or "Recall"
  # prob_thresh_perf_minimize: whether to minimize prob_thresh_perf
  # thresh_set: set of threshold to be evaluated. If NULL seq(0.01, 0.99, length.out = 200)
  
  if (is.null(thresh_set)){
    thresh_set = seq(0.01, 0.99, length.out = 200)
  }
  
  if (prob_thresh_perf == "F1"){
    perf_metr = MLmetrics::F1_Score
  }
  if (prob_thresh_perf == "Precision"){
    perf_metr = MLmetrics::Precision
  }
  if (prob_thresh_perf == "Recall"){
    perf_metr = MLmetrics::Recall
  }
  
  thresh_results = c()
  for (thr in thresh_set){
    df_thr = df_pred %>%
      mutate(y_pred = ifelse(Prob >= thr, 1, 0) %>% as.character())
    if (uniqueN(df_thr$y_pred) != 1){
      thresh_results = thresh_results %>%
        bind_rows(data.frame(threshold = thr, perf = perf_metr(y_true = df_thr$y_true, y_pred = df_thr$y_pred, positive = "1")))
    }
  }
  thresh_results = thresh_results %>%
    filter(is.finite(perf)) %>%
    arrange(desc(perf))
  if (prob_thresh_perf_minimize){
    thresh_results = thresh_results %>%
      arrange(perf)
  }
  
  return(list(best_threshold = thresh_results$threshold[1],
              threshold_results = thresh_results))
}

# fit and evaluates performance metrics for glmnet
get_glmnet_performance = function(data_train, data_test = NULL, alpha = 1, standardize = F, intercept = T, parallel = T,
                                  type.measure = "auc", lambda_final = "lambda.1se", family = "binomial",
                                  fixed_variables = c(), n_fold_cvgmlnet = 10,
                                  prob_thresh = 0.5, prob_thresh_perf = "F1", prob_thresh_perf_minimize = F, prob_thresh_set = "test",
                                  train_subset = NULL, test_subset = NULL){
  
  # fit glmnet with fit_cv_glmnet and evaluates performance metrics for train and test (if any) set
  # prob_thresh: threshold to convert probabilities to class. If "best" optimal value is evaluated according to prob_thresh_perf on prob_thresh_set
  # prob_thresh_perf: performance metric to be used to tune threshold if prob_thresh = "best". Only "F1", "Precision" or "Recall"
  # prob_thresh_perf_minimize: whether to minimize prob_thresh_perf 
  # prob_thresh_set: "train" or "test" set to be used to assess prob_thresh_perf
  # train_subset, test_subset: indices of subset on which evaluate performance
  
  if (!is.null(data_test)){
    x_test_predict = data_test %>% select(-y)
  } else {
    x_test_predict = NULL
  }
  
  fit_train = fit_cv_glmnet(x = data_train %>% select(-y), y = data_train$y, alpha = alpha, standardize = standardize, intercept = intercept, parallel = parallel,
                            type.measure = type.measure, lambda_final = lambda_final, family = family,
                            fixed_variables = fixed_variables, n_fold_cvgmlnet = n_fold_cvgmlnet, x_test_predict = x_test_predict)
  
  # get prediction for train set
  pred_train_orig = fit_train$pred_prob_train %>%
    rownames_to_column("rows") %>%
    left_join(data_train %>%
                select(y) %>%
                rownames_to_column("rows"), by = "rows") %>%
    mutate(y = as.character(y)) %>%
    rename(y_true = y)
  
  # get prediction for test set
  if (!is.null(fit_train$pred_prob_test)){
    pred_test_orig = fit_train$pred_prob_test %>%
      rownames_to_column("rows") %>%
      left_join(data_test %>%
                  select(y) %>%
                  rownames_to_column("rows"), by = "rows") %>%
      mutate(y = as.character(y)) %>%
      rename(y_true = y)
  } else {
    pred_test_orig = pred_test = NULL
  }
  
  # evaluate class labels with fixed threshold
  if (prob_thresh != "best"){
    pred_train_orig = pred_train_orig %>%
      mutate(y_pred = ifelse(Prob >= prob_thresh, 1, 0) %>% as.character(),
             threshold = prob_thresh)
    
    if (!is.null(pred_test_orig)){
      pred_test_orig = pred_test_orig %>%
        mutate(y_pred = ifelse(Prob >= prob_thresh, 1, 0) %>% as.character(),
               threshold = prob_thresh)
    }
    best_thr = prob_thresh
  }
  
  # evaluate class labels with best threshold
  best_thr_res = NULL
  if (prob_thresh == "best"){
    best_out_train = find_best_threshold(df_pred = pred_train_orig, prob_thresh_perf = prob_thresh_perf, prob_thresh_perf_minimize = prob_thresh_perf_minimize)
    if (!is.null(fit_train$pred_prob_test)){
      best_out_test = find_best_threshold(df_pred = pred_test_orig, prob_thresh_perf = prob_thresh_perf, prob_thresh_perf_minimize = prob_thresh_perf_minimize)
    }
    
    if (prob_thresh_set == "train"){
      best_thr = best_out_train$best_threshold
      best_thr_res = best_out_train$threshold_results
    } else if (prob_thresh_set == "test"){
      best_thr = best_out_test$best_threshold
      best_thr_res = best_out_test$threshold_results
    }
    
    pred_train_orig = pred_train_orig %>%
      mutate(y_pred = ifelse(Prob >= best_thr, 1, 0) %>% as.character(),
             threshold = best_thr)
    if (!is.null(pred_test_orig)){
      pred_test_orig = pred_test_orig %>%
        mutate(y_pred = ifelse(Prob >= best_thr, 1, 0) %>% as.character(),
               threshold = best_thr)
    }
  }
  
  # subset predictions
  if (!is.null(train_subset)){
    pred_train = pred_train_orig[train_subset, ]
  } else {
    pred_train = pred_train_orig
  }
  if (!is.null(pred_test_orig) & !is.null(test_subset)){
    pred_test = pred_test_orig[test_subset, ]
  } else {
    pred_test = pred_test_orig
  }
  
  perf_metrics = data.frame(AUC_train = MLmetrics::AUC(y_pred = pred_train$Prob, y_true = pred_train$y_true),
                            AUC_test = ifelse(!is.null(pred_test), MLmetrics::AUC(y_pred = pred_test$Prob, y_true = pred_test$y_true), NA),
                            F1_train = MLmetrics::F1_Score(y_true = pred_train$y_true, y_pred = pred_train$y_pred, positive = "1"),
                            F1_test = ifelse(!is.null(pred_test), MLmetrics::F1_Score(y_true = pred_test$y_true, y_pred = pred_test$y_pred, positive = "1"), NA),
                            Precision_train = MLmetrics::Precision(y_true = pred_train$y_true, y_pred = pred_train$y_pred, positive = "1"),
                            Precision_test = ifelse(!is.null(pred_test), MLmetrics::Precision(y_true = pred_test$y_true, y_pred = pred_test$y_pred, positive = "1"), NA),
                            Recall_train = MLmetrics::Recall(y_true = pred_train$y_true, y_pred = pred_train$y_pred, positive = "1"),
                            Recall_test = ifelse(!is.null(pred_test), MLmetrics::Recall(y_true = pred_test$y_true, y_pred = pred_test$y_pred, positive = "1"), NA),
                            Accuracy_train = MLmetrics::Accuracy(y_true = pred_train$y_true, y_pred = pred_train$y_pred),
                            Accuracy_test = ifelse(!is.null(pred_test), MLmetrics::Accuracy(y_true = pred_test$y_true, y_pred = pred_test$y_pred), NA))
  
  ROC_train = rocit(score= pred_train$Prob, class=pred_train$y_true)
  ROC_train = data.frame(x = ROC_train$FPR, y = ROC_train$TPR)
  if (!is.null(pred_test)){
    ROC_test = rocit(score= pred_test$Prob, class=pred_test$y_true)
    ROC_test = data.frame(x = ROC_test$FPR, y = ROC_test$TPR)
  } else {
    ROC_test = NULL
  }
  
  return(list(pred_train = pred_train,
              pred_test = pred_test,
              perf_metrics = perf_metrics,
              ROC_train = ROC_train,
              ROC_test = ROC_test,
              fit_train = fit_train,
              pred_train_orig = pred_train_orig,
              pred_test_orig = pred_test_orig,
              prob_threshold = best_thr,
              best_thr_res = best_thr_res)
  )
}

# oversample dataset with SMOTE
dataset_oversample = function(data, target_var, oversample_perc, categorical_regressor){
  
  set.seed(666)
  if (length(categorical_regressor) == 0){
    data_over <- suppressMessages(SMOTE(data = data %>%
                                          mutate(!!sym(target_var) := as.factor(!!sym(target_var))) %>%
                                          as.data.frame(),
                                        outcome = target_var, perc_maj = oversample_perc))
  } else {
    # it takes too much, we use SMOTE for dummies and round the results
    # data_over <- suppressMessages(SMOTE_NC(data = data %>%
    #                                          mutate(!!sym(target_var) := as.factor(!!sym(target_var))) %>%
    #                                          mutate_at(all_of(categorical_regressor), as.character) %>%
    #                                          as.data.frame(),
    #                                        outcome = target_var, perc_maj = oversample_perc))
    data_over <- suppressMessages(SMOTE(data = data %>%
                                          mutate(!!sym(target_var) := as.factor(!!sym(target_var))) %>%
                                          as.data.frame(),
                                        outcome = target_var, perc_maj = oversample_perc)) %>%
      mutate_at(all_of(categorical_regressor), function(x) round(x) %>% as.integer)
  }
  
  return(data_over)
}

# balance distribution of abi_ndg between train and test when y=1
balance_abi_ndg_class1 = function(data_train, data_test, abi_ndg_row_reference_class1, abi_ndg_row_index){
  
  # Balance distribution of abi_ndg with y=1 between train and test. Fix the problem of abi_ndg with multiple observations
  # data_train, data_test: data.frame of train and test set. Must have rownames with "row_123" and target variable "y"
  # abi_ndg_row_reference_class1: data.frame with "abi_ndg" and "row_ind" = string of corresponding "row_123" for abi_ndg, e.g. "23,24".
  #                               "abi_ndg" has unique values only. Only abi_ndg with target variable = 1
  # abi_ndg_row_index: data.frame with "row_ind" and "abi_ndg", where "row_ind" = "row_123". "abi_ndg" is repeated over different "row_ind"
  #
  # Output: list of (data_train_new, data_test_new, summary_movement)
  
  # set up main working dataset
  data_work = data_train %>%
    rownames_to_column("row_ind") %>%
    mutate(set = "train") %>%
    bind_rows(
      data_test %>%
        rownames_to_column("row_ind") %>%
        mutate(set = "test"))
  
  # subset y=1
  test_1 = data_test %>%
    filter(y == 1) %>%
    rownames_to_column("row_ind") %>%
    left_join(abi_ndg_row_index, by = "row_ind")
  
  train_1 = data_train %>%
    filter(y == 1) %>%
    rownames_to_column("row_ind") %>%
    left_join(abi_ndg_row_index, by = "row_ind")
  
  # find unbalanced observations
  check_balance = train_1 %>%
    select(abi_ndg) %>%
    group_by(abi_ndg) %>%
    summarize(train_obs = n()) %>%
    full_join(
      test_1 %>%
        select(abi_ndg) %>%
        group_by(abi_ndg) %>%
        summarize(test_obs = n()), by = "abi_ndg") %>%
    replace(is.na(.), 0) %>%
    mutate(unbalance = ifelse((train_obs == 0 & test_obs > 1) | (train_obs > 1 & test_obs == 0), "yes", "no"))
  
  # unbalanced observation
  df_unbalance = check_balance %>%
    filter(unbalance == "yes") %>%
    left_join(abi_ndg_row_reference_class1, by = "abi_ndg")
  
  if (nrow(df_unbalance) > 0){
    # observations in one single set -> candidates to replace movement of unbalanced observations
    df_free_to_move = check_balance %>%
      filter(unbalance == "no") %>%
      filter(train_obs == 0 | test_obs == 0) %>%
      mutate(from = ifelse(train_obs == 0, "test", "train")) %>%
      mutate(to = ifelse(from == "test", "train", "test")) %>%
      left_join(abi_ndg_row_reference_class1, by = "abi_ndg") %>%
      group_by(abi_ndg) %>%
      slice(rep(1:n(), first(length(strsplit(row_ind, "\\,")[[1]])))) %>%
      mutate(index = strsplit(row_ind, "\\,")[[1]] %>% as.numeric()) %>%
      ungroup() %>%
      arrange(abi_ndg, index) %>%  # for reproducibility
      mutate(n_ord = 1:n())
    
    # find remapping rows for unbalanced abi_ndg in train and test - always move first index by row_ind (to ensure reproducibility)
    index_remapping = c()
    for (i in 1:nrow(df_unbalance)){
      row = df_unbalance[i, ]
      train_obs = row$train_obs
      test_obs = row$test_obs
      abi_index = strsplit(row$row_ind, "\\,")[[1]] %>% as.numeric()
      obs_to_move = as.integer((train_obs + test_obs) / 2)
      index_to_move = abi_index[1:obs_to_move]
      
      if (train_obs > test_obs){
        index_remapping = index_remapping %>%
          bind_rows(data.frame(index = index_to_move, from = "train", to = "test", stringsAsFactors = F))
      } else {
        index_remapping = index_remapping %>%
          bind_rows(data.frame(index = index_to_move, from = "test", to = "train", stringsAsFactors = F))
      }
    }
    index_remapping = index_remapping %>%
      mutate(type = "unbalance")
    
    total_train_to_test = (index_remapping$from == "train") %>% sum()
    total_test_to_train = nrow(index_remapping) - total_train_to_test
    
    # find replacement to balance the remapping from df_free_to_move
    if (total_train_to_test > 0){
      index_remapping = index_remapping %>%
        bind_rows(df_free_to_move %>%
                    filter(from == "test") %>%
                    arrange(n_ord) %>%
                    filter(row_number() <= total_train_to_test) %>%
                    select(index, from, to) %>%
                    mutate(type = "free"))
    }
    if (total_test_to_train > 0){
      index_remapping = index_remapping %>%
        bind_rows(df_free_to_move %>%
                    filter(from == "train") %>%
                    arrange(n_ord) %>%
                    filter(row_number() <= total_test_to_train) %>%
                    select(index, from, to) %>%
                    mutate(type = "free"))
    }
    
    # apply observations switching
    data_work = data_work %>%
      left_join(index_remapping %>%
                  mutate(row_ind = paste0("row_", index)) %>%
                  rename(final_set = to) %>%
                  select(row_ind, final_set), by = "row_ind") %>%
      mutate(final_set = ifelse(is.na(final_set), set, final_set)) %>%
      column_to_rownames("row_ind")
    
    data_train_new = data_work %>%
      filter(final_set == "train") %>%
      select(colnames(data_train))
    data_test_new = data_work %>%
      filter(final_set == "test") %>%
      select(colnames(data_test))
    
    # if (abs(nrow(data_train) - nrow(data_train_new)) + abs(nrow(data_test) - nrow(data_test_new)) !=
    #     2*abs(sum(index_remapping$type == "free") - sum(index_remapping$type == "unbalance"))){
    #   cat('\n\n########### error in balancing train and test folds')
    # }
    
    # further check for final balancing
    test_1_new = data_test_new %>%
      filter(y == 1) %>%
      rownames_to_column("row_ind") %>%
      left_join(abi_ndg_row_index, by = "row_ind")
    
    train_1_new = data_train_new %>%
      filter(y == 1) %>%
      rownames_to_column("row_ind") %>%
      left_join(abi_ndg_row_index, by = "row_ind")
    
    
    check_balance_new = train_1_new %>%
      select(abi_ndg) %>%
      group_by(abi_ndg) %>%
      summarize(train_obs = n()) %>%
      full_join(
        test_1_new %>%
          select(abi_ndg) %>%
          group_by(abi_ndg) %>%
          summarize(test_obs = n()), by = "abi_ndg") %>%
      replace(is.na(.), 0) %>%
      mutate(unbalance = ifelse((train_obs == 0 & test_obs > 1) | (train_obs > 1 & test_obs == 0), "yes", "no"))
    if (sum(check_balance_new$unbalance == "yes") > 0){cat('\n\n########### error in final balanced train and test folds')}
    
    summary_movement = table(data_train$y) %>% as.data.frame() %>% mutate(set = "train") %>% spread(Var1, Freq) %>%
      bind_rows(
        table(data_train_new$y) %>% as.data.frame() %>% mutate(set = "train_new") %>% spread(Var1, Freq),
        table(data_test$y) %>% as.data.frame() %>% mutate(set = "test") %>% spread(Var1, Freq),
        table(data_test_new$y) %>% as.data.frame() %>% mutate(set = "test_new") %>% spread(Var1, Freq)
      )
    
  } else {
    data_train_new = data_train
    data_test_new = data_test
    summary_movement = NULL
  } # nrow(df_unbalance) > 0
  if (nrow(data_train) + nrow(data_test) != nrow(data_train_new) + nrow(data_test_new)){cat('\n\n########### error in balancing: rows missing')}
  
  return(list(data_train_new = data_train_new,
              data_test_new = data_test_new,
              summary_movement = summary_movement))
}

# fit Random Forest with ranger
fit_RandomForest = function(data_train, data_test, num.trees, mtry, min.node.size){
  
  # data_train: must contain target variable as "y" and factor
  # data_test: same columns of data_train ("y" not required)
  
  # class weights
  fraction_0 <- 1 - sum(data_train$y == 0) / nrow(data_train)
  fraction_1 <- 1 - sum(data_train$y == 1) / nrow(data_train)
  weights = c("0" = fraction_0, "1" = fraction_1)    # names are useless, class order matters
  
  # fit model
  fit <- ranger(y ~ ., data = data_train, num.trees = num.trees, mtry = mtry, min.node.size = min.node.size,
                probability = T, replace = F, class.weights = weights,
                seed = 66, save.memory = FALSE)
  
  # predict on train and test
  pred_prob_train = predict(fit, data=data_train, type = "response")$predictions[, 2] %>%
    data.frame() %>%
    setNames("Prob") %>%
    `rownames<-`(rownames(data_train))
  if (!is.null(data_test)){
    pred_prob_test = predict(fit, data=data_test, type = "response")$predictions[, 2] %>%
      data.frame() %>%
      setNames("Prob") %>%
      `rownames<-`(rownames(data_test))
  } else {
    pred_prob_test = NULL
  }
  
  return(list(fit = fit,
              pred_prob_train = pred_prob_train,
              pred_prob_test = pred_prob_test,
              options = list(weights = weights,
                             num.trees = num.trees,
                             mtry = mtry,
                             min.node.size = min.node.size)))
}

# fit MARS
fit_MARS = function(data_train, data_test, degree){
  
  # http://www.milbo.org/doc/earth-notes.pdf
  # data_train: must contain target variable as "y" and factor
  # data_test: same columns of data_train ("y" not required)
  
  # fit model
  fit <- earth(y ~ ., data = data_train, degree = degree, glm=list(family=binomial))
  
  # predict on train and test
  pred_prob_train = predict(fit, newdata=data_train, type = "response") %>%
    data.frame() %>%
    setNames("Prob") %>%
    `rownames<-`(rownames(data_train))
  if (!is.null(data_test)){
    pred_prob_test = predict(fit, newdata=data_test, type = "response") %>%
      data.frame() %>%
      setNames("Prob") %>%
      `rownames<-`(rownames(data_test))
  } else {
    pred_prob_test = NULL
  }
  
  # save coefficients
  coeff = fit$glm.coefficients %>%
    as.data.frame() %>%
    setNames("Coeff") %>%
    rownames_to_column("Variable")
  
  # save feature importance
  features_importance = evimp(fit, trim=FALSE) %>%
    unclass() %>%
    data.frame()
  
  # used variables
  used_variables = (fit$dirs %>% colnames())[features_importance %>% filter(used == 1) %>% pull(col)]
  
  # termination condition
  oo = capture.output(print(fit))
  term_cond = oo[which(sapply(oo, function(x) grepl("Termination condition", x), USE.NAMES = F))]
  
  return(list(fit = fit,
              pred_prob_train = pred_prob_train,
              pred_prob_test = pred_prob_test,
              coeff = coeff,
              features_importance = features_importance,
              used_variables = used_variables,
              term_cond = term_cond,
              options = list(degree = degree)))
}

# fit SVM with RBF
fit_SVM_RBF = function(data_train, data_test, sigma, C, scaled = F){
  
  # https://cran.r-project.org/web/packages/kernlab/kernlab.pdf
  # data_train: must contain target variable as "y" and factor
  # data_test: same columns of data_train ("y" not required)
  
  # fit model
  fit <-  ksvm(y~., data = data_train, type = "C-svc", prob.model = T, scaled = scaled,
               kernel="rbfdot", kpar=list(sigma = sigma), C=C)
  
  # predict on train and test
  pred_prob_train = predict(fit, newdata = data_train, type = "probabilities")[,2] %>%
    data.frame() %>%
    setNames("Prob") %>%
    `rownames<-`(rownames(data_train))
  if (!is.null(data_test)){
    pred_prob_test = predict(fit, newdata = data_test, type = "probabilities")[,2] %>%
      data.frame() %>%
      setNames("Prob") %>%
      `rownames<-`(rownames(data_test))
  } else {
    pred_prob_test = NULL
  }
  
  return(list(fit = fit,
              pred_prob_train = pred_prob_train,
              pred_prob_test = pred_prob_test,
              options = list(sigma = sigma,
                             C = C,
                             scaled = scaled)))
}

# fit model with or without cross-validation
fit_model_with_cv = function(df_work, cv_ind, algo_type, parameter_set, non_tunable_param = NULL, no_cv_train_ind = NULL, no_cv_test_ind = NULL,
                             prob_thresh_cv = 0.5, tuning_crit = "F1_test", tuning_crit_minimize = F,
                             balance_abi_ndg_fold = F, abi_ndg_row_reference_class1 = NULL, abi_ndg_row_index = NULL,
                             train_downsample_perc = NULL){
  
  # fit model on each fold, evaluate class based on threshold prob_thresh_cv and return final performance tuning_crit
  
  # df_work: full dataset to sample fold from. Target variable must be "y".
  # cv_ind: dataframe ["ind", "fold"] of index for each fold. Unused if no_cv_train_ind != NULL or no_cv_test_ind != NULL
  # no_cv_train_ind: index for train set for fitting without cross-validation
  # no_cv_test_ind: index for test set for fitting without cross-validation
  # algo_type: algorithm to be fitted. "Elastic-net", "Random_Forest", "MARS", "SVM-RBF"
  # parameter_set: named list of parameters for algo_type
  # non_tunable_param: list of additional parameters not to be tuned - used only when tuning.
  # prob_thresh_cv: threshold to convert probabilities into class. If "best" optimal value is evaluated according to tuning_crit
  # tuning_crit: criterion to be optimized - "AUC" or "Precision" or "Recall" or "Accuracy" for "_test" or "_train"
  # tuning_crit_minimize: whether to minimize or maximize tuning_crit
  # balance_abi_ndg_fold: if TRUE balance distribution of abi_ndg between train and test when y=1 in cross-validation using balance_abi_ndg_class1()
  # abi_ndg_row_reference_class1, abi_ndg_row_index: input for balance_abi_ndg_class1()
  # train_downsample_perc: if not NULL downsample train set for y=0. New y=0 will account to train_downsample_perc*n_total_0    # todo va eventualmente inclusa come input nella funzione di tuning
  
  # check target variable
  if (!"y" %in% colnames(df_work)){stop('No target variable "y" found')}
  
  tuned_param = parameter_set
  parameter_set = c(parameter_set, non_tunable_param)
  tuning_crit_set = strsplit(tuning_crit, "_")[[1]][2]   # "train" or "test"
  tuning_crit_perf = strsplit(tuning_crit, "_")[[1]][1]   # "F1", "Accuracy", etc
  
  # check cross-validation or single fold fitting
  if (!is.null(no_cv_train_ind) | !is.null(no_cv_test_ind)){
    cv_ind = data.frame(fold = 2, ind = no_cv_train_ind) %>% # test will always have fold == fold_i
      bind_rows(data.frame(fold = rep(1,length(no_cv_test_ind)), ind = no_cv_test_ind))
    total_folds = 1
  } else {
    total_folds = unique(cv_ind$fold) %>% sort()
  }
  
  fold_prediction = c()
  fold_model_fit = list()
  start_time = Sys.time()
  for (fold_i in total_folds){
    test_ind = cv_ind %>%
      filter(fold == fold_i) %>%
      pull(ind)
    train_ind = cv_ind %>%
      filter(fold != fold_i) %>%
      pull(ind)
    data_test = df_work[test_ind, ]
    data_train = df_work[train_ind, ]# %>%
    # group_by(y) %>%
    # filter(row_number() <= 1500) %>%   # todo: rimuovi
    # ungroup()
    if (nrow(data_test) == 0){data_test = NULL}
    
    # downsample train set for y=0
    if (!is.null(train_downsample_perc)){
      obs_0_to_remove = (sum(data_train$y == 0) * (1 - train_downsample_perc)) %>% as.integer()
      set.seed(66)
      index_to_remove = sample(which(data_train$y == 0), obs_0_to_remove, replace = F)
      data_train = data_train[-index_to_remove, ]
    }
    
    # balance distribution of abi_ndg between train and test when y=1
    if (balance_abi_ndg_fold & nrow(data_test) != 0){
      bal = balance_abi_ndg_class1(data_train, data_test, abi_ndg_row_reference_class1, abi_ndg_row_index)
      data_train = bal$data_train_new
      data_test = bal$data_test_new
    }
    
    # return column data.frame "Prob" of predicted probabilities for train and test set
    if (algo_type == "Elastic-net"){
      
      alpha = parameter_set$alpha
      standardize = parameter_set$standardize
      intercept = parameter_set$intercept
      parallel = parameter_set$parallel
      type.measure = parameter_set$type.measure
      lambda_final = parameter_set$lambda_final
      family = parameter_set$family
      n_fold_cvgmlnet = parameter_set$n_fold_cvgmlnet
      fixed_variables = parameter_set$fixed_variables
      
      if (is.null(data_test)){
        data_test_glmnet = NULL
      } else {
        data_test_glmnet = data_test %>% select(-y)
      }
      fit_train = fit_cv_glmnet(x = data_train %>% select(-y), y = data_train$y, alpha = alpha, standardize = standardize, intercept = intercept, parallel = parallel,
                                type.measure = type.measure, lambda_final = lambda_final, family = family,
                                fixed_variables = fixed_variables, n_fold_cvgmlnet = n_fold_cvgmlnet, x_test_predict = data_test_glmnet)
      
    } else if (algo_type == "Random_Forest"){
      
      num.trees = parameter_set$num.trees
      mtry = parameter_set$mtry
      min.node.size = parameter_set$min.node.size
      
      fit_train = fit_RandomForest(data_train = data_train %>% mutate(y = as.factor(y)), data_test = data_test,
                                   num.trees = num.trees, mtry = mtry, min.node.size = min.node.size)
      
    } else if (algo_type == "MARS"){
      
      degree = parameter_set$degree
      
      fit_train = fit_MARS(data_train = data_train %>% mutate(y = as.factor(y)), data_test = data_test, degree = degree)
      
    } else if (algo_type == "SVM-RBF"){
      
      sigma = parameter_set$sigma
      C = parameter_set$C
      scaled = parameter_set$scaled
      
      fit_train = fit_SVM_RBF(data_train = data_train %>% mutate(y = as.factor(y)), data_test = data_test, sigma = sigma, C = C, scaled = scaled)
      
    }
    prob_train = fit_train$pred_prob_train
    prob_test = fit_train$pred_prob_test
    fold_model_fit[[paste0("fold_", fold_i)]] = fit_train
    
    # save fold prediction for both train and test - df with "set" = "train"/"test" and "Prob" and "y_true"
    pred_to_bind = prob_train %>%
      rownames_to_column("rows") %>%
      left_join(data_train %>%
                  select(y) %>%
                  rownames_to_column("rows"), by = "rows") %>%
      mutate(set = "train")
    
    if (!is.null(data_test)){
      pred_to_bind = pred_to_bind %>%
        bind_rows(
          prob_test %>%
            rownames_to_column("rows") %>%
            left_join(data_test %>%
                        select(y) %>%
                        rownames_to_column("rows"), by = "rows") %>%
            mutate(set = "test")
        )
    }
    pred_to_bind = pred_to_bind %>%
      mutate(y = as.character(y)) %>%
      rename(y_true = y) %>%
      select(-rows) %>%
      mutate(fold = fold_i)
    
    fold_prediction = fold_prediction %>%
      bind_rows(pred_to_bind)
  } # fold_i
  if (nrow(fold_prediction) != fold_i * nrow(df_work) & is.null(no_cv_train_ind) & is.null(train_downsample_perc)){
    cat('\n\n###### mismatch in fold evaluation: total observation mismatch - ', algo_type, '\n')
    print(unlist(parameter_set))}
  
  # select set to evaluate tuning_crit
  final_performance = fold_prediction %>%
    filter(set == tuning_crit_set)
  
  # evaluate optimal threshold
  df_thresh = c()
  for (fold_i in total_folds){
    if (prob_thresh_cv == "best"){
      thresh_set = NULL
    } else {
      thresh_set = prob_thresh_cv
    }
    tt = find_best_threshold(final_performance %>%
                               filter(fold == fold_i), prob_thresh_perf = tuning_crit_perf,
                             prob_thresh_perf_minimize = tuning_crit_minimize, thresh_set = thresh_set)
    tt = tt$threshold_results %>% rename(!!sym(paste0("fold_", fold_i)) := perf)
    if (is.null(df_thresh)){
      df_thresh = tt
    } else {
      df_thresh = df_thresh %>%
        full_join(tt, by = "threshold")
    }
  } # fold_i
  df_thresh = df_thresh %>%
    drop_na() %>%
    mutate(perf = rowMeans(select(., -threshold), na.rm = T))
  df_thresh = df_thresh %>%
    filter(is.finite(perf)) %>%
    arrange(desc(perf))
  if (tuning_crit_minimize){
    df_thresh = df_thresh %>%
      arrange(perf)
  }
  best_threshold = df_thresh$threshold[1]
  
  # evaluate all performance on both train and test with best threshold
  fold_all_performance = c()
  list_ROC = list()
  for (fold_i in total_folds){
    pred_train = fold_prediction %>%
      filter(set == "train" & fold == fold_i) %>%
      mutate(y_pred = ifelse(Prob >= best_threshold, 1, 0) %>% as.character())
    pred_test = fold_prediction %>%
      filter(set == "test" & fold == fold_i) %>%
      mutate(y_pred = ifelse(Prob >= best_threshold, 1, 0) %>% as.character())
    
    fold_all_performance = fold_all_performance %>%
      bind_rows(
        data.frame(tuned_param) %>%
          bind_cols(
            data.frame(threshold = best_threshold, fold = fold_i, obs_train = nrow(pred_train), obs_test = max(c(0, nrow(pred_test))),
                       perc_1_train = round(sum(pred_train$y_true == "1") / nrow(pred_train), 2),
                       perc_1_test = round(sum(pred_test$y_true == "1") / nrow(pred_test), 2),
                       AUC_train = MLmetrics::AUC(y_pred = pred_train$Prob, y_true = pred_train$y_true),
                       AUC_test = ifelse(nrow(pred_test) != 0, MLmetrics::AUC(y_pred = pred_test$Prob, y_true = pred_test$y_true), NA),
                       F1_train = MLmetrics::F1_Score(y_true = pred_train$y_true, y_pred = pred_train$y_pred, positive = "1"),
                       F1_test = ifelse(nrow(pred_test) != 0, MLmetrics::F1_Score(y_true = pred_test$y_true, y_pred = pred_test$y_pred, positive = "1"), NA),
                       Precision_train = MLmetrics::Precision(y_true = pred_train$y_true, y_pred = pred_train$y_pred, positive = "1"),
                       Precision_test = ifelse(nrow(pred_test) != 0, MLmetrics::Precision(y_true = pred_test$y_true, y_pred = pred_test$y_pred, positive = "1"), NA),
                       Recall_train = MLmetrics::Recall(y_true = pred_train$y_true, y_pred = pred_train$y_pred, positive = "1"),
                       Recall_test = ifelse(nrow(pred_test) != 0, MLmetrics::Recall(y_true = pred_test$y_true, y_pred = pred_test$y_pred, positive = "1"), NA),
                       Accuracy_train = MLmetrics::Accuracy(y_true = pred_train$y_true, y_pred = pred_train$y_pred),
                       Accuracy_test = ifelse(nrow(pred_test) != 0, MLmetrics::Accuracy(y_true = pred_test$y_true, y_pred = pred_test$y_pred), NA), stringsAsFactors = F)
          )
      )
    
    # evaluate predicted class for all folds
    fold_prediction = fold_prediction %>%
      mutate(y_pred = ifelse(Prob >= best_threshold, 1, 0) %>% as.character(),
             threshold = best_threshold)
    
    ROC_train = rocit(score= pred_train$Prob, class=pred_train$y_true)
    ROC_train = data.frame(x = ROC_train$FPR, y = ROC_train$TPR)
    if (nrow(pred_test) != 0){
      ROC_test = rocit(score= pred_test$Prob, class=pred_test$y_true)
      ROC_test = data.frame(x = ROC_test$FPR, y = ROC_test$TPR)
    } else {
      ROC_test = NULL
    }
    list_ROC[[paste0("fold_", fold_i)]] = list(ROC_train = ROC_train,
                                               ROC_test = ROC_test)
  } # fold_i
  
  tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
  total_time = paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff)))
  
  return(list(best_threshold = best_threshold,
              optim_perf = df_thresh$perf[1],
              threshold_list = df_thresh,
              fold_all_performance = fold_all_performance,
              fold_prediction = fold_prediction,
              list_ROC = list_ROC,
              fold_model_fit = fold_model_fit,
              parameter_set = parameter_set,
              tuned_param = tuned_param,
              total_time = total_time))
}

# Bayesian tuning machine learning
ml_tuning = function(df_work, algo_type, cv_ind, prob_thresh_cv, tuning_crit = "F1_test", tuning_crit_minimize = F,
                     save_RDS_additional_lab = '',
                     rds_folder = './Distance_to_Default/Checkpoints/ML_model/',
                     balance_abi_ndg_fold = F, abi_ndg_row_reference_class1 = NULL, abi_ndg_row_index = NULL){
  
  # https://mlrmbo.mlr-org.com/articles/supplementary/mixed_space_optimization.html
  
  # df_work: dataset with target variable ("y") and predictors
  # algo_type: used in fit_model_with_cv()
  # cv_ind: dataframe ["ind", "fold"] of index for each fold.
  # no_cv_train_ind: index for train set for fitting without cross-validation
  # prob_thresh_cv: threshold to convert probabilities into class. If "best" optimal value is evaluated according to tuning_crit
  # tuning_crit: criterion to be optimized - "AUC" or "Precision" or "Recall" or "Accuracy" for "_test" or "_train"
  # tuning_crit_minimize: whether to minimize or maximize tuning_crit
  # save_RDS_additional_lab: adds prefix to saved rds for parameters combination reloading
  # rds_folder: folder for checkpoints - should finish with "/"
  # balance_abi_ndg_fold, abi_ndg_row_reference_class1, abi_ndg_row_index: balance abi_ndg when y=1. See fit_model_with_cv()
  
  # bayes_options: list of options
  #                - par.set: tunable parameters (makeParamSet)
  #                - non_tunable_param: non-tunable parameters (list)
  #                - max_iter: maximum iterations after initial design
  #                - design_iter: initial design iterations
  n_vars = ncol(df_work) - 1   # df_work has target variable
  n_obs = cv_ind %>%
    group_by(fold) %>%
    summarise(obs = n(), .groups = "drop") %>%
    pull(obs) %>%
    min()
  bayes_parameter_set = list(
    `Elastic-net` = list(
      par.set = makeParamSet(
        makeNumericParam("alpha", 0, 1)
      ),
      non_tunable_param = list(standardize = F, 
                               intercept = T, 
                               parallel = T,
                               type.measure = "auc",
                               lambda_final = "lambda.1se",
                               family = "binomial",
                               n_fold_cvgmlnet = 10,
                               fixed_variables = fixed_variables),
      max_iter = 20,   # todo: rimetti
      design_iter = 10   # todo: rimetti
    ),
    
    Random_Forest = list(
      par.set = makeParamSet(
        makeIntegerParam("num.trees", 50, 500),#makeIntegerParam("num.trees", 50, 500),   # todo: rimetti
        makeIntegerParam("mtry", 1, (n_vars - 1)),
        makeIntegerParam("min.node.size", 1, as.integer(n_obs / 2))
      ),
      non_tunable_param = NULL,
      max_iter = 30,   # todo: rimetti
      design_iter = 20   # todo: rimetti
    ),
    
    MARS = list(
      par.set = makeParamSet(
        makeIntegerParam("degree", 1, 5)
      ),
      non_tunable_param = NULL,
      max_iter = 1,
      design_iter = 5
    ),
    
    `SVM-RBF` = list(
      par.set = makeParamSet(
        makeNumericParam("sigma", 0.0001, 10),
        makeNumericParam("C", 0.0001, 100)
      ),
      non_tunable_param = list(scaled = T),
      max_iter = 20,
      design_iter = 10
    )
  )
  bayes_options = bayes_parameter_set[[algo_type]]
  par.set = bayes_options$par.set
  non_tunable_param = bayes_options$non_tunable_param
  max_iter = bayes_options$max_iter
  design_iter = bayes_options$design_iter
  tunable_names = names(par.set$pars)
  
  # check if rds_folder ends with "/"
  if (substr(rds_folder, nchar(rds_folder), nchar(rds_folder)) != "/"){rds_folder = paste0(rds_folder, "/")}
  
  # save RDS with vector of rds path to reload model_fit in order to retrieve all folds performance fold_all_performance from fit_model_with_cv()
  saveRDS(c(), paste0(rds_folder, "current_model_path_list.rds"))
  
  # function to be optimized
  fun = function(x) {
    
    # x is the names list of parameters and their values
    
    p_names = x %>% unlist() %>% names()
    p_values = x %>% unlist()
    p_order = match(p_names, tunable_names)
    param_compact_label = paste0(p_names[p_order], "_", p_values[p_order]) %>% paste0(collapse = ".")
    
    out_label = paste0(ifelse(save_RDS_additional_lab != '', paste0(save_RDS_additional_lab, '_'), ''),
                       param_compact_label, '.rds')
    reload_err = try(model_fit <- suppressWarnings(readRDS(paste0(rds_folder, out_label))), silent = T)
    
    if (class(reload_err) == "try-error"){
      model_fit = fit_model_with_cv(df_work = df_work, cv_ind = cv_ind, algo_type = algo_type,
                                    parameter_set = x, non_tunable_param = non_tunable_param,
                                    no_cv_train_ind = NULL, no_cv_test_ind = NULL,
                                    prob_thresh_cv = prob_thresh_cv, tuning_crit = tuning_crit, tuning_crit_minimize = tuning_crit_minimize,
                                    balance_abi_ndg_fold = balance_abi_ndg_fold,
                                    abi_ndg_row_reference_class1 = abi_ndg_row_reference_class1, abi_ndg_row_index = abi_ndg_row_index)
      model_fit[["fold_model_fit"]] = NULL  # remove fitted model to save space
      model_fit$param_compact_label = param_compact_label
      saveRDS(model_fit, paste0(rds_folder, out_label))
    }
    
    # add model path to current_model_path_list
    current_model_path_list = readRDS(paste0(rds_folder, "current_model_path_list.rds"))
    saveRDS(c(current_model_path_list, paste0(rds_folder, out_label)), paste0(rds_folder, "current_model_path_list.rds"))
    
    perf = model_fit$optim_perf
    
    return(perf)
  }
  
  # set parameter space
  objfun = makeSingleObjectiveFunction(
    name = "ML_tuning",
    fn = fun,
    par.set = par.set,
    has.simple.signature = FALSE,
    minimize = TRUE
  )
  
  # set surrogate and number of design
  surr.rf = makeLearner("regr.randomForest", predict.type = "se")
  
  control = makeMBOControl()
  control = setMBOControlInfill(
    control = control,
    crit = makeMBOInfillCritAdaCB(cb.lambda.start = 4, cb.lambda.end = 0.1)
  )
  control = setMBOControlTermination(
    control = control,
    iters = max_iter    # maximum number of iteration, not including the ones used to generate the design
  )
  
  design = suppressWarnings(generateDesign(n = design_iter, par.set = getParamSet(objfun)))
  
  # run optimizer
  start = Sys.time()
  mlr::configureMlr(show.info = FALSE, show.learner.output = FALSE, on.learner.warning = "quiet")
  results = suppressWarnings(mbo(objfun, design = design, learner = surr.rf, control = control, show.info = F))
  tot_diff=seconds_to_period(difftime(Sys.time(),start, units='secs'))
  
  # reload fold performance for all combinations (both single fold and folds' average)
  current_model_path_list = readRDS(paste0(rds_folder, "current_model_path_list.rds"))
  oo = file.remove(paste0(rds_folder, "current_model_path_list.rds"))
  if (length(current_model_path_list) != nrow(as.data.frame(results$opt.path))){cat("\n #######", algo_type, "error: number of saved models doesnt't match optimization combinations")}
  fold_all_performance = c()
  for (path in current_model_path_list){
    model_fit = readRDS(path)
    fold_all_performance = fold_all_performance %>%
      bind_rows(model_fit$fold_all_performance %>%
                  mutate(rds = path,
                         param_compact_label = model_fit$param_compact_label)) %>%
      unique()
  }
  fold_all_performance_avg = fold_all_performance %>%
    select(-starts_with("obs_"), -starts_with("perc_1_"), -fold) %>%
    group_by_at(c(tunable_names, "threshold", "rds", "param_compact_label")) %>%
    summarize_all(list(avg = function(x) mean(x, na.rm = T),
                       std = function(x) sd(x, na.rm = T))) %>%
    select(names(.) %>% sort()) %>%
    select(all_of(tunable_names), threshold, everything()) %>%
    relocate(param_compact_label, .after = last_col()) %>%
    relocate(rds, .after = last_col()) %>%
    unique()
  
  # save results
  optimization_results = as.data.frame(results$opt.path, stringsAsFactors = F) %>%
    mutate(
      final_state = results$final.state,
      total_time = paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff)))) %>%
    rename(!!sym(tuning_crit) := y) %>%
    left_join(fold_all_performance_avg, by = tunable_names) %>%
    select(all_of(tunable_names), threshold, everything()) %>%
    mutate_if(is.factor, as.character) %>%
    unique()
  
  optimization_results_all_folds = as.data.frame(results$opt.path, stringsAsFactors = F) %>%
    mutate(
      final_state = results$final.state,
      total_time = paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff)))) %>%
    rename(!!sym(paste0("optim_", tuning_crit)) := y) %>%
    left_join(fold_all_performance, by = tunable_names) %>%
    select(all_of(tunable_names), threshold, fold, everything()) %>%
    mutate_if(is.factor, as.character) %>%
    unique()
  
  # write.table(optimization_results, paste0(rds_folder, '00_Optimization_list_', save_RDS_additional_lab, '.csv'), sep = ';', row.names = F, append = F, na = "")
  # write.table(optimization_results_all_folds, paste0(rds_folder, '01_Optimization_list_ALLFOLDS_', save_RDS_additional_lab, '.csv'), sep = ';', row.names = F, append = F, na = "")
  
  # evaluate best parameters set
  optimization_results = optimization_results %>%
    arrange(desc(!!sym(tuning_crit)))
  if (tuning_crit_minimize){
    df_thresh = df_thresh %>%
      arrange(!!sym(tuning_crit))
  }
  best_parameters = optimization_results %>%
    filter(row_number() == 1) %>%
    select(all_of(tunable_names))
  
  return(list(optimization_results = optimization_results,
              optimization_results_all_folds = optimization_results_all_folds,
              best_parameters = best_parameters,
              non_tunable_param = non_tunable_param))
}

# evaluate Shapley values
evaluate_SHAP = function(dataSample, sample_size = 100, trained_model_prediction_function = NULL, obs_index_to_evaluate = NULL,
                         obs_index_to_sample = NULL, obs_index_subset = NULL, adjust_shapley = FALSE, n_batch = 1, n_workers = 5, verbose = 1, seed = 66){

  # evaluate local SHAP values for single observations, global (signed) features effects, global SHAP features importance and input for plot_SHAP_summary().
  # https://christophm.github.io/interpretable-ml-book/shapley.html
  # https://christophm.github.io/interpretable-ml-book/shap.html

  # dataSample: data.frame of predictors ONLY.
  # sample_size: sample size to generate instances (coalitions) with shuffled features. The higher the more accurate the explanations become.
  # trained_model_prediction_function: named list of function(predictors) -> prediction (vector of values for regression,
  #                                   probabilities of "1" for binary classification).
  #                                   E.g. function(x){predict(rf, newdata = x)}. List names will be used as output label for each model.
  # obs_index_to_evaluate: integer vector - row index of observations to evaluate SHAP value. If NULL all observations will be used.
  # obs_index_to_sample: integer vector of index of rows to be used when sampling all coalitions. obs_index_to_evaluate must be a partition of obs_index_to_sample.
  #                      If NULL all observations will be used. It can be used to do stratified sampling and/or mitigate imbalance in dataset.
  # obs_index_subset: data.frame of "obs_index", "class". If not NULL contains class for each obs_index_to_evaluate. All outputs are then evaluated
  #                 for each class and all observations.
  # adjust_shapley: if TRUE adjust the sum of the estimated Shapley values to satisfy the local accuracy property, i.e. to equal the difference between the
  #               model's prediction for of each observation and the average prediction over all the dataset.
  # n_batch: number of batch to split the evaluation. May speed up evaluation and save memory.
  # n_workers: number of workers for parallel calculation. Try not to exceed 30-40.
  # verbose: 1 to display calculation time, 0 for silent.
  # seed: seed for reproducibility

  # Output:
  #   list of:
  #     - local_SHAP: complete list of SHAP values for each observation
  #                     data.frame with "feature", "phi", "phi_std", "feature_value", "obs_index", "model_name", "predicted_val", "predicted_val_avg"
  #                     "phi" and "phi_std" are the average and st.dev of SHAP values over all shuffled coalitions
  #                     "feature" is feature name and "feature_value" is the corresponding value from the original instance (obs_index reference)
  #                     "predicted_val" and "predicted_val_avg" are predicted value of single observation and columns average, respectively
  #     - global_features_effect: simple average of all local (single observation) SHAP values over all observation.
  #                              It is a signed features effect on predicted value.
  #                              data.frame with "model_name", "feature", "phi"
  #     - SHAP_feat_imp: SHAP feature importance. It's the average abs(SHAP values). Shows the magnitude of features effect on predicted value.
  #                     data.frame with "model_name", "feature", "phi"
  #     - summary_plot_data: input for plot_SHAP_summary()
  #     - type: "SHAP". Used in plot_feat_imp().
  # If obs_index_subset != NULL additional nested list is returned for each class and all observations with global_features_effect and SHAP_feat_imp,
  # local_SHAP and summary_plot_data have an additional column "class"

  # code adapted from https://github.com/christophM/iml/blob/master/R/Shapley.R


  # check for numeric predictors only
  invalid_col_type = dataSample %>% select_if(negate(is.numeric)) %>% colnames()
  if (length(invalid_col_type) > 0){
    oo = capture.output(print(sapply(dataSample %>% select(all_of(invalid_col_type)), class)))
    stop(paste0("Only numeric predictors supported:\n", paste0(oo, collapse = "\n")))
  }

  dataSample = dataSample %>% setDT()
  n_features = ncol(dataSample)
  feature_names = colnames(dataSample)
  samp_len = length(obs_index_to_sample)
  if (is.null(obs_index_to_evaluate)){obs_index_to_evaluate = c(1:nrow(dataSample))}
  if (is.null(obs_index_to_sample)){obs_index_to_sample = c(1:nrow(dataSample))}
  if (length(intersect(obs_index_to_evaluate, obs_index_to_sample)) != length(obs_index_to_evaluate)){
    stop('"obs_index_to_evaluate" must be a partition of "obs_index_to_sample"')
  }

  generate_sample_index = function(obs_index){
    # generate indices of samples to be used as coalitions trying not to use duplicates. If sample_size > length(obs_index_to_sample)
    # duplicates are introduced.
    # Returns indices vector of length sample_size

    set.seed(seed + obs_index)
    out = sample(obs_index_to_sample, min(c(sample_size, samp_len)), replace = FALSE)

    if (sample_size > samp_len){
      out = c(out, sample(obs_index_to_sample, sample_size - samp_len, replace = TRUE))
    }

    return(out)
  }

  generate_coalitions = function(obs_index){
    # generate coalitions with shuffled features
    # final sample will have ncol=n_features
    #                        nrow=sample_size*n_features*2  - first half rows are data with features x_+j,
    #                                                         second half is x_-j of algo definition at linked page
    # column with obs_index is added as well

    # select instance
    x.interest = dataSample[obs_index,] %>% as.data.frame()

    n_row = nrow(dataSample)
    runs <- lapply(1:sample_size, function(m) {

      # randomly order features
      set.seed(seed + obs_index*m)
      new.feature.order <- sample(1:n_features)

      # randomly choose sample instance from dataSample to shuffle features order
      sample.instance.shuffled <- dataSample[list_generated_sample[[as.character(obs_index)]][m],
                                             new.feature.order,
                                             with = FALSE
      ]
      # shuffle interest instance with same features order
      x.interest.shuffled <- x.interest[, new.feature.order]

      # create new instances (with and without) for each feature
      featurewise <- lapply(1:n_features, function(k) {
        k.at.index <- which(new.feature.order == k)
        # replace sample.instance.shuffled at features > k  (X_+j of algo definition at linked page "k" is "j")
        instance.with.k <- x.interest.shuffled
        if (k.at.index < ncol(x.interest)) {
          instance.with.k[, (k.at.index + 1):ncol(instance.with.k)] <-
            sample.instance.shuffled[, (k.at.index + 1):ncol(instance.with.k), with = FALSE]
        }
        # replace sample.instance.shuffled at features >= k  (X_-j of algo definition)
        instance.without.k <- instance.with.k
        instance.without.k[, k.at.index] <- sample.instance.shuffled[,k.at.index,with = FALSE]
        cbind(instance.with.k[, feature_names],instance.without.k[, feature_names])
      })
      data.table::rbindlist(featurewise)
    })
    runs <- data.table::rbindlist(runs)
    dat.with.k <- data.frame(runs[, 1:(ncol(runs) / 2)])
    dat.without.k <- data.frame(runs[, (ncol(runs) / 2 + 1):ncol(runs)])

    return(rbind(dat.with.k, dat.without.k) %>% mutate(obs_index = obs_index))
  }

  generate_shapley = function(list_index, trained_model_name){
    # generate Shapley values
    # list_index: index of the list that contains all generate_coalitions() and predictions "list_predicted_data". obs_index will be extracted inside so to avoid
    #             index mismatch when list_generated_coal doesn't keep the order.
    # trained_model_name: model name from trained_model_prediction_function
    #
    # Output: data.frame with "feature", "phi", "phi_std", "feature_value", "obs_index", "model_name", "predicted_val", "predicted_val_avg"
    #         "phi" and "phi_std" are the average and st.dev of SHAP values over all shuffled coalitions
    #         "feature" is feature name and "feature_value" is the corresponding value from the original instance (obs_index reference)
    #         "predicted_val" and "predicted_val_avg" are predicted value of single observation and columns average, respectively

    # select sampled data and predictions from generated list
    data_predicted = list_predicted_data[[list_index]]
    obs_index = data_predicted$obs_index %>% unique()
    var_names = data_predicted %>% select(-obs_index, -Prediction) %>% colnames()
    x.interest = dataSample[obs_index,] %>% as.data.frame()
    data_predicted = data_predicted %>% select(Prediction)

    # evaluate Phi
    # split prediction in yhat_x_+j and yhat_-j
    y.hat.with.k <- data_predicted[1:(nrow(data_predicted) / 2), , drop = FALSE]
    y.hat.without.k <- data_predicted[(nrow(data_predicted) / 2 + 1):nrow(data_predicted), , drop = FALSE]
    y.hat.diff <- y.hat.with.k - y.hat.without.k   # these are Phi_j^m  -> will be averaged after (m=1,...,sample_size)
    cnames <- colnames(y.hat.diff)
    y.hat.diff <- cbind(
      data.table(feature = rep(var_names, times = sample_size)),
      y.hat.diff
    )
    # average over all sampled observation (for each feature) - also include std
    y.hat.diff <- data.table::melt(y.hat.diff, variable.name = "class", value.name = "value", measure.vars = cnames)
    y.hat.diff <- y.hat.diff[, list("phi" = mean(value), "phi_std" = sd(value)), by = c("feature", "class")]
    y.hat.diff$class <- NULL
    # x.original <- unlist(lapply(x.interest[1, ], as.character))
    # y.hat.diff$feature_value <- rep(sprintf("%s=%s", colnames(x.interest), x.original), times = length(cnames))
    y.hat.diff = y.hat.diff %>%
      left_join(data.frame(unlist(x.interest[1, ]), stringsAsFactors = F) %>%
                  setNames("feature_value") %>%
                  rownames_to_column("feature"), by = "feature") %>%
      mutate(obs_index = obs_index,
             model_name = trained_model_name)

    return(y.hat.diff)
  }


  # suppress messages
  if (verbose == 0){sink(tempfile());on.exit(sink())}

  # generate samples indices for coalitions
  list_generated_sample = lapply(obs_index_to_evaluate, generate_sample_index)
  names(list_generated_sample) = as.character(obs_index_to_evaluate)


  #### loop for each batch of obs_index_to_evaluate

  options(future.globals.maxSize = 8000 * 1024^2)
  plan(multisession, workers = n_workers)
  list_split = split(obs_index_to_evaluate, sort(obs_index_to_evaluate %% n_batch))
  start_time = Sys.time()
  split_time_val = local_SHAP = c()
  cat('\nStart time:', as.character(Sys.time()), '\n')
  for (split_name in names(list_split)){

    # check average batch time
    split_time = Sys.time()
    if (split_name != names(list_split)[1]){
      avg_time = seconds_to_period(mean(split_time_val, na.rm = T))
      avg_time_label = paste0('- batch avg time: ', lubridate::hour(avg_time), 'h:', lubridate::minute(avg_time), 'm:', round(lubridate::second(avg_time)))
    } else {
      avg_time_label = ''
    }
    cat('Generating coalitions and SHAP values for batch', paste0(as.numeric(split_name)+1, '/', length(list_split)),
        ' | last timestamp:', as.character(Sys.time()), avg_time_label, end = '')

    # generate all sample to be used for all trained models
    tic()
    list_generated_coal <- future_lapply(list_split[[split_name]], generate_coalitions, future.packages = c("data.table"), future.seed = NULL)
    gen_time = capture.output(toc()) %>% strsplit(" ") %>% .[[1]] %>% .[1] %>% as.numeric() %>% seconds_to_period()
    cat(paste0('(generate: ', lubridate::hour(gen_time), 'h:', lubridate::minute(gen_time), 'm:', round(lubridate::second(gen_time))), end = '')

    # generate SHAP values for all observations and all trained models
    tic()
    for (tr_model in names(trained_model_prediction_function)){

      # select model from trained_model_prediction_function
      trained_model = trained_model_prediction_function[[tr_model]]

      # predict trained model on list_generated_coal
      list_predicted_data = data.table::rbindlist(list_generated_coal)
      list_predicted_data = list_predicted_data %>%
        mutate(Prediction = trained_model(list_predicted_data %>% as.data.frame() %>% select(all_of(colnames(dataSample)))))
      list_predicted_data = split(list_predicted_data , f = list_predicted_data$obs_index)

      # evaluate SHAP values
      list_generated_SHAP <- future_lapply(1:length(list_predicted_data), generate_shapley, trained_model_name = tr_model, future.seed = NULL)

      # add observation predicted values and average observation predicted value
      predicted_obs = data.frame(obs_index = obs_index_to_evaluate,
                                 predicted_val = trained_model(dataSample[obs_index_to_evaluate, ] %>% as.data.frame()))
      predicted_obs_avg = trained_model(dataSample %>% summarise_all(mean))

      # append results
      local_SHAP = local_SHAP %>%
        bind_rows(data.table::rbindlist(list_generated_SHAP) %>%
                    left_join(predicted_obs, by = "obs_index") %>%
                    mutate(predicted_val_avg = predicted_obs_avg))
    } # tr_model
    pred_time = capture.output(toc()) %>% strsplit(" ") %>% .[[1]] %>% .[1] %>% as.numeric() %>% seconds_to_period()
    cat(paste0(' - predict: ', lubridate::hour(pred_time), 'h:', lubridate::minute(pred_time), 'm:', round(lubridate::second(pred_time)), ')'), end = '\r')

    split_time_val = c(split_time_val, difftime(Sys.time(), split_time, units='secs'))
  } # split_name
  future:::ClusterRegistry("stop")
  tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
  cat('\nTotal elapsed time', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))), ' ', as.character(Sys.time()), '\n')
  if (n_features * length(obs_index_to_evaluate) * length(trained_model_prediction_function) != nrow(local_SHAP)){
    warning("Expected number of rows in generated local SHAP values doesn't match")
  }
  local_SHAP = local_SHAP %>%
    arrange(model_name, obs_index, feature)

  #### loop for all classes (if any) to create global_features_effect and SHAP_feat_imp

  class_set = data.frame(set = 'All observations', class = '', stringsAsFactors = F)
  if (!is.null(obs_index_subset)){
    obs_index_subset = obs_index_subset %>% mutate(class = as.character(class))  # remove factors
    class_set = class_set %>%
      bind_rows(data.frame(set = paste0("class ", obs_index_subset$class %>% unique() %>% sort()),
                           class = obs_index_subset$class %>% unique() %>% sort(), stringsAsFactors = F))
    local_SHAP = local_SHAP %>%
      left_join(obs_index_subset, by = "obs_index")
  }
  list_output = list()
  for (class_i in 1:nrow(class_set)){

    if (class_set$set[class_i] != 'All observations'){
      tt_local_SHAP = local_SHAP %>%
        filter(class == class_set$class[class_i])
    } else {
      tt_local_SHAP = local_SHAP
    }

    # evaluate average of SHAP values. Proxy for GLOBAL signed impact on predictions
    global_features_effect = tt_local_SHAP %>%
      group_by(model_name, feature) %>%
      summarize(phi = mean(phi), .groups = "drop") %>%
      arrange(model_name, desc(abs(phi)))


    # evaluate SHAP feature importance. Average of SHAP values absolute value
    SHAP_feat_imp = tt_local_SHAP %>%
      group_by(model_name, feature) %>%
      summarize(phi = mean(abs(phi)), .groups = "drop") %>%
      arrange(model_name, desc(phi))

    list_output[[class_set$set[class_i]]] = list(global_features_effect = global_features_effect,
                                                 SHAP_feat_imp = SHAP_feat_imp)

  } # class_i
  features_level = list_output$`All observations`$SHAP_feat_imp
  if (length(list_output) == 1){list_output = list_output[[1]]}   # remove "All observations" level if it is the only available

  # evaluate input for plot_SHAP_summary()
  scaled_features = dataSample %>%    # scale input features in [0,1]
    as.data.frame() %>%
    mutate_all(~scale_range(., a=0, b=1)) %>%
    mutate(obs_index = 1:n()) %>%
    gather(key = "feature", value = "value_color", -obs_index)

  summary_plot_data = c()
  for (tr_model in names(trained_model_prediction_function)){

    summary_plot_data = summary_plot_data %>%
      bind_rows(
        local_SHAP %>%
          filter(model_name == tr_model) %>%
          left_join(scaled_features, by = c("feature", "obs_index")) %>%
          mutate(feature = factor(feature, levels = features_level %>% filter(model_name == tr_model) %>% pull(feature)))
      )
  } # tr_model

  list_output = c(list(type = "SHAP",
                       local_SHAP = local_SHAP,
                       summary_plot_data = summary_plot_data),
                  list_output)

  return(list_output)
}

# evaluate Permutation Feature Importance
evaluate_Perm_Feat_Imp = function(dataSample, trained_model_prediction_function = NULL, n_repetitions = 5, compare = "difference",
                                  obs_index_to_evaluate = NULL, obs_index_to_shuffle = NULL, obs_index_subset = NULL,
                                  perf_metric = NULL, perf_metric_add_pars = NULL, true_val_name = NULL, prediction_name = NULL, perf_metric_minimize = F,
                                  verbose = 1, n_workers = 5, seed = 66){
  
  # evaluate Permutation Feature Importance.
  
  # dataSample: data.frame of predictors AND target variable as "y". For classification task, "y" must be character
  # trained_model_prediction_function: named list of function(predictors) -> prediction (vector of values for regression,
  #                                   probabilities of "1" for binary classification).
  #                                   E.g. function(x){predict(rf, newdata = x)}. List names will be used as output label for each model.
  # n_repetitions: how many times permutation importance must be evaluated with different seeds and then averaged.
  # compare: "difference" or "ratio" to compare permutation performance with original input performance. See perf_metric_minimize.
  # obs_index_to_evaluate: integer vector - row index of observations to evaluate feature importance value. If NULL all observations will be used.
  # obs_index_to_shuffle: integer vector of index of rows to be used when shuffling each feature. obs_index_to_evaluate must be a partition of obs_index_to_shuffle.
  #                      If NULL all observations will be used.
  # obs_index_subset: data.frame of "obs_index", "class". If not NULL contains class for each obs_index_to_evaluate. All outputs are then evaluated
  #                 for each class and all observations. Meaningless for classification problem and class-specific perf_metric (such as F1)
  # perf_metric, perf_metric_add_pars: function to predict performance. E.g. MLmetrics::F1_Score. If user defined, must return a single value
  #                                   and must have named input arguments. Additional parameters can be passed by named list perf_metric_add_pars. See next.
  # prediction_name, true_val_name: string - expected column name for predicted and true values in perf_metric. Predicted and true values of each shuffled
  #                                set will be assigned prediction_name and true_val_name column name.
  # perf_metric_minimize: whether optimal value of perf_metric should be minimized or not. E.g. TRUE for RMSE, FALSE for Accuracy or F1.
  #                      Used to set right order of "compare" method. For "difference", if FALSE then Imp = Perf_original - Perf_permutation, else reverse order.
  #                      For "ratio", if FALSE then Imp = Perf_original / Perf_permutation, else reverse order.
  # verbose: 1 to display calculation time, 0 for silent.
  # n_workers: number of workers for parallel calculation. Try not to exceed 30-40.
  # seed: seed for reproducibility
  
  # Output:
  #   list of:
  #     - Permutation_feat_imp: permutation feature importance.
  #                           data.frame with "model_name", "feature", "importance", "importance.std", "importance.5", "importance.95"
  #                           "importance", ".std", ".5" and ".95" are the average, st.dev, 5th and 95th percentile of feature importance over repetitions.
  #     - type: "SHAP". Used in plot_feat_imp().
  # If obs_index_subset != NULL additional nested list is returned for each class and all observations with Permutation_feat_imp
  
  # check input index and target variable
  if (is.null(obs_index_to_evaluate)){obs_index_to_evaluate = c(1:nrow(dataSample))}
  if (is.null(obs_index_to_shuffle)){obs_index_to_shuffle = c(1:nrow(dataSample))}
  if (length(intersect(obs_index_to_evaluate, obs_index_to_shuffle)) != length(obs_index_to_evaluate)){
    stop('"obs_index_to_evaluate" must be a partition of "obs_index_to_shuffle"')
  }
  if (!is.null(obs_index_subset) & sum(obs_index_subset$obs_index %>% sort() == obs_index_to_evaluate) != length(obs_index_to_evaluate)){
    stop('"obs_index_subset" must have same obs_index of "obs_index_to_evaluate"')
  }
  if (!"y" %in% colnames(dataSample)){
    stop('Target variable "y" not found')
  }
  if (is.null(true_val_name) | is.null(prediction_name)){
    stop('Please provide "true_val_name" and "prediction_name"')
  }
  
  # set additional parameters to performance metric
  perf_metric_work = function(...){perf_metric(..., perf_metric_add_pars)}
  
  feat_names = setdiff(colnames(dataSample), "y")
  tot_features = ncol(dataSample) - 1
  dataSample = dataSample %>%
    mutate(obs_index = 1:n()) %>%
    as.data.frame()
  
  generate_permutations = function(k){    # k = feature
    
    f_name = feat_names[k]
    
    runs <- lapply(1:n_repetitions, function(m){ # m = repetitions
      
      # shuffle k-th column
      df_temp = dataSample[obs_index_to_shuffle, ]
      set.seed(seed + k*m*(k+m))
      df_temp[, f_name] = sample(df_temp[, f_name], nrow(df_temp))
      df_temp = df_temp[obs_index_to_evaluate, ]
      
      return(df_temp %>% mutate(obs_index = obs_index_to_evaluate,
                                rep_num = m))
    })
    data.table::rbindlist(runs)%>% mutate(feature = f_name)
  }
  
  # suppress messages
  if (verbose == 0){sink(tempfile());on.exit(sink())}
  
  
  options(future.globals.maxSize = 8000 * 1024^2)
  plan(multisession, workers = n_workers)
  start_time = Sys.time()
  cat('Evaluating feature importance...', end = '\r')
  
  #### generate shuffled data for all features and repetitions
  list_generated_perm <- future_lapply(1:tot_features, generate_permutations, future.seed = NULL)
  
  
  #### loop for all classes (if any) to create global_features_effect and SHAP_feat_imp
  
  class_set = data.frame(set = 'All observations', class = '', stringsAsFactors = F)
  if (!is.null(obs_index_subset)){
    obs_index_subset = obs_index_subset %>% mutate(class = as.character(class))  # remove factors
    class_set = class_set %>%
      bind_rows(data.frame(set = paste0("class ", obs_index_subset$class %>% unique() %>% sort()),
                           class = obs_index_subset$class %>% unique() %>% sort(), stringsAsFactors = F))
  }
  
  list_output = list()
  for (class_i in 1:nrow(class_set)){
    
    if (class_set$set[class_i] == 'All observations'){
      class_index = obs_index_to_evaluate
    } else {
      class_index = obs_index_subset %>%
        filter(class == class_set$class[class_i]) %>%
        pull(obs_index)
    }
    
    Permutation_feat_imp = c()
    for (tr_model in names(trained_model_prediction_function)){
      
      # select model from trained_model_prediction_function
      trained_model = trained_model_prediction_function[[tr_model]]
      
      # predict for all features and repetitions
      pred_function = function(x){
        x %>% mutate(!!sym(prediction_name) := trained_model(x %>% as.data.frame() %>% select(all_of(feat_names)))) %>%
          rename(!!sym(true_val_name) := y) %>%
          select(obs_index, feature, rep_num, all_of(c(true_val_name, prediction_name)))
      }
      
      list_predicted_data = future_lapply(list_generated_perm, future.packages = loadedNamespaces(), pred_function, future.seed = NULL)
      
      # evaluate performance for all features and repetitions
      perf_function = function(x){
        f_name = x$feature %>% unique()
        out = c()
        for (i in unique(x$rep_num)){
          tt = x %>%
            filter(rep_num == i) %>%
            filter(obs_index %in% class_index)
          arg_list = list(tt %>% pull(true_val_name), tt %>% pull(prediction_name))
          names(arg_list) = c(true_val_name, prediction_name)
          out = out %>%
            bind_rows(data.frame(feature = f_name, rep_num = i, perf = do.call(perf_metric_work, args = arg_list), stringsAsFactors = F))
        }
        return(out)
      }
      df_final_perf = future_lapply(list_predicted_data, perf_function, future.seed = NULL) %>% data.table::rbindlist()
      
      # original performance
      original_pred = pred_function(dataSample %>%
                                      filter(obs_index %in% class_index) %>%
                                      mutate(feature = "Orginal_perf",
                                             rep_num = -99))
      original_perf = perf_function(original_pred)
      
      # evaluate feature importance
      Permutation_feat_imp = Permutation_feat_imp %>%
        bind_rows(
          df_final_perf %>%
            mutate(perf_original = original_perf$perf )%>%
            rowwise() %>%
            mutate(importance_val = ifelse(compare == "difference",
                                           ifelse(perf_metric_minimize, perf - perf_original, perf_original - perf), # for "difference"
                                           ifelse(perf_metric_minimize, perf / perf_original, perf_original / perf))) %>% # for "ratio"
            as.data.frame() %>%
            group_by(feature) %>%
            summarise(importance = mean(importance_val),     # todo: capisci cosa fare con i valori negativi o < 1 (cioè migliorano le performance)
                      importance.std = sd(importance_val),
                      importance.5 = quantile(importance_val, 0.05),
                      importance.95 = quantile(importance_val, 0.95), .groups = "drop") %>%
            mutate(model_name = tr_model) %>%
            select(model_name, everything())
        )
      
    } # tr_model
    
    list_output[[class_set$set[class_i]]] = list(Permutation_feat_imp = Permutation_feat_imp %>%
                                                   arrange(model_name, desc(abs(importance))))
    
  } # class_i
  future:::ClusterRegistry("stop")
  tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
  cat('Done in', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))), ' ', as.character(Sys.time()), '\n')
  if (tot_features * length(trained_model_prediction_function) != nrow(list_output$`All observations`$Permutation_feat_imp)){
    warning("Expected number of rows in generated Permutation_feat_imp doesn't match")
  }
  if (length(list_output) == 1){list_output = list_output[[1]]}   # remove "All observations" level if it is the only available
  
  list_output = c(list(type = "PFI"),
                  list_output)
  
  return(list_output)
}

# Plot Shapley summary
plot_SHAP_summary = function(list_input, sina_method = "counts", sina_bins = 20, sina_size = 2, sina_alpha = 0.7, plot_model_set = NULL, plot_class_set = NULL,
                             SHAP_axis_lower_limit = 0, magnify_text = 1.4, color_range = c("red", "blue"), bold_features = c(), bold_color = "black",
                             save_path = '', plot_width = 12, plot_height = 12){
  
  # Plot Shapley summary. Returns plots for all trained model splitting results by 'All observations' and 'class ...' subsets if any.
  # list_input: output of evaluate_SHAP()
  # plot_model_set: if not NULL plot only provided trained model results.
  # plot_class_set: if not NULL and if 'All observations', 'class ...' available plot only provided class.
  # sina_ : options of geom_sina (sina plot)
  # SHAP_axis_lower_limit: value to add to minimum value of x-axis (SHAP). Also moves SHAP's magnitude column
  # magnify_text: magnify all text font in plot
  # color_range: legend color of Low and High value of features
  # bold_features: list of features to be displayed in bold font
  # bold_color: if bold_features != c() color of bold features
  # save_path: if not '', save all plots. Only modelName_Class.png will be added. "_" at the end of save_path is automatically added if not provided.
  # plot_width, plot_height: width and height of saved plot
  
  # check last character of save_path
  if (substr(save_path, nchar(save_path), nchar(save_path)) != "_" & save_path != ''){save_path = paste0(save_path, "_")}
  
  # check if class plots are available
  summary_plot_data = list_input$summary_plot_data
  if ("All observations" %in% names(list_input)){
    class_set = setdiff(names(list_input), c("local_SHAP", "summary_plot_data", "type"))
  } else {
    class_set = c('No class')
  }
  if (is.null(plot_class_set)){
    plot_class_set = class_set
  } else if (!is.null(plot_class_set) & class_set == 'No class'){
    plot_class_set = 'No class'
  }
  
  plot_list = list()
  for (class_i in plot_class_set){
    
    # extract data
    if (class_i == 'No class'){
      data_plot = summary_plot_data %>%
        left_join(list_input$SHAP_feat_imp %>% rename(abs.phi = phi), by = c("model_name", "feature"))
    } else if (class_i == 'All observations'){
      data_plot = summary_plot_data %>%
        left_join(list_input[[class_i]]$SHAP_feat_imp %>% rename(abs.phi = phi), by = c("model_name", "feature"))
    } else {
      data_plot = summary_plot_data %>%
        filter(class == class_i %>% gsub("class ", "", .)) %>%
        left_join(list_input[[class_i]]$SHAP_feat_imp %>% rename(abs.phi = phi), by = c("model_name", "feature"))
    }
    
    if (is.null(plot_model_set)){plot_model_set_work = unique(data_plot$model_name)}
    # loop models
    for (tr_model in plot_model_set_work){
      data_plot_tt = data_plot %>%
        filter(model_name == tr_model)
      y_lim = range(data_plot_tt$phi)
      
      additional_top_rows = 3   # adds fake features to increase y-axis (features) limits
      feature_order = data_plot_tt %>%
        select(feature, abs.phi) %>%
        unique() %>%
        mutate(colr = ifelse(feature %in% bold_features, yes = bold_color, no = "black")) %>%
        mutate(abs.phi.perc = abs.phi / sum(abs.phi),
               lab = paste0(' ', round(abs.phi, 3), ' (', round(abs.phi.perc * 100, 2), '%)')) %>%
        arrange(desc(abs.phi)) %>%
        mutate(feature_ref = c(1:n())*2) %>%
        bind_rows(data.frame(feature = paste0("xxx_", 1:nrow(.))) %>%
                    mutate(feature_ref = c(1:n())*2-1),
                  data.frame(feature = paste0("xxx_", c(-additional_top_rows:0))) %>%
                    mutate(feature_ref = c(-additional_top_rows:0))) %>%
        arrange(feature_ref)
      
      SHAP_limits = c(y_lim[1] * (1 - sign(y_lim[1]) * 1.5) + SHAP_axis_lower_limit, # lower x-axis value (added to make room for SHAP magnitude)
                      y_lim[2] * (1 + sign(y_lim[2]) * 0.1))
      SHAP_breaks = c(0, seq(y_lim[1], y_lim[2], length.out = 6)) %>% unique() %>% sort()  # ticks from data
      ticks_spacing = diff(SHAP_breaks)
      # if tick before/after 0 is too close, shift it a bit left by 50%
      tick_before_0_spacing = ticks_spacing[which(SHAP_breaks == 0) - 1]
      if (tick_before_0_spacing < max(ticks_spacing) * 0.3){SHAP_breaks[which(SHAP_breaks == 0) - 1] = SHAP_breaks[which(SHAP_breaks == 0) - 1] * 1.5}
      tick_before_1_spacing = ticks_spacing[which(SHAP_breaks == 0)]
      if (tick_before_1_spacing < max(ticks_spacing) * 0.3){SHAP_breaks[which(SHAP_breaks == 0) + 1] = SHAP_breaks[which(SHAP_breaks == 0) + 1] * 3}
      SHAP_labels = as.character(SHAP_breaks %>% round(2))
      
      data_plot_tt = data_plot_tt %>%  # add fake points to add space between features
        bind_rows(data.frame(feature = paste0("xxx_", min(feature_order$feature_ref):uniqueN(data_plot_tt$feature))) %>%
                    mutate(phi = min(data_plot_tt$phi)))
      bold_levels = feature_order %>%  # 0.11
        mutate(feature = ifelse(grepl("xxx_", feature), "", feature)) %>%
        mutate(face = ifelse(feature %in% bold_features, yes = "bold", no = "plain"))
      out_plot = suppressWarnings(
        ggplot(data_plot_tt %>%
                 group_by(feature) %>%
                 # filter(row_number() <= 200) %>%   # todo: remove
                 mutate(feature = factor(feature, levels = rev(feature_order$feature))),
               aes(x = feature, y = phi, color = value_color)) +
          # scale_y_continuous(expand = c(0, 0), limits = c(SHAP_position_work * SHAP_axis_lower_limit, y_lim[2])) +    # SHAP    expand = (expansion between ticks, expansion outside limits)
          scale_y_continuous(expand = c(0, 0), limits = SHAP_limits, breaks = SHAP_breaks, labels = SHAP_labels) +    # SHAP    expand = (expansion between ticks, expansion outside limits)
          scale_x_discrete(expand=c(0.02, 0), labels = feature_order %>%  # 0.11
                             mutate(feature = ifelse(grepl("xxx_", feature), "", feature)) %>%
                             pull(feature) %>%
                             rev(), limits = feature_order %>%  # 0.11
                             pull(feature) %>%
                             rev(),
                           breaks = feature_order %>%  # 0.11
                             pull(feature) %>%
                             rev()) +     # features
          labs(title = paste0("SHAP summary plot for ",
                              ifelse(class_i %in% c('No class', 'All observations'), 'all classes', class_i)),
               x = "Feature", y = "SHAP value (impact on model predictions)", color = "Feature value") +
          coord_flip() + 
          geom_sina(size = sina_size, bins = sina_bins, method = sina_method, alpha = sina_alpha) +
          geom_hline(yintercept = 0) +
          geom_text(data = feature_order %>%
                      filter(!grepl("xxx_", feature)), aes(x = feature, y=-Inf, label = lab,
                                                           fontface = ifelse(feature %in% bold_features, 2, 1)
                                                           # colour = colr
                      ),
                    size = 4 * magnify_text, hjust = 0, colour = "black") +
          annotate("text", x = feature_order$feature[1], y = -Inf, label = " SHAP abs", size = 5 * magnify_text, hjust = 0, fontface = "bold") +
          annotate("text", x = feature_order$feature[3], y = -Inf, label = " magnitude", size = 5 * magnify_text, hjust = 0, fontface = "bold") +
          scale_color_gradient(low=color_range[1], high=color_range[2], 
                               breaks=c(0,1), labels=c("Low","High"), na.value="white") +
          theme_bw() +
          theme(axis.line.y = element_blank(),
                axis.ticks.y = element_blank(),
                axis.text.y = element_text(size = 13 * magnify_text, vjust = 0.5, face = rev(bold_levels$face), color = rev(bold_levels$colr)),   # features
                axis.text.x = element_text(size = 16),   # SHAP
                axis.title = element_text(size = 20* magnify_text),
                plot.title = element_text(size=30),
                plot.subtitle = element_text(size=25),
                legend.title=element_text(size=18* magnify_text),
                legend.text=element_text(size=15* magnify_text),
                legend.position="bottom") +
          guides(colour = guide_colourbar(title.position="left", title.vjust = 1))
      )
      
      # todo: rimuovi
      # {
      # png("./Distance_to_Default/Results/000p.png",
      #     width = plot_width, height = plot_height, units = 'in', res=300)
      # plot(out_plot)
      # dev.off()
      #   }
      
      plot_list[[class_i]][[tr_model]] = out_plot
      
      # save plot
      if (save_path != ''){
        png(paste0(save_path, tr_model, ifelse(class_i == "No class", "", paste0("_", gsub(" ", "_", class_i))), ".png"),
            width = plot_width, height = plot_height, units = 'in', res=300)
        plot(out_plot)
        dev.off()
      }
      
    } # tr_model
  } # class_i
  if (class_i == "No class"){plot_list = plot_list[[1]]}
  
  return(plot_list)
}

# Plot feature importance
plot_feat_imp = function(list_input, normalize = F, color_pos = "blue", color_neg = "red", plot_model_set = NULL, plot_class_set = NULL,
                         magnify_text = 1, bold_features = c(), bold_color = "black", save_path = '', plot_width = 12, plot_height = 12){
  
  # Plot feature importance. Returns plots for all trained model and 'All observations' and 'class ...' if any. If input is from evaluate_SHAP() plots
  # global (signed) features effects and global SHAP features importance. If input is from evaluate_Perm_Feat_Imp() simply plots feature importance.
  # All input elements must be data.frame with "model_name", "feature" and "phi"/"importance"
  
  # list_input: output of evaluate_SHAP() or evaluate_Perm_Feat_Imp()
  # normalize: if TRUE normalize importance in 0-100%
  # color_pos, color_neg: color for positive and negative (if any) bars
  # plot_model_set: if not NULL plot only provided trained model results.
  # plot_class_set: if not NULL and if 'All observations', 'class ...' available plot only provided class.
  # magnify_text: magnify all text font in plot
  # save_path: if not '', save all plots. Only modelName_Class.png will be added. "_" at the end of save_path is automatically added if not provided.
  # bold_features: list of features to be displayed in bold font
  # bold_color: if bold_features != c() color of bold features
  # plot_width, plot_height: width and height of saved plot
  
  # todo: aggiungi le barre di errore nel caso della PFI e capisci cosa viene fuori se la PFI è negativa
  
  # check last character of save_path
  if (substr(save_path, nchar(save_path), nchar(save_path)) != "_" & save_path != ''){save_path = paste0(save_path, "_")}
  
  # check if class plots are available
  if (list_input$type == "SHAP"){
    if ("All observations" %in% names(list_input)){
      class_set = setdiff(names(list_input), c("local_SHAP", "summary_plot_data", "type"))
    } else {
      class_set = c('No class')
    }
    plot_loop = c("global_features_effect", "SHAP_feat_imp")
    feat_name = "phi"
    normalize = F
  } else if (list_input$type == "PFI"){
    if ("All observations" %in% names(list_input)){
      class_set = names(list_input)
    } else {
      class_set = c('No class')
    }
    plot_loop = "Permutation_feat_imp"
    feat_name = "importance"
  }
  
  if (is.null(plot_class_set)){
    plot_class_set = class_set
  } else if (!is.null(plot_class_set) & class_set == 'No class'){
    plot_class_set = 'No class'
  }
  
  plot_list = list()
  for (class_i in plot_class_set){
    
    for (plot_type in plot_loop){  # "global_features_effect", "SHAP_feat_imp", "feature_importance"
      
      # set importance axis label
      normalize_work = normalize
      if (plot_type == "global_features_effect"){
        imp_axis_label = "Average signed SHAP value\n(impact on model predictions)"
        plot_title = "Average signed SHAP"
      } else if (plot_type == "SHAP_feat_imp"){
        imp_axis_label = "Average absolute SHAP value\n(impact on model predictions)"
        plot_title = "Average absolute SHAP"
      } else if (plot_type == "Permutation_feat_imp"){
        imp_axis_label = "Feature importance"
        if (normalize_work){imp_axis_label = paste0(imp_axis_label, " (normalized)")}
        plot_title = "Permutation Feature Importance"
      }
      
      # extract data
      if (class_i == 'No class'){
        data_plot = list_input[[plot_type]]
      } else {
        data_plot = list_input[[class_i]][[plot_type]]
      }
      
      if (is.null(plot_model_set)){plot_model_set_work = unique(data_plot$model_name)}
      # loop models
      for (tr_model in plot_model_set_work){
        data_plot_tt = data_plot %>%
          rename(importance = !!sym(feat_name)) %>%
          filter(model_name == tr_model) %>%
          # mutate(importance = ifelse(feature == "BILA_roa", -importance, importance)) %>%  # todo: rimuovi
          mutate(value_color = ifelse(importance >= 0, "pos", "neg"),
                 importance = signif(importance, 2)) %>%
          rowwise() %>%
          mutate(max_digits = importance  %>% as.character() %>% strsplit("\\.") %>% .[[1]] %>% .[2] %>% nchar())
        max_digits = max(data_plot_tt$max_digits, na.rm = T)
        max_span = max(abs(data_plot_tt$importance)) * (1 + max_digits / 10)
        
        if (normalize_work){
          data_plot_tt = data_plot_tt %>%
            as.data.frame() %>%
            mutate(importance = round(importance / sum(abs(importance)) * 100, 1))
          max_span = max(abs(data_plot_tt$importance)) * 1.3
        }
        
        bold_levels = data_plot_tt %>%  # 0.11
          mutate(face = ifelse(feature %in% bold_features, yes = "bold", no = "plain"),
                 colr = ifelse(feature %in% bold_features, yes = bold_color, no = "black"))
        out_plot = suppressWarnings(
          ggplot(data_plot_tt %>%
                   mutate(feature = factor(feature, levels = rev(data_plot_tt$feature))),
                 aes(x = feature, y = importance)) +
            geom_bar(stat = "identity", aes(fill = value_color), width=0.9, position = position_dodge(width=0.5)) +
            scale_fill_manual(values = c("pos" = color_pos, "neg" = color_neg)) +
            labs(title = paste0(plot_title, " for ",
                                ifelse(class_i %in% c('No class', 'All observations'), 'all classes', class_i)),
                 x = "Feature", y = imp_axis_label) +
            scale_y_continuous(limits = c(min(c(min(data_plot_tt$importance) - max_span, 0)), max(data_plot_tt$importance) + max_span)) +
            geom_hline(yintercept = 0) +
            coord_flip() +
            theme_bw() +
            theme(axis.line.y = element_blank(),
                  axis.ticks = element_blank(),
                  axis.text.y = element_text(size = 13 * magnify_text, vjust = 0.5, face = rev(bold_levels$face), color = rev(bold_levels$colr)),   # features
                  axis.text.x = element_blank(),   # SHAP
                  axis.title = element_text(size = 20 * magnify_text),
                  plot.title = element_text(size=30),
                  plot.subtitle = element_text(size=25),
                  legend.title=element_text(size=18),
                  legend.text=element_text(size=15),
                  legend.position="none")
        )
        if (normalize_work){
          out_plot = out_plot +
            geom_text(aes(label = ifelse(importance == 0, "", paste0(" ", importance, "%")),
                          vjust = 0.5, hjust = ifelse(importance >= 0, 0, 1)), size = 6 * magnify_text)
        } else {
          out_plot = out_plot +
            geom_text(aes(label = ifelse(importance == 0, "", ifelse(importance > 0, paste0(" ", importance), paste0(importance, " "))),
                          vjust = 0.5, hjust = ifelse(importance >= 0, 0, 1)), size = 6 * magnify_text)
        }
        
        # todo: rimuovi
        # {
        # png("./Distance_to_Default/Results/000p.png",
        #     width = plot_width, height = plot_height, units = 'in', res=300)
        # plot(out_plot)
        # dev.off()
        #   }
        
        plot_list[[class_i]][[tr_model]][[plot_type]] = out_plot
        
        # save plot
        if (save_path != ''){
          png(paste0(save_path, plot_type, "_", tr_model, ifelse(class_i == "No class", "", paste0("_", gsub(" ", "_", class_i))), ".png"),
              width = plot_width, height = plot_height, units = 'in', res=300)
          plot(out_plot)
          dev.off()
        }
        
      } # tr_model
    } # plot_type
  } # class_i
  if (class_i == "No class"){plot_list = plot_list[[1]]}
  
  return(plot_list)
}

# evaluate feature importance
evaluate_feature_importance = function(df_work, model_setting_block, method,
                                       performance_metric = "F1", n_repetitions = 5, compare = "difference",
                                       sample_size = 100, n_batch = 5,
                                       verbose = 1, n_workers = 5, seed = 66){
  
  # Evaluate feature importance with SHAP or Permutation Feature Importance. SHAP will use a subset of observations, i.e. y=0 are downsampled to match y=1.
  
  # df_work: dataset with predictors and target variable as "y" to be used for feature importance
  # model_setting_block: subset of log_fitting with "model_setting_lab", "cluster_lab", "data_type", "model", "algo_type", "rds".
  #                     algorithms in "algo_type" will be evaluated. Corresponding fitted model is reloaded by "rds".
  # method: vector of c("SHAP", "Permutation")
  #  --- Arguments for Permutation Feature Importance
  # performance_metric: performance metric to be used in permutation feature importance. "F1", "Precision" or "Recall". SHAP will use only predicted probabilities.
  # n_repetitions: how many times permutation importance must be evaluated with different seeds and then averaged.
  # compare: "difference" or "ratio" to compare permutation performance with original input performance. See perf_metric_minimize.
  #  --- Arguments for SHAP
  # sample_size: sample size to generate shuffled instances (coalitions). The higher the more accurate the explanations become.
  # n_batch: number of batch to split the evaluation. May speed up evaluation and save memory.
  #
  # verbose: 1 to display calculation time, 0 for silent.
  # n_workers: number of workers for parallel calculation. Try not to exceed 30-40.
  # seed: seed for reproducibility
  
  
  # set performance metric for permutation feature importance (see evaluate_Perm_Feat_Imp() input)
  if (performance_metric == "F1"){
    perf_metric = MLmetrics::F1_Score
    perf_metric_minimize = F
    perf_metric_add_pars = list(positive = "1")
    prediction_name = "y_pred"
    true_val_name = "y_true"
  }
  if (performance_metric == "Precision"){
    perf_metric = MLmetrics::Precision
    perf_metric_minimize = F
    perf_metric_add_pars = list(positive = "1")
    prediction_name = "y_pred"
    true_val_name = "y_true"
  }
  if (performance_metric == "Recall"){
    perf_metric = MLmetrics::Recall
    perf_metric_minimize = F
    perf_metric_add_pars = list(positive = "1")
    prediction_name = "y_pred"
    true_val_name = "y_true"
  }
  
  
  # select prediction method (raw probability or class)
  if (method == "SHAP"){
    pred_type = "raw"
  } else if (method == "Permutation"){
    pred_type = "class"
  }
  
  # create list of prediction function for trained models
  trained_model_prediction_function = list()
  for (alg_type in model_setting_block$algo_type){
    
    # reload model
    fit_fullset = readRDS(model_setting_block %>% filter(algo_type == alg_type) %>% pull(rds))
    
    # create predict model
    if (alg_type == "Elastic-net"){
      
      model_Elastic_net = fit_fullset$fold_model_fit$fold_1$fit
      lambda_opt = fit_fullset$fold_model_fit$fold_1$lambda_opt
      family = fit_fullset$fold_model_fit$fold_1$options$family
      best_threshold_Elastic_net = fit_fullset$best_threshold
      
      pred_function = function(x){
        out = predict(model_Elastic_net, newx = x %>% as.matrix(), s = lambda_opt, family = family, type="response") %>% as.numeric()
        if (pred_type == "class"){
          out = ifelse(out >= best_threshold_Elastic_net, 1, 0) %>% as.character()
        }
        return(out)
      }
      
    } else if (alg_type == "Random_Forest"){
      
      model_Random_Forest = fit_fullset$fold_model_fit$fold_1$fit
      best_threshold_Random_Forest = fit_fullset$best_threshold
      
      pred_function = function(x){
        out = predict(model_Random_Forest, data = x, type = "response")$predictions[, 2] %>% as.numeric()
        if (pred_type == "class"){
          out = ifelse(out >= best_threshold_Random_Forest, 1, 0) %>% as.character()
        }
        return(out)
      }
      
    } else if (alg_type == "MARS"){
      
      model_MARS = fit_fullset$fold_model_fit$fold_1$fit
      best_threshold_MARS = fit_fullset$best_threshold
      
      pred_function = function(x){
        out = predict(model_MARS, newdata = x, type = "response") %>% as.numeric()
        if (pred_type == "class"){
          out = ifelse(out >= best_threshold_MARS, 1, 0) %>% as.character()
        }
        return(out)
      }
      
    } else if (alg_type == "SVM-RBF"){
      
      model_SVM_RBF = fit_fullset$fold_model_fit$fold_1$fit
      best_threshold_SVM_RBF = fit_fullset$best_threshold
      
      pred_function = function(x){
        out = predict(model_SVM_RBF, newdata = x, type = "probabilities")[,2] %>% as.numeric()
        if (pred_type == "class"){
          out = ifelse(out >= best_threshold_SVM_RBF, 1, 0) %>% as.character()
        }
        return(out)
      }
      
    }
    
    trained_model_prediction_function[[alg_type]] = pred_function
    
  } # alg_type
  
  # todo: rimuovi
  # tt = trained_model_prediction_function[["MARS"]]
  # tt(df_predictors)[1:10]
  # predict(model_MARS, newdata = df_predictors, type = "response")[1:10]
  # 
  # 
  # tt = trained_model_prediction_function[["Random_Forest"]]
  # tt(df_predictors)[1:10]
  # predict(model_Random_Forest, data = df_predictors, type = "response")$predictions[, 2][1:10]
  # 
  # 
  # tt = trained_model_prediction_function[["Elastic-net"]]
  # tt(df_predictors)[1:10]
  # predict(model_Elastic_net, newx = df_predictors %>% as.matrix(), s = lambda_opt, family = family, type="response")[1:10]
  
  
  if (method == "Permutation"){
    cat('\n    - Permutation Feature Importance\n\n')
    
    obs_index_to_evaluate = NULL
    obs_index_to_shuffle = NULL
    obs_index_subset = NULL
    
    feat_imp = evaluate_Perm_Feat_Imp(dataSample = df_work %>% mutate(y = as.character(y)),
                                      trained_model_prediction_function = trained_model_prediction_function, n_repetitions = n_repetitions, compare = compare,
                                      obs_index_to_evaluate = obs_index_to_evaluate, obs_index_to_shuffle = obs_index_to_shuffle, obs_index_subset = obs_index_subset,
                                      perf_metric = perf_metric, perf_metric_add_pars = perf_metric_add_pars, true_val_name = true_val_name,
                                      prediction_name = prediction_name, perf_metric_minimize = perf_metric_minimize,
                                      verbose = verbose, n_workers = n_workers, seed = seed)
  }
  
  if (method == "SHAP"){
    cat('\n    - SHAP values\n')
    
    # downsample df_work to match y=1 and y=0 class # todo: find a better way to downsample
    obs_0_to_remove = (sum(df_work$y == 0) * (1 - sum(df_work$y == 1) / sum(df_work$y == 0))) %>% as.integer()
    set.seed(seed)
    index_to_remove = sample(which(df_work$y == 0), obs_0_to_remove, replace = F)
    df_work = df_work[-index_to_remove, ]
    # df_work = df_work[1:100, ]   # todo: rimuovi
    
    obs_index_to_evaluate = NULL   # take all observations
    obs_index_to_sample = NULL   # sample from all observations (df_work is already balanced)
    obs_index_subset = data.frame(obs_index = 1:nrow(df_work), class = df_work$y)   # evaluate SHAP for each class as well
    
    feat_imp = evaluate_SHAP(dataSample = df_work %>% select(-y),
                             sample_size = sample_size, trained_model_prediction_function = trained_model_prediction_function,
                             obs_index_to_evaluate = obs_index_to_evaluate, obs_index_to_sample = obs_index_to_sample, obs_index_subset = obs_index_subset,
                             n_batch = n_batch, verbose = verbose, n_workers = n_workers, seed = seed)
  }
  
  return(feat_imp)
}