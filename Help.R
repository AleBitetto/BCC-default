
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
aggregate_points = function(plot_data, label_values, n_cell = 20){
  
  # plot_data: data.frame with Dim1, Dim2, Dim3 (if available) and Label columns.
  #            Label can contain also a single value (i. e. points will be just aggregated in a single group for each cell)
  # label_values: available values to be converted to factor for the final output (to be used as groups in ggplot)
  # n_cell: number of cells on shortest dimension
  
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
  grid_x = round(sort(unique(c(seq(min_x, max_x, by = cell_size), min_x, max_x))), 4)
  grid_y = round(sort(unique(c(seq(min_y, max_y, by = cell_size), min_y, max_y))), 4)
  grid_z = round(sort(unique(c(seq(min_z, max_z, by = cell_size), min_z, max_z))), 4)
  start_k = ifelse(tot_dim == 3, 2, 1)
  
  cell_summary = c()
  for (i in 2:length(grid_x)){
    for (j in 2:length(grid_y)){
      for (k in start_k:length(grid_z)){
        x_min = grid_x[i-1]
        x_max = grid_x[i]
        y_min = grid_y[j-1]
        y_max = grid_y[j]
        x_filter = paste0('Dim1 >= ', x_min, ' & Dim1 <', ifelse(i == length(grid_x), '=', ''), ' ', x_max)
        y_filter = paste0('Dim2 >= ', y_min, ' & Dim2 <', ifelse(j == length(grid_y), '=', ''), ' ', y_max)
        if (tot_dim == 3){
          z_min = grid_z[k-1]
          z_max = grid_z[k]
          z_filter = paste0('Dim1 >= ', z_min, ' & Dim1 <', ifelse(k == length(grid_z), '=', ''), ' ', z_max)
        } else {
          z_min = z_max = ""
          z_filter = "Dim3 == 0"
        }
        # cat('\n', i,j, x_filter, '     ', y_filter)
        cell = plot_data %>%
          filter(!!quo(eval(parse(text=x_filter)))) %>%
          filter(!!quo(eval(parse(text=y_filter)))) %>%
          filter(!!quo(eval(parse(text=z_filter))))
        for (val in label_values){
          centroid = cell %>%
            filter(Label == val) %>%
            select(-Label) %>%
            summarize_all(mean) %>%
            as.numeric()
          cell_summary = cell_summary %>%
            bind_rows(data.frame(cell = paste0('(', i-1, ',', j-1, ifelse(tot_dim == 3, paste0(',', k-1), ''), ')'), Label = val, size = sum(cell$Label == val),
                                 Dim1 = centroid[1], Dim2 = centroid[2], Dim3 = centroid[3],
                                 cell_range = paste0(x_min, ',', x_max, '|', y_min, ',', y_max, '|', z_min, ',', z_max), stringsAsFactors = F))
        } # val
      } # z
    } # j
  } # i
  if (tot_dim == 2){cell_summary = cell_summary %>% select(-Dim3)}
  cell_summary = cell_summary %>%
    filter(is.finite(Dim1))
  if (nrow(plot_data) - sum(cell_summary$size) >= round(nrow(plot_data)*0.005)){cat('\n\n ###### missing points in cell_summary')}
  
  # reshape centroid to be equally spaced within the cell
  final_points = c()
  for (cel in unique(cell_summary$cell)){
    df_cell = cell_summary %>%
      filter(cell == cel)
    if (nrow(df_cell) >= 1){
      points = generate_points(n_points = nrow(df_cell), dimension = tot_dim)
      x_min = strsplit(strsplit(df_cell$cell_range[1], '\\|')[[1]][1], ',')[[1]][1] %>% as.numeric()
      y_min = strsplit(strsplit(df_cell$cell_range[1], '\\|')[[1]][2], ',')[[1]][1] %>% as.numeric()
      z_min = strsplit(strsplit(df_cell$cell_range[1], '\\|')[[1]][3], ',')[[1]][1] %>% as.numeric()
      for (i in 1:nrow(df_cell)){
        df_cell$Dim1[i] = x_min + cell_size * points[i, 1]
        df_cell$Dim2[i] = y_min + cell_size * points[i, 2]
      }
      # plot(df_cell %>% select(Dim1, Dim2), xlim = c(x_min, x_min+cell_size), ylim = c(y_min, y_min+cell_size))
    }
    final_points = final_points %>%
      bind_rows(df_cell)
  }
  final_points = final_points %>%
    mutate(Label = factor(Label, levels = label_values))
  if (sum(cell_summary$size) != sum(final_points$size)){cat('\n\n ###### missing points in final_points')}
  
  return(list(cell_summary = final_points,
              grid_x = grid_x,
              grid_y = grid_y,
              grid_z = grid_z))
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
  # - xmin, xmax: provided if scaling has to be performed from a different input range [min(x), max(x)]
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
render_3d_grid = function(plot_list, n_col, show = "point", single_class_size = 1, legend_cex = 1, plot_legend_index = c(1:length(plot_list)),
                          MIN_SCALE = 1, MAX_SCALE = 5, MODE_SCALE = "linear", MODE_S = NULL){
  
  # plot_list: list of data to plot. Each element is a list of:
  #      - data: data.frame with Label (factor - legend lable for each point),
  #             size (numeric - size of each point. will be scaled in [MIN_SCALE, MAX_SCALE] according to MODE_SCALE and MODE_S of scale_range()),
  #             Dim1, Dim2, Dim3 (if any) (numeric - points coordinates)
  #      - title: title of each plot
  # n_col: number of columns in the plot
  # show: "point" for points, no size effect or "sphere" for 3D spheres and size effect
  # single_class_size: size of point/sphere when data has a single label/class
  # plot_legend_index: select index of plot to add legend (indices flow column by column, top to bottom)
  
  n_row = ceiling(length(plot_list) / n_col)
  open3d()
  mat <- matrix(1:(n_col*n_row*2), ncol = n_col)   # add spot for title (text3d())
  layout3d(mat, height = rep(c(1, 2), n_row), widths = rep(1, n_col), sharedMouse = T)  # (1,10) is the proportion between text3d and plot3d space
  plot_count = 1
  for (pl_data in plot_list){
    
    plot_data = pl_data$data
    label_values = levels(plot_data$Label)
    
    if(range(plot_data$size) %>% uniqueN() != 1){
      plot_data$size = scale_range(plot_data$size, MIN_SCALE, MAX_SCALE, mode = MODE_SCALE, s = MODE_S) %>% round(1)
    } else {
      plot_data$size = rep(single_class_size, nrow(plot_data))
    }
    
    # define colormap
    cmap = c('dodgerblue3', 'firebrick2', 'chartreuse3', 'cadetblue2', 'gold1', 'darkorange', 'slategray4', 'violet', 'yellow1')
    
    # check if 2D plot must be used
    plot2d_flag = FALSE
    if (!"Dim3" %in% colnames(plot_data)){
      plot_data = plot_data %>% mutate(Dim3 = 0)
      plot2d_flag = TRUE
    }
    
    # plot title
    next3d()
    text3d(0, 0, 0, pl_data$title, cex = 0.8)
    next3d()
    
    if (show == "point"){
      
      for (class_i in 1:length(label_values)){
        
        class_label = label_values[class_i]
        class_color = cmap[class_i]
        class_data = plot_data %>%
          filter(Label == class_label)
        
        for (class_size in unique(class_data$size)){
          size_data = class_data %>%
            filter(size == class_size) %>%
            select(starts_with("Dim")) %>%
            xyz.coords()
          points3d(size_data, size = class_size, col = class_color, axes = F, box = T, xlab = "", ylab = "", zlab = "")
        } # class_size
      } # class_i
    }
    
    
    if (show == "sphere"){
      plot3d(xyz.coords(plot_data %>% select(starts_with("Dim"))), col = cmap[plot_data$Label], type = "s",
             size = plot_data$size, axes = F, box = T, xlab = "", ylab = "", zlab = "")
    }
    
    # add box and focus on 2D view (if any)
    box3d(color = "grey")
    if (plot2d_flag){rgl.viewpoint( theta = 0, phi = 0, fov = 0, interactive = F)}
    
    # plot legend for selected plot
    if (plot_count %in% plot_legend_index){
      legend3d("bottomright", legend = label_values, pch = 16, col = cmap[1:length(label_values)], cex = legend_cex)
    }
    
    plot_count = plot_count + 1
  } # pl_data
}














































prova_3d = function(plot_list, n_col, show = "point"){
  
  # show: "point" for points, no size effect or "sphere" for 3D spheres and size effect
  
  n_row = ceiling(length(plot_list) / n_col)
  open3d()
  mat <- matrix(1:(n_col*n_row*2), ncol = n_col)   # add spot for title (text3d())
  layout3d(mat, height = rep(c(1, 2), n_row), widths = rep(1, n_col), sharedMouse = T)  # (1,10) is the proportion between text3d and plot3d space
  for (pl_data in plot_list){
    
    plot_data = pl_data$data
    
    # check if 2D plot must be used
    plot2d_flag = FALSE
    if (ncol(plot_data) == 2){
      plot_data = plot_data %>% mutate(Dim3 = 0)
      plot2d_flag = TRUE
    }
    
    data_coord = xyz.coords(plot_data[301:700,] %>% select(starts_with("Dim")))
    data_coord_additional = xyz.coords(plot_data[1:300,] %>% select(starts_with("Dim")))
    
    next3d()
    text3d(0, 0, 0, pl_data$title, cex = 0.8)
    next3d()
    
    if (show == "point"){
      # main_data <- plot3d(data_coord, col = "blue", type = "p", axes = F, box = T, xlab = "", ylab = "", zlab = "")
      # plot3d(data_coord, col = "white", type = "p", axes = F, box = T, xlab = "", ylab = "", zlab = "", size = 0)
      main_data <- points3d(data_coord, col = "blue", axes = F, box = T, xlab = "", ylab = "", zlab = "")
      rglwidget(elementId = "aa")
      add_data <- points3d(data_coord_additional, col = "red", axes = F, box = T, xlab = "", ylab = "", zlab = "")
      rglwidget(elementId = "bb")

      toggleWidget(sceneId = "aa", ids = main_data, label = "aa")
      toggleWidget(sceneId = "bb", ids = add_data, label = "bb")
      #   toggleWidget(ids = main_data, label = 'aaa') %>%
      # rglwidget(elementId = "plot3drgl") %>%
      #   toggleWidget(ids = add_data, label = 'bbb') %>% 
      #   asRow(last=2)
    }
    
    if (show == "sphere"){
      if(range(plot_data$size) %>% uniqueN() != 1){
        size_set = scale_range(plot_data$size, 0.5, 2, mode = 'linear')
      } else {
        size_set = rep(1, nrow(plot_data))
      }
      main_data <- plot3d(data_coord, col = "blue", type = "s", size = size_set, axes = F, box = T, xlab = "", ylab = "", zlab = "")
    }
    box3d(color = "grey")
    if (plot2d_flag){rgl.viewpoint( theta = 0, phi = 0, fov = 0, interactive = F)}
  }
}
