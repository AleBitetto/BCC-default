
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
    mutate(COD_FT_RAPPORTO = gsub('\'', '', COD_FT_RAPPORTO))
  
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
    select('abi', 'ndg', all_of(c(col_num, col_char))) %>%
    setNames(c('abi', 'ndg', paste0('RAW_', c(col_num, col_char)))) 
  
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
          mutate(COD_UO = '.') %>%    # used only for RISCHIO.CSV
          select(-COD_ABI, -COD_UO) %>%
          mutate(COD_NAG = gsub('\'', '', COD_NAG),
                 abi = bank_abi) %>%
          rename(ndg = COD_NAG), by = c("abi", "ndg")) %>%
      filter(!is.na(DATA_RIFERIMENTO)) %>%
      mutate(DATA_RIFERIMENTO = as.Date(gsub('\'', '', DATA_RIFERIMENTO), format = '%Y%m%d'))
    if (file_type != 'RISCHIO'){
      data = data %>% left_join(forme_tecniche, by = "COD_FT_RAPPORTO")
    } else {
      data$COD_FT_RAPPORTO = forme_tecniche$COD_FT_RAPPORTO[1]   # random one
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
      data_compact_out = rbindlist(c(list(data_compact_out), data_compact_list)) %>% as.data.frame()
      data_compact_list = list()
      list_ind = 0
    }
    
  } # i
  tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
  cat('\n Total elapsed time:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
  
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
}

# Statistical analysis: numerical, data and character columns
basicStatistics = function(data){

  data=as.data.frame(data)
  
  # Get numerical columns
  nums <- names(which(sapply(data, is.numeric)))
  if (length(nums)>0){
    StatNum = basicStats(data[, nums])
    uni=as.numeric(summarise_all(data.frame(data[,nums]), n_distinct))
    StatNum = rbind(UNIQUE_VALS=uni,StatNum)
    rn=rownames(StatNum)
    StatNum = data.frame(cbind(t(StatNum[-2,])));colnames(StatNum)=rn[-2] # remove num of observ
    rownames(StatNum)=nums
    StatNum$NAs=paste0(StatNum$NAs,' (',signif(StatNum$NAs/nrow(data)*100,digits=2),'%)')
    StatNum$UNIQUE_VALS=as.character(StatNum$UNIQUE_VALS)
    
  } else {StatNum=data.frame()}
  
  # Get dates columns
  dates <- names(which(sapply(data, is.Date)))
  StatDat = c()
  for (i in dates){
    dat=data[,i];dat=dat[!is.na(dat)]
    if (length(dat)>0){
      # quantile for dates
      dd = sort(dat)
      leg=data.frame(val=unique(dd));leg$id=c(1:nrow(leg))
      ind=match(dd,leg$val);aa=data.frame(val=dd,id=leg$id[ind])
      qq=round(quantile(aa$id))
      qq=aa[match(qq,aa$id),1]
    } else {qq=rep('NA',5)}
    StatDat=rbind(StatDat,c(paste0(nrow(data)-length(dat),' (',signif((nrow(data)-length(dat))/nrow(data)*100,digits=2),'%)'),
                            as.character(qq), uniqueN(dat)))
    
  }
  StatDat = as.data.frame(StatDat, stringsAsFactors = F)
  if (length(StatDat)>0){StatDat=StatDat %>% setNames(c('NAs','MIN','25%','50%','75%','MAX', 'UNIQUE_VALS'));rownames(StatDat)=dates}
  
  # Get characters columns
  chars <- names(which(sapply(data, is.character)))
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
  Stat=bind_rows(StatNum,StatDat,StatChar)
  Stat=cbind(VARIABLE=c(rownames(StatNum),rownames(StatDat),rownames(StatChar)),
             TYPE=c(rep('NUMERIC',length(nums)),rep('DATE',length(dates)),rep('CHARACTER',length(chars))),
             NUM_OSS=nrow(data),Stat) %>%
    select(VARIABLE, TYPE, NUM_OSS, UNIQUE_VALS, NAs, BLANKs, everything())
  
  final=data.frame(VARIABLE=colnames(data))
  final = final %>% left_join(Stat, by = "VARIABLE")
  final = as.matrix(final)
  final[is.na(final)]=''
  final = as.data.frame(final)
  
  return(final)
}
