
memory.limit(size=1000000000000000000)
library(readxl)
library(stringr)
library(plm)
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


# define perimeter and load peers variables
{
  ATECO_to_industry = read.csv2('./Distance_to_Default/ATECO_to_industry.csv', stringsAsFactors=FALSE, colClasses = 'character') %>% select(-Note)
  
  df_final = readRDS('./Checkpoints/df_final.rds') %>%
    filter(FLAG_BILANCIO == 'CEBI') %>%
    select(abi, ndg, chiave_comune, year, month, segmento_CRIF, FLAG_Multibank, ANAG_ateco, ANAG_flag_def, starts_with('BILA'),
           Tot_Attivo, Tot_Valore_Produzione, Tot_Attivo_log10, Dimensione_Impresa) %>%
    rename(FLAG_Default = ANAG_flag_def) %>%
    mutate(FLAG_Default = ifelse(is.na(FLAG_Default), 0, FLAG_Default)) %>%
    mutate(COD_ATECO_2DIG = substr(ANAG_ateco, 1, 2)) %>%
    select(-BILA_tipo_bilancio, -BILA_anno_primo_bil, -BILA_attributo, -BILA_anno_secondo_bil, -BILA_flag_esclusione_secondo_bil,
           -BILA_tipo_bilancio_calc_2, -BILA_anno_terzo_bil, -BILA_flag_esclusione_terzo_bil, -BILA_tipo_bilancio_calc_3,
           -BILA_anno_aggancio_bil, -BILA_tipo_dataentry_fin, -BILA_COD_DE, -ANAG_ateco) %>%
    left_join(ATECO_to_industry, by = "COD_ATECO_2DIG") %>%
    select(-month) %>%
    unique()
  if (df_final %>% group_by(abi, ndg) %>% summarize(COUNT = uniqueN(COD_ATECO_2DIG), .groups = 'drop') %>% pull(COUNT) %>% max() > 1){
    cat('\n\n ###### multiple COD_ATECO_2DIG for same abi+ndg')
  }
  
  # remove missing ATECO
  missing_ATECO = df_final %>%
    filter(COD_ATECO_2DIG=='') %>%
    select(abi, ndg) %>%
    unique()
  if (nrow(missing_ATECO) > 0){
    cat('\n ---', nrow(missing_ATECO), 'abi+ndg with missing ATECO removed')
    df_final = df_final %>%
      left_join(missing_ATECO %>% mutate(REMOVE = 'YES'), by = c("abi", "ndg")) %>%
      filter(is.na(REMOVE)) %>%
      select(-REMOVE)
  }
  if (sum(is.na(df_final)) > 0){cat('\n\n ###### missing values in df_final')}
  
  summary_perimeter = df_final %>%
    mutate(Dummy_industry = ifelse(Dummy_industry == '', 'Other', Dummy_industry)) %>%
    select(abi, ndg, Dummy_industry, Industry) %>%
    unique() %>%
    group_by(Dummy_industry, Industry) %>%
    summarize(Tot_Abi_Ndg = n(), .groups = 'drop') %>%
    arrange(desc(Industry))
  if (sum(summary_perimeter$Tot_Abi_Ndg) != df_final %>% select(abi, ndg) %>% uniqueN()){cat('\n\n ###### mismatch in abi+ndg counting')}
  summary_perimeter = summary_perimeter %>%
    mutate(Action = ifelse(Industry == '', 'Removed', '')) %>%
    bind_rows(data.frame(Dummy_industry = 'TOTAL', Industry = '', Action = '',
                         Tot_Abi_Ndg = sum(summary_perimeter$Tot_Abi_Ndg), stringsAsFactors = FALSE)) %>%
    mutate(Tot_Abi_Ndg = format(Tot_Abi_Ndg, big.mark = ','))
  write.table(summary_perimeter, './Distance_to_Default/Stats/01_BILA_perimeter_summary.csv', sep = ';', row.names = F, append = F)
  
  # remove Dummy_industry != 'manifacturing/services' from BILA
  df_final = df_final %>%
    filter(Dummy_industry != '')
  
  # load peers
  df_peers = read.csv2('./Distance_to_Default/Data/Peers_ORBIS.csv', stringsAsFactors=FALSE) %>%
    rename(Industry = NACE.Rev..2.main.section) %>%
    setNames(gsub('......', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('.....', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('....', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('...', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('..', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('.', '_', names(.), fixed = TRUE)) %>%
    mutate(NACE_Rev_2_core_code_4_digits_ = as.character(NACE_Rev_2_core_code_4_digits_))
  df_peers =df_peers %>%
    select(df_peers %>% select_if(is.character) %>% colnames(), df_peers %>% select_if(is.numeric) %>% colnames() %>% sort()) %>%
    left_join(ATECO_to_industry %>% select(Industry, Dummy_industry) %>% unique(), by = "Industry") %>%
    filter(Company_name_Latin_alphabet != 'CAD IT S.P.A.') %>%
    mutate(onerifinanz_mol_2011 = Financial_expenses_th_EUR_2011 / EBITDA_th_EUR_2011,    # used to match BILA_onerifinanz_mol
           onerifinanz_mol_2012 = Financial_expenses_th_EUR_2012 / EBITDA_th_EUR_2012,
           onerifinanz_mol_2013 = Financial_expenses_th_EUR_2013 / EBITDA_th_EUR_2013,
           onerifinanz_mol_2014 = Financial_expenses_th_EUR_2014 / EBITDA_th_EUR_2014)
  write.table(df_peers %>% group_by(Dummy_industry) %>% summarise(Tot_Abi_Ndg = n(), .groups = 'drop'),
              './Distance_to_Default/Stats/01_ORBIS_perimeter_summary.csv', sep = ';', row.names = F, append = F)
}

# baseline model FLAG_Default vs short list BILA
{
  # https://cran.r-project.org/web/packages/plm/vignettes/plmPackage.html#fnref3
  
  short_list = c('BILA_cashflow_ricavi', 'BILA_totdebiti_su_patrim', 'BILA_debitifornit_su_patrim', 'BILA_durata_scorte',
                 'BILA_liquid_imm', 'BILA_oneri_valagg', 'BILA_patr_su_patrml', 'BILA_roa', 'BILA_rotazione_circolante', 'BILA_turnover')
  df_reg = df_final %>% select(abi, ndg, year, month)
}













ORBIS_mapping = data.frame(Year_Variable = df_peers %>% select_if(is.numeric) %>% colnames(), stringsAsFactors = F) %>%
  mutate(Single_Variable = gsub('_2011|_2012|_2013|_2014', '', Year_Variable)) %>%
  left_join(data.frame(Orbis = c('Current_ratio', 'Liquidity_ratio', 'onerifinanz_mol', 'EBITDA_margin',
                                 'Gearing', 'ROA_using_Net_income', 'ROE_using_Net_income', 'Stock_turnover'),
                       BILA = c('BILA_current_ratio', 'BILA_liquid_imm', 'BILA_onerifinanz_mol', 'BILA_mol_ricavi',
                                'BILA_leverage', 'BILA_roa', 'BILA_roe', 'BILA_rotazione_magazzino'), stringsAsFactors = F),
            by = c('Single_Variable' = 'Orbis')) %>%
  replace(is.na(.), '')





#    FALCK RENEWABLES S.P.A. dovrebbe essere un verde (è industry D) -> 22 manifact e 19 service
#    CAD IT S.P.A. è tutta vuota -> rimossa
#    flag_default mancante - > 0
