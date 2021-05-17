
utils::memory.limit(32000)
library(haven)
library(readxl)
library(stringr)
library(ppcor)
library(lme4)
library(optimx)
library(reshape)
library(Hmisc)
library(lubridate)
library(ggplot2)
library(ggforce)
library(gganimate)
library(gtable)
library(grid)
library(gridExtra)
library(randtoolbox)
library(clusterCrit)
library(parallelDist)
library(magick)
library(mlrMBO)
library(rpca)
library(uwot)   # for UMAP
library(DescTools)
library(LatticeDesign)
library(PKPDmisc)
library(parallelMap)
library(parallel)
library(doParallel)
library(future)
library(future.apply)
library(glmnet)
library(HEMDAG)
library(MLmetrics)
library(ROCit)
library(fastDummies)
library(wesanderson)
library(ranger)
library(earth)
library(kernlab)
library(RSBID)
library(rgl)
library(tictoc)
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
# only for Keras - it will automatically activate the R_Keras environment. If you want to switch the env you have to restart R session
{
  library(reticulate)
  np <- import("numpy", convert = FALSE)
  library(keras)
  library(tensorflow)
  source('./Help_Autoencoder.R')
  gpu <- tf$config$experimental$get_visible_devices('GPU')[[1]]
  tf$config$experimental$set_memory_growth(device = gpu, enable = TRUE)   # allows max memory usage and to run multiple session with GPU
}


# define perimeter and load peers variables - ###### FLAG_Default is valid only for 2012 and 2013. Values for 2014 (all 0) are actual NA
reload_df = T  # reload df_final
{
  ATECO_to_industry = read.csv2('./Distance_to_Default/ATECO_to_industry.csv', stringsAsFactors=FALSE, colClasses = 'character') %>% select(-Note)
  
  if (reload_df == FALSE){
  df_final = readRDS('./Checkpoints/df_final.rds') %>%
    filter(FLAG_BILANCIO == 'CEBI') %>%
    select(abi, ndg, chiave_comune, year, month, segmento_CRIF, FLAG_Multibank, ANAG_ateco, ANAG_flag_def, starts_with('BILA'),
           Tot_Attivo, Tot_Equity, Tot_Valore_Produzione, Tot_Attivo_log10, Dimensione_Impresa, Regione_Macro) %>%
    rename(FLAG_Default = ANAG_flag_def) %>%
    mutate(FLAG_Default = ifelse(is.na(FLAG_Default), 0, FLAG_Default)) %>%
    mutate(COD_ATECO_2DIG = substr(ANAG_ateco, 1, 2)) %>%
    select(-BILA_tipo_bilancio, -BILA_anno_primo_bil, -BILA_attributo, -BILA_anno_secondo_bil, -BILA_flag_esclusione_secondo_bil,
           -BILA_tipo_bilancio_calc_2, -BILA_anno_terzo_bil, -BILA_flag_esclusione_terzo_bil, -BILA_tipo_bilancio_calc_3,
           -BILA_anno_aggancio_bil, -BILA_tipo_dataentry_fin, -BILA_COD_DE, -ANAG_ateco, -BILA_natura, -BILA_schemaril, -BILA_strutbil) %>%
    mutate_at(vars(starts_with('BILA_')), ~ . / 100) %>%
    mutate(Tot_Attivo_log10 = round(Tot_Attivo_log10)) %>%
    left_join(ATECO_to_industry, by = "COD_ATECO_2DIG") %>%
    select(-month) %>%
    unique()
  saveRDS(df_final, './Distance_to_Default/Checkpoints/df_final.rds')
  }
  df_final = readRDS('./Distance_to_Default/Checkpoints/df_final.rds')
  if (df_final %>% group_by(abi, ndg) %>% summarize(COUNT = uniqueN(COD_ATECO_2DIG), .groups = 'drop') %>% pull(COUNT) %>% max() > 1){
    cat('\n\n ###### multiple COD_ATECO_2DIG for same abi+ndg')
  }

  # remove missing ATECO
  missing_ATECO = df_final %>%
    filter(COD_ATECO_2DIG=='') %>%
    select(abi, ndg) %>%
    unique()
  if (nrow(missing_ATECO) > 0){
    cat('\n ---', nrow(missing_ATECO), 'abi+ndg with missing ATECO removed in df_final')
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
    arrange(desc(Industry)) %>%
    left_join(
      df_final %>%
        mutate(year = paste0('Y', year),
               Dummy_industry = ifelse(Dummy_industry == '', 'Other', Dummy_industry)) %>%
        group_by(Dummy_industry, Industry, abi, ndg, year) %>%
        summarize(COUNT = n(), .groups = "drop") %>%
        spread(year, COUNT) %>%
        group_by(Dummy_industry, Industry) %>%
        summarize_if(is.numeric, sum, na.rm = T), by = c("Dummy_industry", "Industry"))
  if (sum(summary_perimeter$Tot_Abi_Ndg) != df_final %>% select(abi, ndg) %>% uniqueN()){cat('\n\n ###### mismatch in abi+ndg counting')}
  summary_perimeter = summary_perimeter %>%
    mutate(Action = ifelse(Industry == '', 'Removed', '')) %>%
    bind_rows(data.frame(Dummy_industry = 'TOTAL', Industry = '', Action = '',
                         Tot_Abi_Ndg = sum(summary_perimeter$Tot_Abi_Ndg),
                         Y2012 = sum(summary_perimeter$Y2012, na.rm = T),
                         Y2013 = sum(summary_perimeter$Y2013, na.rm = T),
                         Y2014 = sum(summary_perimeter$Y2014, na.rm = T), stringsAsFactors = FALSE)) %>%
    replace(is.na(.), 0) %>%
    mutate_at(c("Tot_Abi_Ndg", "Y2012", "Y2013", "Y2014"), ~format(., big.mark = ','))
  write.table(summary_perimeter, './Distance_to_Default/Stats/01_BILA_perimeter_summary.csv', sep = ';', row.names = F, append = F)
  
  summary_years = df_final %>%
    group_by(abi, ndg) %>%
    summarize(tot_years = n(), .groups = "drop") %>%
    group_by(tot_years) %>%
    summarize(abi_ndg = format(n(), big.mark = ","), .groups = "drop")
  write.table(summary_years, './Distance_to_Default/Stats/01_BILA_abi_ndg_per_year.csv', sep = ';', row.names = F, append = F)
  
  # remove Dummy_industry != 'manifacturing/services' from BILA
  df_final = df_final %>%
    filter(Dummy_industry != '')
  
  # load peers
  var_div_by_100 = c('EBITDA_margin_', 'Gearing_', 'ROA_using_Net_income_', 'ROE_using_Net_income_', 'Cash_flow_Operating_revenue')
  df_peers = read.csv2('./Distance_to_Default/Data/Peers_ORBIS.csv', stringsAsFactors=FALSE) %>%
    rename(Industry = NACE.Rev..2.main.section) %>%
    setNames(gsub('......', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('.....', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('....', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('...', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('..', '_', names(.), fixed = TRUE)) %>%
    setNames(gsub('.', '_', names(.), fixed = TRUE)) %>%
    mutate(NACE_Rev_2_core_code_4_digits_ = as.character(NACE_Rev_2_core_code_4_digits_)) %>%
    left_join(read.csv2('./Distance_to_Default/Data/Peers_ORBIS_Regione.csv', stringsAsFactors=FALSE) %>% select(-NAME), by = "European_VAT_number") %>%
    mutate(
      Regione_Macro = case_when(
        Regione %in% c("PIE", "LIG", "LOM", "VDA") ~ "NORD-OVEST",
        Regione %in% c("TAA", "VEN", "FVG", "EMR") ~ "NORD-EST",
        Regione %in% c("TOS", "UMB", "MAR", "LAZ") ~ "CENTRO",
        Regione %in% c("ABR", "CAM", "PUG", "BAS", "CAL", "MOL", 'OTH') ~ "SUD",
        Regione %in% c("SIC", "SAR") ~ "ISOLE")
    ) %>%
    select(-Regione) %>%
    mutate_at(vars(-Company_name_Latin_alphabet, -NACE_Rev_2_core_code_4_digits_, -European_VAT_number, -Industry, -Regione_Macro), as.numeric) %>%
    left_join(ATECO_to_industry %>% select(Industry, Dummy_industry) %>% unique(), by = "Industry") %>%
    filter(Company_name_Latin_alphabet != "CHL SPA")
  df_peers = df_peers %>%
    # mutate_at(vars(matches('_th_EUR_')), ~ if_else(is.na(.), 0, .)) %>%   # todo: rimetti?
    select(df_peers %>% select_if(is.character) %>% colnames(), df_peers %>% select_if(is.numeric) %>% colnames() %>% sort()) %>%
    mutate(onerifinanz_mol_2011 = Financial_expenses_th_EUR_2011 / EBITDA_th_EUR_2011,    # used to match BILA_onerifinanz_mol
           onerifinanz_mol_2012 = Financial_expenses_th_EUR_2012 / EBITDA_th_EUR_2012,
           onerifinanz_mol_2013 = Financial_expenses_th_EUR_2013 / EBITDA_th_EUR_2013,
           onerifinanz_mol_2014 = Financial_expenses_th_EUR_2014 / EBITDA_th_EUR_2014,
           
           Tot_Attivo_log10_2011 = round(log10(Total_Current_Assets_th_EUR_2011) + 3),     # used to match Tot_Attivo_log10
           Tot_Attivo_log10_2012 = round(log10(Total_Current_Assets_th_EUR_2012) + 3),
           Tot_Attivo_log10_2013 = round(log10(Total_Current_Assets_th_EUR_2013) + 3),
           Tot_Attivo_log10_2014 = round(log10(Total_Current_Assets_th_EUR_2014) + 3),
           
           Tot_Attivo_2011 = Total_Current_Assets_th_EUR_2011 * 1000,     # used for DD evaluation
           Tot_Attivo_2012 = Total_Current_Assets_th_EUR_2012 * 1000,
           Tot_Attivo_2013 = Total_Current_Assets_th_EUR_2013 * 1000,
           Tot_Attivo_2014 = Total_Current_Assets_th_EUR_2014 * 1000,
           
           Tot_Equity_2011 = Total_Shareholders_Equity_th_EUR_2011 * 1000,     # used for DD evaluation
           Tot_Equity_2012 = Total_Shareholders_Equity_th_EUR_2012 * 1000,
           Tot_Equity_2013 = Total_Shareholders_Equity_th_EUR_2013 * 1000,
           Tot_Equity_2014 = Total_Shareholders_Equity_th_EUR_2014 * 1000,
           
           ARIC_VPROD_2011 = (Operating_revenue_Turnover_th_EUR_2011 - Sales_th_EUR_2011) / Operating_revenue_Turnover_th_EUR_2011,
           ARIC_VPROD_2012 = (Operating_revenue_Turnover_th_EUR_2012 - Sales_th_EUR_2012) / Operating_revenue_Turnover_th_EUR_2012,
           ARIC_VPROD_2013 = (Operating_revenue_Turnover_th_EUR_2013 - Sales_th_EUR_2013) / Operating_revenue_Turnover_th_EUR_2013,
           ARIC_VPROD_2014 = (Operating_revenue_Turnover_th_EUR_2014 - Sales_th_EUR_2014) / Operating_revenue_Turnover_th_EUR_2014,
           
           debbanche_circolante_2011 = Bank_Loans_th_EUR_2011 / Working_capital_th_EUR_2011,
           debbanche_circolante_2012 = Bank_Loans_th_EUR_2012 / Working_capital_th_EUR_2012,
           debbanche_circolante_2013 = Bank_Loans_th_EUR_2013 / Working_capital_th_EUR_2013,
           debbanche_circolante_2014 = Bank_Loans_th_EUR_2014 / Working_capital_th_EUR_2014,
           
           copertura_immob_2011 = (Shareholders_funds_th_EUR_2011 + Long_term_debt_th_EUR_2011) / Fixed_assets_th_EUR_2011,
           copertura_immob_2012 = (Shareholders_funds_th_EUR_2012 + Long_term_debt_th_EUR_2012) / Fixed_assets_th_EUR_2012,
           copertura_immob_2013 = (Shareholders_funds_th_EUR_2013 + Long_term_debt_th_EUR_2013) / Fixed_assets_th_EUR_2013,
           copertura_immob_2014 = (Shareholders_funds_th_EUR_2014 + Long_term_debt_th_EUR_2014) / Fixed_assets_th_EUR_2014,
           
           costolavoro_valagg_2011 = Costs_of_employees_th_EUR_2011 / Added_value_th_EUR_2011,
           costolavoro_valagg_2012 = Costs_of_employees_th_EUR_2012 / Added_value_th_EUR_2012,
           costolavoro_valagg_2013 = Costs_of_employees_th_EUR_2013 / Added_value_th_EUR_2013,
           costolavoro_valagg_2014 = Costs_of_employees_th_EUR_2014 / Added_value_th_EUR_2014,
           
           debbrevi_debbanche_2011 = Total_Current_Liabilities_th_EUR_2011 / Bank_Loans_th_EUR_2011,
           debbrevi_debbanche_2012 = Total_Current_Liabilities_th_EUR_2012 / Bank_Loans_th_EUR_2012,
           debbrevi_debbanche_2013 = Total_Current_Liabilities_th_EUR_2013 / Bank_Loans_th_EUR_2013,
           debbrevi_debbanche_2014 = Total_Current_Liabilities_th_EUR_2014 / Bank_Loans_th_EUR_2014,
           
           totdebiti_su_debitibreve_2011 = (Non_current_liabilities_th_EUR_2011 + Total_Current_Liabilities_th_EUR_2011) / Total_Current_Liabilities_th_EUR_2011,
           totdebiti_su_debitibreve_2012 = (Non_current_liabilities_th_EUR_2012 + Total_Current_Liabilities_th_EUR_2012) / Total_Current_Liabilities_th_EUR_2012,
           totdebiti_su_debitibreve_2013 = (Non_current_liabilities_th_EUR_2013 + Total_Current_Liabilities_th_EUR_2013) / Total_Current_Liabilities_th_EUR_2013,
           totdebiti_su_debitibreve_2014 = (Non_current_liabilities_th_EUR_2014 + Total_Current_Liabilities_th_EUR_2014) / Total_Current_Liabilities_th_EUR_2014,
           
           totdebiti_su_patrim_2011 = (Non_current_liabilities_th_EUR_2011 + Total_Current_Liabilities_th_EUR_2011) / Shareholders_funds_th_EUR_2011,
           totdebiti_su_patrim_2012 = (Non_current_liabilities_th_EUR_2012 + Total_Current_Liabilities_th_EUR_2012) / Shareholders_funds_th_EUR_2012,
           totdebiti_su_patrim_2013 = (Non_current_liabilities_th_EUR_2013 + Total_Current_Liabilities_th_EUR_2013) / Shareholders_funds_th_EUR_2013,
           totdebiti_su_patrim_2014 = (Non_current_liabilities_th_EUR_2014 + Total_Current_Liabilities_th_EUR_2014) / Shareholders_funds_th_EUR_2014,
           
           debitifornit_su_patrim_2011 = Creditors_th_EUR_2011 / Shareholders_funds_th_EUR_2011,
           debitifornit_su_patrim_2012 = Creditors_th_EUR_2012 / Shareholders_funds_th_EUR_2012,
           debitifornit_su_patrim_2013 = Creditors_th_EUR_2013 / Shareholders_funds_th_EUR_2013,
           debitifornit_su_patrim_2014 = Creditors_th_EUR_2014 / Shareholders_funds_th_EUR_2014,
           
           debitifornit_su_totdebiti_2011 = Creditors_th_EUR_2011 / (Non_current_liabilities_th_EUR_2011 + Total_Current_Liabilities_th_EUR_2011),
           debitifornit_su_totdebiti_2012 = Creditors_th_EUR_2012 / (Non_current_liabilities_th_EUR_2012 + Total_Current_Liabilities_th_EUR_2012),
           debitifornit_su_totdebiti_2013 = Creditors_th_EUR_2013 / (Non_current_liabilities_th_EUR_2013 + Total_Current_Liabilities_th_EUR_2013),
           debitifornit_su_totdebiti_2014 = Creditors_th_EUR_2014 / (Non_current_liabilities_th_EUR_2014 + Total_Current_Liabilities_th_EUR_2014),
           
           durata_scorte_2011 = Stock_th_EUR_2011 / (Operating_revenue_Turnover_th_EUR_2011 / 365) / 100,
           durata_scorte_2012 = Stock_th_EUR_2012 / (Operating_revenue_Turnover_th_EUR_2012 / 365) / 100,
           durata_scorte_2013 = Stock_th_EUR_2013 / (Operating_revenue_Turnover_th_EUR_2013 / 365) / 100,
           durata_scorte_2014 = Stock_th_EUR_2014 / (Operating_revenue_Turnover_th_EUR_2014 / 365) / 100,
           
           oneri_ricavi_2011 = Financial_expenses_th_EUR_2011 / Operating_revenue_Turnover_th_EUR_2011,
           oneri_ricavi_2012 = Financial_expenses_th_EUR_2012 / Operating_revenue_Turnover_th_EUR_2012,
           oneri_ricavi_2013 = Financial_expenses_th_EUR_2013 / Operating_revenue_Turnover_th_EUR_2013,
           oneri_ricavi_2014 = Financial_expenses_th_EUR_2014 / Operating_revenue_Turnover_th_EUR_2014,
           
           oneri_valagg_2011 = Financial_expenses_th_EUR_2011 / Added_value_th_EUR_2011,
           oneri_valagg_2012 = Financial_expenses_th_EUR_2012 / Added_value_th_EUR_2012,
           oneri_valagg_2013 = Financial_expenses_th_EUR_2013 / Added_value_th_EUR_2013,
           oneri_valagg_2014 = Financial_expenses_th_EUR_2014 / Added_value_th_EUR_2014,
           
           patr_su_patrml_2011 = Shareholders_funds_th_EUR_2011 / (Shareholders_funds_th_EUR_2011 + Non_current_liabilities_th_EUR_2011),
           patr_su_patrml_2012 = Shareholders_funds_th_EUR_2012 / (Shareholders_funds_th_EUR_2012 + Non_current_liabilities_th_EUR_2012),
           patr_su_patrml_2013 = Shareholders_funds_th_EUR_2013 / (Shareholders_funds_th_EUR_2013 + Non_current_liabilities_th_EUR_2013),
           patr_su_patrml_2014 = Shareholders_funds_th_EUR_2014 / (Shareholders_funds_th_EUR_2014 + Non_current_liabilities_th_EUR_2014),
           
           patr_su_patrrim_2011 = Shareholders_funds_th_EUR_2011 / (Shareholders_funds_th_EUR_2011 + Stock_th_EUR_2011),
           patr_su_patrrim_2012 = Shareholders_funds_th_EUR_2012 / (Shareholders_funds_th_EUR_2012 + Stock_th_EUR_2012),
           patr_su_patrrim_2013 = Shareholders_funds_th_EUR_2013 / (Shareholders_funds_th_EUR_2013 + Stock_th_EUR_2013),
           patr_su_patrrim_2014 = Shareholders_funds_th_EUR_2014 / (Shareholders_funds_th_EUR_2014 + Stock_th_EUR_2014),
           
           rod_2011 = Operating_P_L_EBIT_th_EUR_2011 / Bank_Loans_th_EUR_2011 / 100,
           rod_2012 = Operating_P_L_EBIT_th_EUR_2012 / Bank_Loans_th_EUR_2012 / 100,
           rod_2013 = Operating_P_L_EBIT_th_EUR_2013 / Bank_Loans_th_EUR_2013 / 100,
           rod_2014 = Operating_P_L_EBIT_th_EUR_2014 / Bank_Loans_th_EUR_2014 / 100,
           
           # rod_income_2011 = P_L_for_period_Net_income_th_EUR_2011 / Long_term_debt_th_EUR_2011 / 100,
           # rod_income_2012 = P_L_for_period_Net_income_th_EUR_2012 / Long_term_debt_th_EUR_2012 / 100,
           # rod_income_2013 = P_L_for_period_Net_income_th_EUR_2013 / Long_term_debt_th_EUR_2013 / 100,
           # rod_income_2014 = P_L_for_period_Net_income_th_EUR_2014 / Long_term_debt_th_EUR_2014 / 100,
           # 
           # rod_income_div2_2011 = P_L_for_period_Net_income_th_EUR_2011 / (Long_term_debt_th_EUR_2011 / 2) / 100,
           # rod_income_div2_2012 = P_L_for_period_Net_income_th_EUR_2012 / (Long_term_debt_th_EUR_2012 / 2) / 100,
           # rod_income_div2_2013 = P_L_for_period_Net_income_th_EUR_2013 / (Long_term_debt_th_EUR_2013 / 2) / 100,
           # rod_income_div2_2014 = P_L_for_period_Net_income_th_EUR_2014 / (Long_term_debt_th_EUR_2014 / 2) / 100,
           
           turnover_2011 = Operating_revenue_Turnover_th_EUR_2011 / Total_assets_th_EUR_2011,
           turnover_2012 = Operating_revenue_Turnover_th_EUR_2012 / Total_assets_th_EUR_2012,
           turnover_2013 = Operating_revenue_Turnover_th_EUR_2013 / Total_assets_th_EUR_2013,
           turnover_2014 = Operating_revenue_Turnover_th_EUR_2014 / Total_assets_th_EUR_2014,
           
           # ammor_su_costi_2011 = Depreciation_th_EUR_2011 / (Costs_of_goods_sold_th_EUR_2011 + Other_operating_expenses_th_EUR_2011),
           # ammor_su_costi_2012 = Depreciation_th_EUR_2012 / (Costs_of_goods_sold_th_EUR_2012 + Other_operating_expenses_th_EUR_2012),
           # ammor_su_costi_2013 = Depreciation_th_EUR_2013 / (Costs_of_goods_sold_th_EUR_2013 + Other_operating_expenses_th_EUR_2013),
           # ammor_su_costi_2014 = Depreciation_th_EUR_2014 / (Costs_of_goods_sold_th_EUR_2014 + Other_operating_expenses_th_EUR_2014),
           
           ammor_su_costi_2011 = Depreciation_Amortization_th_EUR_2011 / (Costs_of_goods_sold_th_EUR_2011 + Other_operating_expenses_th_EUR_2011),
           ammor_su_costi_2012 = Depreciation_Amortization_th_EUR_2012 / (Costs_of_goods_sold_th_EUR_2012 + Other_operating_expenses_th_EUR_2012),
           ammor_su_costi_2013 = Depreciation_Amortization_th_EUR_2013 / (Costs_of_goods_sold_th_EUR_2013 + Other_operating_expenses_th_EUR_2013),
           ammor_su_costi_2014 = Depreciation_Amortization_th_EUR_2014 / (Costs_of_goods_sold_th_EUR_2014 + Other_operating_expenses_th_EUR_2014)
           ) %>%
    mutate_at(vars(matches(paste0(var_div_by_100, collapse = '|'))), ~ . / 100) %>%
    mutate_at(vars(starts_with('onerifinanz_mol_')), ~round(., 2)) %>%
    mutate_at(vars(starts_with('debbrevi_debbanche_')), function(x) ifelse(is.infinite(x), 100, x)) %>%
    mutate_at(vars(starts_with('rod_')), function(x) ifelse(is.infinite(x), 1, x))
  write.table(df_peers %>% group_by(Dummy_industry) %>% summarise(Tot_Abi_Ndg = n(), .groups = 'drop'),
              './Distance_to_Default/Stats/01_ORBIS_peers_perimeter_summary.csv', sep = ';', row.names = F, append = F)
  rm(summary_perimeter, missing_ATECO, summary_years, ATECO_to_industry)
}



# todo: rimuovi!    serviva solo a controllare le variabili
# aa = df_peers %>% select(starts_with('Cash_flow_Operating_revenue'), starts_with('Net_Profit_'), starts_with('Depreciation_Amortization_'),
#                          starts_with('Operating_revenue_Turnover')) %>%
#   # select(ends_with('2013')) %>%
#   # setNames(gsub('_2013', '', names(.))) %>%
#   # mutate(dd = Trade_Creditors_th_EUR - Creditors_th_EUR)
#   mutate(ref = 'pp') %>%
#   mutate(cashflow_ricavi_ricalc_2011 = (Net_Profit_th_EUR_2011 + Depreciation_Amortization_th_EUR_2011) / Operating_revenue_Turnover_th_EUR_2011,
#          cashflow_ricavi_ricalc_2012 = (Net_Profit_th_EUR_2012 + Depreciation_Amortization_th_EUR_2012) / Operating_revenue_Turnover_th_EUR_2012,
#          cashflow_ricavi_ricalc_2013 = (Net_Profit_th_EUR_2013 + Depreciation_Amortization_th_EUR_2013) / Operating_revenue_Turnover_th_EUR_2013,
#          cashflow_ricavi_ricalc_2014 = (Net_Profit_th_EUR_2014 + Depreciation_Amortization_th_EUR_2014) / Operating_revenue_Turnover_th_EUR_2014
#   ) %>%
#   select(ref, starts_with('cashflow_ricavi_ricalc')) %>%
#   setDT() %>%
#   melt(id.vars = 'ref',
#        # measure.vars = ORBIS_long_coding$Year_Variable,
#        variable.name = "variable_ricalc",
#        value.name = 'value_ricalc') %>%
#   mutate(value_ricalc = round(value_ricalc, 4)) %>%
#   bind_cols(
#     df_peers %>% select(starts_with('Cash_flow_Operating_revenue')) %>%
#       mutate(ref = 'pp') %>%
#       setDT() %>%
#       melt(id.vars = 'ref',
#            # measure.vars = ORBIS_long_coding$Year_Variable,
#            variable.name = "original",
#            value.name = 'value_original') %>%
#       select(-ref)
#   ) %>%
#   mutate(dd = round(value_ricalc - value_original, 5))

# remove highly correlated variables
corr_thresh = 0.4   # theshold for partial correlation. Correlation > 0.7 is always removed
{
  
  short_list = c('BILA_cashflow_ricavi', 'BILA_totdebiti_su_patrim', 'BILA_debitifornit_su_patrim', 'BILA_durata_scorte',
                 'BILA_liquid_imm', 'BILA_oneri_valagg', 'BILA_patr_su_patrml', 'BILA_roa', 'BILA_rotazione_circolante', 'BILA_turnover')
  
  correlation_list = evaluate_correlation(df_final %>% select(starts_with('BILA_')))
  var_to_remove = correlation_list %>%
    filter(abs > corr_thresh) %>%
    select(Var1, Var2) %>%
    unlist() %>%
    table() %>%
    as.data.frame(stringsAsFactors = F) %>%
    setNames(c('Variable', 'Occurrence')) %>%
    arrange(desc(Occurrence)) %>%
    filter(Occurrence >= 2) %>%
    pull(Variable) %>%
    c(correlation_list %>%
        filter(abs(Corr) > 0.7) %>%
        select(Var1, Var2) %>%
        unlist()) %>%
    unique()
  
  # evaluate correlation also on annual average
  df_final_year_avg = df_final %>%
    select(abi, ndg, starts_with('BILA_')) %>%
    group_by(abi, ndg) %>%
    summarise_all(mean) %>%
    ungroup() %>%
    select(-abi, -ndg)
  correlation_list_avg = evaluate_correlation(df_final_year_avg)
  var_to_remove_avg = correlation_list_avg %>%
    filter(abs > corr_thresh) %>%
    select(Var1, Var2) %>%
    unlist() %>%
    table() %>%
    as.data.frame(stringsAsFactors = F) %>%
    setNames(c('Variable', 'Occurrence')) %>%
    arrange(desc(Occurrence)) %>%
    filter(Occurrence >= 2) %>%
    pull(Variable) %>%
    c(correlation_list_avg %>%
        filter(abs(Corr) > 0.7) %>%
        select(Var1, Var2) %>%
        unlist()) %>%
    unique()
  
  var_to_remove = unique(c(var_to_remove_avg, var_to_remove))
  var_to_remove = setdiff(var_to_remove, short_list)
  
  corr_mat_det = df_final %>% select(-all_of(var_to_remove)) %>% select(starts_with('BILA_')) %>% cov() %>% det()
  correlation_list_post = evaluate_correlation(df_final %>% select(starts_with('BILA_')) %>% select(-all_of(var_to_remove)))
  cat('\nRemoving correlated variables:')
  cat('\n ---', length(var_to_remove), '/', df_final %>% select(starts_with('BILA_')) %>% ncol(), 'variables removed')
  cat('\n   -', length(intersect(var_to_remove, short_list)), 'of which are in the "short list"')
  cat('\n   - Correlation matrix determinant:', corr_mat_det)
  write.table(var_to_remove, './Distance_to_Default/Stats/01_removed_variables.csv', sep = ';', row.names = F, append = F)
  write.table(correlation_list_post, './Distance_to_Default/Stats/01_selected_variables_correlation_matrix.csv', sep = ';', row.names = F, append = F)
  rm(var_to_remove_avg, correlation_list, correlation_list_avg, df_final_year_avg, correlation_list_post)
}
df_final_small = df_final %>%
  select(-all_of(setdiff(var_to_remove, short_list)))

# evaluate embeddings and evaluate UMAP 3D visualization
reload_embedding_input = T    # reload embedding input (scaled original data)
reload_PCA = T    # reload Robust PCA embedding
reload_autoencoder = T   # reload autoencoder embedding
tune_autoencoder_flag = F    # tune autoencoder parameters
reload_peers_embedding = T    # reload list of embedding and predicted embedding on peers dataset - requires Keras environment and will conflict with
                              # densMAP python environment when evaluating visualization data
reload_embedding_visualization = T    # reload evaluation of UMAP
reload_embedding_aggregated_visualization = T    # reload aggregated points visualization
run_embedding_report = F    # create embedding visualization report to choose combination of UMAP parameters
run_embedding_best_report = F    # create embedding visualization report for best paramaters and peers comparison
{
  # evaluate statistics between ORBIS and CRIF data
  {
    ORBIS_mapping = data.frame(Year_Variable = df_peers %>% select_if(is.numeric) %>% colnames(), stringsAsFactors = F) %>%
      mutate(Single_Variable = gsub('_2011|_2012|_2013|_2014', '', Year_Variable)) %>%
      left_join(data.frame(Orbis = c('Current_ratio', 'Liquidity_ratio', 'EBITDA_margin',
                                     'Gearing', 'ROE_using_Net_income', 'Stock_turnover', 'Tot_Attivo_log10',
                                     'ARIC_VPROD', 'debbanche_circolante', 'Cash_flow_Operating_revenue',
                                     'copertura_immob', 'costolavoro_valagg', 'debbrevi_debbanche',
                                     'totdebiti_su_debitibreve', 'totdebiti_su_patrim', 'debitifornit_su_patrim',
                                     'debitifornit_su_totdebiti', 'durata_scorte', 'oneri_ricavi', 'oneri_valagg',
                                     'patr_su_patrml', 'patr_su_patrrim', 'rod', 'Net_assets_turnover',
                                     'turnover', 'onerifinanz_mol', 'ROA_using_Net_income', 'ammor_su_costi'),
                           BILA = c('BILA_current_ratio', 'BILA_liquid_imm', 'BILA_mol_ricavi',
                                    'BILA_leverage', 'BILA_roe', 'BILA_rotazione_magazzino', 'Tot_Attivo_log10',
                                    'BILA_ARIC_VPROD', 'BILA_debbanche_circolante', 'BILA_cashflow_ricavi',
                                    'BILA_copertura_immob', 'BILA_costolavoro_valagg', 'BILA_debbrevi_debbanche',
                                    'BILA_totdebiti_su_debitibreve', 'BILA_totdebiti_su_patrim', 'BILA_debitifornit_su_patrim',
                                    'BILA_debitifornit_su_totdebiti', 'BILA_durata_scorte', 'BILA_oneri_ricavi', 'BILA_oneri_valagg',
                                    'BILA_patr_su_patrml', 'BILA_patr_su_patrrim', 'BILA_rod', 'BILA_rotazione_circolante',
                                    'BILA_turnover', 'BILA_onerifinanz_mol', 'BILA_roa', 'BILA_ammor_su_costi'), stringsAsFactors = F),
                by = c('Single_Variable' = 'Orbis')) %>%
      replace(is.na(.), '') %>%
      filter(BILA %in% (c(df_final_small %>% colnames(), '')))
    CRIF_unmapped = df_final %>% select(starts_with('BILA_')) %>% colnames() %>% setdiff(ORBIS_mapping %>% pull(BILA) %>% unique())
    ORBIS_mapped = ORBIS_mapping %>% filter(BILA != '') %>% pull(BILA) %>% unique()
    
    # count Inf and NaN in ORBIS_mapped
    summary_nan_inf = c()
    for (var in ORBIS_mapped){
      zz = df_peers %>%
        select(all_of(ORBIS_mapping %>% filter(BILA == var) %>% pull(Year_Variable))) %>%
        mutate(tot_na = apply(., 1, function(x) sum(is.na(x) & !is.nan(x))),
               tot_nan = apply(., 1, function(x) sum(is.nan(x))),
               tot_inf = apply(., 1, function(x) sum(is.infinite(x))))
      tot_years = ncol(zz) - 3
      summary_nan_inf = summary_nan_inf %>% bind_rows(
        data.frame(variable = var, Total_Missing = sum(zz$tot_na), Total_0over0 = sum(zz$tot_nan), Total_divby0 = sum(zz$tot_inf),
                   AllYear_Missing = sum(zz$tot_na==tot_years), AllYear_0over0 = sum(zz$tot_nan==tot_years), AllYear_divby0 = sum(zz$tot_inf==tot_years),
                   stringsAsFactors = F))
      rm(zz)
    }
    write.table(summary_nan_inf, './Distance_to_Default/Stats/01b_ORBIS_peers_Nan_and_Inf_summary.csv', sep = ';', row.names = F, append = F)
    
    # evaluate quantile and median for peers on ORBIS mapped variables (combination of Dummy_industry+median or Dummy_industry+[33%,66%]-percentile)
    #      and on all df_final for unmapped variables
    {
      ORBIS_label = c()
      boxplot_data = c()
      for (var in c(ORBIS_mapped, CRIF_unmapped)){
        
        mapped = 'YES'
        # for (ind in unique(df_peers$Dummy_industry)){
        df_values = df_final %>%
          select(all_of(c(var, 'Dummy_industry'))) %>%
          # filter(Dummy_industry == ind) %>%
          select(-Dummy_industry) %>%
          unlist() %>%
          as.numeric()
        if (var %in% ORBIS_mapped){
          ORBIS_values = df_peers %>%
            select(all_of(c(ORBIS_mapping %>% filter(BILA == var) %>% pull(Year_Variable), 'Dummy_industry'))) %>%
            # filter(Dummy_industry == ind) %>%
            select(-Dummy_industry) %>%
            unlist() %>%
            as.numeric() %>%
            replace(is.infinite(.), NA)
          
          boxplot_data = boxplot_data %>% bind_rows(      # different ind will be appended with the same label 'var' -> OK!
            data.frame(Variable = gsub('BILA_', '', var), Dataset = 'ORBIS', values = ORBIS_values, stringsAsFactors = F),
            data.frame(Variable = gsub('BILA_', '', var), Dataset = 'CRIF', values = df_values, stringsAsFactors = F))
        } else {
          ORBIS_values = df_values
          mapped = 'NO - Peer=CRIF'
        }
        
        quant = quantile(ORBIS_values, c(0.33, 0.66), na.rm = T) %>% as.numeric() %>% round(3)
        ORBIS_label = ORBIS_label %>% bind_rows(
          data.frame(Variable = var, mapped = mapped, Peer_NAs = sum(is.na(ORBIS_values)),  # Dummy_industry = ind, 
                     Peer_median = round(median(ORBIS_values, na.rm = T), 3), Peer_33rd = quant[1], Peer_66th = quant[2],
                     Peer_min = round(min(ORBIS_values, na.rm = T), 3), Peer_max = round(max(ORBIS_values, na.rm = T), 3),
                     CRIF_median = round(median(df_values, na.rm = T), 3),
                     CRIF_min = min(df_values, na.rm = T), CRIF_max = max(df_values, na.rm = T), stringsAsFactors = F)
        )
        # } # ind
      } # var
      
      # plot variable comparison boxplot
      if (reload_embedding_input == FALSE){
        png('./Distance_to_Default/Stats/02a0_Peers_variable_distribution.png', width = 32, height = 32, units = 'in', res=300)
        plot(ggplot(boxplot_data %>% filter(!is.na(values)), aes(x=Dataset, y=values, fill=Dataset)) + 
               geom_boxplot() +
               facet_wrap(.~Variable, scales = 'free_y', ncol = 5) +
               theme(legend.text = element_text(size = 28),
                     legend.title = element_text(size = 28),
                     legend.key = element_rect(fill = "white"),
                     legend.key.size = unit(2.5, "cm"),
                     axis.title.x=element_blank(),
                     axis.text.x=element_blank(),
                     axis.ticks.x=element_blank(),
                     axis.title.y=element_blank(),
                     axis.text.y=element_text(size = 20),
                     plot.title = element_text(size = 35, margin=margin(15,0,30,0)),
                     strip.text.x = element_text(size = 28, face = 'bold'),
                     strip.background = element_rect(color = "black", size = 1)) +
               ggtitle('ORBIS vs CRIF variable distribution'))
        dev.off()
      }
      suppressWarnings(rm(boxplot_data, summary_nan_inf))
      write.table(ORBIS_label, './Distance_to_Default/Stats/01b_ORBIS_peers_quantile_median.csv', sep = ';', row.names = F, append = F)
    }

    # df_peers wide to long
    ORBIS_long_coding = data.frame(Year_Variable = df_peers %>% select_if(is.numeric) %>% colnames(), stringsAsFactors = F) %>%
      mutate(Single_Variable = gsub('_2011|_2012|_2013|_2014', '', Year_Variable))
    char_var = df_peers %>% select_if(is.character) %>% colnames()
    df_peers_long = df_peers %>%
      setDT() %>%
      melt(id.vars = char_var,
           measure.vars = ORBIS_long_coding$Year_Variable,
           variable.name = "Year_Variable") %>%
      left_join(ORBIS_long_coding, by = "Year_Variable") %>%
      rowwise() %>%
      mutate(year = as.numeric(gsub(paste0(Single_Variable, '_'), '', Year_Variable))) %>%
      select(-Year_Variable) %>%
      setDT() %>%
      dcast(as.formula(paste0(paste0(c(char_var, 'year'), collapse = '+'), '~ Single_Variable')), value.var = "value")
    if ((uniqueN(ORBIS_long_coding$Single_Variable)+length(char_var)+1 != ncol(df_peers_long)) |
        (nrow(df_peers) * uniqueN(df_peers_long$year) != nrow(df_peers_long))){cat('\n\n ###### error in df_peers_long')}
  }  

  # create input data for embedding (standardize)
  {
    if (reload_embedding_input == FALSE){
      
      # create embedding for CRIF dataset and peers - standardize
      df_emb_input = df_final_small %>%
        select(starts_with('BILA_')) %>%
        mutate_all(~scale(., center=T, scale=T))
      scaling_df_emb_input = c()
      for (var in colnames(df_emb_input)){
        tt = df_emb_input %>% pull(all_of(var)) %>% attributes()
        scaling_df_emb_input = scaling_df_emb_input %>%
          bind_rows(data.frame(variable = var, center = tt$`scaled:center`, scale = tt$`scaled:scale`,
                               min = min(df_final_small %>% pull(var)), max = max(df_final_small %>% pull(var)), stringsAsFactors = F))
        df_emb_input = df_emb_input %>%
          mutate_at(vars(all_of(var)), function(x) { attributes(x) <- NULL; x })
        rm(tt)
      }
      
      # evaluate how many peers have outliers, cap extreme values within 1.75*min and 1.75*max of corresponding df_emb_input variable
      # and standardize with df_emb_input parameters
      
      trim_ind = data.frame(matrix(FALSE, nrow = nrow(df_peers_long), ncol = ncol(df_emb_input))) %>% setNames(colnames(df_emb_input))
      df_emb_input_peers = c()
      for (var in colnames(df_emb_input)){
        norm_param = scaling_df_emb_input %>% filter(variable == var)
        tt = df_peers_long %>% select(all_of(ORBIS_mapping %>% filter(BILA == var) %>% pull(Single_Variable) %>% unique)) %>% `colnames<-`(var)
        trim_min_ind = which(tt < (norm_param$min - 0.75*abs(norm_param$min)))
        trim_max_ind = which(tt > (norm_param$max + 0.75*abs(norm_param$max)))
        tt[trim_min_ind] = norm_param$min
        tt[trim_max_ind] = norm_param$max
        trim_ind[c(trim_min_ind, trim_max_ind), var] = TRUE
        df_emb_input_peers = df_emb_input_peers %>% bind_cols((tt - norm_param$center) / norm_param$scale)
        rm(norm_param)
      }
      
      check_outliers_df = df_peers_long %>%
        select(Company_name_Latin_alphabet, year) %>%
        bind_cols(
          trim_ind %>%
            mutate(total_capped = rowSums(.),
                   capped_variables = apply(., 1, function(x) paste0(colnames(df_emb_input)[which(x != 0)], collapse = ' | '))) %>%
            select(total_capped, capped_variables)) %>%
        filter(total_capped != 0) %>%
        group_by(Company_name_Latin_alphabet) %>%
        summarize(total_capped = sum(total_capped),
                  capped_variables = strsplit(paste0(capped_variables, collapse = " | "), " \\| ")[[1]] %>% unique() %>% setdiff("") %>% paste0(collapse = " | "),
                  .groups = "drop")
      cat('\n ---', nrow(check_outliers_df), '/', nrow(df_peers), 'peers have been trimmed for outliers when standardizing the input for embedding for a maximum of',
          max(check_outliers_df$total_capped), 'values for each peer')
      write.table(check_outliers_df, './Distance_to_Default/Stats/02a0_Peers_quantile_trimming.csv', sep = ';', row.names = F, append = F)
      
      cat('\n--- range of df_emb_input:', range(df_emb_input, na.rm = T), '    missing values:', sum(is.na(df_emb_input)))
      cat('\n--- range of df_emb_input_peers:', range(df_emb_input_peers, na.rm = T), '    missing values:', sum(is.na(df_emb_input_peers)), '\n\n')
      
      # shape embedding input with single abi+ndg per line and save headers
      {
        df_emb_input = df_emb_input %>%
          as.data.frame() %>%
          `rownames<-`(paste0('row_', 1:nrow(df_emb_input)))
        
        df_emb_input_header = df_final %>%
          select(abi, ndg, year) %>%
          mutate(abi_ndg = paste0(abi, '_', ndg),
                 row_names = paste0("row_", 1:n())) %>%
          left_join(df_final %>%
                      group_by(abi, ndg) %>%
                      summarise(Avail_years = n(), .groups = "drop"), by = c("abi", "ndg"))
        
        df_emb_input_peers = df_emb_input_peers %>%
          as.data.frame() %>%
          `rownames<-`(paste0('row_', 1:nrow(df_emb_input_peers)))
        
        df_emb_input_peers_header = df_peers_long %>%
          select(Company_name_Latin_alphabet, European_VAT_number, year) %>%
          mutate(row_names = paste0("row_", 1:n())) %>%
          left_join(df_peers_long %>%
                      group_by(Company_name_Latin_alphabet, European_VAT_number) %>%
                      summarise(Avail_years = n(), .groups = "drop"), by = c("Company_name_Latin_alphabet", "European_VAT_number"))
        
        df_emb_input_wide = df_emb_input_header %>%
          bind_cols(df_emb_input) %>%
          setDT() %>%
          dcast(abi + ndg + abi_ndg + Avail_years ~ year, value.var = colnames(df_emb_input), sep = "___", fill = NA) %>%
          as.data.frame() %>%
          mutate(row_names = paste0("row_", 1:n()))
        
        col_order = c()
        for (yr in unique(df_emb_input_header$year) %>% sort()){
          for (col in colnames(df_emb_input)){
            df_emb_input_wide = df_emb_input_wide %>%
              rename(!!sym(paste0(yr, "_", col)) := !!sym(paste0(col, "___", yr)))
            col_order = c(col_order, paste0(yr, "_", col))
          }
        }
        df_emb_input_wide = df_emb_input_wide %>%
          select(abi, ndg, abi_ndg, row_names, Avail_years, all_of(col_order))
        df_emb_input_wide_header = df_emb_input_wide %>% select(abi, ndg, abi_ndg, row_names, Avail_years)
        df_emb_input_wide = df_emb_input_wide %>% select(-abi, -ndg, -abi_ndg, -Avail_years) %>%
          column_to_rownames(var = "row_names")
        
        df_emb_input_peers_wide = df_emb_input_peers_header %>%
          bind_cols(df_emb_input_peers) %>%
          setDT() %>%
          dcast(Company_name_Latin_alphabet + European_VAT_number + Avail_years ~ year, value.var = colnames(df_emb_input), sep = "___", fill = NA) %>%
          as.data.frame() %>%
          mutate(row_names = paste0("row_", 1:n()))
        
        col_order = c()
        for (yr in unique(df_emb_input_peers_header$year) %>% sort()){
          for (col in colnames(df_emb_input_peers)){
            df_emb_input_peers_wide = df_emb_input_peers_wide %>%
              rename(!!sym(paste0(yr, "_", col)) := !!sym(paste0(col, "___", yr)))
            col_order = c(col_order, paste0(yr, "_", col))
          }
        }
        df_emb_input_peers_wide = df_emb_input_peers_wide %>%
          select(Company_name_Latin_alphabet, European_VAT_number, row_names, Avail_years, all_of(col_order))
        df_emb_input_peers_wide_header = df_emb_input_peers_wide %>% select(Company_name_Latin_alphabet, European_VAT_number, row_names, Avail_years)
        df_emb_input_peers_wide = df_emb_input_peers_wide %>% select(-Company_name_Latin_alphabet, -European_VAT_number, -Avail_years) %>%
          column_to_rownames(var = "row_names")
        
      }
      
      list_emb_input = list(df_emb_input = df_emb_input,
                            df_emb_input_header = df_emb_input_header,
                            df_emb_input_peers = df_emb_input_peers,
                            df_emb_input_peers_header = df_emb_input_peers_header,
                            df_emb_input_wide = df_emb_input_wide,
                            df_emb_input_wide_header = df_emb_input_wide_header,
                            df_emb_input_peers_wide = df_emb_input_peers_wide,
                            df_emb_input_peers_wide_header = df_emb_input_peers_wide_header)
      saveRDS(list_emb_input, './Distance_to_Default/Checkpoints/list_emb_input.rds')
      rm(trim_ind, tt, scaling_df_emb_input, check_outliers_df)
    } else {
      list_emb_input = readRDS('./Distance_to_Default/Checkpoints/list_emb_input.rds')
    }
  }
  
  # run PCA and Autoencoder to evaluate embeddings
  {
    ### PCA
    
    if (reload_PCA == FALSE){
      cat('\n  ==================  PCA  ==================\n')
      PCA_emb = fit_pca(df_emb_input)
      PCA_emb_wide = fit_pca(df_emb_input_wide, NA_masking = 0)    # variables are standardized so the mean is zero, used as masking
      saveRDS(PCA_emb, './Distance_to_Default/Checkpoints/PCA_emb.rds')
      saveRDS(PCA_emb_wide, './Distance_to_Default/Checkpoints/PCA_emb_wide.rds')
    } else {
      PCA_emb = readRDS('./Distance_to_Default/Checkpoints/PCA_emb.rds')
      PCA_emb_wide = readRDS('./Distance_to_Default/Checkpoints/PCA_emb_wide.rds')
    }
    
    ### Autoencoder

    # test different activation function and architectures
    {
      if (tune_autoencoder_flag){
        tune_autoencoder = autoencoder_tuning(dataset = df_emb_input, NA_masking = 0, masking_value = 0, save_RDS_additional_lab = 'AE_emb',
                                              batch_size = 500, epochs = 300, max_iter = 80, design_iter = 15)
        
        tune_autoencoder_wide = autoencoder_tuning(dataset = df_emb_input_wide, NA_masking = 0, masking_value = 0, save_RDS_additional_lab = 'AE_emb_wide',
                                                   batch_size = 500, epochs = 300, max_iter = 80, design_iter = 15)
        
        x_input = create_lstm_input(df = df_emb_input, df_header = df_emb_input_header, ID_col = 'abi_ndg', TIME_col = 'year', NA_masking = 0)
        tune_autoencoderLSTM = autoencoderLSTM_tuning(x_input, masking_value = 0, max_lstm_neurons = 100, save_RDS_additional_lab = 'AE_LSTM_emb', epochs = 250,
                                                      temporal_embedding = FALSE, timestep_label = c(), max_iter = 60, design_iter = 15)
      }
    }

    # fit/reload final embedding
    if (reload_autoencoder == FALSE){
      cat('\n  ==================  Autoencoder  ==================\n')
      aut_emb = Autoencoder_PC(df_emb_input, n_comp = 10, epochs = 500, batch_size = 500, layer_list = c(20, 16, 14, 12),
                     act_fun = 'tanh', latent_act_fun = 'relu', verbose = 0, save_RDS = F,
                     save_model = T, save_model_name = './Distance_to_Default/Checkpoints/AE_emb')
      saveRDS(aut_emb, './Distance_to_Default/Checkpoints/00_aut_emb.rds')

      aut_emb_wide = Autoencoder_PC(df_emb_input_wide, n_comp = 32, epochs = 500, batch_size = 500, layer_list = c(58, 50, 43, 37),
                     act_fun = 'tanh', latent_act_fun = 'relu', NA_masking = 0, masking_value = 0, verbose = 0,
                     save_RDS = F, save_model = T, save_model_name = './Distance_to_Default/Checkpoints/AE_emb_wide')
      saveRDS(aut_emb_wide, './Distance_to_Default/Checkpoints/00_aut_emb_wide.rds')
        
      x_input = create_lstm_input(df = df_emb_input, df_header = df_emb_input_header, ID_col = 'abi_ndg', TIME_col = 'year', NA_masking = 0)
      aut_emb_LSTM = AutoencoderLSTM_PC(x_input, n_comp = 10, epochs = 500, batch_size = 100, layer_list = c(55, 31, 17), kernel_reg_alpha = NULL,
                                    RNN_type = 'gru', temporal_embedding = FALSE, timestep_label = c(), masking_value = 0, verbose = 0,
                                    save_RDS = F, save_model = T, save_model_name = './Distance_to_Default/Checkpoints/AE_LSTM_emb')
      saveRDS(aut_emb_LSTM, './Distance_to_Default/Checkpoints/00_aut_emb_LSTM.rds')
    } else {
      aut_emb = readRDS('./Distance_to_Default/Checkpoints/00_aut_emb.rds')
      aut_emb_wide = readRDS('./Distance_to_Default/Checkpoints/00_aut_emb_wide.rds')
      aut_emb_LSTM = readRDS('./Distance_to_Default/Checkpoints/00_aut_emb_LSTM.rds')
    }
    
    # evaluate embedding quality
    {
      # evaluate distance of original data used for clustering performance
      # ref_dataset = df_final_small %>%
      #   select(starts_with('BILA_')) %>%
      #   as.matrix()    # todo: se cambi l'input degli embedding, cambia anche questo!
      # 
      #   ref_dataset_dist = parDist(ref_dataset, method = "mahalanobis")
      #   embedding_dist_mat = df_final_embedding %>%
      #     filter(Method == 'PCA') %>%
      #     select(Dim1, Dim2) %>% as.matrix() %>% parDist() %>% as.matrix()
      # 
      #   eval_R2_embedding(ref_dataset_dist, embedding_dist_mat)
      
      
      # https://rdrr.io/github/csoneson/dreval/src/R/trustworthiness.R
      # https://rdrr.io/github/csoneson/dreval/man/dreval.html
      # https://github.com/csoneson/dreval
      # https://bioconductor.org/packages/release/bioc/vignettes/SingleCellExperiment/inst/doc/intro.html#3_Adding_low-dimensional_representations
      
      # df = iris %>% select(-Species)
      # 
      # pca_data1 <- prcomp(df, rank = 3)$x
      # pca_data2 <- prcomp(df, rank = 3)$x
      # 
      # 
      # dist_df = dist(df)
      # dist_pca1 = dist(pca_data1)
      # dist_pca2 = dist(pca_data2)
      # 
      # calcTrustworthinessFromDist(dist_df, dist_pca1, kTM = 2)
      # calcTrustworthinessFromDist(dist_df, dist_pca2, kTM = 2)
      # 
      # 
      # d1 = parDist(list_emb_input$df_emb_input %>% as.matrix(), method = "mahalanobis")
      # d2 = parDist(list_embedding$PCA$emb %>% select(starts_with("V")) %>% as.matrix(), method = "mahalanobis")
      # calcTrustworthinessFromDist(d1, d2, kTM = 2)
      # 
      # library(SingleCellExperiment)
      # 
      # 
      # sce <- SingleCellExperiment::SingleCellExperiment(list(pp = t(list_emb_input$df_emb_input)))
      # SingleCellExperiment::reducedDims(sce) <- list(PCA=list_embedding$PCA$emb, AE=list_embedding$AE$emb)
      # 
      # dre <- dreval(sce, kTM = 2, refAssay = 'pp', distNorm = "euclidean", refType = "assay", nSamples=NULL)
      # vv = dre$scores
      
    }
    
    # summary table
    summary_table = PCA_emb$report %>% mutate(Dataset = 'abi_ndg+year (long)', Method = 'PCA', Obs = nrow(list_emb_input$df_emb_input),
                                              Embedding_Range = PCA_emb$Embedding_Range) %>%
      bind_rows(PCA_emb_wide$report %>% mutate(Dataset = 'abi_ndg (wide)', Method = 'PCA', Obs = nrow(list_emb_input$df_emb_input_wide),
                                               Embedding_Range = PCA_emb_wide$Embedding_Range),
                aut_emb$report %>% mutate(Dataset = 'abi_ndg+year (long)', Method = 'AE', Obs = nrow(list_emb_input$df_emb_input),
                                          Embedding_Range = aut_emb$Embedding_Range),
                aut_emb_wide$report %>% mutate(Dataset = 'abi_ndg (wide)', Method = 'AE', Obs = nrow(list_emb_input$df_emb_input_wide),
                                               Embedding_Range = aut_emb_wide$Embedding_Range),
                aut_emb_LSTM$report %>% mutate(Dataset = 'abi_ndg (wide)', Method = 'AE LSTM', Obs = nrow(list_emb_input$df_emb_input_wide),
                                               Embedding_Range = aut_emb_LSTM$Embedding_Range)) %>%
      select(Dataset, Method, Obs, everything()) %>%
      arrange(desc(Dataset), desc(Method)) %>%
      replace(is.na(.), "")
    print(summary_table)
    write.table(summary_table, './Distance_to_Default/Stats/02a1_Embedding_summary.csv', sep = ';', row.names = F, append = F)

    # load embedding and evaluate embedding on peers dataset
    {
      if (reload_peers_embedding == FALSE){
        list_embedding = list()
        embedding_range = c()
        for (emb_type in c("PCA", "PCA_wide", "AE", "AE_wide", "AE_LSTM_wide")){
          
          if (emb_type %in% c("PCA_wide", "AE_wide")){
            emb_header = df_emb_input_wide_header
            emb_input_peers = df_emb_input_peers_wide %>% select(all_of(colnames(df_emb_input_wide)))
            emb_peers_header = df_emb_input_peers_wide_header
          } else {
            emb_header = df_emb_input_header
            emb_peers_header = df_emb_input_peers_header %>% filter(year != "2011")
            emb_input_peers = emb_peers_header %>% left_join(
              df_emb_input_peers %>% select(all_of(colnames(df_emb_input))) %>% rownames_to_column("row_names"), by = "row_names") %>%
              column_to_rownames("row_names") %>%
              select(-all_of(setdiff(colnames(emb_peers_header), "row_names")))
          }
          if (emb_type == "AE_LSTM_wide"){     # input must include timesteps for each observation, output has one row for each observation
            emb_header = df_emb_input_wide_header
            emb_peers_header_create_input = emb_peers_header
            emb_peers_header = df_emb_input_peers_wide_header
          }
          
          if (emb_type %in% c("PCA", "PCA_wide")){
            if (emb_type == "PCA"){
              emb_file = PCA_emb
            } else if (emb_type == "PCA_wide"){
              emb_file = PCA_emb_wide
            }
            
            # predict peers embedding
            emb_peers = as.matrix(emb_input_peers) %*% emb_file$loading_transform
          }
          
          if (emb_type %in% c("AE", "AE_wide", "AE_LSTM_wide")){
            if (emb_type == "AE"){
              emb_file = aut_emb
              emb_model = keras::load_model_hdf5("./Distance_to_Default/Checkpoints/AE_emb.h5")
            } else if (emb_type == "AE_wide"){
              emb_file = aut_emb_wide
              emb_model = keras::load_model_hdf5("./Distance_to_Default/Checkpoints/AE_emb_wide.h5")
            } else if (emb_type == "AE_LSTM_wide"){
              emb_file = aut_emb_LSTM
            }
            
            # predict peers embedding
            if (emb_type == "AE_LSTM_wide"){
              emb_peers = predict_AutoencoderLSTM(aut_emb_LSTM, "./Distance_to_Default/Checkpoints/AE_LSTM_emb_weights.h5",
                                                  create_lstm_input(df = emb_input_peers, df_header = emb_peers_header_create_input,
                                                                    ID_col = 'European_VAT_number', TIME_col = 'year', NA_masking = 0))
            } else {
              emb_peers = emb_model(emb_input_peers %>% as.matrix()) %>% as.matrix() %>% as.data.frame() %>% `rownames<-`(rownames(emb_input_peers))
            }
          }
          
          emb = emb_file$Embedding
          
          embedding_range = embedding_range %>%
            bind_rows(data.frame(Embedding = emb_type, Data = 'CRIF', t(range(emb)), stringsAsFactors = F),
                      data.frame(Embedding = emb_type, Data = 'PEERS', t(range(emb_peers)), stringsAsFactors = F))
          
          join_col_emb = ifelse(emb_type == "AE_LSTM_wide", "abi_ndg", "row_names")
          join_col_emb_peers = ifelse(emb_type == "AE_LSTM_wide", "European_VAT_number", "row_names")
          
          list_embedding[[emb_type]] = list(emb = emb_header %>%
                                              left_join(as.data.frame(emb) %>%
                                                          rownames_to_column(join_col_emb) %>%
                                                          setNames(c(join_col_emb, paste0("V", c(1:(ncol(.)-1))))), by = join_col_emb),
                                            emb_peers = emb_peers_header %>%
                                              left_join(as.data.frame(emb_peers) %>% rownames_to_column(join_col_emb_peers) %>%
                                                          setNames(c(join_col_emb_peers, paste0("V", c(1:(ncol(.)-1))))), by = join_col_emb_peers),
                                            report = emb_file$report)
          
        } # emb_type
        
        saveRDS(list_embedding, './Distance_to_Default/Checkpoints/list_embedding_pre_UMAP.rds')
        
        png('./Distance_to_Default/Stats/02a1_Embedding_Range.png', width = 10, height = 10, units = 'in', res=300)
        plot(
          ggplot(data = embedding_range %>% mutate(label = paste0(Embedding, " - ", Data)))+
            geom_segment(aes(x = label, xend = label, y = X1, yend = X2, color = Embedding), size = 10, alpha = 0.6) +
            coord_flip() +
            ggtitle("Comparison of Embeddings range") +
            ylab("Embedding Range") +
            xlab("Embedding") +
            theme(legend.position = "none",
                  axis.text=element_text(size = 17),
                  axis.title=element_text(size = 25),
                  plot.title=element_text(size = 25))
        )
        dev.off()
        
        # check for list_embedding
        check_list_embedding = c()
        for (nn in names(list_embedding)){
          vv = list_embedding[[nn]]
          check_list_embedding = check_list_embedding %>%
            bind_rows(data.frame(Embedding = nn, NA_emb = sum(is.na(vv$emb)), NA_emb_peers = sum(is.na(vv$emb_peers)),
                                 Emb_columns = vv$emb %>% select(starts_with("V")) %>% ncol(),
                                 Emb_peers_columns = vv$emb_peers %>% select(starts_with("V")) %>% ncol(),
                                 Emb_rows = nrow(vv$emb), Emb_peers_rows = nrow(vv$emb_peers), stringsAsFactors = F))
        }
        if (max(check_list_embedding %>% select(NA_emb, NA_emb_peers) %>% unlist()) > 0){cat('\n\n######## warning: missing in some list_embedding')}
        if (sum(check_list_embedding$Emb_columns != check_list_embedding$Emb_peers_columns) > 0){cat('\n\n######## warning: embeddding/peers columns don\'t match in some list_embedding')}
        
        rm(emb, emb_peers, emb_file, emb_header, emb_input_peers, emb_peers_header, emb_model, embedding_range, check_list_embedding, vv)
      } else {
        list_embedding = readRDS('./Distance_to_Default/Checkpoints/list_embedding_pre_UMAP.rds')
      }
    }
    rm(summary_table, aut_emb, aut_emb_LSTM, aut_emb_wide, PCA_emb, PCA_emb_wide)
  }
  
  # visualize embedding
  {
    # evaluate UMAP to get 3 dimension to be plotted
    {
      n_components = 3
      min_dist_set = c(0, 0.01, 0.05, 0.1, 0.5, 0.99)
      n_neighbors_set = c(5, 15, 30, 50, 100, 500)
      metric = "euclidean"
      init = "agspectral"
      n_epochs = 400
      eval_UMAP = T
      eval_densMAP = F
      # reticulate::use_condaenv("denseMAP", required = TRUE)     # activate conda env for densMAP
      # dm <- reticulate::import('densmap')
      
      if (reload_embedding_visualization == FALSE){
        list_emb_visual = list()
        for (emb_type in names(list_embedding)){
          
          cat('\nFitting', emb_type, end = '...')
          start_time = Sys.time()
          
          input_df = list_embedding[[emb_type]][['emb']] %>% select(starts_with("V"))
          input_df_header = list_embedding[[emb_type]][['emb']] %>% select(-starts_with("V"))
          predict_input = list_embedding[[emb_type]][['emb_peers']] %>% select(starts_with("V"))
          predict_input_header = list_embedding[[emb_type]][['emb_peers']] %>% select(-starts_with("V"))
          
          list_emb_visual[[emb_type]] = evaluate_UMAP(input_df, n_components = n_components, min_dist_set = min_dist_set, n_neighbors_set = n_neighbors_set,
                                                      metric = metric, init = init, n_epochs = n_epochs, predict_input = predict_input,
                                                      eval_UMAP = eval_UMAP, eval_densMAP = eval_densMAP)

          # add header
          for (map_type in names(list_emb_visual[[emb_type]])){
            for (comb in names(list_emb_visual[[emb_type]][[map_type]])){
              list_emb_visual[[emb_type]][[map_type]][[comb]][["emb_visual"]] =
                input_df_header %>% bind_cols(list_emb_visual[[emb_type]][[map_type]][[comb]][["emb_visual"]] %>% as.data.frame())
              list_emb_visual[[emb_type]][[map_type]][[comb]][["emb_predict_visual"]] =
                predict_input_header %>% bind_cols(list_emb_visual[[emb_type]][[map_type]][[comb]][["emb_predict_visual"]] %>% as.data.frame())
            } # comb
          } # map_type
          
          saveRDS(list_emb_visual, './Distance_to_Default/Checkpoints/list_emb_visual.rds')
          tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
          cat(' Done in', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))), ' ', as.character(Sys.time()))
        }
      } else {
        list_emb_visual = readRDS('./Distance_to_Default/Checkpoints/list_emb_visual.rds')
      }
    }
    
    # evaluate aggregated visualization for better visualization
    {
      n_cell = 30   # number of cells (square or cube side)
      
      if (reload_embedding_aggregated_visualization == FALSE){
        list_emb_visual_aggreg = list()
        for (emb_type in names(list_emb_visual)){
          tt = list_emb_visual[[emb_type]]
          
          for (map_type in names(tt)){
            tt1 = tt[[map_type]]
            
            for (comb in names(tt1)){
              agg_data = aggregate_points(plot_data = tt1[[comb]][['emb_visual']] %>%
                                            select(starts_with("V")) %>%
                                            setNames(c("Dim1", "Dim2", "Dim3")) %>%
                                            mutate(Label = "A"),
                                          label_values = "A", n_cell = n_cell, err_lab = paste0(c(emb_type, map_type, comb), collapse = " - "))
              list_emb_visual_aggreg[[emb_type]][[map_type]][[comb]] = list(emb_visual = agg_data$cell_summary,
                                                                            emb_predict_visual = tt1[[comb]][['emb_predict_visual']])
            } # comb
          } # map_type
        } # emb_type
        rm(tt, tt1, agg_data)
        saveRDS(list_emb_visual_aggreg, './Distance_to_Default/Checkpoints/list_emb_visual_aggreg.rds')
      } else {
        list_emb_visual_aggreg = readRDS('./Distance_to_Default/Checkpoints/list_emb_visual_aggreg.rds')
      }
    }
    
    # create an html report of fitted UMAP to choose which combination of n_neighbors and min_dist to keep
    #                      one report for All vs Aggregated points - all combination of map_type+emb_type within each file
    {
      if (run_embedding_report){
        help_path = 'C:/Users/Alessandro Bitetto/Downloads/UniPV/BCC-default/Help.R'
        HTML_fig.height = 10
        HTML_fig.width = 10
        n_col = round(sqrt(list_emb_visual[[1]][[1]] %>% length()))  # sqrt of n_neighbors * min_dist
        show = "point"
        single_class_size = 5
        point_alpha = 1
        MAX_SCALE = 5
        
        avail_maps = names(list_emb_visual[[1]])    # list_emb_visual and list_emb_visual_aggreg have the same list structure
        avail_embs = intersect(c("PCA", "AE", "PCA_wide", "AE_wide", "AE_LSTM_wide"), names(list_emb_visual))    # sort embedding
        for (point_set in c("All", "Aggregated")){
          
          if (point_set == "All"){
            visual_list = list_emb_visual
          } else {
            visual_list = list_emb_visual_aggreg
          }
          
          # set-up Rmarkdown file
          Rmd_text = RMarkdown_settings(title = paste0("Visualization of 3D embedding with ", point_set, " points"), help_path)
          
          
          rds_list = c()
          for (map_type in avail_maps){
            
            # include section headers
            Rmd_text = paste0(Rmd_text, "\n\n\n# ", map_type, "\n")
            
            for (emb_type in avail_embs){
              
              rds_lab = paste0(getwd(), "/", map_type, "_", emb_type, ".rds")
              
              # include 3D grid plot
              Rmd_text = paste0(Rmd_text, "\n",
                                RMarkdow_render_3d_grid(plot_list_rds = rds_lab, emb_type, n_col, show = show, single_class_size = single_class_size,
                                                        point_alpha = point_alpha, MAX_SCALE = MAX_SCALE,
                                                        HTML_fig.height = HTML_fig.height, HTML_fig.width = HTML_fig.height))
              
              plot_list = list()
              for (dd_name in names(visual_list[[emb_type]][[map_type]])){
                dd = visual_list[[emb_type]][[map_type]][[dd_name]]
                dd_title = gsub("n_neig_", "n_neighbors=", dd_name)
                dd_title = gsub("min_dist_", "min_dist=", dd_title)
                dd_title = gsub("_m", " - m", dd_title)
                
                if (point_set == "All"){
                  data = dd$emb_visual %>%
                    select(starts_with("V")) %>%
                    setNames(paste0("Dim", 1:ncol(.))) %>%
                    mutate(size = 1,
                           Label = factor("A"))
                } else {
                  data = dd$emb_visual
                }
                
                plot_list = c(plot_list,
                              list(list(data = data,
                                        title = dd_title))
                )
              } # dd_name
              saveRDS(plot_list, rds_lab)
              rds_list = c(rds_list, rds_lab)
              
            } # emb_type
          } # maps
          rm(dd, data, plot_list, visual_list)
          
          fileConn<-file(paste0("./Distance_to_Default/Stats/99_Embedding_selection_", point_set, ".Rmd"))
          writeLines(strsplit(Rmd_text, "\\n")[[1]], fileConn)
          close(fileConn)
          rmarkdown::render(paste0(getwd(), "/Distance_to_Default/Stats/99_Embedding_selection_", point_set, ".Rmd"),
                            output_format = "html_document", output_file = paste0("99_Embedding_selection_", point_set, ".html"))
          
          for (rr in rds_list){file.remove(rr)}
        } # point_set
      }
    }

    # select best combination of n_neighbors and min_dist to keep and create report with peers comparison
    {
      best_map = "UMAP"
      best_comb_visual = list(UMAP = list(PCA = list(c(n_neighbors = 5, min_dist = 0.1),
                                                     c(n_neighbors = 100, min_dist = 0.01)),
                                          AE = list(c(n_neighbors = 5, min_dist = 0.1),
                                                    c(n_neighbors = 100, min_dist = 0.01)),
                                          PCA_wide = list(c(n_neighbors = 5, min_dist = 0.1),
                                                          c(n_neighbors = 100, min_dist = 0.01)),
                                          AE_wide = list(c(n_neighbors = 5, min_dist = 0.1),
                                                         c(n_neighbors = 100, min_dist = 0.01)),
                                          AE_LSTM_wide = list(c(n_neighbors = 5, min_dist = 0.1),
                                                              c(n_neighbors = 100, min_dist = 0.01))
      )
      )
      
      if (run_embedding_best_report){
        help_path = 'C:/Users/Alessandro Bitetto/Downloads/UniPV/BCC-default/Help.R'
        HTML_fig.height = 9
        HTML_fig.width = 9
        n_col = round(sqrt(list_emb_visual[[1]][[1]] %>% length()))  # sqrt of n_neighbors * min_dist
        show = "point"
        show_additional = "sphere"
        single_class_size = 2
        point_alpha = 0.2
        additional_points_size = 1
        title_cex = 1
        MAX_SCALE = 15
        slider_show = "sphere"
        slider_point_size = 1.2
        slider_text_hor_just = 0
        slider_text_ver_just = 1.2
        slider_text_cex = 1
        slider_height = 300
        slider_zoom_val = 1
        slider_HTML_fig.height = 7
        slider_HTML_fig.width = 5
        
        avail_embs = intersect(c("PCA", "AE", "PCA_wide", "AE_wide", "AE_LSTM_wide"), names(list_emb_visual))    # sort embedding
        
        
        # set-up Rmarkdown file
        Rmd_text = RMarkdown_settings(title = paste0("Visualization of 3D embedding with Peers"), help_path)
        
        # include section headers
        Rmd_text = paste0(Rmd_text, "\n\n\n# ", best_map, "\n")
        
        rds_list = c()
        for (emb_type in avail_embs){
          
          avail_comb = best_comb_visual[[best_map]][[emb_type]] %>%
            lapply(., function(x) paste0("n_neig_", x["n_neighbors"], "_min_dist_", x["min_dist"])) %>% unlist()
          rds_lab = paste0(getwd(), "/", best_map, "_", emb_type, ".rds")
          rds_lab_slider = paste0(getwd(), "/", best_map, "_", emb_type, "_slider.rds")
          
          # add section name
          Rmd_text = paste0(Rmd_text, "\n\n## Embedding: ", emb_type, "\n\n")
          
          # start multi-column markdown
          Rmd_text = paste0(Rmd_text, ":::: {style=\"display: flex;\"}\n\n::: {}")
          
          # include 3D grid plot
          Rmd_text = paste0(Rmd_text, "\n",
                            RMarkdow_render_3d_grid(plot_list_rds = rds_lab, emb_type, n_col = length(avail_comb), show = show, 
                                                    show_additional = show_additional, title_cex = title_cex, additional_points_size = additional_points_size,
                                                    point_alpha = point_alpha, single_class_size = single_class_size, MAX_SCALE = MAX_SCALE,
                                                    HTML_fig.height = HTML_fig.height, HTML_fig.width = HTML_fig.width, add_section = FALSE))
          
          # multi-column break
          Rmd_text = paste0(Rmd_text, ":::\n\n::: {}")
          
          # include 3D slider plot with peers only
          Rmd_text = paste0(Rmd_text, "\n<p style=\"font-size:20pt; font-style:bold\"> Peers breakdown </p>\n",
                            RMarkdow_render_3d_slider(data_rds = rds_lab_slider, emb_type, show = slider_show, point_size = slider_point_size,
                                                      text_hor_just = slider_text_ver_just, text_ver_just = slider_text_ver_just,
                                                      text_cex = slider_text_cex, widget_height = slider_height, zoom_val = slider_zoom_val,
                                                      HTML_fig.height = slider_HTML_fig.height, HTML_fig.width = slider_HTML_fig.width, add_section = FALSE))
          
          # close multi-column
          Rmd_text = paste0(Rmd_text, ":::\n\n::::")
          
          # create input for grid plot
          plot_list = list()
          for (dd_name in avail_comb){
            for (point_set in c("All", "Aggregated")){
              
              if (point_set == "All"){
                visual_list = list_emb_visual
              } else {
                visual_list = list_emb_visual_aggreg
              }
              
              dd = visual_list[[emb_type]][[best_map]][[dd_name]]
              dd_title = gsub("n_neig_", "n_neighbors=", dd_name)
              dd_title = gsub("min_dist_", "min_dist=", dd_title)
              dd_title = gsub("_m", " - m", dd_title)
              dd_title = paste0(dd_title, "\n", point_set, " points")
              
              if (point_set == "All"){
                data = dd$emb_visual %>%
                  select(starts_with("V")) %>%
                  setNames(paste0("Dim", 1:ncol(.))) %>%
                  mutate(size = 1,
                         Label = factor("CRIF"))
                p_alpha = NULL
              } else {
                data = dd$emb_visual
                p_alpha = 1
              }
              
              additional_data = dd$emb_predict_visual %>%
                select(starts_with("V")) %>%
                setNames(paste0("Dim", 1:ncol(.))) %>%
                mutate(size = 1,
                       Label = factor("Peers"))
              
              plot_list = c(plot_list,
                            list(list(data = data,
                                      title = dd_title,
                                      point_alpha = p_alpha,
                                      additional_data = additional_data,
                                      additional_color = "black"))
              )
            } # point_set
          } # dd_name
          saveRDS(plot_list, rds_lab)
          
          # create input for slider plot
          slider_data = dd$emb_predict_visual %>%
            rename(Label = Company_name_Latin_alphabet) %>%
            mutate(Label = factor(Label)) %>%
            setNames(gsub('^[V]', "Dim", names(.)))
          if (emb_type %in% c("PCA", "AE")){
            slider_data = slider_data %>%
              rename(text_annotation = year)
          }
          saveRDS(slider_data, rds_lab_slider)
          
          rds_list = c(rds_list, c(rds_lab, rds_lab_slider))
          rm(dd, data, slider_data, additional_data, plot_list, visual_list)
          
        } # emb_type
        
        fileConn<-file(paste0("./Distance_to_Default/Stats/99_Embedding_visualization_and_peers_mapping.Rmd"))
        writeLines(strsplit(Rmd_text, "\\n")[[1]], fileConn)
        close(fileConn)
        rmarkdown::render(paste0(getwd(), "/Distance_to_Default/Stats/99_Embedding_visualization_and_peers_mapping.Rmd"),
                          output_format = "html_document", output_file = paste0("02a1_Embedding_visualization_and_peers_mapping.html"))
        
        for (rr in rds_list){file.remove(rr)}
      }
      rm(best_comb_visual)
    }
  }
}
  
# test different clustering variables to assign peers to each CRIF observation
reload_manual_label = T    # if FALSE evaluate median/percentile clusters 
reload_cluster_performance = T
plot_manual_clustering = F
{
  best_comb_visual = list(UMAP = list(PCA = c(n_neighbors = 5, min_dist = 0.1),
                                      AE = c(n_neighbors = 100, min_dist = 0.01),
                                      PCA_wide = c(n_neighbors = 100, min_dist = 0.01),
                                      AE_wide = c(n_neighbors = 100, min_dist = 0.01),
                                      AE_LSTM_wide = c(n_neighbors = 100, min_dist = 0.01)
  ))
  
  # assign manual label (based on Peers quantile and Industry)
  {
    # manual clustering will be performed using:
    #   - ORBIS_mapped: mapped columns between peers and CRIF data
    #   - categorical_variables: additional categorical variables
    
    ##### for embedding on long format cluster label are assigned by median of the abi+ndg
    ##### for clustering performance and plot data on long format are averaged by abi+ndg
    
    categorical_variables = c('Dummy_industry', 'Dimensione_Impresa', 'Industry', 'segmento_CRIF', 'Regione_Macro')
    n_cell = 30  # cells for aggregated plot
    help_path = 'C:/Users/Alessandro Bitetto/Downloads/UniPV/BCC-default/Help.R'
    HTML_fig.height = 8
    HTML_fig.width = 15
    n_col = 2
    show = "point"
    show_additional = "sphere"
    single_class_size = 4
    point_alpha = 1
    additional_points_size = 1
    title_cex = 2
    MIN_SCALE = 2
    MAX_SCALE = 20
    legend_cex = 2
    legend_resize = c(1000, 500)
    plot_legend_index = c(2)
    
    # evaluate cluster label
    {
      if (reload_manual_label == FALSE){
        average_data_report = c()
        plot_issue = c()
        list_manual_cluster = list()
        vv = 1
        cat('\n--- Manual cluster evaluation:\n')
        for (label_var in c(ORBIS_mapped, categorical_variables)){
          
          cat('Evaluating variable label: ', vv, '/', length(c(ORBIS_mapped, categorical_variables)), end = '\r')
          
          if (label_var %in% categorical_variables){
            label_type = label_var
            label_values = df_final %>% select(all_of(label_var)) %>% unique() %>% pull(label_var) %>%
              lapply(function(x) substr(x,1,15)) %>% unlist()
            cluster_data = df_final %>%
              mutate(abi_ndg = paste0(abi, '_', ndg)) %>%
              mutate(Label = substr(!!sym(label_var), 1, 15)) %>%
              mutate(Label = factor(Label, levels = label_values)) %>%
              select(abi_ndg, Label)
            loops = '_'
          } else {
            loops = c('Median_Single', 'LowMedHig_Single')  # 'Median_Combined', 'LowMedHig_Combined'
          }
          
          for (loop_main in loops){   # if categorical_variables use only label_values, otherwise test median/quantile
            
            loop = strsplit(loop_main, '_')[[1]][1]
            loop_type = strsplit(loop_main, '_')[[1]][2]
            
            # prepare data for Median/LowMedHig
            eval_cluster = T
            if (loop != ''){
              label_type = paste0(gsub('BILA_', '', label_var), '_', loop)
              if (loop == 'Median'){
                cluster_data = df_final %>%
                  mutate(abi_ndg = paste0(abi, '_', ndg)) %>%
                  # select(all_of(c(label_var, 'Dummy_industry'))) %>%
                  # setNames(c('var', 'Dummy_industry')) %>%
                  # left_join(ORBIS_label %>% filter(Variable == label_var) %>% select(Dummy_industry, Peer_median), by = "Dummy_industry") %>%
                  mutate(Variable = label_var) %>%
                  left_join(ORBIS_label %>% filter(Variable == label_var) %>% select(Variable, Peer_median), by = "Variable") %>%
                  mutate(Label = ifelse(!!sym(label_var) <= Peer_median, 'Down', 'Up'))
                # mutate(Label_comb = paste0(substr(Dummy_industry, 1, 4), '_', Label))
              } else if (loop == 'LowMedHig'){
                cluster_data = df_final %>%
                  mutate(abi_ndg = paste0(abi, '_', ndg)) %>%
                  # select(all_of(c(label_var, 'Dummy_industry'))) %>%
                  # setNames(c('var', 'Dummy_industry')) %>%
                  # left_join(ORBIS_label %>% filter(Variable == label_var) %>% select(Dummy_industry, Peer_33rd, Peer_66th), by = "Dummy_industry") %>%
                  mutate(Variable = label_var) %>%
                  left_join(ORBIS_label %>% filter(Variable == label_var) %>% select(Variable, Peer_33rd, Peer_66th), by = "Variable") %>%
                  mutate(Label = 'Low') %>%
                  mutate(Label = ifelse(!!sym(label_var) > Peer_33rd, 'Medium', Label)) %>%
                  mutate(Label = ifelse(!!sym(label_var) > Peer_66th, 'High', Label))
                # mutate(Label_comb = paste0(substr(Dummy_industry, 1, 4), '_', Label))
              }
              
              # if (loop_type == 'Combined'){
              #   cluster_data = cluster_data %>%
              #     mutate(Label = Label_comb)
              #   label_type = paste0('Dummy_industry+', label_type)
              # }
              
              # check for missing values combinations
              label_values = sort(unique(cluster_data$Label))
              cluster_data = cluster_data %>%
                mutate(Label = factor(Label, levels = label_values)) %>%
                select(abi_ndg, Label)
              nn = length(label_values)
              if ((loop == 'Median' & loop_type == 'Single' & nn != 2) |
                  (loop == 'Median' & loop_type == 'Combined' & nn != 4) |
                  (loop == 'LowMedHig' & loop_type == 'Single' & nn != 3) |
                  (loop == 'LowMedHig' & loop_type == 'Combined' & nn != 6)){eval_cluster = F}
            }
            
            # save cluster label
            if (eval_cluster){
              
              # check different label for each abi+ndg and use median for label and average for coordinates
              label_check = NULL
              label_check = cluster_data %>%
                group_by(abi_ndg) %>%
                summarize(Label_count = uniqueN(Label), .groups = "drop") %>%
                filter(Label_count > 1)
              if (nrow(label_check) > 0){
                average_data_report = average_data_report %>%
                  bind_rows(data.frame(Variable = label_var, Problem = loop_main, Multiple_Labels = nrow(label_check), stringsAsFactors = F))
              }
              cluster_data = cluster_data %>%
                mutate(Label = as.numeric(Label)) %>%
                group_by(abi_ndg) %>%
                summarise_all(median, .groups = "drop") %>%
                mutate(Label = label_values[Label]) %>%
                mutate(Label = factor(Label, levels = label_values))
              
              if (sum(is.na(cluster_data)) > 0){cat('\n######### missing values in', label_type)}
              list_manual_cluster[[label_type]] = list(cluster_label = cluster_data,
                                                       label_check = label_check)
            } else {
              plot_issue = plot_issue %>%
                bind_rows(data.frame(Variable = label_var, Problem = loop_main, stringsAsFactors = F))
            }  
          } # loop_main
          vv = vv + 1
        } # label_Var
        plot_issue = plot_issue %>%
          unique() %>%
          group_by(Variable) %>%
          summarize(Problem = paste0(Problem, collapse = ' - '), .groups = 'drop')
        average_data_report = average_data_report %>%
          mutate(Multiple_Labels = paste0(Problem, ' (', Multiple_Labels, ')')) %>%
          group_by(Variable) %>%
          summarize(Median_on_Labels = paste0(Multiple_Labels, collapse = ' - '), .groups = 'drop')
        final_report = ORBIS_label %>%
          left_join(plot_issue, by = "Variable") %>%
          left_join(average_data_report, by = "Variable") %>%
          replace(is.na(.), '')
        write.table(final_report, './Distance_to_Default/Stats/02b_Manual_Clustering_Issues_Report.csv', sep = ';', row.names = F, append = F)
        
        rm(plot_issue, average_data_report, final_report, cluster_data, label_check)
        saveRDS(list_manual_cluster, './Distance_to_Default/Checkpoints/list_manual_cluster.rds')
      } else {
        list_manual_cluster = readRDS('./Distance_to_Default/Checkpoints/list_manual_cluster.rds')
      } 
    } 
    
    # evaluate cluster performance   
    {
      # https://cran.r-project.org/web/packages/clusterCrit/vignettes/clusterCrit.pdf
      if (reload_cluster_performance == FALSE){
        clustering_criteria = c('davies_bouldin', 'silhouette', 'PBM', 'Calinski_Harabasz')
        # Davies Bouldin: the lower the better
        # Silhouette: 1 good, -1 bad
        # PBM: the higher the better
        # Calinski-Harabasz: the higher the better
        
        # averaged raw data
        df_raw_avg = df_final_small %>%
          mutate(abi_ndg = paste0(abi, '_', ndg)) %>%
          select(abi_ndg, starts_with('BILA_')) %>%
          group_by(abi_ndg) %>%
          summarize_all(mean, .groups = "drop")
        
        clustering_performance = c()
        vv = 1
        cat('\n--- Manual cluster performance evaluation:\n')
        for (label_type in names(list_manual_cluster)){
          
          cat('Testing variable label: ', vv, '/', length(list_manual_cluster), end = '\r')
          
          # cluster raw data
          df_raw = df_raw_avg %>%
            left_join(list_manual_cluster[[label_type]][["cluster_label"]], by = "abi_ndg")
          if (sum(is.na(df_raw)) > 0){cat('\n  ######## missing in df_raw:', label_type)}
          max_class_perc = df_raw$Label %>% table %>% as.data.frame() %>% mutate(Freq = Freq / sum(Freq)) %>% pull(Freq) %>% max()
          
          clust_perf = intCriteria(traj = df_raw %>% select(-abi_ndg, -Label) %>% as.matrix(),
                                   part = as.integer(df_raw$Label), crit = clustering_criteria)
          names(clust_perf) = paste0('Original_', names(clust_perf))
          
          # cluster embedding
          for (emb_type in names(list_embedding)){
            
            R2 = list_embedding[[emb_type]][['report']]$R2
            
            df_emb = list_embedding[[emb_type]][['emb']]
            # average data for long format
            if (emb_type %in% c("PCA", "AE")){
              df_emb = df_emb %>%
                select(abi_ndg, starts_with('V')) %>%
                group_by(abi_ndg) %>%
                summarize_all(mean, .groups = "drop")
            }
            df_emb = df_emb %>%
              left_join(list_manual_cluster[[label_type]][["cluster_label"]], by = "abi_ndg")
            if (sum(is.na(df_emb)) > 0){cat('\n  ######## missing in df_emb:', label_type, '-', emb_type)}

            clust_perf_embed = intCriteria(traj = df_emb %>% select(starts_with("V")) %>% as.matrix(),
                                           part = as.integer(df_emb$Label), crit = clustering_criteria)
            
            names(clust_perf_embed) = paste0('Embedding_', names(clust_perf_embed))
            
            clustering_performance = clustering_performance %>%
              bind_rows(data.frame(Embedding = emb_type, R2 = R2, Label = label_type, Max_Class_Perc = round(max_class_perc * 100, 1),
                                   clust_perf, clust_perf_embed, stringsAsFactors = F))
          } # emb_type
          vv = vv + 1
        } # label_type
        rm(clust_perf, clust_perf_embed, df_emb, df_raw, df_raw_avg)
        saveRDS(clustering_performance, './Distance_to_Default/Checkpoints/clustering_performance.rds')
      } else {
        clustering_performance = readRDS('./Distance_to_Default/Checkpoints/clustering_performance.rds')
      }
      
      # evaluate best performance list
      clustering_performance = clustering_performance %>%   # filter unbalanced clusters
        filter(Max_Class_Perc <= 70)
      clustering_performance_summary = c()
      for (emb_type in unique(clustering_performance$Embedding)){
        tt = clustering_performance %>%
          filter(Embedding == emb_type) %>%
          mutate(Best = '') %>%
          select(Embedding, Best, Label, everything())
        best_list = c()
        for (perf in tt %>% select(matches('Original_|Embedding_')) %>% colnames()){        # assign "**" to best performance for each column
          val = tt %>% select(all_of(perf)) %>% unlist() %>% as.numeric() %>% round(4)
          best_ind = bestCriterion(val, gsub('Original_|Embedding_', '', perf)) %>% as.numeric()
          best_ind = which(val == val[best_ind])  # look for multiple best values
          best_list = c(best_list, best_ind)
          best_mark = rep('', length(val))
          best_mark[best_ind] = '**  '
          tt = tt %>% mutate(!!sym(perf) := paste0(best_mark, val))
        }
        best_ind_overall = best_list %>%                      # mark overall best with "*" where "**" by row are maximum
          table() %>%
          as.data.frame(stringsAsFactors = F) %>%
          setNames(c('Ind', 'Freq')) %>%
          rowwise() %>%
          mutate(lab = paste0(rep('*', Freq), collapse = '')) %>%
          mutate(Ind = as.numeric(Ind))
        # filter(Freq == max(Freq)) %>%
        # pull('Ind') %>%
        # as.numeric()
        tt$Best[best_ind_overall$Ind] = best_ind_overall$lab
        clustering_performance_summary = clustering_performance_summary %>%
          bind_rows(tt)
      } # emb_type
      rm(clustering_performance, best_ind_overall, tt)
      write.table(clustering_performance_summary, './Distance_to_Default/Stats/02b_Manual_Clustering_Summary_Best.csv', sep = ';', row.names = F, append = F)
    }
    
    # plot best clustering for each embedding
    {
      if (plot_manual_clustering){
        best_subset = clustering_performance_summary %>%
          filter(Best != "") %>%
          mutate_at(vars(-Embedding, -Best, -Label, -R2), function(x) gsub("\\*+|\\s+", "", x) %>% as.numeric()) %>%
          mutate(Best_overall = "") %>%
          select(Embedding, Best, Best_overall, everything()) %>%
          mutate_if(is.numeric, function(x) round(x, 2))
        best_list = c()
        for (perf in best_subset %>% select(matches('Original_|Embedding_')) %>% colnames()){        # assign "**" to best performance for each column
          val = best_subset %>% select(all_of(perf)) %>% unlist() %>% as.numeric() %>% round(4)
          best_ind = bestCriterion(val, gsub('Original_|Embedding_', '', perf)) %>% as.numeric()
          best_ind = which(val == val[best_ind])  # look for multiple best values
          best_list = c(best_list, best_ind)
          best_mark = rep('', length(val))
          best_mark[best_ind] = '**  '
          best_subset = best_subset %>% mutate(!!sym(perf) := paste0(best_mark, val))
        }
        best_ind_overall = best_list %>%                      # mark overall best with "*" where "**" by row are maximum
          table() %>%
          as.data.frame(stringsAsFactors = F) %>%
          setNames(c('Ind', 'Freq')) %>%
          rowwise() %>%
          mutate(lab = paste0(rep('*', Freq), collapse = '')) %>%
          mutate(Ind = as.numeric(Ind))
        best_subset$Best_overall[best_ind_overall$Ind] = best_ind_overall$lab
        best_subset = best_subset %>%
          arrange(desc(Best_overall))
        write.table(best_subset, './Distance_to_Default/Stats/02b_Manual_Clustering_Summary_Best_Top.csv', sep = ';', row.names = F, append = F)

        best_label_type = best_subset %>%
          filter(Best_overall != '') %>%
          pull(Label) %>%
          unique()
        
        rds_list = c()
        for (label_type in best_label_type){
          
          avail_emb = best_subset %>%
            filter(Best_overall != '') %>%
            filter(Label == label_type) %>%
            pull(Embedding)
          
          # set-up Rmarkdown file
          Rmd_text = RMarkdown_settings(title = paste0("Visualization of 3D embedding with Peers for ", label_type), help_path)
          
          # add summary table
          rds_lab_summary = paste0(getwd(), "/", label_type, "_summary.rds")
          rds_lab_class_balance = paste0(getwd(), "/", label_type, "_class.rds")
          saveRDS(best_subset %>%
                    filter(Best_overall != '') %>%
                    filter(Label == label_type) %>%
                    select(-Label), rds_lab_summary)
          saveRDS(list_manual_cluster[[label_type]][["cluster_label"]] %>%
            pull(Label) %>%
            table() %>%
            as.data.frame() %>%
            setNames(c("Class", "Obs")) %>%
            mutate(Perc = paste0(round(Obs / sum(Obs) * 100, 1), "%")), rds_lab_class_balance)
          rds_list = c(rds_list, c(rds_lab_summary, rds_lab_class_balance))
          Rmd_text = paste0(Rmd_text, "\n\n# Clustering Performance\n\n",
                            "```{r, include=TRUE, echo=FALSE}\nsummary_table = readRDS('", rds_lab_summary,
                            "')\nkable(summary_table %>% setNames(gsub(\"Original_\", \"Original\\n\", names(.))) %>%",
                            "setNames(gsub(\"Embedding_\", \"Embedding\\n\", names(.)))) %>%\n",
                            "kable_styling(full_width = F, position = \"left\", font_size = 12) %>%\n",
                            "column_spec(1:ncol(summary_table), width = \"30em\")\n```", "\n\n<font size=\"5\"> Class Distribution </font>\n",
                            "```{r, include=TRUE, echo=FALSE}\nclass_table = readRDS('", rds_lab_class_balance,
                            "')\nkable(class_table) %>% kable_styling(full_width = F, position = \"left\")\n```")
          
          for (emb_type in avail_emb){
            
            rds_lab = paste0(getwd(), "/", label_type, "_", emb_type, ".rds")
            
            R2 = best_subset %>%
              filter(Label == label_type) %>%
              filter(Embedding == emb_type) %>%
              pull(R2)
            
            # include section headers
            Rmd_text = paste0(Rmd_text, "\n\n\n# ", emb_type, " - $R^2 = ", R2,"\\%$\n")
            
            # include 3D plot with shared slider
            Rmd_text = paste0(Rmd_text,
                              RMarkdow_render_3d_grid(plot_list_rds = rds_lab, emb_type, n_col = n_col, show = show,
                                                      legend_cex = legend_cex, legend_resize = legend_resize, plot_legend_index = plot_legend_index,
                                                      show_additional = show_additional, title_cex = title_cex, additional_points_size = additional_points_size,
                                                      point_alpha = point_alpha, single_class_size = single_class_size, MIN_SCALE = MIN_SCALE, MAX_SCALE = MAX_SCALE,
                                                      HTML_fig.height = HTML_fig.height, HTML_fig.width = HTML_fig.width, add_section = FALSE, add_shared_slider = T))
            
            # create all points input
            best_comb = best_comb_visual[[1]][[emb_type]] %>% 
              .[c("n_neighbors", "min_dist")] %>% as.numeric() %>%
              paste0(c("n_neig_", "_min_dist_"), ., collapse ="")
            plot_data = list_emb_visual[[emb_type]][[names(best_comb_visual)]][[best_comb]][["emb_visual"]] %>%
              select(abi_ndg, starts_with("V"))
            
            if (emb_type %in% c("PCA", "AE")){
              plot_data = plot_data %>%
                group_by(abi_ndg) %>%
                summarize_all(mean, .groups = "drop")
            }
            
            plot_data = plot_data %>%
              left_join(list_manual_cluster[[label_type]][["cluster_label"]], by = "abi_ndg") %>%
              mutate(size = 1) %>%
              setNames(gsub('^[V]', "Dim", names(.)))
            if (sum(is.na(plot_data)) > 0){cat('\n  ######## missing in plot_data:', label_type, '-', emb_type)}
            
            plot_data_peers = list_emb_visual[[emb_type]][[names(best_comb_visual)]][[best_comb]][["emb_predict_visual"]] %>%
              mutate(size = 1) %>%
              setNames(gsub('^[V]', "Dim", names(.))) 
            
            # assign label to peers
            df_label = map_cluster_on_peers(label_type, categorical_variables, df_peers_long, ORBIS_mapping, ORBIS_label)
            plot_data_peers = plot_data_peers %>%
              left_join(df_label, by = "Company_name_Latin_alphabet")
            if (sum(is.na(plot_data_peers)) > 0){cat('\n  ######## missing in plot_data_peers:', label_type, '-', emb_type)}
            
            # create aggregated points input
            plot_data_agg = aggregate_points(plot_data = plot_data %>%
                                               select(-abi_ndg),
                                             label_values = plot_data$Label %>% levels(), n_cell = n_cell,
                                             err_lab = paste0(c(label_type, emb_type), collapse = " - "))
            plot_data_agg = plot_data_agg$cell_summary
            
            plot_list = list(list(data = plot_data,
                                  title = "All points",
                                  point_alpha = 0.4,
                                  additional_data = plot_data_peers),
                             list(data = plot_data_agg,
                                  title = "Aggregated points",
                                  additional_data = plot_data_peers))
            saveRDS(plot_list, rds_lab)
            rds_list = c(rds_list, rds_lab)
            rm(plot_data, plot_data_agg, plot_data_peers, plot_list, df_label)
          } # emb_type
          
          fileConn<-file(paste0("./Distance_to_Default/Stats/99_Manual_Clustering_visualization.Rmd"))
          writeLines(strsplit(Rmd_text, "\\n")[[1]], fileConn)
          close(fileConn)
          rmarkdown::render(paste0(getwd(), "/Distance_to_Default/Stats/99_Manual_Clustering_visualization.Rmd"),
                            output_format = "html_document", output_file = paste0("02b_Manual_Clustering_visualization_", label_type, ".html"))
        } # label_type
        for (rr in rds_list){file.remove(rr)}
        rm(best_subset, best_ind_overall, summary_table, class_table, plot_list)
      }
    }
  }
  
  # assign closest peers to each CRIF data (cluster label is Company_name_Latin_alphabet)
  {
    # todo: capisci se fare la distanza sul df_final_small o anche sugli embedding - il plot lo puoi fare come sopra (magari aggiungi anche questi clustering nel loop)
    #        forse è meglio usare gli embedding (devi scegliere quello che funziona meglio, in base al Reconst_RMSE?) - per i long devi fare prima la media del vettore
    #        dei 3 anni e poi calcolare il Peer più vicino (o puoi fare per majority voting - se c'è pareggio(difficile) scegli a caso)
    
    # todo:  aggiungi il clustering in list_manual_cluster ma metti un prefisso tipo "POINTWISE_" per far capire sotto quando assegnare la DD puntualmente
    #         o facendo la media dei peers nel clustering "manuale" mappato sui peers (nella DD si assegna l'etichetta median/perc, si fanno le medie di A, L e si calcola la DD media)
  }
  
  # evaluate best clusterization on CRIF data and assign peers to each cluster (cluster label is a "set" of Company_name_Latin_alphabet)
  {
    # todo: capisci se fare il clustering sul df_final_small o anche sugli embedding. Leggi sopra, la vicinanza dei peers si può calcolare sia dal centroide sia con
    #        altre misure che tengono conto di tutti i punti). In questo caso l'embedding migliore si può scegliere in base alle performance del clustering.
    #         Magari lo si usa anche per trovare il migliore nel caso precedente
  }
  
  rm(best_comb_visual, clustering_performance_summary)
}

# evaluate Distance to Default - 2014 is removed
{
  # evaluate DD in the following way:
  #  - for manual_cluster with median/percentile: map labels onto peers and evaluate peers average A, L and volatility and compute DD in order to
  #                                               map DD/Volatility by class label
  #  - for manual_cluster with single observation clustering: map peers DD/Volatility directly by peers ID
  #  - evaluate DD on CRIF data:
  #    + use peers DD of corresponding class mapping "_-_peers_DD"
  #    + use peers Volatility of corresponding class mapping and use A and L from each CRIF observation to calculate DD "_-_peers_Volatility"
  
  #### if equity<0 we evaluate liability=asset-equity and if the resulting L is negative, we set L = 1e-16 so that ln(L) is quite big (negative)
  #### and the resulting DD is quite high (so low PD). Negative L means that equity is bigger than asset so the firm has no debt
  
  manual_clustering_label = c("roa_Median")
  categorical_variables = c('Dummy_industry', 'Dimensione_Impresa', 'Industry', 'segmento_CRIF', 'Regione_Macro')
  
  # evaluate peers DD and volatility
  {
    # take average yield for each year
    DD_risk_free_yield = read.csv2('./Distance_to_Default/Data/Risk_Free_yield.csv', stringsAsFactors=FALSE) %>%
      mutate(Date = as.Date(Date, format = "%d/%m/%Y"),
             Bid_Yield = as.numeric(Bid_Yield) / 100) %>%
      rename(risk_free_yield = Bid_Yield) %>%
      mutate(year = year(Date),
             month = month(Date)) %>%
      group_by(year) %>%
      summarise(risk_free_yield = mean(risk_free_yield))

    # evaluate asset volatility
    {
      peers_folder = './Distance_to_Default/Data/Peers_Asset'
      files_list = data.frame(file = list.files(path = peers_folder, all.files = T, full.names = FALSE, recursive = T), stringsAsFactors = F) %>%
        mutate(clean_name = gsub(".xlsx|.XLSX", "", file)) %>%
        mutate(clean_name = gsub("S.P.A.|S.P.A|SPA|S.p.A.|S.p.A.", "", clean_name)) %>%
        mutate(clean_name = gsub("-", "", clean_name)) %>%
        # mutate(clean_name = str_trim(clean_name)) %>%
        mutate(clean_name = gsub("\\s+", "", clean_name)) %>%
        mutate(clean_name = toupper(clean_name))
      
      peers_match = df_peers %>%
        select(Company_name_Latin_alphabet) %>%
        mutate(clean_name = gsub("S.P.A.|S.P.A|SPA|S.p.A.|S.p.A.", "", Company_name_Latin_alphabet)) %>%
        mutate(clean_name = gsub("-", "", clean_name)) %>%
        # mutate(clean_name = str_trim(clean_name)) %>%
        mutate(clean_name = gsub("\\s+", "", clean_name)) %>%
        mutate(clean_name = toupper(clean_name))
      
      if (length(setdiff(peers_match$clean_name, files_list$clean_name)) > 0){
        cat('\n###### peers name mismatch for asset:\n', paste0(setdiff(peers_match$clean_name, files_list$clean_name), collapse = "\n"))
      }
      peers_match = peers_match %>%
        left_join(files_list, by = "clean_name") %>%
        mutate(file_path = paste0(peers_folder, "/", file))
      
      DD_peers_volatility = c()
      for (peer_name in peers_match$Company_name_Latin_alphabet){
        
        file <- suppressMessages(read_excel(peers_match %>% filter(Company_name_Latin_alphabet == peer_name) %>% pull(file_path), col_names = F)) %>%
          setNames(paste0("X", 1:ncol(.)))
        ind = which(file$X1 == "Exchange Date")
        data_block = file[(ind+1):nrow(file), ] %>%
          setNames(file[ind,]) %>%
          mutate(Exchange_Date = as.Date(as.numeric(`Exchange Date`), origin="1899-12-30")) %>%
          mutate(year = year(Exchange_Date),
                 month = month(Exchange_Date),
                 day = day(Exchange_Date),
                 Company_name_Latin_alphabet = peer_name,
                 Close = as.numeric(Close)) %>%
          select(Company_name_Latin_alphabet, Exchange_Date, year, month, day, Close)
        DD_peers_volatility = DD_peers_volatility %>%
          bind_rows(data_block)
          rm(file, data_block)
      }
      if (sum(is.na(DD_peers_volatility)) > 0){cat('\n###### missing in DD_peers_volatility')}
      DD_peers_volatility_summary = DD_peers_volatility %>%
        group_by(Company_name_Latin_alphabet) %>%
        summarise(Min_date = min(Exchange_Date),
                  Max_date = max(Exchange_Date),
                  Min_year = min(year),
                  Max_year = max(year), .groups = "drop")
      write.table(DD_peers_volatility_summary, './Distance_to_Default/Stats/03a_ORBIS_peers_Asset_availability.csv', sep = ';', row.names = F, append = F)
      
      DD_peers_volatility_annual = DD_peers_volatility %>%
        group_by(Company_name_Latin_alphabet, year) %>%
        summarise(Volatility = sd(Close), .groups = "drop") %>%
        filter(year <= 2013 & year >= 2012)
      
      rm(files_list, peers_match, DD_peers_volatility_summary)
      }
    
    # evaluate DD
    DD_peers = df_peers_long %>%
      filter(year <= 2013 & year >= 2012) %>%
      select(Company_name_Latin_alphabet, year, Tot_Attivo, Tot_Equity) %>%
      mutate(Tot_Liability = Tot_Attivo - Tot_Equity) %>%
      mutate(Tot_Liability_orig = Tot_Liability) %>%
      left_join(DD_peers_volatility_annual, by = c("Company_name_Latin_alphabet", "year")) %>%
      left_join(DD_risk_free_yield %>% select(year, risk_free_yield), by = "year") %>%
      mutate(T = 1) %>%
      mutate(Tot_Liability = ifelse(Tot_Liability <= 0, 1e-16, Tot_Liability)) %>%
      mutate(DD = (log(Tot_Attivo / Tot_Liability) + (risk_free_yield - Volatility^2 / 2) * T) / (Volatility * sqrt(T))) %>%
      mutate(PD = round(pnorm(-DD), 3))
    write.table(DD_peers, './Distance_to_Default/Stats/03a_ORBIS_peers_DD.csv', sep = ';', row.names = F, append = F)
  }
  
  # assign DD (or PD) to CRIF data, both direct DD/PD from peers or Volatility for peers (mean for manual, point-wise for single cluster)
  {
    # return a list of data.frame with abi_ndg, year, DD and PD
    list_DD_CRIF_data = list()
    # apply manual cluster
    {
      for (manual_label in manual_clustering_label){
        cluster_data = list_manual_cluster[[manual_label]][["cluster_label"]]
        
        
        # todo: quando ci sarà il clustering puntale, usa il prefisso "POINTWISE_" per individuarli e assegnare la DD puntualmente anziché quella media
        #        calcolata sul cluster dei peers
        #       quindi ci sarà un'alternativa per il peers_ref, la label deve essere il company_ID e tutto il resto dovrebbe essere uguale
        
        
        
        # evaluate class average (by year) for A, L, r and volatility and re-evaluate DD/PD
        peers_ref = DD_peers %>%
          left_join(map_cluster_on_peers(label_type = manual_label, categorical_variables,
                                         df_peers_long, ORBIS_mapping, ORBIS_label), by = "Company_name_Latin_alphabet") %>%
          group_by(Label, year) %>%
          summarise_at(vars(Tot_Attivo, Tot_Liability_orig, Volatility, risk_free_yield, `T`), mean) %>%
          mutate(Tot_Liability = ifelse(Tot_Liability_orig <= 0, 1e-16, Tot_Liability_orig)) %>%
          mutate(DD = (log(Tot_Attivo / Tot_Liability) + (risk_free_yield - Volatility^2 / 2) * T) / (Volatility * sqrt(T))) %>%
          mutate(PD = round(pnorm(-DD), 3))
        
        # assign direct DD/PD
        df_match_DD = df_final_small %>%
          filter(year != 2014) %>%
          mutate(abi_ndg = paste0(abi, "_", ndg)) %>%
          left_join(cluster_data, by = "abi_ndg") %>%
          left_join(peers_ref %>% select(Label, year, Volatility, DD, PD), by = c("year", "Label"))
        if (nrow(df_match_DD) != nrow(df_final_small %>% filter(year != 2014))){cat('\n\n##### left_join mismatch for df_match:', manual_label)}
        
        # assign average volatility and use CRIF A, L and r
        df_match_Volat = df_final_small %>%
          filter(year != 2014) %>%
          mutate(abi_ndg = paste0(abi, "_", ndg)) %>%
          left_join(cluster_data, by = "abi_ndg") %>%
          left_join(DD_risk_free_yield %>% select(year, risk_free_yield), by = "year") %>%
          left_join(peers_ref %>% select(Label, year, Volatility, `T`), by = c("year", "Label")) %>%
          mutate(Tot_Liability = Tot_Attivo - Tot_Equity) %>%
          mutate(Tot_Liability = ifelse(Tot_Liability <= 0, 1e-16, Tot_Liability)) %>%
          mutate(DD = (log(Tot_Attivo / Tot_Liability) + (risk_free_yield - Volatility^2 / 2) * T) / (Volatility * sqrt(T))) %>%
          mutate(PD = round(pnorm(-DD), 3))
        if (nrow(df_match_Volat) != nrow(df_final_small %>% filter(year != 2014))){cat('\n\n##### left_join mismatch for df_match:', manual_label)}
        
        
        list_DD_CRIF_data[[paste0(manual_label, "_-_peers_DD")]] = df_match_DD %>% select(abi_ndg, year, DD, PD)
        list_DD_CRIF_data[[paste0(manual_label, "_-_peers_Volatility")]] = df_match_Volat %>% select(abi_ndg, year, DD, PD)
        
        
        rm(cluster_data, peers_ref, df_match_DD, df_match_Volat)
      } # manual_label
    }
    
    # apply closest peers cluster
    {
      # todo: si dovrebbe poter già includere in peers_ref
    }
    
    # apply CRIF data cluster
    {
      # todo:
    }
  }
}

rm(list_emb_visual, list_emb_visual_aggreg)


# run logistic regression for each DD assignment in list_DD_CRIF_data
run_oversample_test = F    # run test for oversampling percentage
run_tuning = F    # force parameters tuning with cross-validation. If FALSE saved tuned parameters will be reloaded
fit_final_model = F    # force fit model on full dataset with tuned parameters and reloaded cross-validated performance. If FALSE saved model will be reloaded
{
  # variables to be used as control variables (dummy)
  control_variables = c('Dummy_industry', 'Industry' , 'Dimensione_Impresa',  'segmento_CRIF', 'Regione_Macro') # todo: rimetti
  target_var = "FLAG_Default"
  additional_var = "PD"   # variable to be added to baseline model to check added value
  fixed_variables = c("PD")   # variables to be always kept in the model, i.e. no shrinkage is applied
  n_fold = 5   # todo: rimetti
  algo_set = c("Elastic-net", "Random_Forest", "MARS", "SVM-RBF")    # see fit_model_with_cv() for allowed values
  prob_thresh_cv = "best"    # probability threshold for cross-validation (in tuning)
  prob_thresh_full = "best"    # probability threshold for full dataset
  tuning_crit = "F1_test"  # "F1" or "AUC" or "Precision" or "Recall" or "Accuracy" for "_test" or "_train"
  tuning_crit_full = "F1_train"   # same of tuning_crit but applied to full dataset model when using prob_thresh_full = "best"
  tuning_crit_minimize = F    # if TRUE tuning_crit is minimized
  tuning_crit_minimize_full = F    # if TRUE tuning_crit is minimized
  balance_abi_ndg_fold = F    # if TRUE balance distribution of abi_ndg between train and test when y=1 in cross-validation
  final_oversample_perc = 100     # percentage of oversampling (SMOTE)
  
  
  # define perimeter
  df_main = df_final_small %>%
    filter(year != 2014) %>%
    mutate(abi_ndg = paste0(abi, "_", ndg)) %>%
    mutate(Industry = substr(Industry, 1, 1),
           segmento_CRIF = gsub(" ", "_", segmento_CRIF),
           Regione_Macro = gsub("-", "_", Regione_Macro)) %>%
    select(-abi, -ndg) %>%
    select(abi_ndg, everything()) %>%
    as.data.frame()
  
  # scale df_main, name target_var "y" and save scaling parameters for model coefficients rescaling
  scaled_regressor_main = df_main %>%
    select(starts_with("BILA")) %>%
    mutate_all(~scale(., center=T, scale=T))
  df_main_scaling = c()
  for (var in colnames(scaled_regressor_main)){
    tt = scaled_regressor_main %>% pull(all_of(var)) %>% attributes()
    df_main_scaling = df_main_scaling %>%
      bind_rows(data.frame(variable = var, center = tt$`scaled:center`, scale = tt$`scaled:scale`, stringsAsFactors = F))
    scaled_regressor_main = scaled_regressor_main %>%
      mutate_at(vars(all_of(var)), function(x) { attributes(x) <- NULL; x })
    rm(tt)
  }

  # check variable distribution for each target class
  {
    # all_BILA = df_main %>%
    #   select(abi_ndg, year) %>%
    #   left_join(
    #   df_final %>%
    #   mutate(abi_ndg = paste0(abi, "_", ndg)) %>%
    #   select(abi_ndg, year, starts_with("BILA_")), by = c("abi_ndg", "year")) %>%
    #   select(-abi_ndg, -year)
    
    df_check = 
      scaled_regressor_main %>%
      # all_BILA %>%
      mutate(rows = 1:n()) %>%
      gather('variable', 'val', -rows) %>%
      left_join(df_main %>%
                  select(all_of(target_var)) %>%
                  rename(y = !!sym(target_var)) %>%
                  mutate(rows = 1:n()), by = "rows") %>%
      mutate(y = as.factor(y))
    
    png(paste0('./Distance_to_Default/Results/00_Input_variable_distribution.png'), width = 18, height = 20, units = 'in', res=200)
    plot(ggplot(df_check,
                # filter(variable == "BILA_ARIC_VPROD"),
                aes(x=val, fill = y)) +
           geom_density(alpha = 0.5) +
           scale_fill_manual(values = c("blue", "red")) +
           labs(title = "Distribution of input variables by target",
                y = "Density", x = "Values") +
           facet_wrap(~variable, scales = "free", ncol = 5) +
           theme(axis.text.y = element_blank(),
                 axis.ticks.y = element_blank(),
                 axis.text.x = element_text(size = 14),
                 axis.title = element_text(size = 24),
                 plot.title = element_text(size=30),
                 plot.subtitle = element_text(size=22),
                 legend.title=element_text(size=20),
                 legend.text=element_text(size=17),
                 legend.position="top",
                 strip.text = element_text(size = 14),
                 panel.background = element_rect(fill = "white", colour = "black"),
                 panel.grid.major.x = element_line(colour = "grey", linetype = 'dashed', size = 0.4),
                 panel.grid.minor.x = element_line(colour = "grey", linetype = 'dashed', size = 0.4))
    )
    dev.off()
    rm(df_check)
  }
  
  # test impact of oversampling
  {
    if (run_oversample_test){
      contr_var = NULL
      df_main_work = df_main %>%
        select(abi_ndg, year, all_of(target_var)) %>%
        rename(y = !!sym(target_var)) %>%
        bind_cols(scaled_regressor_main) %>%
        `rownames<-`(paste0("row_", 1:nrow(.))) %>%
        select(-abi_ndg, -year)
      if (sum(is.na(df_main_work)) > 0){cat('\n###### missing in df_main_work')} 
      
      strat_fold = create_stratified_fold(df_main %>%
                                            rename(TARGET = !!sym(target_var)) %>%
                                            select(all_of(setdiff(c("TARGET", contr_var), ""))) %>%
                                            mutate_if(is.character, as.factor), inn_cross_val_fold = 1, out_cross_val_fold = n_fold,
                                          out_stratify_columns = setdiff(c("TARGET", contr_var), ""), out_stratify_target = T)
      cv_ind = strat_fold$final_blocking %>%
        rename(fold = block,
               ind = index)
      if (nrow(cv_ind) != nrow(df_main_work)){cat('\n##### observation missing in cv_ind')}
      cv_ind_check = cv_ind %>% group_by(ind) %>% summarise(count = n())
      if (nrow(cv_ind_check) != nrow(df_main_work) | max(cv_ind_check$count) != 1){cat('\n##### repeated observation in cv_ind')}
      
      test_oversample = c()
      start_time_overall = Sys.time()
      cat('\n--- Testing oversampling percentage:')
      for (oversample_perc in c(25, 50, 75, 100)){
        
        cat('  testing percentage:', oversample_perc, end = '')
        
        start_time = Sys.time()
        for (alpha in alpha_set){
          
          for (fold_i in 1:n_fold){
            test_ind = cv_ind %>%
              filter(fold == fold_i) %>%
              pull(ind)
            train_ind = cv_ind %>%
              filter(fold != fold_i) %>%
              pull(ind)
            data_test = df_main_work[test_ind, ]
            data_train = df_main_work[train_ind, ]
            
            for (seed in c(11, 22, 33, 44, 55, 66)){
              
              # oversample train set
              set.seed(seed)
              data_train_over <- suppressMessages(SMOTE(data = data_train %>% mutate(y = as.factor(y)), outcome = "y", perc_maj = oversample_perc)) %>%
                mutate(y = as.numeric(as.character(y)))
              
              ## baseline regression
              oversample_cv = get_glmnet_performance(data_train_over, data_test = data_test, alpha = alpha, standardize = F, intercept = T, parallel = T,
                                                     type.measure = "auc", lambda_final = "lambda.1se", family = "binomial",
                                                     fixed_variables = fixed_variables, n_fold_cvgmlnet = n_fold_cvgmlnet, prob_thresh = prob_thresh_cv)
              
              
              
              test_oversample = test_oversample %>%
                bind_rows(
                  oversample_cv$perf_metrics %>%
                    mutate(oversample_perc = oversample_perc, orig_train_obs = nrow(data_train), orig_perc_1 = round(sum(data_train$y) / nrow(data_train) * 100, 1),
                           over_train_obs = nrow(data_train_over), over_perc_1 = round(sum(data_train_over$y) / nrow(data_train_over) * 100, 1),
                           alpha = alpha, seed = seed, fold = fold_i)
                )
              
              rm(oversample_cv, data_train_over)
            } # seed
            rm(data_test, data_train)
            saveRDS(test_oversample, './Distance_to_Default/Checkpoints/logistic_regression/00_test_oversample.rds')
          } # fold_i
        } # alpha
        tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
        cat(' elapsed time:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))), end = '\r')
      } # oversample_perc
      tot_diff=seconds_to_period(difftime(Sys.time(), start_time_overall, units='secs'))
      cat('\n\nTotal elapsed time:', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
    } else {
      test_oversample = readRDS('./Distance_to_Default/Checkpoints/logistic_regression/99_test_oversample.rds')
    }
    
    # plot results
    plt_perf = "F1"
    dist_baseline = 1
    dist_dataset = 0.5
    
    perf_lab = ifelse(plt_perf == "F1", "F1-score", plt_perf)
    y_lim = c(test_oversample %>% pull(paste0(plt_perf, "_train")), test_oversample %>% pull(paste0(plt_perf, "_test"))) %>% range()
    tt = test_oversample %>%
      select(oversample_perc, over_perc_1, starts_with(plt_perf)) %>%      # average over fold, seed and alpha
      gather('measure', 'val', starts_with(plt_perf)) %>%
      group_by(oversample_perc, over_perc_1, measure) %>%
      summarize(avg = mean(val, na.rm = T),
                sd = sd(val, na.rm = T), .groups = "drop") %>%
      mutate(label = paste0(oversample_perc, "%\n(% of \"1\": ", over_perc_1, "%)")) %>%
      mutate(measure = gsub(paste0(plt_perf, "_"), "", measure)) %>%
      left_join(data.frame(oversample_perc = test_oversample %>% arrange(oversample_perc) %>% pull(oversample_perc) %>% unique(), stringsAsFactors = F) %>%
                  mutate(pos = (1:nrow(.)) * 5), by = "oversample_perc") %>%
      mutate(position = ifelse(measure == "train", pos - dist_baseline, pos + dist_baseline)) %>%
      mutate(measure = paste0(capitalize(measure), " set")) %>%
      rename(`Performance on:` = measure)
    
    control_label = tt %>%
      select(oversample_perc, label, pos) %>%
      unique() %>%
      mutate(vertical = zoo::rollmean(pos, k=2, fill = NA))
    
    png(paste0('./Distance_to_Default/Results/00_test_oversampling.png'), width = 12, height = 10, units = 'in', res=100)
    plot(ggplot(tt, aes(x=position, y=avg, color=`Performance on:`)) + 
           geom_point(size = 8) +
           scale_color_manual(values = c("blue", "red")) +
           geom_errorbar(aes(ymin=avg-sd, ymax=avg+sd), width = 0.5, size = 1.7) +
           geom_vline(xintercept = control_label$vertical %>% setdiff(NA)) +
           scale_x_continuous(name ="\nOversampling percentage", 
                              breaks= control_label$pos, labels = control_label$label) +
           labs(title = "Performance comparison for different oversampling percentage",
                subtitle = paste0(perf_lab, " is averaged over ", n_fold, " folds, ", uniqueN(test_oversample$alpha), " alphas, ", uniqueN(test_oversample$seed), " seeds"),
                y = paste0(perf_lab, "\n")) +
           scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = c(y_lim[1]*0.8, y_lim[2]*1.2)) +
           theme(axis.text.y = element_text(size = 16),
                 axis.text.x = element_text(size = 18),
                 axis.title = element_text(size = 24),
                 plot.title = element_text(size=30),
                 plot.subtitle = element_text(size=26),
                 legend.title=element_text(size=20),
                 legend.text=element_text(size=17),
                 panel.background = element_rect(fill = "white", colour = "black"),
                 panel.grid.major.y = element_line(colour = "grey", linetype = 'dashed', size = 0.8),
                 panel.grid.minor.y = element_line(colour = "grey", linetype = 'dashed', size = 0.8))
    )
    dev.off()
  }
  
  # check impact of abi_ndg that have different default flag over two years (1->0 or 0->1)
  {
    # check flag distribution
    df_check = df_main %>%
      select(abi_ndg, year, all_of(target_var)) %>%
      rename(y = !!sym(target_var)) %>%
      bind_cols(scaled_regressor)
    
    check_double_flag = df_check %>%
      group_by(abi_ndg) %>%
      summarize(total_flag = uniqueN(y))
    
    df_double = df_check %>%
      filter(abi_ndg %in% (check_double_flag %>% filter(total_flag == 2) %>% pull(abi_ndg)))
    df_single = df_check %>%
      filter(abi_ndg %in% (check_double_flag %>% filter(total_flag == 1) %>% pull(abi_ndg)))
    
    df_target_type_abi_ndg = df_single %>%
      mutate(Target_type = as.character(y)) %>%
      select(abi_ndg, Target_type) %>%
      unique() %>%
      bind_rows(
        df_double %>%
          select(abi_ndg, year, y) %>%
          group_by(abi_ndg) %>%
          arrange(year) %>%
          mutate(Target_type = paste0(y, collapse = "->")) %>%
          select(abi_ndg, Target_type) %>%
          unique()
      )
    
    summary_double_flag = df_single %>%
      group_by(y) %>%
      summarise(Total_rows = n(),
                Total_abi_ndg = uniqueN(abi_ndg)) %>%
      rename(Target = y) %>%
      mutate(Target = as.character(Target)) %>%
      bind_rows(
        df_double %>%
          select(abi_ndg, year, y) %>%
          group_by(abi_ndg) %>%
          arrange(year) %>%
          mutate(Target = paste0(y, collapse = "->")) %>%
          mutate(Target = paste0(y, '(', Target, ')')) %>%
          group_by(Target) %>%
          summarise(Total_rows = n(),
                    Total_abi_ndg = uniqueN(abi_ndg)) %>%
          mutate(Total_abi_ndg = ifelse(grepl("1\\(", Target), NA, Total_abi_ndg))
      ) %>%
      as.data.frame() %>%
      bind_rows(data.frame(Target = "Total", .[,-1] %>% summarise_all(sum, na.rm = T), stringsAsFactors = F)) %>%
      mutate_if(is.numeric, ~format(., big.mark = ",")) %>%
      replace(. == "    NA", "")
    write.table(summary_double_flag, './Distance_to_Default/Results/00_Double_flag_summary.csv', sep = ';', row.names = F, append = F)
    if (nrow(df_target_type_abi_ndg) != summary_double_flag %>% filter(Target == "Total") %>% pull(Total_abi_ndg) %>% gsub(",", "", .)  %>% as.numeric()){
      cat('\n####### error: mismatch in df_target_type_abi_ndg total count of abi+ndg')
    }
    
    # check predictors distribution for double flag
    legend_order = df_target_type_abi_ndg %>%
      group_by(Target_type) %>%
      summarize(Obs = n()) %>%
      arrange(match(Target_type, c("0", "1", "0->1", "1->0"))) %>%
      mutate(label = paste0(Target_type, " (", Obs, " obs)"))
    
    data_plot = df_check %>%
      left_join(df_target_type_abi_ndg, by = "abi_ndg") %>%
      group_by(Target_type, abi_ndg) %>%
      arrange(year) %>%
      summarise_all(~(.[2] - .[1]) / abs(.[1])) %>%
      filter(!is.na(year)) %>%
      select(-abi_ndg, -year, -y) %>%
      gather("Variable", "Value", -Target_type) %>%
      group_by(Variable) %>%
      mutate(p5 = quantile(Value, 0.05) %>% as.numeric(),
             p95 = quantile(Value, 0.95) %>% as.numeric()) %>%
      filter(Value <= p95 & Value >= p5) %>%
      left_join(legend_order, by = "Target_type") %>%
      mutate(label = factor(label, levels = legend_order$label))
    # filter(Variable %in% c("BILA_ARIC_VPROD", "BILA_oneri_valagg"))
    
    png(paste0('./Distance_to_Default/Results/00_Target_variable_distribution.png'), width = 15, height = 30, units = 'in', res=300)
    plot(ggplot(data_plot, aes(x = Value, fill = label)) +
           geom_density(alpha = 0.5) +
           scale_x_continuous(labels = scales::percent_format(accuracy = 1)) +
           scale_fill_manual(values = c('dodgerblue3', 'firebrick2', 'chartreuse3', 'gold1')) +
           labs(title = "Distribution of relative change (%)\nbetween 2012 and 2013", fill = "Target\nvariable:") +
           facet_wrap(.~Variable, scales = 'free', ncol = 3) +
           theme_bw() +
           theme(
             axis.text.y = element_blank(),
             axis.text.x = element_text(size = 14),
             axis.ticks.y = element_blank(),
             # axis.title.x = element_text(size = 22),
             axis.title = element_blank(),
             plot.title = element_text(size = 32),
             strip.text = element_text(size = 14),
             legend.title=element_text(size=20),
             legend.text=element_text(size=17))
    )
    dev.off()
    
    rm(df_check, check_double_flag, df_double, df_single, summary_double_flag, legend_order, data_plot)
  }
  
  log_tuning = log_tuning_all_fold = log_fitting = c()
  start_time_overall = Sys.time()
  for (model_setting in c("", control_variables)){
    # model_setting = 'Industry'   # todo: rimuovi
    
    if (model_setting == ""){
      contr_var = NULL
    } else {
      contr_var = model_setting
    }
    model_setting_lab = ifelse(model_setting == "", "no_control", paste0("control_", model_setting))
    
    cat('\n\n\n===================== ', model_setting_lab, ' =====================')
    
    
    # add dummy
    if (model_setting != ""){
      scaled_regressor = scaled_regressor_main %>%
        bind_cols(dummy_cols(
          df_main %>% select(all_of(model_setting)),
          select_columns = model_setting,
          remove_first_dummy = T,
          remove_selected_columns = TRUE)
        )
      dummy_regressor = setdiff(colnames(scaled_regressor), colnames(scaled_regressor_main))
    } else {
      scaled_regressor = scaled_regressor_main
      dummy_regressor = c()
    }
    main_regressor = colnames(scaled_regressor)
    
    # set up final dataset
    df_main_work = df_main %>%
      select(abi_ndg, year, all_of(target_var)) %>%
      rename(y = !!sym(target_var)) %>%
      bind_cols(scaled_regressor) %>%
      `rownames<-`(paste0("row_", 1:nrow(.)))
    if (sum(is.na(df_main_work)) > 0){cat('\n###### missing in df_main_work')}
    
    # prepare input of function that balance distribution of abi_ndg between train and test when y=1 balance_abi_ndg_class1()
    abi_ndg_row_reference_class1 = df_main_work %>%
      filter(y == 1) %>%
      select(abi_ndg) %>%
      rownames_to_column("row_ind") %>%
      mutate(row_ind = gsub("row_", "", row_ind) %>% as.numeric()) %>%
      group_by(abi_ndg) %>%
      summarise(row_ind = paste0(sort(row_ind), collapse = ","))
    
    abi_ndg_row_index = df_main_work %>%
      select(abi_ndg) %>%
      rownames_to_column("row_ind")
    
    ## split data into train and test using stratified sampling
    # library(HEMDAG)
    # aa = stratified.cv.data.single.class(1:nrow(df_main_work), which(df_main_work$y == 1), kk=n_fold, seed=23)
    # cv_ind = c()
    # for (kk in 1:n_fold){
    #   cv_ind = cv_ind %>%
    #     bind_rows(data.frame(fold = kk, ind = c(aa$fold.positives[[kk]], aa$fold.negatives[[kk]])))
    # }
    strat_fold = create_stratified_fold(df_main %>%
                                          rename(TARGET = !!sym(target_var)) %>%
                                          select(all_of(setdiff(c("TARGET", contr_var), ""))) %>%
                                          mutate_if(is.character, as.factor), inn_cross_val_fold = 1, out_cross_val_fold = n_fold,
                                        out_stratify_columns = setdiff(c("TARGET", contr_var), ""), out_stratify_target = T)
    cv_ind = strat_fold$final_blocking %>%
      rename(fold = block,
             ind = index)
    if (nrow(cv_ind) != nrow(df_main_work)){cat('\n##### observation missing in cv_ind')}
    cv_ind_check = cv_ind %>% group_by(ind) %>% summarise(count = n())
    if (nrow(cv_ind_check) != nrow(df_main_work) | max(cv_ind_check$count) != 1){cat('\n##### repeated observation in cv_ind')}
    
    # plot fold distribution
    {
      final_distr_data = strat_fold$final_distr_data %>%
        group_by(block) %>%
        mutate(block = paste0(block, "\n(", sum(Count), " obs)")) %>%
        ungroup()
      cmap = c(as.character(wes_palette("Cavalcanti1")), as.character(wes_palette("GrandBudapest1")), as.character(wes_palette("GrandBudapest2")),
               as.character(wes_palette("Moonrise1")), as.character(wes_palette("FantasticFox1")), as.character(wes_palette("Darjeeling1")),
               as.character(wes_palette("Zissou1")), as.character(wes_palette("Royal1")), as.character(wes_palette("Rushmore1")))
      
      png(paste0('./Distance_to_Default/Results/01_fold_distribution_', model_setting_lab, '.png'), width = 12, height = 10, units = 'in', res=100)
      plot(ggplot(final_distr_data, aes(fill=Combination, y=perc/100, x=block)) + 
             geom_bar(position="stack", stat="identity") +
             scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
             labs(y = "Distribution (%)", x = "Folds",
                  title = paste0("Stratification by: ", paste0(final_distr_data %>% select(-block, -Combination, -Count, -perc) %>% colnames(), collapse = " - "))) +
             scale_fill_manual(values = cmap) +
             theme(axis.text.y = element_text(size = 16),
                   axis.text.x = element_text(size = 16),
                   axis.title = element_text(size = 22),
                   plot.title = element_text(size=26),
                   legend.text=element_text(size=17),
                   legend.title=element_text(size=20)))
      dev.off()
    }
    
    # fit regression model with cross-validation and return single final model
    for (cluster_lab in names(list_DD_CRIF_data)){
      
      cat('\n\n    --- Evaluating clustering: ', cluster_lab, ' - ', which(names(list_DD_CRIF_data) == cluster_lab), '/', length(list_DD_CRIF_data),'\n')
      
      for (data_type in c("original")){  #, "oversample")){
        # todo: oversampled dataset must be implemented - see OLD_script_chucks.R for details (oversample should be done also at fold level)
        
        for (model in c("baseline", "additional_var")){
          
          cat('\n\n        +++ oversample:', data_type, ' - variables:', model, '\n')
          
          # define dataset
          if (model == "baseline"){
            df_work = df_main_work %>%
              left_join(list_DD_CRIF_data[[cluster_lab]], by = c("abi_ndg", "year")) %>%
              select(y, all_of(main_regressor)) %>%
              `rownames<-`(rownames(df_main_work))
            
          } else if (model == "additional_var"){
            df_work = df_main_work %>%
              left_join(list_DD_CRIF_data[[cluster_lab]], by = c("abi_ndg", "year")) %>%
              select(y, all_of(c(main_regressor, additional_var))) %>%
              `rownames<-`(rownames(df_main_work))
          }
          
          for (algo_type in algo_set){
            
            cat('\n\n          # Fitting:', algo_type)
            save_RDS_additional_lab = paste0("tuning", "_", model_setting_lab, "_", cluster_lab, "_", model, "_", data_type, "_", algo_type)
            
            # run or reload tuning
            r_err = try(tuning_perf <- suppressWarnings(readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/02_reload_', save_RDS_additional_lab, '.rds'))), silent = T)
            if (class(r_err) == "try-error"){tuning_perf = NULL}
            if (is.null(tuning_perf) | run_tuning){
              
              cat('\n            * tuning parameters...', end = '')
              
              tuning_perf = ml_tuning(df_work = df_work, algo_type = algo_type, cv_ind = cv_ind, prob_thresh_cv = prob_thresh_cv,
                                      tuning_crit = tuning_crit, tuning_crit_minimize = tuning_crit_minimize,
                                      save_RDS_additional_lab = save_RDS_additional_lab, rds_folder = './Distance_to_Default/Checkpoints/ML_model/',
                                      balance_abi_ndg_fold = balance_abi_ndg_fold,
                                      abi_ndg_row_reference_class1 = abi_ndg_row_reference_class1, abi_ndg_row_index = abi_ndg_row_index)
              
              saveRDS(tuning_perf, paste0('./Distance_to_Default/Checkpoints/ML_model/02_reload_', save_RDS_additional_lab, '.rds'))
              cat('Done in', tuning_perf$optimization_results$total_time %>% unique(), ' ', as.character(Sys.time()))
              
            } else {
              cat('\n            * reloaded best parameters')
            } # run_tuning
            
            # select best parameters and fit model on full dataset
            best_model_RDS_lab = paste0(gsub("tuning_", "fullset_", save_RDS_additional_lab), "_", tuning_perf$optimization_results$param_compact_label[1])
            best_param_set = tuning_perf$best_parameters
            non_tunable_param = tuning_perf$non_tunable_param
            
            r_err = try(fit_fullset <- suppressWarnings(readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/03_reload_', best_model_RDS_lab, '.rds'))), silent = T)
            if (class(r_err) == "try-error"){fit_fullset = NULL}
            if (is.null(fit_fullset) | fit_final_model){
              
              cat('\n            * fitting full-set model...', end = '')

              fit_fullset = fit_model_with_cv(df_work = df_work, cv_ind = NULL, algo_type = algo_type,
                                              parameter_set = best_param_set, non_tunable_param = non_tunable_param,
                                              no_cv_train_ind = c(1:nrow(df_work)), no_cv_test_ind = NULL,
                                              prob_thresh_cv = prob_thresh_full, tuning_crit = tuning_crit_full, tuning_crit_minimize = tuning_crit_minimize_full,
                                              balance_abi_ndg_fold = balance_abi_ndg_fold,
                                              abi_ndg_row_reference_class1 = abi_ndg_row_reference_class1, abi_ndg_row_index = abi_ndg_row_index)
              
              saveRDS(fit_fullset, paste0('./Distance_to_Default/Checkpoints/ML_model/03_reload_', best_model_RDS_lab, '.rds'))
              cat('Done in', fit_fullset$total_time, ' ', as.character(Sys.time()))
              
            } else {
              cat('\n            * reloaded full-set model')
            } # fit_final_model
            
            # save results
            setting_block = data.frame(model_setting_lab = model_setting_lab,
                                       cluster_lab = cluster_lab,
                                       data_type = data_type,
                                       model = model,
                                       algo_type = algo_type, stringsAsFactors = F)
            
            log_tuning = log_tuning %>%
              bind_rows(
                setting_block %>%
                  bind_cols(tuning_perf$optimization_results %>%
                              mutate(best_param = c("yes", rep("", n() -1 ))) %>%
                              select(-all_of(names(best_param_set))) %>%
                              select(param_compact_label, best_param, everything()))
              )
            
            log_tuning_all_fold = log_tuning_all_fold %>%
              bind_rows(
                setting_block %>%
                  bind_cols(tuning_perf$optimization_results_all_folds %>%
                              left_join(best_param_set %>% mutate(best_param = "yes"), by = names(best_param_set)) %>%
                              select(-all_of(names(best_param_set))) %>%
                              select(param_compact_label, best_param, everything()))
              )

            log_fitting = log_fitting %>%
              bind_rows(
                setting_block %>%
                  bind_cols(fit_fullset$fold_all_performance %>%
                              left_join(tuning_perf$optimization_results %>%
                                          select(all_of(names(best_param_set)), param_compact_label), by = names(best_param_set)) %>%
                              select(-all_of(names(best_param_set)), -fold, -ends_with("_test")) %>%
                              select(param_compact_label, everything()) %>%
                              mutate(rds = paste0('./Distance_to_Default/Checkpoints/ML_model/03_reload_', best_model_RDS_lab, '.rds')))
              )
            
            # todo: controlla non siano commentati
            write.table(log_tuning, './Distance_to_Default/Checkpoints/ML_model/00_Optimization_list.csv', sep = ';', row.names = F, append = F, na = "")
            write.table(log_tuning_all_fold, './Distance_to_Default/Checkpoints/ML_model/01_Optimization_list_ALLFOLDS.csv', sep = ';', row.names = F, append = F, na = "")
            write.table(log_fitting, './Distance_to_Default/Results/02_Fitted_models_performance.csv', sep = ';', row.names = F, append = F, na = "")
 
            # todo: rimuovi, serve per debugging
            # df_work = df_work_baseline
            # algo_type = "MARS"# "Random_Forest"# "Elastic-net"
            # parameter_set = list(alpha = 0.5)
            # non_tunable_param = list(standardize = F, 
            #                      intercept = T, 
            #                      parallel = T,
            #                      type.measure = "auc",
            #                      lambda_final = "lambda.1se",
            #                      family = "binomial",
            #                      n_fold_cvgmlnet = n_fold_cvgmlnet,
            #                      fixed_variables = fixed_variables)
            # parameter_set = list(num.trees = 100,
            #                      mtry = 5,
            #                      min.node.size = 20)
            # parameter_set = list(degree = 2)
            # 
            # model_fit = fit_model_with_cv(df_work = df_work_baseline, cv_ind = cv_ind, algo_type = algo_type,
            #                        parameter_set = parameter_set, non_tunable_param = non_tunable_param,
            #                        no_cv_train_ind = NULL, no_cv_test_ind = NULL,
            #                        prob_thresh_cv = prob_thresh_cv, tuning_crit = tuning_crit, tuning_crit_minimize = tuning_crit_minimize)
            
          } # algo_type
        } # model
      } # data_type
    } # cluster_lab 
  } # model_setting
  tot_diff=seconds_to_period(difftime(Sys.time(), start_time_overall, units='secs'))
  cat('\n\nTotal elapsed time:', paste0(lubridate::day(tot_diff), 'd:', lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
  
  
  # todo: controlla cosa rimuovere
  rm(additional_full, baseline_full, best_alpha, cv_ind, cv_ind_check, data_test_additional, data_test_baseline, data_train_additional, data_train_baseline,
     df_main_work, df_work_additional, df_work_baseline, final_distr_data, scaled_regressor, strat_fold, tuning_perf)
  
  

  
  
  log_tuning = read.csv('./Distance_to_Default/Checkpoints/ML_model/00_Optimization_list - Copy.csv', sep=";", stringsAsFactors=FALSE)
  log_fitting = read.csv('./Distance_to_Default/Results/02_Fitted_models_performance - Copy.csv', sep=";", stringsAsFactors=FALSE)
  
  
  
  ## create report
  {
    # for (cluster_lab in unique(log_fitting$cluster_lab)){}      # todo: cicla
    cl_lab = "roa_Median_-_peers_Volatility"
    d_type = "original"      # todo: cicla
    plt_perf = "F1"
    alg_type = "Random_Forest"
    
    
    # performance comparison - test set for cross-validated folds
    perf_lab = ifelse(plt_perf == "F1", "F1-score", plt_perf)
    y_lim = c(log_fitting %>% pull(paste0(plt_perf, "_train")), log_tuning %>% pull(paste0(plt_perf, "_test_avg"))) %>% range()
    dist_baseline = 1    # x-label spacing
    dist_dataset = 0.5
    tt = log_fitting %>%
      filter(data_type == d_type) %>%
      filter(cluster_lab == cl_lab) %>%
      filter(algo_type == alg_type) %>%
      left_join(log_tuning %>%
                  select(model_setting_lab, cluster_lab, data_type, model, algo_type, param_compact_label, matches("_avg|_std")),
                by = c("model_setting_lab", "cluster_lab", "data_type", "model", "algo_type", "param_compact_label")) %>%
      select(model_setting_lab, model, starts_with(plt_perf)) %>%
      gather('measure', 'val', -c(model_setting_lab, model)) %>%
      mutate(measure = gsub(paste0(plt_perf, "_"), "", measure)) %>%
      filter(!measure %in% c("train_avg", "train_std")) %>%
      mutate(type = ifelse(str_detect(measure, "_std|_avg"), "Cross-Validation", "Full dataset")) %>%
      mutate(dim = ifelse(str_detect(measure, "_std"), "sd", "avg")) %>%
      setDT() %>%
      dcast(model_setting_lab + model + type ~ dim, value.var = "val") %>%
      mutate(width = ifelse(!is.na(sd), 0.4, 0)) %>%
      left_join(data.frame(model_setting_lab = log_fitting %>% arrange(desc(model_setting_lab)) %>% pull(model_setting_lab) %>% unique(), stringsAsFactors = F) %>%
                  mutate(pos = (1:nrow(.)) * 5), by = "model_setting_lab") %>%
      mutate(position = ifelse(model == "baseline", pos - dist_baseline, pos + dist_baseline)) %>%
      mutate(position = ifelse(type == "Cross-Validation", position - dist_dataset, position + dist_dataset)) %>%
      mutate(sd = ifelse(is.na(sd), 0, sd)) %>%
      mutate(type = gsub("Cross-Validation", "Cross-Validation\ntest set", type)) %>%
      mutate(model = gsub("additional_var", paste0("With ", additional_var), model),
             model = gsub("baseline", "Baseline", model)) %>%
      rename(Model = model,
             Dataset = type) %>%
      mutate(Model = as.factor(Model),
             Dataset = as.factor(Dataset))
    
    control_label = tt %>%
      select(model_setting_lab, pos) %>%
      unique() %>%
      mutate(label = gsub("control_", "", model_setting_lab)) %>%
      mutate(label = gsub("_", "\n", label)) %>%
      mutate(vertical = zoo::rollmean(pos, k=2, fill = NA))
    
    p_perf = ggplot(tt, aes(x=position, y=avg, color=Model)) + 
      geom_point(aes(shape = Dataset), size = 8) +
      scale_shape_manual(values = c(16, 18)) +
      scale_color_manual(values = c("blue", "red")) +
      guides(size = FALSE,
             color = guide_legend(override.aes = list(shape = 15, size = 10)),
             shape = guide_legend(override.aes = list(size = 9))) +
      geom_errorbar(aes(ymin=avg-sd, ymax=avg+sd, width=width), size = 1.7) +
      geom_vline(xintercept = control_label$vertical %>% setdiff(NA), size = 1.2) +
      scale_x_continuous(name ="\nControl Variables", 
                       breaks= control_label$pos, labels = control_label$label) +
      labs(title = paste0("Performance comparison for ", cl_lab %>% gsub("_-_", " - ", .) %>% gsub("_", " ", .), "\n"),
           y = paste0(perf_lab, "\n")) +
      scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = c(y_lim[1]*0.8, y_lim[2]*1.2)) +
      theme(axis.text.y = element_text(size = 16),
            axis.text.x = element_text(size = 18),
            axis.title = element_text(size = 24),
            plot.title = element_text(size=30),
            legend.title=element_text(size=20),
            legend.text=element_text(size=17),
            panel.background = element_rect(fill = "white", colour = "black"),
            panel.grid.major.y = element_line(colour = "grey", linetype = 'dashed', size = 0.8),
            panel.grid.minor.y = element_line(colour = "grey", linetype = 'dashed', size = 0.8))

    png(paste0('./Distance_to_Default/Results/03_Perf_comparison_', cl_lab, '_', d_type, '_', alg_type, '.png'), width = 12, height = 10, units = 'in', res=100)
    plot(p_perf)
    dev.off()
    
    
    
    # probability distribution
    tot_mod_set_lab = log_fitting$model_setting_lab %>% unique()
    tot_mod_set_lab = c(tot_mod_set_lab, tot_mod_set_lab)   # todo: rimuovi
    fig_per_row = 3
    
    row_list = c()
    fig_count = 1
    for (mod_set_lab in tot_mod_set_lab){
      # mod_set_lab = "no_control"     # todo: rimuovi
      
      if (fig_count %% fig_per_row == 1){row_img = c()}
      
      rds_ref = log_fitting %>%
        filter(data_type == d_type) %>%
        filter(cluster_lab == cl_lab) %>%
        filter(algo_type == alg_type) %>%
        filter(model_setting_lab == mod_set_lab)
      
      tt = readRDS(rds_ref %>% filter(model == "baseline") %>% pull(rds))$fold_prediction %>%
        mutate(data_type = d_type,
               model = "Baseline") %>%
        group_by(y_true) %>%
        mutate(tot_true = n()) %>%
        ungroup() %>%
        group_by(y_true, y_pred) %>%
        mutate(tot_pred = n()) %>%
        ungroup() %>%
        bind_rows(
          readRDS(rds_ref %>% filter(model == "additional_var") %>% pull(rds))$fold_prediction %>%
            mutate(data_type = d_type,
                   model = "additional_var") %>%
            group_by(y_true) %>%
            mutate(tot_true = n()) %>%
            ungroup() %>%
            group_by(y_true, y_pred) %>%
            mutate(tot_pred = n()) %>%
            ungroup()
        ) %>%
        rename(Predicted = y_pred) %>%
        mutate(y_true = paste0("True: ", y_true, " (", format(tot_true, big.mark = ","), " obs)"),
               model = gsub("additional_var", paste0("With ", additional_var), model))
      
      tt_annotate = tt %>%
        group_by(data_type, model, y_true) %>%
        summarize(Pred_lab_1 = paste0("Pred \"1\"\nobs: ", format(tot_pred[Predicted == 1][1], big.mark = ",")),
                  Pred_lab_0 = paste0("Pred \"0\"\nobs: ", format(tot_pred[Predicted == 0][1], big.mark = ",")), .groups = "drop")
      
      p_distr = ggplot(tt, aes(x=Prob, fill = Predicted)) +
        geom_density(alpha = 0.5) +
        scale_fill_manual(values = c("blue", "red")) +
        scale_x_continuous(labels = scales::percent_format(accuracy = 1)) +
        labs(title = paste0(fig_count, "---", gsub("control_", "", mod_set_lab) %>% gsub("_", " ", .)),  # todo: togli paste0
             y = "Density", x = "Predicted probability", fill = "Predicted\nClass") +
        facet_grid(model~y_true, scales = "fixed", switch = 'y') +
        theme(
          axis.text.y = element_blank(),
          axis.ticks.y = element_blank(),
          axis.text.x = element_text(size = 14),
          axis.title = element_text(size = 20),
          plot.title = element_text(size=27),
          plot.subtitle = element_text(size=22),
          legend.title=element_text(size=20),
          legend.text=element_text(size=17),
          strip.text = element_text(size = 14),
          strip.text.y = element_text(margin = margin(0,0.4,0,0.4, "cm")),
          panel.background = element_rect(fill = "white", colour = "black"),
          panel.grid.major.x = element_line(colour = "grey", linetype = 'dashed', size = 0.4),
          panel.grid.minor.x = element_line(colour = "grey", linetype = 'dashed', size = 0.4))
      max_y_val = suppressWarnings(layer_scales(p_distr)$y$get_limits() %>% max())
      p_distr = p_distr +
        geom_text(data = tt_annotate %>% filter(data_type == d_type) %>% mutate(Prob = 0.75, Predicted = "1"), aes(x = Prob, y = 0.9*max_y_val, label = Pred_lab_1), size = 5) +
        geom_text(data = tt_annotate %>% filter(data_type == d_type) %>% mutate(Prob = 0.25, Predicted = "0"), aes(x = Prob, y = 0.9*max_y_val, label = Pred_lab_0), size = 5) +
        geom_segment(aes(x = threshold , y = 0, xend = threshold, yend = 0.8*max_y_val), linetype = 'dashed', size = 1.2) +
        geom_text(aes(x = threshold, y = 0, label = as.character(round(threshold, 2))), vjust = 1)
      if (fig_count %% fig_per_row != 0 & fig_count != length(tot_mod_set_lab)){
        p_distr = p_distr + theme(legend.position = "none")
      }
      
      img = image_graph(res = 100, width = 800, height = 800, clip = F)
      suppressWarnings(p_distr)
      dev.off()
      
      cat('\n', fig_count, dim(img))
      
      if (is.null(row_img)){
        row_img = img
      } else {
        row_img = image_append(c(row_img, img))
      }
      if (fig_count %% fig_per_row == 0 | fig_count == length(tot_mod_set_lab)){row_list = c(row_list, row_img)}
      cat('\n', fig_count)
      fig_count = fig_count + 1
    } # mod_set_lab
    
    # assemble rows
    eval(parse(text=paste0('final_plot = image_append(c(', paste0('row_list[[', 1:length(row_list), ']]', collapse = ','), '), stack = T)')))
    
    # add main title and subtitle
    title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 200, clip = F)
    plot(
      ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 5) +
        annotate(geom = "text", x = 0, y = 3.5, label = "titletttt", cex = 20, hjust = 0, vjust = 0.5) +
        annotate(geom = "text", x = 0, y = 1.5, label = "subtitletttt", cex = 15, hjust = 0, vjust = 0.5) +
        theme_bw() +
        theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
               axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
               plot.margin=unit(c(0,0.4,0,0.4),"cm"))
    )
    dev.off()
    
    final_plot = image_append(c(title_lab, final_plot), stack = T)

    png(paste0('./Distance_to_Default/Results/04_Prob_distr_', cl_lab, '_', d_type, '_', alg_type, '.png'), width = 7, height = 2.1 * length(row_list), units = 'in', res=300)
    par(mar=c(0,0,0,0))
    par(oma=c(0,0,0,0))
    plot(final_plot)
    dev.off()
    
    # todo: capisci perché magik non funziona nel loop per fig_count = 2
    
    labs(title = "Distribution of true vs predicted",
         subtitle = "Vertical lines represent probability to class thresholds",
    
    
    # todo: togli, servono solo per la call
    png(paste0('./Distance_to_Default/Results/prob_distr_', cl_lab, '_', model_setting_lab, '_', d_type, '_', alg_type, '.png'), width = 12, height = 10, units = 'in', res=100)
    plot(p_distr)
    dev.off()
    
    
    
  }

  
   
}


## todo: per ogni fold calcola F1-score, AUC e ROC (sia per train che per test) e poi crea report finale con i valori medi +st.dev
#        valuta se fare due plot diversi con le misure medie del train e del test

  
## todo: aggiungi distribuzione PD e DD per ogni regressione (anche vs FLAG_default)

## valuta se ritrasformare i coefficienti rispetto alla scala iniziale dei dati
# https://stats.stackexchange.com/questions/74622/converting-standardized-betas-back-to-original-variables

# todo: aggiungi la variable importance

# todo: aggiungi un semplice calcolo di quanti abi_ndg cambiano flag da un anno all'altro. Magari si potrebbe fare un focus di predizione solo su quelli
#       per capire se il modello riesce a predirli bene o li predice sempre 0 o sempre 1.
#       aa = df_main_work %>% group_by(abi_ndg) %>% summarize(cc = uniqueN(y))

vv = df_main_work %>%
  filter(abi_ndg %in% (aa %>% filter(cc == 2) %>% pull(abi_ndg)))

vv1 = vv %>%
  group_by(abi_ndg) %>%
  arrange(year) %>%
  summarise(evol = paste0(y, collapse = ",")) %>%
  group_by(evol) %>%
  summarise(Count = n())



vv2 = df_main_work %>%
  filter(!abi_ndg %in% (aa %>% filter(cc == 2) %>% pull(abi_ndg))) %>%
  group_by(y) %>%
  summarise(count = n())


















# baseline
df_work = df_main_work %>%
  left_join(list_DD_CRIF_data[[cluster_lab]], by = c("abi_ndg", "year")) %>%
  select(y, all_of(main_regressor)) %>%
  `rownames<-`(rownames(df_main_work))


fit_train_baseline = fit_cv_glmnet(x = df_work %>% select(-y), y = df_work$y, alpha = 1, standardize = F, intercept = T, parallel = T,
                          type.measure = "auc", lambda_final = "lambda.1se", family = "binomial",
                          fixed_variables = fixed_variables, n_fold_cvgmlnet = n_fold_cvgmlnet)


# regression with additional_var = PD
df_work_PD = df_main_work %>%
  left_join(list_DD_CRIF_data[[cluster_lab]], by = c("abi_ndg", "year")) %>%
  select(y, all_of(c(main_regressor, additional_var))) %>%
  `rownames<-`(rownames(df_main_work))

fit_train_PD = fit_cv_glmnet(x = df_work_PD %>% select(-y), y = df_work_PD$y, alpha = 1, standardize = F, intercept = T, parallel = T,
                             type.measure = "auc", lambda_final = "lambda.1se", family = "binomial",
                             fixed_variables = fixed_variables, n_fold_cvgmlnet = n_fold_cvgmlnet)

fit_train_PD_no_pen = fit_cv_glmnet(x = df_work_PD %>% select(-y), y = df_work_PD$y, alpha = 1, standardize = F, intercept = T, parallel = T,
                                    type.measure = "auc", lambda_final = "lambda.1se", family = "binomial",
                                    fixed_variables = c(), n_fold_cvgmlnet = n_fold_cvgmlnet)


# regression with additional_var = DD
df_work_DD = df_main_work %>%
  left_join(list_DD_CRIF_data[[cluster_lab]], by = c("abi_ndg", "year")) %>%
  select(y, all_of(c(main_regressor, "DD"))) %>%
  `rownames<-`(rownames(df_main_work)) %>%
  mutate(DD = scale(DD))

fit_train_DD = fit_cv_glmnet(x = df_work_DD %>% select(-y), y = df_work_DD$y, alpha = 1, standardize = F, intercept = T, parallel = T,
                             type.measure = "auc", lambda_final = "lambda.1se", family = "binomial",
                             fixed_variables = c("DD"), n_fold_cvgmlnet = n_fold_cvgmlnet)

fit_train_DD_no_pen = fit_cv_glmnet(x = df_work_DD %>% select(-y), y = df_work_DD$y, alpha = 1, standardize = F, intercept = T, parallel = T,
                                    type.measure = "auc", lambda_final = "lambda.1se", family = "binomial",
                                    fixed_variables = c(), n_fold_cvgmlnet = n_fold_cvgmlnet)




aa_res = data.frame(Variable = "Accuracy", Coeff = pp(fit_train_baseline$pred_prob, df_work$y), stringsAsFactors = F) %>%
  bind_rows(fit_train_baseline$coeff) %>%
  rename(Coeff_baseline = Coeff) %>%
  
  full_join(data.frame(Variable = "Accuracy", Coeff = pp(fit_train_PD$pred_prob, df_work$y), stringsAsFactors = F) %>%
              bind_rows(fit_train_PD$coeff) %>%
              rename(Coeff_PD = Coeff), by = "Variable") %>%
  full_join(data.frame(Variable = "Accuracy", Coeff = pp(fit_train_PD_no_pen$pred_prob, df_work$y), stringsAsFactors = F) %>%
              bind_rows(fit_train_PD_no_pen$coeff) %>%
              rename(Coeff_PD_no_forcing = Coeff), by = "Variable") %>%
  
  full_join(data.frame(Variable = "Accuracy", Coeff = pp(fit_train_DD$pred_prob, df_work$y), stringsAsFactors = F) %>%
              bind_rows(fit_train_DD$coeff) %>%
              rename(Coeff_DD = Coeff), by = "Variable") %>%
  
  full_join(data.frame(Variable = "Accuracy", Coeff = pp(fit_train_DD_no_pen$pred_prob, df_work$y), stringsAsFactors = F) %>%
              bind_rows(fit_train_DD_no_pen$coeff) %>%
              rename(Coeff_DD_no_forcing = Coeff), by = "Variable")


write.table(aa_res, 'logistic_res.csv', sep = ';', row.names = F, append = F)

pp = function(prob, y_true){
  
  y_pred = prob$Prob
  y_pred = ifelse(y_pred >= 0.5, 1, 0)
  
  prec = sum(y_true == y_pred) / length(y_true)
  
  return(prec)
}







aa = df_final_small %>% select(abi, ndg, year, Tot_Attivo, Tot_Equity) %>%
  mutate(abi_ndg = paste0(abi, "_", ndg)) %>%
  mutate(Tot_Equity_floor = ifelse(Tot_Equity < 0, 0, Tot_Equity)) %>%
  mutate(Tot_Liability = Tot_Attivo - Tot_Equity,
         Tot_Liability_floor = Tot_Attivo - Tot_Equity_floor) %>%
  select(-abi, -ndg) %>%
  select(abi_ndg, everything())
write.table(aa, 'check_equity.csv', sep = ';', row.names = F, append = F)


# todo: si può fare clustering oggettivo in due modi: fai clustering di CRIF e poi assegni i peers al centroide più vicino oppure
#       assegni ad ogni CRIF il peer più vicino. In entrambi i casi possiamo usare sia i dati grezzi, sia gli embedding o una media delle varie vicinanze?


# controlla tutti i todo: da rimuovere   <<<---------------




# per il momento possiamo provare a calcolare mediane e percentili con i valori stessi di CRIF, giusto per vedere se i punti vengono divisi con qualche variabile
#     per automatizzare il tutto potrebbe aver senso mettere delle misure di bontà di clustering (Davies-Bouldin, etc.) - aggiungile anche nel titolo dei plot
#     https://scikit-learn.org/stable/modules/clustering.html#clustering-performance-evaluation














# rilegge i file creati nel tuning

# save_RDS_additional_lab = 'AE_LSTM_emb'
# # save observations names
# x_input = create_lstm_input(df = df_emb_input, df_header = df_emb_input_header, ID_col = 'abi_ndg', TIME_col = 'year', NA_masking = 0)
# names(x_input) = NULL
# x_input = np$array(x_input)
# x_input_dim = py_to_r(x_input$shape) %>% unlist()
# 
# # create a copy of x_input, reshaping into a (obs*timestep)x(features) matrix including masked values
# x_input_full = array_reshape(x_input, c(prod(x_input_dim[1:2]), x_input_dim[3]))
# x_input_full = py_to_r(x_input_full)
# 
# # save masking index
# if (!is.null(masking_value)){
#   masking = x_input_full == masking_value
# } else {
#   masking = matrix(FALSE, ncol = ncol(x_input_full), nrow = nrow(x_input_full))
# }
# 
# result_csv = paste0('./Distance_to_Default/Checkpoints/Autoencoder_test/00_Optimization_list_', save_RDS_additional_lab, '.csv')
# result = read.csv(result_csv, sep=";", stringsAsFactors=FALSE) %>%
#   mutate(R2 = -99) %>%
#   relocate(R2, .after = MSE)
# for (i in 1:nrow(result)){
#   if (result$MSE[i] != 999){
#     aut = readRDS(result$RDS_path[i])
#     full_output = aut$reconstr_prediction
#     
#     R2 = round(eval_R2(x_input_full, full_output, masking = masking) * 100, 1)
#     R2_99 = round(eval_R2(x_input_full, full_output, 0.99, masking = masking) * 100, 1)
#     R2_95 = round(eval_R2(x_input_full, full_output, 0.95, masking = masking) * 100, 1)
#     
#     aut$R2 = R2
#     aut$R2_99 = R2_99
#     aut$R2_95 = R2_95
#     
#     saveRDS(aut, result$RDS_path[i])
#     
#     result$R2[i] = aut$R2
#   }
# }
# write.table(result, result_csv, sep = ';', row.names = F, append = F)


# res =  read.csv('./Distance_to_Default/Checkpoints/Autoencoder_test/00_Optimization_list_AE_LSTM_emb.csv', sep=";", stringsAsFactors=FALSE) %>%
#   mutate(layers_list = as.character(layers_list))
# lstm_list = paste0("./Distance_to_Default/Checkpoints/Autoencoder_test/",
#                      list.files(path = './Distance_to_Default/Checkpoints/Autoencoder_test/', pattern = "^AE_LSTM_emb*"), sep = "")
# for (i in 1:length(lstm_list)){
#   aut = readRDS(lstm_list[i])
#   opt = aut$options
#   add_row = data.frame(layers = length(opt$layer_list),
#                        layers_list = paste0(opt$layer_list, collapse = ".") %>% as.character(),
#                        n_comp = opt$n_comp,
#                        RNN_type = opt$RNN_type,
#                        kernel_reg_alpha = ifelse(is.null(opt$kernel_reg_alpha), "", as.character(opt$kernel_reg_alpha)),
#                        batch_size = opt$batch_size,
#                        MSE = aut$ReconstErrorRMSE ^ 2,
#                        RDS_path = lstm_list[i],
#                        R2 = aut$R2, stringsAsFactors = F)
#   res = res %>%
#     bind_rows(add_row)
# }
# write.table(res %>% relocate(R2, .after = MSE) %>% filter(!is.na(R2)), './Distance_to_Default/Checkpoints/Autoencoder_test/00_Optimization_list_AE_LSTM_emb.csv', sep = ';', row.names = F, append = F)
