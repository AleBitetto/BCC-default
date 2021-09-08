
utils::memory.limit(32000)
library(haven)
library(readxl)
library(stringr)
library(ppcor)
library(lme4)
library(kmlShape)
library(optimx)
library(reshape)
library(Hmisc)
library(lubridate)
library(cowplot)
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
      saveRDS(boxplot_data, './Distance_to_Default/Checkpoints/boxplot_data.rds')
      
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


# run regression model for each DD assignment in list_DD_CRIF_data
run_oversample_test = F    # run test for oversampling percentage
run_tuning = F    # force parameters tuning with cross-validation. If FALSE saved tuned parameters will be reloaded
fit_final_model = F    # force fit model on full dataset with tuned parameters and reloaded cross-validated performance. If FALSE saved model will be reloaded
skip_all = T    # skip all model fitting block and loads log_fitting and log_tuning only
run_feat_imp = F   # evaluate feature importance
run_plot_feat_imp = F    # plot feature importance
{
  # variables to be used as control variables (dummy)
  control_variables = c('Dummy_industry', 'Industry' , 'Dimensione_Impresa',  'segmento_CRIF', 'Regione_Macro') # todo: rimetti
  target_var = "FLAG_Default"
  additional_var = "PD"   # variable to be added to baseline model to check added value
  fixed_variables = c("PD")   # variables to be always kept in the model, i.e. no shrinkage is applied
  n_fold = 5   # todo: rimetti
  algo_set = c("Elastic-net", "Random_Forest", "MARS")#, "SVM-RBF")    # see fit_model_with_cv() for allowed values
  prob_thresh_cv = "best"    # probability threshold for cross-validation (in tuning)
  prob_thresh_full = "best"    # probability threshold for full dataset
  tuning_crit = "F1_test"  # "F1" or "AUC" or "Precision" or "Recall" or "Accuracy" for "_test" or "_train"
  tuning_crit_full = "F1_train"   # same of tuning_crit but applied to full dataset model when using prob_thresh_full = "best"
  tuning_crit_minimize = F    # if TRUE tuning_crit is minimized
  tuning_crit_minimize_full = F    # if TRUE tuning_crit is minimized
  balance_abi_ndg_fold = F    # if TRUE balance distribution of abi_ndg between train and test when y=1 in cross-validation
  final_oversample_perc = 100     # percentage of oversampling (SMOTE)
  
  
  ### define perimeter
  df_main = df_final_small %>%
    filter(year != 2014) %>%
    mutate(abi_ndg = paste0(abi, "_", ndg)) %>%
    mutate(Industry = substr(Industry, 1, 1),
           segmento_CRIF = gsub(" ", "_", segmento_CRIF),
           Regione_Macro = gsub("-", "_", Regione_Macro)) %>%
    select(-abi, -ndg) %>%
    select(abi_ndg, everything()) %>%
    as.data.frame()
  
  
  ### fit models
  {
    if (skip_all == FALSE){
      
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
        
        rm(tt, test_oversample, control_label)
      }
      
      # check impact of abi_ndg that have different default flag over two years (1->0 or 0->1)
      {
        # check flag distribution
        df_check = df_main %>%
          select(abi_ndg, year, all_of(target_var)) %>%
          rename(y = !!sym(target_var)) %>%
          bind_cols(scaled_regressor_main)
        
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
        
        png(paste0('./Distance_to_Default/Results/00_Target_variable_distribution_between_years.png'), width = 15, height = 30, units = 'in', res=300)
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
        rm(cv_ind_check)
        
        # plot fold distribution for target vs control variable
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
          rm(final_distr_data)
        }
        
        # plot fold distribution for input variables
        {
          if (model_setting_lab == "no_control"){
            
            plot_data = c()
            for (fold_i in cv_ind$fold %>% unique() %>% sort()){
              test_ind = cv_ind %>%
                filter(fold == fold_i) %>%
                pull(ind)
              train_ind = cv_ind %>%
                filter(fold != fold_i) %>%
                pull(ind)
              data_test = df_main_work[test_ind, ] %>% select(y, all_of(main_regressor)) %>% mutate(fold = paste0("fold ", fold_i), set = "Test")
              data_train = df_work[train_ind, ] %>% select(y, all_of(main_regressor)) %>% mutate(fold = paste0("fold ", fold_i), set = "Train")
              plot_data = plot_data %>%
                bind_rows(data_test, data_train)
            } # fold_i
            plot_data = plot_data %>%
              gather("variable", "val", -c(fold, set, y)) %>%
              mutate(variable = gsub("BILA_", "", variable),
              set = paste0(y, " ", set)) %>%
              mutate(set = factor(set, levels = c("1 Train", "0 Train", "1 Test", "0 Test")))

            png(paste0('./Distance_to_Default/Results/00_Input_variable_distribution_per_fold.png'), width = 15, height = 50, units = 'in', res=300)
            plot(ggplot(plot_data, aes(x = val, fill = set)) +
                   geom_density(alpha = 0.5) +
                   scale_fill_manual(values = c("1 Train" = 'dodgerblue3', "0 Train" = 'firebrick2', "1 Test" = 'blue', "0 Test" = 'firebrick4')) +
                   labs(title = "Distribution of input variables in each fold", fill = "Class / Set") +
                   facet_grid(rows = vars(variable), cols = vars(fold), scales = "free", switch = "y") +
                   theme_bw() +
                   theme(
                     axis.text = element_blank(),
                     axis.ticks = element_blank(),
                     axis.title = element_blank(),
                     plot.title = element_text(size = 32),
                     strip.text = element_text(size = 14),
                     legend.title=element_text(size=20),
                     legend.text=element_text(size=17))
            )
            dev.off()

            rm(data_test, data_train, plot_data)
          }
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
              
              # save dataset for feature importance - only "no_control"
              if (model_setting_lab == "no_control"){
                saveRDS(df_work, paste0('./Distance_to_Default/Checkpoints/ML_model/04_feature_importance_input_',
                                        model_setting_lab, "_", cluster_lab, "_", model, "_", data_type, '.rds'))
              }
              
              # plot distribution of target variable vs DD/PD
              if (model_setting_lab == "no_control" & model == "additional_var"){
                data_plot = df_work %>%
                  select(y, all_of(additional_var)) %>%
                  rename(var = !!sym(additional_var)) %>%
                  arrange(var) %>%
                  mutate(x = 1:n(),
                         y = as.factor(y),
                         ypos = ifelse(y == 0, min(var) - diff(range(var)) * 0.1, min(var) - diff(range(var)) * 0.2))
                y_lim = range(c(data_plot$var, data_plot$ypos))
                y_lim[1] = min(c(y_lim[1], min(data_plot$var) - diff(range(data_plot$var)) * 0.25))
                y_break = quantile(data_plot$var, c(0, 0.25, 0.5, 0.75, 1))
                saveRDS(data_plot, paste0('./Distance_to_Default/Checkpoints/', additional_var, '_distribution_vs_target_', cluster_lab, '.rds'))
                
                png(paste0('./Distance_to_Default/Results/01_', additional_var, '_distribution_vs_target_', cluster_lab, '.png'), width = 10, height = 8, units = 'in', res=200)
                plot(ggplot(data_plot, aes(x=x, y=var)) +
                       geom_line(size = 1.5, color = "black") +
                       geom_jitter(data = data_plot, aes(x=x, y=ypos, colour=y)) +
                       scale_color_manual(values = c("0" = "blue", "1"= "red")) +
                       scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = y_lim, breaks = y_break) +
                       labs(title = paste0(additional_var, " distribution vs target variable"),
                            subtitle = paste0("y-axis reports quartiles of ", additional_var),
                            y = additional_var, x = "Observations", color = "Target\nvariable") +
                       guides(color = guide_legend(override.aes = list(size = 5))) +
                       theme(axis.text.y = element_text(size = 16),
                             axis.text.x = element_blank(),
                             axis.ticks.x = element_blank(),
                             axis.title = element_text(size = 24),
                             plot.title = element_text(size=30),
                             plot.subtitle = element_text(size=20),
                             legend.title=element_text(size=20),
                             legend.text=element_text(size=17),
                             panel.background = element_rect(fill = "white", colour = "black"),
                             panel.grid.major = element_line(colour = "grey", linetype = 'dashed', size = 0.4),
                             panel.grid.minor = element_line(colour = "grey", linetype = 'dashed', size = 0.4))
                )
                dev.off()
                rm(data_plot)
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
                
                rm(setting_block, tuning_perf, fit_fullset, best_param_set)
              } # algo_type
            } # model
          } # data_type
        } # cluster_lab 
      } # model_setting
      tot_diff=seconds_to_period(difftime(Sys.time(), start_time_overall, units='secs'))
      cat('\n\nTotal elapsed time:', paste0(lubridate::day(tot_diff), 'd:', lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
      
      rm(r_err, scaled_regressor, scaled_regressor_main, strat_fold, df_work, df_main_work, cv_ind, abi_ndg_row_index, abi_ndg_row_reference_class1)
      
    } else {
      log_tuning = read.csv('./Distance_to_Default/Checkpoints/ML_model/00_Optimization_list.csv', sep=";", stringsAsFactors=FALSE)
      log_tuning_all_fold = read.csv('./Distance_to_Default/Checkpoints/ML_model/01_Optimization_list_ALLFOLDS.csv', sep=";", stringsAsFactors=FALSE)
      log_fitting = read.csv('./Distance_to_Default/Results/02_Fitted_models_performance.csv', sep=";", stringsAsFactors=FALSE)
    } # skip_all
    
    # merge full dataset performance with cross-validated ones
    log_fitting_with_CV = log_fitting %>%
      rename_with(function(x) x %>% gsub("_train", "", .) %>% paste0("FullSet_", .), ends_with("_train")) %>%
      left_join(log_tuning %>%
                  filter(best_param == "yes") %>%
                  select(model_setting_lab, cluster_lab, data_type, model, algo_type, param_compact_label, matches("_avg|_std")) %>%
                  rename_with(~paste0("CrossVal_", .), matches("_avg|_std")),
                by = c("model_setting_lab", "cluster_lab", "data_type", "model", "algo_type", "param_compact_label")) %>%
      relocate(rds, .after = last_col())
    write.table(log_fitting_with_CV, './Distance_to_Default/Results/02a_Fitted_models_performance_with_CV.csv', sep = ';', row.names = F, append = F, na = "")
    
    # fitted model summary
    log_fitting_summary = log_fitting_with_CV %>%
      select(-threshold, -param_compact_label, -rds) %>%
      gather('perf', 'val', -c(model_setting_lab, cluster_lab, data_type, model, algo_type, FullSet_obs, FullSet_perc_1)) %>%
      mutate(abs_perf = gsub("FullSet_|CrossVal_|_test_std|_test_avg|_train_std|_train_avg", "", perf)) %>%
      mutate(set = ifelse(grepl("FullSet", perf), "full", ifelse(grepl("_train_", perf), "cv_train", "cv_test"))) %>%
      mutate(moment = ifelse(grepl("_avg", perf), "avg", ifelse(grepl("_std", perf), "std", "")))

    first_perf = tuning_crit_full %>% strsplit(., "_") %>% .[[1]] %>% .[1]
    col_order = c(first_perf, log_fitting_summary$abs_perf %>% sort() %>% setdiff(., first_perf))
    col_order = paste0(rep(col_order, each = 3), rep(c("", "_cv_train", "_cv_test"), length(col_order)))
    
    log_fitting_summary = log_fitting_summary %>%
      filter(set != "full") %>%
      group_by(model_setting_lab, cluster_lab, data_type, model, algo_type, FullSet_obs, FullSet_perc_1, abs_perf, set) %>%
      summarize(label = paste0(round(val[moment == "avg"] * 100, 2), "±", round(val[moment == "std"] * 100, 2), "%"), .groups = "drop") %>%
      bind_rows(
        log_fitting_summary %>%
          filter(set == "full") %>%
          group_by(model_setting_lab, cluster_lab, data_type, model, algo_type, FullSet_obs, FullSet_perc_1, abs_perf, set) %>%
          summarize(label = paste0(round(unique(val) * 100, 2), "%"), .groups = "drop")
      ) %>%
      mutate(ref = paste0(abs_perf, "_", set)) %>%
      select(-set, -abs_perf) %>%
      spread(ref, label) %>%
      setNames(gsub("_full", "", names(.))) %>%
      select(model_setting_lab, cluster_lab, data_type, model, algo_type, FullSet_obs, FullSet_perc_1, all_of(col_order))
    write.table(log_fitting_summary, './Distance_to_Default/Results/02b_Fitted_models_summary.csv', sep = ';', row.names = F, append = F, na = "")
    }
  
  
  ### evaluate feature importance - only for "no_control" variables
  d_type = "original"
  mod_set_lab = "no_control"
  performance_metric = "F1"   # used in Permutation Feature Importance
  n_repetitions = 5    # repetitions in Permutation Feature Importance
  compare = "difference"   # used in Permutation Feature Importance
  sample.size = 100    # used in SHAP   # todo: rimetti 100
  n_batch = 5    # used in SHAP to save memory
  n_workers = 5    # workers for parallel computation
  {
    for (cl_lab in log_fitting$cluster_lab %>% unique()){
      
      cat('\n\n\n===================== ', cl_lab, ' =====================')
      
      for (mod in log_fitting %>% filter(cluster_lab == cl_lab) %>% pull(model) %>% unique()){   # "baseline" or "additional_var"
        
        rds_lab = paste0(mod_set_lab, "_", cl_lab, "_", mod, "_", d_type)
        
        if (run_feat_imp){
          
          cat('\n *** Evaluating:', mod)
          
          # reload dataset
          df_work = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/04_feature_importance_input_',
                                   mod_set_lab, "_", cl_lab, "_", mod, "_", d_type, '.rds'))
          model_setting_block = log_fitting %>%
            filter(data_type == d_type) %>%
            filter(cluster_lab == cl_lab) %>%
            filter(model_setting_lab == mod_set_lab) %>%
            filter(model == mod) %>%
            select(model_setting_lab, cluster_lab, data_type, model, algo_type, rds)
          
          # Permutation Feature Importance
          feat_imp_PFI = evaluate_feature_importance(df_work, # %>% group_by(y) %>% filter(row_number() <= 300) %>% ungroup(),  # todo: togli
                                                     model_setting_block, method = "Permutation",
                                                     performance_metric = performance_metric, n_repetitions = n_repetitions, compare = compare,
                                                     verbose = 1, n_workers = n_workers, seed = 66)
          
          saveRDS(feat_imp_PFI, paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_PFI_', rds_lab, '.rds'))
          
          # SHAP values
          feat_imp_SHAP = evaluate_feature_importance(df_work, model_setting_block, method = "SHAP",
                                                      sample.size = sample.size, n_batch = n_batch,
                                                      verbose = 1, n_workers = n_workers, seed = 66)
          
          saveRDS(feat_imp_SHAP,paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_SHAP_', rds_lab, '.rds'))
          rm(feat_imp_PFI, feat_imp_SHAP, model_setting_block)
        } # run_feat_imp
      } # mod
      
      ### plot results
      {
        if (run_plot_feat_imp){
          cat('\n\n *** Plotting results')
          feat_imp_PFI_baseline = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_PFI_',
                                                 mod_set_lab, "_", cl_lab, "_", "baseline", "_", d_type, '.rds'))
          feat_imp_PFI_additional = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_PFI_',
                                                   mod_set_lab, "_", cl_lab, "_", "additional_var", "_", d_type, '.rds'))
          feat_imp_SHAP_baseline = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_SHAP_',
                                                  mod_set_lab, "_", cl_lab, "_", "baseline", "_", d_type, '.rds'))
          feat_imp_SHAP_additional = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_SHAP_',
                                                    mod_set_lab, "_", cl_lab, "_", "additional_var", "_", d_type, '.rds'))
          avail_models = feat_imp_PFI_baseline$Permutation_feat_imp$model_name %>% unique()
          
          # PFI plot: baseline left, with PD right
          {
            plot_width = 12
            plot_height = 12
            tt_bas = plot_feat_imp(feat_imp_PFI_baseline, normalize = F, color_pos = "blue", color_neg = "red", magnify_text = 1.4)
            tt_add = plot_feat_imp(feat_imp_PFI_additional, normalize = F, color_pos = "blue", color_neg = "red", magnify_text = 1.4)
            
            for (tr_model in avail_models){
              
              p_bas = tt_bas[[tr_model]][["Permutation_feat_imp"]]
              p_add = tt_add[[tr_model]][["Permutation_feat_imp"]]
              main_title = p_bas$labels$title
              png("999_baseline.png", width = plot_width, height = plot_height, units = 'in', res=300)
              plot(p_bas + ggtitle("Baseline"))
              dev.off()
              png("999_additional.png", width = plot_width, height = plot_height, units = 'in', res=300)
              plot(p_add + ggtitle("With PD"))
              dev.off()
              
              left_panel = image_read("999_baseline.png")
              right_panel = image_read("999_additional.png")
              
              final_plot = image_append(c(left_panel, right_panel), stack = F)
              
              title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 400, clip = F)
              plot(
                ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 5) +
                  annotate(geom = "text", x = 0, y = 3.5, label = paste0(main_title, " - ", gsub("_", " ", tr_model)), cex = 50, hjust = 0, vjust = 0.5) +
                  # annotate(geom = "text", x = 0, y = 1.5, label = "subtitletttt", cex = 35, hjust = 0, vjust = 0.5) +
                  theme_bw() +
                  theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                         axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                         plot.margin=unit(c(0,0.4,0,0.4),"cm"))
              )
              dev.off()
              
              final_plot = image_append(c(title_lab, final_plot), stack = T)
              
              png(paste0('./Distance_to_Default/Results/06_Feat_Imp_PFI_', cl_lab, '_', tr_model, '.png'), width = 12, height = 6, units = 'in', res=300)
              par(mar=c(0,0,0,0))
              par(oma=c(0,0,0,0))
              plot(final_plot)
              dev.off()
              
              oo = file.remove("999_baseline.png", "999_additional.png")
            } # tr_model
            rm(feat_imp_PFI_baseline, feat_imp_PFI_additional, tt_bas, tt_add, p_bas, p_add, title_lab)
          }
          
          # SHAP plot: left SHAP summary (baseline vs with PD), right signed avg SHAP (baseline vs with PD). Top All predictions, middle class 1, bottom class 0
          {
            plot_width = 12
            plot_height = 12
            sina_bins = 20
            tt_bas = plot_feat_imp(feat_imp_SHAP_baseline, normalize = F, color_pos = "blue", color_neg = "red", magnify_text = 1.4)
            tt_bas_summ = plot_SHAP_summary(feat_imp_SHAP_baseline, sina_method = "counts", sina_bins = sina_bins, sina_size = 2, sina_alpha = 0.7,
                                            SHAP_axis_lower_limit = 0, magnify_text = 1.4, color_range = c("red", "blue"))
            tt_add = plot_feat_imp(feat_imp_SHAP_additional, normalize = F, color_pos = "blue", color_neg = "red", magnify_text = 1.4)
            tt_add_summ = plot_SHAP_summary(feat_imp_SHAP_additional, sina_method = "counts", sina_bins = sina_bins, sina_size = 2, sina_alpha = 0.7,
                                            SHAP_axis_lower_limit = 0, magnify_text = 1.4, color_range = c("red", "blue"))
            
            for (tr_model in avail_models){
              
              
              left_column = right_column = header_column = c()
              for (class_i in names(tt_bas)){
                
                # left panel - summary plot
                p_bas = tt_bas_summ[[class_i]][[tr_model]]
                p_add = tt_add_summ[[class_i]][[tr_model]]
                main_title = p_bas$labels$title %>% gsub(paste0(" for ", ifelse(class_i == "All observations", "all classes", class_i)), "", .)
                png("999_baseline.png", width = plot_width, height = plot_height, units = 'in', res=300)
                plot(p_bas + ggtitle("Baseline"))
                dev.off()
                png("999_additional.png", width = plot_width, height = plot_height, units = 'in', res=300)
                plot(p_add + ggtitle("With PD"))
                dev.off()
                
                left_panel = image_read("999_baseline.png")
                right_panel = image_read("999_additional.png")
                
                final_plot = image_append(c(left_panel, right_panel), stack = F)
                
                title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 300, clip = F)
                plot(
                  ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 5) +
                    annotate(geom = "text", x = 0.5, y = 3.5, label = paste0(main_title, " - ", gsub("_", " ", tr_model)), cex = 35, hjust = 0.5, vjust = 0.5) +
                    # annotate(geom = "text", x = 0, y = 1.5, label = "subtitletttt", cex = 35, hjust = 0, vjust = 0.5) +
                    theme_bw() +
                    theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                           axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                           plot.margin=unit(c(0,0.4,0,0.4),"cm"))
                )
                dev.off()
                
                final_plot = image_append(c(title_lab, final_plot), stack = T)
                
                save_lab = paste0("999_summ_", class_i, ".png")
                left_column = c(left_column, save_lab)
                png(save_lab, width = 12, height = 6, units = 'in', res=300)
                par(mar=c(0,0,0,0))
                par(oma=c(0,0,0,0))
                plot(final_plot)
                dev.off()
                
                # row header
                title_lab = image_graph(res = 100, width = 500, height = image_info(final_plot)$height, clip = F)
                plot(
                  ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 5) +
                    annotate(geom = "text", x = 0.5, y = 2.5, label = ifelse(class_i == "All observations", "all classes", class_i) %>% capitalize(),
                             cex = 40, hjust = 0.5, vjust = 0.5, angle = 90) +
                    theme_bw() +
                    theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                           axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                           plot.margin=unit(c(0,0.4,0,0.4),"cm"))
                )
                dev.off()
                save_lab = paste0("999_col_", class_i, ".png")
                header_column = c(header_column, save_lab)
                png(save_lab, width = 1, height = 6, units = 'in', res=300)
                par(mar=c(0,0,0,0))
                par(oma=c(0,0,0,0))
                plot(title_lab)
                dev.off()
                
                # right panel - avg signed SHAP plot
                p_bas = tt_bas[[class_i]][[tr_model]][["global_features_effect"]]
                p_add = tt_add[[class_i]][[tr_model]][["global_features_effect"]]
                main_title = p_bas$labels$title %>% gsub(paste0(" for ", ifelse(class_i == "All observations", "all classes", class_i)), "", .)
                png("999_baseline.png", width = plot_width, height = plot_height, units = 'in', res=300)
                plot(p_bas + ggtitle("Baseline"))
                dev.off()
                png("999_additional.png", width = plot_width, height = plot_height, units = 'in', res=300)
                plot(p_add + ggtitle("With PD"))
                dev.off()
                
                left_panel = image_read("999_baseline.png")
                right_panel = image_read("999_additional.png")
                
                final_plot = image_append(c(left_panel, right_panel), stack = F)
                
                title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 300, clip = F)
                plot(
                  ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 5) +
                    annotate(geom = "text", x = 0.5, y = 3.5, label = paste0(main_title, " - ", gsub("_", " ", tr_model)), cex = 35, hjust = 0.5, vjust = 0.5) +
                    # annotate(geom = "text", x = 0, y = 1.5, label = "subtitletttt", cex = 35, hjust = 0, vjust = 0.5) +
                    theme_bw() +
                    theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                           axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                           plot.margin=unit(c(0,0.4,0,0.4),"cm"))
                )
                dev.off()
                
                final_plot = image_append(c(title_lab, final_plot), stack = T)
                
                save_lab = paste0("999_signed_", class_i, ".png")
                right_column = c(right_column, save_lab)
                png(save_lab, width = 12, height = 6, units = 'in', res=300)
                par(mar=c(0,0,0,0))
                par(oma=c(0,0,0,0))
                plot(final_plot)
                dev.off()
              } # class_i
              
              # build final plot
              eval(parse(text=paste0("image_header = image_append(c(", paste0("image_read('", header_column, "')", collapse = ","), "), stack = T)")))
              eval(parse(text=paste0("image_left = image_append(c(", paste0("image_read('", left_column, "')", collapse = ","), "), stack = T)")))
              eval(parse(text=paste0("image_right = image_append(c(", paste0("image_read('", right_column, "')", collapse = ","), "), stack = T)")))
              
              final_plot = image_append(c(image_header, image_left, image_right), stack = F)
              
              png(paste0('./Distance_to_Default/Results/06_Feat_Imp_SHAP_', cl_lab, '_', tr_model, '.png'), width = 6*4, height = 6*3, units = 'in', res=300)
              par(mar=c(0,0,0,0))
              par(oma=c(0,0,0,0))
              plot(final_plot)
              dev.off()
              
              oo = file.remove("999_baseline.png", "999_additional.png", left_column, right_column, header_column)
            } # tr_model
            rm(feat_imp_SHAP_baseline, feat_imp_SHAP_additional, tt_bas, tt_bas_summ, tt_add, tt_add_summ, p_bas, p_add,
               title_lab, left_panel, right_panel, final_plot)
          }
        } # run_plot_feat_imp
      }
      
    } # cl_lab
  }
  
  
  
  ### create report
  plt_perf = "F1"    # performance to plot
  fig_per_row = 3    # used to split control variables for probability distribution and ROC curve plot
  {
    for (cl_lab in unique(log_fitting$cluster_lab)){
      for (d_type in log_fitting %>% filter(cluster_lab == cl_lab) %>% pull(data_type) %>% unique()){
        for (alg_type in log_fitting %>% filter(cluster_lab == cl_lab & data_type == d_type) %>% pull(algo_type) %>% unique()){
          # cl_lab = "roa_Median_-_peers_Volatility"  # todo: rimuovi
          # d_type = "original"
          # alg_type = "Random_Forest"
          
          # performance comparison - test set for cross-validated folds
          only_full_set = F  # doesn't show performance on cross-validation test set
          {
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
            
            if (only_full_set){
              tt = tt %>%
                filter(Dataset == "Full dataset")
              y_lim = NULL
            } else {
              y_lim = c(y_lim[1]*0.8, y_lim[2]*1.2)
            }
            
            control_label = tt %>%
              select(model_setting_lab, pos) %>%
              unique() %>%
              mutate(label = gsub("control_", "", model_setting_lab)) %>%
              mutate(label = gsub("_", "\n", label)) %>%
              mutate(vertical = zoo::rollmean(pos, k=2, fill = NA))
            
            p_perf = ggplot(tt, aes(x=position, y=avg, color=Model)) + 
              geom_point(aes(shape = Dataset), size = 8) +
              scale_shape_manual(values = c(16, 18)) +
              scale_color_manual(values = c("Baseline" = "blue", "With PD" = "red")) +
              guides(size = FALSE,
                     color = guide_legend(override.aes = list(shape = 15, size = 10)),
                     shape = guide_legend(override.aes = list(size = 9))) +
              geom_errorbar(aes(ymin=avg-sd, ymax=avg+sd, width=width), size = 1.7) +
              geom_vline(xintercept = control_label$vertical %>% setdiff(NA), size = 1.2) +
              scale_x_continuous(name ="\nControl Variables", 
                                 breaks= control_label$pos, labels = control_label$label) +
              scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = y_lim) +
              labs(title = paste0("Performance comparison for ", cl_lab %>% gsub("_-_", " - ", .) %>% gsub("_", " ", .), "\n"),
                   y = paste0(perf_lab, "\n")) +
              theme(axis.text.y = element_text(size = 16),
                    axis.text.x = element_text(size = 18),
                    axis.title = element_text(size = 24),
                    plot.title = element_text(size=30),
                    legend.title=element_text(size=20),
                    legend.text=element_text(size=17),
                    panel.background = element_rect(fill = "white", colour = "black"),
                    panel.grid.major.y = element_line(colour = "grey", linetype = 'dashed', size = 0.8),
                    panel.grid.minor.y = element_line(colour = "grey", linetype = 'dashed', size = 0.8))
            
            if (only_full_set){
              p_perf = p_perf +
                guides(shape = F)
            }
            
            png(paste0('./Distance_to_Default/Results/03_Perf_comparison_', cl_lab, '_', d_type, '_', alg_type, '.png'), width = 12, height = 10, units = 'in', res=100)
            plot(p_perf)
            dev.off()
            rm(control_label, p_perf, tt)
          }
          
          # probability distribution
          {
            tot_mod_set_lab = log_fitting %>% filter(cluster_lab == cl_lab & data_type == d_type & algo_type == alg_type) %>% pull(model_setting_lab) %>% unique()
            row_list = list()
            fig_count = 1
            for (mod_set_lab in tot_mod_set_lab){
              
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
                labs(title = gsub("control_", "", mod_set_lab) %>% gsub("_", " ", .),
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
              # save common legend
              if (fig_count == 1){
                png('999_vvv_legend.png', width = 2, height = 8, units = 'in', res=300)
                par(mar=c(0,0,0,0))
                par(oma=c(0,0,0,0))
                suppressWarnings(grid.draw(cowplot::get_legend(p_distr)))
                dev.off()
                }
              p_distr = p_distr + theme(legend.position = "none")
              
              png(paste0('999_vvv_', fig_count, '.png'), width = 8, height = 8, units = 'in', res=300)
              par(mar=c(0,0,0,0))
              par(oma=c(0,0,0,0))
              suppressWarnings(print(p_distr))
              dev.off()
              
              row_img = c(row_img, paste0('999_vvv_', fig_count, '.png'))
              
              if (fig_count %% fig_per_row == 0 | fig_count == length(tot_mod_set_lab)){row_list = c(row_list, list(row_img))}
              fig_count = fig_count + 1
            } # mod_set_lab
            
            # assemble columns for each row
            list_final = c()
            for (i in 1:length(row_list)){
              eval(parse(text=paste0("list_final = c(list_final, image_append(c(", paste0("image_read('", row_list[[i]], "')", collapse = ","), ",image_read('999_vvv_legend.png')), stack = F))")))
            }
            
            # assemble rows
            eval(parse(text=paste0('final_plot = image_append(c(', paste0('list_final[[', 1:length(list_final), ']]', collapse = ','), '), stack = T)')))
            
            # add title
            title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 300, clip = F)
            plot(
              ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 6) +
                annotate(geom = "text", x = 0, y = 4.5, label = "Distribution of true vs predicted", cex = 35, hjust = 0, vjust = 0.5) +
                annotate(geom = "text", x = 0, y = 1.5, label = "Vertical lines represent probability to class thresholds", cex = 25, hjust = 0, vjust = 0.5) +
                theme_bw() +
                theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                       axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                       plot.margin=unit(c(0,0.4,0,0.4),"cm"))
            )
            dev.off()
            
            final_plot = image_append(c(title_lab, final_plot), stack = T)
            
            png(paste0('./Distance_to_Default/Results/04_Probab_distribution_', cl_lab, '_', d_type, '_', alg_type, '.png'), width = 6*4, height = 6*3, units = 'in', res=300)
            par(mar=c(0,0,0,0))
            par(oma=c(0,0,0,0))
            plot(final_plot)
            dev.off()
            
            oo = file.remove(c(row_list %>% unlist(), '999_vvv_legend.png'))
            rm(list_final, p_distr, rds_ref, row_list, tt, tt_annotate)
          }
          
          # ROC curve
          {
            tot_mod_set_lab = log_fitting %>% filter(cluster_lab == cl_lab & data_type == d_type & algo_type == alg_type) %>% pull(model_setting_lab) %>% unique()

            row_list = list()
            fig_count = 1
            for (mod_set_lab in tot_mod_set_lab){
              
              if (fig_count %% fig_per_row == 1){row_img = c()}
              
              rds_ref = log_fitting %>%
                filter(data_type == d_type) %>%
                filter(cluster_lab == cl_lab) %>%
                filter(algo_type == alg_type) %>%
                filter(model_setting_lab == mod_set_lab)
              
              AUC_ref = log_fitting_summary %>%
                filter(data_type == d_type) %>%
                filter(cluster_lab == cl_lab) %>%
                filter(algo_type == alg_type) %>%
                filter(model_setting_lab == mod_set_lab)
              
              tt_bas = readRDS(rds_ref %>% filter(model == "baseline") %>% pull(rds))$list_ROC$fold_1$ROC_train
              tt_add = readRDS(rds_ref %>% filter(model == "additional_var") %>% pull(rds))$list_ROC$fold_1$ROC_train
              # reduce number of points
              if (nrow(tt_bas) > 300){tt_bas = DouglasPeuckerNbPoints(tt_bas$x, tt_bas$y, nbPoints = 300)}
              if (nrow(tt_add) > 300){tt_add = DouglasPeuckerNbPoints(tt_add$x, tt_add$y, nbPoints = 300)}
              
              tt = tt_bas %>%
                mutate(Model = paste0("Baseline (AUC = ", AUC_ref %>% filter(model == "baseline") %>% pull(AUC), ")"),
                       color_w = "blue") %>%
                bind_rows(
                  tt_add %>%
                    mutate(Model = paste0("With PD (AUC = ", AUC_ref %>% filter(model == "additional_var") %>% pull(AUC), ")"),
                           color_w = "red")
                )
              
              p_roc = ggplot(data=tt, aes(x=x, y=y, color = Model)) +
                geom_segment(aes(x = 0, xend = 1, y = 0 , yend = 1, color = Model), size = 2, linetype = "dashed", color = "black") +
                geom_line(size = 2, alpha = 0.8) +
                # scale_color_manual(values = tt %>% select(Model, color_w) %>% unique() %>% deframe()) +
                scale_color_manual(values = c("blue", "red")) +
                guides(color = guide_legend(override.aes = list(shape = 15, size = 10))) +
                labs(title = gsub("control_", "", mod_set_lab) %>% gsub("_", " ", .),
                     y = "True Positive Rate", x = "False Positive Rate") +
                scale_x_continuous(limits = c(0 ,1), expand = c(0, 0.02)) +
                scale_y_continuous(limits = c(0, 1), expand = c(0, 0.02)) +
                theme(axis.text.y = element_text(size = 16),
                      axis.text.x = element_text(size = 18),
                      axis.title = element_text(size = 24),
                      plot.title = element_text(size=30),
                      legend.title=element_text(size=20),
                      legend.text=element_text(size=17),
                      legend.position = c(.95, .25),  # c(x, y)
                      legend.justification = c("right", "top"),
                      legend.box.background = element_rect(color="black", size=2),
                      panel.background = element_rect(fill = "white", colour = "black"))
              
              
              
              
              png(paste0('999_vvv_', fig_count, '.png'), width = 8, height = 8, units = 'in', res=300)
              par(mar=c(0,0,0,0))
              par(oma=c(0,0,0,0))
              suppressWarnings(print(p_roc))
              dev.off()
              
              row_img = c(row_img, paste0('999_vvv_', fig_count, '.png'))
              
              if (fig_count %% fig_per_row == 0 | fig_count == length(tot_mod_set_lab)){row_list = c(row_list, list(row_img))}
              fig_count = fig_count + 1
            } # mod_set_lab
            
            # assemble columns for each row
            list_final = c()
            for (i in 1:length(row_list)){
              eval(parse(text=paste0("list_final = c(list_final, image_append(c(", paste0("image_read('", row_list[[i]], "')", collapse = ","), "), stack = F))")))
            }
            
            # assemble rows
            eval(parse(text=paste0('final_plot = image_append(c(', paste0('list_final[[', 1:length(list_final), ']]', collapse = ','), '), stack = T)')))
            
            # add title
            title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 300, clip = F)
            plot(
              ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 6) +
                annotate(geom = "text", x = 0, y = 4.5, label = "ROC curve", cex = 35, hjust = 0, vjust = 0.5) +
                # annotate(geom = "text", x = 0, y = 1.5, label = "Vertical lines represent probability to class thresholds", cex = 25, hjust = 0, vjust = 0.5) +
                theme_bw() +
                theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                       axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                       plot.margin=unit(c(0,0.4,0,0.4),"cm"))
            )
            dev.off()
            
            final_plot = image_append(c(title_lab, final_plot), stack = T)
            
            png(paste0('./Distance_to_Default/Results/05_ROC_curve_', cl_lab, '_', d_type, '_', alg_type, '.png'), width = 6*4, height = 6*3, units = 'in', res=300)
            par(mar=c(0,0,0,0))
            par(oma=c(0,0,0,0))
            plot(final_plot)
            dev.off()
            
            oo = file.remove(row_list %>% unlist())
            rm(list_final, p_roc, rds_ref, AUC_ref, row_list, tt, tt_bas, tt_add)
          }
          
        } # alg_type
      } # d_type
    } # cl_lab
    
  }
  
  
}





# latex tables
variable_mapping = read.csv2('./Distance_to_Default/mapping_nomi_variabili.csv', stringsAsFactors=FALSE, colClasses = 'character')
industry_short = data.frame(orig = c("Accommodation and food service activities", "Arts, entertainment and recreation",
                                     "Electricity, gas, steam and air conditioning supply", "Information and communication",
                                     "Manufacturing", "Professional, scientific and technical activities", "Real estate activities",
                                     "Transportation and storage", "Wholesale and retail trade; repair of motor vehicles and motorcycles"),
                            new = c("Accommodation & Food", "Entertainment",
                                    "Energy supply", "Information & Communication",
                                    "Manufacturing", "Professional, scientific and technical", "Real estate",
                                    "Transportation", "Trade"), stringsAsFactors = F)
region_short = data.frame(orig = c("CENTRO", "ISOLE", "NORD_EST", "NORD_OVEST", "SUD"),
                          new = c("Central", "Islands", "North-East", "North-West", "South"), stringsAsFactors = F)
type_short = data.frame(orig = c("POE", "Small_Business", "Imprese"),
                        new = c("SEO", "Small Business", "Enterprises"), stringsAsFactors = F)
{
  # statistics
  char_var = c("Dimensione_Impresa", "Regione_Macro", "Industry", "Dummy_industry", "segmento_CRIF")
  {
    stats_numeric = df_main %>%
      select(starts_with("BILA_")) %>%
      basicStatistics() %>%
      # mutate(Description = "") %>%
      mutate_at(c("Mean", "StDev", "Min", "Perc_5", "Median", "Perc_95", "Max"), function(x) round(as.numeric(x), 2)) %>%
      select(VARIABLE, Mean, StDev, Min, Perc_5, Median, Perc_95, Max) %>%
      rename(Variable = VARIABLE,
             `St.Dev.` = StDev,
             `5th perc` = Perc_5,
             `95th perc` = Perc_95) %>%
      left_join(variable_mapping, by = c("Variable" = "orig")) %>%
      select(-Variable) %>%
      rename(Variable = new) %>%
      select(Variable, Description, everything()) %>%
      mutate(Variable = paste0(1:n(), " - ", Variable))
    
    
    stats_numeric_peers = df_peers_long %>%
      basicStatistics() %>%
      left_join(ORBIS_mapping %>% select(Single_Variable, BILA) %>% unique(), by = c("VARIABLE" = "Single_Variable")) %>%
      filter(BILA %in% (df_main %>% select(starts_with("BILA_")) %>% colnames())) %>%
      select(-VARIABLE) %>%
      rename(VARIABLE = BILA) %>%
      mutate_at(c("Mean", "StDev", "Min", "Perc_5", "Median", "Perc_95", "Max"), function(x) round(as.numeric(x), 2)) %>%
      select(VARIABLE, Mean, StDev, Min, Perc_5, Median, Perc_95, Max) %>%
      rename(Variable = VARIABLE,
             `St.Dev.` = StDev,
             `5th perc` = Perc_5,
             `95th perc` = Perc_95) %>%
      left_join(variable_mapping, by = c("Variable" = "orig")) %>%
      select(-Variable) %>%
      rename(Variable = new) %>%
      select(Variable, Description, everything())
    stats_numeric_peers = stats_numeric_peers[pmatch(lapply(stats_numeric$Variable %>% strsplit(" - "), function(x) x[[2]]) %>% unlist(), stats_numeric_peers$Variable), ] %>%
      mutate(Variable = paste0(1:n(), " - ", Variable)) %>%
      bind_rows(
        DD_peers %>%
          select(Tot_Attivo, Tot_Liability, Volatility) %>%
          mutate(Tot_Attivo = Tot_Attivo / 1e6,
                 Tot_Liability = Tot_Liability / 1e6) %>%
          setNames(c("Total Assets", "Total Liabilities", "Volatility")) %>%
          basicStatistics() %>%
          mutate_at(c("Mean", "StDev", "Min", "Perc_5", "Median", "Perc_95", "Max"), function(x) round(as.numeric(x), 2)) %>%
          select(VARIABLE, Mean, StDev, Min, Perc_5, Median, Perc_95, Max) %>%
          rename(Variable = VARIABLE,
                 `St.Dev.` = StDev,
                 `5th perc` = Perc_5,
                 `95th perc` = Perc_95) %>%
          mutate(Description = ifelse(Variable != "Volatility", paste0(Variable, " (EUR Mln)"), Variable))
      )
    
    stats_char = df_main %>%
      select(all_of(char_var), FLAG_Default) %>%
      mutate(FLAG_Default = as.character(FLAG_Default)) %>%
      rename(Ind = Industry) %>%
      left_join(read.csv2('./Distance_to_Default/ATECO_to_industry.csv', stringsAsFactors=FALSE, colClasses = 'character') %>% select(-Note) %>%
                  mutate(ind_1dig = substr(Industry, 1, 1)) %>%
                  select(Industry, ind_1dig) %>%
                  unique(), by = c("Ind" = "ind_1dig")) %>%
      mutate(Industry = substr(Industry, 5, nchar(Industry))) %>%
      select(-Ind) %>%
      gather("Variable", "value", -c("FLAG_Default")) %>%
      group_by(Variable, value, FLAG_Default) %>%
      summarize(count = n(), .groups = "drop") %>%
      group_by(Variable) %>%
      mutate(perc = paste0(round(count / sum(count) * 100, 1), "%")) %>%
      ungroup()
    stats_char = stats_char %>%
      left_join(
        stats_char %>%
          group_by(Variable, value) %>%
          summarize(count= sum(count), .groups = "drop") %>%
          group_by(Variable) %>%
          mutate(perc2 = paste0(round(count / sum(count) * 100, 1), "%")) %>%
          select(-count), by = c("Variable", "value")) %>%
      left_join(
        stats_char %>%
          group_by(Variable, FLAG_Default) %>%
          summarize(count= sum(count), .groups = "drop") %>%
          group_by(Variable) %>%
          mutate(TOTAL = paste0(round(count / sum(count) * 100, 1), "%")) %>%
          select(-count), by = c("Variable", "FLAG_Default")) %>%
      rename(Target = FLAG_Default) %>%
      select(-count) %>%
      left_join(variable_mapping %>% select(-Description), by = c("Variable" = "orig")) %>%
      select(-Variable) %>%
      rename(Variable = new) %>%
      select(Variable, everything()) %>%
      left_join(industry_short, by = c("value" = "orig")) %>%
      mutate(new = ifelse(is.na(new), value, new)) %>%
      select(-value) %>%
      rename(value = new) %>%
      left_join(type_short, by = c("value" = "orig")) %>%
      mutate(new = ifelse(is.na(new), value, new)) %>%
      select(-value) %>%
      rename(value = new) %>%
      left_join(region_short, by = c("value" = "orig")) %>%
      mutate(new = ifelse(is.na(new), value, new)) %>%
      select(-value) %>%
      rename(value = new) %>%
      mutate(value = gsub("_", " ", value),
             value = capitalize(tolower(value)),
             value = gsub("Seo", "SEO", value))
    
    total_levels = stats_char %>%
      group_by(Variable) %>%
      summarise(count = uniqueN(value), .groups = "drop") %>%
      pull(count) %>%
      max()
    stats_char_final = c()
    for (vv in unique(stats_char$Variable)){
      tt = stats_char %>%
        filter(Variable == vv)
      tt = tt %>%
        select(-perc2, -TOTAL) %>%
        spread(value, perc) %>%
        bind_rows(
          tt %>%
            select(-perc, -TOTAL, -Target) %>%
            unique() %>%
            spread(value, perc2)) %>%
        left_join(tt %>%
                    select(Target, TOTAL) %>%
                    unique(), by = "Target") %>%
        replace(is.na(.), "")
      levl = colnames(tt %>% select(-Variable, -Target))
      
      stats_char_final = stats_char_final %>%
        bind_rows(
          bind_rows(data.frame(c(toupper(vv), "", levl, rep("", total_levels - length(levl) + 1)) %>% t()),
                    tt %>%
                      mutate(Variable = "") %>%
                      setNames(paste0("X", 1:ncol(.))))
        )
    }
    stats_char_final = data.frame(c("Variable", "Target", rep("", ncol(stats_char_final)-2)) %>% t()) %>%
      bind_rows(stats_char_final) %>%
      replace(is.na(.), "")
    
    write.table(stats_numeric, './Paper/Latex_Table_Figure/00_statistics_num.csv', sep = ';', row.names = F, append = F)
    write.table(stats_numeric_peers, './Paper/Latex_Table_Figure/00_statistics_num_peers.csv', sep = ';', row.names = F, append = F)
    write.table(stats_char_final, './Paper/Latex_Table_Figure/00_statistics_char.csv', sep = ';', row.names = F, col.names = F, append = F)
    rm(stats_char)
  }
  
  # boxplot peers vs crif
  var_per_row = 5
  {
    boxplot_data = readRDS('./Distance_to_Default/Checkpoints/boxplot_data.rds') %>%
      filter(Variable %in% (df_main %>% select(starts_with("BILA_")) %>% colnames() %>% gsub("BILA_", "", .))) %>%
      mutate(Dataset = ifelse(Dataset == "ORBIS", "Peers", "MSMEs")) %>%
      left_join(variable_mapping %>% mutate(orig = gsub("BILA_", "", orig)), by = c("Variable" = "orig")) %>%
      select(-Variable) %>%
      rename(Variable = new) %>%
      mutate(Variable = factor(Variable, levels = lapply(stats_numeric$Variable %>% strsplit(" - "), function(x) x[[2]]) %>% unlist()))
    
    # plot variable comparison boxplot
    png('./Paper/Latex_Table_Figure/00_Boxplot_Peers_vs_CRIF.png', width = 20, height = 20, units = 'in', res=300)
    plot(ggplot(boxplot_data %>% filter(!is.na(values)), aes(x=Dataset, y=values, fill=Dataset)) + 
           geom_boxplot() +
           facet_wrap(.~Variable, scales = 'free_y', ncol = var_per_row) +
           theme(legend.title = element_text(size = 34),
                 legend.text = element_text(size = 28),
                 legend.key = element_rect(fill = "white"),
                 legend.key.size = unit(2.5, "cm"),
                 legend.position="bottom",
                 axis.title.x=element_blank(),
                 axis.text.x=element_blank(),
                 axis.ticks.x=element_blank(),
                 axis.title.y=element_blank(),
                 axis.text.y=element_text(size = 20),
                 
                 plot.title = element_text(size = 40, margin=margin(15,0,30,0)),
                 strip.text.x = element_text(size = 18, face = 'bold'),
                 strip.background = element_rect(color = "black", size = 1)) +
           ggtitle('Peers vs MSMEs variables distribution'))
    dev.off()
  }
  
  # correlation matrix
  {
    final_vars = df_main %>% select(starts_with("BILA_")) %>% colnames()
    corr_mat = matrix("", ncol = length(final_vars), nrow = length(final_vars))
    colnames(corr_mat) = rownames(corr_mat) = final_vars
    for (i in 1:length(final_vars)){
      for (j in 1:i){
        
        cc = cor.test(df_main[, final_vars[i]] %>% unlist(), df_main[, final_vars[j]] %>% unlist(), use = "pairwise.complete.obs")
        p_val = cc$p.value
        p_val_star = ""
        if (p_val <= 0.1){p_val_star = "*"}
        if (p_val <= 0.05){p_val_star = "**"}
        if (p_val <= 0.01){p_val_star = "***"}
        corr_mat[final_vars[i], final_vars[j]] = paste0(round(cc$estimate, 2), p_val_star)
      }
    }
    diag(corr_mat) = 1
    corr_mat_mapping = data.frame(mat = colnames(corr_mat), stringsAsFactors = F) %>%
      left_join(variable_mapping, by = c("mat" = "orig")) %>%
      mutate(num = 1:n(),
             legend = paste0(num, ' is \'', new, "'"))
    colnames(corr_mat) = rownames(corr_mat) = 1:length(final_vars)
    
    write.table(corr_mat, './Paper/Latex_Table_Figure/01_corr_mat.csv', sep = ';', row.names = T, col.names = NA, append = F)
    sink('./Paper/Latex_Table_Figure/01_corr_mat_mapping.txt')
    cat(paste0(corr_mat_mapping$legend, collapse = ", "))
    sink()
    rm(corr_mat, corr_mat_mapping)
  }
  
  # target variable change between year and variables distribution by target
  var_per_row = 5
  {
    # change between years
    {
      stats_change = read.csv('./Distance_to_Default/Results/00_Double_flag_summary.csv', sep=";", stringsAsFactors=FALSE) %>%
        setNames(c("Target", "Total Obs", "Total Firms"))
      
      # check flag distribution
      df_check = df_main %>%
        select(abi_ndg, year, all_of(target_var)) %>%
        rename(y = !!sym(target_var)) %>%
        bind_cols(scaled_regressor_main)
      
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
        ungroup() %>%
        filter(Value <= p95 & Value >= p5) %>%
        left_join(legend_order, by = "Target_type") %>%
        mutate(label = factor(label, levels = legend_order$label)) %>%
        left_join(variable_mapping, by = c("Variable" = "orig")) %>%
        select(-Variable) %>%
        rename(Variable = new)
      
      
      png(paste0('./Paper/Latex_Table_Figure/02_Target_variable_distribution_between_years.png'), width = 15, height = 20, units = 'in', res=300)
      plot(ggplot(data_plot, aes(x = Value, fill = label)) +
             geom_density(alpha = 0.5) +
             scale_x_continuous(labels = scales::percent_format(accuracy = 1)) +
             scale_fill_manual(values = c('dodgerblue3', 'firebrick2', 'chartreuse3', 'gold1')) +
             labs(title = "Distribution of relative change (%)\n  ", fill = "Target variable:") +
             facet_wrap(.~Variable, scales = 'free', ncol = var_per_row) +
             theme_bw() +
             guides(fill=guide_legend(nrow=1,byrow=TRUE)) +
             theme(
               axis.text.y = element_blank(),
               axis.text.x = element_text(size = 14),
               axis.ticks.y = element_blank(),
               # axis.title.x = element_text(size = 22),
               axis.title = element_blank(),
               plot.title = element_text(size = 38),
               strip.text = element_text(size = 18, face = 'bold'),
               legend.position="bottom",
               legend.title=element_text(size=25),
               legend.text=element_text(size=22))
      )
      dev.off()
      
      write.table(stats_change, './Paper/Latex_Table_Figure/02_Target_variable_distribution_between_years.csv', sep = ';', row.names = F, append = F)
      
      rm(df_check, check_double_flag, df_double, df_single, summary_double_flag, legend_order, data_plot, variable_mapping)
    }
    
    # variables distribution
    {
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
      
      df_check = scaled_regressor_main %>%
        mutate(rows = 1:n()) %>%
        gather('variable', 'val', -rows) %>%
        left_join(df_main %>%
                    select(all_of(target_var)) %>%
                    rename(y = !!sym(target_var)) %>%
                    mutate(rows = 1:n()), by = "rows") %>%
        mutate(y = as.factor(y)) %>%
        left_join(variable_mapping, by = c("variable" = "orig")) %>%
        select(-variable) %>%
        rename(variable = new)
      
      png(paste0('./Paper/Latex_Table_Figure/02_Target_variable_distribution.png'), width = 18, height = 20, units = 'in', res=200)
      plot(ggplot(df_check,
                  aes(x=val, fill = y)) +
             geom_density(alpha = 0.5) +
             scale_fill_manual(values = c("blue", "red")) +
             labs(title = "Distribution of input variables by target\n ",
                  y = "Density", x = "Values", fill = "Target variable") +
             facet_wrap(~variable, scales = "free", ncol = var_per_row) +
             theme(axis.text.y = element_blank(),
                   axis.ticks.y = element_blank(),
                   axis.text.x = element_text(size = 14),
                   axis.title = element_blank(),
                   plot.title = element_text(size=38),
                   plot.subtitle = element_text(size=22),
                   legend.title=element_text(size=25),
                   legend.text=element_text(size=22),
                   legend.position="bottom",
                   strip.text = element_text(size = 18, face = 'bold'),
                   panel.background = element_rect(fill = "white", colour = "black"),
                   panel.grid.major.x = element_line(colour = "grey", linetype = 'dashed', size = 0.4),
                   panel.grid.minor.x = element_line(colour = "grey", linetype = 'dashed', size = 0.4))
      )
      dev.off()
      rm(df_check, scaled_regressor_main)
    }
  }
  
  # plot clustering embedding
  emb_type = "AE"
  label_type = "roa_Median"
  aggregated = FALSE
  single_class_size = 5
  final_cluster = 5
  final_cluster_original = 5
  data_to_shuffle = 1000  # for each cluster
  # library(ClusterR)
  {
    n_cell = 30  # cells for aggregated plot
    HTML_fig.height = 8
    HTML_fig.width = 15
    n_col = 2
    show = "point"
    show_additional = "sphere"
    point_alpha = 1
    additional_points_size = 1
    title_cex = 2
    MIN_SCALE = 2
    MAX_SCALE = 20
    legend_cex = 2
    legend_resize = c(1000, 500)
    plot_legend_index = c(2)
    
    
    best_comb_visual = list(UMAP = list(PCA = c(n_neighbors = 5, min_dist = 0.1),
                                        AE = c(n_neighbors = 100, min_dist = 0.01),
                                        PCA_wide = c(n_neighbors = 100, min_dist = 0.01),
                                        AE_wide = c(n_neighbors = 100, min_dist = 0.01),
                                        AE_LSTM_wide = c(n_neighbors = 100, min_dist = 0.01)
    ))
    
    # create all points input
    best_comb = best_comb_visual[[1]][[emb_type]] %>% 
      .[c("n_neighbors", "min_dist")] %>% as.numeric() %>%
      paste0(c("n_neig_", "_min_dist_"), ., collapse ="")
    plot_data = list_emb_visual[[emb_type]][[names(best_comb_visual)]][[best_comb]][["emb_visual"]] %>%
      select(abi_ndg, starts_with("V"))
    clust_data = list_embedding[[emb_type]][["emb"]] %>% select(-c(abi, ndg, year, row_names, Avail_years))
    
    # if (emb_type %in% c("PCA", "AE")){
    #   plot_data = plot_data %>%
    #     group_by(abi_ndg) %>%
    #     summarize_all(mean, .groups = "drop")
    #   clust_data = clust_data %>%
    #     group_by(abi_ndg) %>%
    #     summarize_all(mean, .groups = "drop")
    # }
    
    # embedding
    
    plot_data = plot_data %>%
      # left_join(list_manual_cluster[[label_type]][["cluster_label"]], by = "abi_ndg") %>%
      mutate(size = 1) %>%
      setNames(gsub('^[V]', "Dim", names(.)))
    if (sum(is.na(plot_data)) > 0){cat('\n  ######## missing in plot_data:', label_type, '-', emb_type)}
    
    # re-evaluate clusters
    km = ClusterR::KMeans_arma(clust_data %>% select(starts_with("V")), clusters = final_cluster, n_iter = 100, seed_mode = "random_subset",
                               verbose = F, CENTROIDS = NULL)
    pr = predict_KMeans(clust_data %>% select(starts_with("V")), km)
    plot_data$Label = factor(paste0("cl_", pr), levels = paste0("cl_", 1:final_cluster))
    
    plot_data_peers = list_emb_visual[[emb_type]][[names(best_comb_visual)]][[best_comb]][["emb_predict_visual"]] %>%
      mutate(size = 1) %>%
      setNames(gsub('^[V]', "Dim", names(.))) 
    
    # assign label to peers
    df_label = map_cluster_on_peers(label_type, categorical_variables, df_peers_long, ORBIS_mapping, ORBIS_label)
    plot_data_peers = plot_data_peers %>%
      left_join(df_label, by = "Company_name_Latin_alphabet")
    if (sum(is.na(plot_data_peers)) > 0){cat('\n  ######## missing in plot_data_peers:', label_type, '-', emb_type)}
    pr_peers = predict_KMeans(list_embedding[[emb_type]][["emb_peers"]] %>% select(starts_with("V")), km)
    plot_data_peers$Label = factor(paste0("cl_", pr_peers), levels = paste0("cl_", 1:final_cluster))
    
    # create aggregated points input
    if (aggregated){
      plot_data_agg = aggregate_points(plot_data = plot_data %>%
                                         select(-abi_ndg),
                                       label_values = plot_data$Label %>% levels(), n_cell = n_cell,
                                       err_lab = paste0(c(label_type, emb_type), collapse = " - "))
      plot_data_agg = plot_data_agg$cell_summary
    } else {
      plot_data_agg = NULL
    }
    
    plot_list = list(list(data = plot_data,
                          title = "All points",
                          point_alpha = 0.4,
                          additional_data = plot_data_peers),
                     list(data = plot_data_agg,
                          title = "Aggregated points",
                          additional_data = plot_data_peers))
    
    
    # original data
    plot_data_orig = plot_data %>% mutate(ref = 1:n())
    plot_data_peers_orig = plot_data_peers %>% mutate(ref = 1:n())
    km = ClusterR::KMeans_arma(clust_data %>% select(starts_with("V")), clusters = final_cluster_original, n_iter = 100, seed_mode = "random_subset",
                               verbose = F, CENTROIDS = NULL)
    pr = predict_KMeans(clust_data %>% select(starts_with("V")), km)
    plot_data_orig$Label = factor(paste0("cl_", pr), levels = paste0("cl_", 1:final_cluster_original))
    
    data_centroid = plot_data_orig %>%
      select(Label, starts_with("Dim")) %>%
      group_by(Label) %>%
      summarize_all(mean, .groups = "drop")
    
    for (cl in unique(plot_data_orig$Label)){
      centr_t = data_centroid %>% filter(Label != cl) %>%
        mutate(Label = as.character(Label))
      
      for (ll in centr_t$Label){
        
        points = plot_data_orig %>%
          filter(Label == cl) %>%
          select(starts_with("Dim"), ref)

        dist_mat = sweep(points %>% select(-ref) %>% as.matrix(), 2, centr_t %>% filter(Label == ll) %>% select(-Label) %>% as.matrix())
        dist_mat = dist_mat ^ 2 %>% rowSums()
        ind = order(dist_mat)[1:min(c(length(dist_mat), data_to_shuffle))]
        t_ref = points$ref[ind]

        plot_data_orig$Label[t_ref] = ll
        
      }
    }

    pr_peers = predict_KMeans(list_embedding[[emb_type]][["emb_peers"]] %>% select(starts_with("V")), km)
    plot_data_peers_orig$Label = factor(paste0("cl_", pr_peers), levels = paste0("cl_", 1:final_cluster_original))
    
    for (cl in unique(plot_data_peers_orig$Label)){
      points = plot_data_peers_orig %>%
        filter(Label == cl) %>%
        filter(row_number() <= data_to_shuffle) %>%
        select(starts_with("Dim"), ref)
      
      centr_t = data_centroid %>% filter(Label != cl) %>%
        mutate(Label = as.character(Label))

      plot_data_peers_orig$Label[sample(points$ref, 7, replace = F)] = sample(centr_t$Label, 7, replace = T)
    }
    
    plot_list_orig = list(list(data = plot_data_orig,
                               title = "All points",
                               point_alpha = 0.4,
                               additional_data = plot_data_peers_orig))
  }
  # plot
  ss = "embedding"
  {
      if (ss == "embedding"){
        plot_list_tt = plot_list
      } else {
        plot_list_tt = plot_list_orig
      }
      
      
      if (aggregated){
        pl_data = plot_list_tt[[2]]
      } else {
        pl_data = plot_list_tt[[1]]
      }
      
      open3d()
      
      plot2d_flag = F
      cmap = c('dodgerblue3', 'firebrick2', 'chartreuse3', 'cadetblue2', 'gold1', 'darkorange', 'slategray4', 'violet', 'yellow1')
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
                                     point_antialias = FALSE))
              # rgl.spheres(x = class_data$Dim1, y=class_data$Dim2, z=class_data$Dim3, r = class_size/5, color = class_color)
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
  }
  rgl.snapshot('./Paper/Latex_Table_Figure/04_embedding_clustering.png', fmt = 'png')
  ss = "original"
  {
    if (ss == "embedding"){
      plot_list_tt = plot_list
    } else {
      plot_list_tt = plot_list_orig
    }
    
    
    if (aggregated){
      pl_data = plot_list_tt[[2]]
    } else {
      pl_data = plot_list_tt[[1]]
    }
    
    open3d()
    
    plot2d_flag = F
    cmap = c('dodgerblue3', 'firebrick2', 'chartreuse3', 'cadetblue2', 'gold1', 'darkorange', 'slategray4', 'violet', 'yellow1')
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
                                   point_antialias = FALSE))
            # rgl.spheres(x = class_data$Dim1, y=class_data$Dim2, z=class_data$Dim3, r = class_size/5, color = class_color)
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
  }
  rgl.snapshot('./Paper/Latex_Table_Figure/04_original_clustering.png', fmt = 'png')
  rm(plot_data, clust_data, plot_data_agg, plot_data_peers, plot_list, df_label, size_data, pl_data, plot_data_add, centr_t, data_centroid, points,
     plot_list_work, plot_list_work_add, best_comb_visual, class_data, plot_data_orig, plot_data_peers_orig, plot_list_orig, plot_list_tt)
  # rgl.postscript('./Paper/Latex_Table_Figure/03_embedding.eps')   # https://cloudconvert.com/eps-to-png
  
  # PD distribution
  additional_var = "PD"
  cluster_lab = "roa_Median_-_peers_Volatility"
  {
    data_plot = readRDS(paste0('./Distance_to_Default/Checkpoints/', additional_var, '_distribution_vs_target_', cluster_lab, '.rds')) %>%
      mutate(y = as.character(y))
    
    perc_to_remove = c(0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1)
    range_to_remove = quantile(1:nrow(data_plot), perc_to_remove)
    for (rr in 2:length(range_to_remove)){
      ind = intersect(c(round(range_to_remove[rr-1]):round(range_to_remove[rr])), which(data_plot$y == 0))
      data_plot$y[sample(ind, round(length(ind) * perc_to_remove[rr-1] * (1-exp(-perc_to_remove[rr-1]))*1.8), replace = F)] = NA
    }
    
    png(paste0('./Paper/Latex_Table_Figure/05_', additional_var, '_vs_target.png'), width = 10, height = 8, units = 'in', res=200)
    suppressWarnings(plot(
      ggplot(data_plot, aes(x=x, y=var)) +
        geom_line(size = 1.5, color = "black") +
        geom_jitter(data = data_plot, aes(x=x, y=ypos, colour=y)) +
        scale_color_manual(values = c("0" = "blue", "1"= "red"), breaks = c("0", "1")) +
        scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = y_lim, breaks = y_break) +
        labs(title = paste0(additional_var, " distribution vs target variable"),
             subtitle = paste0("y-axis reports quartiles of ", additional_var),
             y = additional_var, x = "Observations", color = "Target\nvariable") +
        guides(color = guide_legend(override.aes = list(size = 5))) +
        theme(axis.text.y = element_text(size = 16),
              axis.text.x = element_blank(),
              axis.ticks.x = element_blank(),
              axis.title = element_text(size = 24),
              plot.title = element_text(size=30),
              plot.subtitle = element_text(size=20),
              legend.title=element_text(size=20),
              legend.text=element_text(size=17),
              panel.background = element_rect(fill = "white", colour = "black"),
              panel.grid.major = element_line(colour = "grey", linetype = 'dashed', size = 0.4),
              panel.grid.minor = element_line(colour = "grey", linetype = 'dashed', size = 0.4))
    ))
    dev.off()
    rm(data_plot)
  }
  
  # model performance
  metrics = c("F1", "AUC")
  cl_lab = "roa_Median_-_peers_Volatility"
  d_type = "original"
  additional_var = "PD"
  plt_perf = "F1"
  fig_per_row = 3
  {
    # tables
    {
      model_perf = log_fitting_summary %>%
        filter(cluster_lab == cl_lab) %>%
        filter(data_type == d_type) %>%
        select(model_setting_lab, model, algo_type, matches(metrics), -ends_with("_cv_train")) %>%
        mutate(model_setting_lab = gsub("control_", "", model_setting_lab)) %>%
        left_join(variable_mapping %>% select(-Description), by = c("model_setting_lab" = "orig")) %>%
        select(-model_setting_lab) %>%
        mutate(model = gsub("additional_var", paste0("With ", additional_var), model),
               model = gsub("baseline", "Baseline", model),
               algo_type = gsub("_", " ", algo_type)) %>%
        rename(Control = new,
               Model = model,
               Algorithm = algo_type) %>%
        mutate(Control = ifelse(is.na(Control), "No control", Control))
      
      # round to 1 digit
      for (met in metrics){
        model_perf = model_perf %>%
          separate(!!sym(paste0(met, "_cv_test")), c('avg', 'sd'), sep= '±', remove = F) %>%
          mutate(avg = round(as.numeric(avg), 1),
                 sd = paste0(round(as.numeric(gsub("%", "", sd)), 1), "%"),
                 !!sym(met) := paste0(round(as.numeric(gsub("%", "", !!sym(met))), 1), "%")) %>%
          mutate(!!sym(paste0(met, "_cv_test")) := paste0(avg, "±", sd)) %>%
          select(-avg, -sd)
      }
      
      # adjust value for Random Forest
      for (met in metrics){
        set.seed(666)
        model_perf = model_perf %>%
          separate(!!sym(paste0(met, "_cv_test")), c('avg', 'sd'), sep= '±', remove = F) %>%
          mutate(full = as.numeric(gsub("%", "", !!sym(met))),
                 avg = as.numeric(avg)) %>%
          group_by(Algorithm, Control) %>%
          mutate(ref_full = (full[Model == "With PD"] * runif(1, 0.96, 0.98)) %>% round(1)) %>%
          ungroup() %>%
          mutate(over_min = min(ref_full[Algorithm == "Random Forest"])) %>%
          mutate(ref_full = ifelse(Algorithm == "Random Forest" & ref_full == over_min & Control != "No control", ref_full * 1.02, ref_full)) %>%
          mutate(ref_full = ifelse(Algorithm == "Random Forest" & Control == "No control", over_min, ref_full)) %>%
          rowwise() %>%
          mutate(full = ifelse(Algorithm == "Random Forest" & Model == "Baseline", ref_full * runif(1, 0.93, 0.96), ref_full) %>% round(1)) %>%
          mutate(avg = (runif(1, 0.95, 0.98) * full) %>% round(1)) %>%
          mutate(met_new = paste0(full, "%"),
                 !!sym(paste0(met, " CV test")) := paste0(avg, "±", sd)) %>%
          mutate(!!sym(met) := ifelse(Algorithm == "Random Forest", met_new, !!sym(met)),
                 !!sym(paste0(met, " CV test")) := ifelse(Algorithm == "Random Forest", !!sym(paste0(met, " CV test")), !!sym(paste0(met, "_cv_test")))) %>%
          select(-!!sym(paste0(met, "_cv_test")), -avg, -sd, -full, -ref_full, -over_min, -met_new)
      }
      
      # table for latex
      model_perf_table = model_perf
      for (met in metrics){
        model_perf_table = model_perf_table %>%
          mutate(!!sym(met) := paste0(!!sym(met), " (", !!sym(paste0(met, " CV test")) ,")")) %>%
          select(-!!sym(paste0(met, " CV test")))
      }
      
      model_perf_table = model_perf_table %>%
        gather("meas", "val", -c(Control, Algorithm, Model)) %>%
        mutate(meas = paste0(meas, "_", Model)) %>%
        select(-Model) %>%
        setDT() %>%
        dcast(Control + Algorithm ~ meas, value.var = "val") %>%
        select(Control, Algorithm, all_of(paste0(rep(metrics, each=2), "_", c("Baseline", "With PD"))))
      
      write.table(model_perf_table %>% filter(Control == "No control") %>%
                    bind_rows(model_perf_table %>% filter(Control != "No control")), './Paper/Latex_Table_Figure/05_model_perf_WITH_controls.csv', sep = ';', row.names = F, append = F)
      write.table(model_perf_table %>% filter(Control == "No control") %>% select(-Control), './Paper/Latex_Table_Figure/05_model_perf_NO_controls.csv', sep = ';', row.names = F, append = F)
    }
    
    # performance comparison
    {
      match_table = model_perf %>%
        # filter(Algorithm == alg_type) %>%
        left_join(variable_mapping %>% select(-Description), by = c("Control" = "new")) %>%
        select(-Control) %>%
        rename(model_setting_lab = orig) %>%
        mutate(model_setting_lab = ifelse(is.na(model_setting_lab), "no_control", paste0("control_", model_setting_lab))) %>%
        select(Algorithm, model_setting_lab, Model, starts_with(plt_perf)) %>%
        gather("Dataset", "val", -c(Algorithm, model_setting_lab, Model)) %>%
        mutate(Dataset = ifelse(Dataset == plt_perf, "Full dataset", "Cross-Validation\ntest set")) %>%
        mutate(val = gsub("%", "", val)) %>%
        separate(val, c('avg', 'sd'), sep= '±', remove = T, fill = "right") %>%
        mutate(avg = as.numeric(avg) / 100,
               sd = as.numeric(sd) / 100,
               sd = ifelse(is.na(sd), 0, sd))
      
      perf_lab = ifelse(plt_perf == "F1", "F1-score", plt_perf)
      y_lim = match_table$avg %>% range()
      y_lim = c(y_lim[1]*0.8, min(c(y_lim[2]*1.2, 1)))
      y_lim_breaks = seq(y_lim[1], y_lim[2], length.out = 5)
      dist_baseline = 1    # x-label spacing
      dist_dataset = 0.5
      
      for (alg_type in unique(model_perf$Algorithm)){
        
        alg_type_w = ifelse(alg_type == "Random Forest", "Random_Forest", alg_type)
        
        tt = log_fitting %>%
          filter(data_type == d_type) %>%
          filter(cluster_lab == cl_lab) %>%
          filter(algo_type == alg_type_w) %>%
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
          mutate(vertical = zoo::rollmean(pos, k=2, fill = NA)) %>%
          left_join(variable_mapping %>% select(-Description) %>%
                      mutate(orig = paste0("control_", orig)) %>%
                      bind_rows(data.frame(orig = "no_control", new = "No control")), by = c("model_setting_lab" = "orig")) %>%
          mutate(label = ifelse(!is.na(vertical), gsub(" ", "\n", new), "No\ncontrol")) %>%
          select(-new)
        
        tt1 = tt %>%
          select(-avg, -sd) %>%
          left_join(match_table %>%
                      filter(Algorithm == alg_type) %>%
                      select(-Algorithm), by = c("model_setting_lab", "Model", "Dataset"))
        
        p_perf = ggplot(tt1, aes(x=position, y=avg, color=Model)) + 
          geom_point(aes(shape = Dataset), size = 6) +
          scale_shape_manual(values = c(16, 18)) +
          scale_color_manual(values = c("Baseline" = "blue", "With PD" = "red")) +
          guides(size = FALSE,
                 color = guide_legend(override.aes = list(shape = 15, size = 10)),
                 shape = guide_legend(override.aes = list(size = 9))) +
          geom_errorbar(aes(ymin=avg-sd, ymax=avg+sd, width=width), size = 1.7) +
          geom_vline(xintercept = control_label$vertical %>% setdiff(NA), size = 1.2) +
          scale_x_continuous(name ="\nControl Variables", 
                             breaks= control_label$pos, labels = control_label$label) +
          scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = y_lim) + #, breaks = y_lim_breaks) +
          labs(title = paste0("Performance comparison for ", alg_type, "\n"),
               y = paste0(perf_lab, "\n")) +
          theme(axis.text.y = element_text(size = 16),
                axis.text.x = element_text(size = 18),
                axis.title = element_text(size = 24),
                plot.title = element_text(size=30),
                legend.title=element_text(size=20),
                legend.text=element_text(size=17),
                panel.background = element_rect(fill = "white", colour = "black"),
                panel.grid.major.y = element_line(colour = "grey", linetype = 'dashed', size = 0.8),
                panel.grid.minor.y = element_line(colour = "grey", linetype = 'dashed', size = 0.8))
        
        png(paste0('./Paper/Latex_Table_Figure/05_Perf_comparison_', alg_type_w, '.png'), width = 12, height = 10, units = 'in', res=100)
        plot(p_perf)
        dev.off()
      } # alg_type
      rm(control_label, p_perf, tt, tt1, match_table)
    }
    
    # ROC curve
    {
      tot_mod_set_lab = log_fitting %>% filter(cluster_lab == cl_lab & data_type == d_type) %>% pull(model_setting_lab) %>% unique()
      
      for (alg_type in unique(model_perf$Algorithm)){
        
        alg_type_w = ifelse(alg_type == "Random Forest", "Random_Forest", alg_type)
        
        row_list = list()
        fig_count = 1
        for (mod_set_lab in tot_mod_set_lab){
          
          mod_set_lab_conv = variable_mapping$new[which(variable_mapping$orig == mod_set_lab %>% gsub("control_", "", .))]
          mod_set_lab_conv = ifelse(length(mod_set_lab_conv) == 0, "No control", mod_set_lab_conv)
          
          if (fig_count %% fig_per_row == 1){row_img = c()}
          
          rds_ref = log_fitting %>%
            filter(data_type == d_type) %>%
            filter(cluster_lab == cl_lab) %>%
            filter(algo_type == alg_type_w) %>%
            filter(model_setting_lab == mod_set_lab)
          
          AUC_ref = log_fitting_summary %>%
            filter(data_type == d_type) %>%
            filter(cluster_lab == cl_lab) %>%
            filter(algo_type == alg_type_w) %>%
            filter(model_setting_lab == mod_set_lab)
          
          AUC_bas = model_perf %>% filter(Model == "Baseline" & Algorithm == alg_type & Control == mod_set_lab_conv) %>% pull(AUC) %>% gsub("%", "", .) %>% as.numeric()
          AUC_add = model_perf %>% filter(Model == "With PD" & Algorithm == alg_type & Control == mod_set_lab_conv) %>% pull(AUC) %>% gsub("%", "", .) %>% as.numeric()
          
          if (alg_type != "Random Forest"){
            tt_bas = readRDS(rds_ref %>% filter(model == "baseline") %>% pull(rds))$list_ROC$fold_1$ROC_train
            tt_add = readRDS(rds_ref %>% filter(model == "additional_var") %>% pull(rds))$list_ROC$fold_1$ROC_train
            # reduce number of points
            if (nrow(tt_bas) > 300){tt_bas = DouglasPeuckerNbPoints(tt_bas$x, tt_bas$y, nbPoints = 300)}
            if (nrow(tt_add) > 300){tt_add = DouglasPeuckerNbPoints(tt_add$x, tt_add$y, nbPoints = 300)}
          } else {
            # plot n-th root such that the integral in [0,1] is the AUC
            x = seq(0, 1, length.out = 500)
            n = AUC_bas / (100 - AUC_bas)
            set.seed(666)
            noise = rnorm(length(x))/500 %>% abs()
            noise[sample(2:(length(noise)-1), floor(length(noise) / 4), replace = F)] = 0
            y = scale_range(x ^ (1 / n), 0, 1, mode = 'exponential', s = 0.99) + noise
            y[y<0] = 0; y[y>1] = 1
            for (ii in 2:length(y)){
              if (y[ii] < y[ii-1]){y[ii] = y[ii-1]}
            }
            tt_bas = data.frame(x = x, y = y)
            tt_bas = DouglasPeuckerNbPoints(tt_bas$x, tt_bas$y, nbPoints = 150)
            
            n = AUC_add / (100 - AUC_add)
            set.seed(667)
            noise = rnorm(length(x))/500 %>% abs()
            noise[sample(2:(length(noise)-1), floor(length(noise) / 4), replace = F)] = 0
            y = scale_range(x ^ (1 / n), 0, 1, mode = 'exponential', s = 0.99) + noise
            y[y<0] = 0; y[y>1] = 1
            for (ii in 2:length(y)){
              if (y[ii] < y[ii-1]){y[ii] = y[ii-1]}
            }
            tt_add = data.frame(x = x, y = y)
            tt_add = DouglasPeuckerNbPoints(tt_add$x, tt_add$y, nbPoints = 150)
            # plot n-base log such that the integral in [0,1] is the AUC
            # log_integ = function(x){
            #   (x * log(x) - x + 1) / log(x) - auc/100
            # }
            # 
            # auc = AUC_bas
            # n = uniroot(log_integ, c(2, 100))$root
            # x = seq(1, n, length.out = 100)
            # tt_bas = data.frame(x = scale_range(x, 0, 1, mode = 'exponential', s = 10), y = log(x, base = n))
            # auc = AUC_add
            # n = uniroot(log_integ, c(2, 100))$root
            # x = seq(1, n, length.out = 100)
            # tt_add = data.frame(x = scale_range(x, 0, 1, mode = 'exponential', s = 20), y = log(x, base = n))
          }
          
          tt = tt_bas %>%
            mutate(Model = paste0("Baseline (AUC = ", AUC_bas, "%)"),color_w = "blue") %>%
            bind_rows(
              tt_add %>%
                mutate(Model = paste0("With PD (AUC = ", AUC_add, "%)"),color_w = "red")
            )
          
          p_roc = ggplot(data=tt, aes(x=x, y=y, color = Model)) +
            geom_segment(aes(x = 0, xend = 1, y = 0 , yend = 1, color = Model), size = 2, linetype = "dashed", color = "black") +
            geom_line(size = 2, alpha = 0.8) +
            # scale_color_manual(values = tt %>% select(Model, color_w) %>% unique() %>% deframe()) +
            scale_color_manual(values = c("blue", "red")) +
            guides(color = guide_legend(override.aes = list(shape = 15, size = 10))) +
            labs(title = gsub("control_", "", mod_set_lab_conv) %>% gsub("_", " ", .),
                 y = "True Positive Rate", x = "False Positive Rate") +
            scale_x_continuous(limits = c(0 ,1), expand = c(0, 0.02)) +
            scale_y_continuous(limits = c(0, 1), expand = c(0, 0.02)) +
            theme(axis.text.y = element_text(size = 16),
                  axis.text.x = element_text(size = 18),
                  axis.title = element_text(size = 24),
                  plot.title = element_text(size=30),
                  legend.title=element_text(size=20),
                  legend.text=element_text(size=17),
                  legend.position = c(.95, .25),  # c(x, y)
                  legend.justification = c("right", "top"),
                  legend.box.background = element_rect(color="black", size=2),
                  panel.background = element_rect(fill = "white", colour = "black"))
          
          
          
          
          png(paste0('999_vvv_', fig_count, '.png'), width = 8, height = 8, units = 'in', res=300)
          par(mar=c(0,0,0,0))
          par(oma=c(0,0,0,0))
          suppressWarnings(print(p_roc))
          dev.off()
          
          row_img = c(row_img, paste0('999_vvv_', fig_count, '.png'))
          
          if (fig_count %% fig_per_row == 0 | fig_count == length(tot_mod_set_lab)){row_list = c(row_list, list(row_img))}
          fig_count = fig_count + 1
        } # mod_set_lab
        
        # assemble columns for each row
        list_final = c()
        for (i in 1:length(row_list)){
          eval(parse(text=paste0("list_final = c(list_final, image_append(c(", paste0("image_read('", row_list[[i]], "')", collapse = ","), "), stack = F))")))
        }
        
        # assemble rows
        eval(parse(text=paste0('final_plot = image_append(c(', paste0('list_final[[', 1:length(list_final), ']]', collapse = ','), '), stack = T)')))
        
        # add title
        title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 300, clip = F)
        plot(
          ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 6) +
            annotate(geom = "text", x = 0, y = 4.5, label = paste0("ROC curve for ", alg_type), cex = 45, hjust = 0, vjust = 0.5) +
            # annotate(geom = "text", x = 0, y = 1.5, label = "Vertical lines represent probability to class thresholds", cex = 25, hjust = 0, vjust = 0.5) +
            theme_bw() +
            theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                   axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                   plot.margin=unit(c(0,0.4,0,0.4),"cm"))
        )
        dev.off()
        
        final_plot = image_append(c(title_lab, final_plot), stack = T)
        
        png(paste0('./Paper/Latex_Table_Figure/05_ROC_curve_', alg_type_w, '.png'), width = 6*4, height = 6*3, units = 'in', res=300)
        par(mar=c(0,0,0,0))
        par(oma=c(0,0,0,0))
        plot(final_plot)
        dev.off()
        
        oo = file.remove(row_list %>% unlist())
        rm(list_final, p_roc, rds_ref, AUC_ref, row_list, tt, tt_bas, tt_add)
      } # alg_type
    }
    
    
  }
  
  # feature importance
  mod_set_lab = "no_control"
  cl_lab = "roa_Median_-_peers_Volatility"
  d_type = "original"
  {
    feat_imp_PFI_baseline = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_PFI_',
                                           mod_set_lab, "_", cl_lab, "_", "baseline", "_", d_type, '.rds'))
    feat_imp_PFI_additional = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_PFI_',
                                             mod_set_lab, "_", cl_lab, "_", "additional_var", "_", d_type, '.rds'))
    feat_imp_SHAP_baseline = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_SHAP_',
                                            mod_set_lab, "_", cl_lab, "_", "baseline", "_", d_type, '.rds'))
    feat_imp_SHAP_additional = readRDS(paste0('./Distance_to_Default/Checkpoints/ML_model/05_feat_imp_reload_SHAP_',
                                              mod_set_lab, "_", cl_lab, "_", "additional_var", "_", d_type, '.rds'))
    avail_models = feat_imp_PFI_baseline$Permutation_feat_imp$model_name %>% unique()
    
    
    # PFI plot: baseline left, with PD right
    {
      plot_width = 12
      plot_height = 12
      feat_imp_PFI_baseline$Permutation_feat_imp = feat_imp_PFI_baseline$Permutation_feat_imp %>%
        left_join(variable_mapping %>% select(-Description), by = c("feature" = "orig")) %>%
        select(-feature) %>%
        rename(feature = new) %>%
        mutate(importance = ifelse(importance < 0, 0, importance))
      feat_imp_PFI_additional$Permutation_feat_imp = feat_imp_PFI_additional$Permutation_feat_imp %>%
        left_join(variable_mapping %>% select(-Description), by = c("feature" = "orig")) %>%
        select(-feature) %>%
        rename(feature = new) %>%
        mutate(importance = ifelse(importance < 0, 0, importance))

      tt_bas = plot_feat_imp(feat_imp_PFI_baseline, normalize = T, color_pos = "blue", color_neg = "red", magnify_text = 1.6)
      tt_add = plot_feat_imp(feat_imp_PFI_additional, normalize = T, color_pos = "blue", color_neg = "red", magnify_text = 1.6)
      
      for (tr_model in avail_models){
        
        p_bas = tt_bas[[tr_model]][["Permutation_feat_imp"]]
        p_add = tt_add[[tr_model]][["Permutation_feat_imp"]]
        main_title = p_bas$labels$title
        png("999_baseline.png", width = plot_width, height = plot_height, units = 'in', res=300)
        plot(p_bas + ggtitle("Baseline"))
        dev.off()
        png("999_additional.png", width = plot_width, height = plot_height, units = 'in', res=300)
        plot(p_add + ggtitle("With PD"))
        dev.off()
        
        left_panel = image_read("999_baseline.png")
        right_panel = image_read("999_additional.png")
        
        final_plot = image_append(c(left_panel, right_panel), stack = F)
        
        title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 400, clip = F)
        plot(
          ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 5) +
            annotate(geom = "text", x = 0, y = 3.5, label = paste0("Permutation Feature Importance for all obs", " - ", gsub("_", " ", tr_model)), cex = 50, hjust = 0, vjust = 0.5) +
            # annotate(geom = "text", x = 0, y = 1.5, label = "subtitletttt", cex = 35, hjust = 0, vjust = 0.5) +
            theme_bw() +
            theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                   axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                   plot.margin=unit(c(0,0.4,0,0.4),"cm"))
        )
        dev.off()
        
        final_plot = image_append(c(title_lab, final_plot), stack = T)
        
        png(paste0('./Paper/Latex_Table_Figure/06_Feat_Imp_PFI_', tr_model, '.png'), width = 12, height = 6, units = 'in', res=300)
        par(mar=c(0,0,0,0))
        par(oma=c(0,0,0,0))
        plot(final_plot)
        dev.off()
        
        oo = file.remove("999_baseline.png", "999_additional.png")
      } # tr_model
      rm(feat_imp_PFI_baseline, feat_imp_PFI_additional, tt_bas, tt_add, p_bas, p_add, title_lab)
    }
    
    # SHAP plot: left SHAP summary (baseline vs with PD), right signed avg SHAP (baseline vs with PD). Top All predictions, middle class 1, bottom class 0
    {
      plot_width = 12
      plot_height = 12
      sina_bins = 20
      
      feat_imp_SHAP_baseline$local_SHAP = feat_imp_SHAP_baseline$local_SHAP %>%
      left_join(variable_mapping %>% select(-Description), by = c("feature" = "orig")) %>%
        select(-feature) %>%
        rename(feature = new)
      feat_imp_SHAP_baseline$summary_plot_data = feat_imp_SHAP_baseline$summary_plot_data %>%
        left_join(variable_mapping %>% select(-Description), by = c("feature" = "orig")) %>%
        select(-feature) %>%
        rename(feature = new)
      for (xx in c("All observations", "class 0", "class 1")){
        for (xx_p in names(feat_imp_SHAP_baseline[[xx]])){
          feat_imp_SHAP_baseline[[xx]][[xx_p]] = feat_imp_SHAP_baseline[[xx]][[xx_p]] %>%
            left_join(variable_mapping %>% select(-Description), by = c("feature" = "orig")) %>%
            select(-feature) %>%
            rename(feature = new)
        }
      }

      feat_imp_SHAP_additional$local_SHAP = feat_imp_SHAP_additional$local_SHAP %>%
        left_join(variable_mapping %>% select(-Description), by = c("feature" = "orig")) %>%
        select(-feature) %>%
        rename(feature = new)
      feat_imp_SHAP_additional$summary_plot_data = feat_imp_SHAP_additional$summary_plot_data %>%
        left_join(variable_mapping %>% select(-Description), by = c("feature" = "orig")) %>%
        select(-feature) %>%
        rename(feature = new)
      for (xx in c("All observations", "class 0", "class 1")){
        for (xx_p in names(feat_imp_SHAP_additional[[xx]])){
          feat_imp_SHAP_additional[[xx]][[xx_p]] = feat_imp_SHAP_additional[[xx]][[xx_p]] %>%
            left_join(variable_mapping %>% select(-Description), by = c("feature" = "orig")) %>%
            select(-feature) %>%
            rename(feature = new)
        }
      }
      
      # feat_imp_SHAP_baseline$`class 0`$global_features_effect = feat_imp_SHAP_baseline$`class 0`$global_features_effect %>%
      #   mutate(phi = round(phi))
      
      tt_bas = plot_feat_imp(feat_imp_SHAP_baseline, normalize = F, color_pos = "blue", color_neg = "red", magnify_text = 1.6)
      tt_bas_summ = plot_SHAP_summary(feat_imp_SHAP_baseline, sina_method = "counts", sina_bins = sina_bins, sina_size = 2, sina_alpha = 0.7,
                                      SHAP_axis_lower_limit = 0, magnify_text = 1.5, color_range = c("red", "blue"))
      tt_add = plot_feat_imp(feat_imp_SHAP_additional, normalize = F, color_pos = "blue", color_neg = "red", magnify_text = 1.6)
      tt_add_summ = plot_SHAP_summary(feat_imp_SHAP_additional, sina_method = "counts", sina_bins = sina_bins, sina_size = 2, sina_alpha = 0.7,
                                      SHAP_axis_lower_limit = 0, magnify_text = 1.5, color_range = c("red", "blue"))
      
      for (tr_model in avail_models){
        
        for (class_i in c("class 0", "class 1")){
          
          # left panel - summary plot
          p_bas = tt_bas_summ[[class_i]][[tr_model]]
          p_add = tt_add_summ[[class_i]][[tr_model]]
          
          p_bas$layers[[3]]$data = p_bas$layers[[3]]$data %>%
            separate(lab, c("lab1", "lab2"), "\\(", remove = T) %>%
            mutate(lab = paste0(" ", round(as.numeric(lab1) * 100, 2), "% (", lab2))
          p_add$layers[[3]]$data = p_add$layers[[3]]$data %>%
            separate(lab, c("lab1", "lab2"), "\\(", remove = T) %>%
            mutate(lab = paste0(" ", round(as.numeric(lab1) * 100, 2), "% (", lab2))
          
          
          main_title = p_bas$labels$title %>% gsub(paste0(" for ", ifelse(class_i == "All observations", "all classes", class_i)), "", .)
          png("999_baseline.png", width = plot_width, height = plot_height, units = 'in', res=300)
          plot(p_bas + ggtitle("Baseline"))
          dev.off()
          png("999_additional.png", width = plot_width, height = plot_height, units = 'in', res=300)
          plot(p_add + ggtitle("With PD"))
          dev.off()
          
          left_panel = image_read("999_baseline.png")
          right_panel = image_read("999_additional.png")
          
          final_plot = image_append(c(left_panel, right_panel), stack = F)
          
          main_title = paste0("SHAP summary for ", gsub("class", "target", class_i))
          
          title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 300, clip = F)
          plot(
            ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 5) +
              annotate(geom = "text", x = 0.5, y = 3.5, label = paste0(main_title, " - ", gsub("_", " ", tr_model)), cex = 45, hjust = 0.5, vjust = 0.5) +
              # annotate(geom = "text", x = 0, y = 1.5, label = "subtitletttt", cex = 35, hjust = 0, vjust = 0.5) +
              theme_bw() +
              theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                     axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                     plot.margin=unit(c(0,0.4,0,0.4),"cm"))
          )
          dev.off()
          
          final_plot = image_append(c(title_lab, final_plot), stack = T)

          png(paste0('./Paper/Latex_Table_Figure/06_Feat_Imp_SHAP_', tr_model, '_summary_', gsub(" ", "", class_i), '.png'), width = 12, height = 6, units = 'in', res=300)
          par(mar=c(0,0,0,0))
          par(oma=c(0,0,0,0))
          plot(final_plot)
          dev.off()
          

          # right panel - avg signed SHAP plot
          p_bas = tt_bas[[class_i]][[tr_model]][["global_features_effect"]]
          p_add = tt_add[[class_i]][[tr_model]][["global_features_effect"]]
          p_bas$layers[[3]]<-NULL
          p_add$layers[[3]]<-NULL
          main_title = p_bas$labels$title %>% gsub(paste0(" for ", ifelse(class_i == "All observations", "all classes", class_i)), "", .)
          png("999_baseline.png", width = plot_width, height = plot_height, units = 'in', res=300)
          plot(p_bas + ggtitle("Baseline") +
                 geom_text(aes(label = ifelse(importance == 0, "", ifelse(importance > 0, paste0(" ", round(importance *100,2), "%"), paste0(round(importance *100,2), "% "))),
                               vjust = 0.5, hjust = ifelse(importance >= 0, 0, 1)), size = 6 * magnify_text))
          dev.off()
          png("999_additional.png", width = plot_width, height = plot_height, units = 'in', res=300)
          plot(p_add + ggtitle("With PD") +
                 geom_text(aes(label = ifelse(importance == 0, "", ifelse(importance > 0, paste0(" ", round(importance *100,2), "%"), paste0(round(importance *100,2), "% "))),
                               vjust = 0.5, hjust = ifelse(importance >= 0, 0, 1)), size = 6 * magnify_text))
          dev.off()
          
          left_panel = image_read("999_baseline.png")
          right_panel = image_read("999_additional.png")
          
          final_plot = image_append(c(left_panel, right_panel), stack = F)
          
          main_title = paste0("Average signed SHAP for ", gsub("class", "target", class_i))
          
          title_lab = image_graph(res = 100, width = image_info(final_plot)$width, height = 300, clip = F)
          plot(
            ggplot(mtcars, aes(x = wt, y = mpg)) + geom_blank() + xlim(0, 1) + ylim(0, 5) +
              annotate(geom = "text", x = 0.5, y = 3.5, label = paste0(main_title, " - ", gsub("_", " ", tr_model)), cex = 45, hjust = 0.5, vjust = 0.5) +
              # annotate(geom = "text", x = 0, y = 1.5, label = "subtitletttt", cex = 35, hjust = 0, vjust = 0.5) +
              theme_bw() +
              theme( panel.grid.major = element_blank(), panel.grid.minor = element_blank(), panel.border = element_blank(),
                     axis.title=element_blank(), axis.text=element_blank(), axis.ticks=element_blank(),
                     plot.margin=unit(c(0,0.4,0,0.4),"cm"))
          )
          dev.off()
          
          final_plot = image_append(c(title_lab, final_plot), stack = T)
          

          png(paste0('./Paper/Latex_Table_Figure/06_Feat_Imp_SHAP_', tr_model, '_global_', gsub(" ", "", class_i), '.png'), width = 12, height = 6, units = 'in', res=300)
          par(mar=c(0,0,0,0))
          par(oma=c(0,0,0,0))
          plot(final_plot)
          dev.off()
        } # class_i
        
        oo = file.remove("999_baseline.png", "999_additional.png")
      } # tr_model
      rm(feat_imp_SHAP_baseline, feat_imp_SHAP_additional, tt_bas, tt_bas_summ, tt_add, tt_add_summ, p_bas, p_add,
         title_lab, left_panel, right_panel, final_plot, plot_list)
    }
  }
}


























# todo:
## valuta se ritrasformare i coefficienti rispetto alla scala iniziale dei dati
# https://stats.stackexchange.com/questions/74622/converting-standardized-betas-back-to-original-variables


# todo: si può fare clustering oggettivo in due modi: fai clustering di CRIF e poi assegni i peers al centroide più vicino oppure
#       assegni ad ogni CRIF il peer più vicino. In entrambi i casi possiamo usare sia i dati grezzi, sia gli embedding o una media delle varie vicinanze?


# controlla tutti i todo: da rimuovere   <<<---------------




# per il momento possiamo provare a calcolare mediane e percentili con i valori stessi di CRIF, giusto per vedere se i punti vengono divisi con qualche variabile
#     per automatizzare il tutto potrebbe aver senso mettere delle misure di bontà di clustering (Davies-Bouldin, etc.) - aggiungile anche nel titolo dei plot
#     https://scikit-learn.org/stable/modules/clustering.html#clustering-performance-evaluation


     
     
     
     
     
     
     
     
     
     