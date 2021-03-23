
utils::memory.limit(64000)
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
library(rgl)
library(PKPDmisc)
library(parallelMap)
library(parallel)
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

# baseline model FLAG_Default vs short list BILA
{

  # https://cran.r-project.org/web/packages/lme4/vignettes/lmer.pdf
  
  # we want to estimate y_it = A + B*x_it + u_i + e_it
  #                            u_i individual error component (doesn't change over time)
  #                            e_it idiosyncratic error component
  # - if u_i is correlated with X use the FIXED EFFECT (or WITHIN) so there is an individual-specific effect driving the data
  # - if u_i is uncorrelated with X use the RANDOM EFFECT so there's an individual+time-specific effect driving the data
  # - if u_i is missing use the POOLED model i.e. you assume there's no actual panel structure
  
  short_list = c('BILA_cashflow_ricavi', 'BILA_totdebiti_su_patrim', 'BILA_debitifornit_su_patrim', 'BILA_durata_scorte',
                 'BILA_liquid_imm', 'BILA_oneri_valagg', 'BILA_patr_su_patrml', 'BILA_roa', 'BILA_rotazione_circolante', 'BILA_turnover')
  control_variable = c('segmento_CRIF', 'Dimensione_Impresa', 'Industry', 'Dummy_industry')
  df_reg = df_final %>% select(abi, ndg, year, FLAG_Default, all_of(control_variable), all_of(short_list)) %>%
    mutate(abi_ndg = paste0(abi, '_', ndg)) %>%
    select(-abi, -ndg) %>%
    select(abi_ndg, everything()) %>%
    mutate_at(all_of(short_list), ~scale(.)) %>%
    mutate_at(all_of(short_list), function(x) { attributes(x) <- NULL; x }) %>%
    mutate(Industry = substr(Industry, 1, 8)) %>%
    mutate_at(all_of(control_variable), ~as.factor(.))
  
  # check correlation and partial correlation
  correlation_list = evaluate_correlation(df_reg %>% select(all_of(short_list)))

  
  ctrl = 'Dummy_industry'
  ff = c()
  for (v in short_list){
    ff = c(ff, paste0('(', v, '|', ctrl, ')'))
  }
  form = as.formula(paste('FLAG_Default', paste(ff, collapse = " + "), sep = " ~ "))
  
  form = as.formula(paste('FLAG_Default ~', paste(short_list, collapse = " + "), '+ (', paste(short_list, collapse = " + "), ') |', ctrl))
  
  
  
  fit_mixed <- glmer(form, data = df_reg[1:1000,], family = binomial(link = "logit"),
                     control = glmerControl(optimizer ='optimx', optCtrl=list(method='nlminb')))
  ss2 = summary(fit_mixed)
  
  
  ss1 = summary(fit_mixed)
  
  
  confint.merMod(fit_mixed,method="profile") 
  
  
  
  # https://cran.r-project.org/web/packages/plm/vignettes/plmPackage.html#fnref3
  # conver to panel data.frame
  # df_rep_pdata <- pdata.frame(df_reg, index=c("abi_ndg", "year"), drop.index=TRUE, row.names=TRUE)
  # 
  # # check balance (if gamma and nu are close to 1 then is balanced)
  # punbalancedness(df_rep_pdata)
  # 
  # form = as.formula(paste('FLAG_Default', paste(short_list, collapse = " + "), sep = " ~ "))
  # 
  # # fit fixed-effect with different effects
  # for (eff in c('individual', 'time', 'twoways')){
  #   errR = try(mod <- plm(form, data = df_rep_pdata, model = "within", effect = eff), silent = T)
  #   if(is(errR,'try-error')){
  #   } else {
  #     sink(paste0('./Distance_to_Default/Baseline_regression/FixEff_', eff, '_summary.txt'))
  #     cat('\n------------------------  Testing effects  ------------------------\n\n')
  #     print(plmtest(form, data = df_rep_pdata, effect = eff))
  #     cat('\n\n\n\n------------------------  Fitting Model  ------------------------\n\n')
  #     print(summary(mod))
  #     sink()
  #   }
  # }
  # 
  # # fit random-effect with different effects
  # for (eff in c('individual', 'time', 'twoways')){
  #   errR = try(mod <- plm(form, data = df_rep_pdata, model = "random", effect = eff), silent = T)
  #   if(is(errR,'try-error')){
  #   } else {
  #     sink(paste0('./Distance_to_Default/Baseline_regression/RanEff_', eff, '_summary.txt'))
  #     cat('\n------------------------  Testing effects  ------------------------\n\n')
  #     print(plmtest(form, data = df_rep_pdata, effect = eff))
  #     cat('\n\n\n\n------------------------  Fitting Model  ------------------------\n\n')
  #     print(summary(mod))
  #     sink()
  #   }
  # }
  
 
  
}

# evaluate embeddings and evaluate UMAP 3D visualization
reload_embedding_input = T    # reload embedding input (scaled original data)
reload_PCA = T    # reload Robust PCA embedding
reload_autoencoder = T   # reload autoencoder embedding
tune_autoencoder = F    # tune autoencoder parameters
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
      if (tune_autoencoder){
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
          cat(' Done in', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
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
reload_manual_label = T
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
        rm(best_subset, best_ind_overall, clustering_performance_summary, summary_table, class_table, plot_list)
      }
    }
  }
  
  # assign closest peers to each CRIF data (cluster label is Company_name_Latin_alphabet)
  {
    # todo: capisci se fare la distanza sul df_final_small o anche sugli embedding
  }
  
  # evaluate best clusterization on CRIF data and assign peers to each cluster (cluster label is a "set" of Company_name_Latin_alphabet)
  {
    # todo: capisci se fare il clustering sul df_final_small o anche sugli embedding
  }
  
  rm(best_comb_visual)
}

# evaluate Distance to Default - 2014 is removed
{
  # evaluate DD in the following way:
  #  - evaluate peers DD and map to abi+ndg with clustering label (peers DD are averaged over same class label)
  #  - evaluate peers asset volatility and use abi+ndg Asset and Equity
  #  - todo: ogni abi+ndg può essere associato ad un singolo peer e quindi si possono ripetere i due casi precedenti
  
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
      
      # todo: rimuovi ----- per ora copio il valore del 2013 a quelli per cui manca il 2014
      {
        # peers_all_years = DD_peers_volatility_annual %>%
        #   filter(year == 2014) %>%
        #   pull(Company_name_Latin_alphabet)
        # 
        # DD_peers_volatility_annual = DD_peers_volatility_annual %>%
        #   filter(Company_name_Latin_alphabet %in% peers_all_years) %>%
        #   bind_rows(DD_peers_volatility_annual %>%
        #               filter(!Company_name_Latin_alphabet %in% peers_all_years),
        #             DD_peers_volatility_annual %>%
        #               filter(year == 2013) %>%
        #               mutate(year = 2014) %>%
        #               filter(!Company_name_Latin_alphabet %in% peers_all_years))
        # 
        # check_to_delete = DD_peers_volatility_annual %>%
        #   group_by(Company_name_Latin_alphabet) %>%
        #   summarise(yy = uniqueN(Volatility)) %>%
        #   group_by(yy) %>%
        #   summarise(count = n())
        # if (check_to_delete %>% filter(yy == 3) %>% pull(count) != length(peers_all_years)){cat('\n###### error in filling 2014 missing volatility')}
        # rm(check_to_delete)
        }
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
      mutate(Tot_Liability = ifelse(Tot_Liability < 0, 1e-16, Tot_Liability)) %>%
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
        
        # evaluate class average (by year) for A, L, r and volatility and re-evaluate DD/PD
        peers_ref = DD_peers %>%
          left_join(map_cluster_on_peers(label_type = manual_label, categorical_variables,
                                         df_peers_long, ORBIS_mapping, ORBIS_label), by = "Company_name_Latin_alphabet") %>%
          group_by(Label, year) %>%
          summarise_at(vars(Tot_Attivo, Tot_Liability_orig, Volatility, risk_free_yield, `T`), mean) %>%
          mutate(Tot_Liability = ifelse(Tot_Liability_orig < 0, 1e-16, Tot_Liability_orig)) %>%
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
          mutate(Tot_Liability = ifelse(Tot_Liability < 0, 1e-16, Tot_Liability)) %>%
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
      # todo:
    }
    
    # apply CRIF data cluster
    {
      # todo:
    }
  }
}

# run logistic regression for each DD assignment in list_DD_CRIF_data


  
## todo: aggiungi distribuzione PD e DD per ogni regressione (anche vs FLAG_default)







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
# result_csv = paste0('./Distance_to_Default/Stats/Autoencoder_test/00_Optimization_list_', save_RDS_additional_lab, '.csv')
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


# res =  read.csv('./Distance_to_Default/Stats/Autoencoder_test/00_Optimization_list_AE_LSTM_emb.csv', sep=";", stringsAsFactors=FALSE) %>%
#   mutate(layers_list = as.character(layers_list))
# lstm_list = paste0("./Distance_to_Default/Stats/Autoencoder_test/",
#                      list.files(path = './Distance_to_Default/Stats/Autoencoder_test/', pattern = "^AE_LSTM_emb*"), sep = "")
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
# write.table(res %>% relocate(R2, .after = MSE) %>% filter(!is.na(R2)), './Distance_to_Default/Stats/Autoencoder_test/00_Optimization_list_AE_LSTM_emb.csv', sep = ';', row.names = F, append = F)
