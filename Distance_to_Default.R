
memory.limit(size=1000000000000000000)
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


# define perimeter and load peers variables
reload_df = T  # reload df_final
{
  ATECO_to_industry = read.csv2('./Distance_to_Default/ATECO_to_industry.csv', stringsAsFactors=FALSE, colClasses = 'character') %>% select(-Note)
  
  if (reload_df == FALSE){
  df_final = readRDS('./Checkpoints/df_final.rds') %>%
    filter(FLAG_BILANCIO == 'CEBI') %>%
    select(abi, ndg, chiave_comune, year, month, segmento_CRIF, FLAG_Multibank, ANAG_ateco, ANAG_flag_def, starts_with('BILA'),
           Tot_Attivo, Tot_Valore_Produzione, Tot_Attivo_log10, Dimensione_Impresa, Regione_Macro) %>%
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
  rm(summary_perimeter, missing_ATECO, summary_years)
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

# todo: rimuovi
# ss_list = df_final_small%>% select(starts_with('BILA_')) %>% colnames()
# removed_short = intersect(var_to_remove, short_list)
# 
# ff = data.frame(variable = ss_list, stringsAsFactors = F) %>%
#   left_join(data.frame(variable = short_list, short_list_CRIF = 'YES', stringsAsFactors = F), by = "variable") %>%
#   left_join(data.frame(variable = removed_short, to_be_removed = 'YES', stringsAsFactors = F), by = "variable") %>%
#   replace(is.na(.), '')
# write.table(ff, './Distance_to_Default/Short_list_nostra.csv', sep = ';', row.names = F, append = F)



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

# clustering peers <-> df_final
reload_embedding_input = T    # reload embedding input (scaled original data)
reload_PCA = T    # reload Robust PCA embedding
reload_autoencoder = T   # reload autoencoder embedding
tune_autoencoder = F    # tune autoencoder parameters
reload_peers_embedding = T    # reload list of embedding and predicted embedding on peers dataset - requires Keras environment and will conflict with
                              # densMAP python environment when evaluating visualization data
reload_embedding_visualization = T    # reload evaluation of UMAP
{
  # evaluate statistics between ORBIS and CRIF data
  {
    # todo: togli quando avremo deciso se usare BILA_ammor_su_costi o BILA_ammor_su_costi_ammort
    # df_final_small = df_final_small %>% mutate(BILA_ammor_su_costi_ammort = BILA_ammor_su_costi,
    #                                            BILA_rod_income = BILA_rod,
    #                                            BILA_rod_income_div2 = BILA_rod)
    # df_final = df_final %>% mutate(BILA_ammor_su_costi_ammort = BILA_ammor_su_costi,
    #                                BILA_rod_income = BILA_rod,
    #                                BILA_rod_income_div2 = BILA_rod)  
    
    
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
        suppressWarnings(rm(boxplot_data, summary_nan_inf))
      }
      write.table(ORBIS_label, './Distance_to_Default/Stats/01b_ORBIS_peers_quantile_median.csv', sep = ';', row.names = F, append = F)
    }
    
    # todo: togli quando avremo deciso se usare BILA_ammor_su_costi o BILA_ammor_su_costi_ammort
    # df_final_small = df_final_small %>% select(-BILA_ammor_su_costi_ammort, -BILA_rod_income, -BILA_rod_income_div2)
    # df_final = df_final %>% select(-BILA_ammor_su_costi_ammort, -BILA_rod_income, -BILA_rod_income_div2)
    # df_peers = df_peers %>% select(-starts_with('ammor_su_costi_ammort'), -starts_with('rod_income'))
    
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
      
      # todo: rimuovi   - non dovrebbero esserci più problemi con i missing e inf
      # df_emb_input_peers = df_emb_input_peers %>%
      #   replace(is.na(.), 0)
      # df_emb_input_peers[sapply(df_emb_input_peers, is.infinite)] <- 0
      
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
  
  # run PCA and Autoencoder to visualize data clustering
  {
    # PCA
    
    cat('\n  ==================  PCA  ==================\n')
    
    if (reload_PCA == FALSE){
      PCA_emb = fit_pca(df_emb_input)
      PCA_emb_wide = fit_pca(df_emb_input_wide, NA_masking = 0)    # variables are standardized so the mean is zero, used as masking
      saveRDS(PCA_emb, './Distance_to_Default/Checkpoints/PCA_emb.rds')
      saveRDS(PCA_emb_wide, './Distance_to_Default/Checkpoints/PCA_emb_wide.rds')
    } else {
      PCA_emb = readRDS('./Distance_to_Default/Checkpoints/PCA_emb.rds')
      PCA_emb_wide = readRDS('./Distance_to_Default/Checkpoints/PCA_emb_wide.rds')
    }
    
    # Autoencoder
    
    cat('\n  ==================  Autoencoder  ==================\n')
    
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
        embedding_list = list()
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
                                                  create_lstm_input(df = emb_input_peers, df_header = emb_peers_header,
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
          
          embedding_list[[emb_type]] = list(emb = emb_header %>%
                                              left_join(as.data.frame(emb) %>%
                                                          rownames_to_column(join_col_emb) %>%
                                                          setNames(c(join_col_emb, paste0("V", c(1:(ncol(.)-1))))), by = join_col_emb),
                                            emb_peers = emb_peers_header %>%
                                              left_join(as.data.frame(emb_peers) %>% rownames_to_column(join_col_emb_peers) %>%
                                                          setNames(c(join_col_emb_peers, paste0("V", c(1:(ncol(.)-1))))), by = join_col_emb_peers))
          
        } # emb_type
        
        saveRDS(embedding_list, './Distance_to_Default/Checkpoints/embedding_list_pre_UMAP.rds')
        
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
        
        # check for embedding_list
        check_embedding_list = c()
        for (nn in names(embedding_list)){
          vv = embedding_list[[nn]]
          check_embedding_list = check_embedding_list %>%
            bind_rows(data.frame(Embedding = nn, NA_emb = sum(is.na(vv$emb)), NA_emb_peers = sum(is.na(vv$emb_peers)),
                                 Emb_columns = vv$emb %>% select(starts_with("V")) %>% ncol(),
                                 Emb_peers_columns = vv$emb_peers %>% select(starts_with("V")) %>% ncol(),
                                 Emb_rows = nrow(vv$emb), Emb_peers_rows = nrow(vv$emb_peers), stringsAsFactors = F))
        }
        if (max(check_embedding_list %>% select(NA_emb, NA_emb_peers) %>% unlist()) > 0){cat('\n\n######## warning: missing in some embedding_list')}
        if (sum(check_embedding_list$Emb_columns != check_embedding_list$Emb_peers_columns) > 0){cat('\n\n######## warning: embeddding/peers columns don\'t match in some embedding_list')}
        
        rm(emb, emb_peers, emb_file, emb_header, emb_input_peers, emb_peers_header, emb_model, embedding_range, check_embedding_list, vv)
      } else {
        embedding_list = readRDS('./Distance_to_Default/Checkpoints/embedding_list_pre_UMAP.rds')
      }
    }

    # evaluate UMAP to get 3 dimension to be plotted
    {
      n_components = 3
      min_dist_set = c(0, 0.01)#, 0.05, 0.1, 0.5, 0.99)
      n_neighbors_set = c(5, 15)#, 30, 50, 100, 500)
      metric = "euclidean"
      init = "agspectral"
      n_epochs = 4     # todo: rimetti 400
      eval_UMAP = T
      eval_densMAP = F
      # reticulate::use_condaenv("denseMAP", required = TRUE)     # activate conda env for densMAP
      # dm <- reticulate::import('densmap')
      
      if (reload_embedding_visualization == FALSE){
        emb_visual_list = list()
        for (emb_type in names(embedding_list)){
          
          cat('\nFitting', emb_type, end = '...')
          start_time = Sys.time()
          
          input_df = embedding_list[[emb_type]][['emb']] %>% select(starts_with("V"))
          predict_input = embedding_list[[emb_type]][['emb_peers']] %>% select(starts_with("V"))

          emb_visual_list[[emb_type]] = evaluate_UMAP(input_df, n_components = n_components, min_dist_set = min_dist_set, n_neighbors_set = n_neighbors_set,
                                                      metric = metric, init = init, n_epochs = n_epochs, predict_input = predict_input,
                                                      eval_UMAP = eval_UMAP, eval_densMAP = eval_densMAP)

          saveRDS(emb_visual_list, './Distance_to_Default/Checkpoints/emb_visual_list.rds')
          tot_diff=seconds_to_period(difftime(Sys.time(), start_time, units='secs'))
          cat(' Done in', paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))))
        }
      } else {
        emb_visual_list = readRDS('./Distance_to_Default/Checkpoints/emb_visual_list.rds')
      }
    }

    # create a report of fitted UMAP to choose which combination of n_neighbors and min_dist to keep
    
    
    
    
    
    
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

        
    }

    rm(summary_table)
  }
  
  # visualize peers with embedding
  {
    
    
    
    
    
    nn = load_model_hdf5("./Distance_to_Default/Checkpoints/AE_emb.h5")
    aa=nn(df_emb_input_peers %>% as.matrix()) %>% as.matrix() %>% as.data.frame()
    
    
    
    df_final_embedding = df_score[, 1:dim_embedding] %>%
      as.data.frame() %>%
      setNames(paste0('Dim', c(1:dim_embedding))) %>%
      mutate(Method = 'PCA',
             R2 = scree_data %>% filter(Embedding_Dimension == dim_embedding) %>% pull(PCA_R2)) %>%
      bind_rows(aut$Embedding %>%
                  as.data.frame() %>%
                  setNames(paste0('Dim', c(1:dim_embedding))) %>%
                  mutate(Method = 'AEC',
                         R2 = scree_data %>% filter(Embedding_Dimension == dim_embedding) %>% pull(AEC_R2)))
    
    
    # reload encoding
    pca_encoder = readRDS(paste0('./Distance_to_Default/Checkpoints/PCA.rds'))
    ae_encoder = aut = readRDS(paste0('./Distance_to_Default/Checkpoints/autoencoder_', dim_embedding, '.rds'))
    if (sum(pca_encoder$original_vars_name == ae_encoder$original_vars_name) != ncol(df_emb_input_peers)){
      cat('\n\n ###### columns names/oder mismatch in df_emb_input_peers')
    }
    
    for (embed in c('PCA', 'AEC')){
      
      R2 = df_final_embedding %>%
        filter(Method == embed) %>%
        pull(R2) %>%
        unique()
      
      if (embed == 'PCA'){
        plot_data = df_peers_long %>%
          select(Company_name_Latin_alphabet, year) %>%
          bind_cols(as.matrix(df_emb_input_peers) %*% pca_encoder$loadings[, 1:dim_embedding] %>%
                      as.data.frame() %>%
                      setNames(paste0('Dim', c(1:dim_embedding)))) %>%
          rename(company = Company_name_Latin_alphabet)
      } else if (embed == 'AEC'){
        model = load_model_hdf5(paste0('./Distance_to_Default/Checkpoints/autoencoder_', dim_embedding, '.h5'))
        plot_data = df_peers_long %>%
          select(Company_name_Latin_alphabet, year) %>%
          bind_cols(predict(model, as.matrix(df_emb_input_peers)) %>%
                      as.data.frame() %>%
                      setNames(paste0('Dim', c(1:dim_embedding)))) %>%
          rename(company = Company_name_Latin_alphabet)
        rm(model)
      }
      
      # create frames for GIF
      peers_list = unique(df_peers$Company_name_Latin_alphabet)
      path_list = c()
      for (frame in 0:length(peers_list)){   # 0 is All points together, in light gray
        
        if (frame == 0){
          plot_data_work = plot_data %>%
            mutate(year = 0)
          company_name = 'All companies'
          cmap = c('lightgray')
        } else {
          cmap = c('seagreen3', 'deepskyblue4', 'gold', 'darkorange2')
          company_name = peers_list[frame]
          plot_data_work = plot_data %>%
            filter(company == company_name)
          company_name = paste0(frame, '/', length(peers_list), ' - ', company_name)
        }
        
        size_set = c(25, 30, 15, 32, 15, 15)   # legend text, plot title, legend symbols, figure main title, fig_w, fig_h
        
        # plot data
        p = ggplot(plot_data_work %>% mutate(year = as.factor(year)), aes(x=Dim1, y=Dim2, color=year)) +
          geom_point(alpha = 1, shape = 19, size = 8) +
          scale_color_manual(breaks = plot_data_work$year, values=cmap, drop = F) +
          theme(panel.grid.major = element_blank(),
                panel.grid.minor = element_blank(),
                panel.background = element_blank(),
                # axis.line = element_line(colour = "black"),
                legend.text = element_text(size=size_set[1]),
                legend.title = element_blank(),
                legend.key = element_rect(fill = "transparent", colour = "transparent"),
                axis.title=element_blank(),
                axis.text=element_blank(),
                axis.ticks=element_blank(),
                plot.title = element_text(size = size_set[2]),
                panel.border = element_rect(colour = "black", fill=NA, size=1)) +
          guides(colour = guide_legend(override.aes = list(size=size_set[3]))) +
          ggtitle(company_name) +
          # scale_size_continuous(range = size_range) +
          xlim(plot_data %>% pull(Dim1) %>% range()) +
          ylim(plot_data %>% pull(Dim2) %>% range())
        
        if (frame == 0){p = p + theme(legend.position = "none")}
        
        # merge plots and save
        g = ggplotGrob(p)
        g = gtable_add_padding(g, unit(c(3,1,1,1), "cm")) # t,r,b,l
        g = gtable_add_grob(
          g,
          textGrob(as.expression(bquote('Peers mapping with' ~ .(embed) ~
                                          'embedding (' ~ R^2 ~ '=' ~ .(R2) ~ '%)') ), gp=gpar(fontsize=size_set[4])),1,1,1,ncol(g))
        
        save_path = paste0('./Distance_to_Default/Stats/02a_Peers_Mapping/Peers_Mapping_', embed, '_', frame, '.png')
        path_list = c(path_list, save_path)
        png(save_path, width = size_set[5], height = size_set[6], units = 'in', res=100)
        grid.draw(g)
        dev.off()
      } # frame
      
      # create GIF
      img_list = lapply(path_list, image_read)
      img_joined = image_join(img_list)
      img_animated = image_animate(img_joined, fps = 2, loop = 1)
      image_write(image = img_animated, path = paste0('./Distance_to_Default/Stats/02a_Peers_Mapping_', embed, '.gif'))
      # for (f in path_list){file.remove(f)}
    } # embed

  }
  
  # assign manual label (based on Peers quantile and Industry)
  {
    
    
    
    
    
    # plot cluster
    include_CRIF_cluster = F  # if TRUE clustering with CRIF median/percentiles is evaluated on CRIF_unmapped
    {
      categorical_variables = c('Dummy_industry', 'Dimensione_Impresa', 'Industry', 'segmento_CRIF', 'Regione_Macro')
      
      plot_issue = c()
      clustering_performance = c()
      clust_perf_log = list()
      if (include_CRIF_cluster){add_var = CRIF_unmapped} else {add_var = c()}
      dir.create('./Distance_to_Default/Stats/02b_Manual_Clustering/GIF/', recursive = T, showWarnings = F)
      vv = 1
      for (label_var in c(ORBIS_mapped, categorical_variables, add_var)){
        
        cat(vv, '/', length(c(ORBIS_mapped, categorical_variables, add_var)), end = '\r')
        
        for (embed in unique(df_final_embedding$Method)){
          
          # evaluate distance of original data used for clustering performance
          ref_dataset = df_final_small %>%
            select(starts_with('BILA_')) %>%
            as.matrix()    # todo: se cambi l'input degli embedding, cambia anche questo!
          # ref_dataset_dist = parDist(ref_dataset, method = "mahalanobis")
          
          R2 = df_final_embedding %>%
            filter(Method == embed) %>%
            pull(R2) %>%
            unique()
          
          plot_data = df_final_embedding %>%
            filter(Method == embed) %>%
            select(-Method) %>%
            mutate_if(is.numeric, ~round(., 4))
          
          if (label_var %in% categorical_variables){
            label_type = label_var
            label_values = df_final %>% select(all_of(label_var)) %>% unique() %>% pull(label_var) %>%
              lapply(function(x) substr(x,1,15)) %>% unlist()
            plot_data = plot_data %>%
              mutate(Label = factor(df_final %>% select(all_of(label_var)) %>% pull(label_var) %>%
                                      lapply(function(x) substr(x,1,15)) %>% unlist(), levels = label_values))
            loops = '_'
          } else {
            loops = c('Median_Single', 'LowMedHig_Single')  # 'Median_Combined', 'LowMedHig_Combined'
          }
          
          plot_issue_flag = F
          for (loop_main in loops){
            
            loop = strsplit(loop_main, '_')[[1]][1]
            loop_type = strsplit(loop_main, '_')[[1]][2]
            
            # prepare data for Median/LowMedHig
            run = T
            if (loop != ''){
              label_type = paste0(gsub('BILA_', '', label_var), '_', loop)
              if (loop == 'Median'){
                df_label = df_final %>%
                  # select(all_of(c(label_var, 'Dummy_industry'))) %>%
                  # setNames(c('var', 'Dummy_industry')) %>%
                  # left_join(ORBIS_label %>% filter(Variable == label_var) %>% select(Dummy_industry, Peer_median), by = "Dummy_industry") %>%
                  select(all_of(label_var)) %>%
                  setNames(c('var')) %>%
                  mutate(Variable = label_var) %>%
                  left_join(ORBIS_label %>% filter(Variable == label_var) %>% select(Variable, Peer_median), by = "Variable") %>%
                  mutate(Label = ifelse(var <= Peer_median, 'Down', 'Up'))
                  # mutate(Label_comb = paste0(substr(Dummy_industry, 1, 4), '_', Label))
              } else if (loop == 'LowMedHig'){
                df_label = df_final %>%
                  # select(all_of(c(label_var, 'Dummy_industry'))) %>%
                  # setNames(c('var', 'Dummy_industry')) %>%
                  # left_join(ORBIS_label %>% filter(Variable == label_var) %>% select(Dummy_industry, Peer_33rd, Peer_66th), by = "Dummy_industry") %>%
                  select(all_of(label_var)) %>%
                  setNames(c('var')) %>%
                  mutate(Variable = label_var) %>%
                  left_join(ORBIS_label %>% filter(Variable == label_var) %>% select(Variable, Peer_33rd, Peer_66th), by = "Variable") %>%
                  mutate(Label = 'Low') %>%
                  mutate(Label = ifelse(var > Peer_33rd, 'Medium', Label)) %>%
                  mutate(Label = ifelse(var > Peer_66th, 'High', Label))
                  # mutate(Label_comb = paste0(substr(Dummy_industry, 1, 4), '_', Label))
              }
              
              # if (loop_type == 'Combined'){
              #   df_label = df_label %>%
              #     mutate(Label = Label_comb)
              #   label_type = paste0('Dummy_industry+', label_type)
              # }
              
              # check for missing values combinations
              label_values = sort(unique(df_label$Label))
              plot_data = plot_data %>%
                mutate(Label = factor(df_label$Label, levels = label_values))
              nn = length(label_values)
              if ((loop == 'Median' & loop_type == 'Single' & nn != 2) |
                  (loop == 'Median' & loop_type == 'Combined' & nn != 4) |
                  (loop == 'LowMedHig' & loop_type == 'Single' & nn != 3) |
                  (loop == 'LowMedHig' & loop_type == 'Combined' & nn != 6)){plot_issue_flag = T;run = F}
            }
            
            # evaluate clustering performance
            # https://cran.r-project.org/web/packages/clusterCrit/vignettes/clusterCrit.pdf
            if (run == T){
              cluster = as.integer(plot_data$Label)
              if (is.null(clust_perf_log[[label_type]])){ # clustering performance on original data is the same for any embedding
                
                clust_perf = intCriteria(ref_dataset, cluster, c('davies_bouldin', 'silhouette', 'PBM', 'Calinski_Harabasz'))
                clust_perf_log[[label_type]] = clust_perf
                
                cat('\n evaluating', embed, '-', label_type, '\n')
              } else {
                clust_perf = clust_perf_log[[label_type]]
                cat('\n reloading', embed, '-', label_type, '\n')
              }
              clust_perf_embed = intCriteria(plot_data %>% select(starts_with('Dim')) %>% as.matrix, cluster,
                                             c('davies_bouldin', 'silhouette', 'PBM', 'Calinski_Harabasz'))
              names(clust_perf) = paste0('Original_', names(clust_perf))
              names(clust_perf_embed) = paste0('Embedding_', names(clust_perf_embed))
              
              clustering_performance = clustering_performance %>%
                bind_rows(data.frame(Embedding = embed, R2 = R2, Label = label_type, clust_perf, clust_perf_embed, stringsAsFactors = F))
            }
            
            # define colormap
            if (length(label_values) == 2){
              cmap = c('yellow2', 'seagreen3')
            } else if (length(label_values) == 3){
              cmap = c('dodgerblue4', 'gold', 'firebrick3')
            } else if (length(label_values) == 4){
              cmap = c('cadetblue2', 'deepskyblue4', 'gold', 'darkorange2')
            } else if (length(label_values) == 6){
              cmap = c('cadetblue2', 'slategray4', 'dodgerblue4', 'gold', 'chartreuse3', 'firebrick3')
            } else {
              cmap = c('seagreen3', 'dodgerblue4', 'yellow3', 'firebrick3', 'cadetblue2', 'darkorange2', 'slategray4', 'violet', 'yellow')
            }
            
            # plot
            if (run){
              
              min_point_size = 3
              max_point_size = 12
              
              # loop for GIF frames (all labels and single label)
              aggregated_data = aggregate_points(plot_data, label_values, n_cell = 20)
              path_list = c()
              for (frame in -1:length(label_values)){   #-1 is high res All points, 0 is all points frame for GIF
                
                # loop for plot type (all points and aggregated points)
                row_list = list()
                cc = 1
                for (plot_type in c('All points', 'Aggregated points')){
                  
                  # select data to plot
                  if (plot_type == 'All points'){
                    plot_data_work = plot_data %>%
                      mutate(size = 1)
                    size_range = c(min_point_size, min_point_size)
                  } else {
                    plot_data_work = aggregated_data$cell_summary 
                    size_range = c(min_point_size, max_point_size)
                  }
                  
                  # filter frame by label
                  if (frame <= 0){
                    frame_name = '_All'
                    plot_data_work_2 = plot_data_work
                  } else {
                    frame_name = paste0('_', label_values)[frame]#paste0('_label', str_pad(frame, 2, pad = '0'))
                    plot_data_work_2 = plot_data_work %>%
                      filter(Label == label_values[frame])
                    plot_data_work_2 = plot_data_work_2 %>%
                      bind_rows(plot_data_work_2 %>%
                                  filter(row_number() == 1) %>%
                                  mutate(Dim1 = NA, Dim2 = NA, size = max(plot_data_work$size))
                      )
                  }
                  
                  if (frame == -1){   # high res
                    frame_name = ''
                    size_set = c(25, 30, 15, 32, 32, 17)   # legend text, plot title, legend symbols, figure main title, fig_w, fig_h
                  } else {  # gif
                    size_set = c(15, 18, 7, 21, 16, 8)
                    size_range = size_range / 1.7
                  }
                  
                  # plot data
                  p = ggplot(plot_data_work_2, aes(x=Dim1, y=Dim2, color=Label, size = size)) +
                    geom_point(alpha = 0.8, shape = 19) +
                    scale_color_manual(breaks = label_values, values=cmap, drop = F) +
                    theme(panel.grid.major = element_blank(),
                          panel.grid.minor = element_blank(),
                          panel.background = element_blank(),
                          # axis.line = element_line(colour = "black"),
                          legend.text = element_text(size=size_set[1]),
                          legend.title = element_blank(),
                          legend.key = element_rect(fill = "transparent", colour = "transparent"),
                          axis.title=element_blank(),
                          axis.text=element_blank(),
                          axis.ticks=element_blank(),
                          plot.title = element_text(size = size_set[2]),
                          panel.border = element_rect(colour = "black", fill=NA, size=1)) +
                    guides(colour = guide_legend(override.aes = list(size=size_set[3]))) +
                    ggtitle(gsub('_', '', gsub('All', ifelse(frame_name == '', '_All', frame_name), plot_type))) +
                    scale_size_continuous(range = size_range) +
                    xlim(plot_data_work %>% pull(Dim1) %>% range()) +
                    ylim(plot_data_work %>% pull(Dim2) %>% range())
                  
                  if (plot_type == 'All points'){p = p + theme(legend.position = "none")
                  } else {
                    for (i in aggregated_data$grid_x){
                      p = p + geom_vline(xintercept=i, linetype="dotted", color = "gray", size=1, alpha=0.5)
                    }
                    for (i in aggregated_data$grid_y){
                      p = p + geom_hline(yintercept=i, linetype="dotted", color = "gray", size=1, alpha=0.5)
                    }
                  }
                  row_list[[cc]] = suppressWarnings(ggplotGrob(p))
                  cc = cc + 1
                } # plot_type
                
                # merge plots and save
                g = do.call(cbind, c(row_list, size="last"))
                g = gtable_add_padding(g, unit(c(3,1,1,1), "cm")) # t,r,b,l
                g = gtable_add_grob(
                  g,
                  textGrob(as.expression(bquote('Manual Clustering for' ~ .(label_type) ~ 'with' ~ .(embed) ~
                                                  'embedding (' ~ R^2 ~ '=' ~ .(R2) ~ '%)') ), gp=gpar(fontsize=size_set[4])),1,1,1,ncol(g))
                
                save_path = paste0('./Distance_to_Default/Stats/02b_Manual_Clustering/', label_type, '_', embed, frame_name, '.png')
                path_list = c(path_list, save_path)
                png(save_path, width = size_set[5], height = size_set[6], units = 'in', res=100)
                grid.draw(g)
                dev.off()
              } # frame
              
              # create GIF
              img_list = lapply(path_list[-1], image_read)
              img_joined = image_join(img_list)
              img_animated = image_animate(img_joined, fps = ifelse(length(label_values) > 6, 2, 1))
              image_write(image = img_animated, path = paste0('./Distance_to_Default/Stats/02b_Manual_Clustering/GIF/', label_type, '_', embed, '.gif'))
              for (f in path_list[-1]){file.remove(f)}
              
            } else {
              plot_issue = plot_issue %>%
                bind_rows(data.frame(Variable = label_var, Problem = loop_main, stringsAsFactors = F))
            } # run
          } # loop_main
        } # embed
        if (plot_issue_flag == T){cat('\n --- ', label_var, '- skipped (ORBIS median/percentile outside CRIF min/max)\n')}
        vv = vv + 1
      } # label_Var
      plot_issue = plot_issue %>%
        unique() %>%
        group_by(Variable) %>%
        summarize(Problem = paste0(Problem, collapse = ' - '), .groups = 'drop')
      ORBIS_label = ORBIS_label %>%
        left_join(plot_issue, by = "Variable") %>%
        replace(is.na(.), '')
      write.table(ORBIS_label, './Distance_to_Default/Stats/02b_Manual_Clustering_Report.csv', sep = ';', row.names = F, append = F)
      
      clustering_performance_summary = c()
      for (emb in unique(clustering_performance$Embedding)){
        tt = clustering_performance %>%
          filter(Embedding == emb) %>%
          mutate(Best = '') %>%
          select(Embedding, Best, Label, everything())
        best_list = c()
        for (perf in tt %>% select(matches('Original_|Embedding_')) %>% colnames()){        # assign "**" to best performance for each column
          val = tt %>% select(all_of(perf)) %>% unlist() %>% as.numeric() %>% round(2)
          best_ind = bestCriterion(val, gsub('Original_|Embedding_', '', perf)) %>% as.numeric()
          best_list = c(best_list, best_ind)
          best_mark = rep('', length(val))
          best_mark[best_ind] = '**  '
          tt = tt %>% mutate(!!sym(perf) := paste0(best_mark, val))                       # todo: silhouette non ha **, forse perché ci sono gli NA - controlla
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
      }
      write.table(clustering_performance_summary, './Distance_to_Default/Stats/02b_Manual_Clustering_Summary.csv', sep = ';', row.names = F, append = F)
    }
    rm(plot_issue, plot_data_work, plot_data_work_2, aggregated_data, plot_data, df_label, g, row_list, p)
    
  }
  
  


  
   
  
}



######    nel loop del clustering metti gli embedding dentro, così non deve calcolare le performance del clustering sui dati originali due volte!


# predici il dataset con l'autoencoder e fai la PCA e vedi se cambia la varianza spiegata
# se cambi l'input degli embedding, cambia anche ref_dataset

# usa 3 componenti ?   -> puoi fare una GIF che fa ruotare il grafico da più angolazioni

# cambia l'activation function con selu o tanh o sigmoide, in modo che non ci siano gli 0 della Relu. O magari va cambiato solo nel layer centrale?

# autoencoder con LSTM?


# controlla tutti i todo: da rimuovere   <<<---------------




# per il momento possiamo provare a calcolare mediane e percentili con i valori stessi di CRIF, giusto per vedere se i punti vengono divisi con qualche variabile
#     per automatizzare il tutto potrebbe aver senso mettere delle misure di bontà di clustering (Davies-Bouldin, etc.) - aggiungile anche nel titolo dei plot
#     https://scikit-learn.org/stable/modules/clustering.html#clustering-performance-evaluation



# fatti dire il mapping completo delle variabili ORBIS così provo a predire con l'autoencoder i peer e vedo se almeno le 3 (anche 4) osservazioni di ogni anno
#      finiscono vicine (puoi fare la gif con i 4 punti per ogni frame)





#### clustering oggettivo!!!





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
