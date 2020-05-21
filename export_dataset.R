
library(haven)
library(data.table)
library(dplyr)
library(tidyverse)


df_or <- read_dta("C:/Users/Alessandro Bitetto/Downloads/imp_partial_SF (post merging update replace).dta") %>%
  mutate(ndg = as.character(ndg))

# main_abi_ndg = df_or %>%
#   select(chiave_comune, abi, ndg, anno) %>%
#   group_by(abi, ndg) %>%
#   summarize(chiave_comune_final = ifelse(length(unique(chiave_comune[!is.na(chiave_comune)])) == 0, NA, unique(chiave_comune[!is.na(chiave_comune)])))

# df_chiave = read.csv("C:\\Users\\Alessandro Bitetto\\Downloads\\2a_Chiave comune.csv", sep=';', stringsAsFactors = F) %>%
#   rename(chiave_comune2 = chiave_comune) %>%
#   mutate(abi = as.numeric(abi))

df_chiave <- read_dta("C:/Users/Alessandro Bitetto/Downloads/chiave_comune_SF.dta") %>%
  mutate(ndg = as.character(ndg))

df = df_or %>%
  rename(chiave_comune_old = chiave_comune) %>%
  left_join(df_chiave) %>%
  mutate(ateco_2digit = substr(ateco, 1, 2)) %>%
  select(chiave_comune, chiave_comune_old, anno, abi, ndg, ateco, ateco_2digit, everything()) %>%
  mutate(DIVERSO = varT012_th != varT030_th) %>%
  mutate(IS_NA_12 = is.na(varT012_th),
         IS_NA_30 = is.na(varT030_th)) %>%
  filter(!xor(IS_NA_12, IS_NA_30)) %>%
  filter(DIVERSO == FALSE | is.na(DIVERSO)) %>%
  select(-DIVERSO, -IS_NA_12, -IS_NA_30) %>%
  mutate(CHIAVE = paste(chiave_comune, abi, ndg, anno, sep='_')) %>%
  filter(anno != 2088)


  

df_not_dupl = df %>%
  filter(`_merge` != 5)

df_dupl = df %>%
  filter(`_merge` == 5) %>%
  group_by(chiave_comune, abi, ndg, anno) %>%
  mutate(ROW_COUNT = n()) %>%
  ungroup() %>%
  filter((ROW_COUNT == 1) | (ROW_COUNT == 2 & tipo_bil == '00')) %>%
  group_by(chiave_comune, abi, ndg, anno) %>%
  mutate(ROW_COUNT = n()) %>%
  ungroup() %>%
  filter((ROW_COUNT == 1) | (ROW_COUNT == 2 & strutbil == '01')) %>%
  group_by(chiave_comune, abi, ndg, anno) %>%
  mutate(ROW_COUNT = n()) %>%
  ungroup() %>%
  filter((ROW_COUNT == 1) | (natura == 0)) %>%
  select(-ROW_COUNT)
df_dupl_check = df_dupl %>%
  group_by(chiave_comune, abi, ndg, anno) %>%
  summarise(COUNT = n())
if (nrow(df_dupl_check) != nrow(df_dupl)){cat('\n errore in df_dupl')}

df_final = df_not_dupl %>%
  rbind(df_dupl) %>%
  group_by(CHIAVE) %>%
  mutate(ROW_COUNT = n()) %>%
  ungroup() %>%
  filter((ROW_COUNT == 1) | (ROW_COUNT == 2 & tipo_bil == '00')) %>%
  group_by(CHIAVE) %>%
  mutate(ROW_COUNT = n()) %>%
  ungroup() %>%
  filter((ROW_COUNT == 1) | (ROW_COUNT == 2 & strutbil == '01')) %>%
  group_by(CHIAVE) %>%
  mutate(ROW_COUNT = n()) %>%
  ungroup() %>%
  filter((ROW_COUNT == 1) | (natura == 0)) %>%
  select(-ROW_COUNT)
df_final_check = df_final %>%
  group_by(CHIAVE) %>%
  summarise(COUNT = n())
if (nrow(df_final_check) != nrow(df_final)){cat('\n errore in df_final')}  



df_chiave_anno = df_final %>%
  select(chiave_comune, abi, ndg, anno, starts_with('var')) %>%
  mutate(FULL_NA = apply(., 1, function(x) sum(is.na(x[5:length(x)])) == (length(x) - 4))) %>%
  mutate(COUNT_NA = apply(., 1, function(x) sum(is.na(x[5:length(x)])))) %>%
  filter(FULL_NA == FALSE)

df_chiave_anno_with_chiave = df_chiave_anno %>%
  filter(!is.na(chiave_comune)) %>%
  group_by(chiave_comune, anno) %>%
  arrange(COUNT_NA) %>%
  mutate(ROW_COUNT = n(),
         ROW_TO_PICK = 1:n()) %>%
  ungroup() %>%
  filter((ROW_COUNT == 1) | (ROW_COUNT > 1 & ROW_TO_PICK == 1)) %>%
  select(-ROW_COUNT, -ROW_TO_PICK)
check_df_chiave_anno_with_chiave = df_chiave_anno_with_chiave %>%
  group_by(chiave_comune, anno) %>%
  summarise(COUNT = n())
if (nrow(check_df_chiave_anno_with_chiave) != nrow(df_chiave_anno_with_chiave)){cat('\n errore in df_chiave_anno_with_chiave')}  

df_chiave_anno_without_chiave = df_chiave_anno %>%
  filter(is.na(chiave_comune))
check_df_chiave_anno_without_chiave = df_chiave_anno_without_chiave %>%
  group_by(abi, ndg, anno) %>%
  summarise(COUNT = n())
if (nrow(check_df_chiave_anno_without_chiave) != nrow(df_chiave_anno_without_chiave)){cat('\n errore in df_chiave_anno_without_chiave')}

df_chiave_anno_final = df_chiave_anno_with_chiave %>%
  rbind(df_chiave_anno_without_chiave) %>%
  
  mutate(CHIAVE_VAR = ifelse(is.na(chiave_comune), paste('CH_NA_ABI_', abi, '_NDG_', ndg, '_ANNO_', anno, sep = ''),
                             paste('CH_', chiave_comune, '_ABI_NA_NDG_NA_ANNO_', anno, sep = ''))) %>%
  select(-chiave_comune, -abi, -ndg, -anno, -FULL_NA, -COUNT_NA)


df_final_recover = df_final %>%
  mutate(CHIAVE_VAR = ifelse(is.na(chiave_comune), paste('CH_NA_ABI_', abi, '_NDG_', ndg, '_ANNO_', anno, sep = ''),
                             paste('CH_', chiave_comune, '_ABI_NA_NDG_NA_ANNO_', anno, sep = ''))) %>%
  select(-starts_with('var')) %>%
  left_join(df_chiave_anno_final) %>%
  select(-CHIAVE_VAR)

sum(is.na(df_final)) - sum(is.na(df_final_recover))



ind_set_to_NA = which(df_final_recover$anno == 2014 & df_final_recover$cod_de == '')
col_to_set_to_NA = df_final_recover %>% select(ARIC_VPROD:valprod_riman,  debiti_breve:pass_imm) %>% colnames()
df_final_recover[ind_set_to_NA, col_to_set_to_NA] = NA

df_final_recover = df_final_recover %>%
  left_join(
    df_final_recover %>%
      group_by(chiave_comune) %>%
      summarise(num_relationships = uniqueN(abi)) %>%
      filter(!is.na(chiave_comune))
  ) %>%
  mutate(flag_pluriaff_new = ifelse(num_relationships > 1, 1, 0))






anagrafica = read.csv("C:/Users/Alessandro Bitetto/Downloads/anag_imp_SF.csv", sep=';', stringsAsFactors = F) %>%
  mutate(ndg = as.character(ndg),
         abi = as.numeric(abi),
         anno = as.numeric(anno)) %>%
  mutate(ateco = ifelse(nchar(ateco) == 2, paste0('0', ateco), ateco)) %>%
  mutate(ateco = ifelse(nchar(ateco) == 3, paste0('0', ateco), ateco)) %>%
  mutate(provincia = ifelse(is.na(provincia), '', provincia)) %>%
  mutate(provincia = ifelse(grepl('Forl', provincia), 'Forli-Cesena', provincia))

anagr_abi_ndg = anagrafica %>%
  select(abi:ndg, cod_tipo_ndg:segmento, -anno, -data_rif, -data_rif_anno, -length_relationship) %>%
  unique()
check_anagr_abi_ndg = anagr_abi_ndg %>%
  group_by(abi, ndg) %>%
  summarize(COUNT = n())
if (nrow(check_anagr_abi_ndg) != nrow(anagr_abi_ndg)){cat('\n errore in anagr_abi_ndg')}

intersect(colnames(anagr_abi_ndg), colnames(df_final_recover))
anagr_abi_ndg = anagr_abi_ndg %>%
  rename(ateco_NEW = ateco,
         provincia_NEW = provincia,
         segmento_NEW = segmento)

anagr_abi_ndg_anno = anagrafica %>%
  select(abi, ndg, anno, gg_sconf:utilizzato) %>%
  unique()
check_anagr_abi_ndg_anno = anagr_abi_ndg_anno %>%
  group_by(abi, ndg, anno) %>%
  summarize(COUNT = n())
if (nrow(check_anagr_abi_ndg_anno) != nrow(anagr_abi_ndg_anno)){cat('\n errore in anagr_abi_ndg_anno')}
intersect(colnames(anagr_abi_ndg_anno), colnames(df_final_recover))



df_final_recover_with_anagrafica = df_final_recover %>%
  mutate(provincia = ifelse(grepl('Forl', provincia), 'Forli-Cesena', provincia)) %>%
  mutate(ateco = ifelse(nchar(ateco) == 2, paste0('00', ateco), ateco)) %>%
  mutate(ateco = ifelse(nchar(ateco) == 3, paste0('0', ateco), ateco)) %>%
  left_join(anagr_abi_ndg) %>%
  left_join(anagr_abi_ndg_anno) %>%
  mutate(ateco_NEW = ifelse(is.na(ateco_NEW), ateco, ateco_NEW)) %>%
  mutate(provincia_NEW = ifelse(is.na(provincia_NEW), provincia, provincia_NEW)) %>%
  mutate(segmento_NEW = ifelse(is.na(segmento_NEW), segmento, segmento_NEW)) %>%
  mutate(provincia = ifelse(provincia == '', provincia_NEW, provincia)) %>%
  mutate(ateco = ifelse(ateco == '', ateco_NEW, ateco)) %>%
  mutate(segmento = ifelse(segmento == '', segmento_NEW, segmento)) %>%
  mutate(ateco_2digit = substr(ateco, 1, 2)) %>%
  select(-ateco_NEW, -provincia_NEW, -segmento_NEW) %>%
  mutate(length_relationship = anno - start_relationship)
if (nrow(df_final_recover) != nrow(df_final_recover_with_anagrafica)){cat('\n errore in df_final_recover_with_anagrafica')}


write_dta(df_final_recover_with_anagrafica %>% select(-CHIAVE), 'C:/Users/Alessandro Bitetto/Downloads/full_dataset.dta')










df_restricted <- read_dta("C:/Users/Alessandro Bitetto/Downloads/full dataset_Trade Credit_12.04.2020 (panel data).dta")
dta1 <- read_dta("C:/Users/Alessandro Bitetto/Downloads/file2.dta")

total_minu
total_mete


dd = df_restricted %>%
  select(abi, ndg, uti_su_acc_revoca_Y) %>%
  left_join(dta1 %>%
              select(id, bid, utisuaccrevocaY),
            by = c('abi' = 'id', 'ndg' = 'bid'))



