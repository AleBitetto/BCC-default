# visualize peers with embedding - OLD with GIF
{
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

# plot cluster for manual labelling
include_CRIF_cluster = F  # if TRUE clustering with CRIF median/percentiles is evaluated on CRIF_unmapped
{
  # manual clustering will be performed using:
  #   - ORBIS_mapped: mapped columns between peers and CRIF data
  #   - categorical_variables: additional categorical variables
  #   - CRIF_unmapped: unmapped: unmapped CRIF columns (median/quantile is evaluated on CRIF data itself) - only if include_CRIF_cluster == T
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