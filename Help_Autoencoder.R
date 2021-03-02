# Autoencoder for dimensionality reduction
Autoencoder_PC = function(x_train, n_comp = 2, epochs = 300, batch_size = 400, layer_list = c(16, 8, 16, 6),
                          act_fun = 'relu', latent_act_fun = 'selu', NA_masking = NULL, masking_value = NULL, verbose = 1,
                          save_RDS = F, save_RDS_additional_lab = '', save_model = F, save_model_name = '', evaluate_embedding = TRUE){
  
  # layer_list: number of neurons for each layer. Central layer has n_comp neurons and name = "bottleneck" and then the sequence is inverted for the decoder
  # NA_masking: value to replace NA in in x_train
  # masking_value: value to be used as mask in masking_layer
  # verbose : 0 - silent, 1 - progress bar, 2 - progress bar one line for each epoch
  # evaluate_embedding: if false embedding is not returned (speed up tuning) 
  
  if (!is.null(NA_masking)){
    x_train = x_train %>%
      replace(is.na(.), NA_masking)
    masking = is.na(x_train)
  } else {
    masking = matrix(FALSE, ncol = ncol(x_train), nrow = nrow(x_train))
  }
  
  if (save_model & save_model_name == ''){stop('Please provide a name for hdf5 file: save_model_name')}
  
  original_vars_name = colnames(x_train)
  
  # set training data
  x_train <- as.matrix(x_train)
  
  # set model
  # use_session_with_seed(666, disable_gpu = F)
  tensorflow::tf$random$set_seed(666)
  # model <- keras_model_sequential()
  # model %>%
  #   layer_dense(units = 16, activation = act_fun, input_shape = ncol(x_train)) %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = 8, activation = act_fun) %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = 16, activation = act_fun) %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = 6, activation = act_fun) %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = n_comp, activation = latent_act_fun, name = "bottleneck") %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = 6, activation = act_fun) %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = 16, activation = act_fun) %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = 8, activation = act_fun) %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = 16, activation = act_fun) %>%
  #   layer_batch_normalization() %>%
  #   layer_dense(units = ncol(x_train))
  cc = 1
  eval_text = "model"
  if (!is.null(masking_value)){eval_text = paste0(eval_text, " %>% layer_masking(mask_value = ", masking_value, ", input_shape = ", ncol(x_train), ")")}
  for (l in c(layer_list, -1, rev(layer_list))){   # -1 is the bottleneck
    eval_text = paste0(eval_text, " %>% layer_dense(units = ", ifelse(l == -1, n_comp, l), ", activation = \"",
                       ifelse(l == -1, latent_act_fun, act_fun), "\"",
                       ifelse(cc == 1, paste0(', input_shape = ', ncol(x_train)), ""),
                       ifelse(l == -1, ", name = \"bottleneck\"", ""),
                       ") %>% layer_batch_normalization()")
    cc = cc + 1
  }
  eval_text = paste0(eval_text, "%>% layer_dense(units = ", ncol(x_train), ")")
  model <- keras_model_sequential() 
  eval(parse(text = eval_text))
  
  # compile model
  model %>% keras::compile(
    loss = "mean_squared_error", 
    optimizer = optimizer_adam(lr = 0.001, decay = 0.0001, amsgrad = F, clipnorm = 1)
    #optimizer_sgd(momentum = 0.9, decay = 0.0001, nesterov = T, clipnorm = 1)
  )
  
  # fit model
  history = model %>% fit(
    x = x_train, 
    y = x_train, 
    epochs = epochs,
    batch_size = batch_size,
    validation_split = 0.2,
    verbose = verbose,
    # callbacks = list(
    #   callback_reduce_lr_on_plateau(monitor = "val_loss", factor = 0.9)
    # callback_early_stopping(monitor = "val_loss", min_delta = 1, patience = 100,
    #                         verbose = 0, mode = "auto", restore_best_weights = T)
    # )
  )
  
  # evaluate the performance of the model
  # mse <- evaluate(model, x_train, x_train)
  
  # extract the bottleneck layer
  intermediate_layer_model <- keras_model(inputs = model$input, outputs = get_layer(model, "bottleneck")$output)
  if (evaluate_embedding){
    intermediate_output <- predict(intermediate_layer_model, x_train)
  } else {
    intermediate_output = NULL
  }
  
  # reconstruction prediction
  full_output = predict(model, x_train)
  
  # save and return
  recRMSE = mean((as.matrix(x_train) - full_output)^2) %>% sqrt()
  AvgAbsInput = mean(abs(x_train) %>% as.matrix())
  recRMSE_no_mask = mean((as.matrix(x_train)[!masking] - full_output[!masking])^2) %>% sqrt()
  # model mse is sum((as.matrix(x_input_full)[!masking] - full_output[!masking])^2) / prod(dim(masking))
  AvgAbsInput_no_mask = mean(abs(as.matrix(x_train))[!masking])
  R2 = round(eval_R2(x_train, full_output) * 100, 1)
  R2_99 = round(eval_R2(x_train, full_output, 0.99) * 100, 1)
  R2_95 = round(eval_R2(x_train, full_output, 0.95) * 100, 1)
  
  out = list(embedding_dim = n_comp,
             Embedding = intermediate_output,
             AvgAbsInput = AvgAbsInput,
             ReconstErrorRMSE = recRMSE,
             ReconstErrorRMSE_no_mask = recRMSE_no_mask,
             AvgAbsInput_no_mask = AvgAbsInput_no_mask,
             R2 = R2,
             R2_99 = R2_99,
             R2_95 = R2_95,
             history = history,
             reconstr_prediction = full_output,
             original_vars_name = original_vars_name,
             options = list(n_comp = n_comp,
                            epochs = epochs,
                            batch_size = batch_size,
                            act_fun = act_fun,
                            latent_act_fun = latent_act_fun,
                            layer_list = layer_list,
                            masking_value = masking_value),
             report = data.frame(Embedding_Dimension = n_comp,
                                 rr = paste0(round(recRMSE, 4), ' (', round(recRMSE / AvgAbsInput * 100, 1), '%)'),
                                 Reconst_RMSE_no_mask = round(recRMSE_no_mask, 4),
                                 R2 = R2,
                                 R2_99 = R2_99,
                                 R2_95 = R2_95, stringsAsFactors = F) %>%
               rename(`Reconst_RMSE (% of AvgAbsInput)` = rr))
  
  if (save_RDS){
    out_label = paste0(ifelse(save_RDS_additional_lab != '', paste0(save_RDS_additional_lab, '_'), ''),
                       n_comp, '_', batch_size, '_', act_fun, '_', latent_act_fun, '_', paste0(layer_list, collapse = '.'), '.rds')
    saveRDS(out, paste0('./Distance_to_Default/Stats/Autoencoder_test/', out_label))
  }
  
  if (save_model){
    intermediate_layer_model %>% save_model_hdf5(paste0(save_model_name, '.h5'))
  }
  
  return(out)
  
}

# tuning Autoencoder
autoencoder_tuning = function(dataset, NA_masking, masking_value, save_RDS_additional_lab, batch_size = 500, epochs = 300,
                              max_iter = 80, design_iter = 15){
  
  # https://mlrmbo.mlr-org.com/articles/supplementary/mixed_space_optimization.html
  
  # save_RDS_additional_lab = 'prova'
  # dataset = df_emb_input_wide
  # NA_masking = 0
  # masking_value = 0
  # 
  # 
  # batch_size = 500
  # epochs = 3
  
  layers_generator = function(layer_scaling, layer_spread, total_layers){
    return((exp(-layer_scaling * c(1:total_layers) * layer_spread) * ncol(dataset)) %>% ceiling() + 1)
  }
  
  optimization_results = c()
  for (layers_number in c(1:4)){   # test different number of layers in the encoder/decoder
    
    cat('\n -------------- testing', layers_number, 'layers --------------')
    
    fun = function(x) {
      
      # layer_list = x$layer_list
      act_fun = x$act_fun
      latent_act_fun = x$latent_act_fun
      # batch_size = x$batch_size
      # n_comp = x$n_comp
      layer_scaling = x$layer_scaling   # exp(-layer_scaling * layer_spread) so to evaluate decreasing number of layers
      layer_spread = x$layer_spread     # with more or less exponential growth (decrease)
      # layers_number = 1
      # layer_scaling = 5     # in [0.1, 5]
      # layer_spread = 0.2    # in [0.001, 1]
      layer_list = layers_generator(layer_scaling, layer_spread, total_layers = layers_number + 1)
      n_comp = layer_list[length(layer_list)]
      layer_list = layer_list[-length(layer_list)]
      
      if (n_comp >= round(0.6 * ncol(dataset))){
        perf = 999                                  # force bottleneck not to be too large
      } else {
        out_label = paste0(ifelse(save_RDS_additional_lab != '', paste0(save_RDS_additional_lab, '_'), ''),
                           n_comp, '_', batch_size, '_', act_fun, '_', latent_act_fun, '_', paste0(layer_list, collapse = '.'), '.rds')
        reload_err = try(aut <- suppressWarnings(readRDS(paste0('./Distance_to_Default/Stats/Autoencoder_test/', out_label))), silent = T)
        
        if (class(reload_err) == "try-error"){
          aut = Autoencoder_PC(dataset, n_comp = n_comp, epochs = epochs, batch_size = batch_size, layer_list = layer_list,
                               act_fun = act_fun, latent_act_fun = latent_act_fun, verbose = 0, NA_masking = NA_masking, masking_value = masking_value,
                               save_RDS = T, save_RDS_additional_lab = save_RDS_additional_lab, evaluate_embedding = FALSE)
        } else {
          cat('\n Reloaded')
        }
        perf = aut$ReconstErrorRMSE ^ 2   # MSE so to be comparable with past evaluations
      }
      return(perf)
    }
    
    objfun = makeSingleObjectiveFunction(
      name = "autoencoder",
      fn = fun,
      par.set = makeParamSet(
        # makeIntegerVectorParam("layer_list", len=layers_number, lower = 3,upper = ncol(df_emb_input)),
        makeNumericParam("layer_scaling", 0.1, 5),
        makeNumericParam("layer_spread", 0.001, 1),
        makeDiscreteParam("act_fun", values = c("sigmoid", "tanh")),
        makeDiscreteParam("latent_act_fun", values = c("relu", "selu"))
        # makeIntegerParam("batch_size", 300, 1000)
        # makeIntegerParam("n_comp", 2, round(0.7 * ncol(dataset)))
      ),
      has.simple.signature = FALSE,
      minimize = TRUE
    )
    
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
    
    design = generateDesign(n = design_iter, par.set = getParamSet(objfun))
    
    start = Sys.time()
    mlr::configureMlr(show.info = FALSE, show.learner.output = FALSE, on.learner.warning = "quiet")
    results = mbo(objfun, design = design, learner = surr.rf, control = control, show.info = F)
    tot_diff=seconds_to_period(difftime(Sys.time(),start, units='secs'))
    
    
    optim_step = as.data.frame(results$opt.path, stringsAsFactors = F)
    optim_step = optim_step %>%
      mutate(layers = layers_number) %>%
      rowwise() %>%
      mutate(layers_list = layers_generator(layer_scaling, layer_spread, layers) %>% paste0(collapse = '.') %>% as.character()) %>%
      mutate(n_comp = layers_generator(layer_scaling, layer_spread, layers + 1)[layers + 1]) %>%
      select(layers, layers_list, n_comp, everything()) %>%
      select(-layer_scaling, -layer_spread) %>%
      as.data.frame()
    optimization_results = optimization_results %>%
      bind_rows(
        optim_step %>%
          mutate(
            final_state = results$final.state,
            total_time = paste0(lubridate::hour(tot_diff), 'h:', lubridate::minute(tot_diff), 'm:', round(lubridate::second(tot_diff))),
            RDS_path = paste0('./Distance_to_Default/Stats/Autoencoder_test/', save_RDS_additional_lab, '_',
                              n_comp, '_', batch_size, '_', act_fun, '_', latent_act_fun, '_', layers_list, '.rds')) %>%
          rename(MSE = y)
      )%>%
      select(layers, starts_with("layer_list"), everything())
    write.table(optimization_results, paste0('./Distance_to_Default/Stats/Autoencoder_test/00_Optimization_list_', save_RDS_additional_lab, '.csv'), sep = ';', row.names = F, append = F)
  } # layers_number
  
  return(optimization_results)
}

# create LSTM input reshaping dataframe into list of matrix
create_lstm_input = function(df, df_header, ID_col = '', TIME_col = '', NA_masking = NULL){
  
  df_work = df_header %>%
    select(all_of(c(ID_col, TIME_col))) %>%
    expand(!!sym(ID_col), !!sym(TIME_col)) %>%
    left_join(df_header %>%
                select(all_of(c(ID_col, TIME_col))) %>%
                bind_cols(df), by = c(ID_col, TIME_col))
  if (nrow(df_work) != (df_header %>% select(all_of(ID_col)) %>% uniqueN()) * (df_header %>% select(all_of(TIME_col)) %>% uniqueN())){
    cat('\n\n################ warning: number of expected combination doesn\'t match')
  }
  if (sum(!is.na(df)) != sum(!is.na(df_work %>% select(-all_of(c(ID_col, TIME_col)))))){
    cat('\n\n################ warning: non-missing observation lost')
  }
  
  # apply masking
  if (!is.null(NA_masking)){df_work = df_work %>% replace(is.na(.), NA_masking)}
  
  # time_series = list()
  # ID_order = c()
  # for (ID in (df_work %>% pull(ID_col) %>% unique())){
  #   tt = df_work %>%
  #     filter(!!sym(ID_col) == ID) %>%
  #     arrange(!!sym(TIME_col)) %>%
  #     select(-all_of(c(ID_col, TIME_col))) %>%
  #     as.matrix()
  #   
  #   time_series = c(time_series, list(tt))
  #   ID_order = c(ID_order, ID)
  # }
  # names(time_series) = ID_order
  
  
  time_series = df_work %>%
    group_by(!!sym(ID_col)) %>%
    arrange(!!sym(TIME_col)) %>%
    group_split(.keep = TRUE) %>%
    as.list()
  
  time_series_name = lapply(time_series, function(x) x %>% pull(all_of(ID_col)) %>% unique()) %>% unlist()
  time_series = lapply(time_series, function(x) x %>% select(-all_of(c(ID_col, TIME_col))) %>% as.matrix())
  names(time_series) = time_series_name
  
  return(time_series)
}

# custom LSTM layer to keep masking with layer_repeat_vector()
lstm_with_mask <- keras::Layer(
  "lstm_with_mask",
  
  initialize = function(RNN_type = 'lstm',  # or 'gru'
                        lstm_units = NULL,
                        in_shp = NULL,
                        bidirectional_flag = FALSE,
                        return_sequences = FALSE,
                        kernel_regularizer = NULL,
                        activation = 'tanh',   # using following default parameters allow to use cuDNN faster implementation
                        recurrent_activation = 'sigmoid',
                        use_bias = TRUE,
                        recurrent_dropout = 0,
                        unroll = FALSE,
                        reset_after = TRUE   # only for GRU
  ) {
    
    super()$`__init__`()
    self$lstm_units <- lstm_units
    self$in_shp <- in_shp
    self$activation <- activation
    self$bidirectional_flag <- bidirectional_flag
    self$return_sequences <- return_sequences
    if (RNN_type == 'lstm'){
      self$lstm_layer <- layer_lstm(units = lstm_units, input_shape = in_shp,
                                    activation = activation,recurrent_activation = recurrent_activation,
                                    recurrent_dropout = recurrent_dropout, unroll = unroll, use_bias = use_bias,
                                    return_sequences = return_sequences, kernel_regularizer = kernel_regularizer)
    } else if (RNN_type == 'gru'){
      self$lstm_layer <- layer_gru(units = lstm_units, input_shape = in_shp, reset_after = reset_after,
                                   activation = activation,recurrent_activation = recurrent_activation,
                                   recurrent_dropout = recurrent_dropout, unroll = unroll, use_bias = use_bias,
                                   return_sequences = return_sequences, kernel_regularizer = kernel_regularizer)
    }
    self$repeat_layer <- layer_repeat_vector(n=in_shp[1])   # timestep
  },
  
  compute_mask = function(input, mask=NULL) {
    return(mask)
  },
  
  call = function(x, training = FALSE, mask = NULL) {
    
    out = self$lstm_layer(x)
    if (self$bidirectional_flag){out = bidirectional(out)}
    
    if (self$return_sequences == FALSE){out = self$repeat_layer(out)}
    
    return(out)
  }
)

# LSTM Autoencoder for dimensionality reduction
AutoencoderLSTM_PC = function(x_input, n_comp = 2, epochs = 300, batch_size = 400, layer_list = c(100, 50), kernel_reg_alpha = NULL,
                              RNN_type = 'lstm', temporal_embedding = FALSE, timestep_label = c(), masking_value = NULL, verbose = 1,
                              save_RDS = F, save_RDS_additional_lab = '', save_model = F, save_model_name = '', evaluate_embedding = TRUE){
  
  # layer_list: number of neurons for each layer. Central layer has n_comp neurons and name = "bottleneck" and then the sequence is inverted for the decoder
  # timestep_label: must be sorted from oldest to newest (same as in construction of x_input with create_lstm_input()). Used if temporal_embedding == TRUE
  # temporal_embedding: if FALSE one embedding for each observations is returned (layer_repeat_vector is included in bottleneck).
  #                     Embedding is the last state of return_sequence = TRUE. If TRUE, timestep embeddings for each observations
  #                     are returned (layer_repeat_vector is included in bottleneck). E.g. c('2012', '2013', '2014')
  # kernel_reg_alpha: apply regularization to lstm kernel cells (input weights). Default value of 0.01 is splitted by L1 (alpha = 0) and L2 (alpha = 1).
  #                   If NULL no regularization
  # RNN_type: 'lstm' or 'gru'
  # masking_value: value to be used as mask in masking_layer
  # verbose : 0 - silent, 1 - progress bar, 2 - progress bar one line for each epoch
  # evaluate_embedding: if false embedding is not returned (speed up tuning)
  
  # https://stackoverflow.com/questions/48714407/rnn-regularization-which-component-to-regularize
  
  # masking_value = 0
  # x_input
  # n_comp = 10
  # epochs = 300
  # batch_size = 400
  # layer_list = c(16, 8, 16, 6)
  # kernel_reg_alpha = 0.5
  # RNN_type = 'lstm'
  # timestep_label = c()#c('2012', '2013', '2014')
  # temporal_embedding = FALSE
  # verbose = 1
  # save_RDS = F, save_RDS_additional_lab = '', save_model = F, save_model_name = '', evaluate_embedding = TRUE
  
  if (save_model & save_model_name == ''){stop('Please provide a name for hdf5 file: save_model_name')}
  if (temporal_embedding & is.null(timestep_label)){stop('Please provide timestep_label')}
  
  # save observations names
  x_input_obs_names = names(x_input) 
  names(x_input) = NULL
  x_input = np$array(x_input)
  x_input_dim = py_to_r(x_input$shape) %>% unlist()
  
  # create a copy of x_input, reshaping into a (obs*timestep)x(features) matrix including masked values
  x_input_full = array_reshape(x_input, c(prod(x_input_dim[1:2]), x_input_dim[3]))
  x_input_full = py_to_r(x_input_full)
  
  # save masking index
  if (!is.null(masking_value)){
    masking = x_input_full == masking_value
  } else {
    masking = matrix(FALSE, ncol = ncol(x_input_full), nrow = nrow(x_input_full))
  }
  
  # define kernel regularizer (if any)
  if (!is.null(kernel_reg_alpha)){
    kern_reg = regularizer_l1_l2(l1 = 0.01*(1-kernel_reg_alpha), l2 = 0.01*kernel_reg_alpha)
  } else {
    kern_reg = NULL
  }
  
  input_dim = x_input_dim[-1]   # first is #timesteps, second is #features
  tensorflow::tf$random$set_seed(666)
  # model <- keras_model_sequential()
  # model %>%
  #   layer_masking(mask_value = masking_value, input_shape = input_dim) %>%
  #   layer_lstm(units = 100, input_shape = NULL, return_sequences = TRUE, kernel_regularizer = kern_reg, activation='tanh', recurrent_activation = "sigmoid") %>%
  #   # layer_gru(units = 100, return_sequences = TRUE, kernel_regularizer = kern_reg, activation="tanh", recurrent_activation = "sigmoid", reset_after = TRUE) %>%
  #   lstm_with_mask(lstm_units = 50, in_shp = input_dim, return_sequences = temporal_embedding, kernel_regularizer = kern_reg, RNN_type = RNN_type) %>%
  #   layer_lstm(units = 50, input_shape = NULL, return_sequences = TRUE, kernel_regularizer = kern_reg, activation='tanh', recurrent_activation = "sigmoid") %>%
  #   layer_lstm(units = 100, input_shape = NULL, return_sequences = TRUE, kernel_regularizer = kern_reg, activation='tanh', recurrent_activation = "sigmoid") %>%
  #   time_distributed(layer_dense(units = input_dim[2]))
  cc = 1
  eval_text = "model"
  if (!is.null(masking_value)){eval_text = paste0(eval_text, " %>% layer_masking(mask_value = ", masking_value, ", input_shape = c(",
                                                  paste0(input_dim, collapse = ","), "))")}
  for (l in c(layer_list, -1, rev(layer_list))){   # -1 is the bottleneck
    if (l != -1){
      eval_text = paste0(eval_text, ifelse(RNN_type == 'lstm', " %>% layer_lstm(", " %>% layer_gru(reset_after = TRUE, "), "units = ", l,
                         ", return_sequences = TRUE, kernel_regularizer = kern_reg, activation=\"tanh\", recurrent_activation = \"sigmoid\")")
    } else {
      eval_text = paste0(eval_text, " %>% lstm_with_mask(lstm_units = ", n_comp, ", in_shp = c(", paste0(input_dim, collapse = ","), "), return_sequences = ",
                         temporal_embedding, ", kernel_regularizer = kern_reg, RNN_type = \"", RNN_type, "\")",
                         ifelse(RNN_type == 'lstm', " %>% layer_lstm(", " %>% layer_gru(reset_after = TRUE, "), "units = ", n_comp,
                         ", return_sequences = TRUE, kernel_regularizer = kern_reg, activation=\"tanh\", recurrent_activation = \"sigmoid\")")
    }
    cc = cc + 1
  }
  eval_text = paste0(eval_text, "%>% time_distributed(layer_dense(units = ", input_dim[2], "))")
  model <- keras_model_sequential() 
  eval(parse(text = eval_text))
  
  model %>% keras::compile(
    loss = "mean_squared_error",
    # optimizer = optimizer_rmsprop(lr = 0.001, rho = 0.0001, clipnorm = 1)   # better for RNN
    optimizer = optimizer_adam(lr = 0.001, decay = 0.0001, amsgrad = F, clipnorm = 1),
    #optimizer_sgd(momentum = 0.9, decay = 0.0001, nesterov = T, clipnorm = 1)
  )
  
  history = model %>% fit(
    x = x_input, 
    y = x_input, 
    epochs = epochs,
    batch_size = batch_size,
    validation_split = 0.2,
    verbose = verbose
    # callbacks = list(
    # callback_reduce_lr_on_plateau(monitor = "loss", factor = 0.9)   # val_loss
    # callback_early_stopping(monitor = "val_loss", min_delta = 1, patience = 100,
    #                         verbose = 0, mode = "auto", restore_best_weights = T)
    # )
  )
  
  # evaluate the performance of the model
  # mse <- evaluate(model, x_input, x_input)
  
  # extract the bottleneck layer
  bottleneck_layer = which(grepl('lstm_with_mask', lapply(model$layers, function(x) x$name) %>% unlist()))
  bottleneck_layer = (lapply(model$layers, function(x) x$name) %>% unlist())[bottleneck_layer]
  intermediate_layer_model <- keras_model(inputs = model$input, outputs = get_layer(model, bottleneck_layer)$output)
  if (evaluate_embedding){
    intermediate_output <- predict(intermediate_layer_model, x_input)
    if (temporal_embedding == FALSE){
      intermediate_output = intermediate_output[, 1, ]      # lstm_with_mask has layer_repeat_vector, so the embedding is repeated timesteps times
      rownames(intermediate_output) = x_input_obs_names
    } else {
      tt_list = list()
      for (temp_dim in 1:(dim(intermediate_output)[2])){  #  intermediate_output is obs x timestep x embedding_dimension
        tt = intermediate_output[, temp_dim, ]
        rownames(tt) = x_input_obs_names
        tt_list = c(tt_list, list(tt))
      }
      names(tt_list) = timestep_label
      intermediate_output = tt_list
      rm(tt_list, tt)
    }
  } else {
    intermediate_output = NULL
  }
  
  # reconstruction prediction
  full_output = predict(model, x_input)
  full_output = array_reshape(np$array(full_output), c(prod(dim(full_output)[1:2]), dim(full_output)[3]))   # reshape to (obs*timestep)x(features)
  full_output = py_to_r(full_output)
  
  # save and return
  recRMSE = mean((as.matrix(x_input_full) - full_output)^2) %>% sqrt()
  AvgAbsInput = mean(abs(x_input_full) %>% as.matrix())
  recRMSE_no_mask = mean((as.matrix(x_input_full)[!masking] - full_output[!masking])^2) %>% sqrt()
  # model mse is sum((as.matrix(x_input_full)[!masking] - full_output[!masking])^2) / prod(dim(masking))
  AvgAbsInput_no_mask = mean(abs(as.matrix(x_input_full))[!masking])
  R2 = round(eval_R2(x_input_full, full_output) * 100, 1)
  R2_99 = round(eval_R2(x_input_full, full_output, 0.99) * 100, 1)
  R2_95 = round(eval_R2(x_input_full, full_output, 0.95) * 100, 1)
  
  out = list(embedding_dim = n_comp,
             Embedding = intermediate_output,
             AvgAbsInput = AvgAbsInput,
             ReconstErrorRMSE = recRMSE,
             ReconstErrorRMSE_no_mask = recRMSE_no_mask,
             AvgAbsInput_no_mask = AvgAbsInput_no_mask,
             R2 = R2,
             R2_99 = R2_99,
             R2_95 = R2_95,
             history = history,
             reconstr_prediction = full_output,
             options = list(n_comp = n_comp,
                            epochs = epochs,
                            batch_size = batch_size,
                            kernel_reg_alpha = kernel_reg_alpha,
                            RNN_type = RNN_type,
                            timestep_label = timestep_label,
                            temporal_embedding = temporal_embedding,
                            layer_list = layer_list,
                            masking_value = masking_value),
             report = data.frame(Embedding_Dimension = n_comp,
                                 rr = paste0(round(recRMSE, 4), ' (', round(recRMSE / AvgAbsInput * 100, 1), '%)'),
                                 Reconst_RMSE_no_mask = round(recRMSE_no_mask, 4),
                                 R2 = R2,
                                 R2_99 = R2_99,
                                 R2_95 = R2_95, stringsAsFactors = F) %>%
               rename(`Reconst_RMSE (% of AvgAbsInput)` = rr))
  
  if (save_RDS){
    out_label = paste0(ifelse(save_RDS_additional_lab != '', paste0(save_RDS_additional_lab, '_'), ''),
                       n_comp, '_', batch_size, '_', RNN_type, '_', kernel_reg_alpha, '_', paste0(layer_list, collapse = '.'), '.rds')
    saveRDS(out, paste0('./Distance_to_Default/Stats/Autoencoder_test/', out_label))
  }
  
  if (save_model){
    intermediate_layer_model %>% save_model_hdf5(paste0(save_model_name, '.h5'))
  }
  
  return(out)
  
}