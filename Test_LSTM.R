


# https://machinelearningmastery.com/lstm-autoencoders/?unapproved=597585&moderation-hash=8edd3eafe2ead8ff4193e2641e9f80d0#comment-597585
# https://towardsdatascience.com/step-by-step-understanding-lstm-autoencoder-layers-ffab055b6352
# https://blogs.rstudio.com/ai/posts/2018-06-25-sunspots-lstm/


library(reticulate)
library(keras)



np <- import("numpy", convert = FALSE)





time_series = list()
set.seed(10)
for (s in 1:10){
  timesteps = 6
  features = 3
  time_series = c(time_series, list(matrix(sample(c(1:(timesteps*features)), replace = T), ncol = features)))
}

x_input = np$array(time_series)


####### model with input as list of matrix/np.array (no dataloader)


# in order to use cuDNN version of LSTM block use the following:
# activation == tanh
# recurrent_activation == sigmoid
# recurrent_dropout == 0
# unroll is False
# use_bias is True
# Inputs, if use masking, are strictly right-padded.
# Eager execution is enabled in the outermost context.


# tensorflow::tf$random$set_seed(666)
# model <- keras_model_sequential()
# model %>%
#   layer_lstm(units = 100, input_shape = c(timesteps, features), activation='tanh', recurrent_activation = "sigmoid", return_sequences = TRUE) %>%
#   # layer_lstm(units = 50, input_shape = c(timesteps, features), activation='tanh', recurrent_activation = "sigmoid", return_sequences = TRUE) %>%
#   # layer_lstm(units = 10, input_shape = c(timesteps, features), activation='tanh', recurrent_activation = "sigmoid", return_sequences = TRUE) %>%
#   layer_lstm(units = 50, input_shape = c(timesteps, features), activation='tanh', recurrent_activation = "sigmoid", return_sequences = FALSE, name = "bottleneck") %>%
#   layer_repeat_vector(timesteps) %>%
#   layer_lstm(units = 50, input_shape = c(timesteps, features), activation='tanh', recurrent_activation = "sigmoid", return_sequences = TRUE) %>%
#   # layer_lstm(units = 10, input_shape = c(timesteps, features), activation='tanh', recurrent_activation = "sigmoid", return_sequences = TRUE) %>%
#   # layer_lstm(units = 50, input_shape = c(timesteps, features), activation='tanh', recurrent_activation = "sigmoid", return_sequences = TRUE) %>%
#   layer_lstm(units = 100, input_shape = c(timesteps, features), activation='tanh', recurrent_activation = "sigmoid", return_sequences = TRUE) %>%
#   time_distributed(layer_dense(units = features))


model <- keras_model_sequential()
model %>%
  layer_lstm(units = 100, input_shape = c(timesteps, features), activation='relu', return_sequences = TRUE) %>%
  layer_lstm(units = 50, input_shape = c(timesteps, features), activation='relu', return_sequences = FALSE, name = "bottleneck") %>%
  layer_repeat_vector(timesteps) %>%
  layer_lstm(units = 50, input_shape = c(timesteps, features), activation='relu', return_sequences = TRUE) %>%
  layer_lstm(units = 100, input_shape = c(timesteps, features), activation='relu', return_sequences = TRUE) %>%
  time_distributed(layer_dense(units = features))


model %>% compile(
  loss = "mean_squared_error",
  # optimizer = optimizer_rmsprop()
  optimizer = optimizer_adam(lr = 0.001, decay = 0.0001, amsgrad = F, clipnorm = 1)
  #optimizer_sgd(momentum = 0.9, decay = 0.0001, nesterov = T, clipnorm = 1)
)

history = model %>% fit(
  x = x_input, 
  y = x_input, 
  epochs = 300,
  verbose = 2,
  # batch_size = 5
  validation_split = 0.2,
  # verbose = verbose,
  # callbacks = list(
  #   callback_reduce_lr_on_plateau(monitor = "val_loss", factor = 0.9)
    # callback_early_stopping(monitor = "val_loss", min_delta = 1, patience = 100,
    #                         verbose = 0, mode = "auto", restore_best_weights = T)
  # )
)

nn = np$array(time_series[1])
nn$shape
nn1 = time_series[1][[1]]
nn1[1,] = 0
nn1 = np$array(list(nn1))
nn1$shape


model(nn)
model(nn1)

intermediate_layer_model(nn)
intermediate_layer_model(nn1)

# loss
evaluate(model, x_input, x_input)


# extract the bottleneck layer
intermediate_layer_model <- keras_model(inputs = model$input, outputs = get_layer(model, "bottleneck")$output)
intermediate_output <- predict(intermediate_layer_model, x_input)

# reconstruction prediction
full_output = predict(model, x_input)

time_series[[1]]
full_output[1,,]

full_output1 = full_output




####### testing masking for sample with variable timesteps


time_series_var = list()
set.seed(10)
for (s in 1:10){
  timesteps_var = 6
  features = 3
  mask_rows = sample(c(1:timesteps_var), sample(1:(timesteps_var-1), 1), replace = F)
  mat = matrix(sample(c(1:(timesteps_var*features)), replace = T), ncol = features)
  mat[mask_rows, ] = -9999
  time_series_var = c(time_series_var, list(mat))
}

x_input_var = np$array(time_series_var)




lstm_with_mask <- Layer(
  "lstm_with_mask",
  
  initialize = function(lstm_units = NULL,
                        in_shp = NULL,
                        activation = NULL,
                        bidirectional_flag = FALSE) {
    
    super()$`__init__`()
    self$lstm_units <- lstm_units
    self$in_shp <- in_shp
    self$activation <- activation
    self$bidirectional_flag <- bidirectional_flag
    self$lstm_layer <- layer_lstm(units = lstm_units, input_shape = in_shp, 
                                  activation = activation, 
                                  return_sequences = FALSE)
    self$repeat_layer <- layer_repeat_vector(n=in_shp[1])   # timestep
  },
  
  compute_mask = function(input, mask=NULL) {
    return(mask)
  },
  
  call = function(x, training = FALSE, mask = NULL) {
    
    out = self$lstm_layer(x)
    if (self$bidirectional_flag){out = bidirectional(out)}

    return(self$repeat_layer(out))
  }
)


tensorflow::tf$random$set_seed(666)
model_var <- keras_model_sequential()
model_var %>%
  layer_masking(mask_value = -9999,input_shape = c(timesteps_var, features)) %>%
  layer_lstm(units = 100, input_shape = c(timesteps_var, features), activation='relu', return_sequences = TRUE) %>%
  lstm_with_mask(lstm_units = 50, in_shp = c(timesteps_var, features), activation='relu') %>%
  layer_lstm(units = 50, input_shape = c(timesteps_var, features), activation='relu', return_sequences = TRUE) %>%
  layer_lstm(units = 100, input_shape = c(timesteps_var, features), activation='relu', return_sequences = TRUE) %>%
  time_distributed(layer_dense(units = features))


# check if masking is correctly passed till the end
# for (ll in model_var$layers){
#   cat('\n-----')
#   print(ll$name)
#   print(ll$input_mask)
#   print(ll$output_mask)
# }



model_var %>% compile(
  loss = "mean_squared_error",
  # optimizer = optimizer_rmsprop(lr = 0.001, rho = 0.0001, clipnorm = 1)   # better for RNN
  optimizer = optimizer_adam(lr = 0.001, decay = 0.0001, amsgrad = F, clipnorm = 1),
  #optimizer_sgd(momentum = 0.9, decay = 0.0001, nesterov = T, clipnorm = 1)
)

history = model_var %>% fit(
  x = x_input_var, 
  y = x_input_var, 
  epochs = 300,
  verbose = 2,
  # batch_size = 5
  # validation_split = 0.2,
  # verbose = verbose,
  # callbacks = list(
  # callback_reduce_lr_on_plateau(monitor = "loss", factor = 0.9)   # val_loss
  # callback_early_stopping(monitor = "val_loss", min_delta = 1, patience = 100,
  #                         verbose = 0, mode = "auto", restore_best_weights = T)
  # )
)


# loss
evaluate(model_var, x_input_var, x_input_var)



nn = np$array(time_series_var[2])
evaluate(model_var, nn, nn)


nn$shape

nn
nn_hat = model_var(nn)
nn_hat



nn_R = py_to_r(nn)[,,]
mask = (nn_R != -9999)
nn_hat_R = nn_hat$numpy()[,,]


sqrt(sum((nn_hat_R[mask] - nn_R[mask])^2) / prod(dim(nn_R)))
sqrt(mean((nn_hat_R[mask] - nn_R[mask])^2) )


# extract the bottleneck layer
bottleneck_layer = which(grepl('lstm_with_mask', lapply(model_var$layers, function(x) x$name) %>% unlist()))
bottleneck_layer = (lapply(model_var$layers, function(x) x$name) %>% unlist())[bottleneck_layer]
intermediate_layer_model_var <- keras_model(inputs = model_var$input, outputs = get_layer(model_var, bottleneck_layer)$output)
intermediate_output <- predict(intermediate_layer_model_var, x_input_var)    # lstm_with_mask has the repeatVector layer, so the embedding is repeated timesteps times
intermediate_output = intermediate_output[, 1, ]

# reconstruction prediction
full_output_var = predict(model_var, x_input_var)

time_series_var[[1]]
full_output_var[1,,]




####### model with input from dataloader - create dataset from tensor (testing framework for ragged tensors - still not implemented) 
# https://github.com/rstudio/tfdatasets/issues/9

# have a look on my issue https://github.com/rstudio/keras/issues/1194


library(tfdatasets)
# Instantiates a toy dataset instance:
dataset <- tensor_slices_dataset(reticulate::tuple(x_input, x_input)) %>% 
  dataset_batch(10) %>%
  dataset_repeat()

history2 = model %>% fit(
  dataset,
  epochs = 300,
  steps_per_epoch = 2,
  verbose = 2
  # batch_size = batch_size,
  # validation_split = 0.2,
  # verbose = verbose,
  # callbacks = list(
  # callback_reduce_lr_on_plateau(monitor = "val_loss", factor = 0.9)
  # callback_early_stopping(monitor = "val_loss", min_delta = 1, patience = 100,
  #                         verbose = 0, mode = "auto", restore_best_weights = T)
  # )
)




######### ragged tensor - not supported yet

#  https://androidkt.com/training-rnn-model-with-variable-length-sequences-in-keras-using-raggedtensor/
#  https://stackoverflow.com/questions/62031683/ragged-tensors-as-input-for-lstm

time_series = list()
set.seed(10)
for (s in 1:10){
  timesteps = sample(c(3:10), 1)
  features = 3
  time_series = c(time_series, list(matrix(sample(c(1:(timesteps*features)), replace = T), ncol = features)))
}

x_input = np$array(time_series)

tensorflow::tf$random$set_seed(666)
model <- keras_model_sequential()
# model %>%

timestep_var = 6

input = layer_input(shape = shape(NULL, features), ragged = TRUE)
output = input %>%
  layer_lstm(units = 100, input_shape = NULL, activation='relu', return_sequences = TRUE) %>%
  layer_lstm(units = 50, input_shape = NULL, activation='relu', return_sequences = FALSE, name = "bottleneck") %>%
  layer_repeat_vector(timesteps) %>%
  layer_lstm(units = 50, input_shape = NULL, activation='relu', return_sequences = TRUE) %>%
  layer_lstm(units = 100, input_shape = NULL, activation='relu', return_sequences = TRUE) %>%
  time_distributed(layer_dense(units = features))


model = keras_model(inputs = input, outputs = output)
  
  layer_lstm(units = 100, input_shape = c(NULL, features), activation='relu', return_sequences = TRUE) %>%
  layer_lstm(units = 50, input_shape = NULL, activation='relu', return_sequences = FALSE, name = "bottleneck") %>%
  layer_repeat_vector(timesteps) %>%
  layer_lstm(units = 50, input_shape = NULL, activation='relu', return_sequences = TRUE) %>%
  layer_lstm(units = 100, input_shape = NULL, activation='relu', return_sequences = TRUE) %>%
  time_distributed(layer_dense(units = features))

  

model %>% compile(
  loss = "mean_squared_error", 
  optimizer = optimizer_adam(lr = 0.001, decay = 0.0001, amsgrad = F, clipnorm = 1)
  #optimizer_sgd(momentum = 0.9, decay = 0.0001, nesterov = T, clipnorm = 1)
)

history = model %>% fit(
  x = x_input, 
  y = x_input, 
  epochs = 300,
  verbose = 2
)

library(tfdatasets)
dataset <- tensor_slices_dataset(reticulate::tuple(tf$ragged$constant(x_input), tf$ragged$constant(x_input))) %>% 
  dataset_batch(10) %>%
  dataset_repeat()

as_iterator(dataset) %>% iter_next()

history2 = model %>% fit(
  dataset,
  epochs = 300,
  steps_per_epoch = 2,
  verbose = 2
  # batch_size = batch_size,
  # validation_split = 0.2,
  # verbose = verbose,
  # callbacks = list(
  # callback_reduce_lr_on_plateau(monitor = "val_loss", factor = 0.9)
  # callback_early_stopping(monitor = "val_loss", min_delta = 1, patience = 100,
  #                         verbose = 0, mode = "auto", restore_best_weights = T)
  # )
)
