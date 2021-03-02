


#########################     run Rstudio as Administrator     <<<<--------------------



# to run keras on GPU:
# http://narenakurati.com/keras_r_cuda_install/     <<<<------
# download and install CUDA toolkit 10.1 (the only supported in TF 1.2.1) - UPDATE: still works with TF 2.3-gpu
# download and extract cudnn (relative to cuda 10.1) into the corresponding folder
# https://stackoverflow.com/questions/59823283/could-not-load-dynamic-library-cudart64-101-dll-on-tensorflow-cpu-only-install
# https://keras.rstudio.com/reference/install_keras.html


install.packages("keras")   # remotes::install_github("rstudio/keras")
library(keras)
library(tensorflow)
install_tensorflow(version = "2.2.0")

library(reticulate)
reticulate::conda_create("R_keras")
reticulate::use_condaenv("R_keras", required = TRUE)
# install_keras(method = "conda", tensorflow = "1.2.1-gpu")
install_keras(method = "conda", tensorflow = "2.3-gpu")


reticulate::py_config() 
reticulate::py_module_available("keras")
# Successfully opened dynamic library cudart64_101.dll


library(tensorflow)
tf$config$experimental$list_physical_devices()
# Successfully opened dynamic library cudart64_101.dll  and check no other .dll are missing (in case, download the corresponding cudnn package)



# test GPU
library(keras)
library(tfdatasets)
library(dplyr)

boston_housing <- dataset_boston_housing()

c(train_data, train_labels) %<-% boston_housing$train
c(test_data, test_labels) %<-% boston_housing$test


column_names <- c('CRIM', 'ZN', 'INDUS', 'CHAS', 'NOX', 'RM', 'AGE', 
                  'DIS', 'RAD', 'TAX', 'PTRATIO', 'B', 'LSTAT')

train_df <- train_data %>% 
  as_tibble(.name_repair = "minimal") %>% 
  setNames(column_names) %>% 
  mutate(label = train_labels)

test_df <- test_data %>% 
  as_tibble(.name_repair = "minimal") %>% 
  setNames(column_names) %>% 
  mutate(label = test_labels)

spec <- feature_spec(train_df, label ~ . ) %>% 
  step_numeric_column(all_numeric(), normalizer_fn = scaler_standard()) %>% 
  fit()

layer <- layer_dense_features(
  feature_columns = dense_features(spec), 
  dtype = tf$float32
)
layer(train_df)

input <- layer_input_from_dataset(train_df %>% select(-label))

output <- input %>% 
  layer_dense_features(dense_features(spec)) %>% 
  layer_dense(units = 64, activation = "relu") %>%
  layer_dense(units = 64, activation = "relu") %>%
  layer_dense(units = 1) 

model <- keras_model(input, output)

model %>% 
  compile(
    loss = "mse",
    optimizer = optimizer_rmsprop(),
    metrics = list("mean_absolute_error")
  )

build_model <- function() {
  input <- layer_input_from_dataset(train_df %>% select(-label))
  
  output <- input %>% 
    layer_dense_features(dense_features(spec)) %>% 
    layer_dense(units = 64, activation = "relu") %>%
    layer_dense(units = 64, activation = "relu") %>%
    layer_dense(units = 1) 
  
  model <- keras_model(input, output)
  
  model %>% 
    compile(
      loss = "mse",
      optimizer = optimizer_rmsprop(),
      metrics = list("mean_absolute_error")
    )
  
  model
}

print_dot_callback <- callback_lambda(
  on_epoch_end = function(epoch, logs) {
    if (epoch %% 80 == 0) cat("\n")
    cat(".")
  }
)    

model <- build_model()

history <- model %>% fit(
  x = train_df %>% select(-label),
  y = train_df$label,
  epochs = 500,
  validation_split = 0.2,
  verbose = 0,
  callbacks = list(print_dot_callback)
)
