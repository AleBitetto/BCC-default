
# file dimension, missing, blanks and columns format
file_stats = function(x){
  file = read.csv(x, sep=';', stringsAsFactors = F)
  dim = dim(file)
  missing = 0
  missing_perc = 0
  blank = 0
  blank_perc = 0
  num_col = 0
  char_col = 0
  if (dim[1] > 0){
    missing = sum(is.na(file))
    missing_perc = round(missing / prod(dim) * 100)
    blank = sum(file == '', na.rm = T)
    blank_perc = round(blank / prod(dim) * 100)
    char_col = length(which(sapply(file, is.character)))
  }
  rm(file)
  
  return(data.frame(rows = dim[1],
                    cols = dim[2],
                    numeric_cols = dim[2] - char_col,
                    string_cols = char_col,
                    missing = missing,
                    blank = blank,
                    missing_perc = missing_perc,
                    blank_perc = blank_perc)
  )
}