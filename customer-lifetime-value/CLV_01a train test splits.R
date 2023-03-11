# Fader/Hardie Beta-Geometric/Beta-Bernoulli CLV Model
# R. Wagner, 2021

# create training/testing splits, re-calculates x/t_x/n on training matrix

months <- months[months != tail(months, 1)] # exclude current month; can only consider completed months
first_cohort <- '2020-01-01' # declare first cohort

# training/testing sets (testing = most recent n months)
holdout_periods <- 5 
testing_months <- tail(months, holdout_periods)

# include first cohort's initial month for their t_x calcs
training_months <- subset(months, months >= first_cohort & !(months %in% testing_months)) 

# can only train on cohorts up to penultimate period of training months
training_cohorts <- subset(months, months >= first_cohort & months < tail(training_months, 1))

# training version of matrix (i.e., subset to training cohorts/order months only)
df_order_matrix_train <- subset(df_order_matrix, cohort_month %in% training_cohorts, select=c("user_id", paste0('t', training_months)))

# recalc x/t_x/n values in training matrix
df_order_matrix_train$x <- apply(df_order_matrix_train[,paste0("t",training_months)], 1, function(x) sum(!is.na(x))-1) # rm initial order
df_order_matrix_train$tx <- apply(df_order_matrix_train[,paste0("t",training_months)], 1, function(x) max(which(!is.na(x))) - min(which(!is.na(x))) )
df_order_matrix_train$n <- apply(df_order_matrix_train[,paste0("t",training_months)], 1, function(x) length(training_months) - min(which(!is.na(x))))
df_order_matrix_train$x_tx_n <- with(df_order_matrix_train, paste(x, tx, n, sep="_"))

# testing version of matrix (training cohorts, testing order months)
df_order_matrix_testing <- subset(df_order_matrix, cohort_month %in% training_cohorts, select=c("user_id", paste0('t', testing_months)))

# merge x/t_x/n values to testing matrix; x/t_x/n values are NOT recalc'd for testing months
df_order_matrix_testing <- merge(df_order_matrix_testing, 
                                      subset(df_order_matrix_train, select = c('user_id', 'x', 'tx', 'n', 'x_tx_n')),
                                      by = 'user_id')


# append cohorts to training matrix
df_order_matrix_train <- merge(df_order_matrix_train, cohort_assignments[, c('user_id', 'cohort_month')],
                                       by = 'user_id')

# create x/tx/n summary matrix from training matrix
df_xtxn_train <- df_order_matrix_train %>%
  group_by(x, tx, n) %>%
  summarize(count = n()) %>%
  arrange(desc(n), desc(tx), desc(x)) %>%
  mutate(x_tx_n = paste(x, tx, n, sep="_"))
