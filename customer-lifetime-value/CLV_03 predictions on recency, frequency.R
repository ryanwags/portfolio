# Fader/Hardie Beta-Geometric/Beta-Bernoulli CLV Model
# R. Wagner, 2021

# conditional expectations: predict expected number of transactions over interval (n, n+n*], conditional on an investor's x/t_x/n values

compute_ce <- function(mle_results, n_star, df_xtxn){
  
  a = mle_results$par[1]
  b = mle_results$par[2]
  g = mle_results$par[3]
  d = mle_results$par[4]
  x = df_xtxn$x
  n = df_xtxn$n
  
  beta_ab <- gamma(a)*gamma(b)/gamma(a+b)
  beta_gd <- gamma(g)*gamma(d)/gamma(g+d)
  
  lhs <- compute_lhs(abgd = c(a,b,g,d), df = df_xtxn) # x/t_x/n combo LHs
  remaining_terms <- ((gamma(a+x+1)*gamma(b+n-x)/gamma(a+b+n+1))/beta_ab) * (d/(g-1)) * (gamma(g+d)/gamma(1+d)) * ((gamma(1+d+n)/gamma(g+d+n)) - (gamma(1+d+n+n_star)/gamma(g+d+n+n_star)))
  cond_exp = (remaining_terms/lhs)
  
  return(cond_exp)

}

cohort_test <- '2020-05-01'
cohort_test_n <- unique(subset(df_order_matrix_train, cohort_month == cohort_test, select="n", drop=TRUE)) # cohort's N value
df_xtxn_train_ce <- subset(df_xtxn_train, n==cohort_test_n)

# calc expected number of transactions *per person* in each x/t_x/n combo
# choice of N* should always match the number of periods in testing set
df_xtxn_train_ce$ce <- compute_ce(mle_results = mle_results,
                                  n_star = holdout_periods, 
                                  df_xtxn = df_xtxn_train_ce) 

# total expected number of transactions for each x/t_x/n combo
df_xtxn_train_ce$ce_trn_pred <- (df_xtxn_train_ce$count * df_xtxn_train_ce$ce)

# eval conditional expectations on testing set -------------------------------------------

# sum each person's actual number of active months in testing period 
df_order_matrix_test_ce <- subset(df_order_matrix_test, n==cohort_test_n)

df_order_matrix_test_ce$ce_inv_actual <- apply(df_order_matrix_test_ce[,paste0("t",testing_months)], 
                                                  1, 
                                                  function(x) sum(!is.na(x))
                                                  )

# sum total number of transactions by x/t_x/n combo
ce_actual <- df_order_matrix_test_ce %>%
  group_by(x_tx_n) %>%
  summarize(ce_inv_actual = sum(ce_inv_actual))

df_xtxn_train_ce <- merge(df_xtxn_train_ce, ce_actual, by="x_tx_n") # merge to training x/t_x/n

# PREDICT VIA FREQUENCY (X)
# among users in each X category, calc observed/expected avg # transactions in testing period
df_ce_freq <- df_xtxn_train_ce %>%
  group_by(x) %>%
  summarize(n = sum(count), # total count of users
            actual = sum(ce_inv_actual),
            pred = sum(ce_trn_pred),
            ce_freq_actual = actual/n, # per capita
            ce_freq_pred = pred/n) # per capita

# example interpretation: X=3: among users who made 3 repeat transactions in training period, 
# OBSERVED avg # repeat transactions in testing period = 1.54, vs. PREDICTED avg = 1.34

ggplot(df_ce_freq) + 
  geom_line(aes(x=x, y=ce_freq_actual, color="Actual")) + # Observed E(X)
  geom_point(aes(x=x, y=ce_freq_actual, color="Actual")) +
  geom_line(aes(x=x, y=ce_freq_pred, color="Predicted")) + # Predicted E(X)
  geom_point(aes(x=x, y=ce_freq_pred, color="Predicted")) +
  scale_x_continuous(breaks=0:(nrow(df_ce_freq)-1)) +
  labs(title="Expected Avg Number of Repeat Transactions in Testing Period",
       subtitle = 'by Frequency (X) in Training Period',
       x="Number of Repeat Transactons in Training Period",
       y="Repeat Transactions in Testing Period",
       color="Legend") +
  theme(plot.margin = unit(c(1,1,1,1), "cm"),
        axis.text = element_text(face="bold", size=10),
        axis.title = element_text(face="bold", size=10),
        axis.title.x = element_text(vjust = -3),
        axis.title.y = element_text(vjust = 3),
        plot.title = element_text(face="bold", size=11),
        plot.subtitle = element_text(size = 9))

# PREDICT VIA RECENCY (Tx)
# among users whose last transaction in each t_x category, calc observed/expected avg # transactions in testing period
df_ce_rec <- df_xtxn_train_ce %>%
  group_by(tx) %>%
  summarize(n = sum(count), # total count of users
            actual = sum(ce_inv_actual),
            pred = sum(ce_trn_pred),
            ce_freq_actual = actual/n, # per capita
            ce_freq_pred = pred/n) # per capita

ggplot(df_ce_rec) + 
  geom_line(aes(x=tx, y=ce_freq_actual, color="Actual")) + # observed E(X)
  geom_point(aes(x=tx, y=ce_freq_actual, color="Actual")) +
  geom_line(aes(x=tx, y=ce_freq_pred, color="Predicted")) + # predicted E(X)
  geom_point(aes(x=tx, y=ce_freq_pred, color="Predicted")) +
  scale_x_continuous(breaks=0:(nrow(df_ce_rec)-1)) +
  labs(title="Expected Avg Number of Repeat Transactions in Testing Period",
       subtitle = 'by Recency (T_x) of Last Reinvestment in Training Period',
       x="Period of Most Recent Repeat Transaction",
       y="Repeat Transactions in Testing Period",
       color="Legend") +
  theme(plot.margin = unit(c(1,1,1,1), "cm"),
        axis.text = element_text(face="bold", size=10),
        axis.title = element_text(face="bold", size=10),
        axis.title.x = element_text(vjust = -3),
        axis.title.y = element_text(vjust = 3),
        plot.title = element_text(face="bold", size=11),
        plot.subtitle = element_text(size = 9))

