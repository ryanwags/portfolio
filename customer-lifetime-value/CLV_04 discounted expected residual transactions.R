# Fader/Hardie Beta-Geometric/Beta-Bernoulli CLV Model
# R. Wagner, 2021

# DERT: Discounted Expected Residual Transactions
# DERT = present value of a user's expected future transaction stream, calc'd for each x/t_x/n combo
# discounts the NUMBER of transactions, not the monetary value.
# from FH: "can be rescaled by user’s value multiplier to yield estimate of expected RESIDUAL lifetime value."

compute_dert <- function(mle_results, 
                         discount_rate, 
                         df_xtxn){
  
  x = df_xtxn$x
  tx = df_xtxn$tx
  n = df_xtxn$n

  a <- mle_results$par[1]
  b <- mle_results$par[2]
  g <- mle_results$par[3]
  d <- mle_results$par[4]
  
  beta_ab <- gamma(a)*gamma(b)/gamma(a+b)
  beta_gd <- gamma(g)*gamma(d)/gamma(g+d)
  
  lhs <- compute_lhs(abgd=mle_results$par, df=df_xtxn)
  term_x <- (gamma(a+x+1)*gamma(b+n-x)/gamma(a+b+n+1)/beta_ab) * (gamma(g)*gamma(d+n+1)/gamma(g+d+n+1))/(beta_gd*(1+discount_rate))
  
  # Gaussian Hypergeometric params are a/b/c/z (here, a/b values != alpha/beta values from BB/BG model)
  hypergeo <- f21hyper(a=1, b=(d+max(n)+1), c=(g+d+max(n)+1), z=1/(1+discount_rate)) 
  
  dert <- (term_x * hypergeo/lhs)
  
  return(dert)
  
}

# compute DERT values
df_xtxn_train$dert <- compute_dert(mle_results = mle_results, 
                             discount_rate = 0.1, 
                             df_xtxn = df_xtxn_train) 

# append DERT values back to order matrix
df_order_matrix_train <- merge(df_order_matrix_train,
                               subset(df_xtxn_train, select=c('x_tx_n', 'dert')),
                               by = 'x_tx_n')
