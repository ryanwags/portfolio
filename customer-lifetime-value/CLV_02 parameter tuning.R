# Fader/Hardie Beta-Geometric/Beta-Bernoulli CLV Model
# R. Wagner, 2021

# tunes model's alpha/beta/gamma/delta parameters using optim algo based on maximum likelihood

# likelihood wrapper
compute_lhs <- function(abgd, df){
  
  x = df$x    # parse x/tx/n vals
  tx = df$tx 
  n = df$n 
  
  a = abgd[1] # parse param vals
  b = abgd[2]
  g = abgd[3]
  d = abgd[4] 

  beta_ab = gamma(a)*gamma(b)/gamma(a+b) # Beta(alpha, beta)
  beta_gd = gamma(g)*gamma(d)/gamma(g+d) # Beta(gamma, delta)
  
  first_terms <- ((gamma(a+x)*gamma(b+n-x)/gamma(a+b+n)) / beta_ab) * ((gamma(g)*gamma(d+n)/gamma(g+d+n)) / beta_gd)
  
  # successive terms require summation with dynamic upper bound and are therefore calc'd separately for each x/t_x/n combo.
  successive_terms <- NULL # will later sum with vector of first terms.
  
  # upper bound is function of parameters (n, tx) and so will vary for each (x/t_x/n) grouping.
  upper_bounds <- (n-tx-1)
  
  # compute successive terms
  for (k in 1:length(x)){
    # in all cases, bounds of summation are [0, (n-tx-1)], meaning (n-tx-1) must be non-neg.
    if(upper_bounds[k] >= 0){ 
      i <- 0:upper_bounds[k]
      successive_terms[k] <- sum( ((gamma(a+x[k])*gamma(b+tx[k]-x[k]+i)/gamma(a+b+tx[k]+i))/beta_ab) * (gamma(g+1)*gamma(d+tx[k]+i))/gamma(g+d+tx[k]+i+1)/beta_gd )
    }else{  successive_terms[k] <- 0  } # If bound < 0, no successive terms. Just return 0. 
  }
  
  # likelihood for each x/t_x/n combo is pairwise sum of first/successive terms
  lh <- first_terms + successive_terms
  return(lh)
  
}

# negative log-likelihood wrapper: R's optim function minimizes; min of neg func = max of func
# each x/t_x/n combo has its own LH term; all users who share the x/t_x/n combo will therefore have same LH values
# multiply each combo's LH value by the # of users w/ that combo to get combo's overall group LH 
neg_ll <- function(abgd, df){
  count = df$count # count of users w/ each combo
  lh = compute_lhs(abgd, df) # compute LHs
  ll = sum(log(lh) * count) # convert to log-LH to enable summation, then sum
  return(-ll) # return negative log-lh 
}

# parameter tuning via MLE
mle_results <- optim(fn = neg_ll,
                     par = c(1, 1, 1, 1), # initial values for a/b/g/d; should be robust, but can be adjusted for fine-tuning
                     control=list(maxit=1000000),
                     df = df_xtxn_train)

mle_summary <- data.frame(parameter = c("alpha", "beta", "gamma", "delta"),
                          estimate = mle_results$par, # optimal param values
                          se = sqrt(diag(solve(mle_results$hessian)))) # standard errors
