# Fader/Hardie Beta-Geometric/Beta-Bernoulli CLV Model
# R. Wagner, 2021

# fetch data; create order matrix for training

# establish connection
redshift_creds <- Sys.getenv('redshift_creds')
conn <- dbConnect(drv = dbDriver("PostgreSQL"),
                  host = gsub('.*=', '',   strsplit(redshift_creds, '&')[[1]][1]), 
                  port = gsub('.*=', '',   strsplit(redshift_creds, '&')[[1]][2]), 
                  dbname = gsub('.*=', '',   strsplit(redshift_creds, '&')[[1]][3]), 
                  user = gsub('.*=', '',   strsplit(redshift_creds, '&')[[1]][4]),
                  password = gsub('.*=', '',   strsplit(redshift_creds, '&')[[1]][5]) )

# fetch purchase data
df <- dbGetQuery(conn = conn, 
                 statement = "select 
                                order_id,
                                user_id, 
                                ordered_at_pt,
                                amount 
                              from core.orders
                              where status = 'success'"
                 )

# store order month
df$order_month = floor_date(as_date(df$ordered_at_pt), 'month')

# collapse multiple orders from same user in single month
df <- df %>%
  group_by(user_id, order_month) %>%
  summarize(amount = sum(amount))

# assign cohort as month of first order
cohort_assignments <- df %>%
  group_by(user_id) %>%
  summarize(cohort_month = min(order_month))

# merge cohort assignments to order records
df <- merge(df, cohort_assignments[,c('user_id', 'cohort_month')], by="user_id") 

# order months
months <- unique(df$order_month) # unique list of months in data
months <- months[order(months)] # put in temporal order

# pivot: rows = distinct users, cols = distinct months, cells = total amount ordered that month
df_order_matrix <- subset(cohort_assignments, select='user_id') # init as user ID scaffold

for(t in 1:length(months)){
  df_orders_i <- subset(df, order_month == months[t], select=c("user_id", "amount"))
  names(df_orders_i)[2] <- paste0("t", months[t]) # appending 't' to colname; can't start w/ number
  df_order_matrix <- merge(df_order_matrix, df_orders_i, by="user_id", all.x=TRUE)
}; rm(t, df_orders_i)

# append cohorts to order matrix
df_order_matrix <- merge(df_order_matrix, cohort_assignments[,c('user_id', 'cohort_month')], by='user_id')

# x/t_x/n values for model training
df_order_matrix$x = apply(df_order_matrix[,paste0('t', months)], 1, function(x) sum(!is.na(x))-1)
df_order_matrix$tx = apply(df_order_matrix[,paste0('t', months)], 1, function(x) max(which(!is.na(x))) - min(which(!is.na(x))) )
df_order_matrix$n = apply(df_order_matrix[,paste0('t', months)], 1, function(x) length(months) - min(which(!is.na(x))))
df_order_matrix$x_tx_n <- with(df_order_matrix, paste(x, tx, n, sep="_")) # concat x_tx_n as key for use in later merge.

# x/t_x/n summary matrix
df_xtxn <- df_order_matrix %>%
  group_by(x, tx, n) %>%
  summarize(count = n()) %>%
  arrange(desc(n), 
          desc(tx), 
          desc(x))
