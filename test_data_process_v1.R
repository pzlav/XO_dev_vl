# Proceeding data Google sheet API 

#setting variables
setwd('C:\\Users\\pzla\\Desktop\\XO Developer')
library(googlesheets4)
library(dplyr)

#reading data
trans <- read_sheet("https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ",sheet="transactions")
print(format(object.size(trans), units = "MiB"))
clients <- read_sheet("https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ",sheet="clients")
print(format(object.size(clients), units = "MiB"))
managers <- read_sheet("https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ",sheet="managers")
print(format(object.size(managers), units = "MiB"))
leads <- read_sheet("https://docs.google.com/spreadsheets/d/1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ",sheet="leads")
print(format(object.size(leads), units = "MiB"))

#small profiling
names(managers)
names(leads)                      
    
#cleaning d_utm_source
#TODO need correct metadata
leads2 <- leads  %>%  
            mutate(d_utm_source = replace(d_utm_source, d_utm_source=="vk","vkontakte"))  %>%  
            mutate(d_utm_source = replace(d_utm_source, d_utm_source=="ig","instagram"))  %>% 
            mutate(d_utm_source = replace(d_utm_source, d_utm_source=="insta","instagram"))  %>%
            mutate(d_utm_source = ifelse(grepl("ycard",d_utm_source), "ycard", d_utm_source))

#joins df for queries
leads2$created_at <- as.POSIXct(leads2$created_at, format="%Y-%m-%d %H:%M:%S")
leads_w_man <- leads2  %>% left_join(managers, by=c("l_manager_id"="manager_id"))
leads_w_man_w_tr <- leads_w_man  %>% left_join(trans, by=c("l_client_id"="l_client_id"), suffix = c("", ".trans"))
leads_w_man_w_tr$created_at.trans <- as.POSIXct(leads_w_man_w_tr$created_at.trans, format="%Y-%m-%d %H:%M:%S")

#setting base table
result <-  leads_w_man %>% 
              select(d_utm_source, d_club, d_manager) %>%
              distinct() %>%
              arrange(d_utm_source,d_club)

#task1
t1 <- leads_w_man %>%
            group_by(d_utm_source, d_club, d_manager) %>%
            summarise(task_1 = n()) %>%
            select(task_1,d_utm_source, d_club, d_manager)
result <- result %>% left_join(t1)

#task2
t1 <- leads_w_man %>%
  group_by(d_utm_source, d_club, d_manager) %>%
  filter(grepl("00000000",l_client_id)) %>%
  summarise(task_2 = n()) %>%
  select(task_2, d_utm_source, d_club, d_manager)
result <- result %>% left_join(t1)

#task3
t1 <- leads_w_man_w_tr %>%
  group_by(l_client_id) %>%
  arrange(l_client_id,created_at, .by_group = TRUE) %>%
  filter(row_number()==1) %>%
  filter(created_at.trans>created_at) %>%
  group_by(d_utm_source, d_club, d_manager) %>%
  summarise(task_3 = n()) %>%
  select(task_3, d_utm_source, d_club, d_manager)
result <- result %>% left_join(t1)

#task4
t1 <- leads_w_man_w_tr %>%
  filter(between(difftime(created_at.trans,created_at, units="days"), 0, 7) ) %>%
  group_by(d_utm_source, d_club, d_manager) %>%
  summarise(task_4 = n()) %>%
  select(task_4, d_utm_source, d_club, d_manager)
result <- result %>% left_join(t1)

#task5
t1 <- leads_w_man_w_tr %>%
  filter(between(difftime(created_at.trans,created_at, units="days"), 0, 7) ) %>%
  group_by(l_client_id) %>%
  arrange(l_client_id,created_at.trans, .by_group = TRUE) %>%
  filter(row_number()==1) %>%
  group_by(d_utm_source, d_club, d_manager) %>%
  summarise(task_5 = n()) %>%
  select(task_5, d_utm_source, d_club, d_manager)
result <- result %>% left_join(t1)

#task6
t1 <- leads_w_man_w_tr %>%
  filter(between(difftime(created_at.trans,created_at, units="days"), 0, 7) ) %>%
  group_by(l_client_id) %>%
  arrange(l_client_id,created_at.trans, .by_group = TRUE) %>%
  filter(row_number()==1) %>%
  group_by(d_utm_source, d_club, d_manager) %>%
  summarise(task_6 = sum(m_real_amount)) %>%
  select(task_6, d_utm_source, d_club, d_manager)
result <- result %>% left_join(t1)


#writing results
ss <- gs4_create("XO_developer_test", sheets = c("result","desc"))
result %>% sheet_write(ss, sheet = "result")


