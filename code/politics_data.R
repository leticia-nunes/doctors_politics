# Get data from politicians and clean them

# Created
#   by: Calebe Cardia Piacentini (contact: calebecp@al.insper.edu.br)
#   at: 06.2024 (mm.yyyy)

cat('\014')
rm(list = ls())

library(tidyverse)
library(basedosdados)

# ---
# Retrieving ----
# ---

# main directory
f_source <- 'C:/Users/caleb/Dropbox/doctors_politics/doctors_politics_source/'
f_shared <- 'C:/Users/caleb/Dropbox/doctors_politics/doctors_politics_shared/'
list.files(f_shared)

# main source: basedosdados
# preparing for basedosdados
set_billing_id("dadosbrasil-365200")
# in a nutshell: (I use SQL sometimes to easily LIMIT data)
# query <- bdplyr("br_tse_eleicoes.candidatos")
# df <- bd_collect(query)

## 1. Candidates ---- 
query_candidates <- "
SELECT cpf, nome, nome_urna, numero_partido, sigla_partido, sigla_uf
FROM `basedosdados.br_tse_eleicoes.candidatos`
WHERE ano=2022"
candidates <- read_sql(query_candidates)
write_csv(candidates, paste0(f_shared, 'raw/politics/candidates_2022.csv'))

## 2. Political Affiliation ----
query_affiliation <- "
SELECT nome, sigla_partido, sigla_uf, data_cancelamento
FROM `basedosdados.br_tse_filiacao_partidaria.microdados`"
affiliation <- read_sql(query_affiliation)
write_csv(affiliation, paste0(f_shared, 'raw/politics/affiliation.csv'))

# we can get CPF data if they appear at the candidates database
# the others we can try to match ny name using RAIS

## 3. Donation Records ----
"
This part of the data is a bit confusing. We have two kinds of data:
  a. at the candidate level
  b. at the party level
I'm focusing on candidates, (a).

(a) There, for a given year, you can find up until 5 different kind of databases.
  I. the main one seems to be <prestacao_de_contas_eleitorais_candidato> in which you can find candidate's
    i. revenue;
    ii. revenues from original donator; (not sure if there are differences to the other one)
    iii. contracted expenditures;
    iv. realized expenditures.
    => for this one I'll start looking at revenues
  II. candidate's bank statement;
  III. cnpj campaign: not sure of its purpose, but I guess it gives some kind of identification to cnpj's (II has as the main identifier cnpj);
  IV. <prestacao_de_contas_eleitorais_orgaos_partidarios>;
  V. parties's bank statement;'

Data here gets increasingly less detailed as we get back in time. However, we keep having data for both revenues and expenditures.
Therefore, I'll keep with focus on revenues (and possibly comparing with expenditures)
"

### I.i.: Revenues ----

# todo: year to year solution since each year has a different column name

# 2020
y <- 2020
f_candidates_revenues <- paste0(f_source, 'raw/donation/donation_revenues_', y,'.csv')
# although we have specifically a "BR" dataset, that seems to be for president only (that is, national positions), we also have data for presidents in the dataset below
candidates_revenues_y <- read_csv2(f_candidates_revenues, n_max = Inf, locale = locale(encoding = 'latin1'))


# 2022

# general comments
# cpf = '-1' is probably self donations 
# (we also have ='.' and some numbers instead of names, but the firs is only for a single candidate -- ANDERSON BRAGA DORNELES --, while the second still leaves information for cpf)

y <- 2022
f_candidates_revenues <- paste0(f_source, 'raw/donation/donation_revenues_', y,'.csv')
# although we have specifically a "BR" dataset, that seems to be for president only (that is, national positions), we also have data for presidents in the dataset below
candidates_revenues_y <- read_csv2(f_candidates_revenues, n_max = Inf, locale = locale(encoding = 'latin1'))

# restricting to useful information
candidates_revenues_y <- candidates_revenues_y %>% 
  select(cpf_candidato = NR_CPF_CANDIDATO, 
         nome_candidato = NM_CANDIDATO, 
         cpf_doador = NR_CPF_CNPJ_DOADOR, 
         nome_doador = NM_DOADOR, 
         cnae_doador = CD_CNAE_DOADOR, 
         valor_receita = VR_RECEITA) %>% 
  mutate(type_donation = case_when(
    cpf_doador == '-1' ~ 'self',
    cnae_doador == '-1' ~ 'person',
    TRUE ~ 'other'
  ))

# filtering only to donations from persons
df_doadores_y <- candidates_revenues_y %>% 
  filter(type_donation == 'person') %>% 
  group_by(cpf_doador) %>% 
  summarise(doacao = sum(valor_receita))

write_csv(df_doadores_y, paste0(f_shared, 'raw/politics/doadores_', y,'.csv'))

# # loop over years
# # todo: each year has a different collumn name for those information, this solution is still only for 2022
# for (y in seq(2002, 2022, 2)) {
#   print(y)
#   #
#   f_candidates_revenues <- paste0(f_source, 'raw/donation/donation_revenues_', y,'.csv')
#   # although we have specifically a "BR" dataset, that seems to be for president only (that is, national positions), we also have data for presidents in the dataset below
#   candidates_revenues_y <- read_csv2(f_candidates_revenues, n_max = Inf, locale = locale(encoding = 'latin1'))
#   
#   # restricting to useful information
#   df_candidates_revenues_y <- candidates_revenues_y %>% 
#     select(cpf_candidato = NR_CPF_CANDIDATO, 
#            nome_candidato = NM_CANDIDATO, 
#            cpf_doador = NR_CPF_CNPJ_DOADOR, 
#            nome_doador = NM_DOADOR, 
#            cnae_doador = CD_CNAE_DOADOR, 
#            valor_receita = VR_RECEITA) %>% 
#     mutate(type_donation = case_when(
#       cpf_doador == '-1' ~ 'self',
#       cnae_doador == '-1' ~ 'person',
#       TRUE ~ 'other'
#     ))
#   
#   # filtering only to donations from persons
#   df_doadores_y <- df_candidates_revenues_y %>% 
#     filter(type_donation == 'person') %>% 
#     group_by(cpf_doador) %>% 
#     summarise(doacao = sum(valor_receita))
#   
#   # general comments
#   # cpf = '-1' is probably self donations 
#   # (we also have ='.' and some numbers instead of names, but the firs is only for a single candidate -- ANDERSON BRAGA DORNELES --, while the second still leaves information for cpf)
#   
#   write_csv(df_doadores_y, paste0(f_shared, 'raw/politics/doadores_', y,'.csv'))
# }

# # II: not sure if this is really useful in this step
# f_bank_statement <- paste0(f, 'data/raw/donation/prestacao_contas_eleitorais/extrato_bancario_candidato_2022/extrato_bancario_candidato_2022.csv')
# bank_statement <- read_csv2(f_bank_statement, n_max = 100, locale = locale(encoding = 'latin1'))


# ---
# Transforming ----
# ---

## 1. Candidates ---- 

## 2. Political Affiliation ----
df_affiliation <- affiliation %>% 
  mutate(active_2022 = (year(data_cancelamento) < 2022))

## 3. Donation Records ----
### I.i.: candidates revenues  ----

# still using data only for the last year (2022) for many analysis here

#### are there cpfs with more than one name?

# 3k of 481k cpfs (~0.6%)  
many_cpfs <- df_candidates_revenues_y %>% 
  filter(type_donation == 'person') %>%
  select(cpf_doador, nome_doador) %>% 
  distinct() %>% 
  group_by(cpf_doador) %>% summarise(n_nomes = n()) %>% 
  filter(n_nomes > 1)

# example: does not seem to be problematic
print(filter(df_doadores_y, cpf == many_cpfs$cpf_doador[1]))

#### who was the biggest donor (2022)?
# obs.: names match with news on biggest donor
# ordering
df_doadores_y %>% arrange(desc(doacao)) %>% print()
df_doadores_y %>% arrange(doacao) %>% print()

# average ~ of 2k per person (or ~ 4k if only >1 donations considered)
mean(df_doadores_y$doacao)
mean(df_doadores_y[df_doadores_y$doacao>1,]$doacao)

# distribution graph
# TODO: before running this adjust the cleaning process above
# does this mean that we should rule out those with <=1?
for (y in seq(2022, 2022, 2)) {
  print(y)
  f_doadores <- paste0(f_shared, 'raw/politics/doadores_', y, '.csv')
  df_doadores <- read_csv(f_doadores)
  #
  dist_doadores <- ggplot(df_doadores, aes(log(doacao))) +
    geom_density(fill='green', alpha=0.5) + 
    geom_vline(xintercept = log(mean(df_doadores$doacao)), 
               linetype='dashed') +
    geom_vline(xintercept = log(mean(df_doadores[df_doadores$doacao>1,]$doacao)), 
               linetype='dashed', color='blue') +  # considering only donations >1
    theme_minimal() +
    labs(x = 'donations revenue (ln)')
  
  # dist_doadores
  ggsave(paste0(f_shared, 'docs/images/politics/dist_donations_revenue_', y, '.png'), bg='white')
}


#### comparing types of donation sizes

'self-donations represent a minor share of total donations, 
while that from other people represent about 15% (2022) 
in a total of ~1tri reais for 481k donators (~ average of 2k per person -- consistent w/ above)'

# y <- 2022
# f_candidates_revenues <- paste0(f_source, 'raw/donation/donation_revenues_', y,'.csv')
# # although we have specifically a "BR" dataset, that seems to be for president only (that is, national positions), we also have data for presidents in the dataset below
# candidates_revenues <- read_csv2(f_candidates_revenues, n_max = Inf, locale = locale(encoding = 'latin1'))

# restricting to useful information
df_candidates_revenues <- candidates_revenues_y %>% 
  select(cpf_candidato = NR_CPF_CANDIDATO, 
         nome_candidato = NM_CANDIDATO, 
         cpf_doador = NR_CPF_CNPJ_DOADOR, 
         nome_doador = NM_DOADOR, 
         cnae_doador = CD_CNAE_DOADOR, 
         valor_receita = VR_RECEITA) %>% 
  mutate(type_donation = case_when(
    cpf_doador == '-1' ~ 'self',
    cnae_doador == '-1' ~ 'person',
    TRUE ~ 'other'
  ))

type_donation <- df_candidates_revenues %>%
  group_by(type_donation, cpf_doador) %>% 
  summarise(doacao = sum(valor_receita)) %>% 
  group_by(type_donation) %>% 
  summarise(doacao = sum(doacao), n_doadores = n()) %>% 
  mutate(doacao_prop = doacao/sum(doacao))
  
print(type_donation)

# ---
# Crossing with health data ----
# ---





