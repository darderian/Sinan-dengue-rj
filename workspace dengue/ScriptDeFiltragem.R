# ==============================================================================
# SCRIPT 2: ETL - FILTRAGEM, LIMPEZA E CONSOLIDAÇÃO (RJ 2015-2025)
# ==============================================================================
library(dplyr)
library(purrr)

message(">>> Iniciando pipeline de ETL para o Estado do Rio de Janeiro...")

# 1. Mapeamento do Data Lake (arquivos brutos padronizados)
arquivos_brutos <- list.files(pattern = "^dengue_nacional_.*\\.rds$", full.names = TRUE)

if (length(arquivos_brutos) == 0) {
  stop("X Erro: Nenhum arquivo dengue_nacional_YYYY.rds encontrado.")
}

# 2. Função Worker de Ingestão e Filtragem (Otimizada para RAM)
extrair_rj <- function(caminho) {
  message(paste("    Processando:", basename(caminho)))
  
  df_bruto <- readRDS(caminho)
  
  # Filtro determinístico utilizando o dicionário já decodificado pela ingestão
  df_rj <- df_bruto %>% 
    filter(SG_UF_NOT == "Rio de Janeiro")
  
  # Destruição do objeto em memória e coleta de lixo
  rm(df_bruto)
  gc()
  
  return(df_rj)
}

# 3. Execução Vetorizada: Extração e Empilhamento (Bind Rows)
message("\n[PROCESSAMENTO] Iniciando extração dos recortes geográficos...")
df_rj_completo <- map_dfr(arquivos_brutos, extrair_rj)

message(paste("\n>>> Consolidação concluída. Total de notificações no RJ:", nrow(df_rj_completo)))

# 4. Expurgo Estrutural de Variáveis Vazias (NAs Absolutos)
message("[PROCESSAMENTO] Analisando densidade de colunas e removendo NAs absolutos...")

# Seleciona apenas colunas onde a condição "todos os valores são NA" for Falsa
df_rj_limpo <- df_rj_completo %>% 
  select(where(~!all(is.na(.))))

colunas_removidas <- ncol(df_rj_completo) - ncol(df_rj_limpo)

message(">>> Resumo Estrutural Pós-Limpeza:")
message(paste("    - Colunas originais:", ncol(df_rj_completo)))
message(paste("    - Colunas inúteis descartadas:", colunas_removidas))
message(paste("    - Colunas finais úteis:", ncol(df_rj_limpo)))

# 5. Exportação do Data Mart Consolidado
arquivo_final <- "SINAN_DENGUE_RJ_2015_2025.rds"
saveRDS(df_rj_limpo, arquivo_final)

message(paste("\n[SUCESSO] Pipeline ETL finalizado. Base analítica salva:", arquivo_final))