# ==============================================================================
# 1. SETUP DE AMBIENTE E TIME-OUT
# ==============================================================================
# Aumenta o timeout para 1 hora (3600s) para garantir o download de bases > 100MB
options(timeout = 3600)

deps <- c("dplyr", "purrr", "readr", "httr", "stringr", "magrittr", 
          "foreign", "checkmate", "data.table", "lubridate", "curl", 
          "stringi", "tibble")
if (any(!deps %in% installed.packages())) install.packages(setdiff(deps, installed.packages()))

library(read.dbc)
library(dplyr)
library(purrr)

# ==============================================================================
# 2. BOOTSTRAPPING E PATCH ON-THE-FLY DE NAMESPACE (CRÍTICO)
# ==============================================================================
pasta_fonte <- "microdatasus-master"

# 2.0 Autogestão de Dependências (Torna o script reprodutível em qualquer máquina)
if (!dir.exists(pasta_fonte)) {
  message(paste(">>> Diretório", pasta_fonte, "não encontrado. Iniciando clone do repositório..."))
  
  url_repo <- "https://github.com/rfsaldanha/microdatasus/archive/refs/heads/master.zip"
  arquivo_zip <- "microdatasus_temp.zip"
  
  # Download em modo binário ("wb") essencial para integridade de arquivos .zip no Windows
  download.file(url_repo, destfile = arquivo_zip, mode = "wb", quiet = TRUE)
  
  # Extração estrutural
  unzip(arquivo_zip)
  
  # Limpeza do arquivo compactado residual
  unlink(arquivo_zip)
  message(">>> Repositório clonado e descompactado com sucesso.")
}

# 2.1 Carrega os dicionários (paisnet, tabCBO, tabMun) para o Global Environment
arquivos_dados <- list.files(file.path(pasta_fonte, "data"), full.names = TRUE, pattern = "\\.rda$")
invisible(walk(arquivos_dados, load, envir = .GlobalEnv))

# 2.2 Patch Textual: Lê os scripts, remove o namespace explícito e compila
funcoes_r <- list.files(file.path(pasta_fonte, "R"), full.names = TRUE, pattern = "\\.[Rr]$")

for (script in funcoes_r) {
  codigo_bruto <- readLines(script, warn = FALSE)
  codigo_corrigido <- gsub("microdatasus::", "", codigo_bruto)
  eval(parse(text = codigo_corrigido), envir = .GlobalEnv)
}

# 2.3 OTIMIZAÇÃO: Limpeza preventiva de resíduos alocados pelo load()
rm(list = ls(pattern = "_sample", envir = .GlobalEnv), envir = .GlobalEnv)
tabelas_inuteis <- c("equipe", "sigtab", "tabNaturalidade", "tabOcupacao")
rm(list = intersect(ls(envir = .GlobalEnv), tabelas_inuteis), envir = .GlobalEnv)

gc()
message(">>> Patch aplicado. Memória base limpa e otimizada.")

# ==============================================================================
# 3. LOOP DE EXTRAÇÃO: DATA LAKE NACIONAL (SINAN-DENGUE)
# ==============================================================================
# Caso deseje rodar apenas o ano faltante de 2020, altere para: anos_alvo <- 2020
anos_alvo <- 2015:2025

for (ano in anos_alvo) {
  
  # Nomenclatura fixa para a base nacional (Raw Data)
  arquivo_final <- paste0("dengue_nacional_", ano, ".rds")
  
  if (file.exists(arquivo_final)) {
    message(paste(">>> Checkpoint:", arquivo_final, "já existe. Pulando..."))
    next
  }
  
  message(paste("\n[INÍCIO] Requisitando Brasil Integral - Ano:", ano))
  
  tryCatch({
    
    # Execução da requisição FTP 
    res <- fetch_datasus(
      year_start = ano, 
      year_end = ano, 
      information = "SINAN-DENGUE",
      timeout = 3600
    )
    
    if (!is.null(res) && nrow(res) > 0) {
      message(paste("    ->", nrow(res), "registros nacionais obtidos. Processando dicionários..."))
      
      # Processamento integral (tradução de códigos geográficos e clínicos)
      df_proc <- process_sinan_dengue(res, municipality_data = TRUE)
      
      # Persistência em disco
      saveRDS(df_proc, arquivo_final)
      message(paste("    [SUCESSO] Salvo:", arquivo_final))
      
    } else {
      message(paste("    ! Nenhum dado localizado no FTP para o ano", ano))
    }
    
  }, error = function(e) {
    message(paste("    X Erro fatal no ano", ano, ":", e$message))
  }, finally = {
    # OTIMIZAÇÃO: Destruição obrigatória dos objetos massivos de dados brutos
    if (exists("res", envir = .GlobalEnv)) rm(res, envir = .GlobalEnv)
    if (exists("df_proc", envir = .GlobalEnv)) rm(df_proc, envir = .GlobalEnv)
    
    # Aciona o Garbage Collector para desfragmentação de RAM
    gc()
  })
}