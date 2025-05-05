package main

import (
	"archive/zip"
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// Estruturas e Constantes
const (
	maxWorkers = 5
	maxRetries = 3
	retryDelay = 2 * time.Second
)

type DownloadJob struct {
	URL  string
	Path string
}

type progressReader struct {
	Reader    io.Reader
	bar       *progressbar.ProgressBar
	totalSize int64
	current   int64
}

// Implementação de io.Reader para progressReader
func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.Reader.Read(p)
	pr.current += int64(n)
	if pr.totalSize > 0 {
		_ = pr.bar.Set64(pr.current)
	}
	return n, err
}

// setupDB configura a conexão com o banco de dados PostgreSQL
func setupDB() (*sql.DB, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar .env: %v", err)
	}

	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbname := os.Getenv("DB_NAME")

	log.Printf("DB_HOST: %s", host)
	log.Printf("DB_PORT: %s", port)
	log.Printf("DB_USER: %s", user)
	log.Printf("DB_PASSWORD: %s", password)
	log.Printf("DB_NAME: %s", dbname)

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	log.Printf("String de conexão: %s", connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("erro ao conectar ao banco de dados: %v", err)
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("erro ao testar conexão com o banco de dados: %v", err)
	}

	log.Printf("Conexão com o banco de dados estabelecida em %s:%s", host, port)
	return db, nil
}

// createTables cria as tabelas necessárias no banco de dados
func createTables(db *sql.DB, logger *log.Logger) {
	tables := []struct {
		name   string
		schema string
	}{
		{name: "cnae", schema: `
            CREATE TABLE IF NOT EXISTS cnae (
                codigo VARCHAR(7) PRIMARY KEY,
                descricao VARCHAR(200)
            );`},
		{name: "empresas", schema: `
            CREATE TABLE IF NOT EXISTS empresas (
                cnpj_basico VARCHAR(8) PRIMARY KEY,
                razao_social VARCHAR(200),
                natureza_juridica VARCHAR(4),
                qualificacao_responsavel VARCHAR(2),
                capital_social DECIMAL(18,2),
                porte_empresa VARCHAR(2),
                ente_federativo_responsavel VARCHAR(50)
            );`},
		{name: "estabelecimento", schema: `
            CREATE TABLE IF NOT EXISTS estabelecimento (
                cnpj_basico VARCHAR(8),
                cnpj_ordem VARCHAR(4),
                cnpj_dv VARCHAR(2),
                matriz_filial VARCHAR(1),
                nome_fantasia VARCHAR(200),
                situacao_cadastral VARCHAR(2),
                data_situacao_cadastral DATE,
                motivo_situacao_cadastral VARCHAR(2),
                nome_cidade_exterior VARCHAR(200),
                pais VARCHAR(3),
                data_inicio_atividades DATE,
                cnae_fiscal VARCHAR(7),
                cnae_fiscal_secundaria TEXT,
                tipo_logradouro VARCHAR(20),
                logradouro VARCHAR(200),
                numero VARCHAR(10),
                complemento VARCHAR(200),
                bairro VARCHAR(200),
                cep VARCHAR(8),
                uf VARCHAR(2),
                municipio VARCHAR(4),
                ddd1 VARCHAR(4),
                telefone1 VARCHAR(8),
                ddd2 VARCHAR(4),
                telefone2 VARCHAR(8),
                ddd_fax VARCHAR(4),
                fax VARCHAR(8),
                correio_eletronico VARCHAR(200),
                situacao_especial VARCHAR(200),
                data_situacao_especial DATE,
                PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
            );`},
		{name: "motivo", schema: `
            CREATE TABLE IF NOT EXISTS motivo (
                descricao VARCHAR(200) PRIMARY KEY
            );`},
		{name: "municipio", schema: `
            CREATE TABLE IF NOT EXISTS municipio (
                descricao VARCHAR(200) PRIMARY KEY
            );`},
		{name: "natureza_juridica", schema: `
            CREATE TABLE IF NOT EXISTS natureza_juridica (
                descricao VARCHAR(200) PRIMARY KEY
            );`},
		{name: "pais", schema: `
            CREATE TABLE IF NOT EXISTS pais (
                descricao VARCHAR(200) PRIMARY KEY
            );`},
		{name: "qualificacao_socio", schema: `
            CREATE TABLE IF NOT EXISTS qualificacao_socio (
                descricao VARCHAR(200) PRIMARY KEY
            );`},
		{name: "simples", schema: `
            CREATE TABLE IF NOT EXISTS simples (
                cnpj_basico VARCHAR(8) PRIMARY KEY,
                opcao_simples VARCHAR(1),
                data_opcao_simples DATE,
                data_exclusao_simples DATE,
                opcao_mei VARCHAR(1),
                data_opcao_mei DATE,
                data_exclusao_mei DATE
            );`},
		{name: "socios", schema: `
            CREATE TABLE IF NOT EXISTS socios (
                cnpj VARCHAR(14),
                cnpj_basico VARCHAR(8),
                identificador_de_socio VARCHAR(1),
                nome_socio VARCHAR(200),
                cnpj_cpf_socio VARCHAR(14),
                qualificacao_socio VARCHAR(2),
                data_entrada_sociedade DATE,
                pais VARCHAR(3),
                representante_legal VARCHAR(11),
                nome_representante VARCHAR(200),
                qualificacao_representante_legal VARCHAR(2),
                faixa_etaria VARCHAR(1),
                PRIMARY KEY (cnpj, cnpj_basico, identificador_de_socio, cnpj_cpf_socio)
            );`},
		{name: "progress", schema: `
            CREATE TABLE IF NOT EXISTS progress (
                filename VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50),
                downloaded_at TIMESTAMP
            );`},
	}

	logger.Printf("Iniciando criação das tabelas no banco de dados")
	for _, table := range tables {
		logger.Printf("Criando tabela %s", table.name)
		_, err := db.Exec(table.schema)
		if err != nil {
			logger.Fatalf("Erro ao criar tabela %s: %v", table.name, err)
		}
	}
	logger.Printf("Tabelas criadas ou já existentes")
}

// fetchDownloadLinks busca os links de download da Receita Federal
func fetchDownloadLinks(logger *log.Logger) ([]string, error) {
	url := "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-04/"
	var links []string

	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Printf("Tentativa %d/%d para acessar %s", attempt, maxRetries, url)
		resp, err := http.Get(url)
		if err != nil {
			logger.Printf("Erro ao acessar %s (tentativa %d/%d): %v", url, attempt, maxRetries, err)
			if attempt == maxRetries {
				return nil, fmt.Errorf("falha ao acessar %s após %d tentativas: %v", url, maxRetries, err)
			}
			time.Sleep(retryDelay)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			logger.Printf("Status inválido para %s (tentativa %d/%d): %d", url, attempt, maxRetries, resp.StatusCode)
			if attempt == maxRetries {
				return nil, fmt.Errorf("falha ao acessar %s após %d tentativas: status %d", url, maxRetries, resp.StatusCode)
			}
			time.Sleep(retryDelay)
			continue
		}

		logger.Printf("Conexão bem-sucedida na tentativa %d", attempt)
		doc, err := goquery.NewDocumentFromReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("erro ao parsear HTML: %v", err)
		}

		doc.Find("a").Each(func(i int, s *goquery.Selection) {
			href, exists := s.Attr("href")
			if exists && strings.HasSuffix(href, ".zip") {
				fullURL := url + href
				links = append(links, fullURL)
			}
		})
		break
	}

	if len(links) == 0 {
		return nil, fmt.Errorf("nenhum arquivo ZIP encontrado em %s", url)
	}

	logger.Printf("Encontrados %d arquivos ZIP para download", len(links))
	return links, nil
}

// downloadFile realiza o download de um arquivo ZIP
func downloadFile(job DownloadJob, logger *log.Logger, wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()

	start := time.Now()
	filename := filepath.Base(job.URL)

	var status string
	err := db.QueryRow("SELECT status FROM progress WHERE filename = $1", filename).Scan(&status)
	if err == nil && status == "downloaded" {
		logger.Printf("Arquivo %s já foi baixado anteriormente. Pulando download.", filename)
		return
	}
	if err != nil && err != sql.ErrNoRows {
		logger.Printf("Erro ao verificar progresso do arquivo %s: %v", filename, err)
		return
	}

	logger.Printf("Iniciando download de %s", filename)

	if _, err := os.Stat(job.Path); err == nil {
		logger.Printf("Arquivo %s já existe. Removendo para forçar o download.", filename)
		if err := os.Remove(job.Path); err != nil {
			logger.Printf("Erro ao remover arquivo existente %s: %v", job.Path, err)
			return
		}
	}

	client := &http.Client{
		Timeout: 600 * time.Second,
	}
	var resp *http.Response
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("GET", job.URL, nil)
		if err != nil {
			logger.Printf("Erro ao criar requisição para %s (tentativa %d/%d): %v", job.URL, attempt, maxRetries, err)
			if attempt == maxRetries {
				logger.Printf("Falha ao criar requisição para %s após %d tentativas", job.URL, maxRetries)
				return
			}
			time.Sleep(retryDelay)
			continue
		}

		resp, err = client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			logger.Printf("Conexão bem-sucedida para %s na tentativa %d", job.URL, attempt)
			break
		}
		if err != nil {
			logger.Printf("Erro ao baixar %s (tentativa %d/%d): %v", job.URL, attempt, maxRetries, err)
		} else {
			logger.Printf("Status inválido para %s (tentativa %d/%d): %d", job.URL, attempt, maxRetries, resp.StatusCode)
			resp.Body.Close()
		}
		if attempt == maxRetries {
			logger.Printf("Falha ao baixar %s após %d tentativas", job.URL, maxRetries)
			return
		}
		logger.Printf("Aguardando %v antes da próxima tentativa...", retryDelay)
		time.Sleep(retryDelay)
	}

	defer resp.Body.Close()

	out, err := os.Create(job.Path)
	if err != nil {
		logger.Printf("Erro ao criar arquivo %s: %v", job.Path, err)
		return
	}
	defer out.Close()

	size := resp.ContentLength
	if size < 0 {
		logger.Printf("Tamanho do arquivo %s desconhecido, prosseguindo sem barra de progresso precisa", filename)
		size = 0
	} else {
		logger.Printf("Tamanho do arquivo %s: %d bytes", filename, size)
	}

	bar := progressbar.DefaultBytes(size, fmt.Sprintf("%s: ", filename))
	reader := &progressReader{
		Reader:    io.TeeReader(resp.Body, bar),
		bar:       bar,
		totalSize: size,
		current:   0,
	}

	_, err = io.Copy(out, reader)
	if err != nil {
		logger.Printf("Erro ao copiar dados para %s: %v", job.Path, err)
		return
	}

	if isZIPValid(job.Path, logger) {
		_, err = db.Exec("INSERT INTO progress (filename, status, downloaded_at) VALUES ($1, $2, $3) ON CONFLICT (filename) DO UPDATE SET status = $2, downloaded_at = $3",
			filename, "downloaded", time.Now())
		if err != nil {
			logger.Printf("Erro ao registrar progresso do download: %v", err)
			return
		}
		logger.Printf("Download de %s concluído em %v", filename, time.Since(start))
	} else {
		logger.Printf("Arquivo %s está corrompido e será excluído", filename)
		os.Remove(job.Path)
	}
}

// isZIPValid verifica se o arquivo ZIP é válido
func isZIPValid(path string, logger *log.Logger) bool {
	file, err := os.Open(path)
	if err != nil {
		logger.Printf("Erro ao abrir arquivo %s para validação: %v", path, err)
		return false
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		logger.Printf("Erro ao obter estatísticas do arquivo %s: %v", path, err)
		return false
	}

	zipReader, err := zip.NewReader(file, stat.Size())
	if err != nil {
		logger.Printf("Arquivo %s não é um ZIP válido: %v", path, err)
		return false
	}

	if len(zipReader.File) == 0 {
		logger.Printf("Arquivo %s está vazio", path)
		return false
	}

	return true
}

// extractZIP extrai os arquivos ZIP para o diretório especificado e renomeia os arquivos
func extractZIP(zipPath, extractPath string, logger *log.Logger) error {
	logger.Printf("Extraindo %s", zipPath)

	file, err := os.Open(zipPath)
	if err != nil {
		logger.Printf("Erro ao abrir arquivo ZIP %s: %v", zipPath, err)
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		logger.Printf("Erro ao obter estatísticas de %s: %v", zipPath, err)
		return err
	}

	zipReader, err := zip.NewReader(file, stat.Size())
	if err != nil {
		logger.Printf("Erro ao ler arquivo ZIP %s: %v", zipPath, err)
		return err
	}

	var extractedFiles int
	for _, f := range zipReader.File {
		extractedFiles++
		rc, err := f.Open()
		if err != nil {
			logger.Printf("Erro ao abrir arquivo interno %s em %s: %v", f.Name, zipPath, err)
			continue
		}
		defer rc.Close()

		tempName := strings.ToLower(f.Name)
		parts := strings.Split(tempName, ".")
		if len(parts) < 4 {
			logger.Printf("Nome de arquivo %s não está no formato esperado, pulando renomeamento", f.Name)
			continue
		}

		lastPart := parts[len(parts)-1]
		fileType := ""
		switch {
		case strings.HasPrefix(lastPart, "emprescsv"):
			fileType = "empresas"
		case strings.HasPrefix(lastPart, "estabele"):
			fileType = "estabelecimento"
		case strings.HasPrefix(lastPart, "sociocsv"):
			fileType = "socios"
		case strings.HasPrefix(lastPart, "simples"):
			fileType = "simples"
		case strings.HasPrefix(lastPart, "natjucsv"):
			fileType = "natureza_juridica"
		case strings.HasPrefix(lastPart, "paiscsv"):
			fileType = "pais"
		case strings.HasPrefix(lastPart, "municcsv"):
			fileType = "municipio"
		case strings.HasPrefix(lastPart, "moticsv"):
			fileType = "motivo"
		case strings.HasPrefix(lastPart, "qualscsv"):
			fileType = "qualificacao_socio"
		case strings.HasPrefix(lastPart, "cnaecsv"):
			fileType = "cnae"
		default:
			logger.Printf("Tipo de arquivo %s não reconhecido, pulando renomeamento", lastPart)
			continue
		}

		prefix := strings.Join(parts[:len(parts)-1], "")
		newName := prefix + fileType + ".csv"

		outPath := filepath.Join(extractPath, newName)
		if f.FileInfo().IsDir() {
			os.MkdirAll(outPath, os.ModePerm)
			continue
		}

		os.MkdirAll(filepath.Dir(outPath), os.ModePerm)
		outFile, err := os.Create(outPath)
		if err != nil {
			logger.Printf("Erro ao criar arquivo %s: %v", outPath, err)
			continue
		}
		defer outFile.Close()

		latin1Reader := transform.NewReader(rc, charmap.ISO8859_1.NewDecoder())
		scanner := bufio.NewScanner(latin1Reader)
		writer := csv.NewWriter(outFile)
		defer writer.Flush()

		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Split(line, ";")
			if len(fields) < 2 && (fileType == "cnae" || fileType == "motivo" || fileType == "municipio" || fileType == "natureza_juridica" || fileType == "pais" || fileType == "qualificacao_socio") {
				logger.Printf("Linha malformada em %s, pulando: %v", f.Name, fields)
				continue
			}
			for i, field := range fields {
				field = strings.Trim(field, `"`)
				field = strings.ReplaceAll(field, `""`, `"`)
				if strings.Contains(field, `"`) || strings.Contains(field, ";") {
					field = `"` + strings.ReplaceAll(field, `"`, `""`) + `"`
				}
				fields[i] = field
			}
			if err := writer.Write(fields); err != nil {
				logger.Printf("Erro ao escrever linha em %s: %v", outPath, err)
				continue
			}
		}

		if err := scanner.Err(); err != nil {
			logger.Printf("Erro ao ler arquivo interno %s: %v", f.Name, err)
			continue
		}

		logger.Printf("Arquivo %s extraído e renomeado para %s", f.Name, newName)
	}

	logger.Printf("Extração concluída para %s. Extraídos %d arquivos.", zipPath, extractedFiles)
	return nil
}

// importDataToDB importa os dados do CSV para o banco de dados
func importDataToDB(csvPath string, db *sql.DB, logger *log.Logger) error {
	filename := strings.TrimSuffix(filepath.Base(csvPath), ".csv")
	tableName := ""

	var fileType string
	if strings.Contains(filename, "empresas") {
		fileType = "Empresas"
		tableName = "empresas"
	} else if strings.Contains(filename, "estabelecimento") {
		fileType = "Estabelecimento"
		tableName = "estabelecimento"
	} else if strings.Contains(filename, "socios") {
		fileType = "Socios"
		tableName = "socios"
	} else if strings.Contains(filename, "simples") {
		fileType = "Simples"
		tableName = "simples"
	} else if strings.Contains(filename, "natureza_juridica") {
		fileType = "Natureza"
		tableName = "natureza_juridica"
	} else if strings.Contains(filename, "pais") {
		fileType = "Paises"
		tableName = "pais"
	} else if strings.Contains(filename, "municipio") {
		fileType = "Municipio"
		tableName = "municipio"
	} else if strings.Contains(filename, "motivo") {
		fileType = "Motivo"
		tableName = "motivo"
	} else if strings.Contains(filename, "qualificacao_socio") {
		fileType = "Qualificacao"
		tableName = "qualificacao_socio"
	} else if strings.Contains(filename, "cnae") {
		fileType = "Cnae"
		tableName = "cnae"
	} else {
		return fmt.Errorf("tabela para arquivo %s não suportada para importação", filename)
	}

	logger.Printf("Importando %s para a tabela %s", csvPath, tableName)

	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("erro ao abrir arquivo CSV %s: %v", csvPath, err)
	}
	defer file.Close()

	latin1Reader := transform.NewReader(file, charmap.ISO8859_1.NewDecoder())
	reader := csv.NewReader(latin1Reader)
	reader.Comma = ';'
	reader.FieldsPerRecord = -1 // Permitir número variável de colunas

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("erro ao iniciar transação: %v", err)
	}

	var stmt *sql.Stmt
	switch fileType {
	case "Empresas":
		stmt, err = tx.Prepare(`
            INSERT INTO empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo_responsavel)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (cnpj_basico) DO NOTHING`)
	case "Estabelecimento":
		stmt, err = tx.Prepare(`
            INSERT INTO estabelecimento (cnpj_basico, cnpj_ordem, cnpj_dv, matriz_filial, nome_fantasia, situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividades, cnae_fiscal, cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio, ddd1, telefone1, ddd2, telefone2, ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)
            ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO NOTHING`)
	case "Socios":
		stmt, err = tx.Prepare(`
            INSERT INTO socios (cnpj, cnpj_basico, identificador_de_socio, nome_socio, cnpj_cpf_socio, qualificacao_socio, data_entrada_sociedade, pais, representante_legal, nome_representante, qualificacao_representante_legal, faixa_etaria)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (cnpj, cnpj_basico, identificador_de_socio, cnpj_cpf_socio) DO NOTHING`)
	case "Simples":
		stmt, err = tx.Prepare(`
            INSERT INTO simples (cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples, opcao_mei, data_opcao_mei, data_exclusao_mei)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (cnpj_basico) DO NOTHING`)
	case "Natureza":
		stmt, err = tx.Prepare(`
            INSERT INTO natureza_juridica (descricao)
            VALUES ($1)
            ON CONFLICT (descricao) DO NOTHING`)
	case "Paises":
		stmt, err = tx.Prepare(`
            INSERT INTO pais (descricao)
            VALUES ($1)
            ON CONFLICT (descricao) DO NOTHING`)
	case "Municipio":
		stmt, err = tx.Prepare(`
            INSERT INTO municipio (descricao)
            VALUES ($1)
            ON CONFLICT (descricao) DO NOTHING`)
	case "Motivo":
		stmt, err = tx.Prepare(`
            INSERT INTO motivo (descricao)
            VALUES ($1)
            ON CONFLICT (descricao) DO NOTHING`)
	case "Qualificacao":
		stmt, err = tx.Prepare(`
            INSERT INTO qualificacao_socio (descricao)
            VALUES ($1)
            ON CONFLICT (descricao) DO NOTHING`)
	case "Cnae":
		stmt, err = tx.Prepare(`
            INSERT INTO cnae (codigo, descricao)
            VALUES ($1, $2)
            ON CONFLICT (codigo) DO NOTHING`)
	}

	if err != nil {
		tx.Rollback()
		return fmt.Errorf("erro ao preparar statement para %s: %v", tableName, err)
	}
	defer stmt.Close()

	recordNum := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Printf("Erro ao ler linha %d de %s: %v", recordNum+1, csvPath, err)
			continue
		}
		recordNum++

		// Pular linhas malformadas
		if fileType == "Cnae" || fileType == "Motivo" || fileType == "Municipio" || fileType == "Natureza" || fileType == "Paises" || fileType == "Qualificacao" {
			if len(record) < 2 {
				logger.Printf("Linha %d de %s tem menos de 2 colunas, pulando: %v", recordNum, csvPath, record)
				continue
			}
		} else if fileType == "Empresas" && len(record) < 7 {
			logger.Printf("Linha %d de %s tem menos de 7 colunas, pulando: %v", recordNum, csvPath, record)
			continue
		} else if fileType == "Estabelecimento" && len(record) < 30 {
			logger.Printf("Linha %d de %s tem menos de 30 colunas, pulando: %v", recordNum, csvPath, record)
			continue
		} else if fileType == "Socios" && len(record) < 11 {
			logger.Printf("Linha %d de %s tem menos de 11 colunas, pulando: %v", recordNum, csvPath, record)
			continue
		} else if fileType == "Simples" && len(record) < 7 {
			logger.Printf("Linha %d de %s tem menos de 7 colunas, pulando: %v", recordNum, csvPath, record)
			continue
		}

		var args []interface{}
		switch fileType {
		case "Empresas":
			capitalSocial, _ := strconv.ParseFloat(record[4], 64)
			args = []interface{}{record[0], record[1], record[2], record[3], capitalSocial, record[5], record[6]}
		case "Estabelecimento":
			dataSituacao := sql.NullTime{}
			if record[6] != "" {
				t, _ := time.Parse("20060102", record[6])
				dataSituacao = sql.NullTime{Time: t, Valid: true}
			}
			dataInicio := sql.NullTime{}
			if record[10] != "" {
				t, _ := time.Parse("20060102", record[10])
				dataInicio = sql.NullTime{Time: t, Valid: true}
			}
			dataSituacaoEspecial := sql.NullTime{}
			if record[29] != "" {
				t, _ := time.Parse("20060102", record[29])
				dataSituacaoEspecial = sql.NullTime{Time: t, Valid: true}
			}
			args = []interface{}{
				record[0], record[1], record[2], record[3], record[4], record[5],
				dataSituacao, record[7], record[8], record[9], dataInicio,
				record[11], record[12], record[13], record[14], record[15],
				record[16], record[17], record[18], record[19], record[20],
				record[21], record[22], record[23], record[24], record[25],
				record[26], record[27], record[28], dataSituacaoEspecial,
			}
		case "Socios":
			dataEntrada := sql.NullTime{}
			if record[5] != "" {
				t, _ := time.Parse("20060102", record[5])
				dataEntrada = sql.NullTime{Time: t, Valid: true}
			}
			cnpj := record[0] + record[1] + record[2]
			args = []interface{}{
				cnpj, record[0], record[1], record[2], record[3], record[4],
				dataEntrada, record[6], record[7], record[8], record[9], record[10],
			}
		case "Simples":
			dataOpcaoSimples := sql.NullTime{}
			if record[2] != "" {
				t, _ := time.Parse("20060102", record[2])
				dataOpcaoSimples = sql.NullTime{Time: t, Valid: true}
			}
			dataExclusaoSimples := sql.NullTime{}
			if record[3] != "" {
				t, _ := time.Parse("20060102", record[3])
				dataExclusaoSimples = sql.NullTime{Time: t, Valid: true}
			}
			dataOpcaoMei := sql.NullTime{}
			if record[5] != "" {
				t, _ := time.Parse("20060102", record[5])
				dataOpcaoMei = sql.NullTime{Time: t, Valid: true}
			}
			dataExclusaoMei := sql.NullTime{}
			if record[6] != "" {
				t, _ := time.Parse("20060102", record[6])
				dataExclusaoMei = sql.NullTime{Time: t, Valid: true}
			}
			args = []interface{}{
				record[0], record[1], dataOpcaoSimples, dataExclusaoSimples,
				record[4], dataOpcaoMei, dataExclusaoMei,
			}
		case "Natureza", "Paises", "Municipio", "Motivo", "Qualificacao":
			args = []interface{}{record[1]} // Apenas a descrição
		case "Cnae":
			args = []interface{}{record[0], record[1]} // Código e descrição
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			logger.Printf("Erro ao inserir linha %d de %s: %v", recordNum, csvPath, err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("erro ao confirmar transação: %v", err)
	}

	logger.Printf("Dados de %s importados com sucesso para a tabela %s", csvPath, tableName)
	return nil
}

// cleanup remove os arquivos temporários
func cleanup(logger *log.Logger) {
	logger.Printf("Iniciando exclusão de arquivos temporários...")

	dir := "dados-receita"
	files, err := os.ReadDir(dir)
	if err != nil {
		logger.Printf("Erro ao ler diretório %s: %v", dir, err)
		return
	}

	for _, file := range files {
		path := filepath.Join(dir, file.Name())
		if err := os.Remove(path); err != nil {
			logger.Printf("Erro ao excluir arquivo %s: %v", path, err)
		} else {
			logger.Printf("Arquivo %s excluído com sucesso", path)
		}
	}

	if err := os.RemoveAll(dir); err != nil {
		logger.Printf("Erro ao excluir diretório %s: %v", dir, err)
	} else {
		logger.Printf("Diretório principal %s excluído com sucesso", dir)
	}
}

// main é a função principal do script
func main() {
	start := time.Now()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	db, err := setupDB()
	if err != nil {
		logger.Fatalf("Erro ao configurar banco de dados: %v", err)
	}
	defer db.Close()

	createTables(db, logger)

	links, err := fetchDownloadLinks(logger)
	if err != nil {
		logger.Fatalf("Erro ao buscar links de download: %v", err)
	}

	jobs := make([]DownloadJob, len(links))
	for i, link := range links {
		jobs[i] = DownloadJob{
			URL:  link,
			Path: filepath.Join("dados-receita", filepath.Base(link)),
		}
	}

	if err := os.MkdirAll("dados-receita", os.ModePerm); err != nil {
		logger.Fatalf("Erro ao criar diretório dados-receita: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < len(jobs); i += maxWorkers {
		end := i + maxWorkers
		if end > len(jobs) {
			end = len(jobs)
		}
		batch := jobs[i:end]

		for _, job := range batch {
			wg.Add(1)
			go downloadFile(job, logger, &wg, db)
		}
		wg.Wait()
	}

	extractPath := "dados-receita"
	for _, job := range jobs {
		if _, err := os.Stat(job.Path); os.IsNotExist(err) {
			logger.Printf("Arquivo %s não existe, pulando extração", job.Path)
			continue
		}
		if err := extractZIP(job.Path, extractPath, logger); err != nil {
			logger.Printf("Falha na extração de %s, prosseguindo para próximos arquivos", job.Path)
			continue
		}
	}

	files, err := os.ReadDir(extractPath)
	if err != nil {
		logger.Printf("Erro ao ler diretório extraído: %v", err)
	} else {
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			if !strings.HasSuffix(file.Name(), ".csv") {
				logger.Printf("Ignorando arquivo não CSV: %s", file.Name())
				continue
			}
			csvPath := filepath.Join(extractPath, file.Name())
			if err := importDataToDB(csvPath, db, logger); err != nil {
				logger.Printf("Erro ao importar %s para o banco: %v", csvPath, err)
				continue
			}
		}
	}

	cleanup(logger)

	elapsed := time.Since(start)
	logger.Printf("Processo concluído com sucesso em %v!", elapsed)
}
