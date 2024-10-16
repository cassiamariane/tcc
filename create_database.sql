CREATE DATABASE cnpj;
USE cnpj;

CREATE TABLE municipios (
    codigo bigint primary key,
	descricao VARCHAR(255)
);
CREATE TABLE paises (
    codigo INT PRIMARY KEY,
	descricao VARCHAR(255)
);
CREATE TABLE cnaes (
    codigo bigint primary key,
	descricao VARCHAR(255)
);
CREATE TABLE naturezas_juridicas (
    codigo INT PRIMARY KEY,
	descricao VARCHAR(255)
);
CREATE TABLE empresas (
	cnpj_basico bigint PRIMARY KEY,
    razao_social VARCHAR(255),
	natureza_juridica INT,
	capital_social VARCHAR(255),
	porte INT,
	ente_federativo_responsavel VARCHAR(255),
	FOREIGN KEY (natureza_juridica) REFERENCES naturezas_juridicas(codigo)
);

CREATE TABLE estabelecimentos (
    cnpj_basico BIGINT,
    cnpj_ordem BIGINT,
    cnpj_dv INT,
    identificador_matriz_filial INT,
    nome_fantasia VARCHAR(255),
    situacao_cadastral INT,
    data_situacao_cadastral DATETIME,
    motivo_situacao_cadastral INT,
    nome_cidade_exterior VARCHAR(255), 
    pais INT,
    data_inicio_atividade DATETIME,
    cnae_fiscal_principal BIGINT,
    tipo_de_logradouro VARCHAR(255), 
    logradouro VARCHAR(255), 
    numero VARCHAR(255),
    complemento VARCHAR(255), 
    bairro VARCHAR(255), 
    cep VARCHAR(255), 
    uf VARCHAR(255),
    municipio BIGINT,
    ddd_1 INT,
    telefone_1 BIGINT,
    ddd_2 INT,
    telefone_2 BIGINT,
    correio_eletronico VARCHAR(255), 
    situacao_especial VARCHAR(255),
    data_situacao_especial DATETIME,
    PRIMARY KEY (cnpj_basico, cnpj_ordem),
    FOREIGN KEY (cnpj_basico) REFERENCES empresas(cnpj_basico),
    FOREIGN KEY (cnae_fiscal_principal) REFERENCES cnaes(codigo),
    FOREIGN KEY (municipio) REFERENCES municipios(codigo),
    FOREIGN KEY (pais) REFERENCES paises(codigo)
);
CREATE TABLE cnae_fiscal_secundaria (
    cnpj_basico BIGINT,
    cnpj_ordem BIGINT,
    codigo BIGINT,
    FOREIGN KEY (cnpj_basico, cnpj_ordem) REFERENCES estabelecimentos(cnpj_basico, cnpj_ordem),
    FOREIGN KEY (codigo) REFERENCES cnaes(codigo)
);