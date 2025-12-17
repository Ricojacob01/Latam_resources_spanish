-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Preparation Notebook
-- MAGIC
-- MAGIC Este notebook tem como objetivo **criar tabelas e dados fictícios** para fins de demonstração.  
-- MAGIC Todas as estruturas aqui utilizadas (catálogo, schema e tabelas) foram definidas apenas como **exemplo** para simulações controladas.  
-- MAGIC
-- MAGIC  **Catálogo de exemplo:** `daiwt_sp_2025`  
-- MAGIC  **Schema de exemplo:** `digital_bank`  
-- MAGIC
-- MAGIC ## Como utilizar
-- MAGIC Caso você esteja usando este repositório em um ambiente real, **recomenda-se substituir**:
-- MAGIC - O **catálogo** (`daiwt_sp_2025`) pelo catálogo correspondente ao seu ambiente.  
-- MAGIC - O **schema** (`digital_bank`) pelo schema de sua organização.  
-- MAGIC - Os **dados fictícios** pela ingestão dos seus próprios datasets.  
-- MAGIC
-- MAGIC > **Atenção**: As tabelas e dados aqui gerados são apenas para **ilustração**. Não devem ser utilizados como base para análises em ambientes produtivos.
-- MAGIC

-- COMMAND ----------

CREATE CATALOG governance_demo

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS governance_demo.digital_bank;

-- COMMAND ----------

USE CATALOG governance_demo;

-- COMMAND ----------

-- 1) clientes
CREATE OR REPLACE TABLE digital_bank.clientes (
  cliente_id        BIGINT,
  nome_completo     STRING,
  cpf               STRING,
  data_nascimento   DATE,
  sexo              STRING,
  email             STRING,
  telefone_celular  STRING,
  endereco_rua      STRING,
  endereco_numero   STRING,
  endereco_cidade   STRING,
  endereco_uf       STRING,
  data_cadastro     DATE,
  status_cliente    STRING
);

INSERT INTO digital_bank.clientes VALUES
  (1001,'Maria Fernanda Lima','12345678901', DATE'1988-03-14','F','maria.lima@email.com','11987654321','Rua das Palmeiras','45','São Paulo','SP',DATE'2020-05-01','ativo'),
  (1002,'Joana Silva Costa','98765432100', DATE'1992-11-02','F','joana.costa@email.com','21988887777','Av. Atlântica','1200','Rio de Janeiro','RJ',DATE'2023-04-15','ativo'),
  (1003,'Carlos Eduardo Nogueira','45678912355', DATE'1979-07-25','M','carlos.nogueira@email.com','41999998888','Rua XV de Novembro','300','Curitiba','PR',DATE'2021-08-20','bloqueado'),
  (1004,'Rafael dos Santos','32165498777', DATE'1985-12-09','M','rafael.santos@email.com','31977776666','Rua dos Andradas','75','Belo Horizonte','MG',DATE'2019-02-10','ativo'),
  (1005,'Patrícia Moura Almeida','15975348620', DATE'1995-06-17','F','patricia.moura@email.com','11955554444','Rua das Laranjeiras','101','Campinas','SP',DATE'2024-07-01','ativo');

-- 2) transacoes
CREATE OR REPLACE TABLE digital_bank.transacoes (
  transacao_id       BIGINT,
  cliente_id         BIGINT,
  conta_id           BIGINT,
  data_transacao     TIMESTAMP,
  tipo_transacao     STRING,
  valor              DECIMAL(18,2),
  meio_pagamento     STRING,
  estabelecimento_id BIGINT,
  status_transacao   STRING
);

INSERT INTO digital_bank.transacoes VALUES
  (40011,1001,2001,TIMESTAMP'2025-08-10 14:32:11','compra',     150.75,'cartao_credito',3001,'aprovada'),
  (40012,1001,2001,TIMESTAMP'2025-08-11 09:15:22','pix',        320.00,'pix',           NULL,'aprovada'),
  (40123,1002,2002,TIMESTAMP'2025-08-12 10:01:45','compra',     980.50,'cartao_credito',3002,'aprovada'),
  (40234,1003,2003,TIMESTAMP'2025-08-13 20:15:09','compra',      45.90,'cartao_debito', 3004,'negada'),
  (40345,1004,2004,TIMESTAMP'2025-08-13 22:00:12','saque',      200.00,'caixa_eletronico',NULL,'aprovada'),
  (40456,1005,2005,TIMESTAMP'2025-08-14 08:45:33','pagamento',  850.00,'boleto',        NULL,'aprovada'),
  (40555,1001,2001,TIMESTAMP'2025-08-14 18:32:57','compra',     600.00,'cartao_credito',3003,'aprovada');

-- 3) contas
CREATE OR REPLACE TABLE digital_bank.contas (
  conta_id          BIGINT,
  cliente_id        BIGINT,
  tipo_conta        STRING,
  agencia           STRING,
  numero_conta      STRING,
  status            STRING,
  saldo_atual       DECIMAL(18,2),
  data_abertura     DATE,
  data_encerramento DATE
);

INSERT INTO digital_bank.contas VALUES
  (2001,1001,'corrente','1234','00012345-6','ativa',     5823.75, DATE'2022-03-15', NULL),
  (2002,1002,'pagamento','1234','00098765-4','ativa',      312.90, DATE'2024-10-02', NULL),
  (2003,1003,'corrente','2231','00112233-0','bloqueada',    50.00, DATE'2021-07-20', NULL),
  (2004,1004,'poupanca','2231','00990011-9','ativa',    12000.00, DATE'2020-01-10', NULL),
  (2005,1005,'corrente','3131','00770088-1','encerrada',     0.00, DATE'2019-09-01', DATE'2024-12-31');

-- 4) cartoes
CREATE OR REPLACE TABLE digital_bank.cartoes (
  cartao_id         BIGINT,
  cliente_id        BIGINT,
  conta_id          BIGINT,
  tipo_cartao       STRING,
  bandeira          STRING,
  final_4           STRING,
  status            STRING,
  limite_total      DECIMAL(18,2),
  limite_disponivel DECIMAL(18,2),
  dia_fechamento    TINYINT,
  dia_vencimento    TINYINT,
  emissao_data      DATE
);

INSERT INTO digital_bank.cartoes VALUES
  (5001,1001,2001,'credito','visa','1234','ativo',     10000.00, 6400.00,  5, 12, DATE'2023-11-01'),
  (5002,1002,2002,'multiple','mastercard','7788','ativo', 2500.00,  900.00, 10, 17, DATE'2024-10-05'),
  (5003,1003,2003,'debito','visa','9911','ativo',            NULL,     NULL, NULL,NULL, DATE'2022-06-12'),
  (5004,1004,2004,'credito','elo','0042','bloqueado',  6000.00, 1500.00,  7, 14, DATE'2021-02-20');

-- 5) faturas_cartao
CREATE OR REPLACE TABLE digital_bank.faturas_cartao (
  fatura_id        BIGINT,
  cartao_id        BIGINT,
  competencia      STRING,
  valor_total      DECIMAL(18,2),
  valor_minimo     DECIMAL(18,2),
  status           STRING,
  data_fechamento  DATE,
  data_vencimento  DATE,
  pagamento_data   DATE,
  pagamento_valor  DECIMAL(18,2)
);

INSERT INTO digital_bank.faturas_cartao VALUES
  (7001,5001,'2025-07',3560.40,356.04,'paga',     DATE'2025-07-05', DATE'2025-07-12', DATE'2025-07-10',3560.40),
  (7002,5001,'2025-08', 394.90, 39.49,'fechada',  DATE'2025-08-05', DATE'2025-08-12', NULL,              NULL),
  (7003,5002,'2025-08',1660.00,166.00,'em_atraso',DATE'2025-08-10', DATE'2025-08-17', NULL,              NULL),
  (7004,5004,'2025-06', 820.55, 82.05,'paga',     DATE'2025-06-07', DATE'2025-06-14', DATE'2025-06-13', 820.55);

-- 6) estabelecimentos
CREATE OR REPLACE TABLE digital_bank.estabelecimentos (
  estabelecimento_id BIGINT,
  cnpj               STRING,
  nome_fantasia      STRING,
  categoria          STRING,
  mcc                STRING,
  cidade             STRING,
  uf                 STRING,
  risco_score        DECIMAL(5,2)
);

INSERT INTO digital_bank.estabelecimentos VALUES
  (3001,'12345678000190','SuperMais','supermercado','5411','São Paulo','SP',12.30),
  (3002,'99887766000155','Burguer Top','restaurante','5812','Rio de Janeiro','RJ',18.70),
  (3003,'11223344000100','ShopOnline BR','e-commerce','5969','Curitiba','PR',35.10),
  (3004,'55667788000122','Taxi Fácil','transporte','4121','Belo Horizonte','MG',22.00);

-- 7) dispositivos
CREATE OR REPLACE TABLE digital_bank.dispositivos (
  device_id          STRING,
  cliente_id         BIGINT,
  tipo               STRING,
  so                 STRING,
  versao_so          STRING,
  modelo             STRING,
  jailbroken_root    BOOLEAN,
  ultimo_login_ip    STRING,
  ultimo_login_ts    TIMESTAMP,
  score_risco_device DECIMAL(5,2)
);

INSERT INTO digital_bank.dispositivos VALUES
  ('a1f0-001',1001,'mobile','Android','14','Samsung S23', false,'200.147.35.10', TIMESTAMP'2025-08-13 21:45:10', 8.20),
  ('a1f0-002',1001,'desktop','Windows','11','Dell XPS',   false,'187.12.55.2',  TIMESTAMP'2025-08-12 09:12:03',12.50),
  ('b9cc-777',1002,'mobile','iOS','17.5','iPhone 14',     false,'201.55.30.200',TIMESTAMP'2025-08-14 08:05:44', 6.10),
  ('cx-9090',1003,'mobile','Android','13','Moto G',        true,'45.172.10.33', TIMESTAMP'2025-08-10 23:11:02',52.00);

-- 8) chaves_pix
CREATE OR REPLACE TABLE digital_bank.chaves_pix (
  chave_id     BIGINT,
  cliente_id   BIGINT,
  tipo_chave   STRING,
  chave_valor  STRING,
  instituicao  STRING,
  ativa        BOOLEAN,
  data_criacao DATE
);

INSERT INTO digital_bank.chaves_pix VALUES
  (9001,1001,'cpf','12345678901','Banco Digital S.A.', true,  DATE'2023-02-10'),
  (9002,1002,'email','joana.silva@email.com','Banco Digital S.A.', true, DATE'2024-11-01'),
  (9003,1003,'aleatoria','3d2e8c7f-2a1b-4f9c-95e1','Banco X', false, DATE'2022-07-15'),
  (9004,1004,'telefone','+5511999998888','Banco Digital S.A.', true, DATE'2021-05-09');

-- 9) eventos_fraude
CREATE OR REPLACE TABLE digital_bank.eventos_fraude (
  evento_id     BIGINT,
  transacao_id  BIGINT,
  cliente_id    BIGINT,
  tipo_evento   STRING,
  origem        STRING,
  score_fraude  DECIMAL(5,2),
  motivo        STRING,
  acao_tomada   STRING,
  ts_evento     TIMESTAMP
);

INSERT INTO digital_bank.eventos_fraude VALUES
  (8001,40011,1003,'suspeita_dispositivo','autorizacao', 88.50,'Dispositivo com root','negar',              TIMESTAMP'2025-08-10 23:11:05'),
  (8002,40123,1002,'velocity','autorizacao',              72.10,'Múltiplas tentativas em <5min','revisar',   TIMESTAMP'2025-08-12 10:02:44'),
  (8003,40555,1001,'geolocalizacao','monitoramento',      65.00,'Compra fora do padrão geográfico','contactar_cliente', TIMESTAMP'2025-08-13 22:01:10'),
  (8004,   NULL,1004,'chargeback','pos_autorizacao',      91.00,'Contestação pelo emissor','revisar',        TIMESTAMP'2025-07-29 14:55:12');

-- 10) tickets_suporte
CREATE OR REPLACE TABLE digital_bank.tickets_suporte (
  ticket_id     BIGINT,
  cliente_id    BIGINT,
  canal         STRING,
  assunto       STRING,
  status        STRING,
  prioridade    STRING,
  aberto_ts     TIMESTAMP,
  fechado_ts    TIMESTAMP,
  sla_violado   BOOLEAN,
  nps           TINYINT
);

INSERT INTO digital_bank.tickets_suporte VALUES
  (6001,1001,'app','Dúvida sobre limite do cartão','resolvido','baixa',   TIMESTAMP'2025-08-11 09:15:00', TIMESTAMP'2025-08-11 10:02:00', false, 80),
  (6002,1002,'telefone','Compra não reconhecida','em_andamento','critica', TIMESTAMP'2025-08-14 18:42:30', NULL,                           NULL, NULL),
  (6003,1003,'email','App travando ao abrir','resolvido','media',          TIMESTAMP'2025-08-10 08:00:00', TIMESTAMP'2025-08-10 13:20:10', true,  20),
  (6004,1004,'chat','Alteração de data de vencimento','resolvido','baixa', TIMESTAMP'2025-07-30 16:11:45', TIMESTAMP'2025-07-30 16:25:00', false, 70);
