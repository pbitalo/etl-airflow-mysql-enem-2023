# Este arquivo agrupa todas as instruções SQL's utilizadas no processo de ETL, incluindo criação e exclusão de banco de dados e tabelas (DDL) 
# e instruções de inserção em tabelas de dimensão e fato (DML)

CREATE_DATABASES = [
    "CREATE DATABASE IF NOT EXISTS enem_producao;",
    "CREATE DATABASE IF NOT EXISTS enem_dw;"
]

DROP_TABLES = {
    "enem_producao": [
        "DROP TABLE IF EXISTS staging_enem;"
    ],
    "enem_dw": [
        "DROP TABLE IF EXISTS fato_notas;",
        "DROP TABLE IF EXISTS dim_candidato;",
        "DROP TABLE IF EXISTS dim_estado;"
    ]
}

CREATE_TABLES = {
    "enem_producao": """
        CREATE TABLE staging_enem (
            id INT AUTO_INCREMENT PRIMARY KEY,
            TP_FAIXA_ETARIA INT,
            SG_UF_PROVA VARCHAR(2),
            NU_NOTA_MT FLOAT,
            NU_NOTA_CN FLOAT,
            NU_NOTA_LC FLOAT,
            NU_NOTA_CH FLOAT,
            NU_NOTA_REDACAO FLOAT,
            TP_SEXO CHAR(1)
        );
    """,
    "enem_dw": [
        """
        CREATE TABLE dim_estado (
            id INT AUTO_INCREMENT PRIMARY KEY,
            SG_UF_PROVA VARCHAR(2) UNIQUE
        );
        """,
        """
        CREATE TABLE dim_candidato (
            id INT AUTO_INCREMENT PRIMARY KEY,
            TP_FAIXA_ETARIA INT,
            TP_SEXO CHAR(1)
        );
        """,
        """
        CREATE TABLE fato_notas (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_estado INT,
            id_candidato INT,
            NU_NOTA_MT FLOAT,
            NU_NOTA_CN FLOAT,
            NU_NOTA_LC FLOAT,
            NU_NOTA_CH FLOAT,
            NU_NOTA_REDACAO FLOAT,
            FOREIGN KEY (id_estado) REFERENCES dim_estado(id),
            FOREIGN KEY (id_candidato) REFERENCES dim_candidato(id)
        );
        """
    ]
}

# Inserção na tabela `dim_estado`
INSERT_DIM_ESTADO = """
    INSERT IGNORE INTO dim_estado (SG_UF_PROVA) VALUES (%s)
"""

# Inserção na tabela `dim_candidato`
INSERT_DIM_CANDIDATO = """
    INSERT IGNORE INTO dim_candidato (TP_FAIXA_ETARIA, TP_SEXO) 
    VALUES (%s, %s)
"""

# Buscar ID do estado
SELECT_ID_ESTADO = """
    SELECT id FROM dim_estado WHERE SG_UF_PROVA = %s
"""

# Buscar ID do candidato
SELECT_ID_CANDIDATO = """
    SELECT id 
    FROM dim_candidato 
    WHERE TP_FAIXA_ETARIA = %s 
      AND TP_SEXO = %s
"""

# Inserção na tabela `fato_notas`
INSERT_FATO_NOTAS = """
    INSERT INTO fato_notas 
        (id_estado, id_candidato, 
         NU_NOTA_MT, NU_NOTA_CN, NU_NOTA_LC, NU_NOTA_CH, NU_NOTA_REDACAO) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# 1️⃣ Porcentagem de candidatos do sexo masculino e feminino por estado
QUERY_PORCENTAGEM_GENERO_ESTADO = """
    SELECT 
        e.SG_UF_PROVA,
        c.TP_SEXO,
        COUNT(*) AS total_candidatos,
        ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY e.SG_UF_PROVA)), 2) AS percentual
    FROM fato_notas f
    JOIN dim_candidato c ON f.id_candidato = c.id
    JOIN dim_estado e ON f.id_estado = e.id
    GROUP BY e.SG_UF_PROVA, c.TP_SEXO
    ORDER BY e.SG_UF_PROVA, c.TP_SEXO;
"""

# 2️⃣ Médias por disciplina por sexo em cada estado
QUERY_MEDIA_DISCIPLINA_SEXO_ESTADO = """
    SELECT 
        e.SG_UF_PROVA,
        c.TP_SEXO,
        AVG(f.NU_NOTA_MT) AS media_matematica,
        AVG(f.NU_NOTA_CN) AS media_ciencias_natureza,
        AVG(f.NU_NOTA_LC) AS media_linguagens_codigos,
        AVG(f.NU_NOTA_CH) AS media_ciencias_humanas,
        AVG(f.NU_NOTA_REDACAO) AS media_redacao
    FROM fato_notas f
    JOIN dim_candidato c ON f.id_candidato = c.id
    JOIN dim_estado e ON f.id_estado = e.id
    GROUP BY e.SG_UF_PROVA, c.TP_SEXO
    ORDER BY e.SG_UF_PROVA, c.TP_SEXO;
"""

# 3️⃣ Média geral entre homens e mulheres por estado
QUERY_MEDIA_GERAL_SEXO_ESTADO = """
    SELECT 
        e.SG_UF_PROVA,
        c.TP_SEXO,
        ROUND(AVG((f.NU_NOTA_MT + f.NU_NOTA_CN + f.NU_NOTA_LC + f.NU_NOTA_CH + f.NU_NOTA_REDACAO) / 5), 2) AS media_geral
    FROM fato_notas f
    JOIN dim_candidato c ON f.id_candidato = c.id
    JOIN dim_estado e ON f.id_estado = e.id
    GROUP BY e.SG_UF_PROVA, c.TP_SEXO
    ORDER BY e.SG_UF_PROVA, c.TP_SEXO;
"""

# 4️⃣ Estado com maior e menor média geral
QUERY_ESTADO_EXTREMOS = """
    WITH media_estados AS (
        SELECT 
            e.SG_UF_PROVA,
            ROUND(AVG((f.NU_NOTA_MT + f.NU_NOTA_CN + f.NU_NOTA_LC + f.NU_NOTA_CH + f.NU_NOTA_REDACAO) / 5), 2) AS media_geral
        FROM fato_notas f
        JOIN dim_estado e ON f.id_estado = e.id
        GROUP BY e.SG_UF_PROVA
    )
    SELECT SG_UF_PROVA, media_geral
    FROM media_estados
    WHERE media_geral = (SELECT MAX(media_geral) FROM media_estados)
       OR media_geral = (SELECT MIN(media_geral) FROM media_estados)
    ORDER BY media_geral;
"""

# 5️⃣ Total de candidatos contabilizados
QUERY_TOTAL_CANDIDATOS = """
    SELECT COUNT(DISTINCT id_candidato) AS total_candidatos FROM fato_notas;
"""

# 6️⃣ Média geral do ENEM 2023
QUERY_MEDIA_GERAL = """
    SELECT ROUND(AVG((NU_NOTA_MT + NU_NOTA_CN + NU_NOTA_LC + NU_NOTA_CH + NU_NOTA_REDACAO) / 5), 2) AS media_geral_total
    FROM fato_notas;
"""
