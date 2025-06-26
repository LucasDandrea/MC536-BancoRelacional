WITH indicadores_abandono AS (
	SELECT 
		m.id_aluno,
		m.ano AS ano_entrada_no_ifb,
		v.data_admissao_declarada,
		v.tempo_emprego,
		i.nu_taxa_abandono AS taxa_abandono_ifb,
		ROW_NUMBER() OVER (PARTITION BY m.id_aluno ORDER BY v.data_admissao_declarada DESC) AS ordem_empregos
	FROM 
		matricula_ifb m
	JOIN vinculo_empregaticio v 
		ON v.id_aluno = m.id_aluno
	JOIN indicadores_escolares i 
		ON i.nu_ano = CAST(m.ano AS INT) AND i.co_escola_educacenso = '53006178'
	WHERE 
		CAST(m.ano AS INT) BETWEEN 2009 AND 2015
		AND v.motivo_desligamento NOT IN ('NAO DESLIGADO NO ANO', 'null')
		AND i.nu_taxa_abandono IS NOT NULL
), classificados AS (
	SELECT 
		id_aluno,
		ano_entrada_no_ifb,
		data_admissao_declarada,
		CASE 
			WHEN tempo_emprego <= 12 THEN 'BAIXA'
			WHEN tempo_emprego > 12 AND tempo_emprego <= 36 THEN 'MEDIA'
			ELSE 'ALTA'
		END AS abandono_empregaticio,
		CASE 
			WHEN taxa_abandono_ifb < 5 THEN 'BAIXA'
			WHEN taxa_abandono_ifb >= 5 AND taxa_abandono_ifb < 10 THEN 'MEDIA'
			ELSE 'ALTA'
		END AS abandono_escolar
	FROM indicadores_abandono
	WHERE ordem_empregos = 1
)
SELECT * FROM classificados;
