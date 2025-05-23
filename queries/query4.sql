-- Pegar os alunos que entraram em 2009/2010/2011/2012/2013/2014/2015 no ifb, observar a taxa de abandono nos 
-- indicadores_escolares e comparar com o tempo de emprego no vinculo_empregaticio

WITH alunos_ano_de_entrada AS (
	SELECT m.id_aluno, m.ano, 
	ROW_NUMBER() OVER(PARTITION BY m.id_aluno ORDER BY m.ano DESC) AS ano_entrada
	FROM matricula_ifb m
	WHERE m.ano = '2009' OR m.ano = '2010' OR m.ano = '2011' OR m.ano = '2012' OR m.ano = '2013' OR m.ano = '2014' OR m.ano = '2015'
), formatando_uma_matricula AS (  
	SELECT a.id_aluno, a.ano
	FROM alunos_ano_de_entrada a 
	WHERE a.ano_entrada = 1
), comparando_com_ano_empregado AS (
	SELECT v.id_aluno, f.ano, v.data_admissao_declarada, v.tempo_emprego
	FROM formatando_uma_matricula f
	JOIN vinculo_empregaticio v ON v.id_aluno = f.id_aluno WHERE v.motivo_desligamento != 'NAO DESLIGADO NO ANO' AND v.motivo_desligamento != 'null'
), puxando_taxa_abandono_escola AS (
	SELECT c.id_aluno, c.ano, c.data_admissao_declarada, c.tempo_emprego, i.nu_taxa_abandono,
	ROW_NUMBER() OVER(PARTITION BY c.id_aluno ORDER BY c.data_admissao_declarada DESC) AS ordem_empregos
	FROM comparando_com_ano_empregado c
	JOIN indicadores_escolares i ON i.nu_ano = CAST(c.ano AS INT) WHERE i.co_escola_educacenso  = '53006178'
), formatando_primeiro_emprego AS (
	SELECT p.id_aluno, p.ano AS ano_entrada_no_ifb, p.data_admissao_declarada, p.tempo_emprego, p.nu_taxa_abandono AS taxa_abandono_ifb
	FROM puxando_taxa_abandono_escola p
	WHERE ordem_empregos = 1 AND p.nu_taxa_abandono IS NOT NULL
), indicadores_abandono AS (
	SELECT f.id_aluno, f.ano_entrada_no_ifb, f.data_admissao_declarada,
		CASE 
			WHEN f.tempo_emprego <= 12 THEN 'BAIXA'
			WHEN f.tempo_emprego > 12 AND f.tempo_emprego <= 36 THEN 'MEDIA'
			ELSE 'ALTA'
		END AS abandono_empregaticio,
		CASE 
			WHEN f.taxa_abandono_ifb < 5 THEN 'BAIXA'
			WHEN f.taxa_abandono_ifb >= 5 AND f.taxa_abandono_ifb < 10 THEN 'MEDIA'
			ELSE 'ALTA'
		END AS abandono_escolar
	FROM formatando_primeiro_emprego f
) SELECT * FROM indicadores_abandono