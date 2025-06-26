WITH alunos_remunerados_fora_da_cidade AS (
	SELECT 
		m.id_aluno,
		c.no_curso,
		c.modalidade_ensino,
		r.vl_ultima_remuneracao_ano,
		loc_escola.municipio AS municipio_escola,
		loc_empresa.municipio AS municipio_empregado,
		ROW_NUMBER() OVER (PARTITION BY m.id_aluno ORDER BY r.vl_ultima_remuneracao_ano DESC) AS ultima_remuneracao
	FROM 
		matricula_ifb m
	JOIN curso_ifb c ON m.co_curso = c.co_curso
	JOIN vinculo_empregaticio v ON v.id_aluno = m.id_aluno
	JOIN remuneracao r ON v.id_remuneracao = r.id_remuneracao
	JOIN escola e ON e.co_escola_educacenso = m.co_escola_educacenso
	JOIN localizacao loc_escola ON loc_escola.id_loc = e.id_loc
	JOIN localizacao loc_empresa ON loc_empresa.id_loc = v.id_loc
	WHERE 
		c.modalidade_ensino = 'Educação Presencial'
		AND r.vl_ultima_remuneracao_ano >= 1000
		AND loc_escola.id_loc != loc_empresa.id_loc
)
SELECT 
	id_aluno,
	no_curso,
	modalidade_ensino,
	vl_ultima_remuneracao_ano AS ultima_remuneracao_anual,
	municipio_escola,
	municipio_empregado
FROM 
	alunos_remunerados_fora_da_cidade
WHERE 
	ultima_remuneracao = 1
ORDER BY 
	vl_ultima_remuneracao_ano DESC;
