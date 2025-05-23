WITH matriculados_a_distancia AS (
	SELECT m.id_aluno, m.co_curso, c.modalidade_ensino, c.no_curso, 
	ROW_NUMBER() OVER(PARTITION BY m.id_aluno ORDER BY m.id_matricula DESC) AS ultima_matricula
	FROM matricula_ifb m 
	JOIN curso_ifb c ON m.co_curso = c.co_curso 
	WHERE c.modalidade_ensino = 'Educação Presencial'
),
ultima_matricula AS (
	SELECT m.id_aluno, m.no_curso, m.modalidade_ensino
	FROM matriculados_a_distancia m
	WHERE m.ultima_matricula = 1
),
relacionando_com_idremuneracao AS (
	SELECT m.id_aluno, m.no_curso, m.modalidade_ensino, v.id_remuneracao, v.tipo_vinculo_empregaticio, 
	ROW_NUMBER() OVER(PARTITION BY m.id_aluno ORDER BY v.id_remuneracao DESC) AS ultima_remuneracao
	FROM ultima_matricula m
	JOIN vinculo_empregaticio v ON v.id_aluno = m.id_aluno
), 
ultima_remuneracao AS (
	SELECT r.id_aluno, r.no_curso, r.modalidade_ensino, r.tipo_vinculo_empregaticio, r.id_remuneracao
	FROM relacionando_com_idremuneracao r
	WHERE r.ultima_remuneracao = 1
),
matriculados_a_distancia_remuneracao AS (
	SELECT r.id_aluno, r.no_curso, r.modalidade_ensino, e.vl_ultima_remuneracao_ano, r.tipo_vinculo_empregaticio, r.id_remuneracao
	FROM ultima_remuneracao r
	JOIN remuneracao e ON e.id_remuneracao = r.id_remuneracao
	WHERE e.vl_ultima_remuneracao_ano >= 1000
),
quantidade_alunos_que_saem_da_cidade AS (
	SELECT m.id_aluno, m.no_curso, m.modalidade_ensino, m.vl_ultima_remuneracao_ano, loc_escola.municipio AS municipio_escola, loc_empresa.municipio AS municipio_empregado  
	FROM matriculados_a_distancia_remuneracao m 
	JOIN vinculo_empregaticio v ON v.id_aluno = m.id_aluno
	JOIN localizacao loc_empresa ON loc_empresa.id_loc = v.id_loc
	JOIN matricula_ifb matricula ON matricula.id_aluno = m.id_aluno
	JOIN escola e ON e.co_escola_educacenso = matricula.co_escola_educacenso
	JOIN localizacao loc_escola ON loc_escola.id_loc = e.id_loc
	WHERE loc_escola != loc_empresa
	
),
tabela_formatada AS (
    SELECT 
        q.id_aluno, q.no_curso, q.modalidade_ensino, q.vl_ultima_remuneracao_ano, q.municipio_escola, q.municipio_empregado,
        ROW_NUMBER() OVER(PARTITION BY q.id_aluno ORDER BY q.vl_ultima_remuneracao_ano DESC) AS ultima_remuneracao
    FROM quantidade_alunos_que_saem_da_cidade q
),
filtrada AS (
    SELECT 
        id_aluno, vl_ultima_remuneracao_ano AS ultima_remuneracao_anual, no_curso, modalidade_ensino, municipio_escola, municipio_empregado
    FROM tabela_formatada
    WHERE ultima_remuneracao = 1
	ORDER BY ultima_remuneracao_anual DESC
)
SELECT * FROM filtrada