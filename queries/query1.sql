-- Esta query tem como objetivo analisar a relação entre o desempenho médio dos alunos do IFB no ENEM 
-- e a sua remuneração média após ingressarem no mercado de trabalho.
-- Para isso, associamos o ano de admissão do aluno no mercado de trabalho (data_admissao_declarada)
-- com o ano em que ele participou do ENEM (nu_ano), assumindo que ambos representam o mesmo ciclo de formação.

WITH medias_salariais AS (
    -- Calcula a média da remuneração nominal e do salário contratual dos alunos por:
    -- 1. ano de admissão no trabalho (extraído de data_admissao_declarada)
    -- 2. escola em que o aluno estudou (via matrícula)
    SELECT
        EXTRACT(YEAR FROM ve.data_admissao_declarada) AS ano_admissao,
        m.co_escola_educacenso,
        ROUND(AVG(r.vl_remun_media_nom), 2) AS media_remun_media_nom,
        ROUND(AVG(r.vl_salario_contratual), 2) AS media_salario_contratual
    FROM
        vinculo_empregaticio ve
    JOIN remuneracao r ON ve.id_remuneracao = r.id_remuneracao
    JOIN matricula_ifb m ON ve.id_aluno = m.id_aluno
    WHERE
        ve.data_admissao_declarada IS NOT NULL
    GROUP BY
        ano_admissao,
        m.co_escola_educacenso
)

-- Associa as médias salariais calculadas com as médias de desempenho no ENEM 
-- para os mesmos anos e escolas, retornando os indicadores lado a lado.
SELECT
    ms.ano_admissao,
    ms.co_escola_educacenso,
    ms.media_remun_media_nom,
    ms.media_salario_contratual,
    de.nu_media_cn,
    de.nu_media_ch,
    de.nu_media_lp,
    de.nu_media_mt,
    de.nu_media_red
FROM
    medias_salariais ms
JOIN desempenho_enem de
    ON ms.ano_admissao = de.nu_ano
    AND ms.co_escola_educacenso = de.co_escola_educacenso
WHERE
    ms.co_escola_educacenso = '53006178'  -- IFB (Instituto Federal de Brasília)
ORDER BY
    ms.ano_admissao;

