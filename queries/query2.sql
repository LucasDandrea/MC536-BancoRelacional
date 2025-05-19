WITH vinculos_ano AS (
    SELECT
        ve.id_vinculo,
        ve.id_aluno,
        ve.id_loc,
        EXTRACT(YEAR FROM ve.data_admissao_declarada) AS ano_admissao,
        r.vl_salario_contratual
    FROM vinculo_empregaticio ve
    JOIN remuneracao r ON r.id_remuneracao = ve.id_remuneracao
    WHERE ve.data_admissao_declarada IS NOT NULL
),

enem_por_loc_ano AS (
    SELECT
        e.id_loc,
        de.nu_ano,
        AVG(
			(de.nu_media_cn + de.nu_media_ch + de.nu_media_lp + de.nu_media_mt + de.nu_media_red)/5
		) AS media_enem_local_ano
    FROM desempenho_enem de
    JOIN escola es ON de.co_escola_educacenso = es.co_escola_educacenso
    JOIN localizacao e ON es.id_loc = e.id_loc
    GROUP BY e.id_loc, de.nu_ano
)

SELECT
    v.id_loc,
	lo.uf,
	lo.municipio,
    ROUND(AVG(v.vl_salario_contratual), 2) AS media_salarial,
    ROUND(AVG(en.media_enem_local_ano),3) AS media_enem
FROM vinculos_ano v 
JOIN enem_por_loc_ano en ON v.id_loc = en.id_loc AND v.ano_admissao = en.nu_ano
JOIN localizacao lo on lo.id_loc = v.id_loc
GROUP BY v.id_loc, lo.uf, lo.municipio
ORDER BY lo.uf ASC;
