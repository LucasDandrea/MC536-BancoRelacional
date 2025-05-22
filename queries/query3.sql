WITH media_enem_por_localizacao AS (
    SELECT 
        l.id_loc,
        l.uf,
        l.municipio,
        ROUND(AVG((d.nu_media_cn + d.nu_media_ch + d.nu_media_lp + d.nu_media_mt + d.nu_media_red)/5), 3) AS media_enem,
        COUNT(DISTINCT e.co_escola_educacenso) AS qtd_escolas
    FROM 
        localizacao l
    JOIN 
        escola e ON l.id_loc = e.id_loc
    JOIN 
        desempenho_enem d ON e.co_escola_educacenso = d.co_escola_educacenso
    GROUP BY 
        l.id_loc, l.uf, l.municipio
)

SELECT 
    v_loc.uf,
    v_loc.municipio,
    COUNT(v.id_vinculo) AS total_vinculos,
    ROUND(AVG(r.qtd_hora_contr), 2) AS media_horas_contratuais,
    ROUND(AVG(r.vl_remun_media_nom), 2) AS remuneracao_media_nominal,
    ROUND(AVG(r.vl_remun_media_nom) / NULLIF(AVG(r.qtd_hora_contr), 0), 2) AS valor_hora_medio,
    mel.media_enem,
    mel.qtd_escolas
FROM 
    vinculo_empregaticio v
JOIN 
    remuneracao r ON v.id_remuneracao = r.id_remuneracao
JOIN 
    localizacao v_loc ON v.id_loc = v_loc.id_loc
JOIN
    media_enem_por_localizacao mel ON v_loc.id_loc = mel.id_loc
WHERE 
    v.indicador_vinculo_ativo = TRUE
    AND r.qtd_hora_contr > 0
    AND r.vl_remun_media_nom > 0
GROUP BY 
    v_loc.uf, v_loc.municipio, mel.media_enem, mel.qtd_escolas
HAVING 
    COUNT(v.id_vinculo) >= 5
ORDER BY 
    v_loc.uf, remuneracao_media_nominal DESC;