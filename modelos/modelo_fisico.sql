BEGIN;


CREATE TABLE IF NOT EXISTS public.aluno_ifb
(
    id_aluno serial NOT NULL,
    data_nascimento date,
    idade integer,
    sexo character(1) COLLATE pg_catalog."default",
    nacionalidade character varying(50) COLLATE pg_catalog."default",
    raca character varying(50) COLLATE pg_catalog."default",
    portador_deficiencia boolean,
    CONSTRAINT aluno_ifb_pkey PRIMARY KEY (id_aluno)
);

CREATE TABLE IF NOT EXISTS public.curso_ifb
(
    co_curso character varying(50) COLLATE pg_catalog."default" NOT NULL,
    no_curso character varying(200) COLLATE pg_catalog."default" NOT NULL,
    co_tipo_curso character varying(10) COLLATE pg_catalog."default",
    tipo_curso character varying(100) COLLATE pg_catalog."default",
    co_tipo_nivel character varying(10) COLLATE pg_catalog."default",
    ds_tipo_nivel character varying(100) COLLATE pg_catalog."default",
    ds_eixo_tecnologico character varying(200) COLLATE pg_catalog."default",
    modalidade_ensino character varying(100) COLLATE pg_catalog."default",
    carga_horaria integer,
    CONSTRAINT curso_ifb_pkey PRIMARY KEY (co_curso)
);

CREATE TABLE IF NOT EXISTS public.desempenho_enem
(
    id_desempenho serial NOT NULL,
    nu_ano integer NOT NULL,
    nu_media_cn numeric(6, 2),
    nu_media_ch numeric(6, 2),
    nu_media_lp numeric(6, 2),
    nu_media_mt numeric(6, 2),
    nu_media_red numeric(6, 2),
    nu_media_obj numeric(6, 2),
    nu_media_tot numeric(6, 2),
    co_escola_educacenso character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT desempenho_enem_pkey PRIMARY KEY (id_desempenho)
);

CREATE TABLE IF NOT EXISTS public.escola
(
    co_escola_educacenso character varying(50) COLLATE pg_catalog."default" NOT NULL,
    no_escola_educacenso character varying(200) COLLATE pg_catalog."default" NOT NULL,
    tp_dependencia_adm_escola smallint,
    tp_localizacao_escola smallint,
    inse character varying(10) COLLATE pg_catalog."default",
    id_loc integer,
    CONSTRAINT escola_pkey PRIMARY KEY (co_escola_educacenso)
);

CREATE TABLE IF NOT EXISTS public.indicadores_escolares
(
    id_indicadores serial NOT NULL,
    nu_ano integer NOT NULL,
    nu_taxa_permanencia numeric(6, 2),
    nu_taxa_aprovacao numeric(5, 2),
    nu_taxa_reprovacao numeric(5, 2),
    nu_taxa_abandono numeric(5, 2),
    co_escola_educacenso character varying(20) COLLATE pg_catalog."default",
    porte_escola character varying(50) COLLATE pg_catalog."default",
    pc_formacao_docente numeric(5, 2),
    CONSTRAINT indicadores_escolares_pkey PRIMARY KEY (id_indicadores)
);

CREATE TABLE IF NOT EXISTS public.localizacao
(
    id_loc serial NOT NULL,
    uf character varying(2) COLLATE pg_catalog."default" NOT NULL,
    municipio character varying(100) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT localizacao_pkey PRIMARY KEY (id_loc)
);

CREATE TABLE IF NOT EXISTS public.matricula_ifb
(
    id_matricula serial NOT NULL,
    co_ciclo_matricula character varying(50) COLLATE pg_catalog."default",
    periodo character varying(30) COLLATE pg_catalog."default",
    situacao_matricula character varying(50) COLLATE pg_catalog."default",
    tipo_cota character varying(50) COLLATE pg_catalog."default",
    atestado_baixarenda character varying(20) COLLATE pg_catalog."default",
    ano character varying(20) COLLATE pg_catalog."default",
    id_aluno integer,
    co_curso character varying(50) COLLATE pg_catalog."default",
    unidade_ensino character varying(100) COLLATE pg_catalog."default",
    co_escola_educacenso character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT matricula_ifb_pkey PRIMARY KEY (id_matricula)
);

CREATE TABLE IF NOT EXISTS public.participacao_enem
(
    id_participacao serial NOT NULL,
    nu_ano integer NOT NULL,
    nu_matriculas integer,
    nu_participantes_nec_esp integer,
    nu_participantes integer,
    nu_taxa_participacao numeric(5, 2),
    co_escola_educacenso character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT participacao_enem_pkey PRIMARY KEY (id_participacao)
);

CREATE TABLE IF NOT EXISTS public.remuneracao
(
    id_remuneracao serial NOT NULL,
    vl_remun_media_nom numeric(10, 2),
    vl_remun_media_sm numeric(10, 2),
    vl_remun_dezembro_nom numeric(10, 2),
    vl_remun_dezembro_sm numeric(10, 2),
    qtd_hora_contr integer,
    vl_ultima_remuneracao_ano numeric(10, 2),
    vl_salario_contratual numeric(10, 2),
    CONSTRAINT remuneracao_pkey PRIMARY KEY (id_remuneracao)
);

CREATE TABLE IF NOT EXISTS public.vinculo_empregaticio
(
    id_vinculo serial NOT NULL,
    ano_rais integer,
    razao_social character varying(200) COLLATE pg_catalog."default",
    tipo_vinculo_empregaticio character varying(200) COLLATE pg_catalog."default",
    data_admissao_declarada date,
    tempo_emprego integer,
    motivo_desligamento character varying(200) COLLATE pg_catalog."default",
    mes_desligamento character varying(20) COLLATE pg_catalog."default",
    indicador_vinculo_ativo boolean,
    id_aluno integer,
    id_remuneracao integer,
    id_loc integer,
    CONSTRAINT vinculo_empregaticio_pkey PRIMARY KEY (id_vinculo)
);

ALTER TABLE IF EXISTS public.desempenho_enem
    ADD CONSTRAINT desempenho_enem_co_escola_educacenso_fkey FOREIGN KEY (co_escola_educacenso)
    REFERENCES public.escola (co_escola_educacenso) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
CREATE INDEX IF NOT EXISTS idx_desempenho_escola
    ON public.desempenho_enem(co_escola_educacenso);


ALTER TABLE IF EXISTS public.escola
    ADD CONSTRAINT escola_id_loc_fkey FOREIGN KEY (id_loc)
    REFERENCES public.localizacao (id_loc) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
CREATE INDEX IF NOT EXISTS idx_escola_localizacao
    ON public.escola(id_loc);


ALTER TABLE IF EXISTS public.indicadores_escolares
    ADD CONSTRAINT indicadores_escolares_co_escola_educacenso_fkey FOREIGN KEY (co_escola_educacenso)
    REFERENCES public.escola (co_escola_educacenso) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS public.matricula_ifb
    ADD CONSTRAINT matricula_ifb_co_curso_fkey FOREIGN KEY (co_curso)
    REFERENCES public.curso_ifb (co_curso) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
CREATE INDEX IF NOT EXISTS idx_matricula_curso
    ON public.matricula_ifb(co_curso);


ALTER TABLE IF EXISTS public.matricula_ifb
    ADD CONSTRAINT matricula_ifb_escola_fk FOREIGN KEY (co_escola_educacenso)
    REFERENCES public.escola (co_escola_educacenso) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS public.matricula_ifb
    ADD CONSTRAINT matricula_ifb_id_aluno_fkey FOREIGN KEY (id_aluno)
    REFERENCES public.aluno_ifb (id_aluno) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
CREATE INDEX IF NOT EXISTS idx_matricula_aluno
    ON public.matricula_ifb(id_aluno);


ALTER TABLE IF EXISTS public.participacao_enem
    ADD CONSTRAINT participacao_enem_co_escola_educacenso_fkey FOREIGN KEY (co_escola_educacenso)
    REFERENCES public.escola (co_escola_educacenso) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
CREATE INDEX IF NOT EXISTS idx_participacao_escola
    ON public.participacao_enem(co_escola_educacenso);


ALTER TABLE IF EXISTS public.vinculo_empregaticio
    ADD CONSTRAINT vinculo_empregaticio_id_aluno_fkey FOREIGN KEY (id_aluno)
    REFERENCES public.aluno_ifb (id_aluno) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
CREATE INDEX IF NOT EXISTS idx_vinculo_aluno
    ON public.vinculo_empregaticio(id_aluno);


ALTER TABLE IF EXISTS public.vinculo_empregaticio
    ADD CONSTRAINT vinculo_empregaticio_id_remuneracao_fkey FOREIGN KEY (id_remuneracao)
    REFERENCES public.remuneracao (id_remuneracao) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS public.vinculo_empregaticio
    ADD CONSTRAINT vinculo_localizacao_fk FOREIGN KEY (id_loc)
    REFERENCES public.localizacao (id_loc) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

END;

