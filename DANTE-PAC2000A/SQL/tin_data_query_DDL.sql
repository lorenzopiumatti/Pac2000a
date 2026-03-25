-- DDL tabella tin_data_query
-- Contiene gli statement SQL eseguiti da pr_last_operations per update_type, in ordine (ord)
-- id da sequence; flag statement attivo; campi audit

-- Creare la sequence se non esiste (adattare schema se necessario)
-- CREATE SEQUENCE IF NOT EXISTS boom.tsm_receptions_last_cost_id_seq;

-- Per tabella gia'' esistente (solo aggiunta colonna descrizione_step):
-- ALTER TABLE boom.tin_data_query ADD COLUMN IF NOT EXISTS descrizione_step character varying(500);
-- COMMENT ON COLUMN boom.tin_data_query.descrizione_step IS 'Descrizione dello step usata nel log (come in procedura originale)';

-- DROP TABLE IF EXISTS boom.tin_data_query;

CREATE TABLE IF NOT EXISTS boom.tin_data_query (
    id                  bigint NOT NULL DEFAULT nextval('tsm_receptions_last_cost_id_seq'::regclass),
    update_type         character varying(10) NOT NULL,
    ord                 integer NOT NULL,
    descrizione_step    character varying(500),
    sql                 text NOT NULL,
    active_flag         character varying(1) NOT NULL DEFAULT '1',
    creation_date        timestamp(0) with time zone DEFAULT now(),
    update_date         timestamp(0) with time zone DEFAULT now(),
    creation_user       character varying(100),
    last_user           character varying(100),
    transaction_code    character varying(50),
    CONSTRAINT pk_tin_data_query PRIMARY KEY (id)
);

COMMENT ON TABLE boom.tin_data_query IS 'Query eseguite dalla procedura pr_last_operations per update_type, ordinate per ord; active_flag=1 eseguito';
COMMENT ON COLUMN boom.tin_data_query.active_flag IS '1=statement attivo (eseguito), 0=non eseguito';
COMMENT ON COLUMN boom.tin_data_query.descrizione_step IS 'Descrizione dello step usata nel log (come in procedura originale)';
