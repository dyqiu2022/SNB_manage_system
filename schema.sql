--
-- PostgreSQL database dump
--

\restrict XNkcYhHo6WJrEMKF3JRcpeE2AmAbIFld1qg4jk9L2zazdoz1cagNU95ThyK25xH

-- Dumped from database version 15.15 (Debian 15.15-1.pgdg13+1)
-- Dumped by pg_dump version 15.15 (Debian 15.15-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: noco_meta; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA noco_meta;


--
-- Name: pgzgyje645ljgth; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA pgzgyje645ljgth;


--
-- Name: pzm5myopmeqe4jr; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA pzm5myopmeqe4jr;


--
-- Name: bump_stage_instance_version(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.bump_stage_instance_version() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at := CURRENT_TIMESTAMP;
  NEW.row_version := COALESCE(OLD.row_version, 0) + 1;
  RETURN NEW;
END;
$$;


--
-- Name: ensure_project_stage_instances(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.ensure_project_stage_instances() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_project_type text;
BEGIN
  IF NEW.is_active IS NOT TRUE THEN
    RETURN NEW;
  END IF;

  v_project_type := NEW."项目类型";

  IF v_project_type IS NULL OR v_project_type = '' THEN
    RETURN NEW;
  END IF;

  INSERT INTO public."09项目阶段实例表" (
    project_id,
    scope_row_id,
    site_project_id,
    stage_def_id,
    is_active
  )
  SELECT
    NEW.id              AS project_id,
    0                   AS scope_row_id,
    NULL::integer       AS site_project_id,
    d.id                AS stage_def_id,
    TRUE                AS is_active
  FROM public."08项目阶段定义表" d
  WHERE d.project_type = v_project_type
    AND d.stage_scope = 'sync'
    AND d.is_active = TRUE
  ON CONFLICT (project_id, stage_def_id, scope_row_id)
  DO UPDATE SET is_active = TRUE;

  RETURN NEW;
END;
$$;


--
-- Name: ensure_site_stage_instances(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.ensure_site_stage_instances() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_project_id   integer;
  v_project_type text;
  v_proj_active  boolean;
BEGIN
  v_project_id := NEW."project_table 项目总表_id";

  IF v_project_id IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT "项目类型", is_active
  INTO v_project_type, v_proj_active
  FROM public."04项目总表"
  WHERE id = v_project_id;

  IF v_proj_active IS NOT TRUE THEN
    RETURN NEW;
  END IF;

  IF v_project_type IS NULL OR v_project_type = '' THEN
    RETURN NEW;
  END IF;

  INSERT INTO public."09项目阶段实例表" (
    project_id,
    scope_row_id,
    site_project_id,
    stage_def_id,
    is_active
  )
  SELECT
    v_project_id        AS project_id,
    NEW.id              AS scope_row_id,
    NEW.id              AS site_project_id,
    d.id                AS stage_def_id,
    TRUE                AS is_active
  FROM public."08项目阶段定义表" d
  WHERE d.project_type = v_project_type
    AND d.stage_scope = 'site'
    AND d.is_active = TRUE
  ON CONFLICT (project_id, stage_def_id, scope_row_id)
  DO UPDATE SET is_active = TRUE;

  RETURN NEW;
END;
$$;


--
-- Name: filter_stage_json_map(jsonb, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.filter_stage_json_map(raw_json jsonb, wanted_stage_key text) RETURNS jsonb
    LANGUAGE sql
    AS $$
  SELECT COALESCE(
    jsonb_object_agg(e.key, e.value),
    '{}'::jsonb
  )
  FROM jsonb_each(COALESCE(raw_json, '{}'::jsonb)) AS e(key, value)
  WHERE COALESCE(e.value ->> 'stage_key', '') = wanted_stage_key
     OR e.key LIKE wanted_stage_key || '::%';
$$;


--
-- Name: safe_parse_jsonb(json); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.safe_parse_jsonb(raw_json json) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF raw_json IS NULL THEN
    RETURN '{}'::jsonb;
  END IF;
  RETURN raw_json::jsonb;
EXCEPTION
  WHEN others THEN
    RETURN '{}'::jsonb;
END;
$$;


--
-- Name: safe_parse_jsonb(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.safe_parse_jsonb(raw_json jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF raw_json IS NULL THEN
    RETURN '{}'::jsonb;
  END IF;
  RETURN raw_json;
EXCEPTION
  WHEN others THEN
    RETURN '{}'::jsonb;
END;
$$;


--
-- Name: safe_parse_jsonb(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.safe_parse_jsonb(raw_text text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF raw_text IS NULL OR btrim(raw_text) = '' THEN
    RETURN '{}'::jsonb;
  END IF;
  RETURN raw_text::jsonb;
EXCEPTION
  WHEN others THEN
    RETURN '{}'::jsonb;
END;
$$;


--
-- Name: to_date_time_safe(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.to_date_time_safe(value text, format text) RETURNS timestamp without time zone
    LANGUAGE plpgsql
    AS $$
  BEGIN
    RETURN to_timestamp(value, format);
    EXCEPTION
      WHEN others THEN RETURN NULL;  
  END;
  $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: Features; Type: TABLE; Schema: pgzgyje645ljgth; Owner: -
--

CREATE TABLE pgzgyje645ljgth."Features" (
    id integer NOT NULL,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    created_by character varying,
    updated_by character varying,
    nc_order numeric,
    title text
);


--
-- Name: Features_id_seq; Type: SEQUENCE; Schema: pgzgyje645ljgth; Owner: -
--

CREATE SEQUENCE pgzgyje645ljgth."Features_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: Features_id_seq; Type: SEQUENCE OWNED BY; Schema: pgzgyje645ljgth; Owner: -
--

ALTER SEQUENCE pgzgyje645ljgth."Features_id_seq" OWNED BY pgzgyje645ljgth."Features".id;


--
-- Name: 01医院信息表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."01医院信息表" (
    id integer NOT NULL,
    "医院名称" text,
    "医院等级" text,
    "优势科室" text,
    "年门急诊量" text,
    "年住院量" text,
    "样本资源说明" text,
    "LIS获取难度" text,
    "LIS资源说明" text,
    "样本资源获取难度" text,
    "已获取样本信息" text,
    "GCP知情同意" text,
    "科研知情同意" text,
    "科室联系人评价" text,
    "机构联系人评价" text,
    "机构备注" text,
    "地址经度" numeric,
    "地址纬度" numeric,
    "临床合作基地" text,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    updated_by character varying,
    created_by character varying,
    "科室备注" text,
    "HIS与病例资源获取难度" text,
    "HIS与病例资源说明" text
);


--
-- Name: 02仪器资源表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."02仪器资源表" (
    id integer NOT NULL,
    "仪器型号" text,
    "厂家" text,
    "方法学" text,
    "备注" text,
    created_at timestamp without time zone,
    created_by character varying,
    updated_at timestamp without time zone,
    updated_by character varying,
    "01医院信息表_id" integer
);


--
-- Name: 03医院_项目表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."03医院_项目表" (
    id integer NOT NULL,
    "project_table 项目总表_id" integer,
    "01_hos_resource_table医院信息表_id" integer,
    created_at1 timestamp without time zone,
    updated_at1 timestamp without time zone,
    updated_by1 character varying,
    created_by1 character varying
);


--
-- Name: 04项目总表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."04项目总表" (
    id integer NOT NULL,
    "项目名称" text,
    "项目类型" text,
    "05人员表_id" integer,
    "重要紧急程度" text,
    is_active boolean DEFAULT true
);


--
-- Name: 05人员表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."05人员表" (
    id integer NOT NULL,
    "姓名" text,
    "岗位" text,
    "01医院信息表_id" integer,
    "组别" text,
    "工号" text,
    "哈希密码" text,
    "人员状态" text DEFAULT '在职'::text,
    "数据库权限等级" text
);


--
-- Name: 05_team_members_table_人员表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."05_team_members_table_人员表_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: 05_team_members_table_人员表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."05_team_members_table_人员表_id_seq" OWNED BY public."05人员表".id;


--
-- Name: 06样本资源表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."06样本资源表" (
    id integer NOT NULL,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    created_by character varying,
    updated_by character varying,
    nc_order numeric,
    "样本类型分类" text,
    "样本疾病分类" text,
    "01医院信息表_id" integer
);


--
-- Name: 06样本资源表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."06样本资源表_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: 06样本资源表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."06样本资源表_id_seq" OWNED BY public."06样本资源表".id;


--
-- Name: 07操作审计表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."07操作审计表" (
    id integer NOT NULL,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    created_by character varying,
    updated_by character varying,
    nc_order numeric,
    "业务描述" text,
    "操作时间" timestamp without time zone,
    "操作人工号" text,
    "操作人姓名" text,
    "操作类型" text,
    "目标表" text,
    "目标行id" bigint,
    "变更摘要" text,
    "旧值" json,
    "新值" json,
    "备注" text
);


--
-- Name: 07操作审计表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."07操作审计表_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: 07操作审计表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."07操作审计表_id_seq" OWNED BY public."07操作审计表".id;


--
-- Name: 08项目阶段定义表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."08项目阶段定义表" (
    id bigint NOT NULL,
    project_type text NOT NULL,
    stage_key text NOT NULL,
    stage_name text NOT NULL,
    stage_scope text NOT NULL,
    stage_order integer NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    supports_sample boolean DEFAULT false NOT NULL,
    stage_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT "08项目阶段定义表_stage_scope_check" CHECK ((stage_scope = ANY (ARRAY['sync'::text, 'site'::text])))
);


--
-- Name: 08项目阶段定义表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."08项目阶段定义表_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: 08项目阶段定义表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."08项目阶段定义表_id_seq" OWNED BY public."08项目阶段定义表".id;


--
-- Name: 09项目阶段实例表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."09项目阶段实例表" (
    id bigint NOT NULL,
    project_id integer NOT NULL,
    scope_row_id integer DEFAULT 0 NOT NULL,
    site_project_id integer,
    stage_def_id bigint NOT NULL,
    start_date date,
    planned_end_date date,
    actual_end_date date,
    progress integer DEFAULT 0 NOT NULL,
    remark_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    contributors_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    milestones_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    sample_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    extra_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    row_version integer DEFAULT 1 NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by_work_id text,
    updated_by_name text,
    is_active boolean,
    CONSTRAINT "09项目阶段实例表_progress_check" CHECK (((progress >= 0) AND (progress <= 100)))
);


--
-- Name: 09项目阶段实例表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."09项目阶段实例表_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: 09项目阶段实例表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."09项目阶段实例表_id_seq" OWNED BY public."09项目阶段实例表".id;


--
-- Name: _nc_m2m_04项目总表_05人员表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_nc_m2m_04项目总表_05人员表" (
    "05人员表_id" integer NOT NULL,
    "04项目总表_id" integer NOT NULL
);


--
-- Name: _nc_m2m_05人员表_01医院信息表; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public."_nc_m2m_05人员表_01医院信息表" (
    "01医院信息表_id" integer NOT NULL,
    "05人员表_id" integer NOT NULL
);


--
-- Name: hos_equipment_医院仪器资源表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."hos_equipment_医院仪器资源表_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: hos_equipment_医院仪器资源表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."hos_equipment_医院仪器资源表_id_seq" OWNED BY public."02仪器资源表".id;


--
-- Name: hos_resource_table医院资源表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."hos_resource_table医院资源表_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: hos_resource_table医院资源表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."hos_resource_table医院资源表_id_seq" OWNED BY public."01医院信息表".id;


--
-- Name: nc_api_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_api_tokens (
    id integer NOT NULL,
    base_id character varying(20),
    db_alias character varying(255),
    description character varying(255),
    permissions text,
    token text,
    expiry character varying(255),
    enabled boolean DEFAULT true,
    fk_user_id character varying(20),
    fk_workspace_id character varying(20),
    fk_sso_client_id character varying(20),
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);


--
-- Name: nc_api_tokens_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.nc_api_tokens_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nc_api_tokens_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.nc_api_tokens_id_seq OWNED BY public.nc_api_tokens.id;


--
-- Name: nc_audit_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_audit_v2 (
    id uuid NOT NULL,
    "user" character varying(255),
    ip character varying(255),
    source_id character varying(20),
    base_id character varying(20),
    fk_model_id character varying(20),
    row_id character varying(255),
    op_type character varying(255),
    op_sub_type character varying(255),
    status character varying(255),
    description text,
    details text,
    fk_user_id character varying(20),
    fk_ref_id character varying(20),
    fk_parent_id uuid,
    fk_workspace_id character varying(20),
    fk_org_id character varying(20),
    user_agent text,
    version smallint DEFAULT '0'::smallint,
    old_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_automation_executions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_automation_executions (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_workflow_id character varying(20) NOT NULL,
    workflow_data text,
    execution_data text,
    finished boolean DEFAULT false,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    status character varying(50),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    resume_at timestamp with time zone
);


--
-- Name: nc_automations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_automations (
    id character varying(20) NOT NULL,
    title character varying(255),
    description text,
    meta text,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    "order" real,
    type character varying(20),
    created_by character varying(20),
    updated_by character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    enabled boolean DEFAULT false,
    nodes text,
    edges text,
    draft text,
    config text,
    script text
);


--
-- Name: nc_base_users_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_base_users_v2 (
    base_id character varying(20) NOT NULL,
    fk_user_id character varying(20) NOT NULL,
    roles text,
    starred boolean,
    pinned boolean,
    "group" character varying(255),
    color character varying(255),
    "order" real,
    hidden real,
    opened_date timestamp with time zone,
    invited_by character varying(20),
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_bases_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_bases_v2 (
    id character varying(128) NOT NULL,
    title character varying(255),
    prefix character varying(255),
    status character varying(255),
    description text,
    meta text,
    color character varying(255),
    uuid character varying(255),
    password character varying(255),
    roles character varying(255),
    deleted boolean DEFAULT false,
    is_meta boolean,
    "order" real,
    type character varying(200),
    fk_workspace_id character varying(20),
    is_snapshot boolean DEFAULT false,
    fk_custom_url_id character varying(20),
    version smallint DEFAULT '2'::smallint,
    default_role character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    sandbox_master boolean DEFAULT false,
    sandbox_id character varying(20),
    sandbox_version_id character varying(20),
    auto_update boolean DEFAULT true
);


--
-- Name: nc_calendar_view_columns_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_calendar_view_columns_v2 (
    id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    source_id character varying(20),
    fk_view_id character varying(20),
    fk_column_id character varying(20),
    show boolean,
    bold boolean,
    underline boolean,
    italic boolean,
    "order" real,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_calendar_view_range_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_calendar_view_range_v2 (
    id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    fk_to_column_id character varying(20),
    label character varying(40),
    fk_from_column_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_calendar_view_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_calendar_view_v2 (
    fk_view_id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    source_id character varying(20),
    title character varying(255),
    fk_cover_image_col_id character varying(20),
    meta text,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);


--
-- Name: nc_col_barcode_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_barcode_v2 (
    id character varying(20) NOT NULL,
    fk_column_id character varying(20),
    fk_barcode_value_column_id character varying(20),
    barcode_format character varying(15),
    deleted boolean,
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_col_button_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_button_v2 (
    id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    type character varying(255),
    label text,
    theme character varying(255),
    color character varying(255),
    icon character varying(255),
    formula text,
    formula_raw text,
    error character varying(255),
    parsed_tree text,
    fk_webhook_id character varying(20),
    fk_column_id character varying(20),
    fk_integration_id character varying(20),
    model character varying(255),
    output_column_ids text,
    fk_workspace_id character varying(20),
    fk_script_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_col_formula_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_formula_v2 (
    id character varying(20) NOT NULL,
    fk_column_id character varying(20),
    formula text NOT NULL,
    formula_raw text,
    error text,
    deleted boolean,
    "order" real,
    parsed_tree text,
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_col_long_text_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_long_text_v2 (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_model_id character varying(20),
    fk_column_id character varying(20),
    fk_integration_id character varying(20),
    model character varying(255),
    prompt text,
    prompt_raw text,
    error text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_col_lookup_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_lookup_v2 (
    id character varying(20) NOT NULL,
    fk_column_id character varying(20),
    fk_relation_column_id character varying(20),
    fk_lookup_column_id character varying(20),
    deleted boolean,
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_col_qrcode_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_qrcode_v2 (
    id character varying(20) NOT NULL,
    fk_column_id character varying(20),
    fk_qr_value_column_id character varying(20),
    deleted boolean,
    "order" real,
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_col_relations_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_relations_v2 (
    id character varying(20) NOT NULL,
    ref_db_alias character varying(255),
    type character varying(255),
    virtual boolean,
    db_type character varying(255),
    fk_column_id character varying(20),
    fk_related_model_id character varying(20),
    fk_child_column_id character varying(20),
    fk_parent_column_id character varying(20),
    fk_mm_model_id character varying(20),
    fk_mm_child_column_id character varying(20),
    fk_mm_parent_column_id character varying(20),
    ur character varying(255),
    dr character varying(255),
    fk_index_name character varying(255),
    deleted boolean,
    fk_target_view_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    fk_related_base_id character varying(20),
    fk_mm_base_id character varying(20),
    fk_related_source_id character varying(20),
    fk_mm_source_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_col_rollup_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_rollup_v2 (
    id character varying(20) NOT NULL,
    fk_column_id character varying(20),
    fk_relation_column_id character varying(20),
    fk_rollup_column_id character varying(20),
    rollup_function character varying(255),
    deleted boolean,
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_col_select_options_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_col_select_options_v2 (
    id character varying(20) NOT NULL,
    fk_column_id character varying(20),
    title character varying(255),
    color character varying(255),
    "order" real,
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_columns_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_columns_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_model_id character varying(20),
    title character varying(255),
    column_name character varying(255),
    uidt character varying(255),
    dt character varying(255),
    np character varying(255),
    ns character varying(255),
    clen character varying(255),
    cop character varying(255),
    pk boolean,
    pv boolean,
    rqd boolean,
    un boolean,
    ct text,
    ai boolean,
    "unique" boolean,
    cdf text,
    cc text,
    csn character varying(255),
    dtx character varying(255),
    dtxp text,
    dtxs character varying(255),
    au boolean,
    validate text,
    virtual boolean,
    deleted boolean,
    system boolean DEFAULT false,
    "order" real,
    meta text,
    description text,
    readonly boolean DEFAULT false,
    fk_workspace_id character varying(20),
    custom_index_name character varying(64),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    internal_meta text
);


--
-- Name: nc_comment_reactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_comment_reactions (
    id character varying(20) NOT NULL,
    row_id character varying(255),
    comment_id character varying(20),
    source_id character varying(20),
    fk_model_id character varying(20),
    base_id character varying(20) NOT NULL,
    reaction character varying(255),
    created_by character varying(255),
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_comments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_comments (
    id character varying(20) NOT NULL,
    row_id character varying(255),
    comment text,
    created_by character varying(20),
    created_by_email character varying(255),
    resolved_by character varying(20),
    resolved_by_email character varying(255),
    parent_comment_id character varying(20),
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_model_id character varying(20),
    is_deleted boolean,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_custom_urls_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_custom_urls_v2 (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_model_id character varying(20),
    view_id character varying(20),
    original_path character varying(255),
    custom_path character varying(255),
    fk_dashboard_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_dashboards_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_dashboards_v2 (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    title character varying(255) NOT NULL,
    description text,
    meta text,
    "order" integer,
    created_by character varying(20),
    owned_by character varying(20),
    uuid character varying(255),
    password character varying(255),
    fk_custom_url_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_data_reflection; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_data_reflection (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    username character varying(255),
    password character varying(255),
    database character varying(255),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_db_servers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_db_servers (
    id character varying(20) NOT NULL,
    title character varying(255),
    is_shared boolean DEFAULT true,
    max_tenant_count integer,
    current_tenant_count integer DEFAULT 0,
    config text,
    conditions text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_dependency_tracker; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_dependency_tracker (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    source_type character varying(50) NOT NULL,
    source_id character varying(20) NOT NULL,
    dependent_type character varying(50) NOT NULL,
    dependent_id character varying(20) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    queryable_field_0 text,
    queryable_field_1 text,
    meta text,
    queryable_field_2 timestamp with time zone
);


--
-- Name: nc_disabled_models_for_role_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_disabled_models_for_role_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    role character varying(45),
    disabled boolean DEFAULT true,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_extensions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_extensions (
    id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    fk_user_id character varying(20),
    extension_id character varying(255),
    title character varying(255),
    kv_store text,
    meta text,
    "order" real,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_file_references; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_file_references (
    id character varying(20) NOT NULL,
    storage character varying(255),
    file_url text,
    file_size integer,
    fk_user_id character varying(20),
    fk_workspace_id character varying(20),
    base_id character varying(20),
    source_id character varying(20),
    fk_model_id character varying(20),
    fk_column_id character varying(20),
    is_external boolean DEFAULT false,
    deleted boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_filter_exp_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_filter_exp_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    fk_hook_id character varying(20),
    fk_column_id character varying(20),
    fk_parent_id character varying(20),
    logical_op character varying(255),
    comparison_op character varying(255),
    value text,
    is_group boolean,
    "order" real,
    comparison_sub_op character varying(255),
    fk_link_col_id character varying(20),
    fk_value_col_id character varying(20),
    fk_parent_column_id character varying(20),
    fk_workspace_id character varying(20),
    fk_row_color_condition_id character varying(20),
    fk_widget_id character varying(20),
    meta text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_follower; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_follower (
    fk_user_id character varying(20) NOT NULL,
    fk_follower_id character varying(20) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_form_view_columns_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_form_view_columns_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    fk_column_id character varying(20),
    uuid character varying(255),
    label text,
    help text,
    description text,
    required boolean,
    show boolean,
    "order" real,
    meta text,
    enable_scanner boolean,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_form_view_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_form_view_v2 (
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_view_id character varying(20) NOT NULL,
    heading character varying(255),
    subheading text,
    success_msg text,
    redirect_url text,
    redirect_after_secs character varying(255),
    email character varying(255),
    submit_another_form boolean,
    show_blank_form boolean,
    uuid character varying(255),
    banner_image_url text,
    logo_url text,
    meta text,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_gallery_view_columns_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_gallery_view_columns_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    fk_column_id character varying(20),
    uuid character varying(255),
    label character varying(255),
    help character varying(255),
    show boolean,
    "order" real,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_gallery_view_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_gallery_view_v2 (
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_view_id character varying(20) NOT NULL,
    next_enabled boolean,
    prev_enabled boolean,
    cover_image_idx integer,
    fk_cover_image_col_id character varying(20),
    cover_image character varying(255),
    restrict_types character varying(255),
    restrict_size character varying(255),
    restrict_number character varying(255),
    public boolean,
    dimensions character varying(255),
    responsive_columns character varying(255),
    meta text,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_grid_view_columns_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_grid_view_columns_v2 (
    id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    fk_column_id character varying(20),
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    uuid character varying(255),
    label character varying(255),
    help character varying(255),
    width character varying(255) DEFAULT '200px'::character varying,
    show boolean,
    "order" real,
    group_by boolean,
    group_by_order real,
    group_by_sort character varying(255),
    aggregation character varying(30) DEFAULT NULL::character varying,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_grid_view_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_grid_view_v2 (
    fk_view_id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    uuid character varying(255),
    meta text,
    row_height integer,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_hook_logs_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_hook_logs_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_hook_id character varying(20),
    type character varying(255),
    event character varying(255),
    operation character varying(255),
    test_call boolean DEFAULT true,
    payload text,
    conditions text,
    notification text,
    error_code character varying(255),
    error_message character varying(255),
    error text,
    execution_time integer,
    response text,
    triggered_by character varying(255),
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_hook_trigger_fields; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_hook_trigger_fields (
    fk_hook_id character varying(20) NOT NULL,
    fk_column_id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_hooks_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_hooks_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_model_id character varying(20),
    title character varying(255),
    description character varying(255),
    env character varying(255) DEFAULT 'all'::character varying,
    type character varying(255),
    event character varying(255),
    operation character varying(255),
    async boolean DEFAULT false,
    payload boolean DEFAULT true,
    url text,
    headers text,
    condition boolean DEFAULT false,
    notification text,
    retries integer DEFAULT 0,
    retry_interval integer DEFAULT 60000,
    timeout integer DEFAULT 60000,
    active boolean DEFAULT true,
    version character varying(255),
    trigger_field boolean DEFAULT false,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_installations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_installations (
    id character varying(20) NOT NULL,
    fk_subscription_id character varying(20),
    licensed_to character varying(255) NOT NULL,
    license_key character varying(255) NOT NULL,
    installation_secret character varying(255),
    installed_at timestamp with time zone,
    last_seen_at timestamp with time zone,
    expires_at timestamp with time zone,
    license_type character varying(255) NOT NULL,
    status character varying(255) DEFAULT 'active'::character varying NOT NULL,
    seat_count integer DEFAULT 0 NOT NULL,
    config text,
    meta text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_integrations_store_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_integrations_store_v2 (
    id character varying(20) NOT NULL,
    fk_integration_id character varying(20),
    type character varying(20),
    sub_type character varying(20),
    fk_workspace_id character varying(20),
    fk_user_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    slot_0 text,
    slot_1 text,
    slot_2 text,
    slot_3 text,
    slot_4 text,
    slot_5 integer,
    slot_6 integer,
    slot_7 integer,
    slot_8 integer,
    slot_9 integer
);


--
-- Name: nc_integrations_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_integrations_v2 (
    id character varying(20) NOT NULL,
    title character varying(128),
    config text,
    meta text,
    type character varying(20),
    sub_type character varying(20),
    fk_workspace_id character varying(20),
    is_private boolean DEFAULT false,
    deleted boolean DEFAULT false,
    created_by character varying(20),
    "order" real,
    is_default boolean DEFAULT false,
    is_encrypted boolean DEFAULT false,
    is_global boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_jobs (
    id character varying(20) NOT NULL,
    job character varying(255),
    status character varying(20),
    result text,
    fk_user_id character varying(20),
    fk_workspace_id character varying(20),
    base_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_kanban_view_columns_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_kanban_view_columns_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    fk_column_id character varying(20),
    uuid character varying(255),
    label character varying(255),
    help character varying(255),
    show boolean,
    "order" real,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_kanban_view_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_kanban_view_v2 (
    fk_view_id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    show boolean,
    "order" real,
    uuid character varying(255),
    title character varying(255),
    public boolean,
    password character varying(255),
    show_all_fields boolean,
    fk_grp_col_id character varying(20),
    fk_cover_image_col_id character varying(20),
    meta text,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_map_view_columns_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_map_view_columns_v2 (
    id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    project_id character varying(128),
    fk_view_id character varying(20),
    fk_column_id character varying(20),
    uuid character varying(255),
    label character varying(255),
    help character varying(255),
    show boolean,
    "order" real,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_map_view_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_map_view_v2 (
    fk_view_id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    uuid character varying(255),
    title character varying(255),
    fk_geo_data_col_id character varying(20),
    meta text,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);


--
-- Name: nc_mcp_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_mcp_tokens (
    id character varying(20) NOT NULL,
    title character varying(512),
    base_id character varying(20) NOT NULL,
    token character varying(32),
    fk_workspace_id character varying(20),
    "order" real,
    fk_user_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_model_stats_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_model_stats_v2 (
    fk_workspace_id character varying(20) NOT NULL,
    fk_model_id character varying(20) NOT NULL,
    row_count integer DEFAULT 0,
    is_external boolean DEFAULT false,
    base_id character varying(20) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_models_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_models_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    table_name character varying(255),
    title character varying(255),
    type character varying(255) DEFAULT 'table'::character varying,
    meta text,
    schema text,
    enabled boolean DEFAULT true,
    mm boolean DEFAULT false,
    tags character varying(255),
    pinned boolean,
    deleted boolean,
    "order" real,
    description text,
    synced boolean DEFAULT false,
    fk_workspace_id character varying(20),
    created_by character varying(20),
    owned_by character varying(20),
    uuid character varying(255),
    password character varying(255),
    fk_custom_url_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_oauth_authorization_codes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_oauth_authorization_codes (
    code character varying(32) NOT NULL,
    fk_client_id character varying(32),
    fk_user_id character varying(20),
    code_challenge character varying(255),
    code_challenge_method character varying(10) DEFAULT 'S256'::character varying,
    redirect_uri character varying(255),
    scope character varying(255),
    state character varying(1024),
    resource character varying(255),
    granted_resources text,
    expires_at timestamp with time zone NOT NULL,
    is_used boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_oauth_clients; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_oauth_clients (
    client_id character varying(32) NOT NULL,
    client_secret character varying(128),
    client_type character varying(255),
    client_name character varying(255),
    client_description text,
    client_uri character varying(255),
    logo_uri character varying(255),
    redirect_uris text,
    allowed_grant_types text,
    response_types text,
    allowed_scopes text,
    registration_access_token character varying(255),
    registration_client_uri character varying(255),
    client_id_issued_at bigint,
    client_secret_expires_at bigint,
    fk_user_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_oauth_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_oauth_tokens (
    id character varying(20) NOT NULL,
    fk_client_id character varying(32),
    fk_user_id character varying(20),
    access_token text,
    access_token_expires_at timestamp with time zone,
    refresh_token text,
    refresh_token_expires_at timestamp with time zone,
    resource character varying(255),
    audience character varying(255),
    granted_resources text,
    scope character varying(255),
    is_revoked boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_used_at timestamp with time zone
);


--
-- Name: nc_org; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_org (
    id character varying(20) NOT NULL,
    title character varying(255),
    slug character varying(255),
    fk_user_id character varying(20),
    meta text,
    image character varying(255),
    is_share_enabled boolean DEFAULT false,
    deleted boolean DEFAULT false,
    "order" real,
    fk_db_instance_id character varying(20),
    stripe_customer_id character varying(255),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_org_domain; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_org_domain (
    id character varying(20) NOT NULL,
    fk_org_id character varying(20),
    fk_user_id character varying(20),
    domain character varying(255),
    verified boolean,
    txt_value character varying(255),
    last_verified timestamp with time zone,
    deleted boolean DEFAULT false,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_org_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_org_users (
    fk_org_id character varying(20) NOT NULL,
    fk_user_id character varying(20),
    roles character varying(255),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_permission_subjects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_permission_subjects (
    fk_permission_id character varying(20) NOT NULL,
    subject_type character varying(255) NOT NULL,
    subject_id character varying(255) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_permissions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_permissions (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    entity character varying(255),
    entity_id character varying(255),
    permission character varying(255),
    created_by character varying(20),
    enforce_for_form boolean DEFAULT true,
    enforce_for_automation boolean DEFAULT true,
    granted_type character varying(255),
    granted_role character varying(255),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_plans; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_plans (
    id character varying(20) NOT NULL,
    title character varying(255),
    description text,
    stripe_product_id character varying(255) NOT NULL,
    is_active boolean DEFAULT true,
    prices text,
    meta text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_plugins_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_plugins_v2 (
    id character varying(20) NOT NULL,
    title character varying(45),
    description text,
    active boolean DEFAULT false,
    rating real,
    version character varying(255),
    docs character varying(255),
    status character varying(255) DEFAULT 'install'::character varying,
    status_details character varying(255),
    logo character varying(255),
    icon character varying(255),
    tags character varying(255),
    category character varying(255),
    input_schema text,
    input text,
    creator character varying(255),
    creator_website character varying(255),
    price character varying(255),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_principal_assignments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_principal_assignments (
    resource_type character varying(20) NOT NULL,
    resource_id character varying(20) NOT NULL,
    principal_type character varying(20) NOT NULL,
    principal_ref_id character varying(20) NOT NULL,
    roles character varying(255) NOT NULL,
    deleted boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_row_color_conditions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_row_color_conditions (
    id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    color character varying(20),
    nc_order real,
    is_set_as_background boolean,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sandbox_deployment_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sandbox_deployment_logs (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    fk_sandbox_id character varying(20) NOT NULL,
    from_version_id character varying(20),
    to_version_id character varying(20) NOT NULL,
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    deployment_type character varying(20) NOT NULL,
    error_message text,
    deployment_log text,
    meta text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone
);


--
-- Name: nc_sandbox_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sandbox_versions (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20) NOT NULL,
    fk_sandbox_id character varying(20) NOT NULL,
    version character varying(20) NOT NULL,
    version_number integer NOT NULL,
    status character varying(20) DEFAULT 'draft'::character varying NOT NULL,
    schema text,
    release_notes text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    published_at timestamp with time zone
);


--
-- Name: nc_sandboxes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sandboxes (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    title character varying(255) NOT NULL,
    description text,
    created_by character varying(20) NOT NULL,
    visibility character varying(20) DEFAULT 'private'::character varying NOT NULL,
    category character varying(255),
    install_count integer DEFAULT 0,
    meta text,
    deleted boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    published_at timestamp with time zone
);


--
-- Name: nc_scripts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_scripts (
    id character varying(20) NOT NULL,
    title text,
    description text,
    meta text,
    "order" real,
    base_id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    script text,
    config text,
    created_by character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_snapshots (
    id character varying(20) NOT NULL,
    title character varying(512),
    base_id character varying(20),
    snapshot_base_id character varying(20),
    fk_workspace_id character varying(20),
    created_by character varying(20),
    status character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sort_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sort_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_view_id character varying(20),
    fk_column_id character varying(20),
    direction character varying(255) DEFAULT 'false'::character varying,
    "order" real,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sources_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sources_v2 (
    id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    alias character varying(255),
    config text,
    meta text,
    is_meta boolean,
    type character varying(255),
    inflection_column character varying(255),
    inflection_table character varying(255),
    enabled boolean DEFAULT true,
    "order" real,
    description character varying(255),
    erd_uuid character varying(255),
    deleted boolean DEFAULT false,
    is_schema_readonly boolean DEFAULT false,
    is_data_readonly boolean DEFAULT false,
    is_local boolean DEFAULT false,
    fk_sql_executor_id character varying(20),
    fk_workspace_id character varying(20),
    fk_integration_id character varying(20),
    is_encrypted boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sql_executor_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sql_executor_v2 (
    id character varying(20) NOT NULL,
    domain character varying(50),
    status character varying(20),
    priority integer,
    capacity integer,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sso_client; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sso_client (
    id character varying(20) NOT NULL,
    type character varying(20),
    title character varying(255),
    enabled boolean DEFAULT true,
    config text,
    fk_user_id character varying(20),
    fk_org_id character varying(20),
    deleted boolean DEFAULT false,
    "order" real,
    domain_name character varying(255),
    domain_name_verified boolean,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sso_client_domain; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sso_client_domain (
    fk_sso_client_id character varying(20) NOT NULL,
    fk_org_domain_id character varying(20),
    enabled boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_store; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_store (
    id integer NOT NULL,
    base_id character varying(255),
    db_alias character varying(255) DEFAULT 'db'::character varying,
    key character varying(255),
    value text,
    type character varying(255),
    env character varying(255),
    tag character varying(255),
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);


--
-- Name: nc_store_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.nc_store_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: nc_store_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.nc_store_id_seq OWNED BY public.nc_store.id;


--
-- Name: nc_subscriptions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_subscriptions (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    fk_org_id character varying(20),
    fk_plan_id character varying(20) NOT NULL,
    fk_user_id character varying(20),
    stripe_subscription_id character varying(255),
    stripe_price_id character varying(255),
    seat_count integer DEFAULT 1 NOT NULL,
    status character varying(255),
    billing_cycle_anchor timestamp with time zone,
    start_at timestamp with time zone,
    trial_end_at timestamp with time zone,
    canceled_at timestamp with time zone,
    period character varying(255),
    upcoming_invoice_at timestamp with time zone,
    upcoming_invoice_due_at timestamp with time zone,
    upcoming_invoice_amount integer,
    upcoming_invoice_currency character varying(255),
    stripe_schedule_id character varying(255),
    schedule_phase_start timestamp with time zone,
    schedule_stripe_price_id character varying(255),
    schedule_fk_plan_id character varying(20),
    schedule_period character varying(255),
    schedule_type character varying(255),
    meta text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sync_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sync_configs (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_integration_id character varying(20),
    fk_model_id character varying(20),
    sync_type character varying(255),
    sync_trigger character varying(255),
    sync_trigger_cron character varying(255),
    sync_trigger_secret character varying(255),
    sync_job_id character varying(255),
    last_sync_at timestamp with time zone,
    next_sync_at timestamp with time zone,
    title character varying(255),
    sync_category character varying(255),
    fk_parent_sync_config_id character varying(20),
    on_delete_action character varying(255) DEFAULT 'mark_deleted'::character varying,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    created_by character varying(20),
    updated_by character varying(20),
    meta text
);


--
-- Name: nc_sync_logs_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sync_logs_v2 (
    id character varying(20) NOT NULL,
    base_id character varying(20) NOT NULL,
    fk_sync_source_id character varying(20),
    time_taken integer,
    status character varying(255),
    status_details text,
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sync_mappings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sync_mappings (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_sync_config_id character varying(20),
    target_table character varying(255),
    fk_model_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_sync_source_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_sync_source_v2 (
    id character varying(20) NOT NULL,
    title character varying(255),
    type character varying(255),
    details text,
    deleted boolean,
    enabled boolean DEFAULT true,
    "order" real,
    base_id character varying(20) NOT NULL,
    fk_user_id character varying(20),
    source_id character varying(20),
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_teams; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_teams (
    id character varying(20) NOT NULL,
    title character varying(255) NOT NULL,
    meta text,
    fk_org_id character varying(20),
    fk_workspace_id character varying(20),
    created_by character varying(20),
    deleted boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_usage_stats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_usage_stats (
    fk_workspace_id character varying(20) NOT NULL,
    usage_type character varying(255) NOT NULL,
    period_start timestamp with time zone NOT NULL,
    count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_user_comment_notifications_preference; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_user_comment_notifications_preference (
    id character varying(20) NOT NULL,
    row_id character varying(255),
    user_id character varying(20),
    fk_model_id character varying(20),
    source_id character varying(20),
    base_id character varying(20),
    preferences character varying(255),
    fk_workspace_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_user_refresh_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_user_refresh_tokens (
    fk_user_id character varying(20),
    token character varying(255),
    meta text,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_users_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_users_v2 (
    id character varying(20) NOT NULL,
    email character varying(255),
    password character varying(255),
    salt character varying(255),
    invite_token character varying(255),
    invite_token_expires character varying(255),
    reset_password_expires timestamp with time zone,
    reset_password_token character varying(255),
    email_verification_token character varying(255),
    email_verified boolean,
    roles character varying(255) DEFAULT 'editor'::character varying,
    token_version character varying(255),
    blocked boolean DEFAULT false,
    blocked_reason character varying(255),
    deleted_at timestamp with time zone,
    is_deleted boolean DEFAULT false,
    meta text,
    display_name character varying(255),
    user_name character varying(255),
    bio character varying(255),
    location character varying(255),
    website character varying(255),
    avatar character varying(255),
    is_new_user boolean,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_views_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_views_v2 (
    id character varying(20) NOT NULL,
    source_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_model_id character varying(20),
    title character varying(255),
    type integer,
    is_default boolean,
    show_system_fields boolean,
    lock_type character varying(255) DEFAULT 'collaborative'::character varying,
    uuid character varying(255),
    password character varying(255),
    show boolean,
    "order" real,
    meta text,
    description text,
    created_by character varying(20),
    owned_by character varying(20),
    fk_workspace_id character varying(20),
    attachment_mode_column_id character varying(20),
    expanded_record_mode character varying(255),
    fk_custom_url_id character varying(20),
    row_coloring_mode character varying(10),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_widgets_v2; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_widgets_v2 (
    id character varying(20) NOT NULL,
    fk_workspace_id character varying(20),
    base_id character varying(20) NOT NULL,
    fk_dashboard_id character varying(20) NOT NULL,
    fk_model_id character varying(20),
    fk_view_id character varying(20),
    title character varying(255) NOT NULL,
    description text,
    type character varying(50) NOT NULL,
    config text,
    meta text,
    "order" integer,
    "position" text,
    error boolean,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: nc_workflows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nc_workflows (
    id character varying(20) NOT NULL,
    title character varying(255) NOT NULL,
    description text,
    fk_workspace_id character varying(20),
    base_id character varying(20),
    enabled boolean DEFAULT false,
    nodes text,
    edges text,
    meta text,
    "order" real,
    created_by character varying(20),
    updated_by character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    draft text
);


--
-- Name: notification; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.notification (
    id character varying(20) NOT NULL,
    type character varying(40),
    body text,
    is_read boolean DEFAULT false,
    is_deleted boolean DEFAULT false,
    fk_user_id character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: project_table 合作项目分表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."project_table 合作项目分表_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: project_table 合作项目分表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."project_table 合作项目分表_id_seq" OWNED BY public."03医院_项目表".id;


--
-- Name: v_项目阶段甘特视图; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public."v_项目阶段甘特视图" AS
 SELECT si.id AS stage_instance_id,
    si.project_id AS project_db_id,
    COALESCE(NULLIF(p."项目名称", ''::text), ('项目-'::text || (p.id)::text)) AS project_id,
    p."项目类型" AS project_type,
    p."重要紧急程度",
    COALESCE(p.is_active, true) AS project_is_active,
    si.project_id AS proj_row_id,
    si.site_project_id AS site_row_id,
        CASE
            WHEN (d.stage_scope = 'sync'::text) THEN '所有中心（同步）'::text
            ELSE COALESCE(NULLIF(h."医院名称", ''::text), ('中心-'::text || (COALESCE(s.id, 0))::text))
        END AS site_name,
    d.stage_key AS task_name,
    d.stage_name AS task_display_name,
    d.stage_order AS stage_ord,
    d.stage_scope,
    'Process'::text AS task_type,
    si.start_date,
    si.planned_end_date,
    si.actual_end_date,
    ((COALESCE(si.progress, 0))::numeric / 100.0) AS progress,
    ((si.start_date IS NULL) OR (si.planned_end_date IS NULL)) AS is_unplanned,
    (si.remark_json)::text AS remark,
    si.remark_json,
    si.contributors_json,
    si.milestones_json,
    si.sample_json,
    si.row_version,
    mgr."姓名" AS manager_name
   FROM (((((public."09项目阶段实例表" si
     JOIN public."08项目阶段定义表" d ON ((d.id = si.stage_def_id)))
     JOIN public."04项目总表" p ON ((p.id = si.project_id)))
     LEFT JOIN public."03医院_项目表" s ON ((s.id = si.site_project_id)))
     LEFT JOIN public."01医院信息表" h ON ((h.id = s."01_hos_resource_table医院信息表_id")))
     LEFT JOIN public."05人员表" mgr ON ((mgr.id = p."05人员表_id")))
  WHERE (COALESCE(si.is_active, true) = true);


--
-- Name: workspace; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workspace (
    id character varying(20) NOT NULL,
    title character varying(255),
    description text,
    meta text,
    fk_user_id character varying(20),
    deleted boolean DEFAULT false,
    deleted_at timestamp with time zone,
    "order" real,
    status smallint DEFAULT '0'::smallint,
    message character varying(256),
    plan character varying(20) DEFAULT 'free'::character varying,
    infra_meta text,
    fk_org_id character varying(20),
    stripe_customer_id character varying(255),
    grace_period_start_at timestamp with time zone,
    api_grace_period_start_at timestamp with time zone,
    automation_grace_period_start_at timestamp with time zone,
    loyal boolean DEFAULT false,
    loyalty_discount_used boolean DEFAULT false,
    db_job_id character varying(20),
    fk_db_instance_id character varying(20),
    segment_code integer,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: workspace_user; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.workspace_user (
    fk_workspace_id character varying(20) NOT NULL,
    fk_user_id character varying(20) NOT NULL,
    roles character varying(255),
    invite_token character varying(255),
    invite_accepted boolean DEFAULT false,
    deleted boolean DEFAULT false,
    deleted_at timestamp with time zone,
    "order" real,
    invited_by character varying(20),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: xc_knex_migrationsv0; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.xc_knex_migrationsv0 (
    id integer NOT NULL,
    name character varying(255),
    batch integer,
    migration_time timestamp with time zone
);


--
-- Name: xc_knex_migrationsv0_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.xc_knex_migrationsv0_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: xc_knex_migrationsv0_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.xc_knex_migrationsv0_id_seq OWNED BY public.xc_knex_migrationsv0.id;


--
-- Name: xc_knex_migrationsv0_lock; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.xc_knex_migrationsv0_lock (
    index integer NOT NULL,
    is_locked integer
);


--
-- Name: xc_knex_migrationsv0_lock_index_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.xc_knex_migrationsv0_lock_index_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: xc_knex_migrationsv0_lock_index_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.xc_knex_migrationsv0_lock_index_seq OWNED BY public.xc_knex_migrationsv0_lock.index;


--
-- Name: 项目总表_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public."项目总表_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: 项目总表_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public."项目总表_id_seq" OWNED BY public."04项目总表".id;


--
-- Name: Features id; Type: DEFAULT; Schema: pgzgyje645ljgth; Owner: -
--

ALTER TABLE ONLY pgzgyje645ljgth."Features" ALTER COLUMN id SET DEFAULT nextval('pgzgyje645ljgth."Features_id_seq"'::regclass);


--
-- Name: 01医院信息表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."01医院信息表" ALTER COLUMN id SET DEFAULT nextval('public."hos_resource_table医院资源表_id_seq"'::regclass);


--
-- Name: 02仪器资源表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."02仪器资源表" ALTER COLUMN id SET DEFAULT nextval('public."hos_equipment_医院仪器资源表_id_seq"'::regclass);


--
-- Name: 03医院_项目表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."03医院_项目表" ALTER COLUMN id SET DEFAULT nextval('public."project_table 合作项目分表_id_seq"'::regclass);


--
-- Name: 04项目总表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."04项目总表" ALTER COLUMN id SET DEFAULT nextval('public."项目总表_id_seq"'::regclass);


--
-- Name: 05人员表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."05人员表" ALTER COLUMN id SET DEFAULT nextval('public."05_team_members_table_人员表_id_seq"'::regclass);


--
-- Name: 06样本资源表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."06样本资源表" ALTER COLUMN id SET DEFAULT nextval('public."06样本资源表_id_seq"'::regclass);


--
-- Name: 07操作审计表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."07操作审计表" ALTER COLUMN id SET DEFAULT nextval('public."07操作审计表_id_seq"'::regclass);


--
-- Name: 08项目阶段定义表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."08项目阶段定义表" ALTER COLUMN id SET DEFAULT nextval('public."08项目阶段定义表_id_seq"'::regclass);


--
-- Name: 09项目阶段实例表 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."09项目阶段实例表" ALTER COLUMN id SET DEFAULT nextval('public."09项目阶段实例表_id_seq"'::regclass);


--
-- Name: nc_api_tokens id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_api_tokens ALTER COLUMN id SET DEFAULT nextval('public.nc_api_tokens_id_seq'::regclass);


--
-- Name: nc_store id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_store ALTER COLUMN id SET DEFAULT nextval('public.nc_store_id_seq'::regclass);


--
-- Name: xc_knex_migrationsv0 id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.xc_knex_migrationsv0 ALTER COLUMN id SET DEFAULT nextval('public.xc_knex_migrationsv0_id_seq'::regclass);


--
-- Name: xc_knex_migrationsv0_lock index; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.xc_knex_migrationsv0_lock ALTER COLUMN index SET DEFAULT nextval('public.xc_knex_migrationsv0_lock_index_seq'::regclass);


--
-- Name: Features Features_pkey; Type: CONSTRAINT; Schema: pgzgyje645ljgth; Owner: -
--

ALTER TABLE ONLY pgzgyje645ljgth."Features"
    ADD CONSTRAINT "Features_pkey" PRIMARY KEY (id);


--
-- Name: 05人员表 05_team_members_table_人员表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."05人员表"
    ADD CONSTRAINT "05_team_members_table_人员表_pkey" PRIMARY KEY (id);


--
-- Name: 06样本资源表 06样本资源表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."06样本资源表"
    ADD CONSTRAINT "06样本资源表_pkey" PRIMARY KEY (id);


--
-- Name: 07操作审计表 07操作审计表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."07操作审计表"
    ADD CONSTRAINT "07操作审计表_pkey" PRIMARY KEY (id);


--
-- Name: 08项目阶段定义表 08项目阶段定义表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."08项目阶段定义表"
    ADD CONSTRAINT "08项目阶段定义表_pkey" PRIMARY KEY (id);


--
-- Name: 09项目阶段实例表 09项目阶段实例表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."09项目阶段实例表"
    ADD CONSTRAINT "09项目阶段实例表_pkey" PRIMARY KEY (id);


--
-- Name: _nc_m2m_04项目总表_05人员表 _nc_m2m_04项目总表_05人员表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_nc_m2m_04项目总表_05人员表"
    ADD CONSTRAINT "_nc_m2m_04项目总表_05人员表_pkey" PRIMARY KEY ("05人员表_id", "04项目总表_id");


--
-- Name: _nc_m2m_05人员表_01医院信息表 _nc_m2m_05人员表_01医院信息表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_nc_m2m_05人员表_01医院信息表"
    ADD CONSTRAINT "_nc_m2m_05人员表_01医院信息表_pkey" PRIMARY KEY ("01医院信息表_id", "05人员表_id");


--
-- Name: 02仪器资源表 hos_equipment_医院仪器资源表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."02仪器资源表"
    ADD CONSTRAINT "hos_equipment_医院仪器资源表_pkey" PRIMARY KEY (id);


--
-- Name: 01医院信息表 hos_resource_table医院资源表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."01医院信息表"
    ADD CONSTRAINT "hos_resource_table医院资源表_pkey" PRIMARY KEY (id);


--
-- Name: nc_api_tokens nc_api_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_api_tokens
    ADD CONSTRAINT nc_api_tokens_pkey PRIMARY KEY (id);


--
-- Name: nc_audit_v2 nc_audit_v2_pkx; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_audit_v2
    ADD CONSTRAINT nc_audit_v2_pkx PRIMARY KEY (id);


--
-- Name: nc_automation_executions nc_automation_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_automation_executions
    ADD CONSTRAINT nc_automation_executions_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_automations nc_automations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_automations
    ADD CONSTRAINT nc_automations_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_base_users_v2 nc_base_users_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_base_users_v2
    ADD CONSTRAINT nc_base_users_v2_pkey PRIMARY KEY (base_id, fk_user_id);


--
-- Name: nc_sources_v2 nc_bases_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sources_v2
    ADD CONSTRAINT nc_bases_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_calendar_view_columns_v2 nc_calendar_view_columns_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_calendar_view_columns_v2
    ADD CONSTRAINT nc_calendar_view_columns_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_calendar_view_range_v2 nc_calendar_view_range_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_calendar_view_range_v2
    ADD CONSTRAINT nc_calendar_view_range_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_calendar_view_v2 nc_calendar_view_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_calendar_view_v2
    ADD CONSTRAINT nc_calendar_view_v2_pkey PRIMARY KEY (base_id, fk_view_id);


--
-- Name: nc_col_barcode_v2 nc_col_barcode_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_barcode_v2
    ADD CONSTRAINT nc_col_barcode_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_col_button_v2 nc_col_button_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_button_v2
    ADD CONSTRAINT nc_col_button_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_col_formula_v2 nc_col_formula_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_formula_v2
    ADD CONSTRAINT nc_col_formula_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_col_long_text_v2 nc_col_long_text_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_long_text_v2
    ADD CONSTRAINT nc_col_long_text_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_col_lookup_v2 nc_col_lookup_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_lookup_v2
    ADD CONSTRAINT nc_col_lookup_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_col_qrcode_v2 nc_col_qrcode_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_qrcode_v2
    ADD CONSTRAINT nc_col_qrcode_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_col_relations_v2 nc_col_relations_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_relations_v2
    ADD CONSTRAINT nc_col_relations_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_col_rollup_v2 nc_col_rollup_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_rollup_v2
    ADD CONSTRAINT nc_col_rollup_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_col_select_options_v2 nc_col_select_options_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_col_select_options_v2
    ADD CONSTRAINT nc_col_select_options_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_columns_v2 nc_columns_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_columns_v2
    ADD CONSTRAINT nc_columns_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_comment_reactions nc_comment_reactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_comment_reactions
    ADD CONSTRAINT nc_comment_reactions_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_comments nc_comments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_comments
    ADD CONSTRAINT nc_comments_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_custom_urls_v2 nc_custom_urls_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_custom_urls_v2
    ADD CONSTRAINT nc_custom_urls_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_dashboards_v2 nc_dashboards_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_dashboards_v2
    ADD CONSTRAINT nc_dashboards_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_data_reflection nc_data_reflection_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_data_reflection
    ADD CONSTRAINT nc_data_reflection_pkey PRIMARY KEY (id);


--
-- Name: nc_db_servers nc_db_servers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_db_servers
    ADD CONSTRAINT nc_db_servers_pkey PRIMARY KEY (id);


--
-- Name: nc_dependency_tracker nc_dependency_tracker_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_dependency_tracker
    ADD CONSTRAINT nc_dependency_tracker_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_disabled_models_for_role_v2 nc_disabled_models_for_role_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_disabled_models_for_role_v2
    ADD CONSTRAINT nc_disabled_models_for_role_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_extensions nc_extensions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_extensions
    ADD CONSTRAINT nc_extensions_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_file_references nc_file_references_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_file_references
    ADD CONSTRAINT nc_file_references_pkey PRIMARY KEY (id);


--
-- Name: nc_filter_exp_v2 nc_filter_exp_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_filter_exp_v2
    ADD CONSTRAINT nc_filter_exp_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_follower nc_follower_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_follower
    ADD CONSTRAINT nc_follower_pkey PRIMARY KEY (fk_user_id, fk_follower_id);


--
-- Name: nc_form_view_columns_v2 nc_form_view_columns_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_form_view_columns_v2
    ADD CONSTRAINT nc_form_view_columns_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_form_view_v2 nc_form_view_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_form_view_v2
    ADD CONSTRAINT nc_form_view_v2_pkey PRIMARY KEY (base_id, fk_view_id);


--
-- Name: nc_gallery_view_columns_v2 nc_gallery_view_columns_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_gallery_view_columns_v2
    ADD CONSTRAINT nc_gallery_view_columns_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_gallery_view_v2 nc_gallery_view_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_gallery_view_v2
    ADD CONSTRAINT nc_gallery_view_v2_pkey PRIMARY KEY (base_id, fk_view_id);


--
-- Name: nc_grid_view_columns_v2 nc_grid_view_columns_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_grid_view_columns_v2
    ADD CONSTRAINT nc_grid_view_columns_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_grid_view_v2 nc_grid_view_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_grid_view_v2
    ADD CONSTRAINT nc_grid_view_v2_pkey PRIMARY KEY (base_id, fk_view_id);


--
-- Name: nc_hook_logs_v2 nc_hook_logs_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_hook_logs_v2
    ADD CONSTRAINT nc_hook_logs_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_hook_trigger_fields nc_hook_trigger_fields_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_hook_trigger_fields
    ADD CONSTRAINT nc_hook_trigger_fields_pkey PRIMARY KEY (fk_workspace_id, base_id, fk_hook_id, fk_column_id);


--
-- Name: nc_hooks_v2 nc_hooks_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_hooks_v2
    ADD CONSTRAINT nc_hooks_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_installations nc_installations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_installations
    ADD CONSTRAINT nc_installations_pkey PRIMARY KEY (id);


--
-- Name: nc_integrations_store_v2 nc_integrations_store_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_integrations_store_v2
    ADD CONSTRAINT nc_integrations_store_v2_pkey PRIMARY KEY (id);


--
-- Name: nc_integrations_v2 nc_integrations_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_integrations_v2
    ADD CONSTRAINT nc_integrations_v2_pkey PRIMARY KEY (id);


--
-- Name: nc_jobs nc_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_jobs
    ADD CONSTRAINT nc_jobs_pkey PRIMARY KEY (id);


--
-- Name: nc_kanban_view_columns_v2 nc_kanban_view_columns_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_kanban_view_columns_v2
    ADD CONSTRAINT nc_kanban_view_columns_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_kanban_view_v2 nc_kanban_view_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_kanban_view_v2
    ADD CONSTRAINT nc_kanban_view_v2_pkey PRIMARY KEY (base_id, fk_view_id);


--
-- Name: nc_map_view_columns_v2 nc_map_view_columns_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_map_view_columns_v2
    ADD CONSTRAINT nc_map_view_columns_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_map_view_v2 nc_map_view_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_map_view_v2
    ADD CONSTRAINT nc_map_view_v2_pkey PRIMARY KEY (base_id, fk_view_id);


--
-- Name: nc_mcp_tokens nc_mcp_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_mcp_tokens
    ADD CONSTRAINT nc_mcp_tokens_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_model_stats_v2 nc_model_stats_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_model_stats_v2
    ADD CONSTRAINT nc_model_stats_v2_pkey PRIMARY KEY (fk_workspace_id, base_id, fk_model_id);


--
-- Name: nc_models_v2 nc_models_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_models_v2
    ADD CONSTRAINT nc_models_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_oauth_authorization_codes nc_oauth_authorization_codes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_oauth_authorization_codes
    ADD CONSTRAINT nc_oauth_authorization_codes_pkey PRIMARY KEY (code);


--
-- Name: nc_oauth_clients nc_oauth_clients_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_oauth_clients
    ADD CONSTRAINT nc_oauth_clients_pkey PRIMARY KEY (client_id);


--
-- Name: nc_oauth_tokens nc_oauth_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_oauth_tokens
    ADD CONSTRAINT nc_oauth_tokens_pkey PRIMARY KEY (id);


--
-- Name: nc_org_domain nc_org_domain_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_org_domain
    ADD CONSTRAINT nc_org_domain_pkey PRIMARY KEY (id);


--
-- Name: nc_org nc_org_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_org
    ADD CONSTRAINT nc_org_pkey PRIMARY KEY (id);


--
-- Name: nc_org_users nc_org_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_org_users
    ADD CONSTRAINT nc_org_users_pkey PRIMARY KEY (fk_org_id);


--
-- Name: nc_permission_subjects nc_permission_subjects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_permission_subjects
    ADD CONSTRAINT nc_permission_subjects_pkey PRIMARY KEY (base_id, fk_permission_id, subject_type, subject_id);


--
-- Name: nc_permissions nc_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_permissions
    ADD CONSTRAINT nc_permissions_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_plans nc_plans_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_plans
    ADD CONSTRAINT nc_plans_pkey PRIMARY KEY (id);


--
-- Name: nc_plugins_v2 nc_plugins_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_plugins_v2
    ADD CONSTRAINT nc_plugins_v2_pkey PRIMARY KEY (id);


--
-- Name: nc_principal_assignments nc_principal_assignments_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_principal_assignments
    ADD CONSTRAINT nc_principal_assignments_pk PRIMARY KEY (resource_type, resource_id, principal_type, principal_ref_id);


--
-- Name: nc_bases_v2 nc_projects_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_bases_v2
    ADD CONSTRAINT nc_projects_v2_pkey PRIMARY KEY (id);


--
-- Name: nc_row_color_conditions nc_row_color_conditions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_row_color_conditions
    ADD CONSTRAINT nc_row_color_conditions_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_sandbox_deployment_logs nc_sandbox_deployment_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sandbox_deployment_logs
    ADD CONSTRAINT nc_sandbox_deployment_logs_pkey PRIMARY KEY (id);


--
-- Name: nc_sandbox_versions nc_sandbox_versions_number_unique_idx; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sandbox_versions
    ADD CONSTRAINT nc_sandbox_versions_number_unique_idx UNIQUE (fk_sandbox_id, version_number);


--
-- Name: nc_sandbox_versions nc_sandbox_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sandbox_versions
    ADD CONSTRAINT nc_sandbox_versions_pkey PRIMARY KEY (id);


--
-- Name: nc_sandbox_versions nc_sandbox_versions_unique_idx; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sandbox_versions
    ADD CONSTRAINT nc_sandbox_versions_unique_idx UNIQUE (fk_sandbox_id, version);


--
-- Name: nc_sandboxes nc_sandboxes_base_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sandboxes
    ADD CONSTRAINT nc_sandboxes_base_id_unique UNIQUE (base_id);


--
-- Name: nc_sandboxes nc_sandboxes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sandboxes
    ADD CONSTRAINT nc_sandboxes_pkey PRIMARY KEY (id);


--
-- Name: nc_scripts nc_scripts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_scripts
    ADD CONSTRAINT nc_scripts_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_snapshots nc_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_snapshots
    ADD CONSTRAINT nc_snapshots_pkey PRIMARY KEY (id);


--
-- Name: nc_sort_v2 nc_sort_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sort_v2
    ADD CONSTRAINT nc_sort_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_sql_executor_v2 nc_sql_executor_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sql_executor_v2
    ADD CONSTRAINT nc_sql_executor_v2_pkey PRIMARY KEY (id);


--
-- Name: nc_sso_client_domain nc_sso_client_domain_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sso_client_domain
    ADD CONSTRAINT nc_sso_client_domain_pkey PRIMARY KEY (fk_sso_client_id);


--
-- Name: nc_sso_client nc_sso_client_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sso_client
    ADD CONSTRAINT nc_sso_client_pkey PRIMARY KEY (id);


--
-- Name: nc_store nc_store_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_store
    ADD CONSTRAINT nc_store_pkey PRIMARY KEY (id);


--
-- Name: nc_subscriptions nc_subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_subscriptions
    ADD CONSTRAINT nc_subscriptions_pkey PRIMARY KEY (id);


--
-- Name: nc_sync_configs nc_sync_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sync_configs
    ADD CONSTRAINT nc_sync_configs_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_sync_logs_v2 nc_sync_logs_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sync_logs_v2
    ADD CONSTRAINT nc_sync_logs_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_sync_mappings nc_sync_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sync_mappings
    ADD CONSTRAINT nc_sync_mappings_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_sync_source_v2 nc_sync_source_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_sync_source_v2
    ADD CONSTRAINT nc_sync_source_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_teams nc_teams_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_teams
    ADD CONSTRAINT nc_teams_pkey PRIMARY KEY (id);


--
-- Name: nc_usage_stats nc_usage_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_usage_stats
    ADD CONSTRAINT nc_usage_stats_pkey PRIMARY KEY (fk_workspace_id, usage_type, period_start);


--
-- Name: nc_user_comment_notifications_preference nc_user_comment_notifications_preference_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_user_comment_notifications_preference
    ADD CONSTRAINT nc_user_comment_notifications_preference_pkey PRIMARY KEY (id);


--
-- Name: nc_users_v2 nc_users_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_users_v2
    ADD CONSTRAINT nc_users_v2_pkey PRIMARY KEY (id);


--
-- Name: nc_views_v2 nc_views_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_views_v2
    ADD CONSTRAINT nc_views_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_widgets_v2 nc_widgets_v2_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_widgets_v2
    ADD CONSTRAINT nc_widgets_v2_pkey PRIMARY KEY (base_id, id);


--
-- Name: nc_workflows nc_workflows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nc_workflows
    ADD CONSTRAINT nc_workflows_pkey PRIMARY KEY (id);


--
-- Name: notification notification_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.notification
    ADD CONSTRAINT notification_pkey PRIMARY KEY (id);


--
-- Name: 03医院_项目表 project_table 合作项目分表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."03医院_项目表"
    ADD CONSTRAINT "project_table 合作项目分表_pkey" PRIMARY KEY (id);


--
-- Name: 08项目阶段定义表 uq_stage_def; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."08项目阶段定义表"
    ADD CONSTRAINT uq_stage_def UNIQUE (project_type, stage_key);


--
-- Name: 09项目阶段实例表 uq_stage_instance; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."09项目阶段实例表"
    ADD CONSTRAINT uq_stage_instance UNIQUE (project_id, stage_def_id, scope_row_id);


--
-- Name: workspace workspace_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workspace
    ADD CONSTRAINT workspace_pkey PRIMARY KEY (id);


--
-- Name: workspace_user workspace_user_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.workspace_user
    ADD CONSTRAINT workspace_user_pkey PRIMARY KEY (fk_workspace_id, fk_user_id);


--
-- Name: xc_knex_migrationsv0_lock xc_knex_migrationsv0_lock_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.xc_knex_migrationsv0_lock
    ADD CONSTRAINT xc_knex_migrationsv0_lock_pkey PRIMARY KEY (index);


--
-- Name: xc_knex_migrationsv0 xc_knex_migrationsv0_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.xc_knex_migrationsv0
    ADD CONSTRAINT xc_knex_migrationsv0_pkey PRIMARY KEY (id);


--
-- Name: 04项目总表 项目总表_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."04项目总表"
    ADD CONSTRAINT "项目总表_pkey" PRIMARY KEY (id);


--
-- Name: Features_order_idx; Type: INDEX; Schema: pgzgyje645ljgth; Owner: -
--

CREATE INDEX "Features_order_idx" ON pgzgyje645ljgth."Features" USING btree (nc_order);


--
-- Name: 06样本资源表_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "06样本资源表_order_idx" ON public."06样本资源表" USING btree (nc_order);


--
-- Name: 07操作审计表_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX "07操作审计表_order_idx" ON public."07操作审计表" USING btree (nc_order);


--
-- Name: fk_01__02__f3f0wgucgg; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_01__02__f3f0wgucgg ON public."02仪器资源表" USING btree ("01医院信息表_id");


--
-- Name: fk_01__05__rsa0q8as0f; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_01__05__rsa0q8as0f ON public."05人员表" USING btree ("01医院信息表_id");


--
-- Name: fk_01__06__gdwei0dr9x; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_01__06__gdwei0dr9x ON public."06样本资源表" USING btree ("01医院信息表_id");


--
-- Name: fk_01_hos_res_03_hos_pro_ux1nrnq3ml; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_01_hos_res_03_hos_pro_ux1nrnq3ml ON public."03医院_项目表" USING btree ("01_hos_resource_table医院信息表_id");


--
-- Name: fk_04__05__3b5oio3zep; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_04__05__3b5oio3zep ON public."_nc_m2m_04项目总表_05人员表" USING btree ("04项目总表_id");


--
-- Name: fk_04__05__q8oel2kw9l; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_04__05__q8oel2kw9l ON public."_nc_m2m_04项目总表_05人员表" USING btree ("05人员表_id");


--
-- Name: fk_05__01__kkag17q1u6; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_05__01__kkag17q1u6 ON public."_nc_m2m_05人员表_01医院信息表" USING btree ("01医院信息表_id");


--
-- Name: fk_05__01__t2zpmy9atx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_05__01__t2zpmy9atx ON public."_nc_m2m_05人员表_01医院信息表" USING btree ("05人员表_id");


--
-- Name: fk_05__04__4ddbtm2ahr; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_05__04__4ddbtm2ahr ON public."04项目总表" USING btree ("05人员表_id");


--
-- Name: fk_project_ta_hos_projec_uy_npx1dla; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fk_project_ta_hos_projec_uy_npx1dla ON public."03医院_项目表" USING btree ("project_table 项目总表_id");


--
-- Name: idx_stage_def_project_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stage_def_project_type ON public."08项目阶段定义表" USING btree (project_type, stage_scope, stage_order);


--
-- Name: idx_stage_instance_site; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stage_instance_site ON public."09项目阶段实例表" USING btree (site_project_id);


--
-- Name: nc_api_tokens_fk_sso_client_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_api_tokens_fk_sso_client_id_index ON public.nc_api_tokens USING btree (fk_sso_client_id);


--
-- Name: nc_api_tokens_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_api_tokens_fk_user_id_index ON public.nc_api_tokens USING btree (fk_user_id);


--
-- Name: nc_audit_v2_fk_workspace_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_audit_v2_fk_workspace_idx ON public.nc_audit_v2 USING btree (fk_workspace_id);


--
-- Name: nc_audit_v2_old_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_audit_v2_old_id_index ON public.nc_audit_v2 USING btree (old_id);


--
-- Name: nc_audit_v2_tenant_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_audit_v2_tenant_idx ON public.nc_audit_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_automation_executions_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_automation_executions_oldpk_idx ON public.nc_automation_executions USING btree (id);


--
-- Name: nc_automation_executions_resume_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_automation_executions_resume_idx ON public.nc_automation_executions USING btree (fk_workspace_id, base_id, resume_at);


--
-- Name: nc_automations_context_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_automations_context_idx ON public.nc_automations USING btree (base_id, fk_workspace_id);


--
-- Name: nc_automations_enabled_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_automations_enabled_idx ON public.nc_automations USING btree (enabled);


--
-- Name: nc_automations_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_automations_oldpk_idx ON public.nc_automations USING btree (id);


--
-- Name: nc_automations_order_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_automations_order_idx ON public.nc_automations USING btree (base_id, "order");


--
-- Name: nc_automations_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_automations_type_idx ON public.nc_automations USING btree (type);


--
-- Name: nc_base_users_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_base_users_v2_base_id_fk_workspace_id_index ON public.nc_base_users_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_base_users_v2_invited_by_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_base_users_v2_invited_by_index ON public.nc_base_users_v2 USING btree (invited_by);


--
-- Name: nc_bases_sandbox_auto_update_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_bases_sandbox_auto_update_idx ON public.nc_bases_v2 USING btree (sandbox_id, auto_update);


--
-- Name: nc_bases_sandbox_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_bases_sandbox_id_idx ON public.nc_bases_v2 USING btree (sandbox_id);


--
-- Name: nc_bases_sandbox_master_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_bases_sandbox_master_idx ON public.nc_bases_v2 USING btree (sandbox_master);


--
-- Name: nc_bases_sandbox_version_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_bases_sandbox_version_id_idx ON public.nc_bases_v2 USING btree (sandbox_version_id);


--
-- Name: nc_bases_v2_fk_custom_url_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_bases_v2_fk_custom_url_id_index ON public.nc_bases_v2 USING btree (fk_custom_url_id);


--
-- Name: nc_bases_v2_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_bases_v2_fk_workspace_id_index ON public.nc_bases_v2 USING btree (fk_workspace_id);


--
-- Name: nc_calendar_view_columns_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_calendar_view_columns_v2_base_id_fk_workspace_id_index ON public.nc_calendar_view_columns_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_calendar_view_columns_v2_fk_view_id_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_calendar_view_columns_v2_fk_view_id_fk_column_id_index ON public.nc_calendar_view_columns_v2 USING btree (fk_view_id, fk_column_id);


--
-- Name: nc_calendar_view_columns_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_calendar_view_columns_v2_oldpk_idx ON public.nc_calendar_view_columns_v2 USING btree (id);


--
-- Name: nc_calendar_view_range_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_calendar_view_range_v2_base_id_fk_workspace_id_index ON public.nc_calendar_view_range_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_calendar_view_range_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_calendar_view_range_v2_oldpk_idx ON public.nc_calendar_view_range_v2 USING btree (id);


--
-- Name: nc_calendar_view_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_calendar_view_v2_base_id_fk_workspace_id_index ON public.nc_calendar_view_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_calendar_view_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_calendar_view_v2_oldpk_idx ON public.nc_calendar_view_v2 USING btree (fk_view_id);


--
-- Name: nc_col_barcode_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_barcode_v2_base_id_fk_workspace_id_index ON public.nc_col_barcode_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_barcode_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_barcode_v2_fk_column_id_index ON public.nc_col_barcode_v2 USING btree (fk_column_id);


--
-- Name: nc_col_barcode_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_barcode_v2_oldpk_idx ON public.nc_col_barcode_v2 USING btree (id);


--
-- Name: nc_col_button_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_button_context ON public.nc_col_button_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_button_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_button_v2_fk_column_id_index ON public.nc_col_button_v2 USING btree (fk_column_id);


--
-- Name: nc_col_button_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_button_v2_oldpk_idx ON public.nc_col_button_v2 USING btree (id);


--
-- Name: nc_col_formula_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_formula_v2_base_id_fk_workspace_id_index ON public.nc_col_formula_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_formula_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_formula_v2_fk_column_id_index ON public.nc_col_formula_v2 USING btree (fk_column_id);


--
-- Name: nc_col_formula_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_formula_v2_oldpk_idx ON public.nc_col_formula_v2 USING btree (id);


--
-- Name: nc_col_long_text_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_long_text_context ON public.nc_col_long_text_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_long_text_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_long_text_v2_fk_column_id_index ON public.nc_col_long_text_v2 USING btree (fk_column_id);


--
-- Name: nc_col_long_text_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_long_text_v2_oldpk_idx ON public.nc_col_long_text_v2 USING btree (id);


--
-- Name: nc_col_lookup_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_lookup_v2_base_id_fk_workspace_id_index ON public.nc_col_lookup_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_lookup_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_lookup_v2_fk_column_id_index ON public.nc_col_lookup_v2 USING btree (fk_column_id);


--
-- Name: nc_col_lookup_v2_fk_lookup_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_lookup_v2_fk_lookup_column_id_index ON public.nc_col_lookup_v2 USING btree (fk_lookup_column_id);


--
-- Name: nc_col_lookup_v2_fk_relation_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_lookup_v2_fk_relation_column_id_index ON public.nc_col_lookup_v2 USING btree (fk_relation_column_id);


--
-- Name: nc_col_lookup_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_lookup_v2_oldpk_idx ON public.nc_col_lookup_v2 USING btree (id);


--
-- Name: nc_col_qrcode_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_qrcode_v2_base_id_fk_workspace_id_index ON public.nc_col_qrcode_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_qrcode_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_qrcode_v2_fk_column_id_index ON public.nc_col_qrcode_v2 USING btree (fk_column_id);


--
-- Name: nc_col_qrcode_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_qrcode_v2_oldpk_idx ON public.nc_col_qrcode_v2 USING btree (id);


--
-- Name: nc_col_relations_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_base_id_fk_workspace_id_index ON public.nc_col_relations_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_relations_v2_fk_child_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_fk_child_column_id_index ON public.nc_col_relations_v2 USING btree (fk_child_column_id);


--
-- Name: nc_col_relations_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_fk_column_id_index ON public.nc_col_relations_v2 USING btree (fk_column_id);


--
-- Name: nc_col_relations_v2_fk_mm_child_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_fk_mm_child_column_id_index ON public.nc_col_relations_v2 USING btree (fk_mm_child_column_id);


--
-- Name: nc_col_relations_v2_fk_mm_model_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_fk_mm_model_id_index ON public.nc_col_relations_v2 USING btree (fk_mm_model_id);


--
-- Name: nc_col_relations_v2_fk_mm_parent_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_fk_mm_parent_column_id_index ON public.nc_col_relations_v2 USING btree (fk_mm_parent_column_id);


--
-- Name: nc_col_relations_v2_fk_parent_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_fk_parent_column_id_index ON public.nc_col_relations_v2 USING btree (fk_parent_column_id);


--
-- Name: nc_col_relations_v2_fk_related_model_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_fk_related_model_id_index ON public.nc_col_relations_v2 USING btree (fk_related_model_id);


--
-- Name: nc_col_relations_v2_fk_target_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_fk_target_view_id_index ON public.nc_col_relations_v2 USING btree (fk_target_view_id);


--
-- Name: nc_col_relations_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_relations_v2_oldpk_idx ON public.nc_col_relations_v2 USING btree (id);


--
-- Name: nc_col_rollup_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_rollup_v2_base_id_fk_workspace_id_index ON public.nc_col_rollup_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_rollup_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_rollup_v2_fk_column_id_index ON public.nc_col_rollup_v2 USING btree (fk_column_id);


--
-- Name: nc_col_rollup_v2_fk_relation_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_rollup_v2_fk_relation_column_id_index ON public.nc_col_rollup_v2 USING btree (fk_relation_column_id);


--
-- Name: nc_col_rollup_v2_fk_rollup_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_rollup_v2_fk_rollup_column_id_index ON public.nc_col_rollup_v2 USING btree (fk_rollup_column_id);


--
-- Name: nc_col_rollup_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_rollup_v2_oldpk_idx ON public.nc_col_rollup_v2 USING btree (id);


--
-- Name: nc_col_select_options_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_select_options_v2_base_id_fk_workspace_id_index ON public.nc_col_select_options_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_col_select_options_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_select_options_v2_fk_column_id_index ON public.nc_col_select_options_v2 USING btree (fk_column_id);


--
-- Name: nc_col_select_options_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_col_select_options_v2_oldpk_idx ON public.nc_col_select_options_v2 USING btree (id);


--
-- Name: nc_columns_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_columns_v2_base_id_fk_workspace_id_index ON public.nc_columns_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_columns_v2_fk_model_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_columns_v2_fk_model_id_index ON public.nc_columns_v2 USING btree (fk_model_id);


--
-- Name: nc_columns_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_columns_v2_oldpk_idx ON public.nc_columns_v2 USING btree (id);


--
-- Name: nc_comment_reactions_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_comment_reactions_base_id_fk_workspace_id_index ON public.nc_comment_reactions USING btree (base_id, fk_workspace_id);


--
-- Name: nc_comment_reactions_comment_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_comment_reactions_comment_id_index ON public.nc_comment_reactions USING btree (comment_id);


--
-- Name: nc_comment_reactions_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_comment_reactions_oldpk_idx ON public.nc_comment_reactions USING btree (id);


--
-- Name: nc_comment_reactions_row_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_comment_reactions_row_id_index ON public.nc_comment_reactions USING btree (row_id);


--
-- Name: nc_comments_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_comments_base_id_fk_workspace_id_index ON public.nc_comments USING btree (base_id, fk_workspace_id);


--
-- Name: nc_comments_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_comments_oldpk_idx ON public.nc_comments USING btree (id);


--
-- Name: nc_comments_row_id_fk_model_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_comments_row_id_fk_model_id_index ON public.nc_comments USING btree (row_id, fk_model_id);


--
-- Name: nc_custom_urls_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_custom_urls_context ON public.nc_custom_urls_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_custom_urls_v2_custom_path_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_custom_urls_v2_custom_path_index ON public.nc_custom_urls_v2 USING btree (custom_path);


--
-- Name: nc_custom_urls_v2_fk_dashboard_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_custom_urls_v2_fk_dashboard_id_index ON public.nc_custom_urls_v2 USING btree (fk_dashboard_id);


--
-- Name: nc_custom_urls_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_custom_urls_v2_oldpk_idx ON public.nc_custom_urls_v2 USING btree (id);


--
-- Name: nc_dashboards_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dashboards_context ON public.nc_dashboards_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_dashboards_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dashboards_v2_oldpk_idx ON public.nc_dashboards_v2 USING btree (id);


--
-- Name: nc_data_reflection_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_data_reflection_fk_workspace_id_index ON public.nc_data_reflection USING btree (fk_workspace_id);


--
-- Name: nc_dependency_tracker_context_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dependency_tracker_context_idx ON public.nc_dependency_tracker USING btree (base_id, fk_workspace_id);


--
-- Name: nc_dependency_tracker_dependent_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dependency_tracker_dependent_idx ON public.nc_dependency_tracker USING btree (dependent_type, dependent_id);


--
-- Name: nc_dependency_tracker_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dependency_tracker_oldpk_idx ON public.nc_dependency_tracker USING btree (id);


--
-- Name: nc_dependency_tracker_queryable_field_0_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dependency_tracker_queryable_field_0_idx ON public.nc_dependency_tracker USING btree (queryable_field_0);


--
-- Name: nc_dependency_tracker_queryable_field_1_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dependency_tracker_queryable_field_1_idx ON public.nc_dependency_tracker USING btree (queryable_field_1);


--
-- Name: nc_dependency_tracker_queryable_field_2_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dependency_tracker_queryable_field_2_idx ON public.nc_dependency_tracker USING btree (queryable_field_2);


--
-- Name: nc_dependency_tracker_source_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_dependency_tracker_source_idx ON public.nc_dependency_tracker USING btree (source_type, source_id);


--
-- Name: nc_disabled_models_for_role_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_disabled_models_for_role_v2_base_id_fk_workspace_id_index ON public.nc_disabled_models_for_role_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_disabled_models_for_role_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_disabled_models_for_role_v2_fk_view_id_index ON public.nc_disabled_models_for_role_v2 USING btree (fk_view_id);


--
-- Name: nc_disabled_models_for_role_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_disabled_models_for_role_v2_oldpk_idx ON public.nc_disabled_models_for_role_v2 USING btree (id);


--
-- Name: nc_extensions_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_extensions_base_id_fk_workspace_id_index ON public.nc_extensions USING btree (base_id, fk_workspace_id);


--
-- Name: nc_extensions_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_extensions_oldpk_idx ON public.nc_extensions USING btree (id);


--
-- Name: nc_filter_exp_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_base_id_fk_workspace_id_index ON public.nc_filter_exp_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_filter_exp_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_fk_column_id_index ON public.nc_filter_exp_v2 USING btree (fk_column_id);


--
-- Name: nc_filter_exp_v2_fk_hook_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_fk_hook_id_index ON public.nc_filter_exp_v2 USING btree (fk_hook_id);


--
-- Name: nc_filter_exp_v2_fk_link_col_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_fk_link_col_id_index ON public.nc_filter_exp_v2 USING btree (fk_link_col_id);


--
-- Name: nc_filter_exp_v2_fk_parent_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_fk_parent_column_id_index ON public.nc_filter_exp_v2 USING btree (fk_parent_column_id);


--
-- Name: nc_filter_exp_v2_fk_parent_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_fk_parent_id_index ON public.nc_filter_exp_v2 USING btree (fk_parent_id);


--
-- Name: nc_filter_exp_v2_fk_value_col_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_fk_value_col_id_index ON public.nc_filter_exp_v2 USING btree (fk_value_col_id);


--
-- Name: nc_filter_exp_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_fk_view_id_index ON public.nc_filter_exp_v2 USING btree (fk_view_id);


--
-- Name: nc_filter_exp_v2_fk_widget_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_fk_widget_id_index ON public.nc_filter_exp_v2 USING btree (fk_widget_id);


--
-- Name: nc_filter_exp_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_filter_exp_v2_oldpk_idx ON public.nc_filter_exp_v2 USING btree (id);


--
-- Name: nc_follower_fk_follower_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_follower_fk_follower_id_index ON public.nc_follower USING btree (fk_follower_id);


--
-- Name: nc_follower_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_follower_fk_user_id_index ON public.nc_follower USING btree (fk_user_id);


--
-- Name: nc_form_view_columns_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_form_view_columns_v2_base_id_fk_workspace_id_index ON public.nc_form_view_columns_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_form_view_columns_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_form_view_columns_v2_fk_column_id_index ON public.nc_form_view_columns_v2 USING btree (fk_column_id);


--
-- Name: nc_form_view_columns_v2_fk_view_id_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_form_view_columns_v2_fk_view_id_fk_column_id_index ON public.nc_form_view_columns_v2 USING btree (fk_view_id, fk_column_id);


--
-- Name: nc_form_view_columns_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_form_view_columns_v2_fk_view_id_index ON public.nc_form_view_columns_v2 USING btree (fk_view_id);


--
-- Name: nc_form_view_columns_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_form_view_columns_v2_oldpk_idx ON public.nc_form_view_columns_v2 USING btree (id);


--
-- Name: nc_form_view_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_form_view_v2_base_id_fk_workspace_id_index ON public.nc_form_view_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_form_view_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_form_view_v2_fk_view_id_index ON public.nc_form_view_v2 USING btree (fk_view_id);


--
-- Name: nc_form_view_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_form_view_v2_oldpk_idx ON public.nc_form_view_v2 USING btree (fk_view_id);


--
-- Name: nc_fr_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_fr_context ON public.nc_file_references USING btree (base_id, fk_workspace_id);


--
-- Name: nc_gallery_view_columns_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_gallery_view_columns_v2_base_id_fk_workspace_id_index ON public.nc_gallery_view_columns_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_gallery_view_columns_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_gallery_view_columns_v2_fk_column_id_index ON public.nc_gallery_view_columns_v2 USING btree (fk_column_id);


--
-- Name: nc_gallery_view_columns_v2_fk_view_id_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_gallery_view_columns_v2_fk_view_id_fk_column_id_index ON public.nc_gallery_view_columns_v2 USING btree (fk_view_id, fk_column_id);


--
-- Name: nc_gallery_view_columns_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_gallery_view_columns_v2_fk_view_id_index ON public.nc_gallery_view_columns_v2 USING btree (fk_view_id);


--
-- Name: nc_gallery_view_columns_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_gallery_view_columns_v2_oldpk_idx ON public.nc_gallery_view_columns_v2 USING btree (id);


--
-- Name: nc_gallery_view_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_gallery_view_v2_base_id_fk_workspace_id_index ON public.nc_gallery_view_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_gallery_view_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_gallery_view_v2_fk_view_id_index ON public.nc_gallery_view_v2 USING btree (fk_view_id);


--
-- Name: nc_gallery_view_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_gallery_view_v2_oldpk_idx ON public.nc_gallery_view_v2 USING btree (fk_view_id);


--
-- Name: nc_grid_view_columns_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_grid_view_columns_v2_base_id_fk_workspace_id_index ON public.nc_grid_view_columns_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_grid_view_columns_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_grid_view_columns_v2_fk_column_id_index ON public.nc_grid_view_columns_v2 USING btree (fk_column_id);


--
-- Name: nc_grid_view_columns_v2_fk_view_id_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_grid_view_columns_v2_fk_view_id_fk_column_id_index ON public.nc_grid_view_columns_v2 USING btree (fk_view_id, fk_column_id);


--
-- Name: nc_grid_view_columns_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_grid_view_columns_v2_fk_view_id_index ON public.nc_grid_view_columns_v2 USING btree (fk_view_id);


--
-- Name: nc_grid_view_columns_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_grid_view_columns_v2_oldpk_idx ON public.nc_grid_view_columns_v2 USING btree (id);


--
-- Name: nc_grid_view_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_grid_view_v2_base_id_fk_workspace_id_index ON public.nc_grid_view_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_grid_view_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_grid_view_v2_fk_view_id_index ON public.nc_grid_view_v2 USING btree (fk_view_id);


--
-- Name: nc_grid_view_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_grid_view_v2_oldpk_idx ON public.nc_grid_view_v2 USING btree (fk_view_id);


--
-- Name: nc_hook_logs_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_hook_logs_v2_base_id_fk_workspace_id_index ON public.nc_hook_logs_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_hook_logs_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_hook_logs_v2_oldpk_idx ON public.nc_hook_logs_v2 USING btree (id);


--
-- Name: nc_hooks_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_hooks_v2_base_id_fk_workspace_id_index ON public.nc_hooks_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_hooks_v2_fk_model_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_hooks_v2_fk_model_id_index ON public.nc_hooks_v2 USING btree (fk_model_id);


--
-- Name: nc_hooks_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_hooks_v2_oldpk_idx ON public.nc_hooks_v2 USING btree (id);


--
-- Name: nc_installations_license_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_installations_license_key_idx ON public.nc_installations USING btree (license_key);


--
-- Name: nc_integrations_store_v2_fk_integration_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_integrations_store_v2_fk_integration_id_index ON public.nc_integrations_store_v2 USING btree (fk_integration_id);


--
-- Name: nc_integrations_v2_created_by_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_integrations_v2_created_by_index ON public.nc_integrations_v2 USING btree (created_by);


--
-- Name: nc_integrations_v2_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_integrations_v2_fk_workspace_id_index ON public.nc_integrations_v2 USING btree (fk_workspace_id);


--
-- Name: nc_integrations_v2_type_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_integrations_v2_type_index ON public.nc_integrations_v2 USING btree (type);


--
-- Name: nc_jobs_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_jobs_context ON public.nc_jobs USING btree (base_id, fk_workspace_id);


--
-- Name: nc_kanban_view_columns_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_columns_v2_base_id_fk_workspace_id_index ON public.nc_kanban_view_columns_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_kanban_view_columns_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_columns_v2_fk_column_id_index ON public.nc_kanban_view_columns_v2 USING btree (fk_column_id);


--
-- Name: nc_kanban_view_columns_v2_fk_view_id_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_columns_v2_fk_view_id_fk_column_id_index ON public.nc_kanban_view_columns_v2 USING btree (fk_view_id, fk_column_id);


--
-- Name: nc_kanban_view_columns_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_columns_v2_fk_view_id_index ON public.nc_kanban_view_columns_v2 USING btree (fk_view_id);


--
-- Name: nc_kanban_view_columns_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_columns_v2_oldpk_idx ON public.nc_kanban_view_columns_v2 USING btree (id);


--
-- Name: nc_kanban_view_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_v2_base_id_fk_workspace_id_index ON public.nc_kanban_view_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_kanban_view_v2_fk_grp_col_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_v2_fk_grp_col_id_index ON public.nc_kanban_view_v2 USING btree (fk_grp_col_id);


--
-- Name: nc_kanban_view_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_v2_fk_view_id_index ON public.nc_kanban_view_v2 USING btree (fk_view_id);


--
-- Name: nc_kanban_view_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_kanban_view_v2_oldpk_idx ON public.nc_kanban_view_v2 USING btree (fk_view_id);


--
-- Name: nc_map_view_columns_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_columns_v2_base_id_fk_workspace_id_index ON public.nc_map_view_columns_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_map_view_columns_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_columns_v2_fk_column_id_index ON public.nc_map_view_columns_v2 USING btree (fk_column_id);


--
-- Name: nc_map_view_columns_v2_fk_view_id_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_columns_v2_fk_view_id_fk_column_id_index ON public.nc_map_view_columns_v2 USING btree (fk_view_id, fk_column_id);


--
-- Name: nc_map_view_columns_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_columns_v2_fk_view_id_index ON public.nc_map_view_columns_v2 USING btree (fk_view_id);


--
-- Name: nc_map_view_columns_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_columns_v2_oldpk_idx ON public.nc_map_view_columns_v2 USING btree (id);


--
-- Name: nc_map_view_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_v2_base_id_fk_workspace_id_index ON public.nc_map_view_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_map_view_v2_fk_geo_data_col_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_v2_fk_geo_data_col_id_index ON public.nc_map_view_v2 USING btree (fk_geo_data_col_id);


--
-- Name: nc_map_view_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_v2_fk_view_id_index ON public.nc_map_view_v2 USING btree (fk_view_id);


--
-- Name: nc_map_view_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_map_view_v2_oldpk_idx ON public.nc_map_view_v2 USING btree (fk_view_id);


--
-- Name: nc_mc_tokens_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_mc_tokens_context ON public.nc_mcp_tokens USING btree (base_id, fk_workspace_id);


--
-- Name: nc_mcp_tokens_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_mcp_tokens_oldpk_idx ON public.nc_mcp_tokens USING btree (id);


--
-- Name: nc_model_stats_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_model_stats_v2_base_id_fk_workspace_id_index ON public.nc_model_stats_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_model_stats_v2_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_model_stats_v2_fk_workspace_id_index ON public.nc_model_stats_v2 USING btree (fk_workspace_id);


--
-- Name: nc_model_stats_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_model_stats_v2_oldpk_idx ON public.nc_model_stats_v2 USING btree (fk_workspace_id, fk_model_id);


--
-- Name: nc_models_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_models_v2_base_id_fk_workspace_id_index ON public.nc_models_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_models_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_models_v2_oldpk_idx ON public.nc_models_v2 USING btree (id);


--
-- Name: nc_models_v2_source_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_models_v2_source_id_index ON public.nc_models_v2 USING btree (source_id);


--
-- Name: nc_models_v2_type_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_models_v2_type_index ON public.nc_models_v2 USING btree (type);


--
-- Name: nc_models_v2_uuid_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_models_v2_uuid_index ON public.nc_models_v2 USING btree (uuid);


--
-- Name: nc_oauth_authorization_codes_code_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_authorization_codes_code_index ON public.nc_oauth_authorization_codes USING btree (code);


--
-- Name: nc_oauth_authorization_codes_expires_at_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_authorization_codes_expires_at_index ON public.nc_oauth_authorization_codes USING btree (expires_at);


--
-- Name: nc_oauth_authorization_codes_fk_client_id_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_authorization_codes_fk_client_id_fk_user_id_index ON public.nc_oauth_authorization_codes USING btree (fk_client_id, fk_user_id);


--
-- Name: nc_oauth_authorization_codes_fk_client_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_authorization_codes_fk_client_id_index ON public.nc_oauth_authorization_codes USING btree (fk_client_id);


--
-- Name: nc_oauth_authorization_codes_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_authorization_codes_fk_user_id_index ON public.nc_oauth_authorization_codes USING btree (fk_user_id);


--
-- Name: nc_oauth_authorization_codes_is_used_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_authorization_codes_is_used_index ON public.nc_oauth_authorization_codes USING btree (is_used);


--
-- Name: nc_oauth_clients_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_clients_fk_user_id_index ON public.nc_oauth_clients USING btree (fk_user_id);


--
-- Name: nc_oauth_tokens_access_token_expires_at_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_access_token_expires_at_index ON public.nc_oauth_tokens USING btree (access_token_expires_at);


--
-- Name: nc_oauth_tokens_access_token_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_access_token_index ON public.nc_oauth_tokens USING btree (access_token);


--
-- Name: nc_oauth_tokens_fk_client_id_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_fk_client_id_fk_user_id_index ON public.nc_oauth_tokens USING btree (fk_client_id, fk_user_id);


--
-- Name: nc_oauth_tokens_fk_client_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_fk_client_id_index ON public.nc_oauth_tokens USING btree (fk_client_id);


--
-- Name: nc_oauth_tokens_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_fk_user_id_index ON public.nc_oauth_tokens USING btree (fk_user_id);


--
-- Name: nc_oauth_tokens_is_revoked_access_token_expires_at_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_is_revoked_access_token_expires_at_index ON public.nc_oauth_tokens USING btree (is_revoked, access_token_expires_at);


--
-- Name: nc_oauth_tokens_is_revoked_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_is_revoked_index ON public.nc_oauth_tokens USING btree (is_revoked);


--
-- Name: nc_oauth_tokens_last_used_at_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_last_used_at_index ON public.nc_oauth_tokens USING btree (last_used_at);


--
-- Name: nc_oauth_tokens_refresh_token_expires_at_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_refresh_token_expires_at_index ON public.nc_oauth_tokens USING btree (refresh_token_expires_at);


--
-- Name: nc_oauth_tokens_refresh_token_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_oauth_tokens_refresh_token_index ON public.nc_oauth_tokens USING btree (refresh_token);


--
-- Name: nc_org_domain_domain_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_org_domain_domain_index ON public.nc_org_domain USING btree (domain);


--
-- Name: nc_org_domain_fk_org_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_org_domain_fk_org_id_index ON public.nc_org_domain USING btree (fk_org_id);


--
-- Name: nc_org_domain_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_org_domain_fk_user_id_index ON public.nc_org_domain USING btree (fk_user_id);


--
-- Name: nc_org_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_org_fk_user_id_index ON public.nc_org USING btree (fk_user_id);


--
-- Name: nc_org_slug_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_org_slug_index ON public.nc_org USING btree (slug);


--
-- Name: nc_permission_subjects_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_permission_subjects_context ON public.nc_permission_subjects USING btree (fk_workspace_id, base_id);


--
-- Name: nc_permission_subjects_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_permission_subjects_oldpk_idx ON public.nc_permission_subjects USING btree (fk_permission_id, subject_type, subject_id);


--
-- Name: nc_permissions_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_permissions_context ON public.nc_permissions USING btree (base_id, fk_workspace_id);


--
-- Name: nc_permissions_entity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_permissions_entity ON public.nc_permissions USING btree (entity, entity_id, permission);


--
-- Name: nc_permissions_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_permissions_oldpk_idx ON public.nc_permissions USING btree (id);


--
-- Name: nc_plans_stripe_product_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_plans_stripe_product_idx ON public.nc_plans USING btree (stripe_product_id);


--
-- Name: nc_principal_assignments_principal_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_principal_assignments_principal_idx ON public.nc_principal_assignments USING btree (principal_type, principal_ref_id);


--
-- Name: nc_principal_assignments_principal_resource_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_principal_assignments_principal_resource_idx ON public.nc_principal_assignments USING btree (principal_type, principal_ref_id, resource_type);


--
-- Name: nc_principal_assignments_resource_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_principal_assignments_resource_idx ON public.nc_principal_assignments USING btree (resource_type, resource_id);


--
-- Name: nc_principal_assignments_resource_principal_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_principal_assignments_resource_principal_type_idx ON public.nc_principal_assignments USING btree (resource_type, resource_id, principal_type);


--
-- Name: nc_project_users_v2_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_project_users_v2_fk_user_id_index ON public.nc_base_users_v2 USING btree (fk_user_id);


--
-- Name: nc_record_audit_v2_tenant_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_record_audit_v2_tenant_idx ON public.nc_audit_v2 USING btree (base_id, fk_model_id, row_id, fk_workspace_id);


--
-- Name: nc_row_color_conditions_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_row_color_conditions_fk_view_id_index ON public.nc_row_color_conditions USING btree (fk_view_id);


--
-- Name: nc_row_color_conditions_fk_workspace_id_base_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_row_color_conditions_fk_workspace_id_base_id_index ON public.nc_row_color_conditions USING btree (fk_workspace_id, base_id);


--
-- Name: nc_row_color_conditions_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_row_color_conditions_oldpk_idx ON public.nc_row_color_conditions USING btree (id);


--
-- Name: nc_sandbox_deployment_logs_base_created_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_deployment_logs_base_created_idx ON public.nc_sandbox_deployment_logs USING btree (base_id, created_at);


--
-- Name: nc_sandbox_deployment_logs_base_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_deployment_logs_base_id_idx ON public.nc_sandbox_deployment_logs USING btree (base_id);


--
-- Name: nc_sandbox_deployment_logs_from_version_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_deployment_logs_from_version_idx ON public.nc_sandbox_deployment_logs USING btree (from_version_id);


--
-- Name: nc_sandbox_deployment_logs_sandbox_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_deployment_logs_sandbox_id_idx ON public.nc_sandbox_deployment_logs USING btree (fk_sandbox_id);


--
-- Name: nc_sandbox_deployment_logs_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_deployment_logs_status_idx ON public.nc_sandbox_deployment_logs USING btree (status);


--
-- Name: nc_sandbox_deployment_logs_to_version_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_deployment_logs_to_version_idx ON public.nc_sandbox_deployment_logs USING btree (to_version_id);


--
-- Name: nc_sandbox_deployment_logs_workspace_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_deployment_logs_workspace_id_idx ON public.nc_sandbox_deployment_logs USING btree (fk_workspace_id);


--
-- Name: nc_sandbox_versions_ordering_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_versions_ordering_idx ON public.nc_sandbox_versions USING btree (fk_sandbox_id, version_number);


--
-- Name: nc_sandbox_versions_sandbox_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_versions_sandbox_id_idx ON public.nc_sandbox_versions USING btree (fk_sandbox_id);


--
-- Name: nc_sandbox_versions_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_versions_status_idx ON public.nc_sandbox_versions USING btree (fk_sandbox_id, status);


--
-- Name: nc_sandbox_versions_workspace_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandbox_versions_workspace_id_idx ON public.nc_sandbox_versions USING btree (fk_workspace_id);


--
-- Name: nc_sandboxes_base_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandboxes_base_id_idx ON public.nc_sandboxes USING btree (base_id);


--
-- Name: nc_sandboxes_category_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandboxes_category_idx ON public.nc_sandboxes USING btree (category);


--
-- Name: nc_sandboxes_created_by_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandboxes_created_by_idx ON public.nc_sandboxes USING btree (created_by);


--
-- Name: nc_sandboxes_deleted_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandboxes_deleted_idx ON public.nc_sandboxes USING btree (deleted);


--
-- Name: nc_sandboxes_visibility_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandboxes_visibility_idx ON public.nc_sandboxes USING btree (visibility);


--
-- Name: nc_sandboxes_workspace_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sandboxes_workspace_id_idx ON public.nc_sandboxes USING btree (fk_workspace_id);


--
-- Name: nc_scripts_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_scripts_context ON public.nc_scripts USING btree (base_id, fk_workspace_id);


--
-- Name: nc_scripts_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_scripts_oldpk_idx ON public.nc_scripts USING btree (id);


--
-- Name: nc_snapshot_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_snapshot_context ON public.nc_snapshots USING btree (base_id, fk_workspace_id);


--
-- Name: nc_sort_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sort_v2_base_id_fk_workspace_id_index ON public.nc_sort_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_sort_v2_fk_column_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sort_v2_fk_column_id_index ON public.nc_sort_v2 USING btree (fk_column_id);


--
-- Name: nc_sort_v2_fk_view_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sort_v2_fk_view_id_index ON public.nc_sort_v2 USING btree (fk_view_id);


--
-- Name: nc_sort_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sort_v2_oldpk_idx ON public.nc_sort_v2 USING btree (id);


--
-- Name: nc_source_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_source_v2_base_id_fk_workspace_id_index ON public.nc_sources_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_source_v2_fk_integration_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_source_v2_fk_integration_id_index ON public.nc_sources_v2 USING btree (fk_integration_id);


--
-- Name: nc_source_v2_fk_sql_executor_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_source_v2_fk_sql_executor_id_index ON public.nc_sources_v2 USING btree (fk_sql_executor_id);


--
-- Name: nc_sources_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sources_v2_oldpk_idx ON public.nc_sources_v2 USING btree (id);


--
-- Name: nc_sso_client_domain_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sso_client_domain_name_index ON public.nc_sso_client USING btree (domain_name);


--
-- Name: nc_sso_client_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sso_client_fk_user_id_index ON public.nc_sso_client USING btree (fk_user_id);


--
-- Name: nc_sso_client_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sso_client_fk_workspace_id_index ON public.nc_sso_client USING btree (fk_org_id);


--
-- Name: nc_store_key_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_store_key_index ON public.nc_store USING btree (key);


--
-- Name: nc_subscriptions_org_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_subscriptions_org_idx ON public.nc_subscriptions USING btree (fk_org_id);


--
-- Name: nc_subscriptions_stripe_subscription_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_subscriptions_stripe_subscription_idx ON public.nc_subscriptions USING btree (stripe_subscription_id);


--
-- Name: nc_subscriptions_ws_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_subscriptions_ws_idx ON public.nc_subscriptions USING btree (fk_workspace_id);


--
-- Name: nc_sync_configs_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_configs_context ON public.nc_sync_configs USING btree (base_id, fk_workspace_id);


--
-- Name: nc_sync_configs_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_configs_oldpk_idx ON public.nc_sync_configs USING btree (id);


--
-- Name: nc_sync_configs_parent_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_configs_parent_idx ON public.nc_sync_configs USING btree (fk_parent_sync_config_id);


--
-- Name: nc_sync_logs_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_logs_v2_base_id_fk_workspace_id_index ON public.nc_sync_logs_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_sync_logs_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_logs_v2_oldpk_idx ON public.nc_sync_logs_v2 USING btree (id);


--
-- Name: nc_sync_mappings_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_mappings_context ON public.nc_sync_mappings USING btree (base_id, fk_workspace_id);


--
-- Name: nc_sync_mappings_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_mappings_oldpk_idx ON public.nc_sync_mappings USING btree (id);


--
-- Name: nc_sync_mappings_sync_config_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_mappings_sync_config_idx ON public.nc_sync_mappings USING btree (fk_sync_config_id);


--
-- Name: nc_sync_source_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_source_v2_base_id_fk_workspace_id_index ON public.nc_sync_source_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_sync_source_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_source_v2_oldpk_idx ON public.nc_sync_source_v2 USING btree (id);


--
-- Name: nc_sync_source_v2_source_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_sync_source_v2_source_id_index ON public.nc_sync_source_v2 USING btree (source_id);


--
-- Name: nc_teams_created_by_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_teams_created_by_idx ON public.nc_teams USING btree (created_by);


--
-- Name: nc_teams_org_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_teams_org_idx ON public.nc_teams USING btree (fk_org_id);


--
-- Name: nc_teams_workspace_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_teams_workspace_idx ON public.nc_teams USING btree (fk_workspace_id);


--
-- Name: nc_usage_stats_ws_period_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_usage_stats_ws_period_idx ON public.nc_usage_stats USING btree (fk_workspace_id, period_start);


--
-- Name: nc_user_comment_notifications_preference_base_id_fk_workspace_i; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_user_comment_notifications_preference_base_id_fk_workspace_i ON public.nc_user_comment_notifications_preference USING btree (base_id, fk_workspace_id);


--
-- Name: nc_user_refresh_tokens_expires_at_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_user_refresh_tokens_expires_at_index ON public.nc_user_refresh_tokens USING btree (expires_at);


--
-- Name: nc_user_refresh_tokens_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_user_refresh_tokens_fk_user_id_index ON public.nc_user_refresh_tokens USING btree (fk_user_id);


--
-- Name: nc_user_refresh_tokens_token_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_user_refresh_tokens_token_index ON public.nc_user_refresh_tokens USING btree (token);


--
-- Name: nc_users_v2_email_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_users_v2_email_index ON public.nc_users_v2 USING btree (email);


--
-- Name: nc_views_v2_base_id_fk_workspace_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_views_v2_base_id_fk_workspace_id_index ON public.nc_views_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_views_v2_created_by_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_views_v2_created_by_index ON public.nc_views_v2 USING btree (created_by);


--
-- Name: nc_views_v2_fk_custom_url_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_views_v2_fk_custom_url_id_index ON public.nc_views_v2 USING btree (fk_custom_url_id);


--
-- Name: nc_views_v2_fk_model_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_views_v2_fk_model_id_index ON public.nc_views_v2 USING btree (fk_model_id);


--
-- Name: nc_views_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_views_v2_oldpk_idx ON public.nc_views_v2 USING btree (id);


--
-- Name: nc_views_v2_owned_by_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_views_v2_owned_by_index ON public.nc_views_v2 USING btree (owned_by);


--
-- Name: nc_widgets_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_widgets_context ON public.nc_widgets_v2 USING btree (base_id, fk_workspace_id);


--
-- Name: nc_widgets_dashboard_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_widgets_dashboard_idx ON public.nc_widgets_v2 USING btree (fk_dashboard_id);


--
-- Name: nc_widgets_v2_oldpk_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_widgets_v2_oldpk_idx ON public.nc_widgets_v2 USING btree (id);


--
-- Name: nc_workflow_executions_context_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_workflow_executions_context_idx ON public.nc_automation_executions USING btree (base_id, fk_workspace_id);


--
-- Name: nc_workflow_executions_workflow_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_workflow_executions_workflow_idx ON public.nc_automation_executions USING btree (fk_workflow_id);


--
-- Name: nc_workflows_context_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX nc_workflows_context_idx ON public.nc_workflows USING btree (base_id, fk_workspace_id);


--
-- Name: notification_created_at_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX notification_created_at_index ON public.notification USING btree (created_at);


--
-- Name: notification_fk_user_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX notification_fk_user_id_index ON public.notification USING btree (fk_user_id);


--
-- Name: org_domain_fk_workspace_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX org_domain_fk_workspace_id_idx ON public.nc_org_domain USING btree (fk_workspace_id);


--
-- Name: share_uuid_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX share_uuid_idx ON public.nc_dashboards_v2 USING btree (uuid);


--
-- Name: sso_client_fk_workspace_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX sso_client_fk_workspace_id_idx ON public.nc_sso_client USING btree (fk_workspace_id);


--
-- Name: sync_configs_integration_model; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX sync_configs_integration_model ON public.nc_sync_configs USING btree (fk_model_id, fk_integration_id);


--
-- Name: user_comments_preference_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX user_comments_preference_index ON public.nc_user_comment_notifications_preference USING btree (user_id, row_id, fk_model_id);


--
-- Name: workspace_fk_org_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX workspace_fk_org_id_index ON public.workspace USING btree (fk_org_id);


--
-- Name: workspace_user_invited_by_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX workspace_user_invited_by_index ON public.workspace_user USING btree (invited_by);


--
-- Name: 09项目阶段实例表 trg_bump_stage_instance_version; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_bump_stage_instance_version BEFORE UPDATE ON public."09项目阶段实例表" FOR EACH ROW EXECUTE FUNCTION public.bump_stage_instance_version();


--
-- Name: 04项目总表 trg_ensure_project_stage_instances; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_ensure_project_stage_instances AFTER INSERT OR UPDATE OF "项目类型", is_active ON public."04项目总表" FOR EACH ROW EXECUTE FUNCTION public.ensure_project_stage_instances();


--
-- Name: 03医院_项目表 trg_ensure_site_stage_instances; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_ensure_site_stage_instances AFTER INSERT OR UPDATE OF "project_table 项目总表_id" ON public."03医院_项目表" FOR EACH ROW EXECUTE FUNCTION public.ensure_site_stage_instances();


--
-- Name: 09项目阶段实例表 09项目阶段实例表_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."09项目阶段实例表"
    ADD CONSTRAINT "09项目阶段实例表_project_id_fkey" FOREIGN KEY (project_id) REFERENCES public."04项目总表"(id) ON DELETE CASCADE;


--
-- Name: 09项目阶段实例表 09项目阶段实例表_site_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."09项目阶段实例表"
    ADD CONSTRAINT "09项目阶段实例表_site_project_id_fkey" FOREIGN KEY (site_project_id) REFERENCES public."03医院_项目表"(id) ON DELETE CASCADE;


--
-- Name: 09项目阶段实例表 09项目阶段实例表_stage_def_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."09项目阶段实例表"
    ADD CONSTRAINT "09项目阶段实例表_stage_def_id_fkey" FOREIGN KEY (stage_def_id) REFERENCES public."08项目阶段定义表"(id) ON DELETE RESTRICT;


--
-- Name: 02仪器资源表 fk_01__02__kpyhy59mfs; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."02仪器资源表"
    ADD CONSTRAINT fk_01__02__kpyhy59mfs FOREIGN KEY ("01医院信息表_id") REFERENCES public."01医院信息表"(id);


--
-- Name: 05人员表 fk_01__05__y5ku1azgka; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."05人员表"
    ADD CONSTRAINT fk_01__05__y5ku1azgka FOREIGN KEY ("01医院信息表_id") REFERENCES public."01医院信息表"(id);


--
-- Name: 06样本资源表 fk_01__06__ju79q4fjel; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."06样本资源表"
    ADD CONSTRAINT fk_01__06__ju79q4fjel FOREIGN KEY ("01医院信息表_id") REFERENCES public."01医院信息表"(id);


--
-- Name: 03医院_项目表 fk_01_hos_res_03_hos_pro_gayqbsqusl; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."03医院_项目表"
    ADD CONSTRAINT fk_01_hos_res_03_hos_pro_gayqbsqusl FOREIGN KEY ("01_hos_resource_table医院信息表_id") REFERENCES public."01医院信息表"(id);


--
-- Name: _nc_m2m_04项目总表_05人员表 fk_04__05__c_yw4vqbks; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_nc_m2m_04项目总表_05人员表"
    ADD CONSTRAINT fk_04__05__c_yw4vqbks FOREIGN KEY ("04项目总表_id") REFERENCES public."04项目总表"(id);


--
-- Name: _nc_m2m_04项目总表_05人员表 fk_04__05__in8ft69hat; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_nc_m2m_04项目总表_05人员表"
    ADD CONSTRAINT fk_04__05__in8ft69hat FOREIGN KEY ("05人员表_id") REFERENCES public."05人员表"(id);


--
-- Name: _nc_m2m_05人员表_01医院信息表 fk_05__01__iddssi0fjb; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_nc_m2m_05人员表_01医院信息表"
    ADD CONSTRAINT fk_05__01__iddssi0fjb FOREIGN KEY ("01医院信息表_id") REFERENCES public."01医院信息表"(id);


--
-- Name: _nc_m2m_05人员表_01医院信息表 fk_05__01__w6wzpp9yv7; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."_nc_m2m_05人员表_01医院信息表"
    ADD CONSTRAINT fk_05__01__w6wzpp9yv7 FOREIGN KEY ("05人员表_id") REFERENCES public."05人员表"(id);


--
-- Name: 04项目总表 fk_05__04__v43u3z22v8; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."04项目总表"
    ADD CONSTRAINT fk_05__04__v43u3z22v8 FOREIGN KEY ("05人员表_id") REFERENCES public."05人员表"(id);


--
-- Name: 03医院_项目表 fk_project_ta_hos_projec_7kiz8i3b6g; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public."03医院_项目表"
    ADD CONSTRAINT fk_project_ta_hos_projec_7kiz8i3b6g FOREIGN KEY ("project_table 项目总表_id") REFERENCES public."04项目总表"(id);


--
-- PostgreSQL database dump complete
--

\unrestrict XNkcYhHo6WJrEMKF3JRcpeE2AmAbIFld1qg4jk9L2zazdoz1cagNU95ThyK25xH

