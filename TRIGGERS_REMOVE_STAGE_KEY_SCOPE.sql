-- 适配 09 表移除 stage_key、stage_scope 后的触发器更新
-- 逻辑：新建项目/中心时，为该类型下 08 的「所有」阶段在 09 中创建实例行；
-- 09.is_active 与 08.is_active 一致（08 激活则 09 激活，08 未激活则 09 未激活）。

CREATE OR REPLACE FUNCTION public.ensure_project_stage_instances() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_project_type text;
BEGIN
  v_project_type := NEW."项目类型";

  IF v_project_type IS NULL OR v_project_type = '' THEN
    RETURN NEW;
  END IF;

  -- 不论 08 是否 is_active，都创建 09 行；09.is_active 与 08.is_active 一致
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
    COALESCE(d.is_active, TRUE) AS is_active
  FROM public."08项目阶段定义表" d
  WHERE d.project_type = v_project_type
    AND d.stage_scope = 'sync'
  ON CONFLICT (project_id, stage_def_id, scope_row_id)
  DO UPDATE SET is_active = EXCLUDED.is_active;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.ensure_site_stage_instances() RETURNS trigger
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

  -- 仅当项目本身激活时才为该项目补全中心的 09 行（避免归档项目继续生成中心实例）
  IF v_proj_active IS NOT TRUE THEN
    RETURN NEW;
  END IF;

  IF v_project_type IS NULL OR v_project_type = '' THEN
    RETURN NEW;
  END IF;

  -- 不论 08 是否 is_active，都创建 09 行；09.is_active 与 08.is_active 一致
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
    COALESCE(d.is_active, TRUE) AS is_active
  FROM public."08项目阶段定义表" d
  WHERE d.project_type = v_project_type
    AND d.stage_scope = 'site'
  ON CONFLICT (project_id, stage_def_id, scope_row_id)
  DO UPDATE SET is_active = EXCLUDED.is_active;

  RETURN NEW;
END;
$$;
