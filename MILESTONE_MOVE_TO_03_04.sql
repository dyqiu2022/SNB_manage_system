-- ============================================================
-- 里程碑迁移：从 09项目阶段实例表 迁移至 04项目总表 / 03医院_项目表
-- 原因：里程碑属于「项目」或「子中心」维度，与具体阶段实例无关；
--       放在 09 表导致同一份数据分散在多行，且删除/停用阶段后数据难以追踪。
-- ============================================================

-- 1. 新增列 -------------------------------------------------
ALTER TABLE public."04项目总表"
  ADD COLUMN IF NOT EXISTS milestones_json jsonb;

ALTER TABLE public."03医院_项目表"
  ADD COLUMN IF NOT EXISTS milestones_json jsonb;

-- 2. 数据迁移 -----------------------------------------------

-- 2a. 同步阶段里程碑 → 04项目总表
--     将同一项目所有 scope_row_id=0 的 09 行里的 milestone 条目合并成一个 JSON 对象
UPDATE public."04项目总表" p
SET milestones_json = sub.merged
FROM (
  SELECT
    si.project_id,
    jsonb_object_agg(e.key, e.value) AS merged
  FROM public."09项目阶段实例表" si
  CROSS JOIN LATERAL jsonb_each(COALESCE(si.milestones_json, '{}'::jsonb)) AS e(key, value)
  WHERE si.scope_row_id = 0
    AND si.milestones_json IS NOT NULL
    AND si.milestones_json <> '{}'::jsonb
  GROUP BY si.project_id
) sub
WHERE p.id = sub.project_id;

-- 2b. 子中心阶段里程碑 → 03医院_项目表
--     将同一 site_project_id 的所有 09 行里的 milestone 条目合并
UPDATE public."03医院_项目表" s
SET milestones_json = sub.merged
FROM (
  SELECT
    si.site_project_id,
    jsonb_object_agg(e.key, e.value) AS merged
  FROM public."09项目阶段实例表" si
  CROSS JOIN LATERAL jsonb_each(COALESCE(si.milestones_json, '{}'::jsonb)) AS e(key, value)
  WHERE si.site_project_id IS NOT NULL
    AND si.milestones_json IS NOT NULL
    AND si.milestones_json <> '{}'::jsonb
  GROUP BY si.site_project_id
) sub
WHERE s.id = sub.site_project_id;

-- 3. 更新视图 -----------------------------------------------

-- 3a. v_项目阶段甘特视图（仅活跃阶段）
DROP VIEW IF EXISTS public."v_项目阶段甘特视图";

CREATE VIEW public."v_项目阶段甘特视图" AS
SELECT
  si.id AS stage_instance_id,
  si.project_id AS project_db_id,
  COALESCE(NULLIF(p."项目名称", ''), '项目-' || p.id::text) AS project_id,
  p."项目类型" AS project_type,
  p."重要紧急程度" AS "重要紧急程度",
  COALESCE(p.is_active, true) AS project_is_active,
  si.project_id AS proj_row_id,
  si.site_project_id AS site_row_id,
  CASE
    WHEN d.stage_scope = 'sync' THEN '所有中心（同步）'
    ELSE COALESCE(NULLIF(h."医院名称", ''), '中心-' || COALESCE(s.id, 0)::text)
  END AS site_name,
  d.stage_key AS task_name,
  d.stage_name AS task_display_name,
  d.stage_order AS stage_ord,
  d.stage_scope,
  'Process'::text AS task_type,
  si.planned_start_date,
  si.actual_start_date,
  COALESCE(si.actual_start_date, si.planned_start_date) AS start_date,
  si.planned_end_date,
  si.actual_end_date,
  COALESCE(si.progress, 0)::numeric / 100.0 AS progress,
  (COALESCE(si.actual_start_date, si.planned_start_date) IS NULL OR si.planned_end_date IS NULL) AS is_unplanned,
  si.remark_json::text AS remark,
  si.remark_json,
  si.contributors_json,
  CASE WHEN d.stage_scope = 'sync' THEN p.milestones_json ELSE s.milestones_json END AS milestones_json,
  si.sample_json,
  si.row_version,
  mgr."姓名" AS manager_name
FROM public."09项目阶段实例表" si
JOIN public."08项目阶段定义表" d ON d.id = si.stage_def_id
JOIN public."04项目总表" p ON p.id = si.project_id
LEFT JOIN public."03医院_项目表" s ON s.id = si.site_project_id
LEFT JOIN public."01医院信息表" h ON h.id = s."01_hos_resource_table医院信息表_id"
LEFT JOIN public."05人员表" mgr ON mgr.id = p."05人员表_id"
WHERE COALESCE(si.is_active, true) = true;

-- 3b. v_项目阶段甘特视图_全部（含未激活阶段，用于里程碑展示）
CREATE OR REPLACE VIEW public."v_项目阶段甘特视图_全部" AS
SELECT
  si.id AS stage_instance_id,
  si.project_id AS project_db_id,
  COALESCE(NULLIF(p."项目名称", ''), '项目-' || p.id::text) AS project_id,
  p."项目类型" AS project_type,
  p."重要紧急程度" AS "重要紧急程度",
  COALESCE(p.is_active, true) AS project_is_active,
  si.project_id AS proj_row_id,
  si.site_project_id AS site_row_id,
  CASE
    WHEN d.stage_scope = 'sync' THEN '所有中心（同步）'
    ELSE COALESCE(NULLIF(h."医院名称", ''), '中心-' || COALESCE(s.id, 0)::text)
  END AS site_name,
  d.stage_key AS task_name,
  d.stage_name AS task_display_name,
  d.stage_order AS stage_ord,
  d.stage_scope,
  'Process'::text AS task_type,
  si.planned_start_date,
  si.actual_start_date,
  COALESCE(si.actual_start_date, si.planned_start_date) AS start_date,
  si.planned_end_date,
  si.actual_end_date,
  COALESCE(si.progress, 0)::numeric / 100.0 AS progress,
  (COALESCE(si.actual_start_date, si.planned_start_date) IS NULL OR si.planned_end_date IS NULL) AS is_unplanned,
  si.remark_json::text AS remark,
  si.remark_json,
  si.contributors_json,
  CASE WHEN d.stage_scope = 'sync' THEN p.milestones_json ELSE s.milestones_json END AS milestones_json,
  si.sample_json,
  si.row_version,
  mgr."姓名" AS manager_name
FROM public."09项目阶段实例表" si
JOIN public."08项目阶段定义表" d ON d.id = si.stage_def_id
JOIN public."04项目总表" p ON p.id = si.project_id
LEFT JOIN public."03医院_项目表" s ON s.id = si.site_project_id
LEFT JOIN public."01医院信息表" h ON h.id = s."01_hos_resource_table医院信息表_id"
LEFT JOIN public."05人员表" mgr ON mgr.id = p."05人员表_id";

-- 4. 清理 09 表中旧的 milestones_json 列（数据已迁至 03/04）
ALTER TABLE public."09项目阶段实例表" DROP COLUMN IF EXISTS milestones_json;
