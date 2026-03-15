-- 视图更新：适配 04/08/09 的 is_active 语义
-- 04.is_active: 项目是否参与甘特（可归档）
-- 08.is_active: 模板默认，触发器用；视图不再按 08 过滤，以 09 实例为准
-- 09.is_active: 实例是否参与甘特；NULL 视为 TRUE 兼容旧数据

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
    WHEN si.stage_scope = 'sync' THEN '所有中心（同步）'
    ELSE COALESCE(NULLIF(h."医院名称", ''), '中心-' || COALESCE(s.id, 0)::text)
  END AS site_name,
  si.stage_key AS task_name,
  d.stage_name AS task_display_name,
  d.stage_order AS stage_ord,
  si.stage_scope,
  'Process'::text AS task_type,
  si.start_date,
  si.planned_end_date,
  si.actual_end_date,
  COALESCE(si.progress, 0)::numeric / 100.0 AS progress,
  (si.start_date IS NULL OR si.planned_end_date IS NULL) AS is_unplanned,
  si.remark_json::text AS remark,
  si.remark_json,
  si.contributors_json,
  si.milestones_json,
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
