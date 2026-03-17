-- 先修改视图，改为从 08 表取 stage_key、stage_scope，解除对 09 表这两列的依赖
-- 执行本脚本后，即可在 09 表上安全删除 stage_key、stage_scope 列
-- 同时适配 09 表新增的 planned_start_date / actual_start_date，废弃 start_date 列

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
  -- 有效开始日期：实际开始优先，未填则用计划开始
  COALESCE(si.actual_start_date, si.planned_start_date) AS start_date,
  si.planned_end_date,
  si.actual_end_date,
  COALESCE(si.progress, 0)::numeric / 100.0 AS progress,
  (COALESCE(si.actual_start_date, si.planned_start_date) IS NULL OR si.planned_end_date IS NULL) AS is_unplanned,
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
