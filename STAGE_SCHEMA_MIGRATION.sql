-- 项目阶段模型迁移脚本
-- 目标：
-- 1. 保留 `03医院_项目表` / `04项目总表` 作为项目主数据
-- 2. 将“阶段”从按列存储改为按行存储
-- 3. 保留现有备注/贡献者/里程碑/样本的 JSON 形态，降低 app 迁移复杂度
-- 4. 提供统一视图，便于 app.R 读取甘特阶段数据

BEGIN;

-- ---------------------------------------------------------------------------
-- 一、辅助函数
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.safe_parse_jsonb(raw_text text)
RETURNS jsonb
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

CREATE OR REPLACE FUNCTION public.safe_parse_jsonb(raw_json json)
RETURNS jsonb
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

CREATE OR REPLACE FUNCTION public.safe_parse_jsonb(raw_json jsonb)
RETURNS jsonb
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

CREATE OR REPLACE FUNCTION public.filter_stage_json_map(raw_json jsonb, wanted_stage_key text)
RETURNS jsonb
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

CREATE OR REPLACE FUNCTION public.bump_stage_instance_version()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := CURRENT_TIMESTAMP;
  NEW.row_version := COALESCE(OLD.row_version, 0) + 1;
  RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- 二、阶段定义表
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."08项目阶段定义表" (
  id bigserial PRIMARY KEY,
  project_type text NOT NULL,
  stage_key text NOT NULL,
  stage_name text NOT NULL,
  stage_scope text NOT NULL CHECK (stage_scope IN ('sync', 'site')),
  stage_order integer NOT NULL,
  is_active boolean NOT NULL DEFAULT TRUE,
  supports_sample boolean NOT NULL DEFAULT FALSE,
  stage_config jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uq_stage_def UNIQUE (project_type, stage_key)
);

CREATE INDEX IF NOT EXISTS idx_stage_def_project_type
  ON public."08项目阶段定义表" (project_type, stage_scope, stage_order);

INSERT INTO public."08项目阶段定义表"
  (project_type, stage_key, stage_name, stage_scope, stage_order, supports_sample, stage_config)
VALUES
  ('注册', 'S01_需求与背景调研', '需求与背景调研', 'sync', 1, FALSE, '{"work_choices":["背景调研与文档输出"]}'),
  ('注册', 'S02_方案设计审核', '方案设计审核', 'sync', 2, FALSE, '{"work_choices":["方案输出","方案评审","药监局咨询"]}'),
  ('注册', 'S03_医院筛选与专家对接', '医院筛选与专家对接', 'sync', 3, FALSE, '{"work_choices":["医院沟通协调"]}'),
  ('注册', 'S04_医院立项资料输出与递交', '医院立项资料输出与递交', 'site', 4, FALSE, '{"work_choices":["立项资料输出","立项资料递交"]}'),
  ('注册', 'S05_伦理审批与启动会', '伦理审批与启动会', 'site', 5, FALSE, '{"work_choices":["主持启动会","跟进伦理进度"]}'),
  ('注册', 'S06_人员与物资准备', '人员与物资准备', 'site', 6, FALSE, '{"work_choices":["试验物料准备"]}'),
  ('注册', 'S07_试验开展与数据汇总表管理', '试验开展与数据汇总表管理', 'site', 7, FALSE, '{"work_choices":["样本筛选入组","样本检测","数据汇总表输出","数据汇总表评审","数据溯源"]}'),
  ('注册', 'S08_小结输出与定稿', '小结输出与定稿', 'site', 8, FALSE, '{"work_choices":["小结输出","小结评审"]}'),
  ('注册', 'S10_总报告输出与定稿', '总报告输出与定稿', 'sync', 10, FALSE, '{"work_choices":["总报告输出","总报告评审"]}'),
  ('注册', 'S11_资料递交与结题归档', '资料递交与结题归档', 'sync', 11, FALSE, '{"work_choices":["结题资料递交"]}'),
  ('注册', 'S12_临床试验发补与资料递交', '临床试验发补与资料递交', 'sync', 12, FALSE, '{"work_choices":["发补问题沟通","发补资料输出","发布试验执行"]}'),

  ('验证', 'S02_方案设计审核', '方案设计审核', 'sync', 2, FALSE, '{"work_choices":["方案输出","方案评审","药监局咨询"]}'),
  ('验证', 'S03_医院筛选与专家对接', '医院筛选与专家对接', 'sync', 3, FALSE, '{"work_choices":["医院沟通协调"]}'),
  ('验证', 'S09_验证试验开展与数据管理', '验证试验开展与数据管理', 'sync', 9, TRUE, '{"work_choices":["样本检测"]}'),
  ('验证', 'S10_总报告输出与定稿', '总报告输出与定稿', 'sync', 10, FALSE, '{"work_choices":["总报告输出","总报告评审"]}'),

  ('文章', 'S13_文章初稿输出', '文章初稿输出', 'sync', 13, FALSE, '{"work_choices":["初稿输出"]}'),
  ('文章', 'S14_文章内部审评、修改、投递', '文章内部审评、修改、投递', 'sync', 14, FALSE, '{"work_choices":["文章审评","文章修改"]}'),
  ('文章', 'S15_意见反馈与文章返修', '意见反馈与文章返修', 'sync', 15, FALSE, '{"work_choices":["审稿人沟通与文章修改"]}'),

  ('课题', 'S01_需求与背景调研', '需求与背景调研', 'sync', 1, FALSE, '{"work_choices":["背景调研与文档输出"]}'),
  ('课题', 'S02_方案设计审核', '方案设计审核', 'sync', 2, FALSE, '{"work_choices":["方案输出","方案评审","药监局咨询"]}'),
  ('课题', 'S03_医院筛选与专家对接', '医院筛选与专家对接', 'sync', 3, FALSE, '{"work_choices":["医院沟通协调"]}'),
  ('课题', 'S04_医院立项资料输出与递交', '医院立项资料输出与递交', 'site', 4, FALSE, '{"work_choices":["立项资料输出","立项资料递交"]}'),
  ('课题', 'S05_伦理审批与启动会', '伦理审批与启动会', 'site', 5, FALSE, '{"work_choices":["主持启动会","跟进伦理进度"]}'),
  ('课题', 'S06_人员与物资准备', '人员与物资准备', 'site', 6, FALSE, '{"work_choices":["试验物料准备"]}'),
  ('课题', 'S07_试验开展与数据汇总表管理', '试验开展与数据汇总表管理', 'site', 7, FALSE, '{"work_choices":["样本筛选入组","样本检测","数据汇总表输出","数据汇总表评审","数据溯源"]}'),
  ('课题', 'S12_临床试验发补与资料递交', '临床试验发补与资料递交', 'sync', 12, FALSE, '{"work_choices":["发补问题沟通","发补资料输出","发布试验执行"]}'),
  ('课题', 'S13_文章初稿输出', '文章初稿输出', 'sync', 13, FALSE, '{"work_choices":["初稿输出"]}'),
  ('课题', 'S14_文章内部审评、修改、投递', '文章内部审评、修改、投递', 'sync', 14, FALSE, '{"work_choices":["文章审评","文章修改"]}'),
  ('课题', 'S15_意见反馈与文章返修', '意见反馈与文章返修', 'sync', 15, FALSE, '{"work_choices":["审稿人沟通与文章修改"]}')
ON CONFLICT (project_type, stage_key) DO UPDATE
SET stage_name = EXCLUDED.stage_name,
    stage_scope = EXCLUDED.stage_scope,
    stage_order = EXCLUDED.stage_order,
    supports_sample = EXCLUDED.supports_sample,
    stage_config = EXCLUDED.stage_config,
    is_active = TRUE,
    updated_at = CURRENT_TIMESTAMP;

CREATE TABLE IF NOT EXISTS public."09项目阶段实例表" (
  id bigserial PRIMARY KEY,
  project_id integer NOT NULL REFERENCES public."04项目总表"(id) ON DELETE CASCADE,
  scope_row_id integer NOT NULL DEFAULT 0,
  site_project_id integer NULL REFERENCES public."03医院_项目表"(id) ON DELETE CASCADE,
  stage_def_id bigint NOT NULL REFERENCES public."08项目阶段定义表"(id) ON DELETE RESTRICT,
  stage_key text NOT NULL,
  stage_scope text NOT NULL CHECK (stage_scope IN ('sync', 'site')),
  start_date date,
  planned_end_date date,
  actual_end_date date,
  progress integer NOT NULL DEFAULT 0 CHECK (progress >= 0 AND progress <= 100),
  remark_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  contributors_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  milestones_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  sample_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  extra_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  row_version integer NOT NULL DEFAULT 1,
  created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by_work_id text,
  updated_by_name text,
  CONSTRAINT ck_stage_scope_row
    CHECK (
      (stage_scope = 'sync' AND site_project_id IS NULL AND scope_row_id = 0) OR
      (stage_scope = 'site' AND site_project_id IS NOT NULL AND scope_row_id = site_project_id)
    ),
  CONSTRAINT uq_stage_instance UNIQUE (project_id, stage_def_id, scope_row_id)
);

CREATE INDEX IF NOT EXISTS idx_stage_instance_project
  ON public."09项目阶段实例表" (project_id, stage_scope);

CREATE INDEX IF NOT EXISTS idx_stage_instance_site
  ON public."09项目阶段实例表" (site_project_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_bump_stage_instance_version'
  ) THEN
    CREATE TRIGGER trg_bump_stage_instance_version
    BEFORE UPDATE ON public."09项目阶段实例表"
    FOR EACH ROW
    EXECUTE PROCEDURE public.bump_stage_instance_version();
  END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- 三、从旧列式结构回填到新阶段实例表
-- ---------------------------------------------------------------------------

INSERT INTO public."09项目阶段实例表" (
  project_id,
  scope_row_id,
  site_project_id,
  stage_def_id,
  stage_key,
  stage_scope,
  start_date,
  planned_end_date,
  actual_end_date,
  progress,
  remark_json,
  contributors_json,
  milestones_json,
  sample_json
)
SELECT
  p.id AS project_id,
  0 AS scope_row_id,
  NULL AS site_project_id,
  d.id AS stage_def_id,
  d.stage_key,
  'sync' AS stage_scope,
  CASE d.stage_key
    WHEN 'S01_需求与背景调研' THEN p."S01_Start_需求与背景调研_开始时间"
    WHEN 'S02_方案设计审核' THEN p."S02_Start_方案设计审核_开始时间"
    WHEN 'S03_医院筛选与专家对接' THEN p."S03_Start_医院筛选与专家对接_开始时间"
    WHEN 'S09_验证试验开展与数据管理' THEN p."S09_Start_验证试验开展与数据管理_开始时间"
    WHEN 'S10_总报告输出与定稿' THEN p."S10_Start_总报告输出与定稿_开始时间"
    WHEN 'S11_资料递交与结题归档' THEN p."S11_Start_资料递交与结题归档_开始时间"
    WHEN 'S12_临床试验发补与资料递交' THEN p."S12_Start_临床试验发补与资料递交_开始时间"
    WHEN 'S13_文章初稿输出' THEN p."S13_Start_文章初稿输出_开始时间"
    WHEN 'S14_文章内部审评、修改、投递' THEN p."S14_Start_文章内部审评、修改、投递_开始时间"
    WHEN 'S15_意见反馈与文章返修' THEN p."S15_Start_意见反馈与文章返修_开始时间"
    ELSE NULL
  END AS start_date,
  CASE d.stage_key
    WHEN 'S01_需求与背景调研' THEN p."S01_Plan_需求与背景调研_计划完成时间"
    WHEN 'S02_方案设计审核' THEN p."S02_Plan_方案设计审核_计划完成时间"
    WHEN 'S03_医院筛选与专家对接' THEN p."S03_Plan_医院筛选与专家对接_计划完成时间"
    WHEN 'S09_验证试验开展与数据管理' THEN p."S09_Plan_验证试验开展与数据管理_计划完成时间"
    WHEN 'S10_总报告输出与定稿' THEN p."S10_Plan_总报告输出与定稿_计划完成时间"
    WHEN 'S11_资料递交与结题归档' THEN p."S11_Plan_资料递交与结题归档_计划完成时间"
    WHEN 'S12_临床试验发补与资料递交' THEN p."S12_Plan_临床试验发补与资料递交_计划完成时间"
    WHEN 'S13_文章初稿输出' THEN p."S13_Plan_文章初稿输出_计划完成时间"
    WHEN 'S14_文章内部审评、修改、投递' THEN p."S14_Plan_文章内部审评、修改、投递_计划完成时"
    WHEN 'S15_意见反馈与文章返修' THEN p."S15_Plan_意见反馈与文章返修_计划完成时间"
    ELSE NULL
  END AS planned_end_date,
  CASE d.stage_key
    WHEN 'S01_需求与背景调研' THEN p."S01_Act_需求与背景调研_实际完成时间"
    WHEN 'S02_方案设计审核' THEN p."S02_Act_方案设计审核_实际完成时间"
    WHEN 'S03_医院筛选与专家对接' THEN p."S03_Act_医院筛选与专家对接_实际完成时间"
    WHEN 'S09_验证试验开展与数据管理' THEN p."S09_Act_验证试验开展与数据管理_实际完成时间"
    WHEN 'S10_总报告输出与定稿' THEN p."S10_Act_总报告输出与定稿_实际完成时间"
    WHEN 'S11_资料递交与结题归档' THEN p."S11_Act_资料递交与结题归档_实际完成时间"
    WHEN 'S12_临床试验发补与资料递交' THEN p."S12_Act_临床试验发补与资料递交_实际完成时间"
    WHEN 'S13_文章初稿输出' THEN p."S13_Act_文章初稿输出_实际完成时间"
    WHEN 'S14_文章内部审评、修改、投递' THEN p."S14_Act_文章内部审评、修改、投递_实际完成时间"
    WHEN 'S15_意见反馈与文章返修' THEN p."S15_Act_意见反馈与文章返修_实际完成时间"
    ELSE NULL
  END AS actual_end_date,
  COALESCE(
    CASE d.stage_key
      WHEN 'S01_需求与背景调研' THEN p."S01_Progress_需求与背景调研_当前进度"
      WHEN 'S02_方案设计审核' THEN p."S02_Progress_方案设计审核_当前进度"
      WHEN 'S03_医院筛选与专家对接' THEN p."S03_Progress_医院筛选与专家对接_当前进度"
      WHEN 'S09_验证试验开展与数据管理' THEN p."S09_Progress_验证试验开展与数据管理_当前进度"
      WHEN 'S10_总报告输出与定稿' THEN p."S10_Progress_总报告输出与定稿_当前进度"
      WHEN 'S11_资料递交与结题归档' THEN p."S11_Progress_资料递交与结题归档_当前进度"
      WHEN 'S12_临床试验发补与资料递交' THEN p."S12_Progress_临床试验发补与资料递交_当前进度"
      WHEN 'S13_文章初稿输出' THEN p."S13_Progress_文章初稿输出_当前进度"
      WHEN 'S14_文章内部审评、修改、投递' THEN p."S14_Progress_文章内部审评、修改、投递_当前进度"
      WHEN 'S15_意见反馈与文章返修' THEN p."S15_Progress_意见反馈与文章返修_当前进度"
      ELSE 0
    END,
    0
  ) AS progress,
  COALESCE(
    public.safe_parse_jsonb(
      CASE d.stage_key
        WHEN 'S01_需求与背景调研' THEN p."S01_Note_需求与背景调研_备注信息"
        WHEN 'S02_方案设计审核' THEN p."S02_Note_方案设计审核_备注信息"
        WHEN 'S03_医院筛选与专家对接' THEN p."S03_Note_医院筛选与专家对接_备注信息"
        WHEN 'S09_验证试验开展与数据管理' THEN p."S09_Note_验证试验开展与数据管理_备注信息"
        WHEN 'S10_总报告输出与定稿' THEN p."S10_Note_总报告输出与定稿_备注信息"
        WHEN 'S11_资料递交与结题归档' THEN p."S11_Note_资料递交与结题归档_备注信息"
        WHEN 'S12_临床试验发补与资料递交' THEN p."S12_Note_临床试验发补与资料递交_备注信息"
        WHEN 'S13_文章初稿输出' THEN p."S13_Note_文章初稿输出_备注信息"
        WHEN 'S14_文章内部审评、修改、投递' THEN p."S14_Note_文章内部审评_修改_投递_备注信息"
        WHEN 'S15_意见反馈与文章返修' THEN p."S15_Note_意见反馈与文章返修_备注信息"
        ELSE NULL
      END
    ),
    '{}'::jsonb
  ) AS remark_json,
  COALESCE(
    public.safe_parse_jsonb(
      CASE d.stage_key
        WHEN 'S01_需求与背景调研' THEN p."S01_Contributors_需求与背景调研_进度贡献者"::text
        WHEN 'S02_方案设计审核' THEN p."S02_Contributors_方案设计审核_进度贡献者"::text
        WHEN 'S03_医院筛选与专家对接' THEN p."S03_Contributors_医院筛选与专家对接_进度贡献者"::text
        WHEN 'S09_验证试验开展与数据管理' THEN p."S09_Contributors_验证试验开展与数据管理_进度贡献"::text
        WHEN 'S10_总报告输出与定稿' THEN p."S10_Contributors_总报告输出与定稿_进度贡献者"::text
        WHEN 'S11_资料递交与结题归档' THEN p."S11_Contributors_资料递交与结题归档_进度贡献者"::text
        WHEN 'S12_临床试验发补与资料递交' THEN p."S12_Contributors_临床试验发补与资料递交_进度贡献"::text
        WHEN 'S13_文章初稿输出' THEN p."S13_Contributors_文章初稿输出_进度贡献者"::text
        WHEN 'S14_文章内部审评、修改、投递' THEN p."S14_Contributors_文章内部审评_修改_投递_进度贡献"::text
        WHEN 'S15_意见反馈与文章返修' THEN p."S15_Contributors_意见反馈与文章返修_进度贡献者"::text
        ELSE NULL
      END
    ),
    '{}'::jsonb
  ) AS contributors_json,
  COALESCE(
    public.filter_stage_json_map(public.safe_parse_jsonb(p."里程碑"::text), d.stage_key),
    '{}'::jsonb
  ) AS milestones_json,
  COALESCE(
    CASE
      WHEN d.stage_key = 'S09_验证试验开展与数据管理'
        THEN public.safe_parse_jsonb(p."S09_Sample_验证试验开展与数据管理_样本来源与数"::text)
      ELSE '{}'::jsonb
    END,
    '{}'::jsonb
  ) AS sample_json
FROM public."04项目总表" p
JOIN public."08项目阶段定义表" d
  ON d.project_type = p."项目类型"
 AND d.stage_scope = 'sync'
 AND d.is_active = TRUE
ON CONFLICT (project_id, stage_def_id, scope_row_id) DO UPDATE
SET stage_key = EXCLUDED.stage_key,
    stage_scope = EXCLUDED.stage_scope,
    start_date = EXCLUDED.start_date,
    planned_end_date = EXCLUDED.planned_end_date,
    actual_end_date = EXCLUDED.actual_end_date,
    progress = EXCLUDED.progress,
    remark_json = EXCLUDED.remark_json,
    contributors_json = EXCLUDED.contributors_json,
    milestones_json = EXCLUDED.milestones_json,
    sample_json = EXCLUDED.sample_json,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO public."09项目阶段实例表" (
  project_id,
  scope_row_id,
  site_project_id,
  stage_def_id,
  stage_key,
  stage_scope,
  start_date,
  planned_end_date,
  actual_end_date,
  progress,
  remark_json,
  contributors_json,
  milestones_json,
  sample_json
)
SELECT
  p.id AS project_id,
  s.id AS scope_row_id,
  s.id AS site_project_id,
  d.id AS stage_def_id,
  d.stage_key,
  'site' AS stage_scope,
  CASE d.stage_key
    WHEN 'S04_医院立项资料输出与递交' THEN s."S04_Start_医院立项资料输出与递交_开始时间"
    WHEN 'S05_伦理审批与启动会' THEN s."S05_Start_伦理审批与启动会_开始时间"
    WHEN 'S06_人员与物资准备' THEN s."S06_Start_人员与物资准备_开始时间"
    WHEN 'S07_试验开展与数据汇总表管理' THEN s."S07_Start_试验开展与数据汇总表管理_开始时间"
    WHEN 'S08_小结输出与定稿' THEN s."S08_Start_小结输出与定稿_开始时间"
    ELSE NULL
  END AS start_date,
  CASE d.stage_key
    WHEN 'S04_医院立项资料输出与递交' THEN s."S04_Plan_医院立项资料输出与递交_计划完成时间"
    WHEN 'S05_伦理审批与启动会' THEN s."S05_Plan_伦理审批与启动会_计划完成时间"
    WHEN 'S06_人员与物资准备' THEN s."S06_Plan_人员与物资准备_计划完成时间"
    WHEN 'S07_试验开展与数据汇总表管理' THEN s."S07_Plan_试验开展与数据汇总表管理_计划完成时"
    WHEN 'S08_小结输出与定稿' THEN s."S08_Plan_小结输出与定稿_计划完成时间"
    ELSE NULL
  END AS planned_end_date,
  CASE d.stage_key
    WHEN 'S04_医院立项资料输出与递交' THEN s."S04_Act_医院立项资料输出与递交_实际完成时间"
    WHEN 'S05_伦理审批与启动会' THEN s."S05_Act_伦理审批与启动会_实际完成时间"
    WHEN 'S06_人员与物资准备' THEN s."S06_Act_人员与物资准备_实际完成时间"
    WHEN 'S07_试验开展与数据汇总表管理' THEN s."S07_Act_试验开展与数据汇总表管理_实际完成时间"
    WHEN 'S08_小结输出与定稿' THEN s."S08_Act_小结输出与定稿_实际完成时间"
    ELSE NULL
  END AS actual_end_date,
  COALESCE(
    CASE d.stage_key
      WHEN 'S04_医院立项资料输出与递交' THEN s."S04_Progress_医院立项资料输出与递交_当前进度"
      WHEN 'S05_伦理审批与启动会' THEN s."S05_Progress_伦理审批与启动会_当前进度"
      WHEN 'S06_人员与物资准备' THEN s."S06_Progress_人员与物资准备_当前进度"
      WHEN 'S07_试验开展与数据汇总表管理' THEN s."S07_Progress_试验开展与数据汇总表管理_当前进度"
      WHEN 'S08_小结输出与定稿' THEN s."S08_Progress_小结输出与定稿_当前进度"
      ELSE 0
    END,
    0
  ) AS progress,
  COALESCE(
    public.safe_parse_jsonb(
      CASE d.stage_key
        WHEN 'S04_医院立项资料输出与递交' THEN s."S04_Note_医院立项资料输出与递交_备注信息"
        WHEN 'S05_伦理审批与启动会' THEN s."S05_Note_伦理审批与启动会_备注信息"
        WHEN 'S06_人员与物资准备' THEN s."S06_Note_人员与物资准备_备注信息"
        WHEN 'S07_试验开展与数据汇总表管理' THEN s."S07_Note_试验开展与数据汇总表管理_备注信息"
        WHEN 'S08_小结输出与定稿' THEN s."S08_Note_小结输出与定稿_备注信息"
        ELSE NULL
      END
    ),
    '{}'::jsonb
  ) AS remark_json,
  COALESCE(
    public.safe_parse_jsonb(
      CASE d.stage_key
        WHEN 'S04_医院立项资料输出与递交' THEN s."S04_Contributors_医院立项资料输出与递交_进度贡献"::text
        WHEN 'S05_伦理审批与启动会' THEN s."S05_Contributors_伦理审批与启动会_进度贡献者"::text
        WHEN 'S06_人员与物资准备' THEN s."S06_Contributors_人员与物资准备_进度贡献者"::text
        WHEN 'S07_试验开展与数据汇总表管理' THEN s."S07_Contributors_试验开展与数据汇总表管理_进度贡"::text
        WHEN 'S08_小结输出与定稿' THEN s."S08_Contributors_小结输出与定稿_进度贡献者"::text
        ELSE NULL
      END
    ),
    '{}'::jsonb
  ) AS contributors_json,
  COALESCE(
    public.filter_stage_json_map(public.safe_parse_jsonb(s."里程碑"::text), d.stage_key),
    '{}'::jsonb
  ) AS milestones_json,
  '{}'::jsonb AS sample_json
FROM public."03医院_项目表" s
JOIN public."04项目总表" p
  ON p.id = s."project_table 项目总表_id"
JOIN public."08项目阶段定义表" d
  ON d.project_type = p."项目类型"
 AND d.stage_scope = 'site'
 AND d.is_active = TRUE
ON CONFLICT (project_id, stage_def_id, scope_row_id) DO UPDATE
SET stage_key = EXCLUDED.stage_key,
    stage_scope = EXCLUDED.stage_scope,
    site_project_id = EXCLUDED.site_project_id,
    start_date = EXCLUDED.start_date,
    planned_end_date = EXCLUDED.planned_end_date,
    actual_end_date = EXCLUDED.actual_end_date,
    progress = EXCLUDED.progress,
    remark_json = EXCLUDED.remark_json,
    contributors_json = EXCLUDED.contributors_json,
    milestones_json = EXCLUDED.milestones_json,
    sample_json = EXCLUDED.sample_json,
    updated_at = CURRENT_TIMESTAMP;

-- ---------------------------------------------------------------------------
-- 四、统一查询视图
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW public."v_项目阶段甘特视图" AS
SELECT
  si.id AS stage_instance_id,
  si.project_id AS project_db_id,
  COALESCE(NULLIF(p."项目名称", ''), '项目-' || p.id::text) AS project_id,
  p."项目类型" AS project_type,
  p."重要紧急程度" AS "重要紧急程度",
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
JOIN public."08项目阶段定义表" d
  ON d.id = si.stage_def_id
JOIN public."04项目总表" p
  ON p.id = si.project_id
LEFT JOIN public."03医院_项目表" s
  ON s.id = si.site_project_id
LEFT JOIN public."01医院信息表" h
  ON h.id = s."01_hos_resource_table医院信息表_id"
LEFT JOIN public."05人员表" mgr
  ON mgr.id = p."05人员表_id"
WHERE d.is_active = TRUE;

COMMIT;
