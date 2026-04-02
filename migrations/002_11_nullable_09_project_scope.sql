-- =============================================================================
-- 允许 11 阶段问题反馈表 中「不挂 09 阶段实例」的项目级要点（09 为 NULL，仅 04 项目）
-- 并拆分为两个部分唯一索引（有 09：09+条目键；无 09：04+条目键）
-- 依赖：已执行 001_11_feedback_12_meeting_link.sql
-- psql: \i migrations/002_11_nullable_09_project_scope.sql
-- =============================================================================

BEGIN;

ALTER TABLE public."11阶段问题反馈表" DROP CONSTRAINT IF EXISTS uq_11_stage_entry;

ALTER TABLE public."11阶段问题反馈表"
  ALTER COLUMN "09项目阶段实例表_id" DROP NOT NULL;

-- 有阶段实例时：与原语义一致
CREATE UNIQUE INDEX IF NOT EXISTS uq_11_stage_entry_when_09
  ON public."11阶段问题反馈表" ("09项目阶段实例表_id", "条目键")
  WHERE ("09项目阶段实例表_id" IS NOT NULL);

-- 项目级要点（无 09）：按项目 + 条目键 唯一
CREATE UNIQUE INDEX IF NOT EXISTS uq_11_project_entry_no_09
  ON public."11阶段问题反馈表" ("04项目总表_id", "条目键")
  WHERE ("09项目阶段实例表_id" IS NULL);

COMMENT ON COLUMN public."11阶段问题反馈表"."09项目阶段实例表_id" IS 'NULL 表示项目级要点（不分中心/阶段），仅通过 04项目总表_id 归属项目';

COMMIT;
