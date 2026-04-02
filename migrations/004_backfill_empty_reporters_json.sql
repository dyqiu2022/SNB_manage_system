-- =============================================================================
-- 已执行过 001 后：回补仍为空的 11.reporters_json（不依赖 09.remark_json）
--
-- 背景：历史 remark_json 里常见 "reporters":[]，真实反馈人编码在条目键
-- 「姓名_yyyyMMddHHmmss」中（与 app.R 中 is_legacy_name_time_key / parse_entry_key_label 一致）。
-- 001 早期版本照抄空数组，导致 11 表全是 []。
--
-- 本脚本：
--   1) 条目键匹配 _ + 14 位数字结尾时，用去掉后缀部分作为反馈人写入 reporters_json
--   2) 仍为空且 updated_by_name 非空时，用 updated_by_name 作为单元素数组
--
-- 可重复执行；只更新当前 reporters_json 为空（或仅空串）的行。
-- psql: \i migrations/004_backfill_empty_reporters_json.sql
-- =============================================================================

BEGIN;

UPDATE public."11阶段问题反馈表" f
SET reporters_json = to_jsonb(ARRAY[kname])
FROM (
  SELECT
    id,
    trim(regexp_replace("条目键", '_([0-9]{14})$', '')) AS kname
  FROM public."11阶段问题反馈表"
  WHERE "条目键" ~ '_[0-9]{14}$'
) s
WHERE f.id = s.id
  AND length(s.kname) > 0
  AND (
    f.reporters_json IS NULL
    OR f.reporters_json = '[]'::jsonb
    OR f.reporters_json = '[""]'::jsonb
    OR f.reporters_json = 'null'::jsonb
  );

UPDATE public."11阶段问题反馈表"
SET reporters_json = to_jsonb(ARRAY[trim(updated_by_name)])
WHERE (reporters_json IS NULL OR reporters_json = '[]'::jsonb OR reporters_json = '[""]'::jsonb OR reporters_json = 'null'::jsonb)
  AND updated_by_name IS NOT NULL
  AND length(trim(updated_by_name)) > 0;

COMMIT;
