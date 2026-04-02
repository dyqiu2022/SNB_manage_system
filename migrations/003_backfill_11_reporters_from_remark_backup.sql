-- =============================================================================
-- 可选：用 09 表上事先备份的 remark_json 回补 11.reporters_json
--
-- 适用：已跑过 001 且当时未迁移「登记人/反馈人」等字段，但你在清空 remark_json
-- 之前执行过下面备份语句（或能从冷备份恢复出同等数据）。
--
-- 迁移前备份示例（应在执行 001 中「清空 09.remark_json」一段之前完成）：
--   ALTER TABLE public."09项目阶段实例表" ADD COLUMN IF NOT EXISTS remark_json_mig_backup jsonb;
--   UPDATE public."09项目阶段实例表"
--   SET remark_json_mig_backup = remark_json
--   WHERE remark_json IS NOT NULL AND remark_json <> '{}'::jsonb;
--
-- 本脚本：若不存在列 remark_json_mig_backup 则仅 RAISE NOTICE 并退出。
-- psql: \i migrations/003_backfill_11_reporters_from_remark_backup.sql
-- =============================================================================

BEGIN;

CREATE OR REPLACE FUNCTION public.mig_reporters_only_from_remark_json_entry(v jsonb, entry_key text DEFAULT NULL::text)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  rj jsonb := '[]'::jsonb;
  raw text;
  parts text[];
  v_try jsonb;
  kname text;
  rs text;
BEGIN
  IF v IS NULL THEN
    RETURN '[]'::jsonb;
  END IF;
  IF jsonb_typeof(v) = 'object' THEN
    IF (v ? 'reporters') AND jsonb_typeof(v -> 'reporters') = 'array' THEN
      rj := v -> 'reporters';
    ELSIF (v ? 'reporters') AND jsonb_typeof(v -> 'reporters') = 'string' THEN
      rs := trim(COALESCE(v ->> 'reporters', ''));
      IF rs = '' THEN
        rj := '[]'::jsonb;
      ELSIF substring(rs FROM 1 FOR 1) = '[' THEN
        BEGIN
          rj := rs::jsonb;
          IF jsonb_typeof(rj) <> 'array' THEN
            rj := to_jsonb(ARRAY[rs]);
          END IF;
        EXCEPTION
          WHEN OTHERS THEN
            SELECT COALESCE((
              SELECT jsonb_agg(to_jsonb(btrim(t.e)) ORDER BY t.ord)
              FROM unnest(regexp_split_to_array(rs, '[、，,]+')) WITH ORDINALITY AS t(e, ord)
              WHERE btrim(t.e) <> ''
            ), '[]'::jsonb)
            INTO rj;
        END;
      ELSE
        SELECT COALESCE((
          SELECT jsonb_agg(to_jsonb(btrim(t.e)) ORDER BY t.ord)
          FROM unnest(regexp_split_to_array(rs, '[、，,]+')) WITH ORDINALITY AS t(e, ord)
          WHERE btrim(t.e) <> ''
        ), '[]'::jsonb)
        INTO rj;
      END IF;
    ELSIF (v ? 'reporter') THEN
      rj := to_jsonb(ARRAY[trim(COALESCE(v ->> 'reporter', ''))]);
      IF rj = 'null'::jsonb OR rj = '[""]'::jsonb THEN
        rj := '[]'::jsonb;
      END IF;
    END IF;
    IF rj = '[]'::jsonb THEN
      IF (v ? '登记人') THEN
        IF jsonb_typeof(v -> '登记人') = 'array' THEN
          rj := v -> '登记人';
        ELSE
          rj := to_jsonb(ARRAY[trim(COALESCE(v ->> '登记人', ''))]);
          IF rj = 'null'::jsonb OR rj = '[""]'::jsonb THEN
            rj := '[]'::jsonb;
          END IF;
        END IF;
      ELSIF (v ? '反馈人') THEN
        IF jsonb_typeof(v -> '反馈人') = 'array' THEN
          rj := v -> '反馈人';
        ELSE
          rj := to_jsonb(ARRAY[trim(COALESCE(v ->> '反馈人', ''))]);
          IF rj = 'null'::jsonb OR rj = '[""]'::jsonb THEN
            rj := '[]'::jsonb;
          END IF;
        END IF;
      ELSIF (v ? '填报人') THEN
        IF jsonb_typeof(v -> '填报人') = 'array' THEN
          rj := v -> '填报人';
        ELSE
          rj := to_jsonb(ARRAY[trim(COALESCE(v ->> '填报人', ''))]);
          IF rj = 'null'::jsonb OR rj = '[""]'::jsonb THEN
            rj := '[]'::jsonb;
          END IF;
        END IF;
      END IF;
    END IF;
  ELSIF jsonb_typeof(v) = 'string' THEN
    raw := COALESCE(v #>> '{}', '');
    v_try := NULL;
    BEGIN
      IF btrim(raw) <> '' AND substring(btrim(raw) FROM 1 FOR 1) = '{' THEN
        v_try := raw::jsonb;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        v_try := NULL;
    END;
    IF v_try IS NOT NULL AND jsonb_typeof(v_try) = 'object' THEN
      RETURN public.mig_reporters_only_from_remark_json_entry(v_try, entry_key);
    END IF;
    parts := string_to_array(raw, '|');
    IF array_length(parts, 1) IS NULL THEN
      parts := ARRAY[''];
    END IF;
    IF array_length(parts, 1) >= 1 AND NULLIF(trim(parts[1]), '') IS NOT NULL THEN
      rj := to_jsonb(ARRAY[trim(parts[1])]);
    END IF;
  ELSE
    rj := '[]'::jsonb;
  END IF;

  IF jsonb_typeof(rj) = 'array' THEN
    SELECT COALESCE((
      SELECT jsonb_agg(to_jsonb(btrim(t.x)) ORDER BY t.ord)
      FROM jsonb_array_elements_text(rj) WITH ORDINALITY AS t(x, ord)
      WHERE btrim(t.x) <> ''
    ), '[]'::jsonb)
    INTO rj;
  END IF;

  IF (rj = '[]'::jsonb OR rj IS NULL)
     AND entry_key IS NOT NULL
     AND length(trim(entry_key)) > 0
     AND trim(entry_key) ~ '_[0-9]{14}$' THEN
    kname := trim(regexp_replace(trim(entry_key), '_([0-9]{14})$', ''));
    IF length(kname) > 0 THEN
      rj := to_jsonb(ARRAY[kname]);
    END IF;
  END IF;

  RETURN COALESCE(rj, '[]'::jsonb);
END;
$$;

DO $$
DECLARE
  col_exists boolean;
  r RECORD;
  kv RECORD;
  new_rj jsonb;
  n_upd integer := 0;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
      AND c.table_name = '09项目阶段实例表'
      AND c.column_name = 'remark_json_mig_backup'
  ) INTO col_exists;

  IF NOT col_exists THEN
    RAISE NOTICE '003: 未找到 public."09项目阶段实例表".remark_json_mig_backup，跳过（见文件头说明如何备份）';
    RETURN;
  END IF;

  FOR r IN
    SELECT si.id AS sid, si.remark_json_mig_backup AS rj
    FROM public."09项目阶段实例表" si
    WHERE si.remark_json_mig_backup IS NOT NULL
      AND si.remark_json_mig_backup <> '{}'::jsonb
      AND jsonb_typeof(si.remark_json_mig_backup) = 'object'
  LOOP
    FOR kv IN SELECT * FROM jsonb_each(r.rj)
    LOOP
      new_rj := public.mig_reporters_only_from_remark_json_entry(kv.value, kv.key);
      IF new_rj IS NULL OR new_rj = '[]'::jsonb THEN
        CONTINUE;
      END IF;
      UPDATE public."11阶段问题反馈表" f
      SET reporters_json = new_rj
      WHERE f."09项目阶段实例表_id" = r.sid
        AND f."条目键" = kv.key
        AND (f.reporters_json IS NULL OR f.reporters_json = '[]'::jsonb OR f.reporters_json = '[""]'::jsonb);
      IF FOUND THEN
        n_upd := n_upd + 1;
      END IF;
    END LOOP;
  END LOOP;

  RAISE NOTICE '003: 已尝试回补 reporters_json，更新行数（按条目计）约 %', n_upd;
END $$;

DROP FUNCTION IF EXISTS public.mig_reporters_only_from_remark_json_entry(jsonb, text);
DROP FUNCTION IF EXISTS public.mig_reporters_only_from_remark_json_entry(jsonb);

COMMIT;
