-- =============================================================================
-- 1) 将 09.remark_json 迁入 11 阶段问题反馈表
-- 2) 建立 12 会议决策 ↔ 11 关联
-- 3) 原 10.04项目总表_id 的语义由「12→11→04」承担：迁移占位 11 + 12 后删除 10 上该列及外键
--
-- 执行前请备份。psql: \i migrations/001_11_feedback_12_meeting_link.sql
-- 可重复执行：占位回填仅在 10 仍存在 04项目总表_id 时执行；删列使用 IF EXISTS
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. 新表：11 阶段问题反馈
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public."11阶段问题反馈表" (
  id BIGSERIAL PRIMARY KEY,
  "09项目阶段实例表_id" BIGINT NOT NULL REFERENCES public."09项目阶段实例表"(id) ON DELETE CASCADE,
  "04项目总表_id" INTEGER NOT NULL,
  "条目键" TEXT NOT NULL,
  entry_key_legacy TEXT,
  "类型" TEXT NOT NULL DEFAULT '',
  "内容" TEXT NOT NULL DEFAULT '',
  reporters_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  "更新日期" TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by_work_id TEXT,
  updated_by_name TEXT,
  CONSTRAINT uq_11_stage_entry UNIQUE ("09项目阶段实例表_id", "条目键")
);

CREATE INDEX IF NOT EXISTS idx_11_feedback_project ON public."11阶段问题反馈表" ("04项目总表_id");
CREATE INDEX IF NOT EXISTS idx_11_feedback_09 ON public."11阶段问题反馈表" ("09项目阶段实例表_id");

COMMENT ON TABLE public."11阶段问题反馈表" IS '问题/卡点/经验分享（由 09.remark_json 迁出；应用侧主数据源）';

-- -----------------------------------------------------------------------------
-- 2. 新表：12 会议决策 ↔ 11
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public."12会议决策关联问题表" (
  id BIGSERIAL PRIMARY KEY,
  "10会议决策表_id" INTEGER NOT NULL REFERENCES public."10会议决策表"(id) ON DELETE CASCADE,
  "11阶段问题反馈表_id" BIGINT NOT NULL REFERENCES public."11阶段问题反馈表"(id) ON DELETE CASCADE,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uq_12_decision_issue UNIQUE ("10会议决策表_id", "11阶段问题反馈表_id")
);

CREATE INDEX IF NOT EXISTS idx_12_10 ON public."12会议决策关联问题表" ("10会议决策表_id");
CREATE INDEX IF NOT EXISTS idx_12_11 ON public."12会议决策关联问题表" ("11阶段问题反馈表_id");

COMMENT ON TABLE public."12会议决策关联问题表" IS '会议决策记录与 11 反馈条目的关联（项目维度经 11→04 解析）';

-- -----------------------------------------------------------------------------
-- 3. 数据迁移：remark_json → 11
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  r RECORD;
  kv RECORD;
  v jsonb;
  typ text;
  cont text;
  upd text;
  rj jsonb;
  raw text;
  parts text[];
  ek text;
BEGIN
  FOR r IN
    SELECT si.id AS sid, si.project_id AS pid, si.remark_json AS rj
    FROM public."09项目阶段实例表" si
    WHERE si.remark_json IS NOT NULL
      AND si.remark_json <> '{}'::jsonb
      AND jsonb_typeof(si.remark_json) = 'object'
  LOOP
    FOR kv IN SELECT * FROM jsonb_each(r.rj)
    LOOP
      ek := kv.key;
      v := kv.value;
      typ := '';
      cont := '';
      upd := '';
      rj := '[]'::jsonb;

      IF jsonb_typeof(v) = 'object' THEN
        typ := COALESCE(NULLIF(trim(v ->> 'type'), ''), '问题/卡点/经验分享');
        cont := COALESCE(v ->> 'content', '');
        upd := COALESCE(NULLIF(trim(v ->> 'updated_at'), ''), NULLIF(trim(v ->> '更新日期'), ''), '');
        IF (v ? 'reporters') AND jsonb_typeof(v -> 'reporters') = 'array' THEN
          rj := v -> 'reporters';
        ELSIF (v ? 'reporter') THEN
          rj := to_jsonb(ARRAY[trim(COALESCE(v ->> 'reporter', ''))]);
          IF rj = 'null'::jsonb OR rj = '[""]'::jsonb THEN
            rj := '[]'::jsonb;
          END IF;
        END IF;
      ELSIF jsonb_typeof(v) = 'string' THEN
        raw := v #>> '{}';
        IF raw IS NULL THEN
          raw := '';
        END IF;
        parts := string_to_array(raw, '|');
        IF array_length(parts, 1) IS NULL THEN
          parts := ARRAY[''];
        END IF;
        typ := COALESCE(NULLIF(trim(
          CASE WHEN array_length(parts, 1) >= 2 THEN parts[2] ELSE '' END
        ), ''), '问题/卡点/经验分享');
        cont := CASE
          WHEN array_length(parts, 1) > 2 THEN array_to_string(parts[3:array_length(parts, 1)], '|', '')
          ELSE ''
        END;
        upd := '';
        IF array_length(parts, 1) >= 1 AND NULLIF(trim(parts[1]), '') IS NOT NULL THEN
          rj := to_jsonb(ARRAY[trim(parts[1])]);
        END IF;
      ELSE
        typ := '问题/卡点/经验分享';
        cont := COALESCE(trim(v::text), '');
        upd := '';
      END IF;

      IF NULLIF(trim(cont), '') IS NULL THEN
        CONTINUE;
      END IF;

      INSERT INTO public."11阶段问题反馈表" (
        "09项目阶段实例表_id",
        "04项目总表_id",
        "条目键",
        entry_key_legacy,
        "类型",
        "内容",
        reporters_json,
        "更新日期"
      ) VALUES (
        r.sid,
        r.pid,
        ek,
        ek,
        typ,
        cont,
        COALESCE(rj, '[]'::jsonb),
        COALESCE(upd, '')
      )
      ON CONFLICT ("09项目阶段实例表_id", "条目键") DO NOTHING;
    END LOOP;
  END LOOP;
END $$;

-- -----------------------------------------------------------------------------
-- 4. 清空 09.remark_json（数据已在 11）
-- -----------------------------------------------------------------------------
UPDATE public."09项目阶段实例表"
SET remark_json = '{}'::jsonb
WHERE remark_json IS NOT NULL AND remark_json <> '{}'::jsonb;

-- -----------------------------------------------------------------------------
-- 5. 回填：原 10.04项目总表_id 非空且无 12 关联时，插入占位 11 + 12（保留「会议→项目」语义）
--     若该项目下无任何 09 行，则无法建 11，仅 RAISE NOTICE（需业务侧补数据后手工关联）
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  col_exists boolean;
  r RECORD;
  sid bigint;
  ek text;
  fid bigint;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
      AND c.table_name = '10会议决策表'
      AND c.column_name = '04项目总表_id'
  ) INTO col_exists;

  IF NOT col_exists THEN
    RETURN;
  END IF;

  FOR r IN
    SELECT t.id AS tid, t."04项目总表_id" AS pid
    FROM public."10会议决策表" t
    WHERE t."04项目总表_id" IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public."12会议决策关联问题表" l
        WHERE l."10会议决策表_id" = t.id
      )
  LOOP
    SELECT si.id INTO sid
    FROM public."09项目阶段实例表" si
    WHERE si.project_id = r.pid
    ORDER BY si.id
    LIMIT 1;

    IF sid IS NULL THEN
      RAISE NOTICE '会议决策 id=% 的项目 % 下无 09 阶段实例，跳过占位回填', r.tid, r.pid;
      CONTINUE;
    END IF;

    ek := '__mig_placeholder_mtg_' || r.tid::text;

    INSERT INTO public."11阶段问题反馈表" (
      "09项目阶段实例表_id",
      "04项目总表_id",
      "条目键",
      entry_key_legacy,
      "类型",
      "内容",
      reporters_json,
      "更新日期"
    ) VALUES (
      sid,
      r.pid,
      ek,
      ek,
      '系统占位',
      '(迁移生成：保留本条会议决策与原项目的关联；可在甘特中改关联真实反馈后删除本占位条目)',
      '[]'::jsonb,
      ''
    )
    ON CONFLICT ("09项目阶段实例表_id", "条目键") DO NOTHING;

    SELECT f.id INTO fid
    FROM public."11阶段问题反馈表" f
    WHERE f."09项目阶段实例表_id" = sid
      AND f."条目键" = ek
    LIMIT 1;

    IF fid IS NULL THEN
      CONTINUE;
    END IF;

    INSERT INTO public."12会议决策关联问题表" ("10会议决策表_id", "11阶段问题反馈表_id")
    VALUES (r.tid, fid)
    ON CONFLICT ("10会议决策表_id", "11阶段问题反馈表_id") DO NOTHING;
  END LOOP;
END $$;

-- -----------------------------------------------------------------------------
-- 6. 废弃 10 表对 04 的直接外键与列
-- -----------------------------------------------------------------------------
ALTER TABLE public."10会议决策表" DROP CONSTRAINT IF EXISTS fk_04__10__wuo_62xa64;

DROP INDEX IF EXISTS public."fk_04__10__b_9mqw4cim";

ALTER TABLE public."10会议决策表" DROP COLUMN IF EXISTS "04项目总表_id";

COMMIT;
