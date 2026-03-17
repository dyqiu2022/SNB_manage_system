# ==========================================
# 完整版：多中心临床试验大盘 (动态渐变色+未开始灰态修复版)
# ==========================================

library(shiny)
library(timevis)
library(dplyr)
library(tibble)
library(tidyr)
library(RPostgres)
library(DT)
library(DBI)
library(jsonlite)
library(pool)

# ---------------- 0. 数据库连接配置 ----------------
# 环境变量：调试用 localhost，Docker 内用 db
db_config <- list(
  host = Sys.getenv("PG_HOST", "localhost"),
  port = as.integer(Sys.getenv("PG_PORT", "5432")),
  dbname = Sys.getenv("PG_DBNAME", "ivd_data"),
  user = Sys.getenv("PG_USER", "myuser"),
  password = Sys.getenv("PG_PASSWORD", "mypassword")
)

# 全局连接池：所有 Shiny 会话共享，最大 10 连接
pg_pool <- dbPool(
  drv = RPostgres::Postgres(),
  host = db_config$host,
  port = db_config$port,
  dbname = db_config$dbname,
  user = db_config$user,
  password = db_config$password,
  maxSize = 10
)

onStop(function() {
  poolClose(pg_pool)
})

# ---------------- 1. 甘特图常量（与 DB 表结构对应） ----------------
today <- Sys.Date()
# 阶段全名 -> 短名（用于按项目类型过滤），已适配 S01-S15 新结构
task_short_name <- c(
  "S01_需求与背景调研" = "需求与背景调研",
  "S02_方案设计审核" = "方案设计审核",
  "S03_医院筛选与专家对接" = "医院筛选与专家对接",
  "S04_医院立项资料输出与递交" = "医院立项资料输出与递交",
  "S05_伦理审批与启动会" = "伦理审批与启动会",
  "S06_人员与物资准备" = "人员与物资准备",
  "S07_试验开展与数据汇总表管理" = "试验开展与数据汇总表管理",
  "S08_小结输出与定稿" = "小结输出与定稿",
  "S09_验证试验开展与数据管理" = "验证试验开展与数据管理",
  "S10_总报告输出与定稿" = "总报告输出与定稿",
  "S11_资料递交与结题归档" = "资料递交与结题归档",
  "S12_临床试验发补与资料递交" = "临床试验发补与资料递交",
  "S13_文章初稿输出" = "文章初稿输出",
  "S14_文章内部审评、修改、投递" = "文章内部审评、修改、投递",
  "S15_意见反馈与文章返修" = "意见反馈与文章返修"
)
# 阶段全名按 S01-S15 的固定顺序（用于 stage_definition_df 等）
default_stage_order_full <- names(task_short_name)
# 项目类型 -> 有效阶段短名（不在清单中的阶段不显示），按用户提供的新阶段清单
project_type_valid_stages <- list(
  # 注册项目：S01,S02,S03,S04,S05,S06,S07,S08,S10,S11,S12
  "注册" = c(
    "需求与背景调研",
    "方案设计审核",
    "医院筛选与专家对接",
    "医院立项资料输出与递交",
    "伦理审批与启动会",
    "人员与物资准备",
    "试验开展与数据汇总表管理",
    "小结输出与定稿",
    "总报告输出与定稿",
    "资料递交与结题归档",
    "临床试验发补与资料递交"
  ),
  # 验证项目：S02,S03,S09,S10
  "验证" = c(
    "方案设计审核",
    "医院筛选与专家对接",
    "验证试验开展与数据管理",
    "总报告输出与定稿"
  ),
  # 文章项目：S13,S14,S15
  "文章" = c(
    "文章初稿输出",
    "文章内部审评、修改、投递",
    "意见反馈与文章返修"
  ),
  # 课题项目：S01,S02,S03,S04,S05,S06,S07,S12,S13,S14,S15
  "课题" = c(
    "需求与背景调研",
    "方案设计审核",
    "医院筛选与专家对接",
    "医院立项资料输出与递交",
    "伦理审批与启动会",
    "人员与物资准备",
    "试验开展与数据汇总表管理",
    "临床试验发补与资料递交",
    "文章初稿输出",
    "文章内部审评、修改、投递",
    "意见反馈与文章返修"
  )
)
# 同步阶段（项目级）：stage_definition_df 的 fallback，当 DB 不可用时
sync_stages_db <- c(
  "S01_需求与背景调研",
  "S02_方案设计审核",
  "S03_医院筛选与专家对接",
  "S09_验证试验开展与数据管理",
  "S10_总报告输出与定稿",
  "S11_资料递交与结题归档",
  "S12_临床试验发补与资料递交",
  "S13_文章初稿输出",
  "S14_文章内部审评、修改、投递",
  "S15_意见反馈与文章返修"
)
# 分中心阶段（中心级）：stage_definition_df 的 fallback
site_stages_db <- c(
  "S04_医院立项资料输出与递交",
  "S05_伦理审批与启动会",
  "S06_人员与物资准备",
  "S07_试验开展与数据汇总表管理",
  "S08_小结输出与定稿"
)
norm_progress <- function(x) {
  if (is.null(x) || all(is.na(x))) return(0)
  x <- as.numeric(x)
  x[is.na(x)] <- 0
  if (max(x, na.rm = TRUE) > 1) x <- x / 100
  pmin(1, pmax(0, x))
}


# ---------------- 2. UI 界面 ----------------
ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      .vis-labelset { width: 240px !important; }
      .vis-current-time { width: 2px; background-color: red; z-index: 10; }
      /* 项目间分割线样式 */
      .vis-group[id*='MOCK_SEPARATOR'] {
        background-color: #E0E0E0 !important;
        min-height: 4px !important;
      }
      .vis-group[id*='MOCK_SEPARATOR'] .vis-label {
        background-color: #E0E0E0 !important;
        min-height: 4px !important;
        padding: 0 !important;
      }
      /* 按“重要紧急程度”区分行背景（浅色，不干扰进度条） */
      .vis-group.gantt-bg-重要紧急,
      .vis-group.gantt-bg-重要紧急 .vis-label { background-color: #FFEBEE !important; }      /* very light red */
      .vis-group.gantt-bg-重要不紧急,
      .vis-group.gantt-bg-重要不紧急 .vis-label { background-color: #FFF8E1 !important; }  /* very light amber */
      .vis-group.gantt-bg-紧急不重要,
      .vis-group.gantt-bg-紧急不重要 .vis-label { background-color: #E3F2FD !important; }  /* very light blue */
      .vis-group.gantt-bg-不重要不紧急,
      .vis-group.gantt-bg-不重要不紧急 .vis-label { background-color: #F5F5F5 !important; }/* very light grey */
      .vis-group.gantt-bg-default,
      .vis-group.gantt-bg-default .vis-label { background-color: #FAFAFA !important; }
    "))
  ),
  titlePanel(paste("Snibe临床 - 项目进度管理看板 (当前日期:", today, ")")),
  uiOutput("current_user_display"),
  fluidRow(
    column(
      12,
      uiOutput("gantt_filters"),
      uiOutput("gantt_db_msg"),
       HTML("
         <div style='margin-bottom: 10px; font-size: 12px; color: #555;'>
           <b>进度诊断图例：</b>
                   <span style='margin-left:15px; padding:2px 8px; background:#EDEDED; border:2px solid #BDBDBD; border-radius:3px; color:#555;'>未制定计划 (白灰)</span>
                   <span style='margin-left:15px; padding:2px 8px; background:#9E9E9E; border:2px solid #9E9E9E; border-radius:3px; color:white;'>未开始 (深灰)</span>
               <span style='margin-left:15px; padding:2px 8px; background:#F44336; border:2px solid #F44336; border-radius:3px; color:white;'>落后50%以上</span>
               <span style='margin-left:10px; padding:2px 8px; background:#FF6F00; border:2px solid #FF6F00; border-radius:3px; color:white;'>落后30%-50%</span>
               <span style='margin-left:10px; padding:2px 8px; background:#FFC107; border:2px solid #FFC107; border-radius:3px; color:white;'>落后15%-30%</span>
               <span style='margin-left:10px; padding:2px 8px; background:#FFEB3B; border:2px solid #FFEB3B; border-radius:3px; color:#333;'>落后5%-15%</span>
               <span style='margin-left:10px; padding:2px 8px; background:#CDDC39; border:2px solid #CDDC39; border-radius:3px; color:#333;'>偏差5%以内/超前10%以内</span>
               <span style='margin-left:10px; padding:2px 8px; background:#8BC34A; border:2px solid #8BC34A; border-radius:3px; color:#333;'>超前10%-25%</span>
               <span style='margin-left:10px; padding:2px 8px; background:#4CAF50; border:2px solid #4CAF50; border-radius:3px; color:white;'>超前25%以上</span>
             </div>
         "),
         timevisOutput("my_gantt", height = "650px")
    )
  )
)

# ---------------- 3. Server 逻辑 ----------------
server <- function(input, output, session) {
  
  # ----- 数据库连接与表浏览（使用全局连接池） -----
  pg_con <- pg_pool

  gantt_db_error <- reactiveVal(NULL)
  gantt_force_refresh <- reactiveVal(0L)
  task_edit_context <- reactiveVal(NULL)
  sample_row_count <- reactiveVal(0L)
  milestone_row_count <- reactiveVal(0L)
  contrib_row_count <- reactiveVal(0L)
  remark_row_count <- reactiveVal(0L)
  conflict_resolution_state <- reactiveVal(NULL)
  stage_maintain_context <- reactiveVal(NULL)
  stage_maintain_tab1_refresh <- reactiveVal(0L)

  # ---------- 操作审计：写入 07操作审计表 ----------
  insert_audit_log <- function(conn, work_id, name, op_type, target_table, target_row_id, biz_desc, summary, old_val, new_val, remark = NULL) {
    if (is.null(conn) || !DBI::dbIsValid(conn)) return(invisible(NULL))
    work_id <- if (is.null(work_id) || is.na(work_id)) "" else as.character(work_id)
    name    <- if (is.null(name) || is.na(name)) "" else as.character(name)
    # 避免 jsonlite 对 named vector 的 asJSON 告警：统一转成列表再序列化
    to_json_payload <- function(x) {
      if (is.null(x)) return("null")
      if (is.atomic(x) && !is.null(names(x))) x <- as.list(x)
      jsonlite::toJSON(x, auto_unbox = TRUE)
    }
    old_json <- to_json_payload(old_val)
    new_json <- to_json_payload(new_val)
    remark  <- if (is.null(remark) || !nzchar(trimws(as.character(remark)))) NA_character_ else as.character(remark)
    q <- 'INSERT INTO public."07操作审计表" ("操作时间", "操作人工号", "操作人姓名", "操作类型", "目标表", "目标行id", "业务描述", "变更摘要", "旧值", "新值", "备注") VALUES (current_timestamp, $1, $2, $3, $4, $5, $6, $7, $8::json, $9::json, $10)'
    tryCatch({
      DBI::dbExecute(conn, q, params = list(work_id, name, op_type, target_table, as.integer(target_row_id), biz_desc, summary, old_json, new_json, remark))
    }, error = function(e) NULL)
    invisible(NULL)
  }

  `%||%` <- function(x, y) {
    if (is.null(x) || length(x) == 0) y else x
  }

  empty_contrib_df <- function() {
    data.frame(
      entry_key = character(0),
      person = character(0),
      role = character(0),
      work = character(0),
      amount = character(0),
      stringsAsFactors = FALSE
    )
  }

  empty_remark_df <- function() {
    data.frame(
      entry_key = character(0),
      reporter = character(0),
      updated_at = character(0),
      type = character(0),
      content = character(0),
      stringsAsFactors = FALSE
    )
  }

  empty_sample_df <- function() {
    data.frame(
      entry_key = character(0),
      hospital = character(0),
      count = numeric(0),
      stringsAsFactors = FALSE
    )
  }

  empty_milestone_df <- function() {
    data.frame(
      entry_key = character(0),
      stage_key = character(0),
      name = character(0),
      plan = character(0),
      actual = character(0),
      note = character(0),
      stringsAsFactors = FALSE
    )
  }

  normalize_text <- function(x, empty_as_na = TRUE) {
    if (is.null(x) || length(x) == 0 || all(is.na(x))) return(NA_character_)
    out <- trimws(as.character(x[[1]]))
    if (!nzchar(out) && empty_as_na) return(NA_character_)
    out
  }

  normalize_date_text <- function(x) {
    if (is.null(x) || length(x) == 0 || all(is.na(x))) return(NA_character_)
    d <- suppressWarnings(as.Date(x[[1]]))
    if (is.na(d)) return(NA_character_)
    format(d, "%Y-%m-%d")
  }

  normalize_numeric_text <- function(x, digits = 0L, default = NA_real_) {
    if (is.null(x) || length(x) == 0 || all(is.na(x))) {
      num <- default
    } else {
      num <- suppressWarnings(as.numeric(x[[1]]))
      if (is.na(num)) num <- default
    }
    if (is.na(num)) return(NA_character_)
    if (digits <= 0L) as.character(as.integer(round(num))) else format(round(num, digits), trim = TRUE, scientific = FALSE)
  }

  normalize_progress_text <- function(x) {
    txt <- normalize_numeric_text(x, digits = 0L, default = 0)
    num <- suppressWarnings(as.numeric(txt))
    if (is.na(num)) num <- 0
    as.character(as.integer(pmax(0, pmin(100, num))))
  }

  is_legacy_name_time_key <- function(x) {
    is.character(x) && length(x) == 1L && !is.na(x) && grepl("_\\d{14}$", x)
  }

  generate_db_unique_key <- function(conn, prefix = "json") {
    if (!is.null(conn) && DBI::dbIsValid(conn)) {
      key_from_db <- tryCatch({
        res <- DBI::dbGetQuery(
          conn,
          "SELECT $1::text || '_' || replace(gen_random_uuid()::text, '-', '') AS key",
          params = list(prefix)
        )
        if (nrow(res) > 0 && "key" %in% names(res)) as.character(res$key[1]) else NA_character_
      }, error = function(e) {
        tryCatch({
          res <- DBI::dbGetQuery(
            conn,
            "SELECT $1::text || '_' || md5(clock_timestamp()::text || random()::text) AS key",
            params = list(prefix)
          )
          if (nrow(res) > 0 && "key" %in% names(res)) as.character(res$key[1]) else NA_character_
        }, error = function(e2) NA_character_)
      })
      if (!is.na(key_from_db) && nzchar(trimws(key_from_db))) return(key_from_db)
    }
    paste0(prefix, "_", as.integer(as.numeric(Sys.time())), "_", sprintf("%08d", sample.int(10^8 - 1L, 1L)))
  }

  make_entry_key <- function(actor = NULL, existing_keys = character(0), prefix = "json") {
    repeat {
      key <- generate_db_unique_key(pg_con, prefix = prefix)
      if (!(key %in% existing_keys)) return(key)
    }
  }

  parse_entry_key_label <- function(key) {
    key_txt <- normalize_text(key, empty_as_na = FALSE)
    if (!nzchar(key_txt)) return(key_txt)
    if (!is_legacy_name_time_key(key_txt)) return(paste0("记录键 ", key_txt))
    actor <- sub("_\\d{14}$", "", key_txt)
    stamp <- sub("^.*_(\\d{14})$", "\\1", key_txt)
    when <- tryCatch(as.POSIXct(stamp, format = "%Y%m%d%H%M%S"), error = function(e) NA)
    when_txt <- if (is.na(when)) stamp else format(when, "%Y-%m-%d %H:%M:%S")
    paste0(actor, " 于 ", when_txt)
  }

  parse_named_json_map <- function(raw) {
    txt <- if (is.null(raw) || length(raw) == 0 || all(is.na(raw))) "" else as.character(raw[[1]])
    txt <- trimws(txt)
    if (!nzchar(txt) || identical(txt, "null") || identical(txt, "无")) return(list())
    parsed <- tryCatch(jsonlite::fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(parsed)) return(list())
    if (is.atomic(parsed) && !is.null(names(parsed))) parsed <- as.list(parsed)
    if (!is.list(parsed) || is.null(names(parsed))) return(list())
    out <- list()
    for (nm in names(parsed)) {
      val <- parsed[[nm]]
      if (is.null(val)) {
        out[[nm]] <- NA_character_
      } else if (is.atomic(val) && length(val) <= 1L) {
        out[[nm]] <- if (length(val) == 0L || is.na(val)) NA_character_ else as.character(val)
      } else {
        out[[nm]] <- val
      }
    }
    out
  }

  json_value_to_text <- function(val) {
    if (is.null(val) || length(val) == 0) return("（空）")
    if (is.atomic(val) && length(val) <= 1L) {
      txt <- if (length(val) == 0L || is.na(val)) "" else as.character(val)
      return(ifelse(nzchar(trimws(txt)), txt, "（空）"))
    }
    if (is.list(val)) {
      nm <- names(val)
      if (!is.null(nm) && length(nm) > 0) {
        parts <- vapply(seq_along(val), function(i) {
          vv <- val[[i]]
          vv_txt <- if (is.null(vv) || (is.atomic(vv) && length(vv) == 1L && is.na(vv))) "" else if (is.list(vv)) jsonlite::toJSON(vv, auto_unbox = TRUE) else as.character(vv)[1]
          paste0(nm[i], "=", vv_txt)
        }, character(1))
        return(paste(parts, collapse = "; "))
      }
      return(jsonlite::toJSON(val, auto_unbox = TRUE))
    }
    as.character(val)[1]
  }

  json_conflict_subject_text <- function(label, val) {
    if (is.null(val)) return(label)
    if (identical(label, "问题/卡点/经验分享")) {
      core <- remark_core_state(val)
      return(if (nzchar(core$type)) paste0("【", core$type, "】记录") else "反馈记录")
    }
    if (identical(label, "进度贡献者")) {
      person <- if (is.list(val) && "person" %in% names(val)) normalize_text(val$person, empty_as_na = FALSE) else ""
      return(if (nzchar(person)) paste0("进度贡献者（", person, "）") else "进度贡献者记录")
    }
    if (identical(label, "里程碑")) {
      name <- if (is.list(val) && "name" %in% names(val)) normalize_text(val$name, empty_as_na = FALSE) else ""
      return(if (nzchar(name)) paste0("里程碑（", name, "）") else "里程碑记录")
    }
    if (identical(label, "样本来源与数")) {
      hospital <- if (is.list(val) && "hospital" %in% names(val)) normalize_text(val$hospital, empty_as_na = FALSE) else ""
      return(if (nzchar(hospital)) paste0("样本来源记录（", hospital, "）") else "样本来源记录")
    }
    label
  }

  json_conflict_value_text <- function(label, val) {
    if (is.null(val)) return("（空）")
    if (identical(label, "问题/卡点/经验分享")) {
      core <- remark_core_state(val)
      return(paste0(
        "类型：", ifelse(nzchar(core$type), core$type, "（空）"),
        "；内容：", ifelse(nzchar(core$content), core$content, "（空）")
      ))
    }
    if (identical(label, "进度贡献者") && is.list(val)) {
      return(paste0(
        "人员：", normalize_text(val$person, empty_as_na = FALSE),
        "；参与度：", normalize_text(val$role, empty_as_na = FALSE),
        "；工作内容：", normalize_text(val$work, empty_as_na = FALSE),
        "；数量：", normalize_text(val$amount, empty_as_na = FALSE)
      ))
    }
    if (identical(label, "里程碑") && is.list(val)) {
      return(paste0(
        "里程碑名称：", normalize_text(val$name, empty_as_na = FALSE),
        "；计划达成时间：", normalize_text(val$plan, empty_as_na = FALSE),
        "；实际达成时间：", normalize_text(val$actual, empty_as_na = FALSE),
        "；备注：", normalize_text(val$note, empty_as_na = FALSE)
      ))
    }
    if (identical(label, "样本来源与数") && is.list(val)) {
      cnt <- suppressWarnings(as.numeric(val$count %||% 0))
      if (is.na(cnt)) cnt <- 0
      return(paste0(
        "样本来源医院：", normalize_text(val$hospital, empty_as_na = FALSE),
        "；样本数量：", as.character(cnt)
      ))
    }
    json_value_to_text(val)
  }

  json_conflict_field_pairs <- function(label, val) {
    if (is.null(val)) return(list())
    if (identical(label, "问题/卡点/经验分享")) {
      core <- remark_core_state(val)
      return(list(
        "类型" = ifelse(nzchar(core$type), core$type, "（空）"),
        "内容" = ifelse(nzchar(core$content), core$content, "（空）")
      ))
    }
    if (identical(label, "进度贡献者") && is.list(val)) {
      return(list(
        "人员" = {
          x <- normalize_text(val$person, empty_as_na = FALSE)
          ifelse(nzchar(x), x, "（空）")
        },
        "参与度" = {
          x <- normalize_text(val$role, empty_as_na = FALSE)
          ifelse(nzchar(x), x, "（空）")
        },
        "工作内容" = {
          x <- normalize_text(val$work, empty_as_na = FALSE)
          ifelse(nzchar(x), x, "（空）")
        },
        "数量" = {
          x <- normalize_text(val$amount, empty_as_na = FALSE)
          ifelse(nzchar(x), x, "（空）")
        }
      ))
    }
    if (identical(label, "里程碑") && is.list(val)) {
      return(list(
        "里程碑名称" = {
          x <- normalize_text(val$name, empty_as_na = FALSE)
          ifelse(nzchar(x), x, "（空）")
        },
        "计划达成时间" = {
          x <- normalize_text(val$plan, empty_as_na = FALSE)
          ifelse(nzchar(x), x, "（空）")
        },
        "实际达成时间" = {
          x <- normalize_text(val$actual, empty_as_na = FALSE)
          ifelse(nzchar(x), x, "（空）")
        },
        "备注" = {
          x <- normalize_text(val$note, empty_as_na = FALSE)
          ifelse(nzchar(x), x, "（空）")
        }
      ))
    }
    if (identical(label, "样本来源与数") && is.list(val)) {
      cnt <- suppressWarnings(as.numeric(val$count %||% 0))
      if (is.na(cnt)) cnt <- 0
      hospital <- normalize_text(val$hospital, empty_as_na = FALSE)
      return(list(
        "样本来源医院" = ifelse(nzchar(hospital), hospital, "（空）"),
        "样本数量" = as.character(cnt)
      ))
    }
    if (is.list(val) && !is.null(names(val)) && length(names(val)) > 0) {
      out <- lapply(names(val), function(nm) {
        x <- normalize_text(val[[nm]], empty_as_na = FALSE)
        ifelse(nzchar(x), x, "（空）")
      })
      names(out) <- names(val)
      return(out)
    }
    list("内容" = json_value_to_text(val))
  }

  json_conflict_value_ui <- function(label, val, other_val = NULL, side = c("db", "user")) {
    side <- match.arg(side)
    current_pairs <- json_conflict_field_pairs(label, val)
    other_pairs <- json_conflict_field_pairs(label, other_val)
    all_fields <- unique(c(names(current_pairs), names(other_pairs)))
    if (length(all_fields) == 0) return(tags$span("（空）"))
    diff_style <- if (identical(side, "db")) {
      "color: #C62828; font-weight: bold; background: #FFEBEE; padding: 0 2px; border-radius: 2px;"
    } else {
      "color: #1565C0; font-weight: bold; background: #E3F2FD; padding: 0 2px; border-radius: 2px;"
    }
    pieces <- vector("list", length(all_fields) * 2L)
    idx <- 1L
    for (i in seq_along(all_fields)) {
      field_name <- all_fields[[i]]
      cur_txt <- as.character(current_pairs[[field_name]] %||% "（空）")
      other_txt <- as.character(other_pairs[[field_name]] %||% "（空）")
      value_node <- if (!identical(cur_txt, other_txt)) {
        tags$span(style = diff_style, cur_txt)
      } else {
        tags$span(cur_txt)
      }
      pieces[[idx]] <- tagList(paste0(field_name, "："), value_node)
      idx <- idx + 1L
      if (i < length(all_fields)) {
        pieces[[idx]] <- "；"
        idx <- idx + 1L
      }
    }
    do.call(tagList, pieces[seq_len(idx - 1L)])
  }

  state_changed <- function(a, b) {
    !identical(a, b)
  }

  serialize_named_json_map <- function(x) {
    if (is.null(x) || length(x) == 0) return("{}")
    jsonlite::toJSON(x, auto_unbox = TRUE)
  }

  fetch_row_snapshot <- function(conn, table_name, row_id, cols, lock = FALSE) {
    cols <- unique(cols[!is.na(cols) & nzchar(cols)])
    if (is.null(conn) || !DBI::dbIsValid(conn) || is.na(row_id) || length(cols) == 0L) return(NULL)
    sel <- paste(sprintf('"%s"', cols), collapse = ", ")
    q <- sprintf('SELECT %s FROM public."%s" WHERE id = $1%s', sel, table_name, if (isTRUE(lock)) " FOR UPDATE" else "")
    res <- DBI::dbGetQuery(conn, q, params = list(as.integer(row_id)))
    if (nrow(res) == 0) return(NULL)
    as.list(res[1, , drop = FALSE])
  }

  execute_updates <- function(conn, table_name, row_id, updates) {
    if (length(updates) == 0) return(invisible(NULL))
    cols <- names(updates)
    set_clause <- paste(sprintf('"%s" = $%d', cols, seq_along(cols)), collapse = ", ")
    q <- sprintf('UPDATE public."%s" SET %s WHERE id = $%d', table_name, set_clause, length(cols) + 1L)
    DBI::dbExecute(conn, q, params = c(unname(updates), list(as.integer(row_id))))
    invisible(NULL)
  }

  same_state <- function(a_exists, a_value, b_exists, b_value) {
    identical(isTRUE(a_exists), isTRUE(b_exists)) && identical(a_value, b_value)
  }

  merge_scalar_fields <- function(snapshot_row, db_row, user_row, field_specs, overwrite_conflicts = FALSE) {
    merged <- list()
    conflicts <- list()
    changed_labels <- character(0)
    changed_updates <- list()
    for (spec in field_specs) {
      col <- spec$col
      label <- spec$label %||% col
      norm_fun <- spec$normalize %||% normalize_text
      snap_norm <- norm_fun(snapshot_row[[col]])
      db_norm <- norm_fun(db_row[[col]])
      user_norm <- norm_fun(user_row[[col]])
      user_changed <- !identical(user_norm, snap_norm)
      other_changed <- !identical(db_norm, snap_norm)
      final_value <- db_row[[col]]
      if (user_changed && !other_changed) {
        final_value <- user_row[[col]]
      } else if (user_changed && other_changed) {
        if (identical(user_norm, db_norm)) {
          final_value <- db_row[[col]]
        } else if (isTRUE(overwrite_conflicts)) {
          final_value <- user_row[[col]]
          conflicts[[length(conflicts) + 1L]] <- list(
            type = "scalar",
            label = label,
            db_value = db_norm,
            user_value = user_norm
          )
        } else {
          conflicts[[length(conflicts) + 1L]] <- list(
            type = "scalar",
            label = label,
            db_value = db_norm,
            user_value = user_norm
          )
          final_value <- db_row[[col]]
        }
      }
      merged[[col]] <- final_value
      if (!identical(norm_fun(final_value), db_norm)) {
        changed_labels <- c(changed_labels, label)
        changed_updates[[col]] <- final_value
      }
    }
    list(merged = merged, conflicts = conflicts, changed_labels = changed_labels, changed_updates = changed_updates)
  }

  merge_named_json_field <- function(snapshot_map, db_map, user_map, field_label, overwrite_conflicts = FALSE) {
    snapshot_map <- snapshot_map %||% list()
    db_map <- db_map %||% list()
    user_map <- user_map %||% list()
    all_keys <- unique(c(names(snapshot_map), names(db_map), names(user_map)))
    merged <- db_map
    conflicts <- list()
    changed_keys <- character(0)
    for (key in all_keys) {
      snap_exists <- key %in% names(snapshot_map)
      db_exists <- key %in% names(db_map)
      user_exists <- key %in% names(user_map)
      snap_val <- if (snap_exists) snapshot_map[[key]] else NA_character_
      db_val <- if (db_exists) db_map[[key]] else NA_character_
      user_val <- if (user_exists) user_map[[key]] else NA_character_
      user_changed <- !same_state(snap_exists, snap_val, user_exists, user_val)
      other_changed <- !same_state(snap_exists, snap_val, db_exists, db_val)
      if (!user_changed) next
      if (!other_changed || same_state(db_exists, db_val, user_exists, user_val)) {
        if (user_exists) {
          merged[[key]] <- user_val
        } else {
          merged[[key]] <- NULL
        }
        if (!same_state(db_exists, db_val, user_exists, user_val)) changed_keys <- c(changed_keys, key)
        next
      }
      if (isTRUE(overwrite_conflicts)) {
        if (user_exists) {
          merged[[key]] <- user_val
        } else {
          merged[[key]] <- NULL
        }
        changed_keys <- c(changed_keys, key)
      }
      conflicts[[length(conflicts) + 1L]] <- list(
        type = "json",
        label = field_label,
        key = key,
        db_exists = db_exists,
        db_value = db_val,
        user_exists = user_exists,
        user_value = user_val
      )
    }
    list(
      merged = merged,
      merged_json = serialize_named_json_map(merged),
      conflicts = conflicts,
      changed_keys = unique(changed_keys)
    )
  }

  parse_sample_map <- function(raw) {
    txt <- if (is.null(raw) || length(raw) == 0 || all(is.na(raw))) "" else as.character(raw[[1]])
    txt <- trimws(txt)
    if (!nzchar(txt) || identical(txt, "null")) return(list())
    parsed <- tryCatch(jsonlite::fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(parsed)) return(list())
    if (is.data.frame(parsed) && all(c("hospital", "count") %in% names(parsed))) {
      out <- list()
      for (i in seq_len(nrow(parsed))) {
        out[[paste0("legacy_sample_", i)]] <- list(
          hospital = as.character(parsed$hospital[i]),
          count = suppressWarnings(as.numeric(parsed$count[i]))
        )
      }
      return(out)
    }
    if (is.list(parsed) && !is.null(names(parsed))) {
      out <- list()
      for (nm in names(parsed)) {
        val <- parsed[[nm]]
        if (is.list(val) && any(c("hospital", "count") %in% names(val))) {
          out[[nm]] <- list(
            hospital = normalize_text(val$hospital, empty_as_na = FALSE),
            count = suppressWarnings(as.numeric(val$count %||% 0))
          )
        } else if (is.atomic(val) && length(val) <= 1L) {
          pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
          while (length(pieces) < 2L) pieces <- c(pieces, "0")
          out[[nm]] <- list(
            hospital = pieces[1],
            count = suppressWarnings(as.numeric(pieces[2]))
          )
        }
        if (!is.null(out[[nm]])) {
          if (is.na(out[[nm]]$count)) out[[nm]]$count <- 0
        }
      }
      return(out)
    }
    list()
  }

  parse_sample_df <- function(raw) {
    mapped <- parse_sample_map(raw)
    if (length(mapped) == 0) return(empty_sample_df())
    rows <- lapply(names(mapped), function(key) {
      val <- mapped[[key]]
      data.frame(
        entry_key = key,
        hospital = normalize_text(val$hospital, empty_as_na = FALSE),
        count = {
          num <- suppressWarnings(as.numeric(val$count %||% 0))
          if (is.na(num)) 0 else num
        },
        stringsAsFactors = FALSE
      )
    })
    do.call(rbind, rows)
  }

  normalize_sample_json <- function(raw) {
    mapped <- parse_sample_map(raw)
    if (length(mapped) == 0) return(NA_character_)
    serialize_named_json_map(mapped)
  }

  build_sample_map_from_df <- function(df, actor) {
    if (is.null(df) || nrow(df) == 0) return(list())
    out <- list()
    existing_keys <- character(0)
    for (i in seq_len(nrow(df))) {
      hospital <- trimws(as.character(df$hospital[i] %||% ""))
      if (!nzchar(hospital) || identical(hospital, "无")) next
      count <- suppressWarnings(as.numeric(df$count[i]))
      if (is.na(count)) count <- 0
      entry_key <- trimws(as.character(df$entry_key[i] %||% ""))
      if (!nzchar(entry_key) || startsWith(entry_key, "legacy_sample_")) {
        entry_key <- make_entry_key(actor, existing_keys = c(existing_keys, names(out)), prefix = "sample")
      }
      existing_keys <- c(existing_keys, entry_key)
      out[[entry_key]] <- list(
        hospital = hospital,
        count = count
      )
    }
    out
  }

  merge_single_json_blob <- function(snapshot_raw, db_raw, user_raw, field_label, overwrite_conflicts = FALSE) {
    snap_norm <- normalize_sample_json(snapshot_raw)
    db_norm <- normalize_sample_json(db_raw)
    user_norm <- normalize_sample_json(user_raw)
    user_changed <- !identical(user_norm, snap_norm)
    other_changed <- !identical(db_norm, snap_norm)
    if (!user_changed) {
      return(list(final = db_raw, changed = FALSE, conflicts = list()))
    }
    if (!other_changed || identical(db_norm, user_norm) || isTRUE(overwrite_conflicts)) {
      conflicts <- if (other_changed && !identical(db_norm, user_norm)) {
        list(list(type = "blob", label = field_label, db_value = db_norm, user_value = user_norm))
      } else {
        list()
      }
      return(list(final = user_raw, changed = !identical(user_norm, db_norm), conflicts = conflicts))
    }
    list(
      final = db_raw,
      changed = FALSE,
      conflicts = list(list(type = "blob", label = field_label, db_value = db_norm, user_value = user_norm))
    )
  }

  parse_contrib_json_to_df <- function(raw) {
    mapped <- parse_named_json_map(raw)
    if (length(mapped) == 0) return(empty_contrib_df())
    rows <- lapply(names(mapped), function(key) {
      val <- mapped[[key]]
      if (is.list(val) && any(c("person", "role", "work", "amount") %in% names(val))) {
        person <- normalize_text(val$person, empty_as_na = FALSE)
        role <- normalize_text(val$role, empty_as_na = FALSE)
        work <- normalize_text(val$work, empty_as_na = FALSE)
        amount <- normalize_text(val$amount, empty_as_na = FALSE)
      } else {
        pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
        if (is_legacy_name_time_key(key) && length(pieces) < 4L) pieces <- c(key, pieces)
        while (length(pieces) < 4L) pieces <- c(pieces, "")
        person <- pieces[1]
        role <- pieces[2]
        work <- pieces[3]
        amount <- pieces[4]
      }
      data.frame(
        entry_key = key,
        person = person,
        role = role,
        work = work,
        amount = amount,
        stringsAsFactors = FALSE
      )
    })
    do.call(rbind, rows)
  }

  build_contrib_map_from_df <- function(df, actor) {
    if (is.null(df) || nrow(df) == 0) return(list())
    out <- list()
    existing_keys <- character(0)
    for (i in seq_len(nrow(df))) {
      person <- trimws(as.character(df$person[i] %||% ""))
      if (!nzchar(person)) next
      role <- trimws(as.character(df$role[i] %||% ""))
      work <- trimws(as.character(df$work[i] %||% ""))
      amount <- trimws(as.character(df$amount[i] %||% ""))
      entry_key <- trimws(as.character(df$entry_key[i] %||% ""))
      if (!nzchar(entry_key)) entry_key <- make_entry_key(actor, existing_keys = c(existing_keys, names(out)), prefix = "contrib")
      existing_keys <- c(existing_keys, entry_key)
      out[[entry_key]] <- list(
        person = person,
        role = role,
        work = work,
        amount = amount
      )
    }
    out
  }

  parse_remark_json_to_df <- function(raw) {
    txt <- if (is.null(raw) || length(raw) == 0 || all(is.na(raw))) "" else as.character(raw[[1]])
    txt <- trimws(txt)
    # 空串 / null / {} / “无” 都视为“没有备注”
    if (!nzchar(txt) || txt %in% c("null", "{}", "无")) return(empty_remark_df())
    mapped <- parse_named_json_map(txt)
    if (length(mapped) == 0) {
      return(data.frame(
        entry_key = "",
        reporter = "",
        updated_at = "",
        type = "问题/卡点/经验分享",
        content = txt,
        stringsAsFactors = FALSE
      ))
    }
    rows <- lapply(names(mapped), function(key) {
      val <- mapped[[key]]
      if (is.list(val) && any(c("reporter", "reporters", "type", "content") %in% names(val))) {
        reporter <- paste(parse_reporter_names(val), collapse = "、")
        updated_at <- parse_remark_updated_at(val)
        type <- normalize_text(val$type, empty_as_na = FALSE)
        content <- normalize_text(val$content, empty_as_na = FALSE)
      } else {
        pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
        while (length(pieces) < 3L) pieces <- c(pieces, "")
        reporter <- pieces[1]
        updated_at <- ""
        type <- pieces[2]
        content <- paste(pieces[3:length(pieces)], collapse = "|")
      }
      data.frame(
        entry_key = key,
        reporter = reporter,
        updated_at = updated_at,
        type = type,
        content = content,
        stringsAsFactors = FALSE
      )
    })
    do.call(rbind, rows)
  }

  build_remark_map_from_df <- function(df, actor) {
    if (is.null(df) || nrow(df) == 0) return(list())
    out <- list()
    existing_keys <- character(0)
    for (i in seq_len(nrow(df))) {
      content <- trimws(as.character(df$content[i] %||% ""))
      if (!nzchar(content)) next
      reporter <- trimws(as.character(df$reporter[i] %||% ""))
      updated_at <- trimws(as.character(df$updated_at[i] %||% ""))
      type <- trimws(as.character(df$type[i] %||% ""))
      entry_key <- trimws(as.character(df$entry_key[i] %||% ""))
      if (!nzchar(entry_key)) entry_key <- make_entry_key(actor, existing_keys = c(existing_keys, names(out)), prefix = "remark")
      existing_keys <- c(existing_keys, entry_key)
      out[[entry_key]] <- list(
        reporters = if (nzchar(reporter)) strsplit(reporter, "[、，,]+", perl = TRUE)[[1]] else character(0),
        updated_at = updated_at,
        type = type,
        content = content
      )
    }
    out
  }

  parse_reporter_names <- function(val) {
    if (is.null(val)) return(character(0))
    if (is.list(val) && "reporters" %in% names(val)) {
      rr <- unlist(val$reporters, use.names = FALSE)
    } else if (is.list(val) && "reporter" %in% names(val)) {
      rr <- normalize_text(val$reporter, empty_as_na = FALSE)
    } else {
      rr <- character(0)
    }
    rr <- trimws(as.character(rr))
    unique(rr[nzchar(rr)])
  }

  parse_remark_updated_at <- function(val) {
    if (is.null(val)) return("")
    updated_at <- ""
    if (is.list(val) && "updated_at" %in% names(val)) {
      updated_at <- normalize_text(val$updated_at, empty_as_na = FALSE)
    } else if (is.list(val) && "更新日期" %in% names(val)) {
      updated_at <- normalize_text(val[["更新日期"]], empty_as_na = FALSE)
    }
    if (is.na(updated_at) || !nzchar(updated_at)) "" else updated_at
  }

  current_update_date_text <- function() {
    format(Sys.Date(), "%Y/%m/%d")
  }

  parse_update_date_for_display <- function(x) {
    txt <- normalize_text(x, empty_as_na = FALSE)
    if (!nzchar(txt)) return(NA)
    for (fmt in c("%Y/%m/%d", "%Y-%m-%d", "%Y%m%d")) {
      d <- tryCatch(as.Date(txt, format = fmt), error = function(e) NA)
      if (!is.na(d)) return(d)
    }
    suppressWarnings(as.Date(txt))
  }

  remark_date_style <- function(x) {
    d <- parse_update_date_for_display(x)
    if (is.na(d)) return("color:#666;")
    age_days <- as.numeric(today - d)
    if (!is.na(age_days) && age_days < 14) return("color:#D32F2F; font-weight:bold;")
    if (!is.na(age_days) && age_days < 28) return("color:#F57C00;")
    "color:#666;"
  }

  build_remark_value <- function(reporters, type, content, updated_at = "") {
    list(
      reporters = unique(trimws(as.character(reporters))[nzchar(trimws(as.character(reporters)))]),
      updated_at = ifelse(is.null(updated_at), "", as.character(updated_at)),
      type = ifelse(is.null(type), "", as.character(type)),
      content = ifelse(is.null(content), "", as.character(content))
    )
  }

  remark_core_state <- function(val) {
    if (is.null(val)) return(list(type = "", content = "", reporters = character(0), updated_at = ""))
    if (is.list(val) && any(c("type", "content") %in% names(val))) {
      list(
        type = normalize_text(val$type, empty_as_na = FALSE),
        content = normalize_text(val$content, empty_as_na = FALSE),
        reporters = parse_reporter_names(val),
        updated_at = parse_remark_updated_at(val)
      )
    } else {
      pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
      while (length(pieces) < 3L) pieces <- c(pieces, "")
      list(
        type = pieces[2],
        content = paste(pieces[3:length(pieces)], collapse = "|"),
        reporters = unique(trimws(pieces[1])),
        updated_at = ""
      )
    }
  }

  same_remark_state <- function(a_exists, a_val, b_exists, b_val) {
    if (!identical(isTRUE(a_exists), isTRUE(b_exists))) return(FALSE)
    a_core <- remark_core_state(a_val)
    b_core <- remark_core_state(b_val)
    identical(a_core$type, b_core$type) && identical(a_core$content, b_core$content)
  }

  merge_remark_field <- function(snapshot_map, db_map, user_map, current_reporter, overwrite_conflicts = FALSE) {
    snapshot_map <- snapshot_map %||% list()
    db_map <- db_map %||% list()
    user_map <- user_map %||% list()
    all_keys <- unique(c(names(snapshot_map), names(db_map), names(user_map)))
    merged <- db_map
    conflicts <- list()
    changed_keys <- character(0)
    for (key in all_keys) {
      snap_exists <- key %in% names(snapshot_map)
      db_exists <- key %in% names(db_map)
      user_exists <- key %in% names(user_map)
      snap_val <- if (snap_exists) snapshot_map[[key]] else NULL
      db_val <- if (db_exists) db_map[[key]] else NULL
      user_val <- if (user_exists) user_map[[key]] else NULL
      user_changed <- !same_remark_state(snap_exists, snap_val, user_exists, user_val)
      other_changed <- !same_remark_state(snap_exists, snap_val, db_exists, db_val)
      if (!user_changed) next

      user_core <- remark_core_state(user_val)
      db_core <- remark_core_state(db_val)
      snap_core <- remark_core_state(snap_val)
      final_reporters <- unique(c(db_core$reporters, snap_core$reporters, current_reporter))
      final_updated_at <- current_update_date_text()
      final_value <- if (user_exists) build_remark_value(final_reporters, user_core$type, user_core$content, final_updated_at) else NULL

      if (!other_changed || same_remark_state(db_exists, db_val, user_exists, user_val)) {
        if (user_exists) {
          merged[[key]] <- final_value
        } else {
          merged[[key]] <- NULL
        }
        changed_keys <- c(changed_keys, key)
        next
      }

      if (isTRUE(overwrite_conflicts)) {
        if (user_exists) {
          merged[[key]] <- final_value
        } else {
          merged[[key]] <- NULL
        }
        changed_keys <- c(changed_keys, key)
      }

      conflicts[[length(conflicts) + 1L]] <- list(
        type = "json",
        label = "问题/卡点/经验分享",
        key = key,
        db_exists = db_exists,
        db_value = if (db_exists) build_remark_value(db_core$reporters, db_core$type, db_core$content, db_core$updated_at) else NULL,
        user_exists = user_exists,
        user_value = if (user_exists) final_value else NULL
      )
    }
    list(
      merged = merged,
      merged_json = serialize_named_json_map(merged),
      conflicts = conflicts,
      changed_keys = unique(changed_keys)
    )
  }

  parse_milestone_json_to_df <- function(raw) {
    mapped <- parse_named_json_map(raw)
    if (length(mapped) == 0) return(empty_milestone_df())
    rows <- lapply(names(mapped), function(key) {
      val <- mapped[[key]]
      if (is.list(val) && any(c("stage_key", "name", "plan", "actual", "note") %in% names(val))) {
        data.frame(
          entry_key = key,
          stage_key = normalize_text(val$stage_key, empty_as_na = FALSE),
          name = normalize_text(val$name, empty_as_na = FALSE),
          plan = normalize_text(val$plan, empty_as_na = FALSE),
          actual = normalize_text(val$actual, empty_as_na = FALSE),
          note = normalize_text(val$note, empty_as_na = FALSE),
          stringsAsFactors = FALSE
        )
      } else if (grepl("::", key, fixed = TRUE)) {
        key_parts <- strsplit(key, "::", fixed = TRUE)[[1]]
        stage_key <- if (length(key_parts) >= 1L) key_parts[1] else ""
        name <- if (length(key_parts) >= 2L) key_parts[2] else ""
        pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
        while (length(pieces) < 3L) pieces <- c(pieces, "无")
        data.frame(
          entry_key = key,
          stage_key = stage_key,
          name = name,
          plan = ifelse(pieces[1] == "无", "", pieces[1]),
          actual = ifelse(pieces[2] == "无", "", pieces[2]),
          note = ifelse(pieces[3] == "无", "", pieces[3]),
          stringsAsFactors = FALSE
        )
      } else {
        pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
        while (length(pieces) < 5L) pieces <- c(pieces, "无")
        data.frame(
          entry_key = key,
          stage_key = pieces[1],
          name = ifelse(pieces[2] == "无", "", pieces[2]),
          plan = ifelse(pieces[3] == "无", "", pieces[3]),
          actual = ifelse(pieces[4] == "无", "", pieces[4]),
          note = ifelse(pieces[5] == "无", "", pieces[5]),
          stringsAsFactors = FALSE
        )
      }
    })
    do.call(rbind, rows)
  }

  build_milestone_map_from_df <- function(df, actor) {
    if (is.null(df) || nrow(df) == 0) return(list())
    out <- list()
    existing_keys <- character(0)
    for (i in seq_len(nrow(df))) {
      stage_key <- trimws(as.character(df$stage_key[i] %||% ""))
      name <- trimws(as.character(df$name[i] %||% ""))
      if (!nzchar(stage_key) || !nzchar(name)) next
      plan <- trimws(as.character(df$plan[i] %||% ""))
      actual <- trimws(as.character(df$actual[i] %||% ""))
      note <- trimws(as.character(df$note[i] %||% ""))
      entry_key <- trimws(as.character(df$entry_key[i] %||% ""))
      if (!nzchar(entry_key)) entry_key <- make_entry_key(actor, existing_keys = c(existing_keys, names(out)), prefix = "milestone")
      existing_keys <- c(existing_keys, entry_key)
      out[[entry_key]] <- list(
        stage_key = stage_key,
        name = name,
        plan = ifelse(nzchar(plan), plan, ""),
        actual = ifelse(nzchar(actual), actual, ""),
        note = ifelse(nzchar(note), note, "")
      )
    }
    out
  }

  build_conflict_lines_ui <- function(conflicts) {
    if (length(conflicts) == 0) return(tags$span("未检测到冲突。"))
    tags$ul(
      lapply(conflicts, function(item) {
        if (identical(item$type, "scalar")) {
          txt <- sprintf(
            "%s 已被其他用户改为：%s；你当前准备保存为：%s。",
            item$label,
            ifelse(is.null(item$db_value) || is.na(item$db_value) || !nzchar(item$db_value), "（空）", item$db_value),
            ifelse(is.null(item$user_value) || is.na(item$user_value) || !nzchar(item$user_value), "（空）", item$user_value)
          )
        } else if (identical(item$type, "blob")) {
          txt <- tagList(
            paste0(item$label, " 已发生冲突"),
            tags$br(),
            "其他用户更新为：",
            json_conflict_value_ui(item$label, item$db_value, item$user_value, side = "db"),
            tags$br(),
            "你当前为：",
            json_conflict_value_ui(item$label, item$user_value, item$db_value, side = "user"),
            "。"
          )
        } else {
          subject_txt <- if (isTRUE(item$db_exists)) {
            json_conflict_subject_text(item$label, item$db_value)
          } else if (isTRUE(item$user_exists)) {
            json_conflict_subject_text(item$label, item$user_value)
          } else {
            item$label
          }
          txt <- tagList(
            paste0(subject_txt, " 已发生冲突"),
            tags$br(),
            "其他用户更新为：",
            if (isTRUE(item$db_exists)) {
              json_conflict_value_ui(item$label, item$db_value, item$user_value, side = "db")
            } else {
              "（已删除）"
            },
            tags$br(),
            "你当前为：",
            if (isTRUE(item$user_exists)) {
              json_conflict_value_ui(item$label, item$user_value, item$db_value, side = "user")
            } else {
              "（已删除）"
            },
            "。"
          )
        }
        tags$li(txt)
      })
    )
  }

  format_auto_merge_message <- function(items, default_success) {
    items <- unique(items[nzchar(items)])
    if (length(items) == 0) return(default_success)
    paste0(default_success, " 本次自动合并了：", paste(items, collapse = "、"), "。")
  }

  format_json_auto_merge_items <- function(field_label, keys) {
    keys <- unique(keys[nzchar(keys)])
    if (length(keys) == 0) return(character(0))
    paste0(field_label, "（", paste(vapply(keys, parse_entry_key_label, character(1)), collapse = "、"), "）")
  }

  output$conflict_modal_body <- renderUI({
    state <- conflict_resolution_state()
    if (is.null(state)) return(NULL)
    if (isTRUE(state$show_conflicts_only)) {
      return(build_conflict_lines_ui(state$conflicts))
    }
    tagList(
      tags$p("检测到其他用户已在你编辑期间更新了同一条记录。"),
      tags$p(tags$small("你可以只查看冲突项、刷新为数据库最新内容，或按需选择覆盖 / 仅保存无冲突部分。")),
      build_conflict_lines_ui(state$conflicts)
    )
  })

  show_conflict_resolution_modal <- function(title, conflicts, on_overwrite, on_partial, on_refresh = NULL, on_close = NULL) {
    conflict_resolution_state(list(
      title = title,
      conflicts = conflicts,
      overwrite = on_overwrite,
      partial = on_partial,
      refresh = on_refresh,
      close = on_close,
      show_conflicts_only = FALSE
    ))
    showModal(modalDialog(
      title = title,
      size = "l",
      easyClose = FALSE,
      footer = tagList(
        actionButton("btn_conflict_only", "只看冲突项", class = "btn-default"),
        actionButton("btn_conflict_show_all", "查看全部", class = "btn-default"),
        if (is.function(on_refresh)) actionButton("btn_conflict_refresh", "刷新为最新库内容", class = "btn-info"),
        actionButton("btn_conflict_overwrite", "覆盖其他用户更新", class = "btn-danger"),
        actionButton("btn_conflict_partial", "仅更新不冲突部分", class = "btn-warning"),
        actionButton("btn_conflict_close", "返回继续填写", class = "btn-default")
      ),
      uiOutput("conflict_modal_body")
    ))
  }

  observeEvent(input$btn_conflict_only, {
    state <- conflict_resolution_state()
    req(state)
    state$show_conflicts_only <- TRUE
    conflict_resolution_state(state)
  })

  observeEvent(input$btn_conflict_show_all, {
    state <- conflict_resolution_state()
    req(state)
    state$show_conflicts_only <- FALSE
    conflict_resolution_state(state)
  })

  observeEvent(input$btn_conflict_overwrite, {
    state <- conflict_resolution_state()
    req(state, is.function(state$overwrite))
    removeModal()
    state$overwrite()
  })

  observeEvent(input$btn_conflict_partial, {
    state <- conflict_resolution_state()
    req(state, is.function(state$partial))
    removeModal()
    state$partial()
  })

  observeEvent(input$btn_conflict_refresh, {
    state <- conflict_resolution_state()
    req(state, is.function(state$refresh))
    removeModal()
    state$refresh()
  })

  observeEvent(input$btn_conflict_close, {
    state <- conflict_resolution_state()
    req(state)
    removeModal()
    if (is.function(state$close)) state$close()
  })

  # ---------- 系统确权：从 Nginx 传入的 X-User 头推导权限 ----------
  current_user_auth <- reactive({
    wid <- session$request$HTTP_X_USER
    wid <- if (is.null(wid)) "" else trimws(as.character(wid))
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
    if (!nzchar(wid)) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
    tryCatch({
      r <- DBI::dbGetQuery(pg_con, "SELECT id, \"姓名\", \"组别\", \"数据库权限等级\", \"人员状态\" FROM public.\"05人员表\" WHERE \"工号\" = $1 LIMIT 1", params = list(wid))
      if (nrow(r) == 0) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
      status <- trimws(as.character(r[["人员状态"]][1]))
      level  <- trimws(as.character(r[["数据库权限等级"]][1]))
      if (is.na(status) || status != "在职") return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
      if (is.na(level) || level == "deny_access") return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
      name <- if ("姓名" %in% names(r) && !is.na(r[["姓名"]][1])) trimws(as.character(r[["姓名"]][1])) else ""
      if (level == "super_admin") return(list(allow_all = TRUE, allow_none = FALSE, allowed_subquery = "", work_id = wid, name = name, is_super_admin = TRUE))
      if (level == "manager") return(list(allow_all = TRUE, allow_none = FALSE, allowed_subquery = "", work_id = wid, name = name, is_super_admin = FALSE))
      pid <- as.integer(r[["id"]][1])
      if (level == "common_user") {
        subq <- sprintf(
          "SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" = %d UNION SELECT \"04项目总表_id\" FROM public.\"_nc_m2m_04项目总表_05人员表\" WHERE \"05人员表_id\" = %d",
          pid, pid
        )
        return(list(allow_all = FALSE, allow_none = FALSE, allowed_subquery = subq, work_id = wid, name = name, is_super_admin = FALSE))
      }
      if (level == "group_manager") {
        subq <- sprintf(
          "SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" IN (SELECT id FROM public.\"05人员表\" WHERE \"组别\" IS NOT DISTINCT FROM (SELECT \"组别\" FROM public.\"05人员表\" WHERE id = %d)) UNION SELECT \"04项目总表_id\" FROM public.\"_nc_m2m_04项目总表_05人员表\" WHERE \"05人员表_id\" IN (SELECT id FROM public.\"05人员表\" WHERE \"组别\" IS NOT DISTINCT FROM (SELECT \"组别\" FROM public.\"05人员表\" WHERE id = %d))",
          pid, pid
        )
        return(list(allow_all = FALSE, allow_none = FALSE, allowed_subquery = subq, work_id = wid, name = name, is_super_admin = FALSE))
      }
      list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE)
    }, error = function(e) list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
  })

  stage_definition_df <- reactive({
    default_defs <- bind_rows(lapply(names(project_type_valid_stages), function(pt) {
      stage_keys <- default_stage_order_full[task_short_name[default_stage_order_full] %in% project_type_valid_stages[[pt]]]
      tibble(
        project_type = pt,
        stage_key = stage_keys,
        stage_name = unname(task_short_name[stage_keys]),
        stage_scope = ifelse(stage_keys %in% sync_stages_db, "sync", "site"),
        stage_order = match(stage_keys, default_stage_order_full),
        supports_sample = stage_keys == "S09_验证试验开展与数据管理",
        stage_config = replicate(length(stage_keys), list(list()), simplify = FALSE)
      )
    }))
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(default_defs)
    tryCatch({
      q <- paste(
        'SELECT project_type, stage_key, stage_name, stage_scope, stage_order,',
        'COALESCE(is_active, TRUE) AS is_active,',
        'COALESCE(supports_sample, FALSE) AS supports_sample,',
        'COALESCE(stage_config, \'{}\'::jsonb)::text AS stage_config_json',
        'FROM public."08项目阶段定义表"',
        'ORDER BY project_type, stage_order, stage_key'
      )
      df <- DBI::dbGetQuery(pg_con, q)
      if (nrow(df) == 0) return(default_defs)
      df$stage_config <- lapply(df$stage_config_json, function(x) {
        tryCatch(
          jsonlite::fromJSON(if (is.na(x) || !nzchar(x)) "{}" else x, simplifyVector = FALSE),
          error = function(e) list()
        )
      })
      df$stage_config_json <- NULL
      as_tibble(df)
    }, error = function(e) default_defs)
  })

  stage_catalog <- reactive({
    defs <- stage_definition_df()
    unique_defs <- defs %>% arrange(stage_order, stage_key) %>% distinct(stage_key, .keep_all = TRUE)
    list(
      defs = defs,
      unique_defs = unique_defs,
      sync_stages = unique_defs %>% filter(stage_scope == "sync") %>% arrange(stage_order) %>% pull(stage_key),
      site_stages = unique_defs %>% filter(stage_scope == "site") %>% arrange(stage_order) %>% pull(stage_key),
      task_short_name = setNames(unique_defs$stage_name, unique_defs$stage_key)
    )
  })

  stage_keys_for_project_type <- function(project_type = NA_character_) {
    defs <- stage_catalog()$defs
    if (!is.na(project_type) && nzchar(project_type) && project_type %in% defs$project_type) {
      return(defs %>% filter(project_type == !!project_type) %>% arrange(stage_order) %>% pull(stage_key))
    }
    stage_catalog()$unique_defs %>% arrange(stage_order) %>% pull(stage_key)
  }

  stage_label_for_key <- function(stage_key) {
    nms <- stage_catalog()$task_short_name
    lbl <- if (length(stage_key) != 1) NA_character_ else nms[names(nms) == stage_key][1]
    if (!is.null(lbl) && !is.na(lbl) && nzchar(lbl)) return(as.character(lbl))
    sub("^S\\d+_?", "", as.character(stage_key %||% ""))
  }

  stage_config_for <- function(stage_key, project_type = NA_character_) {
    defs <- stage_catalog()$defs
    # 注意：在 dplyr::filter 里不能直接用带长度>1的 if()，否则会触发你看到的
    # “length = n in coercion to 'logical(1)'” 错误。这里先在 R 里判断是否需要按项目类型过滤，
    # 再分别构造两条管道。
    if (!is.na(project_type) && nzchar(project_type) && project_type %in% defs$project_type) {
      row <- defs %>%
        filter(stage_key == !!stage_key, project_type == !!project_type) %>%
        arrange(stage_order) %>%
        slice(1)
    } else {
      row <- defs %>%
        filter(stage_key == !!stage_key) %>%
        arrange(stage_order) %>%
        slice(1)
    }
    if (nrow(row) == 0) return(list())
    row$stage_config[[1]] %||% list()
  }

  stage_work_choices_for <- function(stage_key, project_type = NA_character_) {
    cfg <- stage_config_for(stage_key, project_type)
    if (is.list(cfg) && "work_choices" %in% names(cfg)) {
      vals <- as.character(unlist(cfg$work_choices, use.names = FALSE))
      vals <- vals[nzchar(vals)]
      if (length(vals) > 0) return(vals)
    }
    short_name <- stage_label_for_key(stage_key)
    switch(
      short_name,
      "需求与背景调研" = c("背景调研与文档输出"),
      "方案设计审核" = c("方案输出", "方案评审", "药监局咨询"),
      "医院筛选与专家对接" = c("医院沟通协调"),
      "医院立项资料输出与递交" = c("立项资料输出", "立项资料递交"),
      "伦理审批与启动会" = c("主持启动会", "跟进伦理进度"),
      "人员与物资准备" = c("试验物料准备"),
      "试验开展与数据汇总表管理" = c("样本筛选入组", "样本检测", "数据汇总表输出", "数据汇总表评审", "数据溯源"),
      "小结输出与定稿" = c("小结输出", "小结评审"),
      "验证试验开展与数据管理" = c("样本检测"),
      "总报告输出与定稿" = c("总报告输出", "总报告评审"),
      "资料递交与结题归档" = c("结题资料递交"),
      "临床试验发补与资料递交" = c("发补问题沟通", "发补资料输出", "发布试验执行"),
      "文章初稿输出" = c("初稿输出"),
      "文章内部审评、修改、投递" = c("文章审评", "文章修改"),
      "意见反馈与文章返修" = c("审稿人沟通与文章修改"),
      character(0)
    )
  }

  # 筛选条件防抖，避免快速切换筛选时多次取消未完成的查询导致 “Closing open result set” 警告
  gantt_filter_state <- reactive({
    list(
      refresh = input$gantt_refresh,
      force = gantt_force_refresh(),
      type = input$filter_type,
      name = input$filter_name,
      manager = input$filter_manager,
      participant = input$filter_participant,
      importance = input$filter_importance,
      hospital = input$filter_hospital,
      include_archived = input$filter_include_archived
    )
  })
  gantt_filter_state_debounced <- debounce(gantt_filter_state, 400)

  output$gantt_db_msg <- renderUI({
    err <- gantt_db_error()
    if (!is.null(err)) {
      tags$div(
        tags$p(style = "color: red; font-weight: bold; margin: 10px 0;",
          "⚠️ ", err, "。请检查数据库连接。"),
        style = "margin-bottom: 10px; padding: 8px; background: #FFF3E0; border: 1px solid #FFB74D; border-radius: 4px;"
      )
    } else NULL
  })


  # 标题下显示当前登录帐号：姓名-工号（来自 Nginx 鉴权后的 X-User 工号 + 05人员表 姓名）
  output$current_user_display <- renderUI({
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return(tags$p(style = "color: #888; font-size: 13px; margin: 4px 0;", "当前登录帐号：未登录"))
    wid <- auth$work_id
    name <- auth$name
    if (!nzchar(wid) && !nzchar(name)) return(tags$p(style = "color: #888; font-size: 13px; margin: 4px 0;", "当前登录帐号：—"))
    txt <- if (nzchar(name) && nzchar(wid)) paste0(name, "-", wid) else if (nzchar(wid)) wid else name
    tags$p(style = "color: #333; font-size: 13px; margin: 4px 0;", paste0("当前登录帐号：", txt))
  })

  # 筛选器选项：按当前用户权限仅索要可访问维度的 distinct 清单（数据库层筛选）
  gantt_filter_options <- reactive({
    input$gantt_refresh
    current_user_auth()
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(NULL)
    auth <- current_user_auth()
    if (auth$allow_none) {
      return(list(types = character(0), names = character(0), managers = character(0), participants = character(0), importance = character(0), hospitals = character(0)))
    }
    and_04 <- if (auth$allow_all) "" else paste0(" AND id IN (", auth$allowed_subquery, ")")
    and_g  <- if (auth$allow_all) "" else paste0(" AND g.id IN (", auth$allowed_subquery, ")")
    and_m  <- if (auth$allow_all) "" else paste0(" AND m.\"04项目总表_id\" IN (", auth$allowed_subquery, ")")
    and_s  <- if (auth$allow_all) "" else paste0(" AND s.\"project_table 项目总表_id\" IN (", auth$allowed_subquery, ")")
    tryCatch({
      types <- character(0)
      names_ <- character(0)
      managers <- character(0)
      participants <- character(0)
      importance <- character(0)
      hospitals <- character(0)
      t1 <- DBI::dbGetQuery(pg_con, paste0('SELECT DISTINCT "项目类型" AS v FROM public."04项目总表" WHERE "项目类型" IS NOT NULL', and_04, ' ORDER BY 1'))
      if (nrow(t1) > 0) types <- as.character(t1$v)
      t2 <- DBI::dbGetQuery(pg_con, paste0('SELECT DISTINCT "项目名称" AS v FROM public."04项目总表" WHERE "项目名称" IS NOT NULL', and_04, ' ORDER BY 1'))
      if (nrow(t2) > 0) names_ <- as.character(t2$v)
      t3 <- DBI::dbGetQuery(pg_con, paste0('SELECT DISTINCT p."姓名" AS v FROM public."05人员表" p INNER JOIN public."04项目总表" g ON g."05人员表_id" = p.id WHERE g."05人员表_id" IS NOT NULL', and_g, ' ORDER BY 1'))
      if (nrow(t3) > 0) managers <- as.character(t3$v)
      t4 <- tryCatch(
        DBI::dbGetQuery(pg_con, paste0('SELECT DISTINCT p."姓名" AS v FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."05人员表_id" = p.id WHERE 1=1', and_m, ' ORDER BY 1')),
        error = function(e) data.frame(v = character(0))
      )
      if (nrow(t4) > 0) participants <- as.character(t4$v)
      t5 <- DBI::dbGetQuery(pg_con, paste0('SELECT DISTINCT "重要紧急程度" AS v FROM public."04项目总表" WHERE "重要紧急程度" IS NOT NULL', and_04, ' ORDER BY 1'))
      if (nrow(t5) > 0) importance <- as.character(t5$v)
      t6 <- DBI::dbGetQuery(pg_con, paste0('SELECT DISTINCT h."医院名称" AS v FROM public."01医院信息表" h INNER JOIN public."03医院_项目表" s ON s."01_hos_resource_table医院信息表_id" = h.id WHERE h."医院名称" IS NOT NULL', and_s, ' ORDER BY 1'))
      if (nrow(t6) > 0) hospitals <- as.character(t6$v)
      list(
        types = types,
        names = names_,
        managers = managers,
        participants = participants,
        importance = importance,
        hospitals = hospitals
      )
    }, error = function(e) NULL)
  })

  output$gantt_filters <- renderUI({
    opts <- gantt_filter_options()
    tags$div(
      class = "panel panel-default",
      style = "margin-bottom: 10px;",
      tags$div(
        class = "panel-heading",
        style = "cursor: pointer;",
        role = "button",
        `data-toggle` = "collapse",
        `data-target` = "#ganttFilterCollapse",
        `aria-expanded` = "false",
        `aria-controls` = "ganttFilterCollapse",
        tags$span(class = "glyphicon glyphicon-chevron-right", style = "margin-right: 6px;"),
        tags$span("筛选与刷新")
      ),
      tags$div(
        id = "ganttFilterCollapse",
        class = "panel-collapse collapse",
        tags$div(
          class = "panel-body",
          actionButton("gantt_refresh", "🔄 从数据库刷新", class = "btn-primary", style = "margin-bottom: 10px;"),
          if (!is.null(opts)) fluidRow(
            column(3, selectInput("filter_type", "项目类型", choices = opts$types, multiple = TRUE, selectize = TRUE)),
            column(3, selectInput("filter_name", "项目名称", choices = opts$names, multiple = TRUE, selectize = TRUE)),
            column(3, selectInput("filter_manager", "项目负责人", choices = opts$managers, multiple = TRUE, selectize = TRUE)),
            column(3, selectInput("filter_participant", "项目参与人员", choices = opts$participants, multiple = TRUE, selectize = TRUE))
          ),
          if (!is.null(opts)) fluidRow(
            column(3, selectInput("filter_importance", "重要紧急程度", choices = opts$importance, multiple = TRUE, selectize = TRUE)),
            column(3, selectInput("filter_hospital", "相关医院（有中心）", choices = opts$hospitals, multiple = TRUE, selectize = TRUE)),
            column(3, div(
              style = "margin-top: 10px; font-size: 16px; min-height: 38px; display: flex; align-items: center;",
              checkboxInput("filter_include_archived", "包含已结题项目", value = FALSE)
            ))
          )
        )
      )
    )
  })

  gantt_data_db <- reactive({
    state <- gantt_filter_state_debounced()
    current_user_auth()
    gantt_db_error(NULL)
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      gantt_db_error("无法连接数据库，请检查 PG_HOST/PG_PORT/PG_DBNAME/PG_USER/PG_PASSWORD 或先启动数据库服务")
      return(NULL)
    }
    auth <- current_user_auth()
    if (auth$allow_none) return(NULL)
    ft <- if (is.null(state$type)) character(0) else state$type
    fn <- if (is.null(state$name)) character(0) else state$name
    fm <- if (is.null(state$manager)) character(0) else state$manager
    fp <- if (is.null(state$participant)) character(0) else state$participant
    fi <- if (is.null(state$importance)) character(0) else state$importance
    fh <- if (is.null(state$hospital)) character(0) else state$hospital
    tryCatch({
      manager_ids <- integer(0)
      if (length(fm) > 0) {
        qm <- paste0('SELECT id FROM public."05人员表" WHERE "姓名" IN (', paste(rep("$", length(fm)), seq_along(fm), sep = "", collapse = ","), ")")
        manager_ids <- DBI::dbGetQuery(pg_con, qm, params = as.list(fm))$id
      }
      proj_ids_participant <- integer(0)
      if (length(fp) > 0) {
        qp <- paste0('SELECT id FROM public."05人员表" WHERE "姓名" IN (', paste(rep("$", length(fp)), seq_along(fp), sep = "", collapse = ","), ")")
        pid_p <- DBI::dbGetQuery(pg_con, qp, params = as.list(fp))$id
        if (length(pid_p) > 0) {
          qp2 <- paste0('SELECT DISTINCT "04项目总表_id" FROM public."_nc_m2m_04项目总表_05人员表" WHERE "05人员表_id" IN (', paste(rep("$", length(pid_p)), seq_along(pid_p), sep = "", collapse = ","), ")")
          proj_ids_participant <- DBI::dbGetQuery(pg_con, qp2, params = as.list(pid_p))[["04项目总表_id"]]
        }
      }
      proj_ids_hosp <- integer(0)
      if (length(fh) > 0) {
        qh <- paste0('SELECT DISTINCT s."project_table 项目总表_id" FROM public."03医院_项目表" s INNER JOIN public."01医院信息表" h ON s."01_hos_resource_table医院信息表_id" = h.id WHERE h."医院名称" IN (', paste(rep("$", length(fh)), seq_along(fh), sep = "", collapse = ","), ")")
        proj_ids_hosp <- DBI::dbGetQuery(pg_con, qh, params = as.list(fh))[["project_table 项目总表_id"]]
      }

      where_stage <- c('project_id IS NOT NULL', 'project_type IS NOT NULL')
      if (!auth$allow_all) where_stage <- c(where_stage, paste0("project_db_id IN (", auth$allowed_subquery, ")"))
      params_stage <- list()
      make_placeholder_csv <- function(n, start_idx) {
        paste(sprintf("$%d", start_idx + seq_len(n) - 1L), collapse = ",")
      }
      p <- 0L
      if (length(ft) > 0) {
        ph <- make_placeholder_csv(length(ft), p + 1L)
        where_stage <- c(where_stage, sprintf("project_type IN (%s)", ph))
        params_stage <- c(params_stage, as.list(ft))
        p <- p + length(ft)
      }
      if (length(fn) > 0) {
        ph <- make_placeholder_csv(length(fn), p + 1L)
        where_stage <- c(where_stage, sprintf("project_id IN (%s)", ph))
        params_stage <- c(params_stage, as.list(fn))
        p <- p + length(fn)
      }
      if (length(manager_ids) > 0) {
        ph <- make_placeholder_csv(length(manager_ids), p + 1L)
        where_stage <- c(where_stage, sprintf("project_db_id IN (SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" IN (%s))", ph))
        params_stage <- c(params_stage, as.list(manager_ids))
        p <- p + length(manager_ids)
      }
      if (length(proj_ids_participant) > 0) {
        ph <- make_placeholder_csv(length(proj_ids_participant), p + 1L)
        where_stage <- c(where_stage, sprintf("project_db_id IN (%s)", ph))
        params_stage <- c(params_stage, as.list(proj_ids_participant))
        p <- p + length(proj_ids_participant)
      }
      if (length(fi) > 0) {
        ph <- make_placeholder_csv(length(fi), p + 1L)
        where_stage <- c(where_stage, sprintf("\"重要紧急程度\" IN (%s)", ph))
        params_stage <- c(params_stage, as.list(fi))
        p <- p + length(fi)
      }
      if (length(proj_ids_hosp) > 0) {
        ph <- make_placeholder_csv(length(proj_ids_hosp), p + 1L)
        where_stage <- c(where_stage, sprintf("project_db_id IN (%s)", ph))
        params_stage <- c(params_stage, as.list(proj_ids_hosp))
      }
      if (!isTRUE(state$include_archived)) {
        where_stage <- c(where_stage, 'COALESCE(project_is_active, true) = true')
      }

      sql_stage <- paste(
        'SELECT * FROM public."v_项目阶段甘特视图"',
        'WHERE', paste(where_stage, collapse = ' AND '),
        'ORDER BY project_id, stage_ord, site_name'
      )
      if (length(params_stage) > 0) {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage, params = params_stage)
      } else {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage)
      }
      if (nrow(stage_rows) == 0) return(stage_rows)
      stage_rows <- as_tibble(stage_rows)
      stage_rows$planned_start_date <- as.Date(stage_rows$planned_start_date)
      stage_rows$actual_start_date  <- as.Date(stage_rows$actual_start_date)
      # start_date 已由视图计算为 COALESCE(actual_start_date, planned_start_date)
      stage_rows$start_date <- as.Date(stage_rows$start_date)
      stage_rows$planned_end_date <- as.Date(stage_rows$planned_end_date)
      stage_rows$actual_end_date <- as.Date(stage_rows$actual_end_date)
      stage_rows$progress <- norm_progress(stage_rows$progress)
      stage_rows$is_unplanned <- is.na(stage_rows$start_date) | is.na(stage_rows$planned_end_date)
      ss <- stage_catalog()$sync_stages
      site_rows <- stage_rows %>% filter(!task_name %in% ss)
      sync_rows <- stage_rows %>% filter(task_name %in% ss)
      site_name_map <- site_rows %>% distinct(project_id, site_name)
      if (nrow(sync_rows) > 0) {
        sync_expanded <- sync_rows %>% select(-site_name) %>% left_join(site_name_map, by = "project_id", relationship = "many-to-many")
        sync_expanded <- sync_expanded %>% filter(!is.na(site_name))
        sync_without_sites <- sync_rows %>% filter(!(project_id %in% site_name_map$project_id))
        if (nrow(sync_without_sites) > 0) {
          sync_without_sites$site_name <- "所有中心（同步）"
          sync_expanded <- bind_rows(sync_expanded, sync_without_sites)
        }
      } else {
        sync_expanded <- sync_rows
      }
      df <- bind_rows(sync_expanded, site_rows) %>% arrange(project_id, site_name, stage_ord)
      df$raw_planned_start_date <- df$planned_start_date
      df$raw_actual_start_date  <- df$actual_start_date
      df$raw_start_date <- df$start_date           # 有效开始（coalesce）
      df$raw_planned_end_date <- df$planned_end_date
      cur_proj <- ""
      cur_site <- ""
      anchor <- today
      for (i in seq_len(nrow(df))) {
        if (df$project_id[i] != cur_proj || df$site_name[i] != cur_site) {
          cur_proj <- df$project_id[i]
          cur_site <- df$site_name[i]
          anchor <- today
        }
        if (isTRUE(df$is_unplanned[i])) {
          df$start_date[i] <- anchor
          df$planned_end_date[i] <- anchor + 30L
        }
        tail_i <- NA
        if (!is.na(df$actual_end_date[i])) {
          tail_i <- df$actual_end_date[i]
        } else if (!is.na(df$planned_end_date[i])) {
          tail_i <- df$planned_end_date[i]
        } else if (!is.na(df$start_date[i])) {
          tail_i <- df$start_date[i]
        }
        if (!is.na(tail_i) && tail_i > anchor) {
          anchor <- tail_i
        }
      }
      df %>% select(-stage_ord)
    }, error = function(e) {
      gantt_db_error(paste0("数据库加载失败: ", conditionMessage(e)))
      NULL
    })
  })

  # 甘特数据源（含未激活阶段），用于自由里程碑展示与占位，使未勾选 is_active 的阶段仍可显示/编辑里程碑
  gantt_data_all_stages <- reactive({
    state <- gantt_filter_state_debounced()
    current_user_auth()
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(NULL)
    auth <- current_user_auth()
    if (auth$allow_none) return(NULL)
    ft <- if (is.null(state$type)) character(0) else state$type
    fn <- if (is.null(state$name)) character(0) else state$name
    fm <- if (is.null(state$manager)) character(0) else state$manager
    fp <- if (is.null(state$participant)) character(0) else state$participant
    fi <- if (is.null(state$importance)) character(0) else state$importance
    fh <- if (is.null(state$hospital)) character(0) else state$hospital
    tryCatch({
      manager_ids <- integer(0)
      if (length(fm) > 0) {
        qm <- paste0('SELECT id FROM public."05人员表" WHERE "姓名" IN (', paste(rep("$", length(fm)), seq_along(fm), sep = "", collapse = ","), ")")
        manager_ids <- DBI::dbGetQuery(pg_con, qm, params = as.list(fm))$id
      }
      proj_ids_participant <- integer(0)
      if (length(fp) > 0) {
        qp <- paste0('SELECT id FROM public."05人员表" WHERE "姓名" IN (', paste(rep("$", length(fp)), seq_along(fp), sep = "", collapse = ","), ")")
        pid_p <- DBI::dbGetQuery(pg_con, qp, params = as.list(fp))$id
        if (length(pid_p) > 0) {
          qp2 <- paste0('SELECT DISTINCT "04项目总表_id" FROM public."_nc_m2m_04项目总表_05人员表" WHERE "05人员表_id" IN (', paste(rep("$", length(pid_p)), seq_along(pid_p), sep = "", collapse = ","), ")")
          proj_ids_participant <- DBI::dbGetQuery(pg_con, qp2, params = as.list(pid_p))[["04项目总表_id"]]
        }
      }
      proj_ids_hosp <- integer(0)
      if (length(fh) > 0) {
        qh <- paste0('SELECT DISTINCT s."project_table 项目总表_id" FROM public."03医院_项目表" s INNER JOIN public."01医院信息表" h ON s."01_hos_resource_table医院信息表_id" = h.id WHERE h."医院名称" IN (', paste(rep("$", length(fh)), seq_along(fh), sep = "", collapse = ","), ")")
        proj_ids_hosp <- DBI::dbGetQuery(pg_con, qh, params = as.list(fh))[["project_table 项目总表_id"]]
      }
      where_stage <- c('project_id IS NOT NULL', 'project_type IS NOT NULL')
      if (!auth$allow_all) where_stage <- c(where_stage, paste0("project_db_id IN (", auth$allowed_subquery, ")"))
      params_stage <- list()
      make_placeholder_csv <- function(n, start_idx) {
        paste(sprintf("$%d", start_idx + seq_len(n) - 1L), collapse = ",")
      }
      p <- 0L
      if (length(ft) > 0) {
        ph <- make_placeholder_csv(length(ft), p + 1L)
        where_stage <- c(where_stage, sprintf("project_type IN (%s)", ph))
        params_stage <- c(params_stage, as.list(ft))
        p <- p + length(ft)
      }
      if (length(fn) > 0) {
        ph <- make_placeholder_csv(length(fn), p + 1L)
        where_stage <- c(where_stage, sprintf("project_id IN (%s)", ph))
        params_stage <- c(params_stage, as.list(fn))
        p <- p + length(fn)
      }
      if (length(manager_ids) > 0) {
        ph <- make_placeholder_csv(length(manager_ids), p + 1L)
        where_stage <- c(where_stage, sprintf("project_db_id IN (SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" IN (%s))", ph))
        params_stage <- c(params_stage, as.list(manager_ids))
        p <- p + length(manager_ids)
      }
      if (length(proj_ids_participant) > 0) {
        ph <- make_placeholder_csv(length(proj_ids_participant), p + 1L)
        where_stage <- c(where_stage, sprintf("project_db_id IN (%s)", ph))
        params_stage <- c(params_stage, as.list(proj_ids_participant))
        p <- p + length(proj_ids_participant)
      }
      if (length(fi) > 0) {
        ph <- make_placeholder_csv(length(fi), p + 1L)
        where_stage <- c(where_stage, sprintf("\"重要紧急程度\" IN (%s)", ph))
        params_stage <- c(params_stage, as.list(fi))
        p <- p + length(fi)
      }
      if (length(proj_ids_hosp) > 0) {
        ph <- make_placeholder_csv(length(proj_ids_hosp), p + 1L)
        where_stage <- c(where_stage, sprintf("project_db_id IN (%s)", ph))
        params_stage <- c(params_stage, as.list(proj_ids_hosp))
      }
      if (!isTRUE(state$include_archived)) {
        where_stage <- c(where_stage, 'COALESCE(project_is_active, true) = true')
      }
      sql_stage <- paste(
        'SELECT * FROM public."v_项目阶段甘特视图_全部"',
        'WHERE', paste(where_stage, collapse = ' AND '),
        'ORDER BY project_id, stage_ord, site_name'
      )
      if (length(params_stage) > 0) {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage, params = params_stage)
      } else {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage)
      }
      if (nrow(stage_rows) == 0) return(stage_rows)
      stage_rows <- as_tibble(stage_rows)
      stage_rows$planned_start_date <- as.Date(stage_rows$planned_start_date)
      stage_rows$actual_start_date  <- as.Date(stage_rows$actual_start_date)
      stage_rows$start_date <- as.Date(stage_rows$start_date)
      stage_rows$planned_end_date <- as.Date(stage_rows$planned_end_date)
      stage_rows$actual_end_date <- as.Date(stage_rows$actual_end_date)
      stage_rows$progress <- norm_progress(stage_rows$progress)
      stage_rows$is_unplanned <- is.na(stage_rows$start_date) | is.na(stage_rows$planned_end_date)
      ss <- stage_catalog()$sync_stages
      site_rows <- stage_rows %>% filter(!task_name %in% ss)
      sync_rows <- stage_rows %>% filter(task_name %in% ss)
      site_name_map <- site_rows %>% distinct(project_id, site_name)
      if (nrow(sync_rows) > 0) {
        sync_expanded <- sync_rows %>% select(-site_name) %>% left_join(site_name_map, by = "project_id", relationship = "many-to-many")
        sync_expanded <- sync_expanded %>% filter(!is.na(site_name))
        sync_without_sites <- sync_rows %>% filter(!(project_id %in% site_name_map$project_id))
        if (nrow(sync_without_sites) > 0) {
          sync_without_sites$site_name <- "所有中心（同步）"
          sync_expanded <- bind_rows(sync_expanded, sync_without_sites)
        }
      } else {
        sync_expanded <- sync_rows
      }
      df <- bind_rows(sync_expanded, site_rows) %>% arrange(project_id, site_name, stage_ord)
      df$raw_planned_start_date <- df$planned_start_date
      df$raw_actual_start_date  <- df$actual_start_date
      df$raw_start_date <- df$start_date
      df$raw_planned_end_date <- df$planned_end_date
      cur_proj <- ""
      cur_site <- ""
      anchor <- today
      for (i in seq_len(nrow(df))) {
        if (df$project_id[i] != cur_proj || df$site_name[i] != cur_site) {
          cur_proj <- df$project_id[i]
          cur_site <- df$site_name[i]
          anchor <- today
        }
        if (isTRUE(df$is_unplanned[i])) {
          df$start_date[i] <- anchor
          df$planned_end_date[i] <- anchor + 30L
        }
        tail_i <- NA
        if (!is.na(df$actual_end_date[i])) {
          tail_i <- df$actual_end_date[i]
        } else if (!is.na(df$planned_end_date[i])) {
          tail_i <- df$planned_end_date[i]
        } else if (!is.na(df$start_date[i])) {
          tail_i <- df$start_date[i]
        }
        if (!is.na(tail_i) && tail_i > anchor) {
          anchor <- tail_i
        }
      }
      # 保留 stage_ord 供 processed_data 计算「最后一个阶段」以放置自定义里程碑占位
      df
    }, error = function(e) NULL)
  })

  # 当前甘特数据源（仅 DB）
  current_gantt_data <- reactive({
    gantt_data_db()
  })

  # 当前同步阶段列表
  sync_stages_current <- reactive({
    stage_catalog()$sync_stages
  })

  # ----- 甘特图 -----
  processed_data <- reactive({
    gd <- current_gantt_data()
    if (is.null(gd) || nrow(gd) == 0) return(list(items = data.frame(), groups = data.frame()))
    # 避免 bind_rows 时 pq_jsonb 与缺失列类型冲突：将 JSON 列统一为 character
    json_cols <- c("remark_json", "contributors_json", "milestones_json", "sample_json")
    for (c in json_cols) if (c %in% names(gd)) gd[[c]] <- as.character(gd[[c]])
    if (!"is_unplanned" %in% names(gd)) gd$is_unplanned <- FALSE
    if (!"project_type" %in% names(gd)) gd$project_type <- NA_character_
    if (!"manager_name" %in% names(gd)) gd$manager_name <- NA_character_
    ss <- sync_stages_current()
    groups_centers <- gd %>%
      filter(!task_name %in% ss) %>%
      group_by(project_id, site_name) %>%
      summarise(
        avg_p = mean(progress),
        project_type = first(project_type),
        importance = { x <- suppressWarnings(na.omit(`重要紧急程度`)); if (length(x) == 0) NA_character_ else x[1] },
        .groups = "drop"
      ) %>%
      arrange(project_id, site_name) %>%
      mutate(
        id = paste0(project_id, "_", site_name),
        line1 = if_else(!is.na(project_type) & nzchar(project_type), paste0(project_type, "-", project_id), project_id),
        content = sprintf("<div style='padding:6px; font-size:14px;'><b>%s</b><br><span style='color:#666; font-size:13px;'>%s</span></div>", line1, site_name)
      ) %>%
      select(-line1)
    
    # 创建同步阶段组（每个项目一个），行名下方显示项目负责人
    groups_sync <- gd %>%
      filter(task_name %in% ss) %>%
      group_by(project_id) %>%
      summarise(
        avg_p = mean(progress),
        project_type = first(project_type),
        manager_name = first(manager_name),
        importance = { x <- suppressWarnings(na.omit(`重要紧急程度`)); if (length(x) == 0) NA_character_ else x[1] },
        .groups = "drop"
      ) %>%
      arrange(project_id) %>%
      mutate(
        id = paste0(project_id, "_同步阶段"),
        line1 = if_else(!is.na(project_type) & nzchar(project_type), paste0(project_type, "-", project_id), project_id),
        mgr_line = if_else(!is.na(manager_name) & nzchar(trimws(as.character(manager_name))), sprintf("<br><span style='color:#888; font-size:13px;'>项目负责人：%s</span>", as.character(manager_name)), ""),
        content = sprintf("<div style='padding:6px; font-size:14px;'><b>%s</b><br><span style='color:#666; font-size:13px;'>%s</span>%s</div>", line1, "各中心同步阶段", mgr_line)
      ) %>%
      select(-line1, -mgr_line)
    
    # 合并所有组，同步阶段放在每个项目的第一个位置
    groups_data <- bind_rows(groups_sync, groups_centers) %>%
      arrange(project_id, desc(id == paste0(project_id, "_同步阶段")), site_name)
    
    # 在每个项目之间插入 mock project 作为分割线
    unique_projects <- unique(groups_data$project_id)
    if (length(unique_projects) > 1) {
      # 为每个项目（除了最后一个）创建一个 mock project 组
      mock_groups <- tibble()
      for (i in 1:(length(unique_projects) - 1)) {
        mock_id <- paste0("MOCK_SEPARATOR_", i)
        mock_groups <- bind_rows(
          mock_groups,
          tibble(
            project_id = paste0("MOCK_", unique_projects[i]),
            id = mock_id,
            content = "<div style='background-color:#E0E0E0; height:4px; padding:0;'></div>",
            avg_p = 0,
            site_name = NA_character_
          )
        )
      }
      
      # 将 mock groups 插入到 groups_data 中
      all_groups_list <- list()
      for (i in seq_along(unique_projects)) {
        # 添加当前项目的所有组
        project_groups <- groups_data %>% filter(project_id == unique_projects[i])
        all_groups_list[[length(all_groups_list) + 1]] <- project_groups
        
        # 如果不是最后一个项目，插入 mock project
        if (i < length(unique_projects)) {
          mock_group <- mock_groups %>% filter(grepl(paste0("MOCK_", unique_projects[i]), project_id))
          if (nrow(mock_group) > 0) {
            all_groups_list[[length(all_groups_list) + 1]] <- mock_group
          }
        }
      }
      groups_data <- bind_rows(all_groups_list)
    }
    
    # 按“重要紧急程度”设置行背景浅色（替代原白/灰交替）
    groups_data <- groups_data %>% mutate(
      className = case_when(
        grepl("MOCK_SEPARATOR", id) ~ "gantt-row-sep",
        !is.null(importance) & importance == "重要紧急" ~ "gantt-bg-重要紧急",
        !is.null(importance) & importance == "重要不紧急" ~ "gantt-bg-重要不紧急",
        !is.null(importance) & importance == "紧急不重要" ~ "gantt-bg-紧急不重要",
        !is.null(importance) & importance == "不重要不紧急" ~ "gantt-bg-不重要不紧急",
        TRUE ~ "gantt-bg-default"
      )
    )
    
    items_centers <- gd %>%
      filter(!task_name %in% ss) %>%
      mutate(
        id = row_number(),
        group = paste0(project_id, "_", site_name),
        
        # 分母：计划周期 = 计划结束 - 计划开始（固定基准）
        planned_duration = as.numeric(planned_end_date - planned_start_date),
        is_completed = !is.na(actual_end_date),
        # 分子：实际周期 = (实际结束 或 今天) - 实际开始（actual_start 有则用，否则退回 planned_start）
        # start_date = COALESCE(actual_start_date, planned_start_date)，正好满足此逻辑
        planned_progress = ifelse(
          is_completed,
            ifelse(planned_duration > 0,
                   as.numeric(actual_end_date - start_date) / planned_duration,
                   1.0),
          ifelse(planned_duration > 0,
                 as.numeric(today - start_date) / planned_duration,
                 ifelse(today >= start_date, 1.0, 0.0))
        ),
        actual_progress = ifelse(is_completed, 1.0, progress),
        progress_diff = actual_progress - planned_progress,

        # 7级颜色分类（红黄绿渐变）
        # 落后区间按：>50%、30%-50%、15%-30%、5%-15%、5%以内
        color_level = case_when(
          progress_diff < -0.5 ~ "落后50%以上",
          progress_diff >= -0.5 & progress_diff < -0.3 ~ "落后30%-50%",
          progress_diff >= -0.3 & progress_diff < -0.15 ~ "落后15%-30%",
          progress_diff >= -0.15 & progress_diff < -0.05 ~ "落后5%-15%",
          progress_diff >= -0.05 & progress_diff < 0.1 ~ "偏差5%以内/超前10%以内",
          progress_diff >= 0.1 & progress_diff < 0.25 ~ "超前10%-25%",
          progress_diff >= 0.25 ~ "超前25%以上"
        ),
        
        # --- 灰态判定：未制定计划(白灰) > 未开始(深灰) > 其他 ---
        is_not_started = (today < start_date & progress == 0 & is.na(actual_end_date)),
        missing_actual_done = is.na(actual_end_date) & progress >= 1.0,
        bg_color = case_when(
          is_unplanned ~ "#EDEDED",
          is_not_started ~ "#9E9E9E",
          missing_actual_done ~ "#2196F3",
          progress_diff < -0.5 ~ "#F44336",
          progress_diff >= -0.5 & progress_diff < -0.3 ~ "#FF6F00",
          progress_diff >= -0.3 & progress_diff < -0.15 ~ "#FFC107",
          progress_diff >= -0.15 & progress_diff < -0.05 ~ "#FFEB3B",
          progress_diff >= -0.05 & progress_diff < 0.1 ~ "#CDDC39",
          progress_diff >= 0.1 & progress_diff < 0.25 ~ "#8BC34A",
          progress_diff >= 0.25 ~ "#4CAF50",
          TRUE ~ "#9E9E9E"
        ),
        border_color = if_else(is_unplanned, "#BDBDBD", bg_color),
        text_color = ifelse(
          is_unplanned, "#555555",
          ifelse(
            is_not_started, "#FFFFFF",
            case_when(
              # 极度滞后或极度超前，用白字
              progress_diff < -0.3 ~ "white",
              progress_diff >= 0.25 ~ "white",
              # 其他区间统一用深色字体，保证 5%-15% 落后区间是黑字
              TRUE ~ "#333333"
            )
          )
        ),
        style = ifelse(is_unplanned,
          sprintf("background-color: %s; border-color: %s; color: %s; border-width: 2px; border-style: solid;", bg_color, border_color, text_color),
          sprintf("background-color: %s; border-color: %s; color: %s; border-width: 2px;", bg_color, border_color, text_color)),
        type = "range",
        start = start_date,
        end = ifelse(
            !is.na(actual_end_date),
            as.character(actual_end_date),
            ifelse(
              today > planned_end_date,
              as.character(today),
              as.character(planned_end_date)
          )
        ),
        content = paste0(vapply(task_name, stage_label_for_key, character(1)), ifelse(is_unplanned, " (未制定计划)", ""), " ", ifelse(!is.na(actual_end_date), 100, round(progress * 100, 0)), "%")
      )
    
    # 处理同步阶段的任务（合并到同步阶段组，每个阶段只保留一条记录）
    items_sync <- gd %>%
      filter(task_name %in% ss) %>%
      group_by(project_id, task_name) %>%
      summarise(
        start_date = first(start_date),
        planned_start_date = first(planned_start_date),
        planned_end_date = first(planned_end_date),
        actual_end_date = first(actual_end_date),
        progress = first(progress),
        task_type = first(task_type),
        is_unplanned = first(is_unplanned),
        .groups = 'drop'
      ) %>%
      mutate(
        id = max(items_centers$id, 0) + row_number(),
        group = paste0(project_id, "_同步阶段"),
        
        # 分母：计划周期 = 计划结束 - 计划开始（固定基准）
        planned_duration = as.numeric(planned_end_date - planned_start_date),
        is_completed = !is.na(actual_end_date),
        # 分子：start_date = COALESCE(actual_start_date, planned_start_date)
        planned_progress = ifelse(
          is_completed,
            ifelse(planned_duration > 0,
                   as.numeric(actual_end_date - start_date) / planned_duration,
                   1.0),
          ifelse(planned_duration > 0,
                 as.numeric(today - start_date) / planned_duration,
                 ifelse(today >= start_date, 1.0, 0.0))
        ),
        actual_progress = ifelse(is_completed, 1.0, progress),
        progress_diff = actual_progress - planned_progress,
        
        # 7级颜色分类（红黄绿渐变）
        # 落后区间按：>50%、30%-50%、15%-30%、5%-15%、5%以内
        color_level = case_when(
          progress_diff < -0.5 ~ "落后50%以上",
          progress_diff >= -0.5 & progress_diff < -0.3 ~ "落后30%-50%",
          progress_diff >= -0.3 & progress_diff < -0.15 ~ "落后15%-30%",
          progress_diff >= -0.15 & progress_diff < -0.05 ~ "落后5%-15%",
          progress_diff >= -0.05 & progress_diff < 0.1 ~ "偏差5%以内/超前10%以内",
          progress_diff >= 0.1 & progress_diff < 0.25 ~ "超前10%-25%",
          progress_diff >= 0.25 ~ "超前25%以上"
        ),
        
        is_not_started = (today < start_date & progress == 0 & is.na(actual_end_date)),
        missing_actual_done = is.na(actual_end_date) & progress >= 1.0,
        bg_color = case_when(
          is_unplanned ~ "#EDEDED",
          is_not_started ~ "#9E9E9E",
          missing_actual_done ~ "#2196F3",
          progress_diff < -0.5 ~ "#F44336",
          progress_diff >= -0.5 & progress_diff < -0.3 ~ "#FF6F00",
          progress_diff >= -0.3 & progress_diff < -0.15 ~ "#FFC107",
          progress_diff >= -0.15 & progress_diff < -0.05 ~ "#FFEB3B",
          progress_diff >= -0.05 & progress_diff < 0.1 ~ "#CDDC39",
          progress_diff >= 0.1 & progress_diff < 0.25 ~ "#8BC34A",
          progress_diff >= 0.25 ~ "#4CAF50",
          TRUE ~ "#9E9E9E"
        ),
        border_color = if_else(is_unplanned, "#BDBDBD", bg_color),
        text_color = ifelse(
          is_unplanned, "#555555",
          ifelse(
            is_not_started, "#FFFFFF",
            case_when(
              progress_diff < -0.3 ~ "white",
              progress_diff >= 0.25 ~ "white",
              TRUE ~ "#333333"
            )
          )
        ),
        style = ifelse(is_unplanned,
          sprintf("background-color: %s; border-color: %s; color: %s; border-width: 2px; border-style: solid;", bg_color, border_color, text_color),
          sprintf("background-color: %s; border-color: %s; color: %s; border-width: 2px;", bg_color, border_color, text_color)),
        type = "range",
        start = start_date,
        end = ifelse(
            !is.na(actual_end_date),
            as.character(actual_end_date),
            ifelse(
              today > planned_end_date,
              as.character(today),
              as.character(planned_end_date)
          )
        ),
        content = paste0(vapply(task_name, stage_label_for_key, character(1)), ifelse(is_unplanned, " (未制定计划)", ""), " ", ifelse(!is.na(actual_end_date), 100, round(progress * 100, 0)), "%")
      )

    # 合并所有任务数据。仅文章类型时 items_centers 为 0 行，直接避免 bind_rows 类型冲突
    if (nrow(items_centers) == 0) {
      items_data <- items_sync
    } else if (nrow(items_sync) == 0) {
      items_data <- items_centers
    } else {
      items_data <- bind_rows(items_centers, items_sync)
    }
    
    # 为每个 mock project 创建对应的 item（灰色填充作为分割线）
    if (length(unique_projects) > 1) {
      # 获取时间范围
      all_dates <- c(items_data$start, as.Date(items_data$end[!is.na(items_data$end)]))
      min_date <- min(all_dates, na.rm = TRUE)
      max_date <- max(all_dates, na.rm = TRUE)
      
      # 为每个 mock group 创建一个 item
      mock_items <- groups_data %>%
        filter(grepl("MOCK_SEPARATOR", id)) %>%
        mutate(
          new_id = max(items_data$id, 0) + row_number(),
          new_group = id,
          new_start = min_date,
          new_end = as.character(max_date),
          new_content = "",
          new_style = "background-color: #E0E0E0 !important; border: none !important; height: 4px !important;",
          new_type = "range"
        ) %>%
        select(id = new_id, group = new_group, start = new_start, end = new_end, 
               content = new_content, style = new_style, type = new_type)
      
      if (nrow(mock_items) > 0) {
        items_data <- bind_rows(items_data, mock_items)
      }
    }

    # -------- 自由里程碑：从阶段实例中的里程碑 JSON 解析，并生成 milestone items --------
    # 使用含未激活阶段的数据源 gd_all，使未勾选 is_active 的阶段仍能显示/编辑里程碑；每阶段一行均有一个「自定义里程碑」入口
    milestone_items <- list()
    site_stage_keys <- stage_catalog()$site_stages
    gd_all <- gantt_data_all_stages()
    gd_milestone <- if (is.null(gd_all) || nrow(gd_all) == 0) gd else gd_all
    if (nrow(gd_milestone) > 0) {
      cur_ms_id <- if (nrow(items_data) > 0) max(items_data$id, na.rm = TRUE) + 1L else 1L
      seen_sync_stage <- character(0)
        sync_with_ms <- character(0)
        site_with_ms <- character(0)
        
      for (ri in seq_len(nrow(gd_milestone))) {
        row <- gd_milestone[ri, ]
          pid <- row$project_id
          sname <- row$site_name
        is_sync_row <- row$task_name %in% ss
          
          if (is_sync_row) {
            sync_key <- paste(pid, as.character(row$task_name), sep = "||")
            if (sync_key %in% seen_sync_stage) next
            seen_sync_stage <- union(seen_sync_stage, sync_key)
          }

        group_id <- if (is_sync_row) paste0(pid, "_同步阶段") else paste0(pid, "_", sname)
        ms_json <- if ("milestones_json" %in% names(row)) row$milestones_json else NULL
          this_stage_has_ms <- FALSE

          if (!is.null(ms_json) && !is.na(ms_json)) {
            ms_df_current <- parse_milestone_json_to_df(ms_json)
            if (nrow(ms_df_current) > 0) {
              ms_df_current <- ms_df_current[ms_df_current$stage_key == as.character(row$task_name), , drop = FALSE]
              for (mi in seq_len(nrow(ms_df_current))) {
                ms_name <- ms_df_current$name[mi]
                plan_str <- ifelse(nzchar(ms_df_current$plan[mi]), ms_df_current$plan[mi], "无")
                act_str <- ifelse(nzchar(ms_df_current$actual[mi]), ms_df_current$actual[mi], "无")

                if (!identical(plan_str, "无")) {
                  this_stage_has_ms <- TRUE
                  milestone_items[[length(milestone_items) + 1]] <- data.frame(
                    id = cur_ms_id,
                    group = group_id,
                    start = plan_str,
                    end = NA,
                    content = paste0(ms_name, "（计划）"),
                    style = "color:#0D47A1; border-color:#1976D2; background-color:#BBDEFB;",
                    type = "point",
                    milestone_name = ms_name,
                    milestone_kind = "plan",
                  milestone_stage_key = as.character(row$task_name),
                    stringsAsFactors = FALSE
                  )
                  cur_ms_id <- cur_ms_id + 1L
                }
                if (!identical(act_str, "无")) {
                  this_stage_has_ms <- TRUE
                  milestone_items[[length(milestone_items) + 1]] <- data.frame(
                    id = cur_ms_id,
                    group = group_id,
                    start = act_str,
                    end = NA,
                    content = paste0(ms_name, "（实际）"),
                    style = "color:#1B5E20; border-color:#388E3C; background-color:#C8E6C9;",
                    type = "point",
                    milestone_name = ms_name,
                    milestone_kind = "actual",
                  milestone_stage_key = as.character(row$task_name),
                    stringsAsFactors = FALSE
                  )
                  cur_ms_id <- cur_ms_id + 1L
                }
              }
            }
          }
          if (this_stage_has_ms) {
            if (is_sync_row) {
              sync_with_ms <- union(sync_with_ms, as.character(pid))
            } else {
              site_with_ms <- union(site_with_ms, paste(pid, sname, sep = "||"))
            }
          }
        }

      # 每个子进程（每组）仅一个「自定义里程碑」入口，跟在最后一个阶段后面；本组已有里程碑则不显示占位
      if ("stage_ord" %in% names(gd_milestone)) {
        last_sync_stage <- gd_milestone %>%
          filter(task_name %in% ss) %>%
          group_by(project_id) %>%
          summarise(last_task_name = task_name[which.max(stage_ord)][1], .groups = "drop")
        last_site_stage <- gd_milestone %>%
          filter(task_name %in% site_stage_keys) %>%
          group_by(project_id, site_name) %>%
          summarise(last_task_name = task_name[which.max(stage_ord)][1], .groups = "drop")
      } else {
        last_sync_stage <- gd_milestone %>%
          filter(task_name %in% ss) %>%
          mutate(ord = match(task_name, ss)) %>%
          group_by(project_id) %>%
          summarise(last_task_name = task_name[which.max(ord)[1]], .groups = "drop")
        last_site_stage <- gd_milestone %>%
          filter(task_name %in% site_stage_keys) %>%
          mutate(ord = match(task_name, site_stage_keys)) %>%
          group_by(project_id, site_name) %>%
          summarise(last_task_name = task_name[which.max(ord)[1]], .groups = "drop")
      }
          for (k in seq_len(nrow(last_sync_stage))) {
            lp <- last_sync_stage$project_id[k]
            last_task <- last_sync_stage$last_task_name[k]
            if (lp %in% sync_with_ms) next
        idx <- which(gd_milestone$project_id == lp & gd_milestone$task_name == last_task & gd_milestone$task_name %in% ss)[1]
            if (is.na(idx)) next
        row_k <- gd_milestone[idx, ]
        end_date <- row_k$actual_end_date
        if (is.na(end_date) || is.null(end_date)) end_date <- row_k$planned_end_date
        if (is.na(end_date) || is.null(end_date)) end_date <- today
            milestone_items[[length(milestone_items) + 1]] <- data.frame(
              id = cur_ms_id,
          group = paste0(lp, "_同步阶段"),
              start = as.character(end_date),
              end = NA,
              content = "自定义里程碑",
              style = "color:#37474F; border-color:#78909C; background-color:#ECEFF1;",
              type = "point",
              milestone_name = NA_character_,
              milestone_kind = "placeholder",
          milestone_stage_key = as.character(last_task),
              stringsAsFactors = FALSE
            )
            cur_ms_id <- cur_ms_id + 1L
          }
          for (k in seq_len(nrow(last_site_stage))) {
            lp <- last_site_stage$project_id[k]
            ls <- last_site_stage$site_name[k]
            last_task <- last_site_stage$last_task_name[k]
        if (paste(lp, ls, sep = "||") %in% site_with_ms) next
        idx <- which(gd_milestone$project_id == lp & gd_milestone$site_name == ls & gd_milestone$task_name == last_task & gd_milestone$task_name %in% site_stage_keys)[1]
            if (is.na(idx)) next
        row_k <- gd_milestone[idx, ]
        end_date <- row_k$actual_end_date
        if (is.na(end_date) || is.null(end_date)) end_date <- row_k$planned_end_date
        if (is.na(end_date) || is.null(end_date)) end_date <- today
            milestone_items[[length(milestone_items) + 1]] <- data.frame(
              id = cur_ms_id,
          group = paste0(lp, "_", ls),
              start = as.character(end_date),
              end = NA,
              content = "自定义里程碑",
              style = "color:#37474F; border-color:#78909C; background-color:#ECEFF1;",
              type = "point",
              milestone_name = NA_character_,
              milestone_kind = "placeholder",
          milestone_stage_key = as.character(last_task),
              stringsAsFactors = FALSE
            )
            cur_ms_id <- cur_ms_id + 1L
      }
    }

    if (length(milestone_items) > 0) {
      ms_df <- bind_rows(milestone_items)
      # 统一时间列类型，避免 date 与 character 混合导致 bind_rows 报错
      if ("start" %in% names(items_data)) items_data$start <- as.character(items_data$start)
      if ("end" %in% names(items_data))   items_data$end   <- as.character(items_data$end)
      if ("start" %in% names(ms_df))      ms_df$start      <- as.character(ms_df$start)
      if ("end" %in% names(ms_df))        ms_df$end        <- as.character(ms_df$end)
      items_data <- bind_rows(items_data, ms_df)
    }

    list(
      items = items_data,
      groups = groups_data
    )
  })
  
  output$my_gantt <- renderTimevis({
    data <- processed_data()
    
    tv <- timevis(
      data = data$items,
      groups = data$groups,
      options = list(
        format = list(
          minorLabels = list(month = "M月", year = "YYYY年"),
          majorLabels = list(month = "YYYY年M月", year = "YYYY年")
        ),
        showCurrentTime = TRUE,
        orientation = "top",
        verticalScroll = TRUE,
        zoomKey = "altKey"
      )
    )
    
    
    tv
  })
  
  observeEvent(input$my_gantt_selected, {
    req(input$my_gantt_selected)
    task_id <- as.numeric(input$my_gantt_selected)
    data <- processed_data()
    task_info <- data$items[data$items$id == task_id, ]

    # 如果点击的是自由里程碑相关的点（包括占位点），且该行里确实标记了 milestone_kind，
    # 才进入里程碑查看/编辑逻辑；普通进度条行不应该触发这里。
    if ("milestone_kind" %in% names(task_info) &&
        nrow(task_info) == 1 &&
        !is.na(task_info$milestone_kind[1])) {
      ms_row <- task_info[1, ]
      # 提取项目和中心
      project_id_from_group <- sub("_.*$", "", ms_row$group)
      site_name_from_group <- sub("^[^_]+_", "", ms_row$group)
      is_sync <- grepl("同步阶段", ms_row$group)

      ss <- sync_stages_current()
      stage_key_clicked <- if ("milestone_stage_key" %in% names(ms_row) && !is.na(ms_row$milestone_stage_key[1])) ms_row$milestone_stage_key[1] else NA_character_
      # 用含未激活阶段的数据源查找阶段行，使未勾选 is_active 的阶段仍能点开里程碑编辑
      gd_lookup <- gantt_data_all_stages()
      if (is.null(gd_lookup) || nrow(gd_lookup) == 0) gd_lookup <- current_gantt_data()
      if (is.null(gd_lookup) || nrow(gd_lookup) == 0) return(NULL)
      if (is_sync) {
        original_task <- gd_lookup %>%
          filter(project_id == project_id_from_group, task_name %in% ss) %>%
          { if (nzchar(stage_key_clicked)) filter(., task_name == stage_key_clicked) else . } %>%
          slice(1)
      } else {
        original_task <- gd_lookup %>%
          filter(project_id == project_id_from_group, site_name == site_name_from_group, !task_name %in% ss) %>%
          { if (nzchar(stage_key_clicked)) filter(., task_name == stage_key_clicked) else . } %>%
          slice(1)
      }
      if (nrow(original_task) == 0) return(NULL)

      proj_row_id <- if ("proj_row_id" %in% names(original_task)) original_task$proj_row_id[1] else NA_integer_
      site_row_id <- if ("site_row_id" %in% names(original_task)) original_task$site_row_id[1] else NA_integer_
      stage_instance_id <- if ("stage_instance_id" %in% names(original_task)) original_task$stage_instance_id[1] else NA_integer_

      # 读取现有里程碑 JSON
      ms_json <- if ("milestones_json" %in% names(original_task)) original_task$milestones_json[1] else NULL

      # 解析为 data.frame(entry_key, stage_key, name, plan, actual, note)
      ms_df <- empty_milestone_df()
      stage_key <- as.character(original_task$task_name[1])
      if (!is.null(ms_json) && !is.na(ms_json)) {
        ms_df <- parse_milestone_json_to_df(ms_json)
        if (nrow(ms_df) > 0) {
          ms_df <- ms_df[ms_df$stage_key == stage_key, , drop = FALSE]
        }
      }
      milestone_row_count(nrow(ms_df))
      task_edit_context(list(
        project_id = project_id_from_group,
        site_name = if (is_sync) "各中心同步阶段" else site_name_from_group,
        task_name = stage_key,
        is_sync = is_sync,
        proj_row_id = proj_row_id,
        site_row_id = site_row_id,
        stage_instance_id = stage_instance_id,
        table_name = "09项目阶段实例表",
        milestones = ms_df,
        milestone_raw = ms_json,
        milestone_stage_key = stage_key
      ))

      # 先展示只读的里程碑概览，再由用户点击进入编辑页
      showModal(modalDialog(
        title = "自由里程碑概览",
        size = "l",
        easyClose = TRUE,
        footer = tagList(
          actionButton("btn_open_milestone_edit", "修改 / 新增里程碑", class = "btn-primary"),
          modalButton("关闭")
        ),
        uiOutput("milestone_viewer")
      ))
      return(NULL)
    }
    
    # 从 group 中提取 project_id 和 site_name（非同步阶段）
    project_id_from_group <- sub("_.*$", "", task_info$group)
    site_name_from_group <- sub("^[^_]+_", "", task_info$group)
    
    # 判断是否是同步阶段
    is_sync <- grepl("同步阶段", task_info$group)
    
    gd <- current_gantt_data()
    ss <- sync_stages_current()
    if (is_sync) {
      if ("task_name" %in% names(task_info) && !is.na(task_info$task_name)) {
        original_task <- gd %>%
          filter(project_id == project_id_from_group, task_name == task_info$task_name) %>%
          slice(1)
        task_info$site_name <- "所有中心（同步）"
        task_info$project_id <- project_id_from_group
      } else {
        content_text <- task_info$content
        task_name_clean <- sub("\\s+\\d+%$", "", content_text)
        original_task <- gd %>%
          filter(project_id == project_id_from_group) %>%
          filter(task_name %in% ss) %>%
          filter(grepl(task_name_clean, task_name, fixed = TRUE)) %>%
          slice(1)
        task_info$site_name <- "所有中心（同步）"
        task_info$project_id <- project_id_from_group
        task_info$task_name <- original_task$task_name
      }
    } else {
      if ("task_name" %in% names(task_info) && !is.na(task_info$task_name)) {
        original_task <- gd %>%
          filter(project_id == project_id_from_group, site_name == site_name_from_group, task_name == task_info$task_name) %>%
          slice(1)
        task_info$site_name <- original_task$site_name
        task_info$project_id <- project_id_from_group
      } else {
        content_text <- task_info$content
        task_name_clean <- sub("\\s+\\d+%$", "", content_text)
        original_task <- gd %>%
          filter(project_id == project_id_from_group, site_name == site_name_from_group, !task_name %in% ss) %>%
          filter(grepl(task_name_clean, task_name, fixed = TRUE)) %>%
          slice(1)
        task_info$site_name <- original_task$site_name
        task_info$project_id <- project_id_from_group
        task_info$task_name <- original_task$task_name
      }
    }
    
    # 从原始数据中获取计划完成时间和实际完成时间
    planned_end_date <- original_task$planned_end_date
    actual_end_date  <- original_task$actual_end_date
    is_unplanned <- if ("is_unplanned" %in% names(original_task)) isTRUE(original_task$is_unplanned[1]) else FALSE
    start_for_calc <- as.Date(task_info$start)
    proj_row_id <- if ("proj_row_id" %in% names(original_task)) original_task$proj_row_id[1] else NA_integer_
    site_row_id <- if ("site_row_id" %in% names(original_task)) original_task$site_row_id[1] else NA_integer_
    # 提前计算 actual_progress 供 task_edit_context 使用
    progress_match <- regmatches(task_info$content, regexpr("\\d+%", task_info$content))
    reported_progress <- if (length(progress_match) > 0) as.numeric(sub("%", "", progress_match)) / 100 else original_task$progress
    is_completed <- !is.na(actual_end_date)
    actual_progress <- ifelse(is_completed, 1.0, reported_progress)
    
    raw_planned_start <- if ("raw_planned_start_date" %in% names(original_task)) original_task$raw_planned_start_date[1] else (if (is_unplanned) as.Date(NA) else start_for_calc)
    raw_actual_start  <- if ("raw_actual_start_date"  %in% names(original_task)) original_task$raw_actual_start_date[1]  else as.Date(NA)
    raw_planned <- if ("raw_planned_end_date" %in% names(original_task)) original_task$raw_planned_end_date[1] else (if (is_unplanned) as.Date(NA) else planned_end_date)
    tn <- as.character(task_info$task_name[1])
    stage_instance_id <- if ("stage_instance_id" %in% names(original_task)) original_task$stage_instance_id[1] else NA_integer_
    cm <- list(
      planned_start = "planned_start_date",
      actual_start  = "actual_start_date",
      plan = "planned_end_date",
      act = "actual_end_date",
      note = "remark_json",
      progress = "progress"
    )
    note_col <- "remark_json"
    contrib_col <- "contributors_json"
    tbl_main <- "09项目阶段实例表"
    row_id_main <- stage_instance_id

    snapshot_cols <- c("planned_start_date", "actual_start_date", "planned_end_date", "actual_end_date", "remark_json", "progress", "contributors_json", "milestones_json", "sample_json", "row_version")
    is_s09_task <- identical(as.character(task_info$task_name[1]), "S09_验证试验开展与数据管理") && is_sync
    snapshot_row <- if (!is.na(row_id_main) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      tryCatch(fetch_row_snapshot(pg_con, tbl_main, row_id_main, snapshot_cols, lock = FALSE), error = function(e) NULL)
    } else {
      NULL
    }
    snapshot_project_row <- if (!is.na(proj_row_id) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      tryCatch(fetch_row_snapshot(pg_con, "04项目总表", proj_row_id, "重要紧急程度", lock = FALSE), error = function(e) NULL)
    } else {
      NULL
    }
    remark_raw <- if (!is.null(snapshot_row)) snapshot_row[[note_col]] else NA_character_
    remarks_df <- parse_remark_json_to_df(remark_raw)

    sample_pairs <- if (is_s09_task && !is.null(snapshot_row)) {
      parse_sample_df(snapshot_row[["sample_json"]])
    } else {
      empty_sample_df()
    }
    contrib_df <- if (!is.null(snapshot_row) && contrib_col %in% names(snapshot_row)) {
      parse_contrib_json_to_df(snapshot_row[[contrib_col]])
    } else {
      empty_contrib_df()
    }
    milestone_raw <- if (!is.null(snapshot_row) && "milestones_json" %in% names(snapshot_row)) snapshot_row[["milestones_json"]] else NA_character_
    milestones_df <- parse_milestone_json_to_df(milestone_raw)

    can_edit <- !is.null(pg_con) && DBI::dbIsValid(pg_con) &&
      !is.na(stage_instance_id)
    if (can_edit) {
      if (is_s09_task) {
        sample_row_count(if (is.null(sample_pairs)) 0L else nrow(sample_pairs))
      } else {
        sample_row_count(0L)
      }
      contrib_row_count(nrow(contrib_df))
      remark_row_count(nrow(remarks_df))
      task_edit_context(list(
        project_id = task_info$project_id[1],
        site_name = task_info$site_name[1],
        task_name = tn,
        is_sync = is_sync,
        project_type = if ("project_type" %in% names(original_task)) original_task$project_type[1] else NA_character_,
        importance = if ("重要紧急程度" %in% names(original_task)) as.character(original_task[["重要紧急程度"]][1]) else NA_character_,
        planned_start_date = raw_planned_start,
        actual_start_date  = raw_actual_start,
        planned_end_date = raw_planned,
        actual_end_date = actual_end_date,
        progress = actual_progress,
        stage_instance_id = stage_instance_id,
        remark = remark_raw,
        remark_entries = remarks_df,
        proj_row_id = proj_row_id,
        site_row_id = site_row_id,
        col_map = cm,
        table_name = tbl_main,
        samples = sample_pairs,
        contributors = contrib_df,
        milestones = milestones_df,
        milestone_raw = milestone_raw,
        milestone_stage_key = tn,
        note_col = note_col,
        contrib_col = contrib_col,
        snapshot_row = snapshot_row,
        snapshot_project_row = snapshot_project_row
      ))
    } else {
      task_edit_context(NULL)
    }
    
    # 分母：计划周期 = 计划结束 - 计划开始（固定基准）
    planned_duration <- as.numeric(planned_end_date - raw_planned_start)
    # 分子起点：实际开始有则用，否则退回计划开始
    eff_start_detail <- if (!is.na(raw_actual_start)) raw_actual_start else raw_planned_start
    # 理论计划进度 = 实际周期 / 计划周期
    planned_p <- ifelse(
      is_completed,
      ifelse(planned_duration > 0,
             as.numeric(actual_end_date - eff_start_detail) / planned_duration,
             1.0),
      ifelse(planned_duration > 0,
             as.numeric(today - eff_start_detail) / planned_duration,
             ifelse(!is.na(eff_start_detail) && today >= eff_start_detail, 1.0, 0.0))
    )
    
    # 计划/实际完成日期差异（仅在有 actual_end_date 时计算）
    delay_days <- if (!is.na(actual_end_date)) {
      as.numeric(actual_end_date - planned_end_date)
    } else {
      NA_real_
    }
    
    # 颜色用数值差额计算：实际完成度 - 理论计划进度
    diff_p <- actual_progress - planned_p
    
    # --- 核心修复：弹窗诊断文案更新 ---
    # 是否为“已报完成但未填写实际完成日期”
    missing_actual_done <- is.na(actual_end_date) && reported_progress >= 1.0
    
    diagnostic_text <- if (today < as.Date(task_info$start) && actual_progress == 0) {
      span("⏳ 尚未开始：未到计划启动时间。", style = "color: #757575; font-weight: bold;")
    } else if (missing_actual_done) {
      span("ℹ️ 已报完成，但未填写实际完成日期，请补充“实际完成时间”。", 
           style = "color: #1976D2; font-weight: bold;")
    } else if(diff_p < -0.5) {
      span("❌ 严重落后：进度严重滞后于计划（落后50%以上），请排查！", style = "color: #D32F2F; font-weight: bold;")
    } else if(diff_p < -0.3) {
      span("⚠️ 明显滞后：进度滞后于计划（落后30%-50%），需加快推进。", style = "color: #F44336; font-weight: bold;")
    } else if(diff_p < -0.15) {
      span("⚠️ 轻微滞后：进度滞后于计划（落后15%-30%），需注意。", style = "color: #FF6F00;")
    } else if(diff_p < -0.05) {
      span("⚠️ 轻微滞后：进度滞后于计划（落后5%-15%），需留意。", style = "color: #FFB300;")
    } else if(diff_p < 0.1) {
      span("ℹ️ 进度基本匹配：偏差在5%以内/略微超前（超前10%以内）。", style = "color: #558B2F;")
    } else if(diff_p >= 0.25) {
      span("✅ 大幅超前：执行效率极高（超前25%以上）！", style = "color: #2E7D32; font-weight: bold;")
    } else if(diff_p >= 0.1) {
      span("✅ 明显超前：执行效率较高（超前10%-25%）。", style = "color: #4CAF50; font-weight: bold;")
    } else {
      span("🏃 进度平稳：符合预期计划。", style = "color: #1565C0;")
    }
    
    # 查询项目负责人和参与人员（岗位-姓名）
    manager_display <- "（无）"
    participants_display <- character(0)
    if (!is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      tryCatch({
        pid <- task_info$project_id[1]
        proj_id_db <- if (!is.na(proj_row_id) && proj_row_id > 0) as.integer(proj_row_id) else NA_integer_
        if (is.na(proj_id_db)) {
          if (grepl("^项目-[0-9]+$", pid)) {
            proj_id_db <- as.integer(sub("^项目-", "", pid))
          } else {
            pr <- DBI::dbGetQuery(pg_con, 'SELECT id FROM public."04项目总表" WHERE "项目名称" = $1', params = list(pid))
            if (nrow(pr) > 0) proj_id_db <- pr$id[1]
          }
        }
        if (!is.na(proj_id_db)) {
          mgr <- DBI::dbGetQuery(pg_con,
            'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."04项目总表" proj ON proj."05人员表_id" = p.id WHERE proj.id = $1',
            params = list(as.integer(proj_id_db)))
          if (nrow(mgr) > 0) {
            pos <- if (is.na(mgr[["岗位"]][1]) || !nzchar(trimws(as.character(mgr[["岗位"]][1])))) "" else as.character(mgr[["岗位"]][1])
            nm <- if (is.na(mgr[["姓名"]][1])) "" else as.character(mgr[["姓名"]][1])
            manager_display <- if (nzchar(pos)) paste0(pos, "-", nm) else (if (nzchar(nm)) nm else "（无）")
          }
          parts <- DBI::dbGetQuery(pg_con,
                'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."05人员表_id" = p.id WHERE m."04项目总表_id" = $1',
                params = list(as.integer(proj_id_db)))
          if (nrow(parts) > 0) {
            pos_order <- c("工程师" = 1L, "CRA" = 2L, "统计师" = 3L, "实习生" = 4L)
            ord <- vapply(seq_len(nrow(parts)), function(i) {
              pos <- if (is.na(parts[["岗位"]][i]) || !nzchar(trimws(as.character(parts[["岗位"]][i])))) "" else trimws(as.character(parts[["岗位"]][i]))
              if (pos %in% names(pos_order)) pos_order[[pos]] else 99L
            }, integer(1))
            parts <- parts[order(ord, parts[["姓名"]]), , drop = FALSE]
            participants_display <- vapply(seq_len(nrow(parts)), function(i) {
              pos <- if (is.na(parts[["岗位"]][i]) || !nzchar(trimws(as.character(parts[["岗位"]][i])))) "" else as.character(parts[["岗位"]][i])
              nm <- if (is.na(parts[["姓名"]][i])) "" else as.character(parts[["姓名"]][i])
              if (nzchar(pos)) paste0(pos, "-", nm) else (if (nzchar(nm)) nm else "")
            }, character(1))
            participants_display <- participants_display[nzchar(participants_display)]
          }
        }
      }, error = function(e) { })
    }
    
    # 项目紧急程度（用于弹窗显示与颜色）
    importance_level <- if ("重要紧急程度" %in% names(original_task)) as.character(original_task[["重要紧急程度"]][1]) else NA_character_
    importance_display <- if (is.na(importance_level) || !nzchar(trimws(importance_level))) "（未设置）" else trimws(importance_level)
    importance_color <- switch(
      trimws(importance_level),
      "重要紧急" = "#C62828",
      "重要不紧急" = "#F57C00",
      "紧急不重要" = "#1976D2",
      "不重要不紧急" = "#616161",
      "#757575"
    )
    
    auth <- current_user_auth()
    showModal(modalDialog(
      title = "任务详细进度核查",
      size = "l",
      easyClose = TRUE,
      footer = tagList(
        if (isTRUE(auth$is_super_admin)) tags$span(style = "float: left;",
          actionButton("btn_stage_maintain", "阶段维护", class = "btn-default", title = "管理 08 阶段定义与 09 阶段实例")
        ),
        if (can_edit) tagList(
          actionButton("btn_edit_task", "修改/更新数据", class = "btn-primary"),
          actionButton("btn_edit_contrib", "修改贡献者信息", class = "btn-default")
        ),
        modalButton("关闭")
      ),
      
      fluidRow(
        column(6,
               p(tags$b("项目："), task_info$project_id),
               p(tags$b("中心："), task_info$site_name),
               p(tags$b("阶段："), stage_label_for_key(task_info$task_name)),
               p(tags$b("项目紧急程度："), tags$span(importance_display, style = sprintf("color: %s; font-weight: bold;", importance_color))),
               p(tags$b("项目负责人："), manager_display),
               p(tags$b("项目参与人员名单："),
                 if (length(participants_display) > 0)
                   tags$div(style = "white-space: pre-wrap; margin-top: 4px;", paste(participants_display, collapse = "\n"))
                 else tags$span("（无）", style = "color: #999;")),
               p(tags$b("计划开始日期："), if (length(raw_planned_start) > 0 && !is.na(raw_planned_start)) as.character(raw_planned_start) else "无"),
               p(tags$b("实际开始日期："), if (length(raw_actual_start) > 0 && !is.na(raw_actual_start)) as.character(raw_actual_start) else "无"),
               p(tags$b("计划结束日期："), if (length(raw_planned) > 0 && !is.na(raw_planned)) as.character(raw_planned) else "无"),
               p(tags$b("实际结束日期："), if (length(actual_end_date) > 0 && !is.na(actual_end_date)) as.character(actual_end_date) else "无"),
               p(tags$b("提前/延后："),
                 if (is_unplanned) "—" else
                 ifelse(is.na(actual_end_date),
                        "—",
                        ifelse(delay_days > 0,
                               paste0("延后 ", delay_days, " 天"),
                               ifelse(delay_days < 0,
                                      paste0("提前 ", abs(delay_days), " 天"),
                                      "按计划完成"))))
        ),
        column(6,
               p(tags$b("问题、卡点反馈与经验分享：")),
               {
                 remark_df <- if (!is.null(task_edit_context())) task_edit_context()$remark_entries else empty_remark_df()
                 if (!is.null(remark_df) && nrow(remark_df) > 0) {
                  tags$ul(
                    style = "white-space: normal; word-break: break-all; overflow-wrap: anywhere; padding-left: 20px;",
                     lapply(seq_len(nrow(remark_df)), function(i) {
                       prefix_txt <- paste0(
                        if (nzchar(remark_df$reporter[i])) paste0(remark_df$reporter[i], " - ") else "",
                        if (nzchar(remark_df$type[i])) paste0("【", remark_df$type[i], "】") else ""
                       )
                      tags$li(
                        style = "white-space: normal; word-break: break-all; overflow-wrap: anywhere;",
                        prefix_txt,
                        if (nzchar(remark_df$updated_at[i])) " - " else "",
                        if (nzchar(remark_df$updated_at[i])) {
                          tags$span(
                            style = remark_date_style(remark_df$updated_at[i]),
                            remark_df$updated_at[i]
                          )
                        },
                        "：",
                        remark_df$content[i]
                      )
                     })
                   )
                 } else {
                   tags$span("（无）", style = "color: #999;")
                 }
               },
               if (is_s09_task) {
                 tagList(
                   tags$hr(),
                   p(tags$b("样本来源与数量：")),
                   if (!is.null(sample_pairs) && nrow(sample_pairs) > 0) {
                     tags$pre(
                       style = "white-space: pre-wrap; background: #f5f5f5; padding: 8px; border-radius: 4px; font-size: 14px;",
                       paste(
                         vapply(seq_len(nrow(sample_pairs)), function(i) {
                           sprintf("%s：%s", as.character(sample_pairs$hospital[i]), as.character(sample_pairs$count[i]))
                         }, character(1)),
                         collapse = "\n"
                       )
                     )
                   } else {
                     tags$span("（暂无样本来源与数量记录）", style = "color: #999;")
                   }
                 )
               },
               tags$hr(),
               p(tags$b("进度贡献者：")),
               {
                 contrib_df <- if (!is.null(task_edit_context())) task_edit_context()$contributors else NULL
                 if (!is.null(contrib_df) && nrow(contrib_df) > 0) {
                   tags$ul(
                     lapply(seq_len(nrow(contrib_df)), function(i) {
                       row <- contrib_df[i, ]
                       txt <- paste0(
                         row$person, "：", row$role, " - ", row$work,
                         if (nzchar(row$amount)) paste0("（数量：", row$amount, "）") else ""
                       )
                       tags$li(txt)
                     })
                   )
                 } else {
                   tags$span("（暂无进度贡献者记录）", style = "color:#999;")
                 }
               }
        )
      ),
      tags$hr(),
      
      h4("进度诊断对比"),
      if (is_unplanned) {
        p(tags$span("（未制定计划，无相关数据）", style = "color: #757575;"))
      } else {
        tagList(
      p(tags$b("理论计划进度："), tags$span(sprintf("%.1f%%", planned_p * 100), style="color:gray;"),
        if(planned_p > 1.0) tags$span(" (已超过计划完成时间)", style="color:orange; font-size:0.9em;")),
      p(tags$b("实际提报进度："), tags$span(sprintf("%.1f%%", actual_progress * 100), style="font-weight:bold;")),
      p(tags$b("执行差值 (实际-计划)："), sprintf("%.1f%%", diff_p * 100)),
          p(tags$b("系统诊断结果："), diagnostic_text)
        )
      }
    ))
  })
  
  open_task_edit_modal <- function(ctx) {
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    removeModal()
    psd <- ctx$planned_start_date
    asd <- ctx$actual_start_date
    pd <- ctx$planned_end_date
    ad <- ctx$actual_end_date
    proj_type <- ctx$project_type
    stages_for_proj <- stage_keys_for_project_type(proj_type)
    cur_idx <- which(stages_for_proj == ctx$task_name)[1]
    prev_name <- prev_plan <- prev_act <- "（无）"
    if (!is.na(cur_idx) && cur_idx > 1L) {
      prev_stage <- stages_for_proj[cur_idx - 1L]
      prev_name <- stage_label_for_key(prev_stage)
      gd <- current_gantt_data()
      if (!is.null(gd) && nrow(gd) > 0) {
        prev_row <- if (ctx$is_sync) {
          gd %>% filter(project_id == ctx$project_id, task_name == prev_stage) %>% slice(1)
        } else {
          gd %>% filter(project_id == ctx$project_id, site_name == ctx$site_name, task_name == prev_stage) %>% slice(1)
        }
        if (nrow(prev_row) > 0) {
          pp <- if ("raw_planned_end_date" %in% names(prev_row)) prev_row$raw_planned_end_date[1] else prev_row$planned_end_date[1]
          pa <- prev_row$actual_end_date[1]
          prev_plan <- if (length(pp) > 0 && !is.na(pp)) format(pp, "%Y-%m-%d") else "（无）"
          prev_act <- if (length(pa) > 0 && !is.na(pa)) format(pa, "%Y-%m-%d") else "（无）"
        }
      }
    }
    edit_title <- sprintf(
      "%s-%s-%s-修改/更新数据",
      if (!is.na(ctx$project_type) && nzchar(ctx$project_type)) as.character(ctx$project_type) else "未知",
      ctx$project_id,
      if (ctx$is_sync) "各中心同步阶段" else ctx$site_name
    )
    if (identical(ctx$task_name, "S09_验证试验开展与数据管理") && ctx$is_sync) {
      n_init <- if (!is.null(ctx$samples)) nrow(ctx$samples) else 0L
      sample_row_count(n_init)
    } else {
      sample_row_count(0L)
    }
    remark_row_count(if (!is.null(ctx$remark_entries)) nrow(ctx$remark_entries) else 0L)
    showModal(modalDialog(
      title = edit_title,
      size = "l",
      easyClose = TRUE,
      footer = tagList(
        actionButton("btn_save_task", "保存", class = "btn-primary"),
        modalButton("取消")
      ),
      fluidRow(
        column(6,
          tags$div(style = "background:#f5f5f5; padding:10px; border-radius:4px; margin-bottom:15px;",
            tags$b("上阶段信息（参考）"),
            p(tags$b("上阶段名称："), prev_name),
            p(tags$b("计划结束时间："), prev_plan),
            p(tags$b("实际结束日期："), prev_act)
          ),
          p(tags$span(stage_label_for_key(ctx$task_name), style = "color: #1976D2; font-weight: bold;")),
          textInput("edit_planned_start_date", "计划开始日期：", value = if (length(psd) > 0 && !is.na(psd)) format(psd, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未制定计划"),
          textInput("edit_actual_start_date",  "实际开始日期：", value = if (length(asd) > 0 && !is.na(asd)) format(asd, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，已开始则填写，优先用于位置与色彩计算"),
          textInput("edit_planned_date", "计划结束日期：", value = if (length(pd) > 0 && !is.na(pd)) format(pd, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未制定计划"),
          textInput("edit_actual_date", "实际结束日期：", value = if (length(ad) > 0 && !is.na(ad)) format(ad, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未完成"),
          tags$p(tags$small("（日期格式：YYYY-MM-DD；实际开始日期已填时优先参与甘特位置与色彩计算）")),
          sliderInput("edit_progress", "调整当前实际进度：", 0, 100, round((if (is.null(ctx$progress)) 0 else ctx$progress) * 100), post = "%")
        ),
        column(6,
          selectInput("edit_importance", "项目紧急程度：",
            choices = c("重要紧急", "重要不紧急", "紧急不重要", "不重要不紧急"),
            selected = {
              cur <- if (is.null(ctx$importance) || is.na(ctx$importance)) "" else trimws(ctx$importance)
              if (cur %in% c("重要紧急", "重要不紧急", "紧急不重要", "不重要不紧急")) cur else "重要紧急"
            }),
          uiOutput("remark_editor"),
          actionButton("btn_add_remark_row", "新增问题/卡点/经验分享", class = "btn-success"),
          uiOutput("sample_pairs_editor")
        )
      )
    ))
  }

  refresh_task_context_from_db <- function(ctx) {
    main_cols <- c(ctx$col_map$planned_start, ctx$col_map$actual_start, ctx$col_map$plan, ctx$col_map$act, ctx$col_map$note, ctx$col_map$progress)
    if (identical(ctx$task_name, "S09_验证试验开展与数据管理") && ctx$is_sync) {
      main_cols <- c(main_cols, "sample_json")
    }
    latest_row <- fetch_row_snapshot(pg_con, ctx$table_name, ctx$stage_instance_id, main_cols, lock = FALSE)
    if (is.null(latest_row)) return(NULL)
    ctx$snapshot_row <- latest_row
    if (!is.na(ctx$proj_row_id)) {
      ctx$snapshot_project_row <- fetch_row_snapshot(pg_con, "04项目总表", ctx$proj_row_id, "重要紧急程度", lock = FALSE)
      if (!is.null(ctx$snapshot_project_row)) ctx$importance <- ctx$snapshot_project_row[["重要紧急程度"]]
    }
    ctx$planned_start_date <- suppressWarnings(as.Date(latest_row[[ctx$col_map$planned_start]]))
    ctx$actual_start_date  <- suppressWarnings(as.Date(latest_row[[ctx$col_map$actual_start]]))
    ctx$planned_end_date <- suppressWarnings(as.Date(latest_row[[ctx$col_map$plan]]))
    ctx$actual_end_date <- suppressWarnings(as.Date(latest_row[[ctx$col_map$act]]))
    ctx$progress <- suppressWarnings(as.numeric(latest_row[[ctx$col_map$progress]]) / 100)
    ctx$remark <- latest_row[[ctx$note_col]]
    ctx$remark_entries <- parse_remark_json_to_df(latest_row[[ctx$note_col]])
    if (identical(ctx$task_name, "S09_验证试验开展与数据管理") && ctx$is_sync) {
      ctx$samples <- parse_sample_df(latest_row[["sample_json"]])
    }
    ctx
  }

  observeEvent(input$btn_edit_task, {
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    open_task_edit_modal(ctx)
  })

  observeEvent(input$btn_stage_maintain, {
    auth <- current_user_auth()
    if (!isTRUE(auth$is_super_admin)) return()
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    pt <- ctx$project_type
    req(!is.na(pt) && nzchar(pt))
    stage_maintain_context(list(
      proj_row_id = ctx$proj_row_id,
      site_row_id = ctx$site_row_id,
      project_id = ctx$project_id,
      site_name = ctx$site_name,
      project_type = pt,
      is_sync = ctx$is_sync
    ))
    showModal(modalDialog(
      title = "阶段维护",
      size = "l",
      easyClose = TRUE,
      footer = modalButton("关闭"),
      tabsetPanel(
        id = "stage_maintain_tabs",
        tabPanel("08 阶段定义", value = "tab1", uiOutput("stage_maintain_tab1_ui")),
        tabPanel("09 阶段实例", value = "tab2", uiOutput("stage_maintain_tab2_ui"))
      )
    ))
  })

  output$stage_maintain_tab1_ui <- renderUI({
    ctx <- stage_maintain_context()
    stage_maintain_tab1_refresh()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    pt <- ctx$project_type
    if (is.na(pt) || !nzchar(pt)) return(tags$p("无项目类型，无法加载阶段定义。"))
    tryCatch({
      q <- 'SELECT id, project_type, stage_key, stage_name, stage_scope, stage_order, supports_sample, is_active, COALESCE(stage_config::text, \'{}\') AS stage_config_json FROM public."08项目阶段定义表" WHERE project_type = $1 ORDER BY stage_order, stage_key'
      df <- DBI::dbGetQuery(pg_con, q, params = list(pt))
      tagList(
        fluidRow(
          column(8, tags$p(tags$b("项目类型："), pt, tags$small("（仅影响触发器自动生成 09 时使用的模板）"))),
          column(4, tags$div(style = "text-align: right;",
            actionButton("btn_new_stage_def", "＋ 新建阶段", class = "btn-success")
          ))
        ),
        tags$hr(),
        if (nrow(df) == 0) {
          tags$p("该项目类型下暂无阶段定义，点击「新建阶段」添加。")
        } else {
          tagList(
            lapply(seq_len(nrow(df)), function(i) {
              row <- df[i, ]
              rid <- row$id
              sid <- paste0("sm08_", rid)
              cfg <- row$stage_config_json
              if (is.null(cfg) || !nzchar(trimws(cfg))) cfg <- "{}"
              tags$div(
                class = "panel panel-default",
                style = "margin-bottom: 12px;",
                tags$div(
                  class = "panel-body",
                  fluidRow(column(12, tags$label("stage_key"), tags$p(style = "margin-top: 4px; font-weight: bold;", row$stage_key))),
                  fluidRow(
                    column(4, textInput(paste0(sid, "_name"), "阶段名称", value = row$stage_name, width = "100%")),
                    column(2, numericInput(paste0(sid, "_order"), "排序", value = as.integer(row$stage_order), min = 0, step = 1, width = "100%")),
                    column(2, selectInput(paste0(sid, "_scope"), "scope", choices = c("sync", "site"), selected = row$stage_scope, width = "100%")),
                    column(2, tags$div(style = "margin-top: 25px;", checkboxInput(paste0(sid, "_active"), "模板启用", value = isTRUE(row$is_active))))
                  ),
                  fluidRow(
                    column(12, tags$label("stage_config (JSON)"), textAreaInput(paste0(sid, "_config"), NULL, value = cfg, rows = 3, width = "100%", placeholder = '{"work_choices":["..."]}'))
                  )
                )
              )
            }),
            actionButton("btn_save_stage_defs", "保存 08 阶段定义", class = "btn-primary")
          )
        }
      )
    }, error = function(e) tags$p(style = "color:red;", paste0("加载失败：", conditionMessage(e))))
  })

  observeEvent(input$btn_save_stage_defs, {
    ctx <- stage_maintain_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    pt <- ctx$project_type
    tryCatch({
      q <- 'SELECT id, stage_key, stage_name, stage_order, stage_scope, supports_sample, is_active, COALESCE(stage_config::text, \'{}\') AS stage_config_json FROM public."08项目阶段定义表" WHERE project_type = $1'
      df <- DBI::dbGetQuery(pg_con, q, params = list(pt))
      if (nrow(df) == 0) return()
      for (i in seq_len(nrow(df))) {
        rid <- df$id[i]
        sid <- paste0("sm08_", rid)
        nm <- input[[paste0(sid, "_name")]]
        ord <- input[[paste0(sid, "_order")]]
        sc <- input[[paste0(sid, "_scope")]]
        samp <- identical(as.character(df$stage_key[i]), "S09_验证试验开展与数据管理")
        act <- isTRUE(input[[paste0(sid, "_active")]])
        cfg <- input[[paste0(sid, "_config")]]
        if (is.null(nm) && is.null(ord) && is.null(sc) && is.null(cfg)) next
        cfg_valid <- tryCatch({ jsonlite::fromJSON(cfg %||% "{}"); TRUE }, error = function(e) FALSE)
        if (!cfg_valid) {
          showNotification(paste0("阶段 ", df$stage_key[i], " 的 stage_config 不是合法 JSON，已跳过。"), type = "warning")
          cfg <- df$stage_config_json[i]
        } else {
          cfg <- trimws(cfg %||% "{}")
          if (!nzchar(cfg)) cfg <- "{}"
        }
        uq <- 'UPDATE public."08项目阶段定义表" SET stage_name = $1, stage_order = $2, stage_scope = $3, supports_sample = $4, is_active = $5, stage_config = $6::jsonb, updated_at = CURRENT_TIMESTAMP WHERE id = $7'
        DBI::dbExecute(pg_con, uq, params = list(nm %||% df$stage_name[i], as.integer(ord %||% df$stage_order[i]), sc %||% df$stage_scope[i], samp, act, cfg, rid))
      }
      showNotification("08 阶段定义已保存。", type = "message")
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("保存失败：", conditionMessage(e)), type = "error"))
  })

  observeEvent(input$btn_new_stage_def, {
    ctx <- stage_maintain_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    pt <- ctx$project_type
    showModal(modalDialog(
      title = "新建阶段",
      size = "m",
      easyClose = TRUE,
      footer = tagList(
        actionButton("btn_do_new_stage_def", "创建", class = "btn-primary"),
        modalButton("取消")
      ),
      fluidRow(
        column(6, textInput("new_stage_key", "stage_key（必填，如 S16_自定义阶段）", placeholder = "S16_xxx", width = "100%")),
        column(6, textInput("new_stage_name", "阶段名称", placeholder = "显示名称", width = "100%"))
      ),
      fluidRow(
        column(4, numericInput("new_stage_order", "排序", value = 99, min = 0, step = 1, width = "100%")),
        column(4, selectInput("new_stage_scope", "scope", choices = c("sync", "site"), selected = "site", width = "100%")),
        column(4, tags$div(style = "margin-top: 25px;", checkboxInput("new_stage_active", "模板启用", value = TRUE)))
      ),
      fluidRow(
        column(12, textAreaInput("new_stage_config", "stage_config (JSON)", value = "{}", rows = 4, width = "100%", placeholder = '{"work_choices":["选项1","选项2"]}'))
      ),
      tags$p(tags$small("项目类型：", pt))
    ))
  })

  observeEvent(input$btn_do_new_stage_def, {
    ctx <- stage_maintain_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    pt <- ctx$project_type
    k <- trimws(input$new_stage_key %||% "")
    nm <- trimws(input$new_stage_name %||% "")
    if (!nzchar(k)) {
      showNotification("stage_key 不能为空。", type = "error")
      return()
    }
    cfg <- trimws(input$new_stage_config %||% "{}")
    if (!nzchar(cfg)) cfg <- "{}"
    cfg_valid <- tryCatch({ jsonlite::fromJSON(cfg); TRUE }, error = function(e) FALSE)
    if (!cfg_valid) {
      showNotification("stage_config 必须是合法 JSON。", type = "error")
      return()
    }
    tryCatch({
      ins <- 'INSERT INTO public."08项目阶段定义表" (project_type, stage_key, stage_name, stage_scope, stage_order, supports_sample, is_active, stage_config) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id'
      res <- DBI::dbGetQuery(pg_con, ins, params = list(
        pt, k, nm %||% k, input$new_stage_scope, as.integer(input$new_stage_order %||% 99),
        identical(k, "S09_验证试验开展与数据管理"), isTRUE(input$new_stage_active), cfg
      ))
      new_def_id <- res$id[1]
      sc <- input$new_stage_scope
      new_stage_active <- isTRUE(input$new_stage_active)
      if (sc == "sync") {
        prop <- 'INSERT INTO public."09项目阶段实例表" (project_id, scope_row_id, site_project_id, stage_def_id, is_active)
          SELECT p.id, 0, NULL, $1, $3
          FROM public."04项目总表" p
          WHERE p."项目类型" = $2
          ON CONFLICT (project_id, stage_def_id, scope_row_id) DO UPDATE SET is_active = EXCLUDED.is_active'
        DBI::dbExecute(pg_con, prop, params = list(new_def_id, pt, new_stage_active))
      } else {
        prop <- 'INSERT INTO public."09项目阶段实例表" (project_id, scope_row_id, site_project_id, stage_def_id, is_active)
          SELECT p.id, s.id, s.id, $1, $3
          FROM public."03医院_项目表" s
          JOIN public."04项目总表" p ON p.id = s."project_table 项目总表_id"
          WHERE p."项目类型" = $2
          ON CONFLICT (project_id, stage_def_id, scope_row_id) DO UPDATE SET is_active = EXCLUDED.is_active'
        DBI::dbExecute(pg_con, prop, params = list(new_def_id, pt, new_stage_active))
      }
      removeModal()
      showNotification("阶段已创建，并已为该项目类型下所有项目/中心补齐 09 实例。", type = "message")
      stage_maintain_tab1_refresh(stage_maintain_tab1_refresh() + 1L)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) {
      msg <- conditionMessage(e)
      if (grepl("uq_stage_def|unique|duplicate", msg, ignore.case = TRUE)) {
        showNotification(paste0("stage_key「", k, "」已存在，请换一个。"), type = "error")
      } else {
        showNotification(paste0("创建失败：", msg), type = "error")
      }
    })
  })

  output$stage_maintain_tab2_ui <- renderUI({
    ctx <- stage_maintain_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    proj_id <- as.integer(ctx$proj_row_id)
    scope_row <- if (ctx$is_sync) 0L else as.integer(ctx$site_row_id)
    pt <- ctx$project_type
    tryCatch({
      q08 <- 'SELECT id, stage_key, stage_name, stage_scope, stage_order FROM public."08项目阶段定义表" WHERE project_type = $1 ORDER BY stage_order, stage_key'
      defs <- DBI::dbGetQuery(pg_con, q08, params = list(pt))
      if (nrow(defs) == 0) return(tags$p("该项目类型下暂无阶段定义。"))
      defs <- defs %>% filter(stage_scope == if (ctx$is_sync) "sync" else "site")
      if (nrow(defs) == 0) return(tags$p("当前维度（", if (ctx$is_sync) "同步" else "中心", "）下无对应阶段定义。"))
      q09 <- 'SELECT stage_def_id, is_active FROM public."09项目阶段实例表" WHERE project_id = $1 AND scope_row_id = $2'
      inst <- DBI::dbGetQuery(pg_con, q09, params = list(proj_id, scope_row))
      is_active_true <- nrow(inst) > 0L & !is.na(inst$is_active) & (inst$is_active == TRUE | as.character(inst$is_active) == "t")
      active_ids <- if (nrow(inst) > 0L) as.integer(inst$stage_def_id[is_active_true]) else integer(0)
      tagList(
        tags$p(tags$b("项目："), ctx$project_id, " | ", tags$b("中心："), ctx$site_name),
        tags$p(tags$small("勾选表示该维度下该阶段实例参与甘特展示；取消勾选仅置 is_active=FALSE，不删行。")),
        tags$hr(),
        lapply(seq_len(nrow(defs)), function(i) {
          d <- defs[i, ]
          did <- as.integer(d$id)
          checked <- did %in% active_ids
          sid <- paste0("sm09_", did)
          tags$div(
            style = "margin-bottom: 6px;",
            checkboxInput(sid, paste0(d$stage_name, " (", d$stage_key, ")"), value = checked)
          )
        }),
        actionButton("btn_save_stage_instances", "保存 09 阶段实例", class = "btn-primary")
      )
    }, error = function(e) tags$p(style = "color:red;", paste0("加载失败：", conditionMessage(e))))
  })

  observeEvent(input$btn_save_stage_instances, {
    ctx <- stage_maintain_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    proj_id <- as.integer(ctx$proj_row_id)
    scope_row <- if (ctx$is_sync) 0L else as.integer(ctx$site_row_id)
    site_id <- if (ctx$is_sync) NULL else as.integer(ctx$site_row_id)
    pt <- ctx$project_type
    tryCatch({
      q08 <- 'SELECT id, stage_key, stage_scope FROM public."08项目阶段定义表" WHERE project_type = $1 AND stage_scope = $2'
      defs <- DBI::dbGetQuery(pg_con, q08, params = list(pt, if (ctx$is_sync) "sync" else "site"))
      if (nrow(defs) == 0) return()
      q09 <- 'SELECT id, stage_def_id, is_active FROM public."09项目阶段实例表" WHERE project_id = $1 AND scope_row_id = $2'
      inst <- DBI::dbGetQuery(pg_con, q09, params = list(proj_id, scope_row))
      for (i in seq_len(nrow(defs))) {
        d <- defs[i, ]
        did <- d$id
        sid <- paste0("sm09_", did)
        want_active <- isTRUE(input[[sid]])
        ex <- inst %>% filter(stage_def_id == did)
        has_row <- nrow(ex) > 0
        cur_is_active <- if (has_row) ex$is_active[1] else NA
        cur_active <- has_row && (isTRUE(cur_is_active) || is.na(cur_is_active))
        if (want_active && !has_row) {
          ins <- 'INSERT INTO public."09项目阶段实例表" (project_id, scope_row_id, site_project_id, stage_def_id, is_active) VALUES ($1, $2, $3, $4, TRUE)'
          DBI::dbExecute(pg_con, ins, params = list(proj_id, scope_row, site_id, d$id))
        } else if (want_active && has_row && !cur_active) {
          DBI::dbExecute(pg_con, 'UPDATE public."09项目阶段实例表" SET is_active = TRUE WHERE id = $1', params = list(ex$id[1]))
        } else if (!want_active && has_row && cur_active) {
          DBI::dbExecute(pg_con, 'UPDATE public."09项目阶段实例表" SET is_active = FALSE WHERE id = $1', params = list(ex$id[1]))
        }
      }
      showNotification("09 阶段实例已保存。", type = "message")
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("保存失败：", conditionMessage(e)), type = "error"))
  })

  output$remark_editor <- renderUI({
    ctx <- task_edit_context()
    if (is.null(ctx)) return(NULL)
    n_rows <- remark_row_count()
    if (is.null(n_rows) || n_rows < 0L) n_rows <- 0L

    auth <- current_user_auth()
    current_reporter <- normalize_text(auth$name)
    if (is.na(current_reporter)) current_reporter <- normalize_text(auth$work_id)
    if (is.na(current_reporter)) current_reporter <- "未知用户"

    existing <- ctx$remark_entries
    rows_ui <- if (n_rows > 0L) {
      lapply(seq_len(n_rows), function(i) {
        type_val <- content <- ""
        if (!is.null(existing) && nrow(existing) >= i) {
          type_val <- as.character(existing$type[i])
          content <- as.character(existing$content[i])
        }
        type_choices <- c("问题", "卡点", "经验分享")
        if (nzchar(type_val) && !(type_val %in% type_choices)) type_choices <- c(type_choices, type_val)
        fluidRow(
          column(
            3,
            selectizeInput(
              paste0("remark_type_", i),
              if (i == 1L) "类型" else NULL,
              choices = type_choices,
              selected = if (nzchar(type_val)) type_val else NULL,
              options = list(create = TRUE)
            )
          ),
          column(
            9,
            textAreaInput(
              paste0("remark_content_", i),
              if (i == 1L) sprintf("内容（反馈人：%s）", current_reporter) else NULL,
              value = content,
              rows = 3,
              placeholder = "留空则删除该条"
            )
          )
        )
      })
    } else {
      list(tags$p("当前暂无备注条目。", style = "color:#777;"))
    }
    tagList(
      tags$hr(),
      tags$b("问题、卡点反馈与经验分享"),
      tags$p(tags$small(sprintf("反馈人将自动写入当前登录帐号：%s", current_reporter))),
      tags$div(style = "margin-top:10px;", rows_ui)
    )
  })

  observeEvent(input$btn_add_remark_row, {
    ctx <- task_edit_context()
    req(ctx)
    n_cur <- remark_row_count()
    if (is.null(n_cur) || n_cur < 0L) n_cur <- 0L
    remark_df <- ctx$remark_entries
    if (is.null(remark_df)) remark_df <- empty_remark_df()
    auth <- current_user_auth()
    current_reporter <- normalize_text(auth$name)
    if (is.na(current_reporter)) current_reporter <- normalize_text(auth$work_id)
    if (is.na(current_reporter)) current_reporter <- "未知用户"
    if (n_cur > 0L) {
      for (i in seq_len(n_cur)) {
        type_val <- tryCatch(trimws(as.character(input[[paste0("remark_type_", i)]])), error = function(e) "")
        content <- tryCatch(trimws(as.character(input[[paste0("remark_content_", i)]])), error = function(e) "")
        if (i <= nrow(remark_df)) {
          remark_df$reporter[i] <- current_reporter
          remark_df$updated_at[i] <- if (i <= nrow(remark_df) && nzchar(as.character(remark_df$updated_at[i] %||% ""))) as.character(remark_df$updated_at[i]) else ""
          remark_df$type[i] <- type_val
          remark_df$content[i] <- content
        } else {
          remark_df[i, ] <- list("", current_reporter, "", type_val, content)
        }
      }
    }
    remark_df[nrow(remark_df) + 1L, ] <- list("", current_reporter, "", "问题", "")
    ctx$remark_entries <- remark_df
    task_edit_context(ctx)
    remark_row_count(nrow(remark_df))
  })

  # 进度贡献者编辑 UI
  output$contrib_editor <- renderUI({
    ctx <- task_edit_context()
    if (is.null(ctx)) return(NULL)
    n_rows <- contrib_row_count()
    if (is.null(n_rows) || n_rows < 0L) n_rows <- 0L

    # 参与人员列表（从 05人员表）
    person_choices <- character(0)
    if (!is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      dfp <- tryCatch(
        DBI::dbGetQuery(pg_con, 'SELECT "姓名" FROM public."05人员表" WHERE "姓名" IS NOT NULL ORDER BY "姓名"'),
        error = function(e) data.frame()
      )
      if ("姓名" %in% names(dfp)) {
        person_choices <- unique(na.omit(dfp[["姓名"]]))
      }
    }

    # 各阶段工作内容选项
    work_choices <- stage_work_choices_for(ctx$task_name, ctx$project_type)

    existing <- ctx$contributors
    rows_ui <- if (n_rows > 0L) {
      lapply(seq_len(n_rows), function(i) {
        person <- role <- work <- ""
        amount <- 1
        if (!is.null(existing) && nrow(existing) >= i) {
          person <- as.character(existing$person[i])
          role <- as.character(existing$role[i])
          work <- as.character(existing$work[i])
          amount <- suppressWarnings(as.numeric(existing$amount[i]))
          if (is.na(amount) || amount < 1) amount <- 1
        }
        role_choices_i <- c("主导", "参与", "协助")
        if (nzchar(role) && !(role %in% role_choices_i)) role_choices_i <- c(role_choices_i, role)
        work_choices_i <- work_choices
        if (nzchar(work) && !(work %in% work_choices_i)) work_choices_i <- c(work_choices_i, work)
        fluidRow(
          column(
            3,
            selectInput(
              paste0("contrib_person_", i),
              if (i == 1L) "人员" else NULL,
              choices = c("（请选择）" = "", setNames(person_choices, person_choices)),
              selected = if (nzchar(person)) person else ""
            )
          ),
          column(
            3,
            selectizeInput(
              paste0("contrib_role_", i),
              if (i == 1L) "参与度" else NULL,
              choices = role_choices_i,
              selected = if (nzchar(role)) role else NULL,
              options = list(create = TRUE)
            )
          ),
          column(
            3,
            selectizeInput(
              paste0("contrib_work_", i),
              if (i == 1L) "工作内容" else NULL,
              choices = work_choices_i,
              selected = if (nzchar(work)) work else NULL,
              options = list(create = TRUE)
            )
          ),
          column(
            3,
            numericInput(
              paste0("contrib_amount_", i),
              if (i == 1L) "数量" else NULL,
              value = amount,
              min = 1,
              width = "100px"
            )
          )
        )
      })
    } else {
      list()
    }

    tagList(
      tags$p(
        tags$b("项目："), ctx$project_id, " / ",
        tags$b("子进程："),
        if (ctx$is_sync) "各中心同步阶段" else ctx$site_name,
        " / ",
        tags$b("阶段："), stage_label_for_key(ctx$task_name)
      ),
      tags$hr(),
      tags$b("进度贡献者"),
      tags$p(tags$small("每行：人员（从人员表选择） + 参与度（主导/参与/协助，可自定义） + 工作内容（阶段相关，可自定义） + 数量（>=1 的数字）。")),
      tags$div(style = "margin-top:10px;", rows_ui),
      actionButton("btn_add_contrib_row", "新增贡献者", class = "btn-success"),
      tags$p(tags$small("留空人员的行在保存时会被忽略。"))
    )
  })

  open_contrib_edit_modal <- function(ctx) {
    contrib_row_count(if (is.null(ctx$contributors)) 0L else nrow(ctx$contributors))
    removeModal()
    showModal(modalDialog(
      title = "修改进度贡献者信息",
      size = "l",
      easyClose = TRUE,
      footer = tagList(
        actionButton("btn_save_contrib", "保存贡献者信息", class = "btn-primary"),
        modalButton("取消")
      ),
      uiOutput("contrib_editor")
    ))
  }

  refresh_contrib_context_from_db <- function(ctx) {
    if (is.null(ctx$contrib_col) || is.na(ctx$contrib_col)) return(NULL)
    latest_row <- fetch_row_snapshot(pg_con, ctx$table_name, ctx$stage_instance_id, ctx$contrib_col, lock = FALSE)
    if (is.null(latest_row)) return(NULL)
    ctx$snapshot_row[[ctx$contrib_col]] <- latest_row[[ctx$contrib_col]]
    ctx$contributors <- parse_contrib_json_to_df(latest_row[[ctx$contrib_col]])
    ctx
  }

  observeEvent(input$btn_edit_contrib, {
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    open_contrib_edit_modal(ctx)
  })

  observeEvent(input$btn_add_contrib_row, {
    ctx <- task_edit_context()
    req(ctx)
    n_cur <- contrib_row_count()
    if (is.null(n_cur) || n_cur < 0L) n_cur <- 0L

    # 同步现有输入到 ctx$contributors
    contrib_df <- ctx$contributors
    if (is.null(contrib_df)) contrib_df <- empty_contrib_df()
    if (n_cur > 0L) {
      for (i in seq_len(n_cur)) {
        person_i <- tryCatch(trimws(as.character(input[[paste0("contrib_person_", i)]])), error = function(e) "")
        role_i   <- tryCatch(trimws(as.character(input[[paste0("contrib_role_", i)]])), error = function(e) "")
        work_i   <- tryCatch(trimws(as.character(input[[paste0("contrib_work_", i)]])), error = function(e) "")
        raw_num <- tryCatch(input[[paste0("contrib_amount_", i)]], error = function(e) NA_real_)
        amt_num <- suppressWarnings(as.numeric(raw_num))
        if (is.na(amt_num) || amt_num < 1) amt_num <- 1
        entry_key_i <- if (nrow(contrib_df) >= i) as.character(contrib_df$entry_key[i]) else ""
        if (i <= nrow(contrib_df)) {
          contrib_df$entry_key[i] <- entry_key_i
          contrib_df$person[i] <- person_i
          contrib_df$role[i] <- role_i
          contrib_df$work[i] <- work_i
          contrib_df$amount[i] <- as.character(amt_num)
        } else {
          contrib_df[i, ] <- list(entry_key_i, person_i, role_i, work_i, as.character(amt_num))
        }
      }
    }
    contrib_df[nrow(contrib_df) + 1L, ] <- list("", "", "", "", "1")
    ctx$contributors <- contrib_df
    task_edit_context(ctx)
    contrib_row_count(n_cur + 1L)
  })

  observeEvent(input$btn_save_contrib, {
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    n_rows <- contrib_row_count()
    if (is.null(n_rows) || n_rows < 0L) n_rows <- 0L

    # 校验人员名称是否存在于人员表
    person_choices <- character(0)
    if (!is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      dfp <- tryCatch(
        DBI::dbGetQuery(pg_con, 'SELECT "姓名" FROM public."05人员表" WHERE "姓名" IS NOT NULL'),
        error = function(e) data.frame()
      )
      if ("姓名" %in% names(dfp)) person_choices <- unique(na.omit(dfp[["姓名"]]))
    }
    contrib_df <- ctx$contributors
    if (is.null(contrib_df)) contrib_df <- empty_contrib_df()
    if (n_rows > 0L) {
      for (i in seq_len(n_rows)) {
        p <- tryCatch(trimws(as.character(input[[paste0("contrib_person_", i)]])), error = function(e) "")
        r <- tryCatch(trimws(as.character(input[[paste0("contrib_role_", i)]])), error = function(e) "")
        w <- tryCatch(trimws(as.character(input[[paste0("contrib_work_", i)]])), error = function(e) "")
        if (nzchar(p) && length(person_choices) > 0 && !(p %in% person_choices)) {
          showNotification(sprintf("不存在此人名：%s", p), type = "error")
          return()
        }
        raw_num <- tryCatch(input[[paste0("contrib_amount_", i)]], error = function(e) NA_real_)
        amt_num <- suppressWarnings(as.numeric(raw_num))
        if (nzchar(p)) {
        if (is.na(amt_num) || amt_num < 1) {
          showNotification("数量必须为不小于 1 的数字。", type = "error")
          return()
          }
        } else {
          amt_num <- 1
        }
        entry_key_i <- if (nrow(contrib_df) >= i) as.character(contrib_df$entry_key[i]) else ""
        if (i <= nrow(contrib_df)) {
          contrib_df$entry_key[i] <- entry_key_i
          contrib_df$person[i] <- p
          contrib_df$role[i] <- r
          contrib_df$work[i] <- w
          contrib_df$amount[i] <- as.character(amt_num)
        } else {
          contrib_df[i, ] <- list(entry_key_i, p, r, w, as.character(amt_num))
        }
      }
    }
    if (n_rows < nrow(contrib_df)) contrib_df <- contrib_df[seq_len(n_rows), , drop = FALSE]

    # 确定列名与表
    auth <- current_user_auth()
    actor_name <- normalize_text(auth$name)
    if (is.na(actor_name)) actor_name <- normalize_text(auth$work_id)
    if (is.na(actor_name)) actor_name <- "未知用户"
    contrib_col <- ctx$contrib_col
    if (is.null(contrib_col) || is.na(contrib_col)) {
      showNotification("当前阶段未配置进度贡献者列，无法保存。", type = "error")
      return()
    }
    tbl <- ctx$table_name
    row_id <- ctx$stage_instance_id
    if (is.na(row_id)) {
      showNotification("无法确定数据库行，保存失败。", type = "error")
      return()
    }
    user_contrib_map <- build_contrib_map_from_df(contrib_df, actor = actor_name)
    snapshot_map <- parse_named_json_map(ctx$snapshot_row[[contrib_col]])
    ctx$contributors <- contrib_df
    task_edit_context(ctx)
    reopen_current_contrib <- function() {
      task_edit_context(ctx)
      open_contrib_edit_modal(ctx)
      invisible(NULL)
    }
    refresh_and_reopen_contrib <- function() {
      latest_ctx <- refresh_contrib_context_from_db(ctx)
      if (is.null(latest_ctx)) {
        showNotification("刷新失败：当前记录已不存在。", type = "error")
        return(invisible(NULL))
      }
      task_edit_context(latest_ctx)
      open_contrib_edit_modal(latest_ctx)
      invisible(NULL)
    }

    apply_contrib_save <- function(save_mode = c("block", "partial", "overwrite")) {
      save_mode <- match.arg(save_mode)
      overwrite_conflicts <- identical(save_mode, "overwrite")
      allow_partial_save <- identical(save_mode, "partial")
      result <- tryCatch(
        pool::poolWithTransaction(pg_con, function(conn) {
          locked_row <- fetch_row_snapshot(conn, tbl, row_id, contrib_col, lock = TRUE)
          if (is.null(locked_row)) stop("记录不存在，无法保存。")
          db_map <- parse_named_json_map(locked_row[[contrib_col]])
          merged <- merge_named_json_field(snapshot_map, db_map, user_contrib_map, "进度贡献者", overwrite_conflicts = overwrite_conflicts)
          if (!isTRUE(overwrite_conflicts) && !isTRUE(allow_partial_save) && length(merged$conflicts) > 0) {
            return(list(status = "conflict", conflicts = merged$conflicts))
          }
          final_json <- merged$merged_json
          if (!identical(normalize_text(final_json, empty_as_na = FALSE), normalize_text(locked_row[[contrib_col]], empty_as_na = FALSE))) {
            q <- sprintf('UPDATE public."%s" SET "%s" = $1 WHERE id = $2', tbl, contrib_col)
            DBI::dbExecute(conn, q, params = list(final_json, as.integer(row_id)))
          }
          list(
            status = "saved",
            old_map = db_map,
            new_map = merged$merged,
            final_json = final_json,
            partial_conflicts = merged$conflicts
          )
        }),
        error = function(e) list(status = "error", message = conditionMessage(e))
      )

      if (identical(result$status, "conflict")) {
        show_conflict_resolution_modal(
          "进度贡献者存在并发冲突",
          result$conflicts,
          on_overwrite = function() apply_contrib_save("overwrite"),
          on_partial = function() apply_contrib_save("partial"),
          on_refresh = refresh_and_reopen_contrib,
          on_close = reopen_current_contrib
        )
        return(invisible(NULL))
      }
      if (identical(result$status, "error")) {
        showNotification(paste0("保存进度贡献者失败：", result$message), type = "error")
        return(invisible(NULL))
      }

      insert_audit_log(pg_con,
        work_id = auth$work_id, name = auth$name,
        op_type = "update_contrib", target_table = tbl, target_row_id = row_id,
        biz_desc = "进度贡献者", summary = "修改进度贡献者",
        old_val = list(进度贡献者 = result$old_map), new_val = list(进度贡献者 = result$new_map),
        remark = NULL
      )
      ctx$snapshot_row[[contrib_col]] <- result$final_json
      ctx$contributors <- parse_contrib_json_to_df(result$final_json)
      task_edit_context(ctx)
      contrib_row_count(nrow(ctx$contributors))
      gantt_force_refresh(gantt_force_refresh() + 1L)
      removeModal()
      if (length(result$partial_conflicts %||% list()) > 0) {
        if (identical(save_mode, "partial")) {
        showNotification(sprintf("进度贡献者信息已保存，%d 项冲突内容未覆盖。", length(result$partial_conflicts)), type = "warning")
        } else if (identical(save_mode, "overwrite")) {
          showNotification(sprintf("进度贡献者信息已保存，已覆盖 %d 项冲突内容。", length(result$partial_conflicts)), type = "message")
        } else {
          showNotification("进度贡献者信息已保存。", type = "message")
        }
      } else {
        showNotification("进度贡献者信息已保存。", type = "message")
      }
      invisible(NULL)
    }

    current_row <- tryCatch(fetch_row_snapshot(pg_con, tbl, row_id, contrib_col, lock = FALSE), error = function(e) NULL)
    db_map_now <- if (!is.null(current_row)) parse_named_json_map(current_row[[contrib_col]]) else list()
    precheck <- merge_named_json_field(snapshot_map, db_map_now, user_contrib_map, "进度贡献者", overwrite_conflicts = FALSE)
    auto_merge_items <- if (state_changed(snapshot_map, db_map_now)) format_json_auto_merge_items("进度贡献者", precheck$changed_keys) else character(0)
    if (length(precheck$conflicts) > 0) {
      show_conflict_resolution_modal(
        "进度贡献者存在并发冲突",
        precheck$conflicts,
        on_overwrite = function() apply_contrib_save("overwrite"),
        on_partial = function() apply_contrib_save("partial"),
        on_refresh = refresh_and_reopen_contrib,
        on_close = reopen_current_contrib
      )
    } else {
      apply_contrib_save("block")
    }
  })

  # 自由里程碑编辑 UI
  output$milestone_viewer <- renderUI({
    ctx <- task_edit_context()
    if (is.null(ctx) || is.null(ctx$milestones)) return(NULL)
    ms_df <- ctx$milestones
    tagList(
      tags$p(tags$b("项目："), ctx$project_id),
      tags$p(
        tags$b("子进程："),
        if (ctx$is_sync) {
          paste0(ctx$project_id, " - 各中心同步阶段")
        } else {
          paste0(ctx$project_id, " - ", ctx$site_name)
        }
      ),
      tags$hr(),
      if (nrow(ms_df) == 0) {
        tags$p("当前尚未定义任何自由里程碑。", style = "color:#777;")
      } else {
        tags$div(
          lapply(seq_len(nrow(ms_df)), function(i) {
            nm <- ifelse(nzchar(ms_df$name[i]), ms_df$name[i], "(未命名里程碑)")
            pl <- ifelse(nzchar(ms_df$plan[i]), ms_df$plan[i], "无")
            ac <- ifelse(nzchar(ms_df$actual[i]), ms_df$actual[i], "无")
            nt <- ifelse(nzchar(ms_df$note[i]), ms_df$note[i], "无")
            tags$div(
              style = "margin-bottom:8px; padding:8px; border-bottom:1px solid #eee;",
              tags$b(nm),
              tags$br(),
              tags$span(style="color:#555;", paste0("计划达成时间：", pl)),
              tags$br(),
              tags$span(style="color:#555;", paste0("实际达成时间：", ac)),
              tags$br(),
              tags$span(style="color:#777;", paste0("备注：", nt))
            )
          })
        )
      }
    )
  })

  output$milestone_editor <- renderUI({
    ctx <- task_edit_context()
    if (is.null(ctx) || is.null(ctx$milestones)) return(NULL)
    ms_df <- ctx$milestones
    n_rows <- milestone_row_count()
    if (is.null(n_rows) || n_rows < 0L) n_rows <- nrow(ms_df)
    rows_ui <- if (n_rows > 0L) {
      lapply(seq_len(n_rows), function(i) {
        nm <- if (!is.null(ms_df$name[i])) ms_df$name[i] else ""
        pl <- if (!is.null(ms_df$plan[i])) ms_df$plan[i] else ""
        ac <- if (!is.null(ms_df$actual[i])) ms_df$actual[i] else ""
        nt <- if (!is.null(ms_df$note[i])) ms_df$note[i] else ""
        fluidRow(
          column(3,
                 textInput(paste0("ms_name_", i), if (i == 1L) "里程碑名称" else NULL, value = nm, placeholder = "名称为空将删除")
          ),
          column(3,
                 textInput(
                   paste0("ms_plan_", i),
                   if (i == 1L) "计划达成时间" else NULL,
                   value = pl,
                   placeholder = "YYYY-MM-DD 或留空"
                 )
          ),
          column(3,
                 textInput(
                   paste0("ms_actual_", i),
                   if (i == 1L) "实际达成时间" else NULL,
                   value = ac,
                   placeholder = "YYYY-MM-DD 或留空"
                 )
          ),
          column(3,
                 textInput(paste0("ms_note_", i), if (i == 1L) "备注", value = nt)
          )
        )
      })
    } else {
      list()
    }
    tagList(
      tags$p(tags$b("项目："), ctx$project_id, " / ",
             if (ctx$is_sync) "各中心同步阶段" else ctx$site_name,
             " / ", stage_label_for_key(ctx$task_name)),
      tags$hr(),
      tags$div(rows_ui),
      actionButton("btn_add_milestone_row", "新增里程碑", class = "btn-success"),
      tags$p(tags$small("名称为空则删除该里程碑；时间留空会保存为“无”。"))
    )
  })

  open_milestone_edit_modal <- function(ctx) {
    showModal(modalDialog(
      title = "自由里程碑管理",
      size = "l",
      easyClose = TRUE,
      footer = tagList(
        actionButton("btn_save_milestone", "保存里程碑", class = "btn-primary"),
        modalButton("关闭")
      ),
      uiOutput("milestone_editor")
    ))
  }

  refresh_milestone_context_from_db <- function(ctx) {
    latest_row <- fetch_row_snapshot(pg_con, ctx$table_name, ctx$stage_instance_id, "milestones_json", lock = FALSE)
    if (is.null(latest_row)) return(NULL)
    ctx$milestone_raw <- latest_row[["milestones_json"]]
    ctx$milestones <- parse_milestone_json_to_df(latest_row[["milestones_json"]])
    if (!is.null(ctx$milestones) && nrow(ctx$milestones) > 0) {
      ctx$milestones <- ctx$milestones[ctx$milestones$stage_key == (ctx$milestone_stage_key %||% ctx$task_name), , drop = FALSE]
    }
    ctx
  }

  observeEvent(input$btn_add_milestone_row, {
    ctx <- task_edit_context()
    req(ctx)
    ms_df <- ctx$milestones
    n_cur <- milestone_row_count()
    if (is.null(n_cur) || n_cur < 0L) n_cur <- nrow(ms_df)
    # 先同步现有输入
    if (n_cur > 0L) {
      for (i in seq_len(n_cur)) {
        nm <- tryCatch(trimws(as.character(input[[paste0("ms_name_", i)]])), error = function(e) "")
        pl <- tryCatch(trimws(as.character(input[[paste0("ms_plan_", i)]])), error = function(e) "")
        ac <- tryCatch(trimws(as.character(input[[paste0("ms_actual_", i)]])), error = function(e) "")
        nt <- tryCatch(trimws(as.character(input[[paste0("ms_note_", i)]])), error = function(e) "")
        entry_key_i <- if (nrow(ms_df) >= i) as.character(ms_df$entry_key[i]) else ""
        stage_key_i <- if (nrow(ms_df) >= i) as.character(ms_df$stage_key[i]) else (ctx$milestone_stage_key %||% ctx$task_name)
        if (i <= nrow(ms_df)) {
          ms_df$entry_key[i] <- entry_key_i
          ms_df$stage_key[i] <- stage_key_i
          ms_df$name[i] <- nm
          ms_df$plan[i] <- pl
          ms_df$actual[i] <- ac
          ms_df$note[i] <- nt
        } else {
          ms_df[i, ] <- list(entry_key_i, stage_key_i, nm, pl, ac, nt)
        }
      }
    }
    # 新增一空行
    ms_df[nrow(ms_df) + 1, ] <- list("", ctx$milestone_stage_key %||% ctx$task_name, "", "", "", "")
    ctx$milestones <- ms_df
    task_edit_context(ctx)
    milestone_row_count(nrow(ms_df))
  })

  observeEvent(input$btn_save_milestone, {
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    ms_df <- ctx$milestones
    n_rows <- milestone_row_count()
    if (is.null(n_rows) || n_rows < 0L) n_rows <- nrow(ms_df)

    auth <- current_user_auth()
    actor_name <- normalize_text(auth$name)
    if (is.na(actor_name)) actor_name <- normalize_text(auth$work_id)
    if (is.na(actor_name)) actor_name <- "未知用户"
    stage_key <- ctx$milestone_stage_key %||% ctx$task_name

    if (n_rows > 0L) {
      for (i in seq_len(n_rows)) {
        nm <- tryCatch(trimws(as.character(input[[paste0("ms_name_", i)]])), error = function(e) "")
        pl <- tryCatch(trimws(as.character(input[[paste0("ms_plan_", i)]])), error = function(e) "")
        ac <- tryCatch(trimws(as.character(input[[paste0("ms_actual_", i)]])), error = function(e) "")
        nt <- tryCatch(trimws(as.character(input[[paste0("ms_note_", i)]])), error = function(e) "")
        if (nzchar(nm)) {
        if (nzchar(pl)) {
          pl_date <- tryCatch(as.Date(pl, optional = TRUE), error = function(e) NA)
          if (is.na(pl_date)) {
            showNotification(sprintf("里程碑「%s」的计划达成时间格式不正确：%s", nm, pl), type = "error")
            return()
          }
        }
        if (nzchar(ac)) {
          ac_date <- tryCatch(as.Date(ac, optional = TRUE), error = function(e) NA)
          if (is.na(ac_date)) {
            showNotification(sprintf("里程碑「%s」的实际达成时间格式不正确：%s", nm, ac), type = "error")
            return()
            }
          }
        }
        entry_key_i <- if (nrow(ms_df) >= i) as.character(ms_df$entry_key[i]) else ""
        if (i <= nrow(ms_df)) {
          ms_df$entry_key[i] <- entry_key_i
          ms_df$stage_key[i] <- stage_key
          ms_df$name[i] <- nm
          ms_df$plan[i] <- pl
          ms_df$actual[i] <- ac
          ms_df$note[i] <- nt
        } else {
          ms_df[i, ] <- list(entry_key_i, stage_key, nm, pl, ac, nt)
        }
      }
    }
    if (n_rows < nrow(ms_df)) ms_df <- ms_df[seq_len(n_rows), , drop = FALSE]

    snapshot_ms_map <- parse_named_json_map(ctx$milestone_raw)
    snapshot_ms_df <- parse_milestone_json_to_df(ctx$milestone_raw)
    current_stage_keys <- if (nrow(snapshot_ms_df) > 0) snapshot_ms_df$entry_key[snapshot_ms_df$stage_key == stage_key] else character(0)
    user_stage_map <- build_milestone_map_from_df(ms_df, actor = actor_name)
    user_ms_map <- snapshot_ms_map
    for (k in current_stage_keys) user_ms_map[[k]] <- NULL
    for (k in names(user_stage_map)) user_ms_map[[k]] <- user_stage_map[[k]]

    tbl <- ctx$table_name
    row_id <- ctx$stage_instance_id
    if (is.na(row_id)) {
      removeModal()
      return()
    }
    ctx$milestones <- ms_df
    task_edit_context(ctx)
    reopen_current_milestone <- function() {
      task_edit_context(ctx)
      open_milestone_edit_modal(ctx)
      invisible(NULL)
    }
    refresh_and_reopen_milestone <- function() {
      latest_ctx <- refresh_milestone_context_from_db(ctx)
      if (is.null(latest_ctx)) {
        showNotification("刷新失败：当前记录已不存在。", type = "error")
        return(invisible(NULL))
      }
      task_edit_context(latest_ctx)
      open_milestone_edit_modal(latest_ctx)
      invisible(NULL)
    }

    apply_milestone_save <- function(save_mode = c("block", "partial", "overwrite")) {
      save_mode <- match.arg(save_mode)
      overwrite_conflicts <- identical(save_mode, "overwrite")
      allow_partial_save <- identical(save_mode, "partial")
      result <- tryCatch(
        pool::poolWithTransaction(pg_con, function(conn) {
          locked_row <- fetch_row_snapshot(conn, tbl, row_id, "milestones_json", lock = TRUE)
          if (is.null(locked_row)) stop("记录不存在，无法保存里程碑。")
          db_map <- parse_named_json_map(locked_row[["milestones_json"]])
          merged <- merge_named_json_field(snapshot_ms_map, db_map, user_ms_map, "里程碑", overwrite_conflicts = overwrite_conflicts)
          if (!isTRUE(overwrite_conflicts) && !isTRUE(allow_partial_save) && length(merged$conflicts) > 0) {
            return(list(status = "conflict", conflicts = merged$conflicts))
          }
          final_json <- merged$merged_json
          if (!identical(normalize_text(final_json, empty_as_na = FALSE), normalize_text(locked_row[["milestones_json"]], empty_as_na = FALSE))) {
            q <- sprintf('UPDATE public."%s" SET "milestones_json" = $1 WHERE id = $2', tbl)
            DBI::dbExecute(conn, q, params = list(final_json, row_id))
          }
          list(status = "saved", old_map = db_map, new_map = merged$merged, final_json = final_json, partial_conflicts = merged$conflicts)
        }),
        error = function(e) list(status = "error", message = conditionMessage(e))
      )

      if (identical(result$status, "conflict")) {
        show_conflict_resolution_modal(
          "里程碑存在并发冲突",
          result$conflicts,
          on_overwrite = function() apply_milestone_save("overwrite"),
          on_partial = function() apply_milestone_save("partial"),
          on_refresh = refresh_and_reopen_milestone,
          on_close = reopen_current_milestone
        )
        return(invisible(NULL))
      }
      if (identical(result$status, "error")) {
        showNotification(paste0("里程碑保存失败：", result$message), type = "error")
        return(invisible(NULL))
      }

      insert_audit_log(pg_con,
        work_id = auth$work_id, name = auth$name,
        op_type = "update_milestone", target_table = tbl, target_row_id = row_id,
        biz_desc = "里程碑", summary = "修改自由里程碑",
        old_val = list(里程碑 = result$old_map), new_val = list(里程碑 = result$new_map),
        remark = NULL
      )
      ctx$milestone_raw <- result$final_json
      ctx$milestones <- parse_milestone_json_to_df(result$final_json)
      if (!is.null(ctx$milestones) && nrow(ctx$milestones) > 0) {
        ctx$milestones <- ctx$milestones[ctx$milestones$stage_key == stage_key, , drop = FALSE]
      }
      task_edit_context(ctx)
      milestone_row_count(nrow(ctx$milestones))
      gantt_force_refresh(gantt_force_refresh() + 1L)
      removeModal()
      if (length(result$partial_conflicts %||% list()) > 0) {
        if (identical(save_mode, "partial")) {
        showNotification(sprintf("里程碑已保存，%d 项冲突内容未覆盖。", length(result$partial_conflicts)), type = "warning")
        } else if (identical(save_mode, "overwrite")) {
          showNotification(sprintf("里程碑已保存，已覆盖 %d 项冲突内容。", length(result$partial_conflicts)), type = "message")
        } else {
          showNotification("里程碑已保存。", type = "message")
        }
      } else {
        showNotification("里程碑已保存。", type = "message")
      }
      invisible(NULL)
    }

    current_row <- tryCatch(fetch_row_snapshot(pg_con, tbl, row_id, "milestones_json", lock = FALSE), error = function(e) NULL)
    db_map_now <- if (!is.null(current_row)) parse_named_json_map(current_row[["milestones_json"]]) else list()
    precheck <- merge_named_json_field(snapshot_ms_map, db_map_now, user_ms_map, "里程碑", overwrite_conflicts = FALSE)
    auto_merge_items <- if (state_changed(snapshot_ms_map, db_map_now)) format_json_auto_merge_items("里程碑", precheck$changed_keys) else character(0)
    if (length(precheck$conflicts) > 0) {
      show_conflict_resolution_modal(
        "里程碑存在并发冲突",
        precheck$conflicts,
        on_overwrite = function() apply_milestone_save("overwrite"),
        on_partial = function() apply_milestone_save("partial"),
        on_refresh = refresh_and_reopen_milestone,
        on_close = reopen_current_milestone
      )
    } else {
      apply_milestone_save("block")
    }
  })

  observeEvent(input$btn_open_milestone_edit, {
    ctx <- task_edit_context()
    req(ctx)
    open_milestone_edit_modal(ctx)
  })

  # S09“验证试验开展与数据管理”样本来源与数量编辑区
  output$sample_pairs_editor <- renderUI({
    ctx <- task_edit_context()
    if (is.null(ctx) || !identical(ctx$task_name, "S09_验证试验开展与数据管理") || !ctx$is_sync) {
      return(NULL)
    }
    n_rows <- sample_row_count()
    if (is.null(n_rows) || n_rows < 0L) n_rows <- 0L
    # 读取医院名称列表，供下拉选择（可自定义）
    hosp_choices <- character(0)
    if (!is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      hosp_df <- tryCatch(
        DBI::dbGetQuery(pg_con, 'SELECT "医院名称" FROM public."01医院信息表" ORDER BY "医院名称"'),
        error = function(e) data.frame()
      )
      if ("医院名称" %in% names(hosp_df)) {
        hosp_choices <- unique(na.omit(hosp_df[["医院名称"]]))
      }
    }
    # 选项增加一个“无”，用于显式表示删除
    choice_pool <- c("无", hosp_choices)
    # 现有样本对（用于初始化前 n_rows 行）
    existing <- ctx$samples
    rows_ui <- if (n_rows > 0L) {
      lapply(seq_len(n_rows), function(i) {
        hosp_val <- ""
        cnt_val <- 0
        if (!is.null(existing) && nrow(existing) >= i) {
          hosp_val <- as.character(existing$hospital[i])
          cnt_val <- suppressWarnings(as.numeric(existing$count[i]))
          if (is.na(cnt_val)) cnt_val <- 0
        }
        fluidRow(
          column(
            8,
            selectizeInput(
              inputId = paste0("sample_hospital_", i),
              label = if (i == 1L) "样本来源医院" else NULL,
              choices = choice_pool,
              selected = if (nzchar(hosp_val)) hosp_val else "无",
              options = list(create = TRUE)
            )
          ),
          column(
            4,
            numericInput(
              inputId = paste0("sample_count_", i),
              label = if (i == 1L) "样本数量" else NULL,
              value = cnt_val,
              min = 0,
              step = 1
            )
          )
        )
      })
    } else {
      list()
    }
    tagList(
      tags$hr(),
      tags$b("样本来源与数量"),
      tags$div(style = "margin-top:10px;",
               rows_ui
      ),
      actionButton("btn_add_sample_pair", "新增样本来源记录", class = "btn-success"),
      tags$p(tags$small("每条记录为“样本来源医院 + 样本数量”一对。选择“无”或删掉医院名称点保存会删除该条；删除数字点保存会记录为 0。"))
    )
  })

  observeEvent(input$btn_add_sample_pair, {
    ctx <- task_edit_context()
    req(ctx)
    if (!identical(ctx$task_name, "S09_验证试验开展与数据管理") || !ctx$is_sync) return()
    n_cur <- sample_row_count()
    if (is.null(n_cur) || n_cur < 0L) n_cur <- 0L

    # 先把当前已填写的内容同步到 ctx$samples 中，避免点击新增时被清空
    sample_df <- ctx$samples
    if (is.null(sample_df)) sample_df <- empty_sample_df()
    if (n_cur > 0L) {
      for (i in seq_len(n_cur)) {
        h_input <- input[[paste0("sample_hospital_", i)]]
        c_input <- input[[paste0("sample_count_", i)]]
        h_val <- tryCatch(trimws(as.character(h_input)), error = function(e) "")
        c_num <- suppressWarnings(as.numeric(c_input))
        if (is.na(c_num)) c_num <- 0
        entry_key_i <- if (nrow(sample_df) >= i) as.character(sample_df$entry_key[i]) else ""
        if (i <= nrow(sample_df)) {
          sample_df$entry_key[i] <- entry_key_i
          sample_df$hospital[i] <- h_val
          sample_df$count[i] <- c_num
        } else {
          sample_df[i, ] <- list(entry_key_i, h_val, c_num)
        }
      }
    }
    sample_df[nrow(sample_df) + 1L, ] <- list("", "无", 0)

    # 更新上下文中的样本数据和行数
    ctx$samples <- sample_df
    task_edit_context(ctx)
    sample_row_count(nrow(sample_df))
  })
  
  observeEvent(input$btn_save_task, {
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con), ctx$col_map)
    # 校验并解析日期：非空输入必须能解析为合法日期，否则阻止保存
    validate_date_input <- function(x) {
      if (is.null(x)) return(list(ok = TRUE, value = NA))
      xc <- tryCatch(trimws(as.character(x)), error = function(e) "")
      if (!nzchar(xc)) return(list(ok = TRUE, value = NA))
      d <- tryCatch(as.Date(xc, optional = TRUE), error = function(e) NA)
      if (is.na(d)) return(list(ok = FALSE, value = NA))
      list(ok = TRUE, value = d)
    }
    psd_res <- validate_date_input(input$edit_planned_start_date)
    asd_res <- validate_date_input(input$edit_actual_start_date)
    pd_res  <- validate_date_input(input$edit_planned_date)
    ad_res  <- validate_date_input(input$edit_actual_date)
    if (!psd_res$ok || !asd_res$ok || !pd_res$ok || !ad_res$ok) {
      showNotification("日期格式错误或不存在", type = "error")
      return()
    }
    psd_val <- psd_res$value
    asd_val <- asd_res$value
    pd_val  <- pd_res$value
    ad_val  <- ad_res$value
    # 业务校验：计划结束日期不能早于有效开始日期（实际开始优先）
    eff_start <- if (!is.na(asd_val)) asd_val else psd_val
    if (!is.na(eff_start) && !is.na(pd_val) && pd_val < eff_start) {
      showNotification("计划结束日期不能早于开始日期，请检查后重新填写。", type = "error")
      return()
    }
    # 进度：09 表 progress 存 0-100
    progress_val <- if (is.null(input$edit_progress)) 0 else as.numeric(input$edit_progress)
    importance_val <- if (is.null(input$edit_importance) || !nzchar(trimws(as.character(input$edit_importance)))) NA_character_ else trimws(as.character(input$edit_importance))
    auth <- current_user_auth()
    actor_name <- normalize_text(auth$name)
    if (is.na(actor_name)) actor_name <- normalize_text(auth$work_id)
    if (is.na(actor_name)) actor_name <- "未知用户"

    # 备注 JSON：逐条读取并生成用户版本
    remark_df <- ctx$remark_entries
    if (is.null(remark_df)) remark_df <- empty_remark_df()
    n_remark_rows <- remark_row_count()
    if (is.null(n_remark_rows) || n_remark_rows < 0L) n_remark_rows <- 0L
    if (n_remark_rows > 0L) {
      for (i in seq_len(n_remark_rows)) {
        type_val <- tryCatch(trimws(as.character(input[[paste0("remark_type_", i)]])), error = function(e) "")
        content <- tryCatch(trimws(as.character(input[[paste0("remark_content_", i)]])), error = function(e) "")
        entry_key_i <- if (nrow(remark_df) >= i) as.character(remark_df$entry_key[i]) else ""
        updated_at_i <- if (nrow(remark_df) >= i) as.character(remark_df$updated_at[i] %||% "") else ""
        if (i <= nrow(remark_df)) {
          remark_df$entry_key[i] <- entry_key_i
          remark_df$reporter[i] <- actor_name
          remark_df$updated_at[i] <- updated_at_i
          remark_df$type[i] <- type_val
          remark_df$content[i] <- content
        } else {
          remark_df[i, ] <- list(entry_key_i, actor_name, updated_at_i, type_val, content)
        }
      }
    }
    if (n_remark_rows < nrow(remark_df)) remark_df <- remark_df[seq_len(n_remark_rows), , drop = FALSE]
    user_remark_map <- build_remark_map_from_df(remark_df, actor = actor_name)

    # 针对 S09“验证试验开展与数据管理”的样本来源与数量特殊处理
    is_s09_task <- identical(ctx$task_name, "S09_验证试验开展与数据管理") && ctx$is_sync
    sample_col <- "sample_json"
    sample_df <- ctx$samples
    if (is.null(sample_df)) sample_df <- empty_sample_df()
    if (is_s09_task) {
      n_rows <- sample_row_count()
      if (is.null(n_rows) || n_rows < 0L) n_rows <- 0L
      if (n_rows > 0L) {
        for (i in seq_len(n_rows)) {
          h_input <- input[[paste0("sample_hospital_", i)]]
          c_input <- input[[paste0("sample_count_", i)]]
          h_val <- tryCatch(trimws(as.character(h_input)), error = function(e) "")
          c_num <- suppressWarnings(as.numeric(c_input))
          if (is.na(c_num)) c_num <- 0
          entry_key_i <- if (nrow(sample_df) >= i) as.character(sample_df$entry_key[i]) else ""
          if (i <= nrow(sample_df)) {
            sample_df$entry_key[i] <- entry_key_i
            sample_df$hospital[i] <- h_val
            sample_df$count[i] <- c_num
          } else {
            sample_df[i, ] <- list(entry_key_i, h_val, c_num)
          }
        }
      }
      if (n_rows < nrow(sample_df)) sample_df <- sample_df[seq_len(n_rows), , drop = FALSE]
    }
    tbl <- ctx$table_name
    cm <- ctx$col_map
    row_id <- ctx$stage_instance_id
    if (is.na(row_id)) return()
    progress_num <- suppressWarnings(as.numeric(progress_val))
    if (is.na(progress_num)) progress_num <- 0
    progress_num <- pmax(0, pmin(100, progress_num))
    main_cols <- c(cm$planned_start, cm$actual_start, cm$plan, cm$act, cm$note, cm$progress)
    if (is_s09_task) main_cols <- c(main_cols, sample_col)
    snapshot_main_row <- ctx$snapshot_row %||% list()
    snapshot_project_row <- ctx$snapshot_project_row %||% list()
    snapshot_note_map <- parse_named_json_map(snapshot_main_row[[ctx$note_col]])
    snapshot_sample_map <- if (is_s09_task) parse_sample_map(snapshot_main_row[[sample_col]]) else list()

    user_main_row <- list()
    user_main_row[[cm$planned_start]] <- psd_val
    user_main_row[[cm$actual_start]]  <- asd_val
    user_main_row[[cm$plan]] <- pd_val
    user_main_row[[cm$act]] <- ad_val
    user_main_row[[cm$progress]] <- progress_num
    user_sample_map <- if (is_s09_task) build_sample_map_from_df(sample_df, actor = actor_name) else list()

    main_field_specs <- list(
      list(col = cm$planned_start, label = "计划开始时间", normalize = normalize_date_text),
      list(col = cm$actual_start,  label = "实际开始时间", normalize = normalize_date_text),
      list(col = cm$plan,          label = "计划完成时间", normalize = normalize_date_text),
      list(col = cm$act,           label = "实际完成时间", normalize = normalize_date_text),
      list(col = cm$progress,      label = "当前进度",     normalize = normalize_progress_text)
    )
    ctx$planned_start_date <- psd_val
    ctx$actual_start_date  <- asd_val
    ctx$planned_end_date <- pd_val
    ctx$actual_end_date <- ad_val
    ctx$progress <- progress_num / 100
    ctx$importance <- importance_val
    ctx$remark_entries <- remark_df
    if (is_s09_task) ctx$samples <- sample_df
    task_edit_context(ctx)
    reopen_current_task <- function() {
      task_edit_context(ctx)
      open_task_edit_modal(ctx)
      invisible(NULL)
    }
    refresh_and_reopen_task <- function() {
      latest_ctx <- refresh_task_context_from_db(ctx)
      if (is.null(latest_ctx)) {
        showNotification("刷新失败：当前记录已不存在。", type = "error")
        return(invisible(NULL))
      }
      task_edit_context(latest_ctx)
      open_task_edit_modal(latest_ctx)
      invisible(NULL)
    }

    apply_task_save <- function(save_mode = c("block", "partial", "overwrite")) {
      save_mode <- match.arg(save_mode)
      overwrite_conflicts <- identical(save_mode, "overwrite")
      allow_partial_save <- identical(save_mode, "partial")
      result <- tryCatch(
        pool::poolWithTransaction(pg_con, function(conn) {
          locked_main_row <- fetch_row_snapshot(conn, tbl, row_id, main_cols, lock = TRUE)
          if (is.null(locked_main_row)) stop("记录不存在，无法保存。")
          main_scalar <- merge_scalar_fields(snapshot_main_row, locked_main_row, user_main_row, main_field_specs, overwrite_conflicts = overwrite_conflicts)
          note_merge <- merge_remark_field(
            snapshot_note_map,
            parse_named_json_map(locked_main_row[[ctx$note_col]]),
            user_remark_map,
            current_reporter = actor_name,
            overwrite_conflicts = overwrite_conflicts
          )
          sample_merge <- list(merged = list(), merged_json = locked_main_row[[sample_col]], changed_keys = character(0), conflicts = list())
          if (is_s09_task) {
            sample_merge <- merge_named_json_field(
              snapshot_sample_map,
              parse_sample_map(locked_main_row[[sample_col]]),
              user_sample_map,
              "样本来源与数",
              overwrite_conflicts = overwrite_conflicts
            )
          }

          project_scalar <- list(merged = list(), conflicts = list(), changed_updates = list())
          locked_project_row <- snapshot_project_row
          if (!is.na(ctx$proj_row_id)) {
            locked_project_row <- fetch_row_snapshot(conn, "04项目总表", ctx$proj_row_id, "重要紧急程度", lock = TRUE)
            if (is.null(locked_project_row)) stop("所属项目不存在，无法更新重要紧急程度。")
            project_scalar <- merge_scalar_fields(
              snapshot_project_row,
              locked_project_row,
              list("重要紧急程度" = importance_val),
              list(list(col = "重要紧急程度", label = "重要紧急程度", normalize = normalize_text)),
              overwrite_conflicts = overwrite_conflicts
            )
          }

          all_conflicts <- c(main_scalar$conflicts, note_merge$conflicts, sample_merge$conflicts, project_scalar$conflicts)
          if (!isTRUE(overwrite_conflicts) && !isTRUE(allow_partial_save) && length(all_conflicts) > 0) {
            return(list(status = "conflict", conflicts = all_conflicts))
          }

          updates_main <- main_scalar$changed_updates
          if (!identical(normalize_text(note_merge$merged_json, empty_as_na = FALSE), normalize_text(locked_main_row[[ctx$note_col]], empty_as_na = FALSE))) {
            updates_main[[ctx$note_col]] <- note_merge$merged_json
          }
          if (is_s09_task && !identical(normalize_text(sample_merge$merged_json, empty_as_na = FALSE), normalize_text(locked_main_row[[sample_col]], empty_as_na = FALSE))) {
            updates_main[[sample_col]] <- sample_merge$merged_json
          }
          if (length(updates_main) > 0) {
            execute_updates(conn, tbl, row_id, updates_main)
          }
          if (!is.na(ctx$proj_row_id) && length(project_scalar$changed_updates) > 0) {
            execute_updates(conn, "04项目总表", ctx$proj_row_id, project_scalar$changed_updates)
          }

          new_main_row <- locked_main_row
          for (nm in names(updates_main)) new_main_row[[nm]] <- updates_main[[nm]]
          new_project_row <- locked_project_row
          if (!is.na(ctx$proj_row_id)) {
            for (nm in names(project_scalar$changed_updates)) new_project_row[[nm]] <- project_scalar$changed_updates[[nm]]
          }

          old_audit <- list(
            计划开始时间 = locked_main_row[[cm$planned_start]],
            实际开始时间 = locked_main_row[[cm$actual_start]],
            计划完成时间 = locked_main_row[[cm$plan]],
            实际完成时间 = locked_main_row[[cm$act]],
            备注 = parse_named_json_map(locked_main_row[[ctx$note_col]]),
            当前进度 = locked_main_row[[cm$progress]]
          )
          new_audit <- list(
            计划开始时间 = new_main_row[[cm$planned_start]],
            实际开始时间 = new_main_row[[cm$actual_start]],
            计划完成时间 = new_main_row[[cm$plan]],
            实际完成时间 = new_main_row[[cm$act]],
            备注 = note_merge$merged,
            当前进度 = new_main_row[[cm$progress]]
          )
          old_importance <- locked_project_row[["重要紧急程度"]]
          new_importance <- new_project_row[["重要紧急程度"]]
          old_audit[["重要紧急程度"]] <- old_importance
          new_audit[["重要紧急程度"]] <- new_importance
          if (is_s09_task) {
            old_audit[["样本来源与数"]] <- parse_sample_df(locked_main_row[[sample_col]])
            new_audit[["样本来源与数"]] <- parse_sample_df(new_main_row[[sample_col]])
          }
          list(status = "saved", old_audit = old_audit, new_audit = new_audit, partial_conflicts = all_conflicts)
        }),
        error = function(e) list(status = "error", message = conditionMessage(e))
      )

      if (identical(result$status, "conflict")) {
        show_conflict_resolution_modal(
          "阶段数据存在并发冲突",
          result$conflicts,
          on_overwrite = function() apply_task_save("overwrite"),
          on_partial = function() apply_task_save("partial"),
          on_refresh = refresh_and_reopen_task,
          on_close = reopen_current_task
        )
        return(invisible(NULL))
      }
      if (identical(result$status, "error")) {
        showNotification(paste0("保存失败：", result$message), type = "error")
        return(invisible(NULL))
      }

      insert_audit_log(pg_con,
        work_id = auth$work_id, name = auth$name,
        op_type = "update_stage", target_table = tbl, target_row_id = row_id,
        biz_desc = "阶段时间与进度", summary = "修改阶段时间、进度、备注与重要紧急程度",
        old_val = result$old_audit, new_val = result$new_audit, remark = NULL
      )
      ctx$snapshot_row <- tryCatch(fetch_row_snapshot(pg_con, tbl, row_id, main_cols, lock = FALSE), error = function(e) ctx$snapshot_row)
      if (!is.na(ctx$proj_row_id)) {
        ctx$snapshot_project_row <- tryCatch(fetch_row_snapshot(pg_con, "04项目总表", ctx$proj_row_id, "重要紧急程度", lock = FALSE), error = function(e) ctx$snapshot_project_row)
      }
      ctx$planned_start_date <- psd_val
      ctx$actual_start_date  <- asd_val
      ctx$planned_end_date <- pd_val
      ctx$actual_end_date <- ad_val
      ctx$progress <- progress_num / 100
      ctx$importance <- importance_val
      ctx$remark <- ctx$snapshot_row[[ctx$note_col]]
      ctx$remark_entries <- parse_remark_json_to_df(ctx$snapshot_row[[ctx$note_col]])
      if (is_s09_task) ctx$samples <- parse_sample_df(ctx$snapshot_row[[sample_col]])
      task_edit_context(ctx)
      remark_row_count(nrow(ctx$remark_entries))
      if (is_s09_task) sample_row_count(nrow(ctx$samples))
      gantt_force_refresh(gantt_force_refresh() + 1L)
      tryCatch({ invisible(gantt_data_db()) }, error = function(e) NULL)
      removeModal()
      if (length(result$partial_conflicts %||% list()) > 0) {
        if (identical(save_mode, "partial")) {
        showNotification(sprintf("已保存不冲突内容，仍有 %d 项冲突内容未覆盖。", length(result$partial_conflicts)), type = "warning")
        } else if (identical(save_mode, "overwrite")) {
          showNotification(sprintf("已保存并覆盖 %d 项冲突内容，数据已重新加载。", length(result$partial_conflicts)), type = "message")
        } else {
          showNotification("保存成功，已重新加载数据。", type = "message")
        }
      } else {
        showNotification("保存成功，已重新加载数据。", type = "message")
      }
      invisible(NULL)
    }

    current_main_row <- tryCatch(fetch_row_snapshot(pg_con, tbl, row_id, main_cols, lock = FALSE), error = function(e) NULL)
    if (is.null(current_main_row)) {
      showNotification("记录不存在，无法保存。", type = "error")
      return()
    }
    pre_main_scalar <- merge_scalar_fields(snapshot_main_row, current_main_row, user_main_row, main_field_specs, overwrite_conflicts = FALSE)
    pre_note_merge <- merge_remark_field(
      snapshot_note_map,
      parse_named_json_map(current_main_row[[ctx$note_col]]),
      user_remark_map,
      current_reporter = actor_name,
      overwrite_conflicts = FALSE
    )
    pre_sample_merge <- list(conflicts = list(), changed_keys = character(0), merged_json = current_main_row[[sample_col]])
    if (is_s09_task) {
      pre_sample_merge <- merge_named_json_field(
        snapshot_sample_map,
        parse_sample_map(current_main_row[[sample_col]]),
        user_sample_map,
        "样本来源与数",
        overwrite_conflicts = FALSE
      )
    }
    pre_project_conflicts <- list()
    pre_project_merge_labels <- character(0)
    current_project_row <- NULL
    if (!is.na(ctx$proj_row_id)) {
      current_project_row <- tryCatch(fetch_row_snapshot(pg_con, "04项目总表", ctx$proj_row_id, "重要紧急程度", lock = FALSE), error = function(e) NULL)
      if (!is.null(current_project_row)) {
        pre_project_merge <- merge_scalar_fields(
          snapshot_project_row,
          current_project_row,
          list("重要紧急程度" = importance_val),
          list(list(col = "重要紧急程度", label = "重要紧急程度", normalize = normalize_text)),
          overwrite_conflicts = FALSE
        )
        pre_project_conflicts <- pre_project_merge$conflicts
        pre_project_merge_labels <- pre_project_merge$changed_labels
      }
    }
    pre_conflicts <- c(pre_main_scalar$conflicts, pre_note_merge$conflicts, pre_sample_merge$conflicts, pre_project_conflicts)
    stale_any <- state_changed(snapshot_main_row, current_main_row) || state_changed(snapshot_note_map, parse_named_json_map(current_main_row[[ctx$note_col]])) ||
      (is_s09_task && state_changed(snapshot_sample_map, parse_sample_map(current_main_row[[sample_col]]))) ||
      (!is.null(current_project_row) && state_changed(snapshot_project_row, current_project_row))
    auto_merge_items <- character(0)
    if (stale_any) {
      auto_merge_items <- c(
        pre_main_scalar$changed_labels,
        format_json_auto_merge_items("问题/卡点/经验分享", pre_note_merge$changed_keys),
        if (is_s09_task) format_json_auto_merge_items("样本来源与数", pre_sample_merge$changed_keys) else character(0),
        pre_project_merge_labels
      )
    }
    if (length(pre_conflicts) > 0) {
      show_conflict_resolution_modal(
        "阶段数据存在并发冲突",
        pre_conflicts,
        on_overwrite = function() apply_task_save("overwrite"),
        on_partial = function() apply_task_save("partial"),
        on_refresh = refresh_and_reopen_task,
        on_close = reopen_current_task
      )
    } else {
      apply_task_save("block")
    }
  })
}



shinyApp(ui = ui, server = server)
