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
# 04项目总表：同步阶段（项目级，来自 04 表）
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
# 03医院_项目表：分中心阶段（来自 03 表）
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

  # ---------- 操作审计：写入 07操作审计表 ----------
  insert_audit_log <- function(conn, work_id, name, op_type, target_table, target_row_id, biz_desc, summary, old_val, new_val, remark = NULL) {
    if (is.null(conn) || !DBI::dbIsValid(conn)) return(invisible(NULL))
    work_id <- if (is.null(work_id) || is.na(work_id)) "" else as.character(work_id)
    name    <- if (is.null(name) || is.na(name)) "" else as.character(name)
    old_json <- if (is.null(old_val)) "null" else jsonlite::toJSON(old_val, auto_unbox = TRUE)
    new_json <- if (is.null(new_val)) "null" else jsonlite::toJSON(new_val, auto_unbox = TRUE)
    remark  <- if (is.null(remark) || !nzchar(trimws(as.character(remark)))) NA_character_ else as.character(remark)
    q <- 'INSERT INTO public."07操作审计表" ("操作时间", "操作人工号", "操作人姓名", "操作类型", "目标表", "目标行id", "业务描述", "变更摘要", "旧值", "新值", "备注") VALUES (current_timestamp, $1, $2, $3, $4, $5, $6, $7, $8::json, $9::json, $10)'
    tryCatch({
      DBI::dbExecute(conn, q, params = list(work_id, name, op_type, target_table, as.integer(target_row_id), biz_desc, summary, old_json, new_json, remark))
    }, error = function(e) NULL)
    invisible(NULL)
  }

  # ---------- 系统确权：从 Nginx 传入的 X-User 头推导权限 ----------
  current_user_auth <- reactive({
    wid <- session$request$HTTP_X_USER
    wid <- if (is.null(wid)) "" else trimws(as.character(wid))
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = ""))
    if (!nzchar(wid)) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = ""))
    tryCatch({
      r <- DBI::dbGetQuery(pg_con, "SELECT id, \"姓名\", \"组别\", \"数据库权限等级\", \"人员状态\" FROM public.\"05人员表\" WHERE \"工号\" = $1 LIMIT 1", params = list(wid))
      if (nrow(r) == 0) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = ""))
      status <- trimws(as.character(r[["人员状态"]][1]))
      level  <- trimws(as.character(r[["数据库权限等级"]][1]))
      if (is.na(status) || status != "在职") return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = ""))
      if (is.na(level) || level == "deny_access") return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = ""))
      name <- if ("姓名" %in% names(r) && !is.na(r[["姓名"]][1])) trimws(as.character(r[["姓名"]][1])) else ""
      if (level == "super_admin") return(list(allow_all = TRUE, allow_none = FALSE, allowed_subquery = "", work_id = wid, name = name))
      pid <- as.integer(r[["id"]][1])
      if (level == "common_user") {
        subq <- sprintf(
          "SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" = %d UNION SELECT \"04项目总表_id\" FROM public.\"_nc_m2m_04项目总表_05人员表\" WHERE \"05人员表_id\" = %d",
          pid, pid
        )
        return(list(allow_all = FALSE, allow_none = FALSE, allowed_subquery = subq, work_id = wid, name = name))
      }
      if (level == "group_manager") {
        subq <- sprintf(
          "SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" IN (SELECT id FROM public.\"05人员表\" WHERE \"组别\" IS NOT DISTINCT FROM (SELECT \"组别\" FROM public.\"05人员表\" WHERE id = %d)) UNION SELECT \"04项目总表_id\" FROM public.\"_nc_m2m_04项目总表_05人员表\" WHERE \"05人员表_id\" IN (SELECT id FROM public.\"05人员表\" WHERE \"组别\" IS NOT DISTINCT FROM (SELECT \"组别\" FROM public.\"05人员表\" WHERE id = %d))",
          pid, pid
        )
        return(list(allow_all = FALSE, allow_none = FALSE, allowed_subquery = subq, work_id = wid, name = name))
      }
      list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "")
    }, error = function(e) list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = ""))
  })

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
      hospital = input$filter_hospital
    )
  })
  gantt_filter_state_debounced <- debounce(gantt_filter_state, 400)
  # 阶段 -> 数据库列映射（用于 UPDATE），已适配 S01-S15 新结构
  stage_col_map_04 <- list(
    "S01_需求与背景调研" = list(
      start = "S01_Start_需求与背景调研_开始时间",
      plan = "S01_Plan_需求与背景调研_计划完成时间",
      act = "S01_Act_需求与背景调研_实际完成时间",
      note = "S01_Note_需求与背景调研_备注信息",
      progress = "S01_Progress_需求与背景调研_当前进度"
    ),
    "S02_方案设计审核" = list(
      start = "S02_Start_方案设计审核_开始时间",
      plan = "S02_Plan_方案设计审核_计划完成时间",
      act = "S02_Act_方案设计审核_实际完成时间",
      note = "S02_Note_方案设计审核_备注信息",
      progress = "S02_Progress_方案设计审核_当前进度"
    ),
    "S03_医院筛选与专家对接" = list(
      start = "S03_Start_医院筛选与专家对接_开始时间",
      plan = "S03_Plan_医院筛选与专家对接_计划完成时间",
      act = "S03_Act_医院筛选与专家对接_实际完成时间",
      note = "S03_Note_医院筛选与专家对接_备注信息",
      progress = "S03_Progress_医院筛选与专家对接_当前进度"
    ),
    "S09_验证试验开展与数据管理" = list(
      start = "S09_Start_验证试验开展与数据管理_开始时间",
      plan = "S09_Plan_验证试验开展与数据管理_计划完成时间",
      act = "S09_Act_验证试验开展与数据管理_实际完成时间",
      note = "S09_Note_验证试验开展与数据管理_备注信息",
      progress = "S09_Progress_验证试验开展与数据管理_当前进度"
    ),
    "S10_总报告输出与定稿" = list(
      start = "S10_Start_总报告输出与定稿_开始时间",
      plan = "S10_Plan_总报告输出与定稿_计划完成时间",
      act = "S10_Act_总报告输出与定稿_实际完成时间",
      note = "S10_Note_总报告输出与定稿_备注信息",
      progress = "S10_Progress_总报告输出与定稿_当前进度"
    ),
    "S11_资料递交与结题归档" = list(
      start = "S11_Start_资料递交与结题归档_开始时间",
      plan = "S11_Plan_资料递交与结题归档_计划完成时间",
      act = "S11_Act_资料递交与结题归档_实际完成时间",
      note = "S11_Note_资料递交与结题归档_备注信息",
      progress = "S11_Progress_资料递交与结题归档_当前进度"
    ),
    "S12_临床试验发补与资料递交" = list(
      start = "S12_Start_临床试验发补与资料递交_开始时间",
      plan = "S12_Plan_临床试验发补与资料递交_计划完成时间",
      act = "S12_Act_临床试验发补与资料递交_实际完成时间",
      note = "S12_Note_临床试验发补与资料递交_备注信息",
      progress = "S12_Progress_临床试验发补与资料递交_当前进度"
    ),
    "S13_文章初稿输出" = list(
      start = "S13_Start_文章初稿输出_开始时间",
      plan = "S13_Plan_文章初稿输出_计划完成时间",
      act = "S13_Act_文章初稿输出_实际完成时间",
      note = "S13_Note_文章初稿输出_备注信息",
      progress = "S13_Progress_文章初稿输出_当前进度"
    ),
    "S14_文章内部审评、修改、投递" = list(
      start = "S14_Start_文章内部审评、修改、投递_开始时间",
      plan = "S14_Plan_文章内部审评、修改、投递_计划完成时",
      act = "S14_Act_文章内部审评、修改、投递_实际完成时间",
      note = "S14_Note_文章内部审评、修改、投递_备注信息",
      progress = "S14_Progress_文章内部审评、修改、投递_当前进度"
    ),
    "S15_意见反馈与文章返修" = list(
      start = "S15_Start_意见反馈与文章返修_开始时间",
      plan = "S15_Plan_意见反馈与文章返修_计划完成时间",
      act = "S15_Act_意见反馈与文章返修_实际完成时间",
      note = "S15_Note_意见反馈与文章返修_备注信息",
      progress = "S15_Progress_意见反馈与文章返修_当前进度"
    )
  )
  stage_col_map_03 <- list(
    "S04_医院立项资料输出与递交" = list(
      start = "S04_Start_医院立项资料输出与递交_开始时间",
      plan = "S04_Plan_医院立项资料输出与递交_计划完成时间",
      act = "S04_Act_医院立项资料输出与递交_实际完成时间",
      note = "S04_Note_医院立项资料输出与递交_备注信息",
      progress = "S04_Progress_医院立项资料输出与递交_当前进度"
    ),
    "S05_伦理审批与启动会" = list(
      start = "S05_Start_伦理审批与启动会_开始时间",
      plan = "S05_Plan_伦理审批与启动会_计划完成时间",
      act = "S05_Act_伦理审批与启动会_实际完成时间",
      note = "S05_Note_伦理审批与启动会_备注信息",
      progress = "S05_Progress_伦理审批与启动会_当前进度"
    ),
    "S06_人员与物资准备" = list(
      start = "S06_Start_人员与物资准备_开始时间",
      plan = "S06_Plan_人员与物资准备_计划完成时间",
      act = "S06_Act_人员与物资准备_实际完成时间",
      note = "S06_Note_人员与物资准备_备注信息",
      progress = "S06_Progress_人员与物资准备_当前进度"
    ),
    "S07_试验开展与数据汇总表管理" = list(
      start = "S07_Start_试验开展与数据汇总表管理_开始时间",
      plan = "S07_Plan_试验开展与数据汇总表管理_计划完成时",
      act = "S07_Act_试验开展与数据汇总表管理_实际完成时间",
      note = "S07_Note_试验开展与数据汇总表管理_备注信息",
      progress = "S07_Progress_试验开展与数据汇总表管理_当前进度"
    ),
    "S08_小结输出与定稿" = list(
      start = "S08_Start_小结输出与定稿_开始时间",
      plan = "S08_Plan_小结输出与定稿_计划完成时间",
      act = "S08_Act_小结输出与定稿_实际完成时间",
      note = "S08_Note_小结输出与定稿_备注信息",
      progress = "S08_Progress_小结输出与定稿_当前进度"
    )
  )

  # 阶段 -> 进度贡献者 JSON 列映射
  stage_contrib_04 <- c(
    "S01_需求与背景调研"           = "S01_Contributors_需求与背景调研_进度贡献者",
    "S02_方案设计审核"             = "S02_Contributors_方案设计审核_进度贡献者",
    "S03_医院筛选与专家对接"       = "S03_Contributors_医院筛选与专家对接_进度贡献者",
    "S09_验证试验开展与数据管理"   = "S09_Contributors_验证试验开展与数据管理_进度贡献",
    "S10_总报告输出与定稿"         = "S10_Contributors_总报告输出与定稿_进度贡献者",
    "S11_资料递交与结题归档"       = "S11_Contributors_资料递交与结题归档_进度贡献者",
    "S12_临床试验发补与资料递交"   = "S12_Contributors_临床试验发补与资料递交_进度贡献",
    "S13_文章初稿输出"             = "S13_Contributors_文章初稿输出_进度贡献者",
    "S14_文章内部审评、修改、投递" = "S14_Contributors_文章内部审评_修改_投递_进度贡献",
    "S15_意见反馈与文章返修"       = "S15_Contributors_意见反馈与文章返修_进度贡献者"
  )

  stage_contrib_03 <- c(
    "S04_医院立项资料输出与递交"   = "S04_Contributors_医院立项资料输出与递交_进度贡献",
    "S05_伦理审批与启动会"         = "S05_Contributors_伦理审批与启动会_进度贡献者",
    "S06_人员与物资准备"           = "S06_Contributors_人员与物资准备_进度贡献者",
    "S07_试验开展与数据汇总表管理" = "S07_Contributors_试验开展与数据汇总表管理_进度贡",
    "S08_小结输出与定稿"           = "S08_Contributors_小结输出与定稿_进度贡献者"
  )
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
            column(3, selectInput("filter_hospital", "相关医院（有中心）", choices = opts$hospitals, multiple = TRUE, selectize = TRUE))
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
      # 按筛选条件在数据库层取 04 表：先解析参与人/负责人/医院对应的 id 列表
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
      # 构建 04项目总表 的 WHERE 与参数（含权限：仅可访问项目）
      where_04 <- c('"项目名称" IS NOT NULL', '"项目类型" IS NOT NULL')
      if (!auth$allow_all) where_04 <- c(where_04, paste0("id IN (", auth$allowed_subquery, ")"))
      params_04 <- list()
      p <- 0L
      if (length(ft) > 0) {
        where_04 <- c(where_04, paste0('"项目类型" IN (', paste(sprintf("$%d", p + seq_along(ft)), collapse = ","), ")"))
        params_04 <- c(params_04, as.list(ft))
        p <- p + length(ft)
      }
      if (length(fn) > 0) {
        where_04 <- c(where_04, paste0('"项目名称" IN (', paste(sprintf("$%d", p + seq_along(fn)), collapse = ","), ")"))
        params_04 <- c(params_04, as.list(fn))
        p <- p + length(fn)
      }
      if (length(manager_ids) > 0) {
        where_04 <- c(where_04, paste0('"05人员表_id" IN (', paste(sprintf("$%d", p + seq_along(manager_ids)), collapse = ","), ")"))
        params_04 <- c(params_04, as.list(manager_ids))
        p <- p + length(manager_ids)
      }
      if (length(proj_ids_participant) > 0) {
        where_04 <- c(where_04, paste0('id IN (', paste(sprintf("$%d", p + seq_along(proj_ids_participant)), collapse = ","), ")"))
        params_04 <- c(params_04, as.list(proj_ids_participant))
        p <- p + length(proj_ids_participant)
      }
      if (length(fi) > 0) {
        where_04 <- c(where_04, paste0('"重要紧急程度" IN (', paste(sprintf("$%d", p + seq_along(fi)), collapse = ","), ")"))
        params_04 <- c(params_04, as.list(fi))
        p <- p + length(fi)
      }
      if (length(proj_ids_hosp) > 0) {
        where_04 <- c(where_04, paste0('id IN (', paste(sprintf("$%d", p + seq_along(proj_ids_hosp)), collapse = ","), ")"))
        params_04 <- c(params_04, as.list(proj_ids_hosp))
      }
      sql_04 <- paste0('SELECT * FROM public."04项目总表" WHERE ', paste(where_04, collapse = " AND "))
      if (length(params_04) > 0) {
        proj <- DBI::dbGetQuery(pg_con, sql_04, params = params_04)
      } else {
        proj <- DBI::dbGetQuery(pg_con, sql_04)
      }
      proj_id_list <- if (nrow(proj) > 0) proj$id else integer(0)
      # 只取属于当前筛选后项目的中心数据（无项目时取空结构避免后续 join 报错）
      if (length(proj_id_list) == 0) {
        site <- DBI::dbGetQuery(pg_con, 'SELECT * FROM public."03医院_项目表" WHERE 1 = 0')
      } else {
        sql_site <- paste0('SELECT * FROM public."03医院_项目表" WHERE "project_table 项目总表_id" IN (', paste(sprintf("$%d", seq_along(proj_id_list)), collapse = ","), ")")
        site <- DBI::dbGetQuery(pg_con, sql_site, params = as.list(proj_id_list))
      }
      hosp <- DBI::dbGetQuery(pg_con, 'SELECT id, "医院名称" FROM public."01医院信息表"')
      person <- DBI::dbGetQuery(pg_con, 'SELECT id, "姓名" FROM public."05人员表"')
      person_name_col <- intersect(c("姓名", "name"), names(person))[1]
      proj_name_col <- intersect(c("项目名称", "project_name"), names(proj))[1]
      proj$project_id <- if (!is.na(proj_name_col) && proj_name_col %in% names(proj)) coalesce(proj[[proj_name_col]], paste0("项目-", proj$id)) else paste0("项目-", proj$id)
      if (!is.na(person_name_col) && "05人员表_id" %in% names(proj))
        proj <- proj %>% left_join(
          person %>% select(id, manager_name = all_of(person_name_col)),
          by = c("05人员表_id" = "id")
        )
      else proj$manager_name <- NA_character_
      proj_type_col <- intersect(c("项目类型", "project_type"), names(proj))[1]
      importance_col <- intersect(c("重要紧急程度", "importance_level"), names(proj))[1]
      hosp_name_col <- intersect(c("医院名称", "hospital_name"), names(hosp))[1]
      hosp$hospital_name <- if (!is.na(hosp_name_col)) hosp[[hosp_name_col]] else paste0("中心-", hosp$id)
      hosp <- hosp %>% select(id, hospital_name)
      site <- site %>%
        left_join(hosp, by = c("01_hos_resource_table医院信息表_id" = "id")) %>%
        mutate(site_name = coalesce(hospital_name, paste0("中心-", id))) %>%
        left_join(proj %>% select(id, project_id), by = c("project_table 项目总表_id" = "id"))

      proj_sites <- if (nrow(proj) > 0) {
        out <- list()
        for (pid in unique(proj$project_id)) {
          s <- site %>% filter(project_id == pid, !is.na(project_id)) %>% pull(site_name) %>% unique()
          sites <- if (length(s) == 0) paste0(pid, "_待分配中心") else s
          for (sn in sites) out[[length(out) + 1]] <- tibble(project_id = pid, site_name = sn)
        }
        bind_rows(out)
      } else {
        tibble(project_id = "待创建项目", site_name = "待分配中心")
      }

      stage_notes_04 <- c(
        "S01_需求与背景调研" = "S01_Note_需求与背景调研_备注信息",
        "S02_方案设计审核" = "S02_Note_方案设计审核_备注信息",
        "S03_医院筛选与专家对接" = "S03_Note_医院筛选与专家对接_备注信息",
        "S09_验证试验开展与数据管理" = "S09_Note_验证试验开展与数据管理_备注信息",
        "S10_总报告输出与定稿" = "S10_Note_总报告输出与定稿_备注信息",
        "S11_资料递交与结题归档" = "S11_Note_资料递交与结题归档_备注信息",
        "S12_临床试验发补与资料递交" = "S12_Note_临床试验发补与资料递交_备注信息",
        "S13_文章初稿输出" = "S13_Note_文章初稿输出_备注信息",
        "S14_文章内部审评、修改、投递" = "S14_Note_文章内部审评、修改、投递_备注信息",
        "S15_意见反馈与文章返修" = "S15_Note_意见反馈与文章返修_备注信息"
      )
      stage_notes_03 <- c(
        "S04_医院立项资料输出与递交" = "S04_Note_医院立项资料输出与递交_备注信息",
        "S05_伦理审批与启动会" = "S05_Note_伦理审批与启动会_备注信息",
        "S06_人员与物资准备" = "S06_Note_人员与物资准备_备注信息",
        "S07_试验开展与数据汇总表管理" = "S07_Note_试验开展与数据汇总表管理_备注信息",
        "S08_小结输出与定稿" = "S08_Note_小结输出与定稿_备注信息"
      )
      stage_cols_04 <- list(
        "S01_需求与背景调研" = c(
          "S01_Start_需求与背景调研_开始时间",
          "S01_Progress_需求与背景调研_当前进度",
          "S01_Plan_需求与背景调研_计划完成时间",
          "S01_Act_需求与背景调研_实际完成时间"
        ),
        "S02_方案设计审核" = c(
          "S02_Start_方案设计审核_开始时间",
          "S02_Progress_方案设计审核_当前进度",
          "S02_Plan_方案设计审核_计划完成时间",
          "S02_Act_方案设计审核_实际完成时间"
        ),
        "S03_医院筛选与专家对接" = c(
          "S03_Start_医院筛选与专家对接_开始时间",
          "S03_Progress_医院筛选与专家对接_当前进度",
          "S03_Plan_医院筛选与专家对接_计划完成时间",
          "S03_Act_医院筛选与专家对接_实际完成时间"
        ),
        "S09_验证试验开展与数据管理" = c(
          "S09_Start_验证试验开展与数据管理_开始时间",
          "S09_Progress_验证试验开展与数据管理_当前进度",
          "S09_Plan_验证试验开展与数据管理_计划完成时间",
          "S09_Act_验证试验开展与数据管理_实际完成时间"
        ),
        "S10_总报告输出与定稿" = c(
          "S10_Start_总报告输出与定稿_开始时间",
          "S10_Progress_总报告输出与定稿_当前进度",
          "S10_Plan_总报告输出与定稿_计划完成时间",
          "S10_Act_总报告输出与定稿_实际完成时间"
        ),
        "S11_资料递交与结题归档" = c(
          "S11_Start_资料递交与结题归档_开始时间",
          "S11_Progress_资料递交与结题归档_当前进度",
          "S11_Plan_资料递交与结题归档_计划完成时间",
          "S11_Act_资料递交与结题归档_实际完成时间"
        ),
        "S12_临床试验发补与资料递交" = c(
          "S12_Start_临床试验发补与资料递交_开始时间",
          "S12_Progress_临床试验发补与资料递交_当前进度",
          "S12_Plan_临床试验发补与资料递交_计划完成时间",
          "S12_Act_临床试验发补与资料递交_实际完成时间"
        ),
        "S13_文章初稿输出" = c(
          "S13_Start_文章初稿输出_开始时间",
          "S13_Progress_文章初稿输出_当前进度",
          "S13_Plan_文章初稿输出_计划完成时间",
          "S13_Act_文章初稿输出_实际完成时间"
        ),
        "S14_文章内部审评、修改、投递" = c(
          "S14_Start_文章内部审评、修改、投递_开始时间",
          "S14_Progress_文章内部审评、修改、投递_当前进度",
          "S14_Plan_文章内部审评、修改、投递_计划完成时",
          "S14_Act_文章内部审评、修改、投递_实际完成时间"
        ),
        "S15_意见反馈与文章返修" = c(
          "S15_Start_意见反馈与文章返修_开始时间",
          "S15_Progress_意见反馈与文章返修_当前进度",
          "S15_Plan_意见反馈与文章返修_计划完成时间",
          "S15_Act_意见反馈与文章返修_实际完成时间"
        )
      )
      stage_cols_03 <- list(
        "S04_医院立项资料输出与递交" = c(
          "S04_Start_医院立项资料输出与递交_开始时间",
          "S04_Progress_医院立项资料输出与递交_当前进度",
          "S04_Plan_医院立项资料输出与递交_计划完成时间",
          "S04_Act_医院立项资料输出与递交_实际完成时间"
        ),
        "S05_伦理审批与启动会" = c(
          "S05_Start_伦理审批与启动会_开始时间",
          "S05_Progress_伦理审批与启动会_当前进度",
          "S05_Plan_伦理审批与启动会_计划完成时间",
          "S05_Act_伦理审批与启动会_实际完成时间"
        ),
        "S06_人员与物资准备" = c(
          "S06_Start_人员与物资准备_开始时间",
          "S06_Progress_人员与物资准备_当前进度",
          "S06_Plan_人员与物资准备_计划完成时间",
          "S06_Act_人员与物资准备_实际完成时间"
        ),
        "S07_试验开展与数据汇总表管理" = c(
          "S07_Start_试验开展与数据汇总表管理_开始时间",
          "S07_Progress_试验开展与数据汇总表管理_当前进度",
          "S07_Plan_试验开展与数据汇总表管理_计划完成时",
          "S07_Act_试验开展与数据汇总表管理_实际完成时间"
        ),
        "S08_小结输出与定稿" = c(
          "S08_Start_小结输出与定稿_开始时间",
          "S08_Progress_小结输出与定稿_当前进度",
          "S08_Plan_小结输出与定稿_计划完成时间",
          "S08_Act_小结输出与定稿_实际完成时间"
        )
      )

      all_rows <- list()
      # 阶段排序：按业务顺序 S01-S15（同步阶段与分中心阶段混排）
      stage_order <- c(
        "S01_需求与背景调研",
        "S02_方案设计审核",
        "S03_医院筛选与专家对接",
        "S04_医院立项资料输出与递交",
        "S05_伦理审批与启动会",
        "S06_人员与物资准备",
        "S07_试验开展与数据汇总表管理",
        "S08_小结输出与定稿",
        "S09_验证试验开展与数据管理",
        "S10_总报告输出与定稿",
        "S11_资料递交与结题归档",
        "S12_临床试验发补与资料递交",
        "S13_文章初稿输出",
        "S14_文章内部审评、修改、投递",
        "S15_意见反馈与文章返修"
      )
      for (ri in seq_len(nrow(proj_sites))) {
        pid <- proj_sites$project_id[ri]
        sname <- proj_sites$site_name[ri]
        proj_idx <- which(proj$project_id == pid)[1]
        proj_type <- if (!is.na(proj_idx) && !is.na(proj_type_col)) as.character(proj[[proj_type_col]][proj_idx]) else NA_character_
        valid_short <- if (!is.na(proj_type) && proj_type %in% names(project_type_valid_stages)) project_type_valid_stages[[proj_type]] else unname(task_short_name)
        site_row <- site %>% filter(project_id == pid, site_name == sname) %>% slice(1)
        for (st in names(stage_cols_04)) {
          if (!is.na(task_short_name[st]) && !task_short_name[[st]] %in% valid_short) next
          cols <- stage_cols_04[[st]]
          sd <- pd <- ad <- NA
          pg <- 0
          unplanned <- TRUE
          remark <- NA_character_
          if (!is.na(proj_idx) && all(cols %in% names(proj))) {
            sd <- as.Date(proj[[cols[1]]][proj_idx])
            pd <- as.Date(proj[[cols[3]]][proj_idx])
            ad <- as.Date(proj[[cols[4]]][proj_idx])
            pg <- norm_progress(proj[[cols[2]]][proj_idx])
            unplanned <- is.na(sd) || is.na(pd)
            if (st %in% names(stage_notes_04) && stage_notes_04[[st]] %in% names(proj))
              remark <- as.character(proj[[stage_notes_04[[st]]]][proj_idx])
          }
          all_rows[[length(all_rows) + 1]] <- tibble(
            project_id = pid,
            site_name = sname,
            project_type = proj_type,
            `重要紧急程度` = if (!is.na(proj_idx) && "重要紧急程度" %in% names(proj)) as.character(proj[["重要紧急程度"]][proj_idx]) else NA_character_,
            task_name = st,
            task_type = "Process",
            start_date = sd,
            planned_end_date = pd,
            actual_end_date = ad,
            progress = pg,
            is_unplanned = unplanned,
            stage_ord = match(st, stage_order),
            remark = remark,
            proj_row_id = if (!is.na(proj_idx)) proj$id[proj_idx] else NA_integer_,
            site_row_id = NA_integer_,
            manager_name = if (!is.na(proj_idx) && "manager_name" %in% names(proj)) proj$manager_name[proj_idx] else NA_character_
          )
        }
        for (st in names(stage_cols_03)) {
          if (!is.na(task_short_name[st]) && !task_short_name[[st]] %in% valid_short) next
          cols <- stage_cols_03[[st]]
          sd <- pd <- ad <- NA
          pg <- 0
          unplanned <- TRUE
          remark <- NA_character_
          if (nrow(site_row) > 0 && all(cols %in% names(site_row))) {
            sd <- as.Date(site_row[[cols[1]]][1])
            pd <- as.Date(site_row[[cols[3]]][1])
            ad <- as.Date(site_row[[cols[4]]][1])
            pg <- norm_progress(site_row[[cols[2]]][1])
            unplanned <- is.na(sd) || is.na(pd)
            if (st %in% names(stage_notes_03) && stage_notes_03[[st]] %in% names(site_row))
              remark <- as.character(site_row[[stage_notes_03[[st]]]][1])
          }
          all_rows[[length(all_rows) + 1]] <- tibble(
            project_id = pid,
            site_name = sname,
            project_type = proj_type,
            `重要紧急程度` = if (!is.na(proj_idx) && "重要紧急程度" %in% names(proj)) as.character(proj[["重要紧急程度"]][proj_idx]) else NA_character_,
            task_name = st,
            task_type = "Process",
            start_date = sd,
            planned_end_date = pd,
            actual_end_date = ad,
            progress = pg,
            is_unplanned = unplanned,
            stage_ord = match(st, stage_order),
            remark = remark,
            proj_row_id = if (!is.na(proj_idx)) proj$id[proj_idx] else NA_integer_,
            site_row_id = if (nrow(site_row) > 0) site_row$id[1] else NA_integer_,
            manager_name = if (!is.na(proj_idx) && "manager_name" %in% names(proj)) proj$manager_name[proj_idx] else NA_character_
          )
        }
      }

      df <- bind_rows(all_rows) %>% arrange(project_id, site_name, stage_ord)
      df$raw_start_date <- df$start_date
      df$raw_planned_end_date <- df$planned_end_date
      # 未制定计划阶段的起点逻辑：
      # - 以当前日期 today 为基线；
      # - 如果任一前置阶段（同步/分中心、已完成/进行中/未制定计划）条形右端超过 today，
      #   则锚点移动到这些尾部的最大值；
      # - 当前未制定计划阶段从锚点起，默认持续 30 天；
      # 这样可保证同步阶段与分中心阶段之间的顺序连贯。
      cur_proj <- ""
      cur_site <- ""
      anchor <- today
      for (i in seq_len(nrow(df))) {
        if (df$project_id[i] != cur_proj || df$site_name[i] != cur_site) {
          cur_proj <- df$project_id[i]
          cur_site <- df$site_name[i]
          anchor <- today
        }

        # 对未制定计划的阶段，用锚点生成占位时间段
        if (isTRUE(df$is_unplanned[i])) {
          df$start_date[i] <- anchor
          df$planned_end_date[i] <- anchor + 30L
        }

        # 计算当前阶段的“尾部”：优先实际结束时间，其次计划结束时间，再其次开始时间
        tail_i <- NA
        if (!is.na(df$actual_end_date[i])) {
          tail_i <- df$actual_end_date[i]
        } else if (!is.na(df$planned_end_date[i])) {
          tail_i <- df$planned_end_date[i]
        } else if (!is.na(df$start_date[i])) {
          tail_i <- df$start_date[i]
        }

        # 锚点始终不早于 today，且跟随前置阶段的最远尾部
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

  # 当前甘特数据源（仅 DB）
  current_gantt_data <- reactive({
    gantt_data_db()
  })

  # 当前同步阶段列表
  sync_stages_current <- reactive({
    sync_stages_db
  })

  # ----- 甘特图 -----
  processed_data <- reactive({
    gd <- current_gantt_data()
    if (is.null(gd) || nrow(gd) == 0) return(list(items = data.frame(), groups = data.frame()))
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
        
        # 计算理论计划进度
        # 理论计划进度仅在任务实际完成的时候停止增长
        # 如果任务没完成就一直增长，可以超过100%
        # 如果任务提前完成就停在不到100%的地方
        planned_duration = as.numeric(planned_end_date - start_date),
        # 判断任务是否已完成：必须有 actual_end_date 才视为已完成
        is_completed = !is.na(actual_end_date),
        planned_progress = ifelse(
          is_completed,
          # 已完成：如果有actual_end_date，使用actual_end_date计算；否则使用planned_end_date计算
          ifelse(
            !is.na(actual_end_date),
            # 有actual_end_date：理论计划进度停在actual_end_date对应的计划进度
            ifelse(planned_duration > 0,
                   as.numeric(actual_end_date - start_date) / planned_duration,
                   1.0),
            # 没有actual_end_date但progress=1.0：使用planned_end_date计算（假设按计划完成）
            ifelse(planned_duration > 0, 1.0, 1.0)
          ),
          # 未完成：理论计划进度一直增长，可以超过100%
          ifelse(planned_duration > 0,
                 as.numeric(today - start_date) / planned_duration,
                 ifelse(today >= start_date, 1.0, 0.0))
        ),
        # 实际完成度：已完成任务为1.0，未完成任务为progress（0-1之间）
        actual_progress = ifelse(is_completed, 1.0, progress),
        # 颜色用数值差额计算：实际完成度 - 理论计划进度
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
        content = paste0(sub("^S\\d+_?", "", task_name), ifelse(is_unplanned, " (未制定计划)", ""), " ", ifelse(!is.na(actual_end_date), 100, round(progress * 100, 0)), "%")
      )
    
    # 处理同步阶段的任务（合并到同步阶段组，每个阶段只保留一条记录）
    items_sync <- gd %>%
      filter(task_name %in% ss) %>%
      group_by(project_id, task_name) %>%
      summarise(
        start_date = first(start_date),
        planned_end_date = first(planned_end_date),
        actual_end_date = first(actual_end_date),
        progress = first(progress),
        task_type = first(task_type),
        is_unplanned = first(is_unplanned),
        .groups = 'drop'
      ) %>%
      mutate(
        id = max(items_centers$id, 0) + row_number(),  # 确保ID不重复
        group = paste0(project_id, "_同步阶段"),
        
        # 计算理论计划进度（与items_centers相同的逻辑）
        planned_duration = as.numeric(planned_end_date - start_date),
        # 判断任务是否已完成：必须有 actual_end_date 才视为已完成
        is_completed = !is.na(actual_end_date),
        planned_progress = ifelse(
          is_completed,
          # 已完成：如果有actual_end_date，使用actual_end_date计算；否则使用planned_end_date计算
          ifelse(
            !is.na(actual_end_date),
            # 有actual_end_date：理论计划进度停在actual_end_date对应的计划进度
            ifelse(planned_duration > 0,
                   as.numeric(actual_end_date - start_date) / planned_duration,
                   1.0),
            # 没有actual_end_date但progress=1.0：使用planned_end_date计算（假设按计划完成）
            ifelse(planned_duration > 0, 1.0, 1.0)
          ),
          # 未完成：理论计划进度一直增长，可以超过100%
          ifelse(planned_duration > 0,
                 as.numeric(today - start_date) / planned_duration,
                 ifelse(today >= start_date, 1.0, 0.0))
        ),
        # 实际完成度：已完成任务为1.0，未完成任务为progress（0-1之间）
        actual_progress = ifelse(is_completed, 1.0, progress),
        # 颜色用数值差额计算：实际完成度 - 理论计划进度
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
        content = paste0(sub("^S\\d+_?", "", task_name), ifelse(is_unplanned, " (未制定计划)", ""), " ", ifelse(!is.na(actual_end_date), 100, round(progress * 100, 0)), "%")
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

    # -------- 自由里程碑：从里程碑 JSON 解析，并生成 milestone items --------
    milestone_items <- list()
    if (!is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      # 预先读取 03/04 表的里程碑列
      proj_ms <- tryCatch(
        DBI::dbGetQuery(pg_con, 'SELECT id, "里程碑" FROM public."04项目总表"'),
        error = function(e) data.frame()
      )
      site_ms <- tryCatch(
        DBI::dbGetQuery(pg_con, 'SELECT id, "里程碑" FROM public."03医院_项目表"'),
        error = function(e) data.frame()
      )
      # 用于快速查找行 id -> 里程碑 json
      proj_ms_map <- if (nrow(proj_ms) > 0) setNames(as.list(proj_ms[["里程碑"]]), as.character(proj_ms$id)) else list()
      site_ms_map <- if (nrow(site_ms) > 0) setNames(as.list(site_ms[["里程碑"]]), as.character(site_ms$id)) else list()

      # 先找出每个子进程的最后一个阶段，用来挂自定义里程碑占位点：
      # - 同步阶段子进程：仅在 sync_stages_db 内按阶段顺序取最后一个；
      # - 分中心子进程：仅在 site_stages_db 内按阶段顺序取最后一个。
      if (nrow(gd) > 0) {
        full_order <- c(
          "S01_需求与背景调研",
          "S02_方案设计审核",
          "S03_医院筛选与专家对接",
          "S04_医院立项资料输出与递交",
          "S05_伦理审批与启动会",
          "S06_人员与物资准备",
          "S07_试验开展与数据汇总表管理",
          "S08_小结输出与定稿",
          "S09_验证试验开展与数据管理",
          "S10_总报告输出与定稿",
          "S11_资料递交与结题归档",
          "S12_临床试验发补与资料递交",
          "S13_文章初稿输出",
          "S14_文章内部审评、修改、投递",
          "S15_意见反馈与文章返修"
        )

        # 同步子进程：每个 project_id 一条（[1] 保证每组 1 行，避免 dplyr summarise 警告）
        last_sync_stage <- gd %>%
          filter(task_name %in% sync_stages_db) %>%
          group_by(project_id) %>%
          summarise(
            last_task_name = task_name[which.max(match(task_name, full_order))][1],
            .groups = "drop"
          )

        # 分中心子进程：每个 project_id, site_name 一条
        last_site_stage <- gd %>%
          filter(task_name %in% site_stages_db) %>%
          group_by(project_id, site_name) %>%
          summarise(
            last_task_name = task_name[which.max(match(task_name, full_order))][1],
            .groups = "drop"
          )

        ms_id_start <- if (nrow(items_data) > 0) max(items_data$id, na.rm = TRUE) + 1L else 1L
        cur_ms_id <- ms_id_start

        # 标记哪些子进程（同步 / 分中心）已经至少有一个里程碑
        sync_with_ms <- character(0)
        site_with_ms <- character(0)
        # 为避免“各中心同步阶段”在多中心项目中重复渲染同一里程碑，
        # 记录已处理过的 (project_id, task_name) 组合，仅处理一次
        seen_sync_stage <- character(0)
        
        for (ri in seq_len(nrow(gd))) {
          row <- gd[ri, ]
          pid <- row$project_id
          sname <- row$site_name
          is_sync_row <- row$task_name %in% sync_stages_db
          
          if (is_sync_row) {
            sync_key <- paste(pid, as.character(row$task_name), sep = "||")
            if (sync_key %in% seen_sync_stage) next
            seen_sync_stage <- union(seen_sync_stage, sync_key)
          }
          proj_id <- row$proj_row_id
          site_id <- row$site_row_id
          group_id <- if (row$task_name %in% sync_stages_db) {
            paste0(pid, "_同步阶段")
          } else {
            paste0(pid, "_", sname)
          }

          key_row <- if (is_sync_row) {
            if (!is.na(proj_id)) as.character(proj_id) else NULL
          } else {
            if (!is.na(site_id)) as.character(site_id) else NULL
          }

          ms_json <- NULL
          if (!is.null(key_row)) {
            if (is_sync_row && key_row %in% names(proj_ms_map)) {
              ms_json <- proj_ms_map[[key_row]]
            } else if (!is_sync_row && key_row %in% names(site_ms_map)) {
              ms_json <- site_ms_map[[key_row]]
            }
          }

          # 解析里程碑 JSON，键：Sxx_阶段::名称，值：计划|实际|备注
          this_stage_has_ms <- FALSE
          if (!is.null(ms_json) && !is.na(ms_json)) {
            txt <- as.character(ms_json)
            if (nzchar(trimws(txt)) && !identical(txt, "无")) {
              parsed <- tryCatch(fromJSON(txt), error = function(e) NULL)
              # 兼容 json 列既可能解析成 list，也可能解析成带名字的原子向量的情况
              if (!is.null(parsed) && length(parsed) > 0 && length(names(parsed)) > 0) {
                for (nm in names(parsed)) {
                  # 只保留当前阶段对应的里程碑
                  parts <- strsplit(nm, "::", fixed = TRUE)[[1]]
                  if (length(parts) == 2L) {
                    stage_key <- parts[1]
                    ms_name <- parts[2]
                  } else {
                    stage_key <- row$task_name
                    ms_name <- nm
                  }
                  if (!identical(stage_key, as.character(row$task_name))) next
                  val <- as.character(parsed[[nm]])
                  pieces <- strsplit(val, "\\|", fixed = FALSE)[[1]]
                  while (length(pieces) < 3L) pieces <- c(pieces, "无")
                  plan_str <- ifelse(nzchar(pieces[1]), pieces[1], "无")
                  act_str <- ifelse(nzchar(pieces[2]), pieces[2], "无")
                  note_str <- ifelse(nzchar(pieces[3]), pieces[3], "无")

                  # 计划时间点
                  if (!identical(plan_str, "无")) {
                    this_stage_has_ms <- TRUE
                    milestone_items[[length(milestone_items) + 1]] <- data.frame(
                      id = cur_ms_id,
                      group = group_id,
                      start = plan_str,
                      end = NA,
                      content = paste0(ms_name, "（计划）"),
                      # 计划达成时间：蓝系
                      style = "color:#0D47A1; border-color:#1976D2; background-color:#BBDEFB;",
                      type = "point",
                      milestone_name = ms_name,
                      milestone_kind = "plan",
                      stringsAsFactors = FALSE
                    )
                    cur_ms_id <- cur_ms_id + 1L
                  }
                  # 实际时间点
                  if (!identical(act_str, "无")) {
                    this_stage_has_ms <- TRUE
                    milestone_items[[length(milestone_items) + 1]] <- data.frame(
                      id = cur_ms_id,
                      group = group_id,
                      start = act_str,
                      end = NA,
                      content = paste0(ms_name, "（实际）"),
                      # 实际达成时间：绿系
                      style = "color:#1B5E20; border-color:#388E3C; background-color:#C8E6C9;",
                      type = "point",
                      milestone_name = ms_name,
                      milestone_kind = "actual",
                      stringsAsFactors = FALSE
                    )
                    cur_ms_id <- cur_ms_id + 1L
                  }
                }
              }
            }
          }

          # 如本阶段所属的子进程已存在至少一个里程碑，则记录下来（用于后面抑制自定义里程碑占位点）
          if (this_stage_has_ms) {
            if (is_sync_row) {
              sync_with_ms <- union(sync_with_ms, as.character(pid))
            } else {
              site_with_ms <- union(site_with_ms, paste(pid, sname, sep = "||"))
            }
          }
        }

        # 自定义里程碑占位点：仅对没有任何里程碑的子进程，并放在各自子进程的最后一个阶段末尾
        # 1) 同步子进程
        if (nrow(last_sync_stage) > 0) {
          for (k in seq_len(nrow(last_sync_stage))) {
            lp <- last_sync_stage$project_id[k]
            last_task <- last_sync_stage$last_task_name[k]
            # 如果该同步子进程已经有任意里程碑，则不再添加占位点
            if (lp %in% sync_with_ms) next
            # 找到任意一个该项目下的该同步阶段行（site_name 随便取一个）
            idx <- which(gd$project_id == lp & gd$task_name == last_task & gd$task_name %in% sync_stages_db)[1]
            if (is.na(idx)) next

            grp <- paste0(lp, "_同步阶段")
            end_date <- gd$actual_end_date[idx]
            if (is.na(end_date)) end_date <- gd$planned_end_date[idx]
            if (is.na(end_date)) end_date <- today
            milestone_items[[length(milestone_items) + 1]] <- data.frame(
              id = cur_ms_id,
              group = grp,
              start = as.character(end_date),
              end = NA,
              content = "自定义里程碑",
              style = "color:#37474F; border-color:#78909C; background-color:#ECEFF1;",
              type = "point",
              milestone_name = NA_character_,
              milestone_kind = "placeholder",
              stringsAsFactors = FALSE
            )
            cur_ms_id <- cur_ms_id + 1L
          }
        }

        # 2) 分中心子进程
        if (nrow(last_site_stage) > 0) {
          for (k in seq_len(nrow(last_site_stage))) {
            lp <- last_site_stage$project_id[k]
            ls <- last_site_stage$site_name[k]
            last_task <- last_site_stage$last_task_name[k]
            key_pair <- paste(lp, ls, sep = "||")
            # 如果该分中心子进程已经有任意里程碑，则不再添加占位点
            if (key_pair %in% site_with_ms) next
            idx <- which(gd$project_id == lp & gd$site_name == ls & gd$task_name == last_task & gd$task_name %in% site_stages_db)[1]
            if (is.na(idx)) next

            grp <- paste0(lp, "_", ls)
            end_date <- gd$actual_end_date[idx]
            if (is.na(end_date)) end_date <- gd$planned_end_date[idx]
            if (is.na(end_date)) end_date <- today
            milestone_items[[length(milestone_items) + 1]] <- data.frame(
              id = cur_ms_id,
              group = grp,
              start = as.character(end_date),
              end = NA,
              content = "自定义里程碑",
              style = "color:#37474F; border-color:#78909C; background-color:#ECEFF1;",
              type = "point",
              milestone_name = NA_character_,
              milestone_kind = "placeholder",
              stringsAsFactors = FALSE
            )
            cur_ms_id <- cur_ms_id + 1L
          }
        }
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

      gd <- current_gantt_data()
      ss <- sync_stages_current()
      # 找到对应阶段行
      if (is_sync) {
        original_task <- gd %>%
          filter(project_id == project_id_from_group, task_name %in% ss) %>%
          slice(1)
      } else {
        original_task <- gd %>%
          filter(project_id == project_id_from_group, site_name == site_name_from_group, !task_name %in% ss) %>%
          slice(1)
      }
      if (nrow(original_task) == 0) return(NULL)

      proj_row_id <- if ("proj_row_id" %in% names(original_task)) original_task$proj_row_id[1] else NA_integer_
      site_row_id <- if ("site_row_id" %in% names(original_task)) original_task$site_row_id[1] else NA_integer_

      # 读取现有里程碑 JSON
      ms_json <- NULL
      if (!is.null(pg_con) && DBI::dbIsValid(pg_con)) {
        if (is_sync && !is.na(proj_row_id)) {
          tmp <- tryCatch(
            DBI::dbGetQuery(pg_con, 'SELECT "里程碑" FROM public."04项目总表" WHERE id = $1', params = list(as.integer(proj_row_id))),
            error = function(e) data.frame()
          )
          if (nrow(tmp) > 0) ms_json <- tmp[1, 1]
        } else if (!is_sync && !is.na(site_row_id)) {
          tmp <- tryCatch(
            DBI::dbGetQuery(pg_con, 'SELECT "里程碑" FROM public."03医院_项目表" WHERE id = $1', params = list(as.integer(site_row_id))),
            error = function(e) data.frame()
          )
          if (nrow(tmp) > 0) ms_json <- tmp[1, 1]
        }
      }

      # 解析为 data.frame(name, plan, actual, note)
      ms_df <- data.frame(name = character(0), plan = character(0), actual = character(0), note = character(0), stringsAsFactors = FALSE)
      stage_key <- as.character(original_task$task_name[1])
      if (!is.null(ms_json) && !is.na(ms_json)) {
        txt <- as.character(ms_json)
        if (nzchar(trimws(txt)) && !identical(txt, "无")) {
          parsed <- tryCatch(fromJSON(txt), error = function(e) NULL)
          if (is.list(parsed) && length(parsed) > 0) {
            for (nm in names(parsed)) {
              parts <- strsplit(nm, "::", fixed = TRUE)[[1]]
              if (length(parts) == 2L) {
                sk <- parts[1]
                nm2 <- parts[2]
              } else {
                sk <- stage_key
                nm2 <- nm
              }
              if (!identical(sk, stage_key)) next
              val <- as.character(parsed[[nm]])
              pieces <- strsplit(val, "\\|", fixed = FALSE)[[1]]
              while (length(pieces) < 3L) pieces <- c(pieces, "无")
              ms_df[nrow(ms_df) + 1, ] <- list(
                name = ifelse(pieces[1] == "无", "", nm2),
                plan = ifelse(pieces[1] == "无", "", pieces[1]),
                actual = ifelse(pieces[2] == "无", "", pieces[2]),
                note = ifelse(pieces[3] == "无", "", pieces[3])
              )
            }
          }
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
        table_name = if (is_sync) "04项目总表" else "03医院_项目表",
        milestones = ms_df
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
    remark <- if ("remark" %in% names(original_task)) as.character(original_task$remark[1]) else NA_character_
    if (is.na(remark) || identical(remark, "NA") || nchar(trimws(remark)) == 0) remark <- ""
    proj_row_id <- if ("proj_row_id" %in% names(original_task)) original_task$proj_row_id[1] else NA_integer_
    site_row_id <- if ("site_row_id" %in% names(original_task)) original_task$site_row_id[1] else NA_integer_
    # 提前计算 actual_progress 供 task_edit_context 使用
    progress_match <- regmatches(task_info$content, regexpr("\\d+%", task_info$content))
    reported_progress <- if (length(progress_match) > 0) as.numeric(sub("%", "", progress_match)) / 100 else original_task$progress
    is_completed <- !is.na(actual_end_date)
    actual_progress <- ifelse(is_completed, 1.0, reported_progress)
    
    raw_start <- if ("raw_start_date" %in% names(original_task)) original_task$raw_start_date[1] else (if (is_unplanned) as.Date(NA) else start_for_calc)
    raw_planned <- if ("raw_planned_end_date" %in% names(original_task)) original_task$raw_planned_end_date[1] else (if (is_unplanned) as.Date(NA) else planned_end_date)
    # 是否为“验证试验开展与数据管理”同步阶段（S09，项目级）
    is_s09_task <- identical(as.character(task_info$task_name[1]), "S09_验证试验开展与数据管理") && is_sync
    # 读取样本来源与数量（存储在 04项目总表 的 JSON 列）
    sample_pairs <- NULL
    if (is_s09_task && !is.na(proj_row_id) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      sample_raw <- tryCatch(
        DBI::dbGetQuery(
          pg_con,
          'SELECT "S09_Sample_验证试验开展与数据管理_样本来源与数" AS sample_json FROM public."04项目总表" WHERE id = $1',
          params = list(as.integer(proj_row_id))
        ),
        error = function(e) NULL
      )
      if (!is.null(sample_raw) && nrow(sample_raw) > 0) {
        sj <- sample_raw$sample_json[1]
        if (!is.null(sj) && !is.na(sj)) {
          sj_char <- as.character(sj)
          if (nzchar(trimws(sj_char))) {
            parsed <- tryCatch(fromJSON(sj_char), error = function(e) NULL)
            if (is.data.frame(parsed) && all(c("hospital", "count") %in% names(parsed))) {
              sample_pairs <- parsed
            }
          }
        }
      }
    }
    # 读取进度贡献者 JSON 并解析为 data.frame(person, role, work, amount)
    contrib_df <- NULL
    contrib_col <- if (is_sync) stage_contrib_04[[as.character(task_info$task_name[1])]] else stage_contrib_03[[as.character(task_info$task_name[1])]]
    if (!is.null(contrib_col) && !is.na(contrib_col) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      tbl_c <- if (is_sync) "04项目总表" else "03医院_项目表"
      row_id_c <- if (is_sync) proj_row_id else site_row_id
      if (!is.na(row_id_c)) {
        q_c <- sprintf('SELECT "%s" AS contrib FROM public."%s" WHERE id = $1', contrib_col, tbl_c)
        tmp_c <- tryCatch(
          DBI::dbGetQuery(pg_con, q_c, params = list(as.integer(row_id_c))),
          error = function(e) data.frame()
        )
        if (nrow(tmp_c) > 0) {
          cj <- tmp_c$contrib[1]
          if (!is.null(cj) && !is.na(cj)) {
            txt_c <- as.character(cj)
            if (nzchar(trimws(txt_c)) && !identical(txt_c, "null")) {
              parsed_c <- tryCatch(fromJSON(txt_c), error = function(e) NULL)
              if (is.list(parsed_c) && length(parsed_c) > 0) {
                rows <- lapply(names(parsed_c), function(nm) {
                  val <- as.character(parsed_c[[nm]])
                  parts <- strsplit(val, "|", fixed = TRUE)[[1]]
                  if (suppressWarnings(!is.na(as.integer(nm)))) {
                    while (length(parts) < 4L) parts <- c(parts, "")
                    data.frame(
                      person = parts[1], role = parts[2],
                      work = parts[3], amount = parts[4],
                      stringsAsFactors = FALSE
                    )
                  } else {
                    while (length(parts) < 2L) parts <- c(parts, "")
                    data.frame(
                      person = nm, role = parts[1],
                      work = if (length(parts) >= 2) parts[2] else "",
                      amount = if (length(parts) >= 3) parts[3] else "",
                      stringsAsFactors = FALSE
                    )
                  }
                })
                contrib_df <- do.call(rbind, rows)
              }
            }
          }
        }
      }
    }

    can_edit <- !is.null(pg_con) && DBI::dbIsValid(pg_con) &&
      (is_sync && !is.na(proj_row_id) || (!is_sync && !is.na(site_row_id)))
    if (can_edit) {
      col_map <- if (is_sync) stage_col_map_04 else stage_col_map_03
      tn <- as.character(task_info$task_name[1])
      cm <- col_map[[tn]]
      if (is_s09_task) {
        # 初始化样本编辑行数
        sample_row_count(if (is.null(sample_pairs)) 0L else nrow(sample_pairs))
      } else {
        sample_row_count(0L)
      }
      contrib_row_count(if (is.null(contrib_df)) 0L else nrow(contrib_df))
      task_edit_context(list(
        project_id = task_info$project_id[1],
        site_name = task_info$site_name[1],
        task_name = tn,
        is_sync = is_sync,
        project_type = if ("project_type" %in% names(original_task)) original_task$project_type[1] else NA_character_,
        importance = if ("重要紧急程度" %in% names(original_task)) as.character(original_task[["重要紧急程度"]][1]) else NA_character_,
        start_date = raw_start,
        planned_end_date = raw_planned,
        actual_end_date = actual_end_date,
        progress = actual_progress,
        remark = remark,
        proj_row_id = proj_row_id,
        site_row_id = site_row_id,
        col_map = cm,
        table_name = if (is_sync) "04项目总表" else "03医院_项目表",
        samples = sample_pairs,
        contributors = contrib_df
      ))
    } else {
      task_edit_context(NULL)
    }
    
    # 计算计划总时长：严格按照计划开始/结束日期
    planned_duration <- as.numeric(planned_end_date - start_for_calc)
    
    # 计算理论计划进度：
    # - 已完成：理论计划进度停在 actual_end_date 对应的计划进度（可早可晚）
    # - 未完成：按 today 相对于计划结束时间的比例来计算，可以超过 100%
    planned_p <- ifelse(
      is_completed,
      ifelse(planned_duration > 0,
             as.numeric(actual_end_date - start_for_calc) / planned_duration,
             1.0),
      ifelse(planned_duration > 0,
             as.numeric(today - start_for_calc) / planned_duration,
             ifelse(today >= start_for_calc, 1.0, 0.0))
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
          parts <- tryCatch(
            DBI::dbGetQuery(pg_con,
              'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."X05人员表_id" = p.id WHERE m."X04项目总表_id" = $1',
              params = list(as.integer(proj_id_db))),
            error = function(e) {
              DBI::dbGetQuery(pg_con,
                'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."05人员表_id" = p.id WHERE m."04项目总表_id" = $1',
                params = list(as.integer(proj_id_db)))
            }
          )
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
    
    showModal(modalDialog(
      title = "任务详细进度核查",
      size = "l",
      easyClose = TRUE,
      footer = tagList(
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
               p(tags$b("阶段："), sub("^S\\d+_?", "", task_info$task_name)),
               p(tags$b("项目紧急程度："), tags$span(importance_display, style = sprintf("color: %s; font-weight: bold;", importance_color))),
               p(tags$b("项目负责人："), manager_display),
               p(tags$b("项目参与人员名单："),
                 if (length(participants_display) > 0)
                   tags$div(style = "white-space: pre-wrap; margin-top: 4px;", paste(participants_display, collapse = "\n"))
                 else tags$span("（无）", style = "color: #999;")),
               p(tags$b("开始日期："), if (length(raw_start) > 0 && !is.na(raw_start)) as.character(raw_start) else "无"),
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
               p(if (length(remark) > 0 && !is.na(remark) && nchar(trimws(as.character(remark))) > 0)
                 tags$div(style = "white-space: pre-wrap; background: #f5f5f5; padding: 8px; border-radius: 4px;", remark)
                 else tags$span("（无）", style = "color: #999;")),
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
  
  observeEvent(input$btn_edit_task, {
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    removeModal()
    sd <- ctx$start_date
    pd <- ctx$planned_end_date
    ad <- ctx$actual_end_date
    # 计算上阶段信息（根据项目类型过滤阶段顺序：S01-S15）
    stage_order_full <- c(
      "S01_需求与背景调研",
      "S02_方案设计审核",
      "S03_医院筛选与专家对接",
      "S04_医院立项资料输出与递交",
      "S05_伦理审批与启动会",
      "S06_人员与物资准备",
      "S07_试验开展与数据汇总表管理",
      "S08_小结输出与定稿",
      "S09_验证试验开展与数据管理",
      "S10_总报告输出与定稿",
      "S11_资料递交与结题归档",
      "S12_临床试验发补与资料递交",
      "S13_文章初稿输出",
      "S14_文章内部审评、修改、投递",
      "S15_意见反馈与文章返修"
    )
    proj_type <- ctx$project_type
    valid_short <- if (!is.na(proj_type) && proj_type %in% names(project_type_valid_stages))
      project_type_valid_stages[[proj_type]] else unname(task_short_name)
    stages_for_proj <- stage_order_full[task_short_name[stage_order_full] %in% valid_short]
    cur_idx <- which(stages_for_proj == ctx$task_name)[1]
    prev_name <- prev_plan <- prev_act <- "（无）"
    if (!is.na(cur_idx) && cur_idx > 1L) {
      prev_stage <- stages_for_proj[cur_idx - 1L]
      prev_name <- task_short_name[prev_stage]
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
    edit_title <- sprintf("%s-%s-%s-修改/更新数据",
      if (!is.na(ctx$project_type) && nzchar(ctx$project_type)) as.character(ctx$project_type) else "未知",
      ctx$project_id,
      if (ctx$is_sync) "各中心同步阶段" else ctx$site_name
    )
    # 对于 S09 验证试验开展与数据管理，同步阶段，初始化样本编辑行数
    if (identical(ctx$task_name, "S09_验证试验开展与数据管理") && ctx$is_sync) {
      n_init <- if (!is.null(ctx$samples)) nrow(ctx$samples) else 0L
      sample_row_count(n_init)
    } else {
      sample_row_count(0L)
    }
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
                 p(tags$b("实际结束日期："), prev_act)),
               p(tags$span(sub("^S\\d+_?", "", ctx$task_name), style = "color: #1976D2; font-weight: bold;")),
               textInput("edit_start_date", "开始日期：", value = if (length(sd) > 0 && !is.na(sd)) format(sd, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未制定计划"),
               textInput("edit_planned_date", "计划结束日期：", value = if (length(pd) > 0 && !is.na(pd)) format(pd, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未制定计划"),
               textInput("edit_actual_date", "实际结束日期：", value = if (length(ad) > 0 && !is.na(ad)) format(ad, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未完成"),
               tags$p(tags$small("（日期格式：YYYY-MM-DD；留空表示未制定计划/未完成）")),
               sliderInput("edit_progress", "调整当前实际进度：", 0, 100, round((if (is.null(ctx$progress)) 0 else ctx$progress) * 100), post = "%")
        ),
        column(6,
               selectInput("edit_importance", "项目紧急程度：",
                 choices = c("重要紧急", "重要不紧急", "紧急不重要", "不重要不紧急"),
                 selected = {
                   cur <- if (is.null(ctx$importance) || is.na(ctx$importance)) "" else trimws(ctx$importance)
                   if (cur %in% c("重要紧急", "重要不紧急", "紧急不重要", "不重要不紧急")) cur else "重要紧急"
                 }),
               textAreaInput("edit_remark", "问题、卡点反馈与经验分享：", value = if (is.null(ctx$remark)) "" else as.character(ctx$remark), rows = 6),
               uiOutput("sample_pairs_editor")
        )
      )
    ))
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
    short_name <- task_short_name[[ctx$task_name]]
    work_choices <- switch(
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
        tags$b("阶段："), sub("^S\\d+_?", "", ctx$task_name)
      ),
      tags$hr(),
      tags$b("进度贡献者"),
      tags$p(tags$small("每行：人员（从人员表选择） + 参与度（主导/参与/协助，可自定义） + 工作内容（阶段相关，可自定义） + 数量（>=1 的数字）。")),
      tags$div(style = "margin-top:10px;", rows_ui),
      actionButton("btn_add_contrib_row", "新增贡献者", class = "btn-success"),
      tags$p(tags$small("不允许在任一字段中输入“|”，以免破坏存储结构。留空人员的行在保存时会被忽略。"))
    )
  })

  observeEvent(input$btn_edit_contrib, {
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
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
  })

  observeEvent(input$btn_add_contrib_row, {
    ctx <- task_edit_context()
    req(ctx)
    n_cur <- contrib_row_count()
    if (is.null(n_cur) || n_cur < 0L) n_cur <- 0L

    # 同步现有输入到 ctx$contributors
    persons <- roles <- works <- amounts <- character(0)
    if (n_cur > 0L) {
      for (i in seq_len(n_cur)) {
        persons <- c(persons, tryCatch(trimws(as.character(input[[paste0("contrib_person_", i)]])), error = function(e) ""))
        roles   <- c(roles,   tryCatch(trimws(as.character(input[[paste0("contrib_role_", i)]])), error = function(e) ""))
        works   <- c(works,   tryCatch(trimws(as.character(input[[paste0("contrib_work_", i)]])), error = function(e) ""))
        raw_num <- tryCatch(input[[paste0("contrib_amount_", i)]], error = function(e) NA_real_)
        amt_num <- suppressWarnings(as.numeric(raw_num))
        if (is.na(amt_num) || amt_num < 1) amt_num <- 1
        amounts <- c(amounts, as.character(amt_num))
      }
    }
    persons <- c(persons, "")
    roles   <- c(roles, "")
    works   <- c(works, "")
    amounts <- c(amounts, "1")

    ctx$contributors <- data.frame(
      person = persons,
      role = roles,
      work = works,
      amount = amounts,
      stringsAsFactors = FALSE
    )
    task_edit_context(ctx)
    contrib_row_count(n_cur + 1L)
  })

  observeEvent(input$btn_save_contrib, {
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    n_rows <- contrib_row_count()
    if (is.null(n_rows) || n_rows < 0L) n_rows <- 0L

    persons <- roles <- works <- amounts <- character(0)
    # 校验人员名称是否存在于人员表
    person_choices <- character(0)
    if (!is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      dfp <- tryCatch(
        DBI::dbGetQuery(pg_con, 'SELECT "姓名" FROM public."05人员表" WHERE "姓名" IS NOT NULL'),
        error = function(e) data.frame()
      )
      if ("姓名" %in% names(dfp)) person_choices <- unique(na.omit(dfp[["姓名"]]))
    }
    if (n_rows > 0L) {
      for (i in seq_len(n_rows)) {
        p <- tryCatch(trimws(as.character(input[[paste0("contrib_person_", i)]])), error = function(e) "")
        r <- tryCatch(trimws(as.character(input[[paste0("contrib_role_", i)]])), error = function(e) "")
        w <- tryCatch(trimws(as.character(input[[paste0("contrib_work_", i)]])), error = function(e) "")
        a <- ""
        # 拦截 “|”
        if (any(grepl("\\|", c(p, r, w), fixed = TRUE))) {
          showNotification("任意字段中不允许包含“|”字符，请修改后再保存。", type = "error")
          return()
        }
        if (nzchar(p) && length(person_choices) > 0 && !(p %in% person_choices)) {
          showNotification(sprintf("不存在此人名：%s", p), type = "error")
          return()
        }
        if (!nzchar(p)) next
        raw_num <- tryCatch(input[[paste0("contrib_amount_", i)]], error = function(e) NA_real_)
        amt_num <- suppressWarnings(as.numeric(raw_num))
        if (is.na(amt_num) || amt_num < 1) {
          showNotification("数量必须为不小于 1 的数字。", type = "error")
          return()
        }
        a <- as.character(amt_num)
        persons <- c(persons, p)
        roles   <- c(roles, r)
        works   <- c(works, w)
        amounts <- c(amounts, a)
      }
    }

    contrib_list <- list()
    if (length(persons) > 0) {
      for (i in seq_along(persons)) {
        contrib_list[[as.character(i)]] <- paste(c(persons[i], roles[i], works[i], amounts[i]), collapse = "|")
      }
    }
    contrib_json <- if (length(contrib_list) > 0) toJSON(contrib_list, auto_unbox = TRUE) else NA_character_

    # 确定列名与表
    tn <- ctx$task_name
    contrib_col <- if (ctx$is_sync) stage_contrib_04[[tn]] else stage_contrib_03[[tn]]
    if (is.null(contrib_col) || is.na(contrib_col)) {
      showNotification("当前阶段未配置进度贡献者列，无法保存。", type = "error")
      return()
    }
    tbl <- ctx$table_name
    row_id <- if (ctx$is_sync) ctx$proj_row_id else ctx$site_row_id
    if (is.na(row_id)) {
      showNotification("无法确定数据库行，保存失败。", type = "error")
      return()
    }

    # 审计：读取旧值
    old_contrib_raw <- tryCatch({
      q_old <- sprintf('SELECT "%s" FROM public."%s" WHERE id = $1', contrib_col, tbl)
      r <- DBI::dbGetQuery(pg_con, q_old, params = list(as.integer(row_id)))
      if (nrow(r) > 0 && contrib_col %in% names(r)) r[[contrib_col]][1] else NA_character_
    }, error = function(e) NA_character_)
    old_contrib_list <- if (is.na(old_contrib_raw) || !nzchar(trimws(old_contrib_raw))) list() else tryCatch(jsonlite::fromJSON(old_contrib_raw), error = function(e) list())

    removeModal()
    tryCatch({
      q <- sprintf('UPDATE public."%s" SET "%s" = $1 WHERE id = $2', tbl, contrib_col)
      DBI::dbExecute(pg_con, q, params = list(contrib_json, as.integer(row_id)))
      auth <- current_user_auth()
      insert_audit_log(pg_con,
        work_id = auth$work_id, name = auth$name,
        op_type = "update_contrib", target_table = tbl, target_row_id = row_id,
        biz_desc = "进度贡献者", summary = "修改进度贡献者",
        old_val = list(进度贡献者 = old_contrib_list), new_val = list(进度贡献者 = contrib_list),
        remark = NULL
      )
      # 更新上下文
      if (length(persons) > 0) {
        ctx$contributors <- data.frame(
          person = persons,
          role = roles,
          work = works,
          amount = amounts,
          stringsAsFactors = FALSE
        )
      } else {
        ctx$contributors <- NULL
      }
      task_edit_context(ctx)
      contrib_row_count(if (is.null(ctx$contributors)) 0L else nrow(ctx$contributors))
      showNotification("进度贡献者信息已保存。", type = "message")
    }, error = function(e) {
      showNotification(paste0("保存进度贡献者失败：", conditionMessage(e)), type = "error")
    })
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
             " / ", sub("^S\\d+_?", "", ctx$task_name)),
      tags$hr(),
      tags$div(rows_ui),
      actionButton("btn_add_milestone_row", "新增里程碑", class = "btn-success"),
      tags$p(tags$small("名称为空则删除该里程碑；时间留空会保存为“无”。"))
    )
  })

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
        if (i <= nrow(ms_df)) {
          ms_df$name[i] <- nm
          ms_df$plan[i] <- pl
          ms_df$actual[i] <- ac
          ms_df$note[i] <- nt
        } else {
          ms_df[i, ] <- list(name = nm, plan = pl, actual = ac, note = nt)
        }
      }
    }
    # 新增一空行
    ms_df[nrow(ms_df) + 1, ] <- list(name = "", plan = "", actual = "", note = "")
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

    hospitals <- character(0)
    values <- character(0)
    stage_key <- ctx$task_name

    if (n_rows > 0L) {
      for (i in seq_len(n_rows)) {
        nm <- tryCatch(trimws(as.character(input[[paste0("ms_name_", i)]])), error = function(e) "")
        pl <- tryCatch(trimws(as.character(input[[paste0("ms_plan_", i)]])), error = function(e) "")
        ac <- tryCatch(trimws(as.character(input[[paste0("ms_actual_", i)]])), error = function(e) "")
        nt <- tryCatch(trimws(as.character(input[[paste0("ms_note_", i)]])), error = function(e) "")
        if (!nzchar(nm)) next
        # 校验日期格式：如果用户填写了日期，必须能被 as.Date 正常解析
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
        pl2 <- ifelse(nzchar(pl), pl, "无")
        ac2 <- ifelse(nzchar(ac), ac, "无")
        nt2 <- ifelse(nzchar(nt), nt, "无")
        key <- paste0(stage_key, "::", nm)
        hospitals <- c(hospitals, key)
        values <- c(values, paste(pl2, ac2, nt2, sep = "|"))
      }
    }

    ms_json <- NULL
    if (length(hospitals) > 0) {
      ms_list <- as.list(values)
      names(ms_list) <- hospitals
      ms_json <- toJSON(ms_list, auto_unbox = TRUE)
    } else {
      ms_json <- NA_character_
    }

    tbl <- ctx$table_name
    row_id <- if (ctx$is_sync) ctx$proj_row_id else ctx$site_row_id
    if (is.na(row_id)) {
      removeModal()
      return()
    }
    # 审计：读取旧值
    old_ms_raw <- tryCatch({
      r <- DBI::dbGetQuery(pg_con, sprintf('SELECT "里程碑" FROM public."%s" WHERE id = $1', tbl), params = list(row_id))
      if (nrow(r) > 0 && "里程碑" %in% names(r)) r[["里程碑"]][1] else NA_character_
    }, error = function(e) NA_character_)
    old_ms_val <- if (is.na(old_ms_raw) || !nzchar(trimws(as.character(old_ms_raw)))) list() else tryCatch(jsonlite::fromJSON(old_ms_raw), error = function(e) list())
    new_ms_val <- if (is.na(ms_json) || !nzchar(trimws(ms_json))) list() else tryCatch(jsonlite::fromJSON(ms_json), error = function(e) list())
    # 写回里程碑列
    tryCatch({
      q <- sprintf('UPDATE public."%s" SET "里程碑" = $1 WHERE id = $2', tbl)
      DBI::dbExecute(pg_con, q, params = list(ms_json, row_id))
      auth <- current_user_auth()
      insert_audit_log(pg_con,
        work_id = auth$work_id, name = auth$name,
        op_type = "update_milestone", target_table = tbl, target_row_id = row_id,
        biz_desc = "里程碑", summary = "修改自由里程碑",
        old_val = list(里程碑 = old_ms_val), new_val = list(里程碑 = new_ms_val),
        remark = NULL
      )
      gantt_force_refresh(gantt_force_refresh() + 1L)
      showNotification("里程碑已保存。", type = "message")
    }, error = function(e) {
      showNotification(paste0("里程碑保存失败：", conditionMessage(e)), type = "error")
    })
    removeModal()
  })

  observeEvent(input$btn_open_milestone_edit, {
    ctx <- task_edit_context()
    req(ctx)
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
    hospitals <- character(0)
    counts <- numeric(0)
    if (n_cur > 0L) {
      for (i in seq_len(n_cur)) {
        h_input <- input[[paste0("sample_hospital_", i)]]
        c_input <- input[[paste0("sample_count_", i)]]
        h_val <- tryCatch(trimws(as.character(h_input)), error = function(e) "")
        c_num <- suppressWarnings(as.numeric(c_input))
        if (is.na(c_num)) c_num <- 0
        # 这里暂不过滤“无”，保存时再统一处理
        hospitals <- c(hospitals, h_val)
        counts <- c(counts, c_num)
      }
    }
    # 新增一行，默认医院为“无”，数量为 0
    hospitals <- c(hospitals, "无")
    counts <- c(counts, 0)

    df_samples <- data.frame(
      hospital = hospitals,
      count = counts,
      stringsAsFactors = FALSE
    )

    # 更新上下文中的样本数据和行数
    ctx$samples <- df_samples
    task_edit_context(ctx)
    sample_row_count(nrow(df_samples))
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
    sd_res <- validate_date_input(input$edit_start_date)
    pd_res <- validate_date_input(input$edit_planned_date)
    ad_res <- validate_date_input(input$edit_actual_date)
    if (!sd_res$ok || !pd_res$ok || !ad_res$ok) {
      showNotification("日期格式错误或不存在", type = "error")
      return()
    }
    sd_val <- sd_res$value
    pd_val <- pd_res$value
    ad_val <- ad_res$value
    # 业务校验：对于普通阶段，如果计划结束日期早于开始日期，则阻止写入
    # 仅当两者都不为空时才做比较
    if (!is.na(sd_val) && !is.na(pd_val) && pd_val < sd_val) {
      showNotification("计划结束日期不能早于开始日期，请检查后重新填写。", type = "error")
      return()
    }
    # 进度：DB 中 S01-S03 为 integer 0-100，S04-S12 为 double；统一存 0-100 以便兼容
    progress_val <- if (is.null(input$edit_progress)) 0 else as.numeric(input$edit_progress)
    remark_val <- if (is.null(input$edit_remark)) "" else as.character(input$edit_remark)
    importance_val <- if (is.null(input$edit_importance) || !nzchar(trimws(as.character(input$edit_importance)))) NA_character_ else trimws(as.character(input$edit_importance))
    # 针对 S09“验证试验开展与数据管理”的样本来源与数量特殊处理
    is_s09_task <- identical(ctx$task_name, "S09_验证试验开展与数据管理") && ctx$is_sync
    sample_json <- NULL
    if (is_s09_task) {
      n_rows <- sample_row_count()
      if (is.null(n_rows) || n_rows < 0L) n_rows <- 0L
      hospitals <- character(0)
      counts <- numeric(0)
      if (n_rows > 0L) {
        for (i in seq_len(n_rows)) {
          h_input <- input[[paste0("sample_hospital_", i)]]
          c_input <- input[[paste0("sample_count_", i)]]
          h_val <- tryCatch(trimws(as.character(h_input)), error = function(e) "")
          # 选中“无”或空字符串，视为删除该对子
          if (!nzchar(h_val) || identical(h_val, "无")) {
            next
          }
          c_num <- suppressWarnings(as.numeric(c_input))
          if (is.na(c_num)) c_num <- 0
          hospitals <- c(hospitals, h_val)
          counts <- c(counts, c_num)
        }
      }
      if (length(hospitals) > 0) {
        df_samples <- data.frame(
          hospital = hospitals,
          count = counts,
          stringsAsFactors = FALSE
        )
        sample_json <- toJSON(df_samples, auto_unbox = TRUE)
      } else {
        sample_json <- NA_character_
      }
    }
    tbl <- ctx$table_name
    cm <- ctx$col_map
    row_id <- if (ctx$is_sync) ctx$proj_row_id else ctx$site_row_id
    if (is.na(row_id)) return()
    progress_num <- suppressWarnings(as.numeric(progress_val))
    if (is.na(progress_num)) progress_num <- 0
    progress_num <- pmax(0, pmin(100, progress_num))
    # 审计：读取当前行旧值（阶段时间、进度、重要紧急程度、S09 样本）
    cols_old <- c(cm$start, cm$plan, cm$act, cm$note, cm$progress)
    if (ctx$is_sync) cols_old <- c(cols_old, "重要紧急程度")
    if (is_s09_task) cols_old <- c(cols_old, "S09_Sample_验证试验开展与数据管理_样本来源与数")
    old_row <- tryCatch({
      sel <- paste0('"', cols_old, '"', collapse = ", ")
      q_old <- sprintf('SELECT %s FROM public."%s" WHERE id = $1', sel, tbl)
      r <- DBI::dbGetQuery(pg_con, q_old, params = list(row_id))
      if (nrow(r) == 0) NULL else as.list(r[1, , drop = FALSE])
    }, error = function(e) NULL)
    new_row <- setNames(list(sd_val, pd_val, ad_val, remark_val, progress_num), c(cm$start, cm$plan, cm$act, cm$note, cm$progress))
    if (ctx$is_sync) new_row[["重要紧急程度"]] <- importance_val
    if (is_s09_task) new_row[["S09_Sample_验证试验开展与数据管理_样本来源与数"]] <- sample_json
    # 旧值/新值用简短键供审计查看
    audit_key_names <- c("开始时间", "计划完成时间", "实际完成时间", "备注", "当前进度")
    audit_cols <- c(cm$start, cm$plan, cm$act, cm$note, cm$progress)
    if (ctx$is_sync) { audit_key_names <- c(audit_key_names, "重要紧急程度"); audit_cols <- c(audit_cols, "重要紧急程度") }
    if (is_s09_task) { audit_key_names <- c(audit_key_names, "样本来源与数"); audit_cols <- c(audit_cols, "S09_Sample_验证试验开展与数据管理_样本来源与数") }
    old_audit <- if (is.null(old_row)) setNames(vector("list", length(audit_key_names)), audit_key_names) else setNames(lapply(audit_cols, function(c) old_row[[c]]), audit_key_names)
    new_audit <- setNames(lapply(audit_cols, function(c) new_row[[c]]), audit_key_names)
    removeModal()
    tryCatch({
      if (ctx$is_sync) {
        # 项目级（04项目总表）需同时更新 重要紧急程度
        if (is_s09_task) {
          q1 <- sprintf(
            'UPDATE public."%s" SET "%s" = $1, "%s" = $2, "%s" = $3, "%s" = $4, "%s" = $5, "重要紧急程度" = $6 WHERE id = $7',
            tbl, cm$start, cm$plan, cm$act, cm$note, "S09_Sample_验证试验开展与数据管理_样本来源与数"
          )
          DBI::dbExecute(pg_con, q1, params = list(sd_val, pd_val, ad_val, remark_val, sample_json, importance_val, row_id))
        } else {
          q1 <- sprintf(
            'UPDATE public."%s" SET "%s" = $1, "%s" = $2, "%s" = $3, "%s" = $4, "重要紧急程度" = $5 WHERE id = $6',
            tbl, cm$start, cm$plan, cm$act, cm$note
          )
          DBI::dbExecute(pg_con, q1, params = list(sd_val, pd_val, ad_val, remark_val, importance_val, row_id))
        }
      } else {
        if (is_s09_task) {
          q1 <- sprintf(
            'UPDATE public."%s" SET "%s" = $1, "%s" = $2, "%s" = $3, "%s" = $4, "%s" = $5 WHERE id = $6',
            tbl, cm$start, cm$plan, cm$act, cm$note, "S09_Sample_验证试验开展与数据管理_样本来源与数"
          )
          DBI::dbExecute(pg_con, q1, params = list(sd_val, pd_val, ad_val, remark_val, sample_json, row_id))
        } else {
          q1 <- sprintf(
            'UPDATE public."%s" SET "%s" = $1, "%s" = $2, "%s" = $3, "%s" = $4 WHERE id = $5',
            tbl, cm$start, cm$plan, cm$act, cm$note
          )
          DBI::dbExecute(pg_con, q1, params = list(sd_val, pd_val, ad_val, remark_val, row_id))
        }
      }
      q2 <- sprintf(
        'UPDATE public."%s" SET "%s" = $1 WHERE id = $2',
        tbl, cm$progress
      )
      DBI::dbExecute(pg_con, q2, params = list(progress_num, row_id))
      # 分中心任务：重要紧急程度在 04项目总表，需单独更新
      if (!ctx$is_sync && !is.na(ctx$proj_row_id)) {
        DBI::dbExecute(pg_con, 'UPDATE public."04项目总表" SET "重要紧急程度" = $1 WHERE id = $2',
          params = list(importance_val, ctx$proj_row_id))
      }
      auth <- current_user_auth()
      insert_audit_log(pg_con,
        work_id = auth$work_id, name = auth$name,
        op_type = "update_stage", target_table = tbl, target_row_id = row_id,
        biz_desc = "阶段时间与进度", summary = "修改阶段时间、进度与重要紧急程度",
        old_val = old_audit, new_val = new_audit, remark = NULL
      )
      gantt_force_refresh(gantt_force_refresh() + 1L)
      # 立即触发 gantt_data_db 重新执行，确保下次打开弹窗时读到最新数据
      tryCatch({ invisible(gantt_data_db()) }, error = function(e) NULL)
      showNotification("保存成功，已重新加载数据。", type = "message")
    }, error = function(e) {
      showNotification(paste0("保存失败：", conditionMessage(e)), type = "error")
    })
    task_edit_context(NULL)
  })
}

shinyApp(ui = ui, server = server)