# ==========================================
# 数据库信息维护应用 (db_maintain.R)
# 以医院为中心的单页设计（纯 renderUI 版）
#   选择医院 → 展示/编辑医院信息 + 关联仪器 + 关联样本
# 所有操作写入 07操作审计表（逐字段审计）
# ==========================================

library(shiny)
library(dplyr)
library(RPostgres)
library(DBI)
library(jsonlite)
library(pool)

# ==================== 0. 数据库连接 ====================

db_config <- list(
  host = Sys.getenv("PG_HOST", "localhost"),
  port = as.integer(Sys.getenv("PG_PORT", "5432")),
  dbname = Sys.getenv("PG_DBNAME", "ivd_data"),
  user = Sys.getenv("PG_USER", "myuser"),
  password = Sys.getenv("PG_PASSWORD", "mypassword")
)

pg_pool <- dbPool(
  drv = RPostgres::Postgres(),
  host = db_config$host,
  port = db_config$port,
  dbname = db_config$dbname,
  user = db_config$user,
  password = db_config$password,
  maxSize = 10
)

onStop(function() { poolClose(pg_pool) })

# ==================== 1. 常量 ====================

ALLOWED_LEVELS <- c("common_user", "manager", "group_manager", "super_admin")

HOSP_EDITABLE_COLS <- c(
  "医院名称", "医院等级", "省份", "城市", "优势科室",
  "年门急诊量", "年住院量",
  "样本资源说明", "LIS获取难度", "LIS资源说明", "样本资源获取难度",
  "已获取样本信息", "GCP知情同意", "科研知情同意",
  "科室联系人评价", "机构联系人评价", "机构备注",
  "地址经度", "地址纬度", "医院合作等级",
  "科室备注", "HIS与病例资源获取难度", "HIS与病例资源说明"
)

INST_COLS <- c("仪器型号", "厂家", "方法学", "备注")
SAMPLE_COLS <- c("样本类型分类", "样本疾病分类")

is_free_text_col <- function(col) grepl("说明$|备注$", col)

# ==================== 2. 辅助函数 ====================

insert_audit_log <- function(conn, work_id, name, op_type, target_table, target_row_id,
                              biz_desc, summary, old_val, new_val, remark = NULL) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(invisible(NULL))
  work_id <- if (is.null(work_id) || is.na(work_id)) "" else as.character(work_id)
  name    <- if (is.null(name) || is.na(name)) "" else as.character(name)
  to_json_payload <- function(x) {
    if (is.null(x)) return("null")
    if (is.atomic(x) && !is.null(names(x))) x <- as.list(x)
    jsonlite::toJSON(x, auto_unbox = TRUE)
  }
  old_json <- to_json_payload(old_val)
  new_json <- to_json_payload(new_val)
  remark <- if (is.null(remark) || !nzchar(trimws(as.character(remark)))) NA_character_ else as.character(remark)
  q <- 'INSERT INTO public."07操作审计表" ("操作时间", "操作人工号", "操作人姓名", "操作类型", "目标表", "目标行id", "业务描述", "变更摘要", "旧值", "新值", "备注") VALUES (current_timestamp, $1, $2, $3, $4, $5, $6, $7, $8::json, $9::json, $10)'
  tryCatch({
    DBI::dbExecute(conn, q, params = list(work_id, name, op_type, target_table,
      as.integer(target_row_id), biz_desc, summary, old_json, new_json, remark))
  }, error = function(e) NULL)
  invisible(NULL)
}

next_id <- function(conn, table_name) {
  q <- sprintf('SELECT COALESCE(MAX(id), 0) + 1 AS nid FROM public."%s"', table_name)
  as.integer(DBI::dbGetQuery(conn, q)$nid[1])
}

safe_val <- function(x, default = "") {
  if (is.null(x) || length(x) == 0 || (length(x) == 1 && is.na(x))) default else as.character(x)
}

col_choices <- function(conn, table_name, col) {
  tryCatch({
    q <- sprintf('SELECT DISTINCT "%s" FROM public."%s" WHERE "%s" IS NOT NULL AND "%s" <> \'\' ORDER BY 1',
                 col, table_name, col, col)
    df <- DBI::dbGetQuery(conn, q)
    if (nrow(df) == 0) character(0) else na.omit(unique(df[[1]]))
  }, error = function(e) character(0))
}

# ==================== 3. UI ====================

ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
      .user-bar { display: flex; align-items: center; margin-bottom: 6px; }
      .section-title { font-size: 16px; font-weight: bold; margin: 18px 0 8px 0; padding-bottom: 4px; border-bottom: 2px solid #337ab7; }
      .edit-section { background: #fafbfc; border: 1px solid #e1e4e8; border-radius: 6px; padding: 12px; margin-bottom: 14px; }
      .sub-row { display: flex; align-items: center; margin-bottom: 8px; padding: 6px; border-bottom: 1px solid #eee; }
      .sub-row:last-child { border-bottom: none; }
      .sub-row .form-group { margin-bottom: 0; flex: 1; }
      .btn-del-row { background: none; border: none; color: #dc3545; cursor: pointer; font-size: 18px; margin-left: 8px; padding: 0 8px; }
      .btn-del-row:hover { color: #a71d2a; }
    ")),
    tags$script(HTML("
      $(document).on('click', '.btn-del-inst', function(e) {
        e.preventDefault();
        var idx = $(this).attr('data-idx');
        Shiny.onInputChange('del_inst_row', idx + '_' + Date.now());
      });
      $(document).on('click', '.btn-del-samp', function(e) {
        e.preventDefault();
        var idx = $(this).attr('data-idx');
        Shiny.onInputChange('del_samp_row', idx + '_' + Date.now());
      });
    "))
  ),
  uiOutput("page_title"),
  uiOutput("user_bar"),
  uiOutput("no_access"),
  fluidRow(
    column(8,
      selectizeInput("sel_hosp", "选择医院",
        choices = NULL,
        width = "100%",
        options = list(placeholder = "输入关键词搜索医院...", highlight = TRUE))
    ),
    column(4,
      tags$div(style = "margin-top: 24px;",
        actionButton("btn_new_hosp", "新增医院", class = "btn-success", style = "margin-right: 8px;")
      )
    )
  ),
  tags$hr(),
  uiOutput("hosp_editor"),
  uiOutput("inst_editor"),
  uiOutput("sample_editor"),
  uiOutput("save_area")
)

# ==================== 4. Server ====================

server <- function(input, output, session) {

  # ---- 权限 ----
  auth <- reactive({
    wid <- session$request$HTTP_X_USER
    wid <- if (is.null(wid)) "" else trimws(as.character(wid))
    if (!nzchar(wid)) return(list(ok = FALSE, reason = "未获取到用户身份"))
    tryCatch({
      r <- DBI::dbGetQuery(pg_pool,
        'SELECT id, "姓名", "数据库权限等级", "人员状态" FROM public."05人员表" WHERE "工号" = $1 LIMIT 1',
        params = list(wid))
      if (nrow(r) == 0) return(list(ok = FALSE, reason = "用户不存在"))
      status <- trimws(as.character(r[["人员状态"]][1]))
      level  <- trimws(as.character(r[["数据库权限等级"]][1]))
      if (is.na(status) || status != "在职") return(list(ok = FALSE, reason = "用户状态非在职"))
      if (is.na(level) || !(level %in% ALLOWED_LEVELS)) return(list(ok = FALSE, reason = paste("权限不足:", level)))
      name <- if (!is.na(r[["姓名"]][1])) trimws(as.character(r[["姓名"]][1])) else wid
      list(ok = TRUE, work_id = wid, name = name, level = level)
    }, error = function(e) list(ok = FALSE, reason = paste("查询失败:", e$message)))
  })

  output$page_title <- renderUI({ titlePanel("Snibe临床 - 数据库信息维护") })

  output$user_bar <- renderUI({
    a <- auth()
    if (!a$ok) return(NULL)
    txt <- if (nzchar(a$name) && nzchar(a$work_id)) paste0(a$name, "-", a$work_id) else if (nzchar(a$work_id)) a$work_id else a$name
    tags$div(style = "display: flex; align-items: center; margin-bottom: 6px;",
      tags$p(style = "color: #333; font-size: 13px; margin: 4px 0;", paste0("当前登录帐号：", txt)),
      tags$a(href = "/", "返回导航页", style = "margin-left: 20px; font-size: 13px;")
    )
  })

  output$no_access <- renderUI({
    a <- auth()
    if (a$ok) return(NULL)
    tags$div(class = "alert alert-danger",
      tags$h4("无法访问"), tags$p(a$reason), tags$a(href = "/", "返回导航页"))
  })

  # ---- 医院列表 ----
  hosp_list_df <- reactive({
    hosp_list_version()
    tryCatch({
      DBI::dbGetQuery(pg_pool, 'SELECT id, "医院名称" FROM public."01医院信息表" ORDER BY id')
    }, error = function(e) data.frame(id = integer(0), "医院名称" = character(0), check.names = FALSE))
  })

  hosp_choices_vec <- reactive({
    df <- hosp_list_df()
    if (nrow(df) == 0) return(setNames(character(0), character(0)))
    ids <- as.character(df$id)
    labels <- ifelse(is.na(df[["医院名称"]]) | !nzchar(trimws(df[["医院名称"]])),
                      paste0("(未命名 id:", df$id, ")"), df[["医院名称"]])
    setNames(ids, labels)
  })

  # ---- 核心状态 ----
  refresh_token <- reactiveVal(0L)
  selected_hosp_id <- reactiveVal(NA_integer_)
  is_new_hosp <- reactiveVal(FALSE)
  show_editor <- reactiveVal(FALSE)

  # 工作数据（用于 UI 渲染，add/delete 时更新）
  hosp_vals <- reactiveVal(list())
  inst_vals <- reactiveVal(list())
  sample_vals <- reactiveVal(list())

  # 原始快照（仅用于变更检测，加载后不修改）
  hosp_orig <- reactiveVal(list())
  inst_orig <- reactiveVal(list())
  sample_orig <- reactiveVal(list())
  hosp_list_version <- reactiveVal(0L)

  # ---- 辅助：从当前 input 读取所有值 ----

  snapshot_hosp_inputs <- function() {
    vals <- list()
    for (col in HOSP_EDITABLE_COLS) {
      input_id <- paste0("h_", col)
      val <- input[[input_id]]
      if (col %in% c("地址经度", "地址纬度")) {
        vals[[col]] <- if (is.null(val) || is.na(val) || !nzchar(trimws(as.character(val)))) NA_real_ else as.numeric(val)
      } else {
        vals[[col]] <- if (is.null(val) || !nzchar(trimws(val))) NA_character_ else trimws(val)
      }
    }
    vals
  }

  snapshot_inst_inputs <- function() {
    iv <- inst_vals()
    n <- length(iv)
    if (n == 0) return(list())
    result <- vector("list", n)
    for (i in seq_len(n)) {
      row <- list(id = iv[[i]]$id)
      for (col in INST_COLS) {
        input_id <- paste0("inst_", i, "_", col)
        val <- input[[input_id]]
        row[[col]] <- if (is.null(val) || !nzchar(trimws(val))) NA_character_ else trimws(val)
      }
      result[[i]] <- row
    }
    result
  }

  snapshot_sample_inputs <- function() {
    sv <- sample_vals()
    n <- length(sv)
    if (n == 0) return(list())
    result <- vector("list", n)
    for (i in seq_len(n)) {
      row <- list(id = sv[[i]]$id)
      for (col in SAMPLE_COLS) {
        input_id <- paste0("samp_", i, "_", col)
        val <- input[[input_id]]
        row[[col]] <- if (is.null(val) || !nzchar(trimws(val))) NA_character_ else trimws(val)
      }
      result[[i]] <- row
    }
    result
  }

  # ---- 从数据库加载医院数据 ----

  load_hosp_data <- function(hid) {
    tryCatch({
      # 医院信息
      hr <- DBI::dbGetQuery(pg_pool,
        'SELECT * FROM public."01医院信息表" WHERE id = $1', params = list(hid))
      hv <- list()
      ho <- list()
      if (nrow(hr) > 0) {
        for (col in HOSP_EDITABLE_COLS) {
          val <- hr[1, col]
          if (col %in% c("地址经度", "地址纬度")) {
            num_val <- if (is.na(val) || !nzchar(trimws(as.character(val)))) NA else as.numeric(val)
            hv[[col]] <- num_val
            ho[[col]] <- num_val
          } else {
            hv[[col]] <- safe_val(val)
            ho[[col]] <- safe_val(val)
          }
        }
      }
      hosp_vals(hv)
      hosp_orig(ho)

      # 仪器
      ir <- DBI::dbGetQuery(pg_pool,
        sprintf('SELECT id, %s FROM public."02仪器资源表" WHERE "01医院信息表_id" = $1 ORDER BY id',
                paste(sprintf('"%s"', INST_COLS), collapse = ", ")),
        params = list(hid))
      iv <- list()
      iov <- list()
      if (nrow(ir) > 0) {
        for (i in seq_len(nrow(ir))) {
          row <- list(id = as.integer(ir$id[i]))
          orig_row <- list(id = as.integer(ir$id[i]))
          for (col in INST_COLS) {
            row[[col]] <- safe_val(ir[i, col])
            orig_row[[col]] <- safe_val(ir[i, col])
          }
          iv[[i]] <- row
          iov[[i]] <- orig_row
        }
      }
      inst_vals(iv)
      inst_orig(iov)

      # 样本
      sr <- DBI::dbGetQuery(pg_pool,
        sprintf('SELECT id, %s FROM public."06样本资源表" WHERE "01医院信息表_id" = $1 ORDER BY id',
                paste(sprintf('"%s"', SAMPLE_COLS), collapse = ", ")),
        params = list(hid))
      sv <- list()
      sov <- list()
      if (nrow(sr) > 0) {
        for (i in seq_len(nrow(sr))) {
          row <- list(id = as.integer(sr$id[i]))
          orig_row <- list(id = as.integer(sr$id[i]))
          for (col in SAMPLE_COLS) {
            row[[col]] <- safe_val(sr[i, col])
            orig_row[[col]] <- safe_val(sr[i, col])
          }
          sv[[i]] <- row
          sov[[i]] <- orig_row
        }
      }
      sample_vals(sv)
      sample_orig(sov)
    }, error = function(e) {
      showNotification(paste("加载数据失败:", e$message), type = "error")
    })
  }

  # ---- 医院选择器（静态 UI，用 updateSelectizeInput 同步）----
  observe({
    hc <- hosp_choices_vec()
    if (is_new_hosp()) {
      hc <- c(setNames("__new__", "\u65b0\u589e\u533b\u9662"), hc)
      sel <- "__new__"
    } else {
      hid <- selected_hosp_id()
      sel <- if (is.na(hid)) character(0) else as.character(hid)
    }
    updateSelectizeInput(session, "sel_hosp", choices = hc, selected = sel)
  })

  # ---- 医院信息编辑区（独立渲染，不受仪器/样本影响）----
  output$hosp_editor <- renderUI({
    a <- auth()
    if (!a$ok || !show_editor()) return(NULL)
    title_text <- if (is_new_hosp()) "新增医院" else paste0("医院信息 (id=", selected_hosp_id(), ")")
    hv <- hosp_vals()
    hosp_inputs <- lapply(HOSP_EDITABLE_COLS, function(col) {
      input_id <- paste0("h_", col)
      val <- hv[[col]]
      if (is.null(val)) val <- if (col %in% c("地址经度", "地址纬度")) NA else ""
      if (col %in% c("地址经度", "地址纬度")) {
        column(4, numericInput(input_id, col, value = val, width = "100%"))
      } else if (is_free_text_col(col)) {
        column(4, textInput(input_id, col, value = safe_val(val), width = "100%"))
      } else {
        cc <- col_choices(pg_pool, "01医院信息表", col)
        choices <- c(setNames("", "(未填写)"), setNames(cc, cc))
        sv <- safe_val(val)
        if (nzchar(sv) && !(sv %in% unname(choices))) choices <- c(choices, setNames(sv, sv))
        column(4, selectizeInput(input_id, col,
          choices = choices,
          selected = sv,
          options = list(create = TRUE), width = "100%"))
      }
    })
    hosp_rows <- lapply(seq(1, length(hosp_inputs), by = 3), function(i) {
      fluidRow(hosp_inputs[seq(i, min(i + 2, length(hosp_inputs)))])
    })
    tagList(
      tags$div(class = "section-title", title_text),
      tags$div(class = "edit-section", hosp_rows)
    )
  })

  # ---- 仪器编辑区（独立渲染，仅依赖 inst_vals）----
  output$inst_editor <- renderUI({
    a <- auth()
    if (!a$ok || !show_editor()) return(NULL)
    iv <- inst_vals()
    inst_ui <- lapply(seq_along(iv), function(i) {
      row_data <- iv[[i]]
      row_divs <- lapply(INST_COLS, function(col) {
        input_id <- paste0("inst_", i, "_", col)
        val <- safe_val(row_data[[col]])
        if (is_free_text_col(col)) {
          div(style = "flex:1; margin-right:8px;",
              textInput(input_id, NULL, value = val, width = "100%", placeholder = col))
        } else {
          cc <- col_choices(pg_pool, "02仪器资源表", col)
          choices <- c(setNames("", "(未填写)"), setNames(cc, cc))
          if (nzchar(val) && !(val %in% unname(choices))) choices <- c(choices, setNames(val, val))
          div(style = "flex:1; margin-right:8px;",
              selectizeInput(input_id, NULL,
                choices = choices,
                selected = val,
                options = list(create = TRUE), width = "100%"))
        }
      })
      tags$div(class = "sub-row",
        row_divs,
        tags$button(class = "btn-del-row btn-del-inst", `data-idx` = as.character(i),
                    type = "button", "\u2716", title = "删除此行")
      )
    })
    tagList(
      tags$div(class = "section-title", "关联仪器"),
      tags$div(class = "edit-section",
        inst_ui,
        actionButton("btn_add_inst", "新增仪器", class = "btn-sm btn-success")
      )
    )
  })

  # ---- 样本编辑区（独立渲染，仅依赖 sample_vals）----
  output$sample_editor <- renderUI({
    a <- auth()
    if (!a$ok || !show_editor()) return(NULL)
    sv_data <- sample_vals()
    sample_ui <- lapply(seq_along(sv_data), function(i) {
      row_data <- sv_data[[i]]
      row_divs <- lapply(SAMPLE_COLS, function(col) {
        input_id <- paste0("samp_", i, "_", col)
        val <- safe_val(row_data[[col]])
        if (is_free_text_col(col)) {
          div(style = "flex:1; margin-right:8px;",
              textInput(input_id, NULL, value = val, width = "100%", placeholder = col))
        } else {
          cc <- col_choices(pg_pool, "06样本资源表", col)
          choices <- c(setNames("", "(未填写)"), setNames(cc, cc))
          if (nzchar(val) && !(val %in% unname(choices))) choices <- c(choices, setNames(val, val))
          div(style = "flex:1; margin-right:8px;",
              selectizeInput(input_id, NULL,
                choices = choices,
                selected = val,
                options = list(create = TRUE), width = "100%"))
        }
      })
      tags$div(class = "sub-row",
        row_divs,
        tags$button(class = "btn-del-row btn-del-samp", `data-idx` = as.character(i),
                    type = "button", "\u2716", title = "删除此行")
      )
    })
    tagList(
      tags$div(class = "section-title", "关联样本资源"),
      tags$div(class = "edit-section",
        sample_ui,
        actionButton("btn_add_sample", "新增样本", class = "btn-sm btn-success")
      )
    )
  })

  # ---- 保存按钮区 ----
  output$save_area <- renderUI({
    a <- auth()
    if (!a$ok || !show_editor()) return(NULL)
    tags$div(style = "margin: 20px 0;",
      actionButton("btn_save", "保存当前医院信息", class = "btn-primary btn-lg"),
      tags$span(id = "save_status", style = "margin-left: 12px; color: #888;")
    )
  })

  # ---- 选择医院 ----
  observeEvent(input$sel_hosp, {
    sel <- input$sel_hosp
    if (is.null(sel) || !nzchar(sel)) {
      if (is_new_hosp()) return()
      show_editor(FALSE)
      is_new_hosp(FALSE)
      selected_hosp_id(NA_integer_)
      hosp_vals(list())
      inst_vals(list())
      sample_vals(list())
      hosp_orig(list())
      inst_orig(list())
      sample_orig(list())
      refresh_token(refresh_token() + 1L)
      return()
    }
    if (sel == "__new__") return()
    hid <- as.integer(sel)
    selected_hosp_id(hid)
    is_new_hosp(FALSE)
    show_editor(TRUE)
    load_hosp_data(hid)
    refresh_token(refresh_token() + 1L)
  }, ignoreInit = TRUE)

  # ---- 新增医院 ----
  observeEvent(input$btn_new_hosp, {
    is_new_hosp(TRUE)
    selected_hosp_id(NA_integer_)
    show_editor(TRUE)
    hosp_vals(list())
    inst_vals(list())
    sample_vals(list())
    hosp_orig(list())
    inst_orig(list())
    sample_orig(list())
    refresh_token(refresh_token() + 1L)
    updateSelectizeInput(session, "sel_hosp",
      choices = c(setNames("__new__", "\u65b0\u589e\u533b\u9662"), hosp_choices_vec()),
      selected = "__new__")
  })

  # ---- 删除仪器行（JS 事件委托）----
  observeEvent(input$del_inst_row, {
    raw <- input$del_inst_row
    idx <- as.integer(sub("_.*$", "", as.character(raw)))
    if (is.na(idx)) return
    current <- snapshot_inst_inputs()
    if (idx < 1 || idx > length(current)) return
    inst_vals(current[-idx])
  }, ignoreInit = TRUE)

  # ---- 删除样本行（JS 事件委托）----
  observeEvent(input$del_samp_row, {
    raw <- input$del_samp_row
    idx <- as.integer(sub("_.*$", "", as.character(raw)))
    if (is.na(idx)) return
    current <- snapshot_sample_inputs()
    if (idx < 1 || idx > length(current)) return
    sample_vals(current[-idx])
  }, ignoreInit = TRUE)

  # ---- 新增仪器行 ----
  observeEvent(input$btn_add_inst, {
    current <- snapshot_inst_inputs()
    empty_row <- list(id = NA_integer_)
    for (col in INST_COLS) empty_row[[col]] <- ""
    inst_vals(c(current, list(empty_row)))
  })

  # ---- 新增样本行 ----
  observeEvent(input$btn_add_sample, {
    current <- snapshot_sample_inputs()
    empty_row <- list(id = NA_integer_)
    for (col in SAMPLE_COLS) empty_row[[col]] <- ""
    sample_vals(c(current, list(empty_row)))
  })

  # ---- 保存（变更检测 + 确认弹窗）----
  observeEvent(input$btn_save, {
    a <- auth()
    req(a$ok)

    hid <- selected_hosp_id()
    new_mode <- is_new_hosp()

    # 读取当前输入值
    new_hosp <- snapshot_hosp_inputs()
    new_inst <- snapshot_inst_inputs()
    new_sample <- snapshot_sample_inputs()

    # 原始数据
    ho <- hosp_orig()
    iov <- inst_orig()
    sov <- sample_orig()

    # ---- 变更检测 ----
    has_changes <- FALSE
    change_desc <- character(0)

    # 1) 医院信息变更
    if (new_mode) {
      has_changes <- TRUE
      change_desc <- c(change_desc, "新增医院")
    } else if (length(ho) > 0) {
      for (col in HOSP_EDITABLE_COLS) {
        old_v <- if (col %in% c("地址经度", "地址纬度")) {
          if (is.null(ho[[col]]) || is.na(ho[[col]])) "" else as.character(ho[[col]])
        } else safe_val(ho[[col]])
        new_v <- if (col %in% c("地址经度", "地址纬度")) {
          if (is.null(new_hosp[[col]]) || is.na(new_hosp[[col]])) "" else as.character(new_hosp[[col]])
        } else safe_val(new_hosp[[col]])
        if (old_v != new_v) {
          has_changes <- TRUE
          change_desc <- c(change_desc, sprintf("医院.%s: [%s] -> [%s]", col, old_v, new_v))
        }
      }
    }

    # 2) 仪器变更
    orig_inst_ids <- if (length(iov) > 0) sapply(iov, function(r) r$id) else integer(0)
    curr_inst_ids <- if (length(new_inst) > 0) sapply(new_inst, function(r) r$id) else integer(0)
    deleted_inst_ids <- setdiff(orig_inst_ids, curr_inst_ids)

    if (length(deleted_inst_ids) > 0) {
      has_changes <- TRUE
      change_desc <- c(change_desc, sprintf("删除 %d 条仪器记录", length(deleted_inst_ids)))
    }

    for (i in seq_along(new_inst)) {
      r <- new_inst[[i]]
      if (is.na(r$id)) {
        non_empty <- sum(!is.na(r[INST_COLS]) & nzchar(sapply(r[INST_COLS], safe_val)))
        if (non_empty > 0) {
          has_changes <- TRUE
          change_desc <- c(change_desc, sprintf("新增仪器: %s", safe_val(r[["仪器型号"]], "(未填)")))
        }
      } else {
        orig_row <- NULL
        for (j in seq_along(iov)) {
          if (iov[[j]]$id == r$id) { orig_row <- iov[[j]]; break }
        }
        if (!is.null(orig_row)) {
          for (col in INST_COLS) {
            old_v <- safe_val(orig_row[[col]])
            new_v <- safe_val(r[[col]])
            if (old_v != new_v) {
              has_changes <- TRUE
              change_desc <- c(change_desc, sprintf("仪器(id=%d).%s: [%s]->[%s]", r$id, col, old_v, new_v))
            }
          }
        }
      }
    }

    # 3) 样本变更
    orig_sample_ids <- if (length(sov) > 0) sapply(sov, function(r) r$id) else integer(0)
    curr_sample_ids <- if (length(new_sample) > 0) sapply(new_sample, function(r) r$id) else integer(0)
    deleted_sample_ids <- setdiff(orig_sample_ids, curr_sample_ids)

    if (length(deleted_sample_ids) > 0) {
      has_changes <- TRUE
      change_desc <- c(change_desc, sprintf("删除 %d 条样本记录", length(deleted_sample_ids)))
    }

    for (i in seq_along(new_sample)) {
      r <- new_sample[[i]]
      if (is.na(r$id)) {
        non_empty <- sum(!is.na(r[SAMPLE_COLS]) & nzchar(sapply(r[SAMPLE_COLS], safe_val)))
        if (non_empty > 0) {
          has_changes <- TRUE
          change_desc <- c(change_desc, sprintf("新增样本: %s/%s", safe_val(r[["样本类型分类"]], "-"), safe_val(r[["样本疾病分类"]], "-")))
        }
      } else {
        orig_row <- NULL
        for (j in seq_along(sov)) {
          if (sov[[j]]$id == r$id) { orig_row <- sov[[j]]; break }
        }
        if (!is.null(orig_row)) {
          for (col in SAMPLE_COLS) {
            old_v <- safe_val(orig_row[[col]])
            new_v <- safe_val(r[[col]])
            if (old_v != new_v) {
              has_changes <- TRUE
              change_desc <- c(change_desc, sprintf("样本(id=%d).%s: [%s]->[%s]", r$id, col, old_v, new_v))
            }
          }
        }
      }
    }

    # ---- 无变更 ----
    if (!has_changes) {
      showNotification("没有检测到任何修改。", type = "warning")
      return()
    }

    # ---- 确认弹窗 ----
    showModal(modalDialog(
      title = "确认保存",
      size = "l", easyClose = TRUE,
      tags$div(
        tags$p(strong("检测到以下修改：")),
        tags$ul(style = "max-height: 300px; overflow-y: auto;",
          lapply(head(change_desc, 30), function(d) tags$li(d))
        ),
        if (length(change_desc) > 30) tags$p(sprintf("... 共 %d 条变更", length(change_desc)))
      ),
      footer = tagList(
        actionButton("btn_save_confirm", "确认保存", class = "btn-primary"),
        modalButton("取消")
      )
    ))

    session$userData$pending_save <- list(
      new_mode = new_mode, hid = hid,
      new_hosp = new_hosp, new_inst = new_inst, new_sample = new_sample,
      deleted_inst_ids = deleted_inst_ids, deleted_sample_ids = deleted_sample_ids,
      hosp_orig = ho, inst_orig = iov, sample_orig = sov
    )
  })

  # ---- 确认保存执行（逐字段审计）----
  observeEvent(input$btn_save_confirm, {
    a <- auth()
    req(a$ok)
    ps <- session$userData$pending_save
    if (is.null(ps)) return()

    tryCatch({
      hid <- ps$hid
      hosp_name <- safe_val(ps$new_hosp[["医院名称"]], paste0("id=", hid))

      # ========== 新增医院 ==========
      if (ps$new_mode) {
        new_hid <- next_id(pg_pool, "01医院信息表")
        cols_str <- paste(c('"id"', paste0('"', HOSP_EDITABLE_COLS, '"'), '"created_by"', '"updated_by"'), collapse = ", ")
        placeholders <- paste(paste0("$", seq_len(length(ps$new_hosp) + 3)), collapse = ", ")
        q <- sprintf('INSERT INTO public."01医院信息表" (%s) VALUES (%s)', cols_str, placeholders)
        vals <- lapply(ps$new_hosp, function(v) if (is.null(v)) NA_character_ else v)
        DBI::dbExecute(pg_pool, q, params = c(list(new_hid), unname(vals), list(a$work_id, a$work_id)))

        insert_audit_log(pg_pool, a$work_id, a$name,
          "INSERT", "01医院信息表", new_hid,
          sprintf("新增医院: %s", hosp_name),
          sprintf("新增医院 id=%d", new_hid),
          NULL, ps$new_hosp, NULL)

        hid <- new_hid

      } else {
        # ========== 更新医院（逐字段审计）==========
        ho <- ps$hosp_orig
        updates <- list()
        for (col in HOSP_EDITABLE_COLS) updates[[col]] <- ps$new_hosp[[col]]
        updates[["updated_by"]] <- a$work_id
        cols <- names(updates)
        set_clause <- paste(sprintf('"%s" = $%d', cols, seq_along(cols)), collapse = ", ")
        q <- sprintf('UPDATE public."01医院信息表" SET %s WHERE id = $%d', set_clause, length(cols) + 1L)
        DBI::dbExecute(pg_pool, q, params = c(unname(updates), list(as.integer(hid))))

        for (col in HOSP_EDITABLE_COLS) {
          old_v <- if (col %in% c("地址经度", "地址纬度")) {
            if (is.null(ho[[col]]) || is.na(ho[[col]])) "" else as.character(ho[[col]])
          } else safe_val(ho[[col]])
          new_v <- if (col %in% c("地址经度", "地址纬度")) {
            if (is.null(ps$new_hosp[[col]]) || is.na(ps$new_hosp[[col]])) "" else as.character(ps$new_hosp[[col]])
          } else safe_val(ps$new_hosp[[col]])
          if (old_v != new_v) {
            insert_audit_log(pg_pool, a$work_id, a$name,
              "UPDATE", "01医院信息表", hid,
              sprintf("修改医院[%s].%s: [%s] -> [%s]", hosp_name, col, old_v, new_v),
              sprintf("医院.%s变更", col),
              setNames(list(old_v), col), setNames(list(new_v), col), NULL)
          }
        }
      }

      # ========== 删除仪器 ==========
      for (did in ps$deleted_inst_ids) {
        old_row <- NULL
        for (j in seq_along(ps$inst_orig)) {
          if (ps$inst_orig[[j]]$id == did) { old_row <- ps$inst_orig[[j]]; break }
        }
        DBI::dbExecute(pg_pool, 'DELETE FROM public."02仪器资源表" WHERE id = $1', params = list(as.integer(did)))
        model_name <- if (!is.null(old_row)) safe_val(old_row[["仪器型号"]]) else "(未知)"
        insert_audit_log(pg_pool, a$work_id, a$name,
          "DELETE", "02仪器资源表", did,
          sprintf("删除仪器[%s](id=%d), 医院[%s]", model_name, did, hosp_name),
          sprintf("删除仪器 id=%d", did),
          old_row, NULL, NULL)
      }

      # ========== 仪器新增/更新 ==========
      for (row_data in ps$new_inst) {
        if (is.na(row_data$id)) {
          non_empty <- sum(!is.na(row_data[INST_COLS]) & nzchar(sapply(row_data[INST_COLS], safe_val)))
          if (non_empty == 0) next
          new_id <- next_id(pg_pool, "02仪器资源表")
          q_ins <- sprintf(
            'INSERT INTO public."02仪器资源表" ("id", %s, "01医院信息表_id", "created_by", "updated_by") VALUES ($1, %s, $%d, $%d, $%d)',
            paste(sprintf('"%s"', INST_COLS), collapse = ", "),
            paste(paste0("$", 2:(1 + length(INST_COLS))), collapse = ", "),
            2L + length(INST_COLS), 3L + length(INST_COLS), 4L + length(INST_COLS))
          params <- unname(c(list(new_id),
                      lapply(row_data[INST_COLS], function(v) if (is.null(v) || is.na(v)) NA_character_ else v),
                      list(as.integer(hid), a$work_id, a$work_id)))
          DBI::dbExecute(pg_pool, q_ins, params = params)
          insert_audit_log(pg_pool, a$work_id, a$name,
            "INSERT", "02仪器资源表", new_id,
            sprintf("新增仪器[%s](id=%d), 医院[%s]", safe_val(row_data[["仪器型号"]], "(未填)"), new_id, hosp_name),
            sprintf("新增仪器 id=%d", new_id),
            NULL, row_data[INST_COLS], NULL)
        } else {
          orig_row <- NULL
          for (j in seq_along(ps$inst_orig)) {
            if (ps$inst_orig[[j]]$id == row_data$id) { orig_row <- ps$inst_orig[[j]]; break }
          }
          updates <- lapply(row_data[INST_COLS], function(v) if (is.null(v) || is.na(v)) NA_character_ else v)
          names(updates) <- INST_COLS
          updates[["updated_by"]] <- a$work_id
          cols <- names(updates)
          set_clause <- paste(sprintf('"%s" = $%d', cols, seq_along(cols)), collapse = ", ")
          q_upd <- sprintf('UPDATE public."02仪器资源表" SET %s WHERE id = $%d', set_clause, length(cols) + 1L)
          DBI::dbExecute(pg_pool, q_upd, params = c(unname(updates), list(as.integer(row_data$id))))

          if (!is.null(orig_row)) {
            for (col in INST_COLS) {
              old_v <- safe_val(orig_row[[col]])
              new_v <- safe_val(row_data[[col]])
              if (old_v != new_v) {
                insert_audit_log(pg_pool, a$work_id, a$name,
                  "UPDATE", "02仪器资源表", row_data$id,
                  sprintf("修改仪器[%s](id=%d).%s: [%s]->[%s]",
                    safe_val(orig_row[["仪器型号"]]), row_data$id, col, old_v, new_v),
                  sprintf("仪器.%s变更", col),
                  setNames(list(old_v), col), setNames(list(new_v), col), NULL)
              }
            }
          }
        }
      }

      # ========== 删除样本 ==========
      for (did in ps$deleted_sample_ids) {
        old_row <- NULL
        for (j in seq_along(ps$sample_orig)) {
          if (ps$sample_orig[[j]]$id == did) { old_row <- ps$sample_orig[[j]]; break }
        }
        DBI::dbExecute(pg_pool, 'DELETE FROM public."06样本资源表" WHERE id = $1', params = list(as.integer(did)))
        type_cat <- if (!is.null(old_row)) safe_val(old_row[["样本类型分类"]]) else "-"
        disease_cat <- if (!is.null(old_row)) safe_val(old_row[["样本疾病分类"]]) else "-"
        insert_audit_log(pg_pool, a$work_id, a$name,
          "DELETE", "06样本资源表", did,
          sprintf("删除样本[%s/%s](id=%d), 医院[%s]", type_cat, disease_cat, did, hosp_name),
          sprintf("删除样本 id=%d", did),
          old_row, NULL, NULL)
      }

      # ========== 样本新增/更新 ==========
      for (row_data in ps$new_sample) {
        if (is.na(row_data$id)) {
          non_empty <- sum(!is.na(row_data[SAMPLE_COLS]) & nzchar(sapply(row_data[SAMPLE_COLS], safe_val)))
          if (non_empty == 0) next
          new_id <- next_id(pg_pool, "06样本资源表")
          q_ins <- sprintf(
            'INSERT INTO public."06样本资源表" ("id", %s, "01医院信息表_id", "created_by", "updated_by") VALUES ($1, %s, $%d, $%d, $%d)',
            paste(sprintf('"%s"', SAMPLE_COLS), collapse = ", "),
            paste(paste0("$", 2:(1 + length(SAMPLE_COLS))), collapse = ", "),
            2L + length(SAMPLE_COLS), 3L + length(SAMPLE_COLS), 4L + length(SAMPLE_COLS))
          params <- unname(c(list(new_id),
                      lapply(row_data[SAMPLE_COLS], function(v) if (is.null(v) || is.na(v)) NA_character_ else v),
                      list(as.integer(hid), a$work_id, a$work_id)))
          DBI::dbExecute(pg_pool, q_ins, params = params)
          insert_audit_log(pg_pool, a$work_id, a$name,
            "INSERT", "06样本资源表", new_id,
            sprintf("新增样本[%s/%s](id=%d), 医院[%s]",
              safe_val(row_data[["样本类型分类"]], "-"),
              safe_val(row_data[["样本疾病分类"]], "-"),
              new_id, hosp_name),
            sprintf("新增样本 id=%d", new_id),
            NULL, row_data[SAMPLE_COLS], NULL)
        } else {
          orig_row <- NULL
          for (j in seq_along(ps$sample_orig)) {
            if (ps$sample_orig[[j]]$id == row_data$id) { orig_row <- ps$sample_orig[[j]]; break }
          }
          updates <- lapply(row_data[SAMPLE_COLS], function(v) if (is.null(v) || is.na(v)) NA_character_ else v)
          names(updates) <- SAMPLE_COLS
          updates[["updated_by"]] <- a$work_id
          cols <- names(updates)
          set_clause <- paste(sprintf('"%s" = $%d', cols, seq_along(cols)), collapse = ", ")
          q_upd <- sprintf('UPDATE public."06样本资源表" SET %s WHERE id = $%d', set_clause, length(cols) + 1L)
          DBI::dbExecute(pg_pool, q_upd, params = c(unname(updates), list(as.integer(row_data$id))))

          if (!is.null(orig_row)) {
            for (col in SAMPLE_COLS) {
              old_v <- safe_val(orig_row[[col]])
              new_v <- safe_val(row_data[[col]])
              if (old_v != new_v) {
                insert_audit_log(pg_pool, a$work_id, a$name,
                  "UPDATE", "06样本资源表", row_data$id,
                  sprintf("修改样本[%s/%s](id=%d).%s: [%s]->[%s]",
                    safe_val(orig_row[["样本类型分类"]]),
                    safe_val(orig_row[["样本疾病分类"]]),
                    row_data$id, col, old_v, new_v),
                  sprintf("样本.%s变更", col),
                  setNames(list(old_v), col), setNames(list(new_v), col), NULL)
              }
            }
          }
        }
      }

      # ========== 保存成功 ==========
      removeModal()
      showNotification("保存成功！", type = "message")

      if (ps$new_mode) {
        is_new_hosp(FALSE)
        selected_hosp_id(hid)
        show_editor(TRUE)
        hosp_list_version(hosp_list_version() + 1L)
      }
      load_hosp_data(hid)
      refresh_token(refresh_token() + 1L)

    }, error = function(e) {
      showNotification(paste("保存失败:", e$message), type = "error")
    })
  })
}

shinyApp(ui = ui, server = server)
