library(shiny)
library(DBI)
library(RPostgres)
library(pool)

if (!requireNamespace("digest", quietly = TRUE)) {
  stop("需要先安装 R 包 'digest' 用于密码哈希：install.packages('digest')", call. = FALSE)
}

# ---------------- 数据库连接配置（与 app.R 保持一致） ----------------
db_config <- list(
  host     = Sys.getenv("PG_HOST",     "localhost"),
  port     = as.integer(Sys.getenv("PG_PORT",     "5432")),
  dbname   = Sys.getenv("PG_DBNAME",   "ivd_data"),
  user     = Sys.getenv("PG_USER",     "myuser"),
  password = Sys.getenv("PG_PASSWORD", "mypassword")
)

pg_pool <- dbPool(
  drv      = RPostgres::Postgres(),
  host     = db_config$host,
  port     = db_config$port,
  dbname   = db_config$dbname,
  user     = db_config$user,
  password = db_config$password,
  maxSize  = 10
)

onStop(function() { poolClose(pg_pool) })

# ---------------- 工具函数 ----------------
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a

hash_password <- function(pwd) digest::digest(pwd, algo = "sha256")

is_valid_password <- function(pwd) {
  if (is.null(pwd)) return(FALSE)
  pwd <- as.character(pwd)
  nchar(pwd) >= 6 && grepl("[A-Za-z]", pwd)
}

get_user_by_work_id <- function(con, work_id) {
  DBI::dbGetQuery(
    con,
    'SELECT id, "工号", "姓名", "哈希密码", "人员状态", "数据库权限等级"
       FROM public."05人员表"
      WHERE "工号" = $1
      LIMIT 1',
    params = list(work_id)
  )
}

parse_cookies <- function(cookie_header) {
  if (is.null(cookie_header) || !nzchar(cookie_header)) return(list())
  parts <- strsplit(cookie_header, ";\\s*")[[1]]
  res <- list()
  for (p in parts) {
    kv <- strsplit(p, "=", fixed = TRUE)[[1]]
    if (length(kv) >= 2) {
      name  <- kv[1]
      value <- paste(kv[-1], collapse = "=")
      res[[name]] <- value
    }
  }
  res
}

# ---------------- 带颜色的提示消息 ----------------
# 使用方式：output$xxx_message <- renderUI(msg_error("..."))
msg_error <- function(text) {
  tags$div(
    style = "margin-top:8px; padding:8px 12px; border-radius:4px;
             background:#FFEBEE; color:#C62828; border-left:4px solid #EF5350;",
    tags$b("\u26a0 "), text
  )
}

msg_success <- function(text) {
  tags$div(
    style = "margin-top:8px; padding:8px 12px; border-radius:4px;
             background:#E8F5E9; color:#2E7D32; border-left:4px solid #66BB6A;",
    tags$b("\u2713 "), text
  )
}

msg_warning <- function(text) {
  tags$div(
    style = "margin-top:8px; padding:8px 12px; border-radius:4px;
             background:#FFF8E1; color:#E65100; border-left:4px solid #FFA726;",
    tags$b("\u2139 "), text
  )
}

msg_info <- function(text) {
  tags$div(
    style = "margin-top:8px; padding:8px 12px; border-radius:4px;
             background:#E3F2FD; color:#1565C0; border-left:4px solid #42A5F5;",
    text
  )
}

# ---------------- UI ----------------
base_ui <- fluidPage(
  titlePanel("临床项目管理系统 - 登录与注册"),
  fluidRow(
    column(
      width = 6, offset = 3,
      uiOutput("main_ui")
    )
  ),
  tags$script(HTML("
    Shiny.addCustomMessageHandler('set-cookie', function(msg) {
      var v = encodeURIComponent(msg.value);
      document.cookie = msg.name + '=' + v + '; path=/';
    });
    Shiny.addCustomMessageHandler('redirect', function(msg) {
      var url = (msg && msg.url) ? msg.url : (typeof msg === 'string' ? msg : null);
      if (url) window.location.href = url;
    });
  "))
)

ui <- function(req) {
  if (identical(req$HTTP_X_AUTH_VERIFY, "1")) {
    cookies <- parse_cookies(req$HTTP_COOKIE %||% "")
    wid <- cookies[["ivd_user"]] %||% ""
    ok <- FALSE
    if (nzchar(wid)) {
      df <- tryCatch(
        DBI::dbGetQuery(
          pg_pool,
          'SELECT "人员状态", "数据库权限等级" FROM public."05人员表" WHERE "工号" = $1 LIMIT 1',
          params = list(wid)
        ),
        error = function(e) data.frame()
      )
      if (nrow(df) == 1) {
        status <- trimws(as.character(df[["人员状态"]][1]))
        level  <- trimws(as.character(df[["数据库权限等级"]][1]))
        ok <- !is.na(status) && identical(status, "在职") &&
          !is.na(level) && !identical(level, "deny_access")
      }
    }
    if (ok) {
      return(shiny::httpResponse(status = 200L, headers = list("X-User" = wid)))
    } else {
      return(shiny::httpResponse(status = 401L))
    }
  }
  base_ui
}

# ---------------- Server ----------------
server <- function(input, output, session) {
  pg_con <- pg_pool

  mode            <- reactiveVal("login")
  current_user    <- reactiveVal(NULL)
  show_forgot_pw  <- reactiveVal(FALSE)   # 登录页"忘记密码"展开状态

  # ---------- 各界面 UI ----------
  login_ui <- reactive({
    tagList(
      wellPanel(
        h3("登录"),
        textInput("login_work_id",   "工号", value = ""),
        passwordInput("login_password", "密码", value = ""),
        actionButton("btn_login",           "登录",       class = "btn-primary"),
        tags$span(style = "margin-left:10px;"),
        actionButton("btn_go_register",     "新用户注册", class = "btn-link"),
        tags$span(style = "margin-left:10px;"),
        actionButton("btn_go_change_pw",    "修改密码",   class = "btn-link"),
        tags$span(style = "margin-left:10px;"),
        actionButton("btn_forgot_pw",       "忘记密码",   class = "btn-link"),
        tags$hr(),
        uiOutput("forgot_pw_message"),
        uiOutput("login_message")
      )
    )
  })

  register_ui <- reactive({
    tagList(
      wellPanel(
        h3("新用户注册"),
        textInput("reg_work_id",           "工号",     value = ""),
        passwordInput("reg_password",      "密码",     value = ""),
        passwordInput("reg_password_confirm", "确认密码", value = ""),
        actionButton("btn_register",            "确定注册", class = "btn-success"),
        tags$span(style = "margin-left:10px;"),
        actionButton("btn_back_login_from_reg", "返回登录", class = "btn-link"),
        tags$span(style = "margin-left:10px;"),
        actionButton("btn_forgot_pw_reg",       "忘记密码", class = "btn-link"),
        tags$hr(),
        uiOutput("forgot_pw_reg_message"),
        uiOutput("register_message")
      )
    )
  })

  change_pw_ui <- reactive({
    tagList(
      wellPanel(
        h3("修改密码"),
        textInput("chg_work_id",                  "工号",     value = ""),
        passwordInput("chg_old_password",         "原密码",   value = ""),
        passwordInput("chg_new_password",         "新密码",   value = ""),
        passwordInput("chg_new_password_confirm", "确认新密码", value = ""),
        actionButton("btn_change_password",           "确定修改", class = "btn-warning"),
        tags$span(style = "margin-left:10px;"),
        actionButton("btn_back_login_from_chg",       "返回登录", class = "btn-link"),
        tags$span(style = "margin-left:10px;"),
        actionButton("btn_forgot_pw_chg",             "忘记密码", class = "btn-link"),
        tags$hr(),
        uiOutput("forgot_pw_chg_message"),
        uiOutput("change_pw_message")
      )
    )
  })

  output$main_ui <- renderUI({
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      return(wellPanel(
        h3("数据库连接失败"),
        p("请检查 PG_HOST / PG_PORT / PG_DBNAME / PG_USER / PG_PASSWORD 等环境变量，或确保数据库服务已启动。")
      ))
    }
    switch(mode(),
      login           = login_ui(),
      register        = register_ui(),
      change_password = change_pw_ui(),
      login_ui()
    )
  })

  # ---------- 忘记密码 ----------
  forgot_msg <- tags$div(
    style = "margin-top:6px; padding:6px 10px; border-radius:4px;
             background:#F3E5F5; color:#6A1B9A; border-left:4px solid #AB47BC;",
    "\U1F4DE  如忘记密码请联系邱东艺"
  )

  observeEvent(input$btn_forgot_pw,     { output$forgot_pw_message     <- renderUI(forgot_msg) })
  observeEvent(input$btn_forgot_pw_reg, { output$forgot_pw_reg_message <- renderUI(forgot_msg) })
  observeEvent(input$btn_forgot_pw_chg, { output$forgot_pw_chg_message <- renderUI(forgot_msg) })

  # ---------- 界面切换 ----------
  observeEvent(input$btn_go_register,       { mode("register") })
  observeEvent(input$btn_go_change_pw,      { mode("change_password") })
  observeEvent(input$btn_back_login_from_reg, { mode("login") })
  observeEvent(input$btn_back_login_from_chg, { mode("login") })

  # ---------- 登录逻辑 ----------
  observeEvent(input$btn_login, {
    output$login_message <- renderUI(NULL)
    wid <- trimws(as.character(input$login_work_id %||% ""))
    pwd <- as.character(input$login_password %||% "")

    if (!nzchar(wid) || !nzchar(pwd)) {
      output$login_message <- renderUI(msg_error("请输入工号和密码。"))
      return()
    }
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      output$login_message <- renderUI(msg_error("数据库连接不可用，请联系管理员。"))
      return()
    }

    u <- tryCatch(get_user_by_work_id(pg_con, wid), error = function(e) NULL)
    if (is.null(u) || nrow(u) == 0) {
      output$login_message <- renderUI(msg_error("未知帐号，请确认工号是否有误。"))
      return()
    }

    hash_db <- as.character(u[["哈希密码"]][1])
    status  <- trimws(as.character(u[["人员状态"]][1]))

    if (is.na(hash_db) || !nzchar(hash_db)) {
      output$login_message <- renderUI(msg_warning("新用户请先完成注册后再登录。"))
      return()
    }
    if (!identical(status, "在职")) {
      output$login_message <- renderUI(msg_error("该用户已离职或无效，无法访问系统。"))
      return()
    }
    if (!identical(hash_password(pwd), hash_db)) {
      output$login_message <- renderUI(msg_error("帐号或密码错误，请重新输入。"))
      return()
    }

    current_user(list(
      work_id = wid,
      name    = if ("姓名" %in% names(u)) as.character(u[["姓名"]][1]) else ""
    ))
    session$sendCustomMessage("set-cookie", list(name = "ivd_user", value = wid))
    session$sendCustomMessage("redirect",   list(url = "/"))
  })

  # ---------- 注册逻辑 ----------
  observeEvent(input$btn_register, {
    output$register_message <- renderUI(NULL)
    wid  <- trimws(as.character(input$reg_work_id          %||% ""))
    pwd  <- as.character(input$reg_password                %||% "")
    pwd2 <- as.character(input$reg_password_confirm        %||% "")

    if (!nzchar(wid) || !nzchar(pwd) || !nzchar(pwd2)) {
      output$register_message <- renderUI(msg_error("请完整填写工号和两次密码。"))
      return()
    }
    if (pwd != pwd2) {
      output$register_message <- renderUI(msg_error("两次输入的密码不一致，请重新确认。"))
      return()
    }
    if (!is_valid_password(pwd)) {
      output$register_message <- renderUI(msg_error("密码至少 6 位且必须包含至少一个字母。"))
      return()
    }
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      output$register_message <- renderUI(msg_error("数据库连接不可用，请联系管理员。"))
      return()
    }

    u <- tryCatch(get_user_by_work_id(pg_con, wid), error = function(e) NULL)
    if (is.null(u) || nrow(u) == 0) {
      output$register_message <- renderUI(msg_error("未知帐号，请确认工号是否有误。"))
      return()
    }

    hash_db <- as.character(u[["哈希密码"]][1])
    if (!is.na(hash_db) && nzchar(hash_db)) {
      output$register_message <- renderUI(msg_warning("该帐号已完成注册，请直接登录。"))
      return()
    }

    new_hash <- hash_password(pwd)
    uid <- as.integer(u[["id"]][1])
    ok <- tryCatch({
      DBI::dbExecute(
        pg_con,
        'UPDATE public."05人员表" SET "哈希密码" = $1 WHERE id = $2',
        params = list(new_hash, uid)
      )
      TRUE
    }, error = function(e) {
      output$register_message <- renderUI(msg_error(paste0("注册失败：", conditionMessage(e))))
      FALSE
    })

    if (isTRUE(ok)) {
      output$register_message <- renderUI(msg_success("\u6ce8\u518c\u6210\u529f\uff01\u8bf7\u70b9\u51fb\u300c\u8fd4\u56de\u767b\u5f55\u300d\u4f7f\u7528\u8be5\u5de5\u53f7\u767b\u5f55\u3002"))
    }
  })

  # ---------- 修改密码逻辑 ----------
  observeEvent(input$btn_change_password, {
    output$change_pw_message <- renderUI(NULL)
    wid      <- trimws(as.character(input$chg_work_id             %||% ""))
    old_pwd  <- as.character(input$chg_old_password               %||% "")
    new_pwd  <- as.character(input$chg_new_password               %||% "")
    new_pwd2 <- as.character(input$chg_new_password_confirm       %||% "")

    if (!nzchar(wid) || !nzchar(old_pwd) || !nzchar(new_pwd) || !nzchar(new_pwd2)) {
      output$change_pw_message <- renderUI(msg_error("请完整填写工号、原密码和新密码。"))
      return()
    }
    if (new_pwd != new_pwd2) {
      output$change_pw_message <- renderUI(msg_error("两次输入的新密码不一致，请重新确认。"))
      return()
    }
    if (!is_valid_password(new_pwd)) {
      output$change_pw_message <- renderUI(msg_error("新密码至少 6 位且必须包含至少一个字母。"))
      return()
    }
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      output$change_pw_message <- renderUI(msg_error("数据库连接不可用，请联系管理员。"))
      return()
    }

    u <- tryCatch(get_user_by_work_id(pg_con, wid), error = function(e) NULL)
    if (is.null(u) || nrow(u) == 0) {
      output$change_pw_message <- renderUI(msg_error("未知帐号，请确认工号是否有误。"))
      return()
    }

    hash_db <- as.character(u[["哈希密码"]][1])
    status  <- trimws(as.character(u[["人员状态"]][1]))

    if (is.na(hash_db) || !nzchar(hash_db)) {
      output$change_pw_message <- renderUI(msg_warning("新用户请先完成注册再修改密码。"))
      return()
    }
    if (!identical(status, "在职")) {
      output$change_pw_message <- renderUI(msg_error("该用户已离职或无效，无法修改密码。"))
      return()
    }
    if (!identical(hash_password(old_pwd), hash_db)) {
      output$change_pw_message <- renderUI(msg_error("原密码错误，请重新输入。"))
      return()
    }

    new_hash <- hash_password(new_pwd)
    uid <- as.integer(u[["id"]][1])
    ok <- tryCatch({
      DBI::dbExecute(
        pg_con,
        'UPDATE public."05人员表" SET "哈希密码" = $1 WHERE id = $2',
        params = list(new_hash, uid)
      )
      TRUE
    }, error = function(e) {
      output$change_pw_message <- renderUI(msg_error(paste0("修改失败：", conditionMessage(e))))
      FALSE
    })

    if (isTRUE(ok)) {
      output$change_pw_message <- renderUI(msg_success("密码修改成功！请使用新密码重新登录。"))
    }
  })
}

shinyApp(ui = ui, server = server)
