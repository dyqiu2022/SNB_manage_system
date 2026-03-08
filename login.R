library(shiny)
library(DBI)
library(RPostgres)
library(pool)

if (!requireNamespace("digest", quietly = TRUE)) {
  stop("需要先安装 R 包 'digest' 用于密码哈希：install.packages('digest')", call. = FALSE)
}

# ---------------- 数据库连接配置（与 app.R 保持一致） ----------------
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

# ---------------- 工具函数 ----------------
`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) b else a
}

hash_password <- function(pwd) {
  digest::digest(pwd, algo = "sha256")
}

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
      name <- kv[1]
      value <- paste(kv[-1], collapse = "=")
      res[[name]] <- value
    }
  }
  res
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
      var pathPart = '; path=/';
      document.cookie = msg.name + '=' + v + pathPart;
    });
    Shiny.addCustomMessageHandler('redirect', function(msg) {
      var url = (msg && msg.url) ? msg.url : (typeof msg === 'string' ? msg : null);
      if (url) window.location.href = url;
    });
  "))
)

ui <- function(req) {
  # 如果带有专门的鉴权标记头，则作为 Nginx auth_request 的子请求处理，只返回 200/401
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
      return(shiny::httpResponse(
        status = 200L,
        headers = list("X-User" = wid)
      ))
    } else {
      return(shiny::httpResponse(status = 401L))
    }
  }

  # 其他情况正常渲染登录 / 注册 / 修改密码页面
  base_ui
}

# ---------------- Server ----------------
server <- function(input, output, session) {
  # 数据库连接：使用全局连接池
  pg_con <- pg_pool

  # 界面状态：login / register / change_password
  mode <- reactiveVal("login")
  current_user <- reactiveVal(NULL)  # list(work_id=, name=)

  # 各界面 UI ----
  login_ui <- reactive({
    tagList(
      wellPanel(
        h3("登录"),
        textInput("login_work_id", "工号", value = ""),
        passwordInput("login_password", "密码", value = ""),
        actionButton("btn_login", "登录", class = "btn-primary"),
        tags$span(style = "margin-left: 10px;"),
        actionButton("btn_go_register", "新用户注册", class = "btn-link"),
        tags$span(style = "margin-left: 10px;"),
        actionButton("btn_go_change_pw", "修改密码", class = "btn-link"),
        tags$hr(),
        verbatimTextOutput("login_message")
      )
    )
  })

  register_ui <- reactive({
    tagList(
      wellPanel(
        h3("新用户注册"),
        textInput("reg_work_id", "工号", value = ""),
        passwordInput("reg_password", "密码", value = ""),
        passwordInput("reg_password_confirm", "确认密码", value = ""),
        actionButton("btn_register", "确定注册", class = "btn-success"),
        tags$span(style = "margin-left: 10px;"),
        actionButton("btn_back_login_from_reg", "返回登录", class = "btn-link"),
        tags$hr(),
        verbatimTextOutput("register_message")
      )
    )
  })

  change_pw_ui <- reactive({
    tagList(
      wellPanel(
        h3("修改密码"),
        textInput("chg_work_id", "工号", value = ""),
        passwordInput("chg_old_password", "原密码", value = ""),
        passwordInput("chg_new_password", "新密码", value = ""),
        passwordInput("chg_new_password_confirm", "确认新密码", value = ""),
        actionButton("btn_change_password", "确定修改", class = "btn-warning"),
        tags$span(style = "margin-left: 10px;"),
        actionButton("btn_back_login_from_chg", "返回登录", class = "btn-link"),
        tags$hr(),
        verbatimTextOutput("change_pw_message")
      )
    )
  })

  output$main_ui <- renderUI({
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      return(
        wellPanel(
          h3("数据库连接失败"),
          p("请检查 PG_HOST / PG_PORT / PG_DBNAME / PG_USER / PG_PASSWORD 等环境变量，或确保数据库服务已启动。")
        )
      )
    }
    switch(
      mode(),
      login = login_ui(),
      register = register_ui(),
      change_password = change_pw_ui(),
      login_ui()
    )
  })

  # 事件：界面切换 ----
  observeEvent(input$btn_go_register, {
    mode("register")
  })

  observeEvent(input$btn_go_change_pw, {
    mode("change_password")
  })

  observeEvent(input$btn_back_login_from_reg, {
    mode("login")
  })

  observeEvent(input$btn_back_login_from_chg, {
    mode("login")
  })

  # 登录逻辑 ----
  observeEvent(input$btn_login, {
    output$login_message <- renderText("")
    wid <- trimws(as.character(input$login_work_id %||% ""))
    pwd <- as.character(input$login_password %||% "")

    if (!nzchar(wid) || !nzchar(pwd)) {
      output$login_message <- renderText("请输入工号和密码。")
      return()
    }

    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      output$login_message <- renderText("数据库连接不可用，请联系管理员。")
      return()
    }

    u <- tryCatch(get_user_by_work_id(pg_con, wid), error = function(e) NULL)
    if (is.null(u) || nrow(u) == 0) {
      output$login_message <- renderText("未知帐号，请确认输入是否有误。")
      return()
    }

    hash_db <- as.character(u[["哈希密码"]][1])
    status  <- trimws(as.character(u[["人员状态"]][1]))

    if (is.na(hash_db) || !nzchar(hash_db)) {
      output$login_message <- renderText("新用户请先注册。")
      return()
    }

    if (!identical(status, "在职")) {
      output$login_message <- renderText("该用户已离职或无效，无法访问系统。")
      return()
    }

    hash_input <- hash_password(pwd)
    if (!identical(hash_input, hash_db)) {
      output$login_message <- renderText("帐号或密码错误。")
      return()
    }

    # 登录成功：写 Cookie 后跳转到主应用（Nginx 会通过 /auth-verify 校验 Cookie 并放行）
    current_user(list(
      work_id = wid,
      name = if ("姓名" %in% names(u)) as.character(u[["姓名"]][1]) else ""
    ))
    session$sendCustomMessage("set-cookie", list(name = "ivd_user", value = wid))
    session$sendCustomMessage("redirect", list(url = "/"))
  })

  # 注册逻辑 ----
  observeEvent(input$btn_register, {
    output$register_message <- renderText("")
    wid <- trimws(as.character(input$reg_work_id %||% ""))
    pwd <- as.character(input$reg_password %||% "")
    pwd2 <- as.character(input$reg_password_confirm %||% "")

    if (!nzchar(wid) || !nzchar(pwd) || !nzchar(pwd2)) {
      output$register_message <- renderText("请完整填写工号和两次密码。")
      return()
    }

    if (pwd != pwd2) {
      output$register_message <- renderText("两次输入的密码不一致。")
      return()
    }

    if (!is_valid_password(pwd)) {
      output$register_message <- renderText("密码至少6位，包含一个字母。")
      return()
    }

    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      output$register_message <- renderText("数据库连接不可用，请联系管理员。")
      return()
    }

    u <- tryCatch(get_user_by_work_id(pg_con, wid), error = function(e) NULL)
    if (is.null(u) || nrow(u) == 0) {
      output$register_message <- renderText("未知帐号，请确认输入是否有误。")
      return()
    }

    hash_db <- as.character(u[["哈希密码"]][1])
    if (!is.na(hash_db) && nzchar(hash_db)) {
      output$register_message <- renderText("帐号已存在，请直接登录。")
      return()
    }

    # 为新用户设置密码哈希
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
      output$register_message <- renderText(paste0("注册失败：", conditionMessage(e)))
      FALSE
    })

    if (ok) {
      output$register_message <- renderText("注册成功，请返回登录页面使用该工号登录。")
    }
  })

  # 修改密码逻辑 ----
  observeEvent(input$btn_change_password, {
    output$change_pw_message <- renderText("")
    wid <- trimws(as.character(input$chg_work_id %||% ""))
    old_pwd <- as.character(input$chg_old_password %||% "")
    new_pwd <- as.character(input$chg_new_password %||% "")
    new_pwd2 <- as.character(input$chg_new_password_confirm %||% "")

    if (!nzchar(wid) || !nzchar(old_pwd) || !nzchar(new_pwd) || !nzchar(new_pwd2)) {
      output$change_pw_message <- renderText("请完整填写工号、原密码和新密码。")
      return()
    }

    if (new_pwd != new_pwd2) {
      output$change_pw_message <- renderText("两次输入的新密码不一致。")
      return()
    }

    if (!is_valid_password(new_pwd)) {
      output$change_pw_message <- renderText("新密码至少6位，包含一个字母。")
      return()
    }

    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      output$change_pw_message <- renderText("数据库连接不可用，请联系管理员。")
      return()
    }

    u <- tryCatch(get_user_by_work_id(pg_con, wid), error = function(e) NULL)
    if (is.null(u) || nrow(u) == 0) {
      output$change_pw_message <- renderText("未知帐号，请确认输入是否有误。")
      return()
    }

    hash_db <- as.character(u[["哈希密码"]][1])
    status  <- trimws(as.character(u[["人员状态"]][1]))

    if (is.na(hash_db) || !nzchar(hash_db)) {
      output$change_pw_message <- renderText("新用户请先注册。")
      return()
    }

    if (!identical(status, "在职")) {
      output$change_pw_message <- renderText("该用户已离职或无效，无法修改密码。")
      return()
    }

    # 校验原密码
    if (!identical(hash_password(old_pwd), hash_db)) {
      output$change_pw_message <- renderText("原密码错误。")
      return()
    }

    # 写入新密码
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
      output$change_pw_message <- renderText(paste0("修改失败：", conditionMessage(e)))
      FALSE
    })

    if (ok) {
      output$change_pw_message <- renderText("密码修改成功，请使用新密码登录。")
    }
  })
}

shinyApp(ui = ui, server = server)

