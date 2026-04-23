# ivd_server_helpers.R — 每个 R 进程 source 一次；仅函数定义。
# 无会话级可变状态、无按用户累积的全局缓存；DB 连接一律由参数传入。

now_beijing_str <- function(fmt = "%Y-%m-%d %H:%M") {
  format(Sys.time(), fmt, tz = APP_TZ_CN)
}
today_beijing <- function() {
  as.Date(format(Sys.time(), "%Y-%m-%d", tz = APP_TZ_CN))
}
parse_datetime_beijing <- function(s) {
  s <- trimws(as.character(s %||% "")[1])
  if (!nzchar(s)) return(NULL)
  p <- suppressWarnings(tryCatch(
    as.POSIXct(s, format = "%Y-%m-%d %H:%M", tz = APP_TZ_CN),
    error = function(e) NA
  ))
  if (inherits(p, "POSIXct") && length(p) == 1L && !is.na(p)) return(p)
  p2 <- suppressWarnings(tryCatch(as.POSIXct(s, tz = APP_TZ_CN), error = function(e) NA))
  if (inherits(p2, "POSIXct") && length(p2) == 1L && !is.na(p2)) return(p2)
  NULL
}

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

# ---------- 会议决策辅助函数 ----------

# 获取项目的负责人+参与人，返回 c("姓名-工号" = "人员id", ...)
get_project_persons <- function(conn, project_id) {
  tryCatch({
    # 负责人
    mgr <- DBI::dbGetQuery(conn,
      'SELECT p.id, p."姓名", p."工号" FROM public."05人员表" p INNER JOIN public."04项目总表" g ON g."05人员表_id" = p.id WHERE g.id = $1 AND p."人员状态" = \'在职\'',
      params = list(as.integer(project_id)))
    # 参与人
    parts <- DBI::dbGetQuery(conn,
      'SELECT p.id, p."姓名", p."工号" FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."05人员表_id" = p.id WHERE m."04项目总表_id" = $1 AND p."人员状态" = \'在职\'',
      params = list(as.integer(project_id)))
    all_p <- unique(rbind(mgr, parts))
    if (nrow(all_p) == 0) return(character(0))
    labels <- paste0(all_p[["姓名"]], "-", all_p[["工号"]])
    setNames(as.character(all_p$id), labels)
  }, error = function(e) character(0))
}

# 根据人员ID列表构建初始执行人JSON
build_executor_json <- function(conn, person_ids) {
  if (length(person_ids) == 0) return("{}")
  tryCatch({
    ph <- paste0("$", seq_along(person_ids))
    q <- sprintf('SELECT id, "姓名", "工号" FROM public."05人员表" WHERE id IN (%s)',
                 paste(ph, collapse = ", "))
    df <- DBI::dbGetQuery(conn, q, params = as.list(as.integer(person_ids)))
    if (nrow(df) == 0) return("{}")
    keys <- paste0(df[["姓名"]], "-", df[["工号"]])
    result <- list()
    for (k in keys) result[[k]] <- list(状态 = "未执行", 说明 = "")
    jsonlite::toJSON(result, auto_unbox = TRUE)
  }, error = function(e) "{}")
}

merge_executor_json_for_edit <- function(conn, person_ids, old_json_text) {
  old_json_text <- as.character(old_json_text %||% "")[1]
  new_json_str <- build_executor_json(conn, person_ids)
  if (identical(trimws(new_json_str), "{}")) return(new_json_str)
  old_lst <- tryCatch(jsonlite::fromJSON(old_json_text, simplifyVector = FALSE), error = function(e) list())
  if (!is.list(old_lst) || length(old_lst) == 0L) return(new_json_str)
  new_lst <- tryCatch(jsonlite::fromJSON(new_json_str, simplifyVector = FALSE), error = function(e) return(old_lst))
  if (!is.list(new_lst) || length(new_lst) == 0L) return(new_json_str)
  for (k in names(new_lst)) {
    if (k %in% names(old_lst) && is.list(old_lst[[k]])) new_lst[[k]] <- old_lst[[k]]
  }
  jsonlite::toJSON(new_lst, auto_unbox = TRUE)
}

person_ids_from_executor_json <- function(conn, json_text) {
  edf <- parse_executor_json(json_text)
  if (nrow(edf) == 0L) return(NULL)
  keys <- unique(trimws(as.character(edf$key)))
  keys <- keys[nzchar(keys)]
  if (length(keys) == 0L) return(NULL)
  ids <- integer(0)
  tryCatch({
    ph <- DBI::dbGetQuery(conn, 'SELECT id, "姓名", "工号" FROM public."05人员表" WHERE "人员状态" = \'在职\'')
    if (is.null(ph) || nrow(ph) == 0L) return(NULL)
    labs <- paste0(trimws(as.character(ph[["姓名"]])), "-", trimws(as.character(ph[["工号"]])))
    for (k in keys) {
      hit <- which(labs == k)
      if (length(hit) >= 1L) ids <- c(ids, as.integer(ph$id[hit[1]]))
    }
  }, error = function(e) NULL)
  if (length(ids) == 0L) return(NULL)
  unique(ids)
}

# 解析执行人JSON为data.frame
parse_executor_json <- function(json_text) {
  json_text <- as.character(json_text)
  if (is.null(json_text) || length(json_text) == 0 || is.na(json_text) || !nzchar(trimws(json_text)) || trimws(json_text) == "null") {
    return(data.frame(key = character(0), 状态 = character(0), 说明 = character(0), stringsAsFactors = FALSE))
  }
  tryCatch({
    lst <- jsonlite::fromJSON(json_text, simplifyVector = FALSE)
    if (length(lst) == 0) return(data.frame(key = character(0), 状态 = character(0), 说明 = character(0), stringsAsFactors = FALSE))
    keys <- names(lst)
    statuses <- sapply(keys, function(k) lst[[k]][["状态"]] %||% "未执行")
    notes <- sapply(keys, function(k) lst[[k]][["说明"]] %||% "")
    data.frame(key = keys, 状态 = statuses, 说明 = notes, stringsAsFactors = FALSE)
  }, error = function(e) data.frame(key = character(0), 状态 = character(0), 说明 = character(0), stringsAsFactors = FALSE))
}

# 获取指定项目的参与人员+项目负责人（姓名-工号去重并集）
# 返回 named character vector：names = "姓名-工号", values = 05人员表.id
project_member_choices <- function(conn, project_ids) {
  pids <- unique(suppressWarnings(as.integer(project_ids)))
  pids <- pids[!is.na(pids)]
  if (length(pids) == 0L) return(character(0))
  tryCatch({
    ph <- DBI::dbGetQuery(conn, 'SELECT id, "姓名", "工号" FROM public."05人员表" WHERE "人员状态" = \'在职\' ORDER BY "姓名"')
    if (is.null(ph) || nrow(ph) == 0L) return(character(0))
    member_ids <- integer(0)
    for (pid in pids) {
      # 负责人
      mgr <- DBI::dbGetQuery(conn,
        'SELECT "05人员表_id" AS mid FROM public."04项目总表" WHERE id = $1',
        params = list(pid))
      if (nrow(mgr) > 0L && !is.na(mgr$mid[1L])) member_ids <- c(member_ids, as.integer(mgr$mid[1L]))
      # 参与人员
      parts <- DBI::dbGetQuery(conn,
        'SELECT "05人员表_id" AS mid FROM public."_nc_m2m_04项目总表_05人员表" WHERE "04项目总表_id" = $1',
        params = list(pid))
      if (nrow(parts) > 0L) member_ids <- c(member_ids, as.integer(parts$mid))
    }
    member_ids <- unique(member_ids)
    if (length(member_ids) == 0L) return(character(0))
    sub <- ph[ph$id %in% member_ids, , drop = FALSE]
    if (nrow(sub) == 0L) return(character(0))
    labs <- paste0(sub[["姓名"]], "-", sub[["工号"]])
    setNames(as.character(sub$id), labs)
  }, error = function(e) character(0))
}

# 执行人 JSON 键为「姓名-工号」：界面只展示姓名（工号仅用于库内防重名）
executor_display_name_from_key <- function(key) {
  k <- trimws(as.character(key %||% ""))
  if (!nzchar(k)) return("")
  parts <- strsplit(k, "-", fixed = TRUE)[[1]]
  if (length(parts) < 2L) return(k)
  paste(parts[-length(parts)], collapse = "-")
}

# 甘特/汇总/任务详情：按 11 表 id 批量拉取关联的会议决策与执行人 JSON
fetch_decisions_linked_to_feedback_ids <- function(conn, fb_ids) {
  fb_ids <- unique(suppressWarnings(as.integer(fb_ids)))
  fb_ids <- fb_ids[!is.na(fb_ids) & fb_ids > 0L]
  empty <- data.frame(
    decision_id = integer(0),
    meeting_label = character(0),
    decision_content = character(0),
    exec_json = character(0),
    stringsAsFactors = FALSE
  )
  if (is.null(conn) || !DBI::dbIsValid(conn) || length(fb_ids) == 0L) {
    return(structure(list(), names = character(0)))
  }
  df <- tryCatch({
    q <- sprintf(
      'SELECT l."11阶段问题反馈表_id" AS fid,
              t.id AS decision_id,
              (COALESCE(NULLIF(t."会议名称", \'\'), \'会议\') || \'（\' || COALESCE(to_char(t."会议时间", \'YYYY-MM-DD HH24:MI\'), \'\') || \'）\') AS meeting_lbl,
              COALESCE(t."决策内容", \'\') AS decision_content,
              t."决策执行人及执行确认"::text AS exec_json
       FROM public."12会议决策关联问题表" l
       INNER JOIN public."10会议决策表" t ON t.id = l."10会议决策表_id"
       WHERE l."11阶段问题反馈表_id" IN (%s)
       ORDER BY fid, t."会议时间" DESC NULLS LAST, t.id DESC',
      paste(fb_ids, collapse = ","))
    DBI::dbGetQuery(conn, q)
  }, error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0L) {
    out <- vector("list", length(fb_ids))
    names(out) <- as.character(fb_ids)
    for (nm in names(out)) out[[nm]] <- empty
    return(out)
  }
  by_fid <- split(df, df$fid)
  out <- vector("list", length(fb_ids))
  names(out) <- as.character(fb_ids)
  for (i in seq_along(fb_ids)) {
    key <- as.character(fb_ids[i])
    sub <- by_fid[[key]]
    if (is.null(sub) || nrow(sub) == 0L) {
      out[[i]] <- empty
    } else {
      out[[i]] <- data.frame(
        decision_id = as.integer(sub$decision_id),
        meeting_label = as.character(sub$meeting_lbl),
        decision_content = as.character(sub$decision_content),
        exec_json = as.character(sub$exec_json),
        stringsAsFactors = FALSE
      )
    }
  }
  out
}

# 与会议历史一致的执行人块；allow_exec_link=FALSE 时当前用户也不使用 actionLink（避免同页重复 decision id）
gantt_executor_block_tags <- function(auth, dec_id, executor_json, allow_exec_link = TRUE) {
  dec_id <- suppressWarnings(as.integer(dec_id))
  if (is.na(dec_id)) return(NULL)
  executor_json <- as.character(executor_json %||% "{}")
  exec_df <- parse_executor_json(executor_json)
  if (nrow(exec_df) == 0L) {
    return(tags$div(
      class = "meeting-exec-wrap",
      tags$span(style = "font-weight: 600; color: #424242;", "执行人："),
      tags$div(style = "font-size: 12px; color: #888; margin-top: 2px;", "（无执行人）")
    ))
  }
  exec_cells <- vector("list", nrow(exec_df))
  for (ei in seq_len(nrow(exec_df))) {
    ek <- exec_df$key[ei]
    es <- trimws(as.character(exec_df$状态[ei] %||% ""))
    en <- trimws(as.character(exec_df$说明[ei] %||% "")[1])
    dname <- executor_display_name_from_key(ek)
    is_done <- identical(es, "已执行")
    is_pending <- identical(es, "未执行")
    status_txt <- if (is_done) "已执行" else if (is_pending) "未执行" else es
    status_class <- if (is_done) "executor-status-done" else if (is_pending) "executor-status-pending" else ""
    title_text <- paste0(dname, "-", status_txt, "-", if (nzchar(en)) en else "")
    is_current_user <- isTRUE(allow_exec_link) && grepl(paste0("-", auth$work_id, "$"), ek)

    esc_name <- htmltools::htmlEscape(dname)
    esc_st <- htmltools::htmlEscape(status_txt)
    esc_note <- htmltools::htmlEscape(en)
    status_inner <- if (nzchar(status_class)) {
      sprintf("<span class=\"%s\">%s</span>", status_class, esc_st)
    } else {
      sprintf("<span style=\"color:#616161;\">%s</span>", esc_st)
    }
    note_inner <- if (nzchar(esc_note)) {
      sprintf("<span style=\"color:#555;\">%s</span>", esc_note)
    } else {
      "<span style=\"color:#bbb;\"></span>"
    }
    lab_html <- paste0(
      "<span style=\"color:#333;\">", esc_name, "</span>",
      "<span style=\"color:#616161;\">-</span>",
      status_inner,
      "<span style=\"color:#616161;\">-</span>",
      note_inner
    )

    person_unit <- if (is_current_user) {
      tags$a(
        id = paste0("exec_status_", dec_id),
        href = "javascript:void(0)",
        HTML(lab_html),
        class = "exec-status-link",
        style = "font-size: 13px; cursor: pointer; text-decoration: none; white-space: normal;",
        title = title_text
      )
    } else {
      tags$span(
        style = "font-size: 13px; cursor: default; line-height: 1.45; display: block;",
        title = title_text,
        tags$span(style = "color:#333;", dname),
        tags$span(style = "color:#616161;", "-"),
        if (nzchar(status_class)) {
          tags$span(class = status_class, status_txt)
        } else {
          tags$span(style = "color:#616161;", status_txt)
        },
        tags$span(style = "color:#616161;", "-"),
        if (nzchar(en)) tags$span(style = "color:#555;", en)
      )
    }
    exec_cells[[ei]] <- tags$div(class = "meeting-exec-cell", person_unit)
  }
  tags$div(
    class = "meeting-exec-wrap",
    tags$span(style = "font-weight: 600; color: #424242;", "执行人："),
    tags$div(class = "meeting-exec-grid", exec_cells)
  )
}

# 备注表增加展示列并排序（与项目汇总「项目要点」一致）；fixed_stage/fixed_site 用于任务详情单阶段视图
ensure_remark_display_columns <- function(remark_df, fixed_stage = NULL, fixed_site = NULL, fixed_task_key = NULL) {
  if (is.null(remark_df) || nrow(remark_df) == 0L) return(remark_df)
  if (!"fb_id" %in% names(remark_df)) remark_df$fb_id <- NA_integer_
  if (!"task_key_raw" %in% names(remark_df)) remark_df$task_key_raw <- rep(NA_character_, nrow(remark_df))
  if (!is.null(fixed_stage)) {
    sl <- as.character(fixed_stage)[1]
    remark_df$stage_label <- sl
  }
  if (!is.null(fixed_site)) {
    remark_df$site_name <- as.character(fixed_site)[1]
  }
  if (!is.null(fixed_task_key)) {
    remark_df$task_key_raw <- rep(as.character(fixed_task_key)[1], nrow(remark_df))
  }
  if (!"stage_label" %in% names(remark_df)) remark_df$stage_label <- ""
  if (!"site_name" %in% names(remark_df)) remark_df$site_name <- ""
  remark_df$sort_date <- vapply(remark_df$updated_at, function(u) {
    d <- parse_update_date_for_display(u)
    if (is.na(d)) as.Date("1970-01-01") else d
  }, as.Date(1))
  remark_df$typ_norm <- trimws(as.character(remark_df$type %||% ""))
  remark_df$typ_norm[!nzchar(remark_df$typ_norm)] <- NA_character_
  remark_df$type_display <- ifelse(is.na(remark_df$typ_norm), "（无类型）", remark_df$typ_norm)
  std_types <- c("卡点", "问题", "经验分享")
  all_disp <- unique(remark_df$type_display)
  custom_sorted <- sort(setdiff(all_disp, c(std_types, "（无类型）")))
  type_rank_map <- list()
  type_rank_map[["卡点"]] <- 1L
  type_rank_map[["问题"]] <- 2L
  type_rank_map[["经验分享"]] <- 3L
  rk <- 4L
  for (nm in custom_sorted) {
    type_rank_map[[nm]] <- rk
    rk <- rk + 1L
  }
  type_rank_map[["（无类型）"]] <- 99999L
  remark_df$type_rank <- vapply(remark_df$type_display, function(x) {
    v <- type_rank_map[[x]]
    if (is.null(v)) 5000L else as.integer(v)
  }, integer(1))
  remark_df %>% dplyr::arrange(.data$type_rank, desc(.data$sort_date), desc(.data$reporter))
}

# 项目汇总 / 任务详情：按类型分组展示要点 + 关联会议决策与执行人
ui_gantt_feedback_with_decisions <- function(auth, conn, remark_df, dec_by_fid) {
  if (is.null(remark_df) || nrow(remark_df) == 0L) {
    return(tags$span("（暂无）", style = "color:#999;"))
  }
  if (!"fb_id" %in% names(remark_df)) remark_df$fb_id <- NA_integer_
  if (!"task_key_raw" %in% names(remark_df)) remark_df$task_key_raw <- rep(NA_character_, nrow(remark_df))
  seen_dec <- integer(0)
  type_order_vec <- remark_df %>%
    dplyr::distinct(.data$type_display, .data$type_rank) %>%
    dplyr::arrange(.data$type_rank) %>%
    dplyr::pull(.data$type_display)
  tagList(lapply(type_order_vec, function(td) {
    sub <- remark_df %>% dplyr::filter(.data$type_display == td)
    hdr_col <- if (identical(td, "（无类型）")) "#616161" else remark_type_color(td)
    tags$div(
      style = "margin-bottom: 18px;",
      tags$div(
        style = paste0(
          "font-weight: 700; font-size: 14px; margin-bottom: 8px; padding-bottom: 4px; border-bottom: 1px solid #e0e0e0; color: ",
          hdr_col, ";"
        ),
        td
      ),
      tagList(lapply(seq_len(nrow(sub)), function(i) {
        pt_idx <- i
        fid <- suppressWarnings(as.integer(sub$fb_id[i]))
        dec_df <- if (!is.na(fid)) dec_by_fid[[as.character(fid)]] else NULL
        if (is.null(dec_df) || nrow(dec_df) == 0L) {
          dec_df <- data.frame(
            decision_id = integer(0),
            meeting_label = character(0),
            decision_content = character(0),
            exec_json = character(0),
            stringsAsFactors = FALSE
          )
        }
        tags$div(
          style = "margin-bottom: 14px; white-space: normal; word-break: break-word;",
          tags$div(
            style = "margin-bottom: 4px;",
            tags$span(style = "font-weight: 400; color: #333;", tagList({
              typ <- as.character(sub$type[i] %||% "")
              typ <- trimws(typ)
              in_bracket <- if (nzchar(typ)) paste0(typ, pt_idx) else paste0("要点", pt_idx)
              br_col <- if (nzchar(typ)) remark_type_color(typ) else "#616161"
              tagList(
                "\u3010",
                tags$span(
                  style = paste0("color: ", br_col, "; font-weight: 600;"),
                  in_bracket
                ),
                "\u3011 ",
                if (nzchar(as.character(sub$updated_at[i] %||% ""))) {
                  tags$span(style = remark_date_style(sub$updated_at[i]), paste0(sub$updated_at[i], " "))
                } else NULL,
                if (nzchar(as.character(sub$reporter[i] %||% ""))) {
                  paste0(as.character(sub$reporter[i]), " ")
                } else NULL,
                as.character(sub$content[i] %||% ""),
                " ",
                tags$span(
                  style = "color:#78909c;",
                  feedback_location_paren(
                    sub$site_name[i], sub$stage_label[i],
                    sub$task_key_raw[i]
                  )
                )
              )
            }))
          ),
          if (nrow(dec_df) > 0L) {
            tagList(lapply(seq_len(nrow(dec_df)), function(k) {
              did <- suppressWarnings(as.integer(dec_df$decision_id[k]))
              content <- as.character(dec_df$decision_content[k] %||% "")
              mlbl <- as.character(dec_df$meeting_label[k] %||% "")
              ej <- as.character(dec_df$exec_json[k] %||% "{}")
              allow_link <- !is.na(did) && !(did %in% seen_dec)
              if (allow_link) seen_dec <<- c(seen_dec, did)
              tags$div(
                style = "margin-top: 8px; margin-left: 4px; padding-left: 10px; border-left: 2px solid #e0e0e0;",
                tags$div(
                  style = "font-size: 13px; color: #37474f;",
                  tags$span(style = "font-weight: 600;", sprintf("决策%d：", k)),
                  tags$span(content)
                ),
                if (nzchar(mlbl)) {
                  tags$div(style = "font-size: 12px; color: #78909c; margin-top: 2px;", mlbl)
                } else NULL,
                gantt_executor_block_tags(auth, did, ej, allow_exec_link = allow_link)
              )
            }))
          } else NULL
        )
      }))
    )
  }))
}

# 获取会议决策的下一个id
meeting_next_id <- function(conn) {
  as.integer(DBI::dbGetQuery(conn, 'SELECT COALESCE(MAX(id), 0) + 1 AS nid FROM public."10会议决策表"')$nid[1])
}

empty_contrib_df <- function() {
  data.frame(
    entry_key = character(0),
    person = character(0),
    role = character(0),
    work = character(0),
    amount = character(0),
    note = character(0),
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
    fb_id = integer(0),
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

make_entry_key <- function(conn, actor = NULL, existing_keys = character(0), prefix = "json") {
  repeat {
    key <- generate_db_unique_key(conn, prefix = prefix)
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
      entry_key <- make_entry_key(pg_pool, actor, existing_keys = c(existing_keys, names(out)), prefix = "sample")
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
      note <- if (!is.null(val$note)) normalize_text(val$note, empty_as_na = FALSE) else ""
    } else {
      pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
      if (is_legacy_name_time_key(key) && length(pieces) < 4L) pieces <- c(key, pieces)
      while (length(pieces) < 4L) pieces <- c(pieces, "")
      person <- pieces[1]
      role <- pieces[2]
      work <- pieces[3]
      amount <- pieces[4]
      note <- ""
    }
    data.frame(
      entry_key = key,
      person = person,
      role = role,
      work = work,
      amount = amount,
      note = note,
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
    note <- if ("note" %in% names(df)) trimws(as.character(df$note[i] %||% "")) else ""
    entry_key <- trimws(as.character(df$entry_key[i] %||% ""))
    if (!nzchar(entry_key)) entry_key <- make_entry_key(pg_pool, actor, existing_keys = c(existing_keys, names(out)), prefix = "contrib")
    existing_keys <- c(existing_keys, entry_key)
    entry_val <- list(person = person, role = role, work = work, amount = amount)
    if (nzchar(note)) entry_val$note <- note
    out[[entry_key]] <- entry_val
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
      fb_id = NA_integer_,
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
      fb_id = NA_integer_,
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
    if (!nzchar(entry_key)) entry_key <- make_entry_key(pg_pool, actor, existing_keys = c(existing_keys, names(out)), prefix = "remark")
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
  } else if (is.list(val) && "登记人" %in% names(val)) {
    x <- val[["登记人"]]
    rr <- if (is.atomic(x) && length(x) <= 1L) normalize_text(x, empty_as_na = FALSE) else unlist(x, use.names = FALSE)
  } else if (is.list(val) && "反馈人" %in% names(val)) {
    x <- val[["反馈人"]]
    rr <- if (is.atomic(x) && length(x) <= 1L) normalize_text(x, empty_as_na = FALSE) else unlist(x, use.names = FALSE)
  } else if (is.list(val) && "填报人" %in% names(val)) {
    x <- val[["填报人"]]
    rr <- if (is.atomic(x) && length(x) <= 1L) normalize_text(x, empty_as_na = FALSE) else unlist(x, use.names = FALSE)
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
  format(today_beijing(), "%Y/%m/%d")
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
  age_days <- as.numeric(today_beijing() - d)
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

# ---------- 11阶段问题反馈表：从 remark_json 迁出后的读写 ----------
sort_feedback_map <- function(m) {
  m <- m %||% list()
  ks <- sort(names(m))
  if (length(ks) == 0L) return(list())
  m[ks]
}

fetch_11_feedback_map <- function(conn, stage_instance_id) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(list())
  sid <- suppressWarnings(as.integer(stage_instance_id))
  if (is.na(sid)) return(list())
  df <- tryCatch(
    DBI::dbGetQuery(conn,
      'SELECT "条目键", "类型", "内容", "更新日期", reporters_json::text AS rj
       FROM public."11阶段问题反馈表" WHERE "09项目阶段实例表_id" = $1 ORDER BY "条目键"',
      params = list(sid)),
    error = function(e) data.frame()
  )
  if (is.null(df) || nrow(df) == 0L) return(list())
  out <- list()
  for (i in seq_len(nrow(df))) {
    key <- as.character(df[["条目键"]][i])
    if (!nzchar(key)) next
    rj <- df[["rj"]][i]
    reps <- tryCatch({
      jj <- jsonlite::fromJSON(rj, simplifyVector = TRUE)
      if (is.null(jj)) character(0) else as.character(jj)
    }, error = function(e) character(0))
    reps <- unique(trimws(reps[nzchar(trimws(reps))]))
    out[[key]] <- build_remark_value(reps, df[["类型"]][i], df[["内容"]][i], df[["更新日期"]][i])
  }
  sort_feedback_map(out)
}

fetch_11_feedback_df <- function(conn, stage_instance_id) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(empty_remark_df())
  sid <- suppressWarnings(as.integer(stage_instance_id))
  if (is.na(sid)) return(empty_remark_df())
  df <- tryCatch(
    DBI::dbGetQuery(conn,
      'SELECT id AS fb_row_id, "条目键", "类型", "内容", "更新日期", reporters_json::text AS rj
       FROM public."11阶段问题反馈表" WHERE "09项目阶段实例表_id" = $1 ORDER BY id',
      params = list(sid)),
    error = function(e) data.frame()
  )
  if (is.null(df) || nrow(df) == 0L) return(empty_remark_df())
  rows <- lapply(seq_len(nrow(df)), function(i) {
    rj <- df[["rj"]][i]
    reps <- tryCatch({
      jj <- jsonlite::fromJSON(rj, simplifyVector = TRUE)
      if (is.null(jj)) character(0) else as.character(jj)
    }, error = function(e) character(0))
    reps <- unique(trimws(reps[nzchar(trimws(reps))]))
    reporter <- paste(reps, collapse = "、")
    data.frame(
      entry_key = as.character(df[["条目键"]][i]),
      reporter = reporter,
      updated_at = as.character(df[["更新日期"]][i] %||% ""),
      type = as.character(df[["类型"]][i] %||% ""),
      content = as.character(df[["内容"]][i] %||% ""),
      fb_id = suppressWarnings(as.integer(df[["fb_row_id"]][i])),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

apply_merged_feedback_to_11 <- function(conn, stage_instance_id, project_id, merged_named_list, actor_work_id, actor_name) {
  sid <- suppressWarnings(as.integer(stage_instance_id))
  pid <- suppressWarnings(as.integer(project_id))
  if (is.na(sid) || is.na(pid)) return(invisible(NULL))
  merged_named_list <- merged_named_list %||% list()
  db_keys <- tryCatch({
    r <- DBI::dbGetQuery(conn,
      'SELECT "条目键" FROM public."11阶段问题反馈表" WHERE "09项目阶段实例表_id" = $1',
      params = list(sid))
    if (is.null(r) || nrow(r) == 0L) character(0) else as.character(r[[1]])
  }, error = function(e) character(0))
  merged_keys <- names(merged_named_list)
  to_del <- setdiff(db_keys, merged_keys)
  for (k in to_del) {
    DBI::dbExecute(conn,
      'DELETE FROM public."11阶段问题反馈表" WHERE "09项目阶段实例表_id" = $1 AND "条目键" = $2',
      params = list(sid, k))
  }
  aw <- as.character(actor_work_id %||% "")[1]
  an <- as.character(actor_name %||% "")[1]
  for (key in merged_keys) {
    val <- merged_named_list[[key]]
    if (is.null(val)) next
    core <- remark_core_state(val)
    rj <- jsonlite::toJSON(as.list(unique(core$reporters[nzchar(core$reporters)])), auto_unbox = TRUE)
    DBI::dbExecute(conn,
      'INSERT INTO public."11阶段问题反馈表" (
        "09项目阶段实例表_id", "04项目总表_id", "条目键", entry_key_legacy,
        "类型", "内容", reporters_json, "更新日期",
        updated_at, updated_by_work_id, updated_by_name
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7::jsonb, $8, CURRENT_TIMESTAMP, $9, $10
      )
      ON CONFLICT ("09项目阶段实例表_id", "条目键") WHERE ("09项目阶段实例表_id" IS NOT NULL)
      DO UPDATE SET
        "类型" = EXCLUDED."类型",
        "内容" = EXCLUDED."内容",
        reporters_json = EXCLUDED.reporters_json,
        "更新日期" = EXCLUDED."更新日期",
        updated_at = EXCLUDED.updated_at,
        updated_by_work_id = EXCLUDED.updated_by_work_id,
        updated_by_name = EXCLUDED.updated_by_name',
      params = list(sid, pid, key, key, core$type, core$content, as.character(rj), core$updated_at, aw, an))
  }
  invisible(NULL)
}

# 项目级要点：09 为 NULL，仅 04；需已执行 migrations/002
insert_project_scope_feedback_11 <- function(conn, project_id, entry_key, type_, content_, reporters_chr, updated_at_str, actor_work_id, actor_name) {
  pid <- suppressWarnings(as.integer(project_id))
  if (is.na(pid)) return(invisible(NULL))
  ek <- trimws(as.character(entry_key %||% "")[1])
  if (!nzchar(ek)) return(invisible(NULL))
  typ <- trimws(as.character(type_ %||% "")[1])
  if (!nzchar(typ)) typ <- "问题/卡点/经验分享"
  cont <- as.character(content_ %||% "")[1]
  reps <- unique(trimws(as.character(reporters_chr %||% character(0))))
  reps <- reps[nzchar(reps)]
  rj <- jsonlite::toJSON(as.list(reps), auto_unbox = TRUE)
  upd <- trimws(as.character(updated_at_str %||% "")[1])
  if (!nzchar(upd)) upd <- now_beijing_str("%Y-%m-%d %H:%M")
  aw <- as.character(actor_work_id %||% "")[1]
  an <- as.character(actor_name %||% "")[1]
  tryCatch({
    DBI::dbExecute(conn,
      'INSERT INTO public."11阶段问题反馈表" (
        "09项目阶段实例表_id", "04项目总表_id", "条目键", entry_key_legacy,
        "类型", "内容", reporters_json, "更新日期",
        updated_at, updated_by_work_id, updated_by_name
      ) VALUES (
        NULL, $1, $2, $2, $3, $4, $5::jsonb, $6, CURRENT_TIMESTAMP, $7, $8
      )
      ON CONFLICT ("04项目总表_id", "条目键") WHERE ("09项目阶段实例表_id" IS NULL)
      DO UPDATE SET
        "类型" = EXCLUDED."类型",
        "内容" = EXCLUDED."内容",
        reporters_json = EXCLUDED.reporters_json,
        "更新日期" = EXCLUDED."更新日期",
        updated_at = EXCLUDED.updated_at,
        updated_by_work_id = EXCLUDED.updated_by_work_id,
        updated_by_name = EXCLUDED.updated_by_name',
      params = list(pid, ek, typ, cont, as.character(rj), upd, aw, an))
  }, error = function(e) {
    warning(conditionMessage(e))
    invisible(NULL)
  })
  invisible(NULL)
}

# 获取项目级要点（09 IS NULL）列表
fetch_project_scope_feedback_11 <- function(conn, project_id) {
  pid <- suppressWarnings(as.integer(project_id))
  if (is.null(conn) || !DBI::dbIsValid(conn) || is.na(pid)) return(empty_remark_df())
  df <- tryCatch(
    DBI::dbGetQuery(conn,
      'SELECT id AS fb_row_id, "条目键", "类型", "内容", "更新日期", reporters_json::text AS rj
       FROM public."11阶段问题反馈表"
       WHERE "04项目总表_id" = $1 AND "09项目阶段实例表_id" IS NULL
       ORDER BY id',
      params = list(pid)),
    error = function(e) data.frame()
  )
  if (is.null(df) || nrow(df) == 0L) return(empty_remark_df())
  rows <- lapply(seq_len(nrow(df)), function(i) {
    rj <- df[["rj"]][i]
    reps <- tryCatch({
      jj <- jsonlite::fromJSON(rj, simplifyVector = TRUE)
      if (is.null(jj)) character(0) else as.character(jj)
    }, error = function(e) character(0))
    reps <- unique(trimws(reps[nzchar(trimws(reps))]))
    reporter <- paste(reps, collapse = "、")
    data.frame(
      entry_key = as.character(df[["条目键"]][i]),
      reporter = reporter,
      updated_at = as.character(df[["更新日期"]][i] %||% ""),
      type = as.character(df[["类型"]][i] %||% ""),
      content = as.character(df[["内容"]][i] %||% ""),
      fb_id = suppressWarnings(as.integer(df[["fb_row_id"]][i])),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

# 合并项目级要点（upsert + delete）
apply_merged_project_scope_feedback_11 <- function(conn, project_id, merged_named_list, actor_work_id, actor_name) {
  pid <- suppressWarnings(as.integer(project_id))
  if (is.na(pid)) return(invisible(NULL))
  merged_named_list <- merged_named_list %||% list()
  db_keys <- tryCatch({
    r <- DBI::dbGetQuery(conn,
      'SELECT "条目键" FROM public."11阶段问题反馈表" WHERE "04项目总表_id" = $1 AND "09项目阶段实例表_id" IS NULL',
      params = list(pid))
    if (is.null(r) || nrow(r) == 0L) character(0) else as.character(r[[1]])
  }, error = function(e) character(0))
  merged_keys <- names(merged_named_list)
  to_del <- setdiff(db_keys, merged_keys)
  for (k in to_del) {
    DBI::dbExecute(conn,
      'DELETE FROM public."11阶段问题反馈表" WHERE "04项目总表_id" = $1 AND "09项目阶段实例表_id" IS NULL AND "条目键" = $2',
      params = list(pid, k))
  }
  aw <- as.character(actor_work_id %||% "")[1]
  an <- as.character(actor_name %||% "")[1]
  for (key in merged_keys) {
    val <- merged_named_list[[key]]
    if (is.null(val)) next
    core <- remark_core_state(val)
    rj <- jsonlite::toJSON(as.list(unique(core$reporters[nzchar(core$reporters)])), auto_unbox = TRUE)
    DBI::dbExecute(conn,
      'INSERT INTO public."11阶段问题反馈表" (
        "09项目阶段实例表_id", "04项目总表_id", "条目键", entry_key_legacy,
        "类型", "内容", reporters_json, "更新日期",
        updated_at, updated_by_work_id, updated_by_name
      ) VALUES (
        NULL, $1, $2, $2, $3, $4, $5::jsonb, $6, CURRENT_TIMESTAMP, $7, $8
      )
      ON CONFLICT ("04项目总表_id", "条目键") WHERE ("09项目阶段实例表_id" IS NULL)
      DO UPDATE SET
        "类型" = EXCLUDED."类型",
        "内容" = EXCLUDED."内容",
        reporters_json = EXCLUDED.reporters_json,
        "更新日期" = EXCLUDED."更新日期",
        updated_at = EXCLUDED.updated_at,
        updated_by_work_id = EXCLUDED.updated_by_work_id,
        updated_by_name = EXCLUDED.updated_by_name',
      params = list(pid, key, core$type, core$content, as.character(rj), core$updated_at, aw, an))
  }
  invisible(NULL)
}

# 某项目下全部 11 行（含阶段内 + 项目级），供会议「项目要点」弹窗与展示
fetch_11_feedback_all_for_project_df <- function(conn, project_id) {
  pid <- suppressWarnings(as.integer(project_id))
  if (is.null(conn) || !DBI::dbIsValid(conn) || is.na(pid)) {
    return(data.frame(
      id = integer(0), fb_type = character(0), fb_content = character(0),
      fb_updated = character(0), fb_rj = character(0),
      task_key_raw = character(0), site_name = character(0),
      fb_created = as.POSIXct(character(0)),
      task_display_name = character(0),
      stringsAsFactors = FALSE
    ))
  }
  tryCatch({
    DBI::dbGetQuery(conn,
      'SELECT f.id,
              f."类型" AS fb_type,
              f."内容" AS fb_content,
              f."更新日期" AS fb_updated,
              f.reporters_json::text AS fb_rj,
              f.created_at AS fb_created,
              CASE WHEN f."09项目阶段实例表_id" IS NULL THEN \'__project_scope__\' ELSE d.stage_key END AS task_key_raw,
              CASE WHEN f."09项目阶段实例表_id" IS NULL THEN \'（不分中心/阶段）\'
                   WHEN d.stage_scope = \'sync\' THEN \'所有中心（同步）\'
                   ELSE COALESCE(NULLIF(h."医院名称", \'\'), \'中心-\' || COALESCE(s.id, 0)::text) END AS site_name,
              CASE WHEN f."09项目阶段实例表_id" IS NULL THEN NULL
                   ELSE COALESCE(si."阶段实例自定义名称", d.stage_name) END AS task_display_name
       FROM public."11阶段问题反馈表" f
       LEFT JOIN public."09项目阶段实例表" si ON si.id = f."09项目阶段实例表_id"
       LEFT JOIN public."08项目阶段定义表" d ON d.id = si.stage_def_id
       LEFT JOIN public."03医院_项目表" s ON s.id = si.site_project_id
       LEFT JOIN public."01医院信息表" h ON h.id = s."01_hos_resource_table医院信息表_id"
       WHERE f."04项目总表_id" = $1
       ORDER BY (f."09项目阶段实例表_id" IS NULL) DESC, d.stage_order NULLS LAST, si.id NULLS LAST, f.id',
      params = list(pid))
  }, error = function(e) {
    data.frame(
      id = integer(0), fb_type = character(0), fb_content = character(0),
      fb_updated = character(0), fb_rj = character(0),
      task_key_raw = character(0), site_name = character(0),
      fb_created = as.POSIXct(character(0)),
      task_display_name = character(0),
      stringsAsFactors = FALSE
    )
  })
}

# 将 fetch_11_feedback_all_for_project_df 结果转为与「项目汇总-项目要点」类似的排序与展示字段
feedback_all_proj_df_to_display <- function(fb_df) {
  empty <- data.frame(
    id = integer(0), type_raw = character(0), type_display = character(0), type_rank = integer(0),
    sort_date = as.Date(character(0)), updated_at = character(0), reporter = character(0), content = character(0),
    stage_label = character(0), site_name = character(0), task_key_raw = character(0), choice_label = character(0),
    stringsAsFactors = FALSE
  )
  if (is.null(fb_df) || nrow(fb_df) == 0L) return(empty)
  rows <- list()
  for (j in seq_len(nrow(fb_df))) {
    tk <- as.character(fb_df$task_key_raw[j])
    sn <- as.character(fb_df$site_name[j])
    rj <- fb_df$fb_rj[j]
    reps <- tryCatch({
      jj <- jsonlite::fromJSON(rj, simplifyVector = TRUE)
      if (is.null(jj)) character(0) else as.character(jj)
    }, error = function(e) character(0))
    reps <- unique(trimws(reps[nzchar(trimws(reps))]))
    reporter <- paste(reps, collapse = "、")
    st_lab <- if (identical(tk, "__project_scope__")) "项目要点" else {
      dn_col <- if ("task_display_name" %in% names(fb_df)) as.character(fb_df$task_display_name[j]) else NA_character_
      if (!is.na(dn_col) && nzchar(dn_col)) dn_col
      else if (!is.null(stage_label_fn) && is.function(stage_label_fn)) stage_label_fn(tk)
      else sub("^S\\d+_?", "", as.character(tk %||% ""))
    }
    typ <- trimws(as.character(fb_df$fb_type[j] %||% ""))
    cont <- as.character(fb_df$fb_content[j] %||% "")
    uid <- suppressWarnings(as.integer(fb_df$id[j]))
    if (is.na(uid)) next
    rows[[length(rows) + 1L]] <- data.frame(
      id = uid,
      type_raw = typ,
      updated_at = as.character(fb_df$fb_updated[j] %||% ""),
      reporter = reporter,
      content = cont,
      stage_label = st_lab,
      site_name = sn,
      task_key_raw = tk,
      stringsAsFactors = FALSE
    )
  }
  if (length(rows) == 0L) return(empty)
  remark_df <- bind_rows(rows)
  remark_df$sort_date <- vapply(remark_df$updated_at, function(u) {
    d <- parse_update_date_for_display(u)
    if (is.na(d)) as.Date("1970-01-01") else d
  }, as.Date(1))
  remark_df$typ_norm <- trimws(as.character(remark_df$type_raw %||% ""))
  remark_df$typ_norm[!nzchar(remark_df$typ_norm)] <- NA_character_
  remark_df$type_display <- ifelse(is.na(remark_df$typ_norm), "（无类型）", remark_df$typ_norm)
  std_types <- c("卡点", "问题", "经验分享")
  all_disp <- unique(remark_df$type_display)
  custom_sorted <- sort(setdiff(all_disp, c(std_types, "（无类型）")))
  type_rank_map <- list()
  type_rank_map[["卡点"]] <- 1L
  type_rank_map[["问题"]] <- 2L
  type_rank_map[["经验分享"]] <- 3L
  rk <- 4L
  for (nm in custom_sorted) {
    type_rank_map[[nm]] <- rk
    rk <- rk + 1L
  }
  type_rank_map[["（无类型）"]] <- 99999L
  remark_df$type_rank <- vapply(remark_df$type_display, function(x) {
    v <- type_rank_map[[x]]
    if (is.null(v)) 5000L else as.integer(v)
  }, integer(1))
  remark_df <- remark_df %>%
    arrange(.data$type_rank, desc(.data$sort_date), desc(.data$reporter))
  remark_df$choice_label <- vapply(seq_len(nrow(remark_df)), function(i) {
    cs <- substr(remark_df$content[i], 1, 80)
    loc <- feedback_location_paren(remark_df$site_name[i], remark_df$stage_label[i], remark_df$task_key_raw[i])
    tp <- as.character(remark_df$type_display[i])
    ua <- trimws(as.character(remark_df$updated_at[i] %||% ""))
    rp <- trimws(as.character(remark_df$reporter[i] %||% ""))
    parts <- c(sprintf("\u3010%s\u3011", tp), if (nzchar(ua)) ua else NULL, if (nzchar(rp)) rp else NULL, cs, loc)
    paste(parts, collapse = " ")
  }, character(1))
  remark_df
}

# 新建会议主列表：按登记时间（created_at）降序，扁平一行一条（不分类型版块）
meeting_new_flat_feedback_rows <- function(fb_df, stage_label_fn = NULL) {
  empty <- data.frame(
    id = integer(0), stage_label = character(0), site_name = character(0),
    task_key_raw = character(0),
    typ_show = character(0), type_display = character(0), updated_at = character(0),
    reporter = character(0), content = character(0), stringsAsFactors = FALSE
  )
  if (is.null(fb_df) || nrow(fb_df) == 0L) return(empty)
  if (!"fb_created" %in% names(fb_df)) fb_df$fb_created <- as.POSIXct(NA)
  fb_df <- fb_df %>% arrange(desc(.data$fb_created), desc(.data$id))
  rows <- list()
  for (j in seq_len(nrow(fb_df))) {
    tk <- as.character(fb_df$task_key_raw[j])
    sn <- as.character(fb_df$site_name[j])
    rj <- fb_df$fb_rj[j]
    reps <- tryCatch({
      jj <- jsonlite::fromJSON(rj, simplifyVector = TRUE)
      if (is.null(jj)) character(0) else as.character(jj)
    }, error = function(e) character(0))
    reps <- unique(trimws(reps[nzchar(trimws(reps))]))
    reporter <- paste(reps, collapse = "、")
    if (!nzchar(reporter)) reporter <- "（无）"
    st_lab <- if (identical(tk, "__project_scope__")) "项目要点" else {
      dn_col <- if ("task_display_name" %in% names(fb_df)) as.character(fb_df$task_display_name[j]) else NA_character_
      if (!is.na(dn_col) && nzchar(dn_col)) dn_col
      else if (!is.null(stage_label_fn) && is.function(stage_label_fn)) stage_label_fn(tk)
      else sub("^S\\d+_?", "", as.character(tk %||% ""))
    }
    typ <- trimws(as.character(fb_df$fb_type[j] %||% ""))
    cont <- as.character(fb_df$fb_content[j] %||% "")
    uid <- suppressWarnings(as.integer(fb_df$id[j]))
    if (is.na(uid)) next
    typ_show <- if (nzchar(typ)) typ else "（无类型）"
    rows[[length(rows) + 1L]] <- data.frame(
      id = uid,
      stage_label = st_lab,
      site_name = sn,
      task_key_raw = tk,
      typ_show = typ_show,
      type_display = typ_show,
      updated_at = as.character(fb_df$fb_updated[j] %||% ""),
      reporter = reporter,
      content = cont,
      stringsAsFactors = FALSE
    )
  }
  if (length(rows) == 0L) return(empty)
  bind_rows(rows)
}

meeting_new_pt_line_tags <- function(stage_lab, site_lab, typ_show, upd_at, reporter, content, task_key_raw = NULL) {
  loc <- feedback_location_paren(site_lab, stage_lab, task_key_raw)
  tagList(
    if (nzchar(typ_show) && !identical(typ_show, "（无类型）")) {
      tagList(
        "\u3010",
        tags$span(style = paste0("color: ", remark_type_color(typ_show), "; font-weight: 600;"), typ_show),
        "\u3011 "
      )
    } else NULL,
    if (nzchar(trimws(as.character(upd_at %||% "")))) {
      tags$span(style = remark_date_style(upd_at), paste0(trimws(as.character(upd_at)), " "))
    } else NULL,
    if (nzchar(trimws(as.character(reporter %||% ""))) && !identical(trimws(as.character(reporter)), "（无）")) {
      tags$span(style = "color:#333;", paste0(trimws(as.character(reporter)), " "))
    } else NULL,
    tags$span(class = "meeting-pt-txt", style = "color:#37474f;font-weight:500;", as.character(content %||% "")),
    " ",
    tags$span(class = "meeting-pt-meta", style = "color:#78909c;", loc)
  )
}

meeting_new_build_project_choices <- function(conn, auth) {
  project_choices <- c("共性决策" = "__common__")
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(project_choices)
  if (is.null(auth) || isTRUE(auth$allow_none)) return(project_choices)
  tryCatch({
    and_auth <- if (auth$allow_all) "" else paste0(' AND id IN (', auth$allowed_subquery, ')')
    pq <- paste0('SELECT id, "项目名称" FROM public."04项目总表" WHERE "项目名称" IS NOT NULL', and_auth, ' ORDER BY "项目名称"')
    pdf <- DBI::dbGetQuery(conn, pq)
    if (nrow(pdf) > 0) project_choices <- c(project_choices, setNames(as.character(pdf$id), pdf[["项目名称"]]))
  }, error = function(e) {})
  project_choices
}

#' 与甘特侧栏相同的维度 distinct 选项（按 04 与权限裁剪）
fetch_gantt_dim_filter_options <- function(conn, auth) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(NULL)
  if (is.null(auth) || isTRUE(auth$allow_none)) {
    return(list(
      types = character(0), names = character(0), managers = character(0),
      participants = character(0), importance = character(0), hospitals = character(0),
      research_groups = character(0)
    ))
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
    research_groups <- character(0)
    t1 <- DBI::dbGetQuery(conn, paste0('SELECT DISTINCT "项目类型" AS v FROM public."04项目总表" WHERE "项目类型" IS NOT NULL', and_04, " ORDER BY 1"))
    if (nrow(t1) > 0) types <- as.character(t1$v)
    t2 <- DBI::dbGetQuery(conn, paste0('SELECT DISTINCT "项目名称" AS v FROM public."04项目总表" WHERE "项目名称" IS NOT NULL', and_04, " ORDER BY 1"))
    if (nrow(t2) > 0) names_ <- as.character(t2$v)
    t3 <- DBI::dbGetQuery(conn, paste0('SELECT DISTINCT p."姓名" AS v FROM public."05人员表" p INNER JOIN public."04项目总表" g ON g."05人员表_id" = p.id WHERE g."05人员表_id" IS NOT NULL', and_g, " ORDER BY 1"))
    if (nrow(t3) > 0) managers <- as.character(t3$v)
    t4 <- tryCatch(
      DBI::dbGetQuery(conn, paste0('SELECT DISTINCT p."姓名" AS v FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."05人员表_id" = p.id WHERE 1=1', and_m, " ORDER BY 1")),
      error = function(e) data.frame(v = character(0))
    )
    if (nrow(t4) > 0) participants <- as.character(t4$v)
    t5 <- DBI::dbGetQuery(conn, paste0('SELECT DISTINCT "重要紧急程度" AS v FROM public."04项目总表" WHERE "重要紧急程度" IS NOT NULL', and_04, " ORDER BY 1"))
    if (nrow(t5) > 0) importance <- as.character(t5$v)
    t6 <- DBI::dbGetQuery(conn, paste0('SELECT DISTINCT h."医院名称" AS v FROM public."01医院信息表" h INNER JOIN public."03医院_项目表" s ON s."01_hos_resource_table医院信息表_id" = h.id WHERE h."医院名称" IS NOT NULL', and_s, " ORDER BY 1"))
    if (nrow(t6) > 0) hospitals <- as.character(t6$v)
    t7 <- DBI::dbGetQuery(conn, paste0('SELECT DISTINCT "课题组" AS v FROM public."04项目总表" WHERE "课题组" IS NOT NULL', and_04, " ORDER BY 1"))
    if (nrow(t7) > 0) research_groups <- as.character(t7$v)
    list(
      types = types,
      names = names_,
      managers = managers,
      participants = participants,
      importance = importance,
      hospitals = hospitals,
      research_groups = research_groups
    )
  }, error = function(e) NULL)
}

#' 按甘特相同维度在 v_项目阶段甘特视图_全部 上筛出 04 项目 id；无维度子句时返回 NULL（调用方用全量 choices）
meeting_new_project_ids_by_gantt_dims <- function(
    conn, auth,
    ft, fn, fm, fp, fi, fh, fg,
    include_archived,
    combine_mode = c("and", "or")) {
  combine_mode <- match.arg(combine_mode)
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(integer(0))
  if (is.null(auth) || isTRUE(auth$allow_none)) return(integer(0))
  manager_ids <- integer(0)
  if (length(fm) > 0L) {
    qm <- paste0('SELECT id FROM public."05人员表" WHERE "姓名" IN (', paste(rep("$", length(fm)), seq_along(fm), sep = "", collapse = ","), ")")
    manager_ids <- DBI::dbGetQuery(conn, qm, params = as.list(fm))$id
  }
  proj_ids_participant <- integer(0)
  if (length(fp) > 0L) {
    qp <- paste0('SELECT id FROM public."05人员表" WHERE "姓名" IN (', paste(rep("$", length(fp)), seq_along(fp), sep = "", collapse = ","), ")")
    pid_p <- DBI::dbGetQuery(conn, qp, params = as.list(fp))$id
    if (length(pid_p) > 0L) {
      qp2 <- paste0('SELECT DISTINCT "04项目总表_id" FROM public."_nc_m2m_04项目总表_05人员表" WHERE "05人员表_id" IN (', paste(rep("$", length(pid_p)), seq_along(pid_p), sep = "", collapse = ","), ")")
      proj_ids_participant <- DBI::dbGetQuery(conn, qp2, params = as.list(pid_p))[["04项目总表_id"]]
    }
  }
  proj_ids_hosp <- integer(0)
  if (length(fh) > 0L) {
    qh <- paste0('SELECT DISTINCT s."project_table 项目总表_id" FROM public."03医院_项目表" s INNER JOIN public."01医院信息表" h ON s."01_hos_resource_table医院信息表_id" = h.id WHERE h."医院名称" IN (', paste(rep("$", length(fh)), seq_along(fh), sep = "", collapse = ","), ")")
    proj_ids_hosp <- DBI::dbGetQuery(conn, qh, params = as.list(fh))[["project_table 项目总表_id"]]
  }
  proj_ids_rg <- integer(0)
  if (length(fg) > 0L) {
    qrg <- paste0('SELECT id FROM public."04项目总表" WHERE "课题组" IN (', paste(rep("$", length(fg)), seq_along(fg), sep = "", collapse = ","), ")")
    proj_ids_rg <- DBI::dbGetQuery(conn, qrg, params = as.list(fg))$id
  }
  dim_built <- build_gantt_filter_dimension_parts(ft, fn, fi, manager_ids, proj_ids_participant, proj_ids_hosp, proj_ids_rg)
  if (length(dim_built$dim_parts) == 0L) return(NULL)
  include_archived_sql <- if (isTRUE(include_archived)) "TRUE" else "COALESCE(g.project_is_active, true) = true"
  auth_sql <- if (auth$allow_all) {
    "TRUE"
  } else {
    paste0("g.proj_row_id IN (", auth$allowed_subquery, ")")
  }
  dim_sql <- if (identical(combine_mode, "or")) {
    paste0("(", paste(dim_built$dim_parts, collapse = " OR "), ")")
  } else {
    paste(dim_built$dim_parts, collapse = " AND ")
  }
  q <- paste0(
    'SELECT DISTINCT g.proj_row_id::bigint AS id FROM public."v_项目阶段甘特视图_全部" g WHERE ',
    include_archived_sql, " AND (", auth_sql, ") AND (", dim_sql, ") AND g.proj_row_id IS NOT NULL"
  )
  res <- DBI::dbGetQuery(conn, q, params = dim_built$params_stage)
  if (is.null(res) || nrow(res) == 0L) return(integer(0))
  as.integer(res$id)
}

#' 在 meeting_new_build_project_choices 结果上按 id 列表保留「共性决策」+ 匹配项目
meeting_new_intersect_project_choices <- function(base_choices, allowed_ids) {
  if (is.null(allowed_ids)) return(base_choices)
  bc <- base_choices
  if (length(allowed_ids) == 0L) {
    return(bc[names(bc) == "共性决策" | as.character(bc) == "__common__"])
  }
  idset <- as.character(allowed_ids)
  keep <- names(bc) == "共性决策" | as.character(bc) %in% c("__common__", idset)
  bc[keep]
}

meeting_feedback_id_choices <- function(flat) {
  if (is.null(flat) || nrow(flat) == 0L) return(character(0))
  lbls <- vapply(seq_len(nrow(flat)), function(i) {
    cs <- gsub("[\r\n]+", " ", substr(as.character(flat$content[i] %||% ""), 1, 44))
    sprintf("#%s %s", flat$id[i], cs)
  }, character(1))
  setNames(as.character(flat$id), lbls)
}

parse_milestone_json_to_df <- function(raw) {
  mapped <- parse_named_json_map(raw)
  if (length(mapped) == 0) return(empty_milestone_df())
  rows <- lapply(names(mapped), function(key) {
    val <- mapped[[key]]
    if (is.list(val)) {
      # 当前格式（含旧版有 stage_key 字段的数据）：直接读 name/plan/actual/note
      data.frame(
        entry_key = key,
        name   = normalize_text(val$name,   empty_as_na = FALSE),
        plan   = normalize_text(val$plan,   empty_as_na = FALSE),
        actual = normalize_text(val$actual, empty_as_na = FALSE),
        note   = normalize_text(val$note,   empty_as_na = FALSE),
        stringsAsFactors = FALSE
      )
    } else if (grepl("::", key, fixed = TRUE)) {
      # 遗留格式：stage_key::里程碑名称 作为 key，管道分隔字符串作为值
      key_parts <- strsplit(key, "::", fixed = TRUE)[[1]]
      name <- if (length(key_parts) >= 2L) key_parts[2] else ""
      pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
      while (length(pieces) < 3L) pieces <- c(pieces, "无")
      data.frame(
        entry_key = key,
        name   = name,
        plan   = ifelse(pieces[1] == "无", "", pieces[1]),
        actual = ifelse(pieces[2] == "无", "", pieces[2]),
        note   = ifelse(pieces[3] == "无", "", pieces[3]),
        stringsAsFactors = FALSE
      )
    } else {
      # 最早遗留格式：管道分隔字符串（首段为旧 stage_key，跳过）
      pieces <- strsplit(as.character(val %||% ""), "|", fixed = TRUE)[[1]]
      while (length(pieces) < 5L) pieces <- c(pieces, "无")
      data.frame(
        entry_key = key,
        name   = ifelse(pieces[2] == "无", "", pieces[2]),
        plan   = ifelse(pieces[3] == "无", "", pieces[3]),
        actual = ifelse(pieces[4] == "无", "", pieces[4]),
        note   = ifelse(pieces[5] == "无", "", pieces[5]),
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
    name <- trimws(as.character(df$name[i] %||% ""))
    if (!nzchar(name)) next
    plan   <- trimws(as.character(df$plan[i]   %||% ""))
    actual <- trimws(as.character(df$actual[i] %||% ""))
    note   <- trimws(as.character(df$note[i]   %||% ""))
    entry_key <- trimws(as.character(df$entry_key[i] %||% ""))
    if (!nzchar(entry_key) || entry_key %in% c(existing_keys, names(out))) {
      entry_key <- make_entry_key(pg_pool, actor, existing_keys = c(existing_keys, names(out)), prefix = "ms")
    }
    existing_keys <- c(existing_keys, entry_key)
    out[[entry_key]] <- list(
      name   = name,
      plan   = ifelse(nzchar(plan),   plan,   ""),
      actual = ifelse(nzchar(actual), actual, ""),
      note   = ifelse(nzchar(note),   note,   "")
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

# ---------- 甘特 SQL 筛选维度 / 行整理（原 server 内，每会话重复构造闭包；依赖 norm_progress，须在 app.R 定义 norm_progress 后 source 本文件） ----------

# 甘特筛选：各维度子句列表；combine_mode 为 or 时用 OR 包成一组，为 and 时逐项 AND。同一维度内多选为 IN。与权限、归档条件仍为 AND。
build_gantt_filter_dimension_parts <- function(ft, fn, fi, manager_ids, proj_ids_participant, proj_ids_hosp, proj_ids_rg = integer(0)) {
  p <- 0L
  or_parts <- character(0)
  params_stage <- list()
  make_ph <- function(n, start_idx) {
    paste(sprintf("$%d", start_idx + seq_len(n) - 1L), collapse = ",")
  }
  if (length(ft) > 0L) {
    ph <- make_ph(length(ft), p + 1L)
    or_parts <- c(or_parts, sprintf("project_type IN (%s)", ph))
    params_stage <- c(params_stage, as.list(ft))
    p <- p + length(ft)
  }
  if (length(fn) > 0L) {
    ph <- make_ph(length(fn), p + 1L)
    or_parts <- c(or_parts, sprintf("project_id IN (%s)", ph))
    params_stage <- c(params_stage, as.list(fn))
    p <- p + length(fn)
  }
  if (length(manager_ids) > 0L) {
    ph <- make_ph(length(manager_ids), p + 1L)
    or_parts <- c(or_parts, sprintf("project_db_id IN (SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" IN (%s))", ph))
    params_stage <- c(params_stage, as.list(manager_ids))
    p <- p + length(manager_ids)
  }
  if (length(proj_ids_participant) > 0L) {
    ph <- make_ph(length(proj_ids_participant), p + 1L)
    or_parts <- c(or_parts, sprintf("project_db_id IN (%s)", ph))
    params_stage <- c(params_stage, as.list(proj_ids_participant))
    p <- p + length(proj_ids_participant)
  }
  if (length(fi) > 0L) {
    ph <- make_ph(length(fi), p + 1L)
    or_parts <- c(or_parts, sprintf("\"重要紧急程度\" IN (%s)", ph))
    params_stage <- c(params_stage, as.list(fi))
    p <- p + length(fi)
  }
  if (length(proj_ids_hosp) > 0L) {
    ph <- make_ph(length(proj_ids_hosp), p + 1L)
    or_parts <- c(or_parts, sprintf("project_db_id IN (%s)", ph))
    params_stage <- c(params_stage, as.list(proj_ids_hosp))
    p <- p + length(proj_ids_hosp)
  }
  if (length(proj_ids_rg) > 0L) {
    ph <- make_ph(length(proj_ids_rg), p + 1L)
    or_parts <- c(or_parts, sprintf("project_db_id IN (%s)", ph))
    params_stage <- c(params_stage, as.list(proj_ids_rg))
  }
  list(dim_parts = or_parts, params_stage = params_stage)
}

# 甘特：视图查询结果 → 与 DB 管道一致的 data.frame（向量化未制定计划占位）
finalize_gantt_stage_rows <- function(stage_rows, today, ss, drop_stage_ord) {
  if (nrow(stage_rows) == 0L) return(stage_rows)
  stage_rows <- dplyr::as_tibble(stage_rows)
  stage_rows$planned_start_date <- as.Date(stage_rows$planned_start_date)
  stage_rows$actual_start_date  <- as.Date(stage_rows$actual_start_date)
  stage_rows$start_date <- as.Date(stage_rows$start_date)
  stage_rows$planned_end_date <- as.Date(stage_rows$planned_end_date)
  stage_rows$actual_end_date <- as.Date(stage_rows$actual_end_date)
  stage_rows$progress <- norm_progress(stage_rows$progress)
  stage_rows$is_unplanned <- is.na(stage_rows$start_date) |
    is.na(stage_rows$planned_start_date) |
    is.na(stage_rows$planned_end_date)
  site_rows <- stage_rows %>% dplyr::filter(!task_name %in% ss)
  sync_rows <- stage_rows %>% dplyr::filter(task_name %in% ss)
  site_name_map <- site_rows %>% dplyr::distinct(project_id, site_name)
  if (nrow(sync_rows) > 0) {
    sync_expanded <- sync_rows %>% dplyr::select(-site_name) %>%
      dplyr::left_join(site_name_map, by = "project_id", relationship = "many-to-many")
    sync_expanded <- sync_expanded %>% dplyr::filter(!is.na(site_name))
    sync_without_sites <- sync_rows %>% dplyr::filter(!(project_id %in% site_name_map$project_id))
    if (nrow(sync_without_sites) > 0) {
      sync_without_sites$site_name <- "所有中心（同步）"
      sync_expanded <- dplyr::bind_rows(sync_expanded, sync_without_sites)
    }
  } else {
    sync_expanded <- sync_rows
  }
  df <- dplyr::bind_rows(sync_expanded, site_rows) %>%
    dplyr::arrange(project_id, site_name, stage_ord)
  df$raw_planned_start_date <- df$planned_start_date
  df$raw_actual_start_date  <- df$actual_start_date
  df$raw_start_date <- df$start_date
  df$raw_planned_end_date <- df$planned_end_date
  iu <- !is.na(df$is_unplanned) & df$is_unplanned
  if (any(iu)) {
    df <- df %>%
      dplyr::group_by(project_id, site_name) %>%
      dplyr::mutate(._k = cumsum(as.integer(!is.na(is_unplanned) & is_unplanned))) %>%
      dplyr::ungroup()
    slot <- today + (df$._k[iu] - 1L) * 30L
    df$planned_start_date[iu] <- slot
    df$planned_end_date[iu]   <- slot + 30L
    df$start_date[iu]         <- slot
    df$._k <- NULL
  }
  if (isTRUE(drop_stage_ord)) {
    df %>% dplyr::select(-stage_ord)
  } else {
    df
  }
}

sel_intersect_choices <- function(cur, choices) {
  ch <- as.character(choices %||% character(0))
  if (is.null(cur) || length(cur) == 0L) return(character(0))
  cur <- as.character(cur)
  cur[cur %in% ch]
}

# 会议筛选：维度内多选为「任一命中」；维度间按 combine_mode 与甘特一致（且 = 各活跃维度同时满足，或 = 至少一维满足）
filter_meeting_decisions_by_dims <- function(df, fn, fp, fe, combine_mode) {
  n <- nrow(df)
  if (n == 0L) return(df)
  fn <- as.character(fn %||% character(0))
  fp <- as.character(fp %||% character(0))
  fe <- as.character(fe %||% character(0))
  active_name <- length(fn) > 0L
  active_proj <- length(fp) > 0L
  active_exec <- length(fe) > 0L
  if (!active_name && !active_proj && !active_exec) return(df)
  name_ok <- if (!active_name) rep(TRUE, n) else !is.na(df[["会议名称"]]) & df[["会议名称"]] %in% fn
  proj_ok <- if (!active_proj) rep(TRUE, n) else !is.na(df[["项目名称"]]) & df[["项目名称"]] %in% fp
  exec_ok <- if (!active_exec) {
    rep(TRUE, n)
  } else {
    vapply(seq_len(n), function(i) {
      edf <- parse_executor_json(df[["决策执行人及执行确认"]][i])
      if (nrow(edf) == 0L) return(FALSE)
      any(edf[["key"]] %in% fe)
    }, logical(1L), USE.NAMES = FALSE)
  }
  cm <- if (is.null(combine_mode) || !combine_mode %in% c("and", "or")) "and" else combine_mode
  if (identical(cm, "or")) {
    mask <- rep(FALSE, n)
    if (active_name) mask <- mask | name_ok
    if (active_proj) mask <- mask | proj_ok
    if (active_exec) mask <- mask | exec_ok
    df[mask, , drop = FALSE]
  } else {
    df[name_ok & proj_ok & exec_ok, , drop = FALSE]
  }
}

# 会议列表 SQL：无 12 关联视为「共性」；有 12 则按 11→04 鉴权（与 allow_all / allowed_subquery 一致）
meeting_decision_auth_sql <- function(auth) {
  if (is.null(auth) || isTRUE(auth$allow_all)) return("")
  if (isTRUE(auth$allow_none)) return(" AND FALSE")
  paste0(
    " AND (",
    "NOT EXISTS (SELECT 1 FROM public.\"12会议决策关联问题表\" l0 WHERE l0.\"10会议决策表_id\" = t.id)",
    " OR EXISTS (SELECT 1 FROM public.\"12会议决策关联问题表\" l1",
    " INNER JOIN public.\"11阶段问题反馈表\" f1 ON f1.id = l1.\"11阶段问题反馈表_id\"",
    " WHERE l1.\"10会议决策表_id\" = t.id AND f1.\"04项目总表_id\" IN (", auth$allowed_subquery, "))",
    ")"
  )
}

# 非「共性」时：所选反馈须存在且同属 project_id_int；返回 NA_character_ 表示通过，否则为错误文案
check_meeting_feedback_matches_project <- function(conn, feedback_ids, project_id_int) {
  if (is.na(project_id_int)) return(NA_character_)
  if (length(feedback_ids) == 0L) {
    return("选择具体项目时，请至少关联一条甘特问题/卡点（或使用「共性决策」）")
  }
  if (is.null(conn) || !DBI::dbIsValid(conn)) return("数据库未连接")
  ids <- unique(as.character(feedback_ids))
  ids <- ids[grepl("^[0-9]+$", ids)]
  if (length(ids) == 0L) return("关联反馈 id 无效")
  q <- sprintf(
    "SELECT DISTINCT \"04项目总表_id\" AS pid FROM public.\"11阶段问题反馈表\" WHERE id IN (%s)",
    paste(ids, collapse = ",")
  )
  pids <- tryCatch(DBI::dbGetQuery(conn, q)[["pid"]], error = function(e) integer(0))
  if (length(pids) == 0L) return("关联反馈 id 无效或已删除")
  if (length(unique(pids)) > 1L) return("所选反馈属于多个项目，请只选同一项目下的条目")
  if (as.integer(pids[[1]]) != as.integer(project_id_int)) {
    return("所选甘特反馈与当前选择的项目不一致，请重新选择")
  }
  NA_character_
}

# ---------- 从 server() 迁出的纯工具函数 ----------

# 按显示宽度累计截断文字（processed_data 用）
truncate_label <- function(txt, max_u = 30L) {
  txt <- as.character(txt %||% "")
  chars <- strsplit(txt, "", fixed = TRUE)[[1]]
  units <- 0L
  for (k in seq_along(chars)) {
    units <- units + nchar(chars[k], type = "width", allowNA = TRUE, keepNA = FALSE)
    if (units > max_u) {
      keep <- if (k <= 1L) "" else paste(chars[seq_len(k - 1L)], collapse = "")
      return(paste0(keep, ".."))
    }
  }
  txt
}

# 项目汇总「各中心进度」百分比计算（observeEvent project_header_clicked 用）
effective_center_progress_pct_for_summary <- function(ps, pe, asd, aed, raw_pct) {
  if (length(aed) == 1L && !is.na(aed)) return(100)
  rp <- suppressWarnings(as.numeric(raw_pct))
  if (length(rp) != 1L || is.na(rp)) rp <- 0
  if (rp <= 1 && rp >= 0) rp <- rp * 100
  if (is.na(ps) || is.na(pe)) return(0)
  if (is.na(asd)) return(0)
  max(0, min(100, round(rp)))
}

# 格式化数量（整数或保留2位小数）
fmt_amt <- function(x) {
  x <- as.numeric(x)
  if (length(x) != 1L || is.na(x)) return("1")
  if (abs(x - round(x)) < 1e-9) as.character(as.integer(round(x))) else format(round(x, 2), trim = TRUE)
}

# 合并贡献者按 person+role+work 汇总数量（项目汇总用）
merge_contrib_totals <- function(df) {
  if (is.null(df) || nrow(df) == 0L) return(df)
  df$amt_num <- vapply(seq_len(nrow(df)), function(i) parse_contrib_amount_num(df$amount[i]), numeric(1))
  df %>%
    group_by(person, role, work) %>%
    summarise(total_amt = sum(amt_num, na.rm = TRUE), .groups = "drop") %>%
    mutate(ro = contrib_role_sort_key(role)) %>%
    arrange(person, ro, work) %>%
    select(-ro)
}

# 日期输入校验（保存任务用）
validate_date_input <- function(x) {
  if (is.null(x)) return(list(ok = TRUE, value = NA))
  xc <- tryCatch(trimws(as.character(x)), error = function(e) "")
  if (!nzchar(xc)) return(list(ok = TRUE, value = NA))
  d <- tryCatch(as.Date(xc, optional = TRUE), error = function(e) NA)
  if (is.na(d)) return(list(ok = FALSE, value = NA))
  list(ok = TRUE, value = d)
}

# ---------- 个人消息 Tab ----------

.empty_msg_df <- function() {
  data.frame(
    category = character(0),
    priority = character(0),
    project_name = character(0),
    stage_name = character(0),
    message_text = character(0),
    related_date = as.Date(character(0)),
    days_value = integer(0),
    sort_key = numeric(0),
    project_type = character(0),
    importance = character(0),
    manager_name = character(0),
    project_db_id = integer(0),
    stage_instance_id = integer(0),
    task_key = character(0),
    feedback_id = integer(0),
    decision_id = integer(0),
    executor_json = character(0),
    stringsAsFactors = FALSE
  )
}

# M1/M5/M6/M7/M8：阶段相关消息（合并为一条 SQL）
.fetch_stage_personal_msgs <- function(conn, and_auth, today) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(.empty_msg_df())
  q <- paste0("
    SELECT g.\"项目名称\", g.id AS project_db_id,
           si.id AS stage_instance_id, d.stage_key AS task_key,
           g.\"项目类型\", g.\"重要紧急程度\",
           p.\"姓名\" AS manager_name,
           COALESCE(si.\"阶段实例自定义名称\", d.stage_name) AS stage_name,
           si.planned_start_date, si.planned_end_date,
           si.actual_start_date, si.actual_end_date,
           si.progress, si.updated_at,
           CURRENT_DATE - si.planned_end_date AS days_overdue,
           si.planned_end_date - CURRENT_DATE AS days_remaining,
           CURRENT_DATE - si.planned_start_date AS days_late,
           CURRENT_DATE - COALESCE(si.updated_at::date, si.planned_start_date) AS days_stale,
           CASE
             WHEN si.actual_start_date IS NOT NULL THEN
               (CURRENT_DATE - si.actual_start_date)::numeric /
               NULLIF((si.planned_end_date - si.planned_start_date)::numeric, 0)
             WHEN si.planned_start_date <= CURRENT_DATE THEN
               (CURRENT_DATE - si.planned_start_date)::numeric /
               NULLIF((si.planned_end_date - si.planned_start_date)::numeric, 0)
             ELSE 0
           END AS planned_progress
    FROM public.\"09项目阶段实例表\" si
    JOIN public.\"08项目阶段定义表\" d ON d.id = si.stage_def_id
    JOIN public.\"04项目总表\" g ON g.id = si.project_id
    LEFT JOIN public.\"05人员表\" p ON p.id = g.\"05人员表_id\"
    WHERE COALESCE(si.is_active, TRUE) = TRUE
      AND COALESCE(g.\"is_active\", TRUE) = TRUE
      AND si.actual_end_date IS NULL", and_auth, "
    ORDER BY g.\"项目名称\", d.stage_order
  ")
  df <- tryCatch(DBI::dbGetQuery(conn, q), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0L) return(.empty_msg_df())

  rows <- list()
  for (i in seq_len(nrow(df))) {
    prog <- suppressWarnings(as.integer(df$progress[i]))
    if (is.na(prog)) prog <- 0L
    prog_pct <- prog
    actual_prog <- prog / 100
    plan_prog <- suppressWarnings(as.numeric(df$planned_progress[i]))
    if (is.na(plan_prog)) plan_prog <- 0
    diff <- plan_prog - actual_prog
    pdo <- as.Date(df$planned_end_date[i])
    pds <- as.Date(df$planned_start_date[i])
    aed <- as.Date(df$actual_end_date[i])
    upd <- as.POSIXct(df$updated_at[i])
    pname <- as.character(df[["项目名称"]][i])
    sname <- as.character(df$stage_name[i])
    ptype <- as.character(df[["项目类型"]][i] %||% "")
    pimp <- as.character(df[["重要紧急程度"]][i] %||% "")
    pmgr <- as.character(df$manager_name[i] %||% "")
    days_od <- suppressWarnings(as.integer(df$days_overdue[i]))
    days_rem <- suppressWarnings(as.integer(df$days_remaining[i]))
    days_lt <- suppressWarnings(as.integer(df$days_late[i]))
    days_st <- suppressWarnings(as.integer(df$days_stale[i]))
    p_db_id <- suppressWarnings(as.integer(df$project_db_id[i]))
    si_id <- suppressWarnings(as.integer(df$stage_instance_id[i]))
    tk <- as.character(df$task_key[i] %||% "")
    if (is.na(days_od)) days_od <- 0L
    if (is.na(days_rem)) days_rem <- 0L
    if (is.na(days_lt)) days_lt <- 0L
    if (is.na(days_st)) days_st <- 0L

    # M1+M8 合并：阶段落后或逾期（逾期 OR 偏差>30%，去重：同一阶段只出一条）
    is_overdue <- !is.na(pdo) && pdo < today && prog < 100
    is_behind <- !is.na(diff) && diff > 0.30 && prog < 100 && is.na(aed)
    if (is_overdue || is_behind) {
      if (is_overdue && is_behind) {
        msg <- sprintf("计划完成: %s, 已逾期 %d 天, 当前进度: %d%%, 偏差 %d%%",
                       pdo, days_od, prog_pct, round(diff * 100))
      } else if (is_overdue) {
        msg <- sprintf("计划完成: %s, 已逾期 %d 天, 当前进度: %d%%", pdo, days_od, prog_pct)
      } else {
        msg <- sprintf("进度严重落后: 实际 %d%%, 计划应达 %d%%, 偏差 %d%%",
                       prog_pct, round(plan_prog * 100), round(diff * 100))
      }
      worst <- max(if (is_overdue) days_od else 0L, if (is_behind) round(diff * 100) else 0L)
      pri <- if (worst > 30L || (is_overdue && prog == 0L)) "critical"
             else if (worst > 15L) "high"
             else if (worst > 7L) "medium"
             else "low"
      rows[[length(rows) + 1L]] <- data.frame(
        category = "阶段落后或逾期",
        priority = pri,
        project_name = pname,
        stage_name = sname,
        message_text = msg,
        related_date = if (!is.na(pdo)) pdo else today,
        days_value = worst,
        sort_key = worst + 0.5,
        project_type = ptype,
        importance = pimp,
        manager_name = pmgr,
        project_db_id = p_db_id,
        stage_instance_id = si_id,
        task_key = tk,
        feedback_id = NA_integer_,
        decision_id = NA_integer_,
        executor_json = NA_character_,
        stringsAsFactors = FALSE
      )
    }

    # M6: 未启动（已过计划开始日，实际未开始）
    asd <- as.Date(df$actual_start_date[i])
    if (!is.na(pds) && pds <= today && is.na(asd) && is.na(aed) && prog == 0L) {
      pri <- if (days_lt > 14L) "critical"
             else if (days_lt > 7L) "high"
             else if (days_lt > 3L) "medium"
             else "low"
      rows[[length(rows) + 1L]] <- data.frame(
        category = "未启动",
        priority = pri,
        project_name = pname,
        stage_name = sname,
        message_text = sprintf("应开始于: %s, 已延迟 %d 天未启动", pds, days_lt),
        related_date = pds,
        days_value = days_lt,
        sort_key = days_lt + 0.3,
        project_type = ptype,
        importance = pimp,
        manager_name = pmgr,
        project_db_id = p_db_id,
        stage_instance_id = si_id,
        task_key = tk,
        feedback_id = NA_integer_,
        decision_id = NA_integer_,
        executor_json = NA_character_,
        stringsAsFactors = FALSE
      )
    }

    # M7: 久未更新（已开始、未结束、0 < progress < 100，超过7天无更新）
    asd <- as.Date(df$actual_start_date[i])
    if (prog > 0L && prog < 100L && !is.na(upd) && !is.na(asd) && is.na(aed)) {
      if (days_st > 7L) {
        pri <- if (days_st > 30L) "critical"
               else if (days_st > 14L) "high"
               else "medium"
        upd_date <- as.Date(upd)
        rows[[length(rows) + 1L]] <- data.frame(
          category = "久未更新",
          priority = pri,
          project_name = pname,
          stage_name = sname,
          message_text = sprintf("当前进度: %d%%, 最后更新: %s (%d 天无更新)", prog_pct, upd_date, days_st),
          related_date = upd_date,
          days_value = days_st,
          sort_key = days_st + 0.2,
          project_type = ptype,
          importance = pimp,
          manager_name = pmgr,
          project_db_id = p_db_id,
          stage_instance_id = si_id,
          task_key = tk,
          feedback_id = NA_integer_,
          decision_id = NA_integer_,
          executor_json = NA_character_,
          stringsAsFactors = FALSE
        )
      }
    }
  }

  if (length(rows) == 0L) return(.empty_msg_df())
  do.call(rbind, rows)
}

# M3/M4：近期卡点与问题
.fetch_feedback_personal_msgs <- function(conn, and_auth, today, days_back) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(.empty_msg_df())
  q <- paste0("
    SELECT f.id AS feedback_id, f.\"类型\", f.\"内容\", f.created_at,
           f.reporters_json::text AS rj,
           g.\"项目名称\", g.id AS project_db_id,
           g.\"项目类型\", g.\"重要紧急程度\",
           p.\"姓名\" AS manager_name,
           COALESCE(si.\"阶段实例自定义名称\", d.stage_name) AS stage_name,
           si.id AS stage_instance_id, d.stage_key AS task_key
    FROM public.\"11阶段问题反馈表\" f
    JOIN public.\"04项目总表\" g ON g.id = f.\"04项目总表_id\"
    LEFT JOIN public.\"05人员表\" p ON p.id = g.\"05人员表_id\"
    LEFT JOIN public.\"09项目阶段实例表\" si ON si.id = f.\"09项目阶段实例表_id\"
    LEFT JOIN public.\"08项目阶段定义表\" d ON d.id = si.stage_def_id
    WHERE f.\"类型\" IN ('卡点', '问题')
      AND f.created_at >= CURRENT_DATE - (", days_back, " || ' days')::interval
      AND COALESCE(g.\"is_active\", TRUE) = TRUE", and_auth, "
    ORDER BY f.created_at DESC
  ")
  df <- tryCatch(DBI::dbGetQuery(conn, q), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0L) return(.empty_msg_df())

  rows <- list()
  for (i in seq_len(nrow(df))) {
    typ <- as.character(df[["类型"]][i])
    content <- as.character(df[["内容"]][i])
    pname <- as.character(df[["项目名称"]][i])
    sname <- as.character(df$stage_name[i])
    imp <- as.character(df[["重要紧急程度"]][i])
    ptype <- as.character(df[["项目类型"]][i] %||% "")
    pmgr <- as.character(df$manager_name[i] %||% "")
    created <- as.POSIXct(df$created_at[i])
    if (is.na(created)) next
    days_ago <- as.integer(difftime(today, as.Date(created), units = "days"))
    if (is.na(days_ago)) days_ago <- 0L

    # 解析报告人
    rj <- df[["rj"]][i]
    reporters <- tryCatch({
      jj <- jsonlite::fromJSON(rj, simplifyVector = TRUE)
      if (is.null(jj)) "" else paste(as.character(jj), collapse = "、")
    }, error = function(e) "")

    cat_label <- if (identical(typ, "卡点")) "近期卡点" else "近期问题"

    if (identical(typ, "卡点")) {
      pri <- if (grepl("重要|紧急", imp) || days_ago > 7L) "critical"
             else if (days_ago > 3L) "high"
             else if (days_ago > 1L) "medium"
             else "low"
    } else {
      pri <- if (days_ago > 7L) "high"
             else if (days_ago > 3L) "medium"
             else "low"
    }

    msg <- paste0(
      if (nzchar(sname)) paste0("[", sname, "] ") else "",
      typ, ": ", substr(content, 1, 80),
      if (nzchar(reporters)) paste0(" (报告人: ", reporters, ")") else ""
    )

    rows[[length(rows) + 1L]] <- data.frame(
      category = cat_label,
      priority = pri,
      project_name = pname,
      stage_name = if (nzchar(sname)) sname else "",
      message_text = msg,
      related_date = as.Date(created),
      days_value = days_ago,
      sort_key = days_ago + 0.4,
      project_type = ptype,
      importance = imp,
      manager_name = pmgr,
      project_db_id = suppressWarnings(as.integer(df$project_db_id[i])),
      stage_instance_id = suppressWarnings(as.integer(df$stage_instance_id[i] %||% NA_integer_)),
      task_key = as.character(df$task_key[i] %||% ""),
      feedback_id = suppressWarnings(as.integer(df$feedback_id[i])),
      decision_id = NA_integer_,
      executor_json = NA_character_,
      stringsAsFactors = FALSE
    )
  }

  if (length(rows) == 0L) return(.empty_msg_df())
  do.call(rbind, rows)
}

# M2：待执行的会议决策
# common_user: 仅展示自己作为执行人且状态为"未执行"的决策
# 领导(manager/super_admin/group_manager): 展示职权范围内所有含"未执行"执行人的决策（去重，一条决策只算一次）
.fetch_meeting_action_msgs <- function(conn, auth, and_auth_meeting, today) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(.empty_msg_df())
  if (is.null(auth) || !nzchar(auth$work_id)) return(.empty_msg_df())

  is_leader <- isTRUE(auth$can_manage_project)
  wid <- auth$work_id

  q <- paste0("
    SELECT t.id, t.\"会议名称\", t.\"会议时间\", t.\"决策内容\",
           t.\"决策执行人及执行确认\"::text AS exec_json
    FROM public.\"10会议决策表\" t
    WHERE t.\"决策执行人及执行确认\" IS NOT NULL", and_auth_meeting, "
    ORDER BY t.\"会议时间\" DESC NULLS LAST
  ")
  df <- tryCatch(DBI::dbGetQuery(conn, q), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0L) return(.empty_msg_df())

  rows <- list()
  for (i in seq_len(nrow(df))) {
    ej <- as.character(df$exec_json[i])
    exec_df <- parse_executor_json(ej)
    if (nrow(exec_df) == 0L) next

    pending_df <- exec_df[trimws(as.character(exec_df[["状态"]])) == "未执行", , drop = FALSE]
    if (nrow(pending_df) == 0L) next

    if (!is_leader) {
      # 普通用户：只看自己作为执行人且未执行的
      my_rows <- pending_df[grepl(paste0("-", wid, "$"), pending_df$key), , drop = FALSE]
      if (nrow(my_rows) == 0L) next
    }
    # 领导：只要该决策有任何"未执行"的执行人就纳入（去重，一条决策一次）

    mtg_name <- as.character(df[["会议名称"]][i])
    mtg_time <- as.POSIXct(df[["会议时间"]][i])
    decision <- as.character(df[["决策内容"]][i])
    days_since <- if (!is.na(mtg_time)) as.integer(difftime(today, as.Date(mtg_time), units = "days")) else 0L
    if (is.na(days_since)) days_since <- 0L

    pri <- if (days_since > 30L) "critical"
           else if (days_since > 14L) "high"
           else if (days_since > 7L) "medium"
           else "low"

    mtg_label <- if (nzchar(mtg_name)) mtg_name else "会议"
    mtg_date_str <- if (!is.na(mtg_time)) format(as.Date(mtg_time), "%Y-%m-%d") else "未知日期"

    # 附加未执行执行人摘要
    pending_names <- vapply(pending_df$key, executor_display_name_from_key, character(1))
    pending_summary <- paste(pending_names, collapse = "、")

    rows[[length(rows) + 1L]] <- data.frame(
      category = "待执行决策",
      priority = pri,
      project_name = paste0(mtg_label, " (", mtg_date_str, ")"),
      stage_name = "",
      message_text = paste0("决策: ", substr(decision, 1, 60), if (nchar(decision) > 60) "..." else "",
                            " | 待执行: ", pending_summary),
      related_date = if (!is.na(mtg_time)) as.Date(mtg_time) else today,
      days_value = days_since,
      sort_key = days_since + 0.6,
      project_type = "",
      importance = "",
      manager_name = "",
      project_db_id = NA_integer_,
      stage_instance_id = NA_integer_,
      task_key = NA_character_,
      feedback_id = NA_integer_,
      decision_id = as.integer(df$id[i]),
      executor_json = ej,
      stringsAsFactors = FALSE
    )
  }

  if (length(rows) == 0L) return(.empty_msg_df())
  do.call(rbind, rows)
}

# 个人消息汇总：合并所有类别
fetch_personal_messages <- function(conn, auth, days_back = 14L) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(.empty_msg_df())
  if (is.null(auth) || isTRUE(auth$allow_none)) return(.empty_msg_df())

  and_auth <- if (auth$allow_all) "" else paste0(' AND g.id IN (', auth$allowed_subquery, ')')
  and_auth_meeting <- meeting_decision_auth_sql(auth)
  today <- today_beijing()

  stage_msgs <- .fetch_stage_personal_msgs(conn, and_auth, today)
  feedback_msgs <- .fetch_feedback_personal_msgs(conn, and_auth, today, as.integer(days_back))
  meeting_msgs <- .fetch_meeting_action_msgs(conn, auth, and_auth_meeting, today)

  all_msgs <- rbind(stage_msgs, feedback_msgs, meeting_msgs)
  if (nrow(all_msgs) == 0L) return(.empty_msg_df())

  # 按优先级排序
  pri_rank <- c(critical = 1L, high = 2L, medium = 3L, low = 4L)
  all_msgs$pri_rank <- pri_rank[all_msgs$priority]
  all_msgs <- all_msgs[order(all_msgs$pri_rank, -all_msgs$sort_key), ]
  all_msgs$pri_rank <- NULL
  rownames(all_msgs) <- NULL
  all_msgs
}

# 消息优先级颜色与标签
msg_priority_info <- function(priority) {
  switch(priority,
    critical = list(color = "#F44336", bg = "#FFEBEE", label = "关键"),
    high     = list(color = "#FF6F00", bg = "#FFF3E0", label = "高"),
    medium   = list(color = "#F57C00", bg = "#FFF8E1", label = "中"),
    low      = list(color = "#2196F3", bg = "#E3F2FD", label = "低"),
    list(color = "#757575", bg = "#F5F5F5", label = "信息")
  )
}

# ---------- 单项目甘特图数据构建（消息页下钻用） ----------

# 甘特颜色逻辑复用（与 processed_data 中 items_centers/items_sync 完全一致）
.gantt_item_color_style <- function(is_unplanned, plan_no_actual_start, is_not_started, missing_actual_done,
                                     progress_diff, is_completed, bg_default = "#9E9E9E") {
  bg_color <- case_when(
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
    TRUE ~ bg_default
  )
  border_color <- if_else(is_unplanned, "#D0D0D0", bg_color)
  text_color <- ifelse(
    is_unplanned, "#555555",
    ifelse(is_not_started, "#FFFFFF",
      case_when(
        progress_diff < -0.3 ~ "white",
        progress_diff >= 0.25 ~ "white",
        TRUE ~ "#333333"
      )
    )
  )
  style <- ifelse(is_unplanned,
    sprintf("background-color: %s; border-color: %s; color: %s; border-width: 1px; border-style: solid;", bg_color, border_color, text_color),
    sprintf("background-color: %s; border-color: %s; color: %s; border-width: 2px;", bg_color, border_color, text_color))
  list(bg_color = bg_color, style = style)
}

build_single_project_gantt_data <- function(conn, project_db_id, sync_stages, stage_label_fn) {
  if (is.null(conn) || !DBI::dbIsValid(conn)) return(NULL)
  pid <- suppressWarnings(as.integer(project_db_id))
  if (is.na(pid)) return(NULL)
  today <- today_beijing()
  ss <- sync_stages

  stage_rows <- tryCatch(
    DBI::dbGetQuery(conn,
      'SELECT * FROM public."v_项目阶段甘特视图_全部" WHERE proj_row_id = $1 ORDER BY stage_ord, site_name',
      params = list(pid)),
    error = function(e) NULL
  )
  if (is.null(stage_rows) || nrow(stage_rows) == 0) return(NULL)

  gd <- finalize_gantt_stage_rows(stage_rows, today, ss, drop_stage_ord = TRUE)

  # 避免 bind_rows 时 pq_jsonb 与缺失列类型冲突：将 JSON 列统一为 character
  json_cols <- c("remark_json", "contributors_json", "milestones_json", "sample_json")
  for (jc in json_cols) if (jc %in% names(gd)) gd[[jc]] <- as.character(gd[[jc]])

  # --- Groups ---
  groups_centers <- gd %>%
    filter(!task_name %in% ss) %>%
    group_by(project_id, site_name) %>%
    summarise(avg_p = mean(progress), .groups = "drop") %>%
    arrange(project_id, site_name) %>%
    mutate(
      id = paste0(project_id, "_", site_name),
      content = sprintf("<div style='padding:2px 6px;font-size:13px;'>%s</div>", site_name),
      className = "gantt-bg-default"
    )

  groups_sync <- gd %>%
    filter(task_name %in% ss) %>%
    group_by(project_id) %>%
    summarise(avg_p = mean(progress), .groups = "drop") %>%
    mutate(
      id = paste0(project_id, "_同步阶段"),
      content = "<div style='padding:2px 6px;font-size:13px;line-height:1.35;'>各中心同步阶段</div>",
      className = "gantt-bg-default"
    )

  groups_data <- bind_rows(groups_sync, groups_centers) %>%
    arrange(project_id, desc(id == paste0(project_id, "_同步阶段")), site_name)

  # --- Items ---
  # Center items
  if (nrow(gd %>% filter(!task_name %in% ss)) > 0) {
    items_centers <- gd %>%
      filter(!task_name %in% ss) %>%
      mutate(
        id = row_number(),
        group = paste0(project_id, "_", site_name),
        planned_duration = dplyr::if_else(is_unplanned, 30.0, as.numeric(planned_end_date - planned_start_date)),
        is_completed = !is.na(actual_end_date),
        plan_no_actual_start = !is_unplanned & is.na(actual_start_date) &
          !is.na(planned_start_date) & !is.na(planned_end_date) & is.na(actual_end_date),
        planned_progress = ifelse(plan_no_actual_start, 0.0,
          ifelse(is_completed,
            ifelse(planned_duration > 0, as.numeric(actual_end_date - start_date) / planned_duration, 1.0),
            ifelse(planned_duration > 0, as.numeric(today - start_date) / planned_duration,
              ifelse(today >= start_date, 1.0, 0.0)))),
        actual_progress = ifelse(is_completed, 1.0, progress),
        progress_diff = actual_progress - planned_progress,
        is_not_started = plan_no_actual_start |
          (!is_unplanned & today < start_date & progress == 0 & is.na(actual_end_date)),
        missing_actual_done = is.na(actual_end_date) & progress >= 1.0,
        start = dplyr::if_else(is.na(start_date), today, start_date),
        theoretical_end = dplyr::case_when(
          is_unplanned ~ planned_end_date,
          !is.na(actual_start_date) & !is.na(planned_start_date) & !is.na(planned_end_date) ~
            actual_start_date + as.numeric(planned_end_date - planned_start_date),
          TRUE ~ planned_end_date),
        end = dplyr::case_when(
          is_unplanned ~ as.character(dplyr::coalesce(planned_end_date, start_date + 30L)),
          !is.na(actual_end_date) ~ as.character(actual_end_date),
          today > theoretical_end ~ as.character(today),
          TRUE ~ as.character(theoretical_end)),
        content = paste0(
          ifelse(!is.na(task_display_name) & nzchar(as.character(task_display_name)),
                 as.character(task_display_name), vapply(task_name, stage_label_fn, character(1))),
          ifelse(is_unplanned, " (未制定计划)", ""), " ",
          ifelse(!is.na(actual_end_date), 100, round(progress * 100, 0)), "%"),
        type = "range"
      )
    # Apply color
    cs <- mapply(function(up, pnas, ins, mad, pd) {
      .gantt_item_color_style(up, pnas, ins, mad, pd, FALSE)
    }, items_centers$is_unplanned, items_centers$plan_no_actual_start,
       items_centers$is_not_started, items_centers$missing_actual_done,
       items_centers$progress_diff, SIMPLIFY = FALSE)
    items_centers$style <- sapply(cs, `[[`, "style")
  } else {
    items_centers <- data.frame()
  }

  # Sync items
  if (nrow(gd %>% filter(task_name %in% ss)) > 0) {
    items_sync <- gd %>%
      filter(task_name %in% ss) %>%
      group_by(project_id, task_name) %>%
      summarise(
        start_date = first(start_date), actual_start_date = first(actual_start_date),
        planned_start_date = first(planned_start_date), planned_end_date = first(planned_end_date),
        actual_end_date = first(actual_end_date), progress = first(progress),
        is_unplanned = first(is_unplanned), task_display_name = first(task_display_name),
        .groups = "drop"
      ) %>%
      mutate(
        id = max(items_centers$id, 0) + row_number(),
        group = paste0(project_id, "_同步阶段"),
        planned_duration = dplyr::if_else(is_unplanned, 30.0, as.numeric(planned_end_date - planned_start_date)),
        is_completed = !is.na(actual_end_date),
        plan_no_actual_start = !is_unplanned & is.na(actual_start_date) &
          !is.na(planned_start_date) & !is.na(planned_end_date) & is.na(actual_end_date),
        planned_progress = ifelse(plan_no_actual_start, 0.0,
          ifelse(is_completed,
            ifelse(planned_duration > 0, as.numeric(actual_end_date - start_date) / planned_duration, 1.0),
            ifelse(planned_duration > 0, as.numeric(today - start_date) / planned_duration,
              ifelse(today >= start_date, 1.0, 0.0)))),
        actual_progress = ifelse(is_completed, 1.0, progress),
        progress_diff = actual_progress - planned_progress,
        is_not_started = plan_no_actual_start |
          (!is_unplanned & today < start_date & progress == 0 & is.na(actual_end_date)),
        missing_actual_done = is.na(actual_end_date) & progress >= 1.0,
        start = dplyr::if_else(is.na(start_date), today, start_date),
        theoretical_end = dplyr::case_when(
          is_unplanned ~ planned_end_date,
          !is.na(actual_start_date) & !is.na(planned_start_date) & !is.na(planned_end_date) ~
            actual_start_date + as.numeric(planned_end_date - planned_start_date),
          TRUE ~ planned_end_date),
        end = dplyr::case_when(
          is_unplanned ~ as.character(dplyr::coalesce(planned_end_date, start_date + 30L)),
          !is.na(actual_end_date) ~ as.character(actual_end_date),
          today > theoretical_end ~ as.character(today),
          TRUE ~ as.character(theoretical_end)),
        content = paste0(
          ifelse(!is.na(task_display_name) & nzchar(as.character(task_display_name)),
                 as.character(task_display_name), vapply(task_name, stage_label_fn, character(1))),
          ifelse(is_unplanned, " (未制定计划)", ""), " ",
          ifelse(!is.na(actual_end_date), 100, round(progress * 100, 0)), "%"),
        type = "range"
      )
    cs2 <- mapply(function(up, pnas, ins, mad, pd) {
      .gantt_item_color_style(up, pnas, ins, mad, pd, FALSE)
    }, items_sync$is_unplanned, items_sync$plan_no_actual_start,
       items_sync$is_not_started, items_sync$missing_actual_done,
       items_sync$progress_diff, SIMPLIFY = FALSE)
    items_sync$style <- sapply(cs2, `[[`, "style")
  } else {
    items_sync <- data.frame()
  }

  # Merge items
  if (nrow(items_centers) == 0) {
    items_data <- items_sync
  } else if (nrow(items_sync) == 0) {
    items_data <- items_centers
  } else {
    items_data <- bind_rows(items_centers, items_sync)
  }

  list(items = items_data, groups = groups_data)
}
