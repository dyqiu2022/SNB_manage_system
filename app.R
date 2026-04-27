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

# 空值合并（全局）：server 外定义的函数依赖此运算符，须在首屏 source 前可用
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

# 性能诊断：容器日志里搜 IVD_PERF，对比各段耗时（首屏白屏定位用）
ivd_perf_ts <- function(label) {
  print(noquote(paste0("[IVD_PERF] ", format(Sys.time(), "%Y-%m-%d %H:%M:%OS3"), " ", label)))
}
# 相对某 t0 的耗时（秒），用于嵌套段落
ivd_perf_elapsed <- function(t0, label) {
  print(noquote(paste0(
    "[IVD_PERF] ", format(Sys.time(), "%Y-%m-%d %H:%M:%OS3"), " ", label,
    " (+s=", round((proc.time() - t0)[3], 3), ")"
  )))
}

# 业务日期时间：界面默认值与手填「日期+时刻」按北京时间（服务器/Docker 常为 UTC，避免与本地少 8 小时）
APP_TZ_CN <- "Asia/Shanghai"

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
ivd_perf_ts("GLOBAL after dbPool()")

onStop(function() {
  poolClose(pg_pool)
})

# ---------------- 1. 甘特图常量（与 DB 表结构对应） ----------------
today <- as.Date(format(Sys.time(), "%Y-%m-%d", tz = APP_TZ_CN))
# ⚠️  以下常量（task_short_name / project_type_valid_stages / sync_stages_db / site_stages_db）
# 仅作 DB 离线 fallback：正常运行时阶段数据全部来自 08 表（stage_definition_df reactive）。
# 不要在此处维护阶段信息，请通过"阶段维护"界面直接修改 08 表。
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
# 阶段默认顺序（fallback only）
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
# 同步阶段（项目级）— fallback only，DB 正常时不使用
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
# 分中心阶段（中心级）— fallback only，DB 正常时不使用
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

# 项目汇总弹窗：解析展示用项目主键、参与度颜色与数量（server 内亦会用到同名函数）
resolve_project_db_id_from_display <- function(con, display_pid) {
  pid <- as.character(display_pid %||% "")
  if (!nzchar(pid)) return(NA_integer_)
  if (grepl("^项目-[0-9]+$", pid)) {
    return(as.integer(sub("^项目-", "", pid)))
  }
  pr <- DBI::dbGetQuery(con, 'SELECT id FROM public."04项目总表" WHERE "项目名称" = $1', params = list(pid))
  if (nrow(pr) > 0) return(as.integer(pr$id[1]))
  NA_integer_
}

contrib_role_color <- function(role) {
  r <- trimws(as.character(role %||% ""))
  if (identical(r, "主导")) return("#C62828")
  if (identical(r, "参与")) return("#1565C0")
  if (identical(r, "协助")) return("#2E7D32")
  "#616161"
}

# 问题/卡点/经验分享：仅「类型」词着色（【】括号不染色）
remark_type_color <- function(type) {
  t <- trimws(as.character(type %||% ""))
  if (identical(t, "卡点")) return("#C62828")
  if (identical(t, "经验分享")) return("#2E7D32")
  if (identical(t, "问题")) return("#F57C00")
  "#616161"
}

# 要点行尾：(中心·阶段) 或 (无中心·阶段)；与 11 表 task_key_raw 一致时优先识别项目级要点
feedback_location_paren <- function(site_name, stage_label, task_key_raw = NULL) {
  stg <- trimws(as.character(stage_label %||% ""))
  site <- trimws(as.character(site_name %||% ""))
  tk <- ""
  if (!is.null(task_key_raw) && length(task_key_raw) >= 1L) {
    tk <- trimws(as.character(task_key_raw[1]))
    if (length(tk) != 1L || is.na(tk) || !nzchar(tk)) tk <- ""
  }
  if (identical(tk, "__project_scope__")) {
    return(sprintf("(无中心·%s)", stg))
  }
  if (nzchar(stg) && identical(stg, "项目要点")) {
    return(sprintf("(无中心·%s)", stg))
  }
  if (nzchar(site) && grepl("不分中心", site, fixed = TRUE)) {
    return(sprintf("(无中心·%s)", stg))
  }
  sprintf("(%s·%s)", site, stg)
}

# 项目汇总弹窗：一行贡献者，仅「参与度」三字着色，其余为默认深色
contrib_summary_line_tag <- function(role, work, amt_txt, font_size = "14px") {
  r <- trimws(as.character(role %||% ""))
  col <- contrib_role_color(r)
  tags$div(
    style = paste0("margin-left: 10px; font-size: ", font_size, "; color: #333;"),
    tags$span(style = paste0("color: ", col, "; font-weight: 600;"), r),
    sprintf("-%s-数量：%s", work, amt_txt)
  )
}

contrib_role_sort_key <- function(role) {
  r <- trimws(as.character(role %||% ""))
  if (identical(r, "主导")) return(1L)
  if (identical(r, "参与")) return(2L)
  if (identical(r, "协助")) return(3L)
  99L
}

parse_contrib_amount_num <- function(amt) {
  s <- trimws(as.character(amt %||% ""))
  if (!nzchar(s)) return(1)
  n <- suppressWarnings(as.numeric(s))
  if (!is.na(n)) return(n)
  1
}


# ---------------- 2. UI 界面 ----------------
ui <- fluidPage(
  tags$head(
    tags$script(HTML("
      // timevis 把 vis Timeline 放在 el.widget.timeline（见 timevis.js container.widget = that）
      (function() {
        window.__ganttGetTimeline = function(root) {
          if (!root) return null;
          if (root.widget && root.widget.timeline) return root.widget.timeline;
          if (root.timeline) return root.timeline;
          var inner = root.querySelector && root.querySelector('.html-widget');
          if (inner) {
            if (inner.widget && inner.widget.timeline) return inner.widget.timeline;
            if (inner.timeline) return inner.timeline;
          }
          return null;
        };
      })();

      Shiny.addCustomMessageHandler('resetGanttSelection', function(ganttId) {
        var el = document.getElementById(ganttId);
        var tl = window.__ganttGetTimeline(el);
        if (tl) { tl.setSelection([]); }
        Shiny.setInputValue(ganttId + '_selected', null);
      });

      // 自适应甘特图高度 + 滚动位置保持
      (function() {
        var GANTT_IDS  = ['my_gantt', 'my_gantt2'];
        var BOTTOM_PAD = 16;
        var MIN_HEIGHT = 300;
        var _ganttScrollTops = {};  // 每个 widget 独立的滚动位置

        // 获取 vis-timeline 的垂直滚动容器
        function getScrollEl(outer) {
          return outer.querySelector('.vis-panel.vis-center');
        }

        // 保存当前滚动位置
        function saveScroll(ganttId) {
          var outer = document.getElementById(ganttId);
          if (!outer) return;
          var el = getScrollEl(outer);
          if (el) _ganttScrollTops[ganttId] = el.scrollTop;
        }

        // 恢复滚动位置
        function restoreScroll(ganttId) {
          var outer = document.getElementById(ganttId);
          if (!outer || !_ganttScrollTops[ganttId] || _ganttScrollTops[ganttId] <= 0) return;
          var el = getScrollEl(outer);
          if (el) el.scrollTop = _ganttScrollTops[ganttId];
        }

        // 仅调整高度，不 redraw
        function resizeGanttHeight(ganttId) {
          var outer = document.getElementById(ganttId);
          if (!outer) return;
          var inner = outer.querySelector('.html-widget') || outer;
          var rect = outer.getBoundingClientRect();
          var vh = window.visualViewport ? window.visualViewport.height : window.innerHeight;
          var topInViewport = Math.max(rect.top, 0);
          var avail = Math.max(vh - topInViewport - BOTTOM_PAD, MIN_HEIGHT);
          var css = avail + 'px';
          outer.style.minHeight = MIN_HEIGHT + 'px';
          outer.style.height    = css;
          inner.style.minHeight = MIN_HEIGHT + 'px';
          inner.style.height    = css;
          var timelineRoot = outer.querySelector('.vis-timeline');
          if (timelineRoot) timelineRoot.style.height = css;
        }

        // 调整高度 + redraw，但保持滚动位置
        function resizeGanttFull(ganttId) {
          saveScroll(ganttId);
          resizeGanttHeight(ganttId);
          var outer = document.getElementById(ganttId);
          if (!outer) return;
          var tl = window.__ganttGetTimeline(outer);
          if (tl && tl.redraw) tl.redraw();
          requestAnimationFrame(function() {
            restoreScroll(ganttId);
          });
        }

        // 监听甘特图内部滚动，实时记录位置
        function hookScrollSave(ganttId) {
          var outer = document.getElementById(ganttId);
          if (!outer || outer._scrollHooked) return;
          outer._scrollHooked = true;
          var el = getScrollEl(outer);
          if (el) {
            el.addEventListener('scroll', function() {
              _ganttScrollTops[ganttId] = el.scrollTop;
            }, { passive: true });
          }
        }

        // 窗口 resize（节流）— 所有 widget
        var _resizeTimer = null;
        function onResize() {
          if (_resizeTimer) return;
          _resizeTimer = setTimeout(function() {
            _resizeTimer = null;
            GANTT_IDS.forEach(resizeGanttFull);
          }, 200);
        }
        window.addEventListener('resize', onResize);
        if (window.visualViewport) {
          window.visualViewport.addEventListener('resize', onResize);
        }

        // 页面滚动只调高度（不 redraw），用节流避免频繁触发布局
        var _pageScrollTimer = null;
        window.addEventListener('scroll', function() {
          if (_pageScrollTimer) return;
          _pageScrollTimer = setTimeout(function() {
            _pageScrollTimer = null;
            GANTT_IDS.forEach(resizeGanttHeight);
          }, 200);
        }, { passive: true });

        // shiny:value 仅在 widget 初次创建时触发
        $(document).on('shiny:value', function(e) {
          GANTT_IDS.forEach(function(gid) {
            var isTarget = (e.name === gid) || (e.target && e.target.id === gid);
            if (isTarget) {
              setTimeout(function() {
                resizeGanttHeight(gid);
                hookScrollSave(gid);
              }, 200);
            }
          });
        });

        // 页面就绪
        $(document).ready(function() {
          setTimeout(function() {
            GANTT_IDS.forEach(function(gid) {
              resizeGanttHeight(gid);
              hookScrollSave(gid);
            });
          }, 800);
        });
      })();

      // 猴子补丁：替换 timevis widget 的 setItems/setGroups
      // 默认用增量更新（保留滚动位置），筛选器触发时切回全量重建
      (function() {
        var GANTT_IDS = ['my_gantt', 'my_gantt2'];
        var _updateMode = 'full';  // 'full' = 原始 clear+add, 'incremental' = update/remove

        Shiny.addCustomMessageHandler('ganttSetUpdateMode', function(mode) {
          _updateMode = mode;
        });

        function patchWidget(ganttId) {
          var outer = document.getElementById(ganttId);
          if (!outer || !outer.widget || outer._proxyPatched) return;
          outer._proxyPatched = true;
          var widget = outer.widget;
          var tl = window.__ganttGetTimeline(outer);
          if (!tl) { outer._proxyPatched = false; return; }

          // 保存原始方法
          var origSetItems = widget.setItems.bind(widget);
          var origSetGroups = widget.setGroups.bind(widget);

          widget.setItems = function(params) {
            if (_updateMode === 'full') {
              origSetItems(params);
              _updateMode = 'full';  // 重置
              return;
            }
            // 增量模式
            if (!tl.itemsData) return;
            var newData = params.data || [];
            var newIds = newData.map(function(d) { return String(d.id); });
            var existingIds = tl.itemsData.getIds().map(String);
            var toRemove = existingIds.filter(function(id) { return newIds.indexOf(id) === -1; });
            if (toRemove.length > 0) tl.itemsData.remove(toRemove);
            if (newData.length > 0) tl.itemsData.update(newData);
          };

          widget.setGroups = function(params) {
            if (_updateMode === 'full') {
              origSetGroups(params);
              return;
            }
            // 增量模式
            if (!tl.groupsData) return;
            var newData = params.data || [];
            var newIds = newData.map(function(d) { return String(d.id); });
            var existingIds = tl.groupsData.getIds().map(String);
            var toRemove = existingIds.filter(function(id) { return newIds.indexOf(id) === -1; });
            if (toRemove.length > 0) tl.groupsData.remove(toRemove);
            if (newData.length > 0) tl.groupsData.update(newData);
          };
        }

        // MutationObserver 在 widget 创建时立即 patch
        GANTT_IDS.forEach(function(gid) {
          var el = document.getElementById(gid);
          if (el) {
            var obs = new MutationObserver(function() {
              if (el.widget) { patchWidget(gid); obs.disconnect(); }
            });
            obs.observe(el, { childList: true, subtree: true, attributes: true });
          }
        });
        $(document).on('shiny:value', function(e) {
          GANTT_IDS.forEach(function(gid) {
            if (e.name === gid || (e.target && e.target.id === gid)) {
              setTimeout(function() { patchWidget(gid); }, 10);
              setTimeout(function() { patchWidget(gid); }, 100);
            }
          });
        });
      })();

      // 项目标题点击：用 timeline.getEventProperties(event)（不依赖 el.timeline，应使用 el.widget.timeline）
      (function() {
        var GANTT_IDS = ['my_gantt', 'my_gantt2'];
        function ensureProjectSummaryClick(ganttId) {
          var outer = document.getElementById(ganttId);
          if (!outer || outer._projectSummaryClickDom) return;
          outer._projectSummaryClickDom = true;
          outer.addEventListener('click', function(ev) {
            var tl = window.__ganttGetTimeline(outer);
            if (!tl || typeof tl.getEventProperties !== 'function') return;
            var props = tl.getEventProperties(ev);
            if (!props || props.what !== 'group-label') return;
            var gid = props.group;
            if (gid === null || typeof gid === 'undefined') return;
            gid = String(gid);
            if (gid.indexOf('MOCK_SEPARATOR_') !== 0) return;
            var projId = gid.substring('MOCK_SEPARATOR_'.length);
            Shiny.setInputValue('project_header_clicked', { project_id: projId, nonce: Date.now() }, { priority: 'event' });
          }, false);
        }
        $(document).on('shiny:value', function(e) {
          var t = e && e.target;
          if (!t) return;
          GANTT_IDS.forEach(function(gid) {
            var host = t.id === gid ? t : (t.closest ? t.closest('#' + gid) : null);
            if (host) {
              setTimeout(function() { ensureProjectSummaryClick(gid); }, 0);
              setTimeout(function() { ensureProjectSummaryClick(gid); }, 100);
            }
          });
        });
        $(document).ready(function() {
          GANTT_IDS.forEach(function(gid) {
            setTimeout(function() { ensureProjectSummaryClick(gid); }, 0);
            setTimeout(function() { ensureProjectSummaryClick(gid); }, 600);
          });
        });
      })();
    ")),
    tags$script(HTML("
      // 甘特 / 会议决策 筛选「且条件 / 或条件」：点击即时改文案 + 同步 Shiny
      (function() {
        $(document).on('click', '#gantt_filter_combine_btn, #gantt2_filter_combine_btn, #mtg_filter_combine_btn, #mtg_new_filter_combine_btn', function(e) {
          e.preventDefault();
          var btn = $(this);
          var mode = btn.attr('data-mode') || 'and';
          mode = (mode === 'and') ? 'or' : 'and';
          btn.attr('data-mode', mode);
          btn.text(mode === 'and' ? '且条件' : '或条件');
          if (window.Shiny && Shiny.setInputValue) {
            var bid = btn.attr('id');
            var inputName = bid === 'gantt_filter_combine_btn' ? 'gantt_filter_combine_mode'
              : (bid === 'gantt2_filter_combine_btn' ? 'gantt2_filter_combine_mode'
              : (bid === 'mtg_filter_combine_btn' ? 'mtg_filter_combine_mode' : 'mtg_new_filter_combine_mode'));
            Shiny.setInputValue(inputName, mode, { priority: 'event' });
          }
        });
      })();
    ")),
    tags$script(HTML("
      // 执行人状态点击 & 会议编辑按钮：事件委托，每次点击附带唯一 nonce 保证 observeEvent 可重复触发
      $(document).on('click', 'a[id^=\"exec_status_\"], .exec-status-link', function(e) {
        e.preventDefault();
        var id = (this.id || '').replace('exec_status_', '');
        if (!id) return;
        Shiny.onInputChange('exec_status_clicked', id + '_' + Date.now());
      });
      $(document).on('click', '.btn-edit-mtg', function(e) {
        e.preventDefault();
        var id = $(this).attr('data-mtg-id');
        if (!id) return;
        Shiny.onInputChange('mtg_edit_clicked', id + '_' + Date.now());
      });
      $(document).on('click', '[id^=\"sm09_rename_\"]', function(e) {
        e.preventDefault();
        var did = this.id.replace('sm09_rename_', '');
        Shiny.onInputChange('sm09_rename_trigger', did + '_' + Date.now());
      });
      // 个人消息：折叠区标题点击（全局注册一次，避免 renderUI 重复追加）
      $(document).on('click', '.msg-section-header', function() {
        var body = $(this).next('.msg-section-body');
        if (body.length) {
          body.slideToggle(200);
          var arrow = $(this).find('.msg-section-arrow');
          arrow.text(body.is(':visible') ? ' ▼' : ' ▶');
        }
      });
      // 个人消息：项目名称点击 → 单项目甘特图
      $(document).on('click', '.msg-project-link', function(e) {
        e.preventDefault();
        var pid = $(this).attr('data-project-db-id');
        if (!pid) return;
        Shiny.onInputChange('msg_project_clicked', pid + '_' + Date.now());
      });
      // 个人消息：阶段名称点击 → 任务详情
      $(document).on('click', '.msg-stage-link', function(e) {
        e.preventDefault();
        var sid = $(this).attr('data-stage-instance-id');
        if (!sid) return;
        Shiny.onInputChange('msg_stage_clicked', sid + '_' + Date.now());
      });
      // 修改项目基础信息：删除中心按钮事件委托
      $(document).on('click', '.pc-del-center-btn', function(e) {
        e.preventDefault();
        var id = $(this).attr('data-id');
        if (!id) return;
        Shiny.onInputChange('pc_del_center_trigger', id + '_' + Date.now());
      });
    ")),
    tags$style(HTML("
      /* 紧凑进度条与里程碑控件：让 item 高度贴近内部文字，节省纵向空间 */
      .vis-item {
        font-size: 12px;
        line-height: 1.15;
      }
      .vis-item .vis-item-content {
        padding: 2px 5px;
        white-space: nowrap;
      }
    ")),
    tags$style(HTML("
      .vis-labelset { width: 240px !important; }
      .vis-current-time { width: 2px; background-color: red; z-index: 10; }
      /* 项目标题行（间隔行升级为项目标题） */
      .vis-group.gantt-row-sep {
        background-color: #D8DDE3 !important;
      }
      .vis-group.gantt-row-sep .vis-label,
      .vis-group.gantt-row-sep .vis-label .vis-inner {
        background-color: #B7BEC7 !important;
        color: #1F2933 !important;
      }
      .vis-group.gantt-row-sep .vis-label,
      .vis-group.gantt-row-sep .vis-label .vis-inner,
      .vis-group.gantt-row-sep .vis-label .vis-inner * {
        font-weight: 800 !important;
        font-size: 14px !important;
        line-height: 1.2 !important;
        font-family: 'Noto Sans CJK SC','Microsoft YaHei','PingFang SC','Helvetica Neue',Arial,sans-serif !important;
      }
      /* 间隔行（项目标题）：行高 = 文字 + 上下各 2px */
      .vis-labelset .vis-label.gantt-row-sep {
        padding: 0 !important;
      }
      .vis-labelset .vis-label.gantt-row-sep .vis-inner {
        padding: 2px 8px !important;
        line-height: 1.2 !important;
        box-sizing: border-box !important;
      }
      /* 左侧行头标签（用户点击的是这里，不是右侧 .vis-group 轨道） */
      .vis-labelset .vis-label.gantt-row-sep,
      .vis-labelset .vis-label.gantt-row-sep .vis-inner {
        cursor: pointer !important;
      }
      .vis-group.gantt-row-sep .vis-label {
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
    ")),
    tags$style(HTML("
      /* 所有弹出窗口横向宽度 +20%（相对 Bootstrap 3 常见默认：中 600px / 大 900px / 小 300px） */
      .modal-dialog:not(.modal-sm):not(.modal-lg) {
        width: 720px !important;
        max-width: min(720px, 98vw) !important;
      }
      .modal-dialog.modal-lg {
        width: 1080px !important;
        max-width: min(1080px, 98vw) !important;
      }
      /* 项目汇总：在 modal-lg 基础上再 +30% 宽（1080 × 1.3 ≈ 1404） */
      .modal-dialog.modal-lg:has(#project_summary_tabs) {
        width: 1404px !important;
        max-width: min(1404px, 98vw) !important;
      }
      .modal-dialog.modal-sm {
        width: 360px !important;
        max-width: min(360px, 98vw) !important;
      }
      /* bslib / Bootstrap 5 下 modal-lg 常用 max-width */
      @media (min-width: 992px) {
        .modal-lg { max-width: 1080px !important; }
        .modal-lg:has(#project_summary_tabs) { max-width: min(1404px, 98vw) !important; }
      }
    ")),
    tags$style(HTML("
      .gantt-row-flex {
        display: flex;
        flex-direction: row;
        align-items: flex-start;
        width: 100%;
        gap: 0;
      }
      .gantt-sidebar-wrap {
        flex: 0 0 300px;
        max-width: 320px;
        border-right: 1px solid #e0e0e0;
        padding-right: 12px;
        margin-right: 0;
        position: sticky;
        top: 8px;
        align-self: flex-start;
        max-height: calc(100vh - 24px);
        overflow-y: auto;
        transition: flex-basis 0.2s ease, max-width 0.2s ease;
      }
      .gantt-sidebar-wrap.collapsed {
        flex: 0 0 26px;
        max-width: 26px;
        padding-right: 0;
        overflow: visible;
      }
      .gantt-sidebar-wrap.collapsed .gantt-sidebar-body {
        display: none !important;
      }
      .gantt-sidebar-wrap.collapsed #gantt_sidebar_toggle,
      .gantt-sidebar-wrap.collapsed #meeting_sidebar_toggle {
        width: 100% !important;
        max-width: 24px;
        min-width: 22px;
        padding: 5px 2px !important;
        margin-bottom: 0 !important;
        font-size: 13px;
        line-height: 1;
      }
      .btn-del-row {
        flex-shrink: 0;
        align-self: flex-start;
        margin-top: 24px;
        margin-left: 6px;
        padding: 4px 8px !important;
        font-size: 13px !important;
        line-height: 1.2 !important;
        color: #c62828 !important;
        border: 1px solid #e0e0e0;
        background: #fff !important;
        cursor: pointer;
      }
      .btn-del-row:hover { background: #ffebee !important; border-color: #ef9a9a; }
      .gantt-main-wrap {
        flex: 1 1 auto;
        min-width: 0;
        padding-left: 12px;
      }
      /* 甘特在 timevis 首帧渲染前仍有占位，避免整片纯白 */
      .gantt-chart-host {
        min-height: 58vh;
        background: #f0f2f5;
        border-radius: 6px;
        padding: 4px 6px 8px 6px;
        box-sizing: border-box;
      }
      @media (max-width: 767px) {
        .gantt-row-flex { flex-direction: column !important; }
        .gantt-sidebar-wrap {
          flex: 1 1 auto !important;
          max-width: 100% !important;
          border-right: none;
          border-bottom: 1px solid #e0e0e0;
          padding-bottom: 10px;
          margin-bottom: 8px;
          position: relative;
          max-height: none;
        }
        .gantt-sidebar-wrap.collapsed .gantt-sidebar-body { display: none !important; }
        .gantt-main-wrap { padding-left: 0; }
      }
      /* 个人消息 Tab */
      .msg-panel { border: 1px solid #e0e0e0; border-radius: 6px; margin-bottom: 8px; background: #fff; }
      .msg-panel-heading { padding: 6px 12px; border-bottom: 1px solid #e0e0e0; border-radius: 6px 6px 0 0; font-size: 14px; display: flex; justify-content: space-between; align-items: center; }
      .msg-panel-body { padding: 8px 12px; font-size: 13px; line-height: 1.5; }
      .msg-badge { display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 12px; font-weight: 600; margin-right: 6px; }
      .msg-badge-critical { background: #F44336; color: #fff; }
      .msg-badge-high { background: #FF6F00; color: #fff; }
      .msg-badge-medium { background: #F57C00; color: #fff; }
      .msg-badge-low { background: #2196F3; color: #fff; }
      .msg-category-tag { display: inline-block; padding: 1px 6px; border-radius: 3px; font-size: 12px; background: #ECEFF1; color: #37474F; margin-right: 4px; }
      .msg-summary-card { display: inline-block; padding: 8px 16px; border-radius: 6px; margin-right: 12px; text-align: center; min-width: 80px; }
      .msg-summary-card .count { font-size: 24px; font-weight: 700; }
      .msg-summary-card .label { font-size: 12px; margin-top: 2px; }
      .msg-section-header { font-weight: 700; font-size: 15px; padding: 8px 0; margin-bottom: 8px; border-bottom: 2px solid #e0e0e0; cursor: pointer; }
      .msg-section-header:hover { background: #fafafa; }
      /* 会议决策与甘特共用 gantt-row-flex / gantt-sidebar-wrap / gantt-main-wrap */
      .meeting-panel {
        border: 1px solid #ddd;
        border-radius: 6px;
        margin-bottom: 16px;
        background: #fff;
      }
      .meeting-panel-heading {
        background: #f5f5f5;
        padding: 10px 14px;
        border-bottom: 1px solid #ddd;
        border-radius: 6px 6px 0 0;
        font-weight: bold;
        font-size: 15px;
        display: flex;
        justify-content: space-between;
        align-items: center;
      }
      .meeting-panel-body {
        padding: 12px 14px;
      }
      .meeting-project-group {
        border-left: 3px solid #2196F3;
        padding-left: 12px;
        margin-bottom: 12px;
      }
      .meeting-decision-item {
        padding: 8px 0;
        border-bottom: 1px solid #eee;
      }
      .meeting-decision-item:last-child { border-bottom: none; }
      .executor-tag {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 3px;
        margin: 2px 4px 2px 0;
        font-size: 13px;
      }
      .executor-pending { color: #C62828; font-weight: bold; }
      .executor-done { color: #2E7D32; font-weight: bold; }
      /* 历史会议：仅「已执行/未执行」上色，姓名保持默认色 */
      .executor-status-pending { color: #C62828; font-weight: bold; }
      .executor-status-done { color: #2E7D32; font-weight: bold; }
      a.meeting-exec-actionlink:hover { text-decoration: underline !important; }
      /* 历史会议执行人：每行 4 格，格内过长自动换行 */
      .meeting-exec-wrap { margin-top: 8px; font-size: 13px; line-height: 1.5; width: 100%; box-sizing: border-box; }
      .meeting-exec-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 8px 12px;
        margin-top: 6px;
        width: 100%;
        box-sizing: border-box;
      }
      .meeting-exec-cell {
        min-width: 0;
        max-width: 100%;
        word-wrap: break-word;
        overflow-wrap: anywhere;
        word-break: break-word;
        border: 1px solid #e8e8e8;
        border-radius: 4px;
        padding: 6px 8px;
        box-sizing: border-box;
        background: #fafafa;
        align-self: start;
      }
      .meeting-exec-cell a.meeting-exec-actionlink {
        display: block;
        white-space: normal !important;
        word-break: break-word;
        overflow-wrap: anywhere;
      }
    ")),
    tags$script(HTML("
      $(document).on('click', '#gantt_sidebar_toggle, #meeting_sidebar_toggle, #msg_sidebar_toggle', function() {
        var col = $(this).closest('.gantt-sidebar-wrap');
        if (!col.length) return;
        col.toggleClass('collapsed');
        var c = col.hasClass('collapsed');
        if (c) {
          $(this).text('\\u25b6');
          $(this).attr('title', '\\u5c55\\u5f00\\u7b5b\\u9009');
        } else {
          $(this).text('\\u25c0 \\u6536\\u8d77\\u7b5b\\u9009');
          $(this).attr('title', '\\u6536\\u8d77\\u7b5b\\u9009');
        }
        setTimeout(function() { window.dispatchEvent(new Event('resize')); }, 150);
        setTimeout(function() { window.dispatchEvent(new Event('resize')); }, 400);
      });
    "))
  ),
  uiOutput("app_title_panel"),
  uiOutput("current_user_display"),
  tabsetPanel(
    id = "main_tabs",
    type = "tabs",
    tabPanel(
      "个人消息",
      value = "tab_messages",
      uiOutput("personal_msg_ui")
    ),
    tabPanel(
      "项目甘特图",
      value = "tab_gantt",
      fluidRow(
        column(
          12,
          tags$div(
            id = "gantt_row_flex",
            class = "gantt-row-flex",
            tags$div(
              id = "gantt_sidebar_col",
              class = "gantt-sidebar-wrap",
              tags$button(
                type = "button",
                id = "gantt_sidebar_toggle",
                class = "btn btn-default btn-sm",
                title = "收起筛选",
                style = "width: 100%; margin-bottom: 10px; white-space: nowrap;",
                "◀ 收起筛选"
              ),
              tags$div(
                class = "gantt-sidebar-body",
                # ---- Tab1 筛选器：参与/负责项目 ----
                conditionalPanel(
                  condition = "typeof input.gantt_sub_tabs === 'undefined' || !input.gantt_sub_tabs || input.gantt_sub_tabs == 'gantt_view1'",
                  tags$div(
                    class = "panel panel-default",
                    style = "margin-bottom: 8px;",
                    tags$div(class = "panel-heading", style = "padding: 8px 12px; font-size: 14px;", tags$b("筛选与刷新")),
                    tags$div(
                      class = "panel-body",
                      style = "padding: 10px 12px;",
                      tags$div(
                        style = "display: flex; flex-direction: row; align-items: center; flex-wrap: wrap; gap: 10px; margin-bottom: 12px;",
                        actionButton("gantt_refresh", "🔄 从数据库刷新", class = "btn btn-primary", style = "margin: 0;"),
                        tags$button(
                          type = "button",
                          id = "gantt_filter_combine_btn",
                          class = "btn btn-default",
                          style = "font-size: 13px; padding: 5px 14px; line-height: 1.35; font-weight: 700; flex-shrink: 0; margin: 0;",
                          `data-mode` = "and",
                          "且条件"
                        )
                      ),
                      selectInput("filter_type", "项目类型", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("filter_name", "项目名称", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("filter_manager", "项目负责人", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("filter_participant", "项目参与人员", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("filter_importance", "重要紧急程度", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("filter_research_group", "课题组", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("filter_hospital", "相关医院（有中心）", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("filter_manager_group", "负责人专业组", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      div(
                        style = "margin-top: 6px; font-size: 14px;",
                        checkboxInput("filter_include_archived", "包含已结题项目", value = FALSE)
                      )
                    )
                  )
                ),
                # ---- Tab2 筛选器：未参与/负责项目 ----
                conditionalPanel(
                  condition = "input.gantt_sub_tabs == 'gantt_view2'",
                  tags$div(
                    class = "panel panel-default",
                    style = "margin-bottom: 8px;",
                    tags$div(class = "panel-heading", style = "padding: 8px 12px; font-size: 14px;", tags$b("筛选与刷新")),
                    tags$div(
                      class = "panel-body",
                      style = "padding: 10px 12px;",
                      tags$div(
                        style = "display: flex; flex-direction: row; align-items: center; flex-wrap: wrap; gap: 10px; margin-bottom: 12px;",
                        actionButton("gantt2_refresh", "🔄 从数据库刷新", class = "btn btn-primary", style = "margin: 0;"),
                        tags$button(
                          type = "button",
                          id = "gantt2_filter_combine_btn",
                          class = "btn btn-default",
                          style = "font-size: 13px; padding: 5px 14px; line-height: 1.35; font-weight: 700; flex-shrink: 0; margin: 0;",
                          `data-mode` = "and",
                          "且条件"
                        )
                      ),
                      selectInput("gantt2_filter_type", "项目类型", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("gantt2_filter_name", "项目名称", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("gantt2_filter_manager", "项目负责人", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("gantt2_filter_participant", "项目参与人员", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("gantt2_filter_importance", "重要紧急程度", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("gantt2_filter_research_group", "课题组", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("gantt2_filter_hospital", "相关医院（有中心）", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("gantt2_filter_manager_group", "负责人专业组", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      div(
                        style = "margin-top: 6px; font-size: 14px;",
                        checkboxInput("gantt2_filter_include_archived", "包含已结题项目", value = FALSE)
                      )
                    )
                  )
                ),
                uiOutput("gantt_db_msg")
              )
            ),
            tags$div(
              class = "gantt-main-wrap",
              HTML("
             <div style='margin-bottom: 10px; font-size: 12px; color: #555;'>
               <b>进度诊断图例：</b>
                       <span style='margin-left:15px; padding:2px 8px; background:#EDEDED; border:1px solid #D0D0D0; border-radius:3px; color:#555;'>未制定计划 (白灰)</span>
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
              tabsetPanel(
                id = "gantt_sub_tabs",
                type = "tabs",
                tabPanel("参与/负责项目甘特图", value = "gantt_view1",
                  tags$div(class = "gantt-chart-host",
                    timevisOutput("my_gantt", height = "100%")
                  )
                ),
                tabPanel("未参与/负责项目甘特图", value = "gantt_view2",
                  tags$div(class = "gantt-chart-host",
                    timevisOutput("my_gantt2", height = "100%")
                  )
                )
              )
            )
          )
        )
      )
    ),
    tabPanel(
      "会议决策",
      value = "tab_meeting",
      uiOutput("meeting_tab_ui")
    )
  )
)
ivd_perf_ts("GLOBAL after ui fluidPage() built")
# Shiny 在专用 envir 中加载 app.R；默认 source() 会把函数落到 .GlobalEnv，与 APP_TZ_CN 所在环境脱节 → today_beijing() 报找不到 APP_TZ_CN
source("R/ivd_server_helpers.R", encoding = "UTF-8", local = environment())
ivd_perf_ts("GLOBAL after source(ivd_server_helpers.R)")

# ---------------- 3. Server 逻辑 ----------------
server <- function(input, output, session) {
  ivd_perf_ts("server() enter (new session)")
  t_ivd_sess0 <- proc.time()
  # 定位「server enter → 首条业务日志」空档：首次把本轮输出推送到浏览器后打点
  session$onFlushed(function() {
    ivd_perf_elapsed(t_ivd_sess0, "sess|onFlushed#1 (first Shiny cycle to client)")
  }, once = TRUE)
  .ivd_sess_once <- new.env(parent = emptyenv())
  today <- today_beijing()   # session 级「业务日」= 北京日历日

  # ----- 数据库连接与表浏览（使用全局连接池） -----
  pg_con <- pg_pool

  gantt_db_error <- reactiveVal(NULL)
  gantt_force_refresh <- reactiveVal(0L)
  gantt_use_incremental <- reactiveVal(FALSE)  # 用户交互保存时设 TRUE，筛选器触发时保持 FALSE
  gantt_init_data <- reactiveVal(NULL)
  # 首次进入页面时跳过一次筛选 debounce，避免首屏多等 400ms 才发查询（仍保留后续防抖）
  gantt_skip_query_debounce_once <- reactiveVal(TRUE)
  task_edit_context <- reactiveVal(NULL)
  sample_row_count <- reactiveVal(0L)
  milestone_row_count <- reactiveVal(0L)
  contrib_row_count <- reactiveVal(0L)
  remark_row_count <- reactiveVal(0L)
  conflict_resolution_state <- reactiveVal(NULL)
  stage_maintain_context <- reactiveVal(NULL)
  sm09_rename_def_id <- reactiveVal(NULL)
  stage_maintain_tab1_refresh <- reactiveVal(0L)
  personnel_center_context <- reactiveVal(NULL)
  pc_center_refresh <- reactiveVal(0L)

  # ---------- 会议决策 reactiveVal ----------
  meeting_force_refresh <- reactiveVal(0L)
  meeting_edit_ctx <- reactiveVal(NULL)
  meeting_edit_nonce <- reactiveVal(0L)
  meeting_new_add_point_pid <- reactiveVal(NULL)
  meeting_new_modal_refresh <- reactiveVal(0L)
  # 新建会议：弹窗 — none | common | pt_single（点击某一条要点后针对该要点登记）
  meeting_new_bulk_mode <- reactiveVal("none")
  meeting_new_bulk_proj_id <- reactiveVal(NA_integer_)
  meeting_new_bulk_nblocks <- reactiveVal(1L)
  meeting_new_pt_fb_id <- reactiveVal(NA_integer_)
  meeting_new_pt_proj_id <- reactiveVal(NA_integer_)
  meeting_new_pt_nblocks <- reactiveVal(1L)
  executor_modal_ctx <- reactiveVal(NULL)

  # ---------- 个人消息 ----------
  msg_force_refresh <- reactiveVal(0L)
  msg_gantt_project_id <- reactiveVal(NULL)

  # ---------- 项目汇总 — 管理项目要点 ----------
  proj_summary_ctx <- reactiveVal(NULL)
  proj_summary_remark_row_count <- reactiveVal(0L)
  proj_summary_remark_refresh <- reactiveVal(0L)

  .EDIT_MEETING_MAX_DEC_ID <- 3000L
  ivd_perf_elapsed(t_ivd_sess0, "sess|after reactiveVal / session init block")

  ivd_perf_elapsed(t_ivd_sess0, "sess|after in-server function defs (before first output$)")

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
    if (is.null(.ivd_sess_once$auth_first)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|current_user_auth FIRST run (05人员表等)")
      .ivd_sess_once$auth_first <- TRUE
    }
    wid <- session$request$HTTP_X_USER
    wid <- if (is.null(wid)) "" else trimws(as.character(wid))
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
    if (!nzchar(wid)) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
    tryCatch({
      r <- DBI::dbGetQuery(pg_con, "SELECT id, \"姓名\", \"组别\", \"数据库权限等级\", \"人员状态\" FROM public.\"05人员表\" WHERE \"工号\" = $1 LIMIT 1", params = list(wid))
      if (nrow(r) == 0) return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", is_super_admin = FALSE))
      status <- trimws(as.character(r[["人员状态"]][1]))
      level  <- trimws(as.character(r[["数据库权限等级"]][1]))
      if (is.na(status) || status != "在职") return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", disallowed_subquery = "SELECT 0 WHERE 1=0", is_super_admin = FALSE))
      if (is.na(level) || level == "deny_access") return(list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", disallowed_subquery = "SELECT 0 WHERE 1=0", is_super_admin = FALSE))
      name <- if ("姓名" %in% names(r) && !is.na(r[["姓名"]][1])) trimws(as.character(r[["姓名"]][1])) else ""
      pid <- as.integer(r[["id"]][1])
      if (level == "super_admin") return(list(allow_all = TRUE, allow_none = FALSE, allowed_subquery = "", disallowed_subquery = "SELECT 0 WHERE 1=0", work_id = wid, name = name, is_super_admin = TRUE, can_manage_project = TRUE, personnel_id = pid))
      if (level == "manager") return(list(allow_all = TRUE, allow_none = FALSE, allowed_subquery = "", disallowed_subquery = "SELECT 0 WHERE 1=0", work_id = wid, name = name, is_super_admin = FALSE, can_manage_project = TRUE, personnel_id = pid))
      .empty_disq <- "SELECT 0 WHERE 1=0"
      if (level == "common_user") {
        subq <- sprintf(
          "SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" = %d UNION SELECT \"04项目总表_id\" FROM public.\"_nc_m2m_04项目总表_05人员表\" WHERE \"05人员表_id\" = %d",
          pid, pid
        )
        disq <- sprintf("SELECT id FROM public.\"04项目总表\" WHERE id NOT IN (%s)", subq)
        return(list(allow_all = FALSE, allow_none = FALSE, allowed_subquery = subq, disallowed_subquery = disq, work_id = wid, name = name, is_super_admin = FALSE, can_manage_project = FALSE, personnel_id = pid))
      }
      if (level == "group_manager") {
        subq <- sprintf(
          "SELECT id FROM public.\"04项目总表\" WHERE \"05人员表_id\" IN (SELECT id FROM public.\"05人员表\" WHERE \"组别\" IS NOT DISTINCT FROM (SELECT \"组别\" FROM public.\"05人员表\" WHERE id = %d)) UNION SELECT \"04项目总表_id\" FROM public.\"_nc_m2m_04项目总表_05人员表\" WHERE \"05人员表_id\" IN (SELECT id FROM public.\"05人员表\" WHERE \"组别\" IS NOT DISTINCT FROM (SELECT \"组别\" FROM public.\"05人员表\" WHERE id = %d))",
          pid, pid
        )
        disq <- sprintf("SELECT id FROM public.\"04项目总表\" WHERE id NOT IN (%s)", subq)
        return(list(allow_all = FALSE, allow_none = FALSE, allowed_subquery = subq, disallowed_subquery = disq, work_id = wid, name = name, is_super_admin = FALSE, can_manage_project = TRUE, personnel_id = pid))
      }
      list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", disallowed_subquery = .empty_disq, is_super_admin = FALSE)
    }, error = function(e) list(allow_all = FALSE, allow_none = TRUE, allowed_subquery = "", disallowed_subquery = "SELECT 0 WHERE 1=0", is_super_admin = FALSE))
  })

  # 判断当前用户是否是指定项目的项目负责人
  is_current_user_project_manager <- function(proj_row_id) {
    auth <- current_user_auth()
    if (is.null(auth) || is.null(auth$personnel_id)) return(FALSE)
    if (isTRUE(auth$is_super_admin)) return(FALSE)  # super_admin 单独处理

    tryCatch({
      r <- DBI::dbGetQuery(pg_con,
        'SELECT "05人员表_id" FROM public."04项目总表" WHERE id = $1',
        params = list(as.integer(proj_row_id)))

      if (nrow(r) > 0 && !is.na(r[["05人员表_id"]][1])) {
        return(as.integer(r[["05人员表_id"]][1]) == auth$personnel_id)
      }
      return(FALSE)
    }, error = function(e) FALSE)
  }

  stage_definition_df <- reactive({
    if (is.null(.ivd_sess_once$sd_first)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|stage_definition_df FIRST run")
      .ivd_sess_once$sd_first <- TRUE
    }
    t0_sd <- proc.time()
    ivd_perf_ts("stage_definition_df BEGIN")
    on.exit(ivd_perf_ts(paste0("stage_definition_df END elapsed_s=", round((proc.time() - t0_sd)[3], 3))), add = TRUE)
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
    if (is.null(.ivd_sess_once$sc_first)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|stage_catalog FIRST run")
      .ivd_sess_once$sc_first <- TRUE
    }
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

  # 优先使用自定义名称，无则回退到 stage_label_for_key
  display_name_for_ctx <- function(ctx) {
    dn <- ctx$task_display_name
    if (!is.na(dn) && nzchar(dn)) dn else stage_label_for_key(ctx$task_name)
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

  # 根据 stage_catalog（即 08 表）判断某阶段是否支持样本追踪
  # 取代全代码中对 "S09_验证试验开展与数据管理" 的字符串硬判断
  stage_supports_sample <- function(stage_key, project_type = NA_character_) {
    defs <- stage_catalog()$defs
    if (!is.na(project_type) && nzchar(project_type) && project_type %in% defs$project_type) {
      row <- defs %>% filter(stage_key == !!stage_key, project_type == !!project_type) %>% slice(1)
    } else {
      row <- defs %>% filter(stage_key == !!stage_key) %>% slice(1)
    }
    if (nrow(row) == 0) return(FALSE)
    isTRUE(row$supports_sample[1])
  }

  # 筛选条件防抖，避免快速切换筛选时多次取消未完成的查询导致 “Closing open result set” 警告
  gantt_filter_state <- reactive({
    list(
      refresh = input$gantt_refresh,
      type = input$filter_type,
      name = input$filter_name,
      manager = input$filter_manager,
      participant = input$filter_participant,
      importance = input$filter_importance,
      research_group = input$filter_research_group,
      hospital = input$filter_hospital,
      manager_group = input$filter_manager_group,
      include_archived = input$filter_include_archived,
      combine_mode = {
        v <- input$gantt_filter_combine_mode
        if (is.null(v) || !v %in% c("and", "or")) "and" else v
      }
    )
  })
  gantt_filter_state_debounced <- debounce(gantt_filter_state, 400)

  # 供 gantt_sql_filter_bundle 使用：首帧用即时筛选状态，之后用 debounce（减轻快速点选筛选时的重复查询）
  gantt_filter_state_for_query <- reactive({
    if (is.null(.ivd_sess_once$gfsq_first)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|gantt_filter_state_for_query FIRST")
      .ivd_sess_once$gfsq_first <- TRUE
    }
    st <- gantt_filter_state()
    if (isTRUE(shiny::isolate(gantt_skip_query_debounce_once()))) {
      gantt_skip_query_debounce_once(FALSE)
      return(st)
    }
    gantt_filter_state_debounced()
  })

  # 甘特双视图共用：筛选维度 + WHERE 子句（避免 gantt_data_db / gantt_data_all_stages 重复跑人员/医院子查询）
  gantt_sql_filter_bundle <- reactive({
    if (is.null(.ivd_sess_once$sfb_first)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|gantt_sql_filter_bundle FIRST run")
      .ivd_sess_once$sfb_first <- TRUE
    }
    t0_bun <- proc.time()
    ivd_perf_ts("gantt_sql_filter_bundle BEGIN")
    on.exit(ivd_perf_ts(paste0("gantt_sql_filter_bundle END elapsed_s=", round((proc.time() - t0_bun)[3], 3))), add = TRUE)
    input$gantt_refresh
    gantt_force_refresh()
    state <- gantt_filter_state_for_query()
    auth <- current_user_auth()
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) {
      return(list(ok = FALSE, err = "no_db"))
    }
    if (auth$allow_none) {
      return(list(ok = FALSE, err = "no_auth"))
    }
    ft <- if (is.null(state$type)) character(0) else state$type
    fn <- if (is.null(state$name)) character(0) else state$name
    fm <- if (is.null(state$manager)) character(0) else state$manager
    fp <- if (is.null(state$participant)) character(0) else state$participant
    fi <- if (is.null(state$importance)) character(0) else state$importance
    fh <- if (is.null(state$hospital)) character(0) else state$hospital
    frg <- if (is.null(state$research_group)) character(0) else state$research_group
    fmg <- if (is.null(state$manager_group)) character(0) else state$manager_group
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
    proj_ids_rg <- integer(0)
    if (length(frg) > 0) {
      qrg <- paste0('SELECT id FROM public."04项目总表" WHERE "课题组" IN (', paste(rep("$", length(frg)), seq_along(frg), sep = "", collapse = ","), ")")
      proj_ids_rg <- DBI::dbGetQuery(pg_con, qrg, params = as.list(frg))$id
    }
    manager_group_ids <- integer(0)
    if (length(fmg) > 0) {
      qmg <- paste0('SELECT id FROM public."05人员表" WHERE "组别" IN (', paste(rep("$", length(fmg)), seq_along(fmg), sep = "", collapse = ","), ")")
      manager_group_ids <- DBI::dbGetQuery(pg_con, qmg, params = as.list(fmg))$id
    }
    where_stage <- c('project_id IS NOT NULL', 'project_type IS NOT NULL')
    if (!auth$allow_all) where_stage <- c(where_stage, paste0("project_db_id IN (", auth$allowed_subquery, ")"))
    dim_built <- build_gantt_filter_dimension_parts(ft, fn, fi, manager_ids, proj_ids_participant, proj_ids_hosp, proj_ids_rg, manager_group_ids)
    combine_mode <- if (is.null(state$combine_mode)) "and" else state$combine_mode
    if (length(dim_built$dim_parts) > 0L) {
      if (identical(combine_mode, "or")) {
        where_stage <- c(where_stage, paste0("(", paste(dim_built$dim_parts, collapse = " OR "), ")"))
      } else {
        where_stage <- c(where_stage, dim_built$dim_parts)
      }
    }
    if (!isTRUE(state$include_archived)) {
      where_stage <- c(where_stage, 'COALESCE(project_is_active, true) = true')
    }
    list(
      ok = TRUE,
      where_stage = where_stage,
      params_stage = dim_built$params_stage
    )
  })

  output$gantt_db_msg <- renderUI({
    if (is.null(.ivd_sess_once$gantt_msg)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|output$gantt_db_msg renderUI first")
      .ivd_sess_once$gantt_msg <- TRUE
    }
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
  output$app_title_panel <- renderUI({
    if (is.null(.ivd_sess_once$title)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|output$app_title_panel renderUI first")
      .ivd_sess_once$title <- TRUE
    }
    titlePanel(paste("Snibe临床 - 项目进度管理看板 (当前日期:", format(Sys.time(), "%Y-%m-%d", tz = APP_TZ_CN), ")"))
  })

  output$current_user_display <- renderUI({
    if (is.null(.ivd_sess_once$user_disp)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|output$current_user_display renderUI first")
      .ivd_sess_once$user_disp <- TRUE
    }
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return(tags$div(style = "display: flex; align-items: center;",
      tags$p(style = "color: #888; font-size: 13px; margin: 4px 0;", "当前登录帐号：未登录"),
      tags$a(href = "/", "返回导航页", style = "margin-left: 20px; font-size: 13px;")))
    wid <- auth$work_id
    name <- auth$name
    if (!nzchar(wid) && !nzchar(name)) return(tags$div(style = "display: flex; align-items: center;",
      tags$p(style = "color: #888; font-size: 13px; margin: 4px 0;", "当前登录帐号：—"),
      tags$a(href = "/", "返回导航页", style = "margin-left: 20px; font-size: 13px;")))
    txt <- if (nzchar(name) && nzchar(wid)) paste0(name, "-", wid) else if (nzchar(wid)) wid else name
    tags$div(style = "display: flex; align-items: center;",
      tags$p(style = "color: #333; font-size: 13px; margin: 4px 0;", paste0("当前登录帐号：", txt)),
      tags$a(href = "/", "返回导航页", style = "margin-left: 20px; font-size: 13px;"))
  })

  # 筛选器选项：按当前用户权限仅索要可访问维度的 distinct 清单（数据库层筛选）
  gantt_filter_options <- reactive({
    if (is.null(.ivd_sess_once$gfo)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|gantt_filter_options reactive FIRST run")
      .ivd_sess_once$gfo <- TRUE
    }
    t0_go <- proc.time()
    ivd_perf_ts("gantt_filter_options BEGIN")
    on.exit(ivd_perf_ts(paste0("gantt_filter_options END elapsed_s=", round((proc.time() - t0_go)[3], 3))), add = TRUE)
    input$gantt_refresh
    auth <- current_user_auth()
    fetch_gantt_dim_filter_options(pg_con, auth)
  })

  # 筛选下拉选项来自 DB；控件在 UI 中静态声明，避免 renderUI 整页重建导致 input 销毁/重建 → 甘特反复全量重算
  observe({
    if (is.null(.ivd_sess_once$gfo_obs)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|observe(gantt_filter updateSelect) FIRST fire")
      .ivd_sess_once$gfo_obs <- TRUE
    }
    t0_fs <- proc.time()
    opts <- gantt_filter_options()
    if (is.null(opts)) return(NULL)
    isolate({
      ft <- input$filter_type
      fn <- input$filter_name
      fm <- input$filter_manager
      fp <- input$filter_participant
      fi <- input$filter_importance
      frg <- input$filter_research_group
      fh <- input$filter_hospital
      fmg <- input$filter_manager_group
    })
    updateSelectInput(session, "filter_type", choices = opts$types, selected = sel_intersect_choices(ft, opts$types))
    updateSelectInput(session, "filter_name", choices = opts$names, selected = sel_intersect_choices(fn, opts$names))
    updateSelectInput(session, "filter_manager", choices = opts$managers, selected = sel_intersect_choices(fm, opts$managers))
    updateSelectInput(session, "filter_participant", choices = opts$participants, selected = sel_intersect_choices(fp, opts$participants))
    updateSelectInput(session, "filter_importance", choices = opts$importance, selected = sel_intersect_choices(fi, opts$importance))
    updateSelectInput(session, "filter_research_group", choices = opts$research_groups, selected = sel_intersect_choices(frg, opts$research_groups))
    updateSelectInput(session, "filter_hospital", choices = opts$hospitals, selected = sel_intersect_choices(fh, opts$hospitals))
    updateSelectInput(session, "filter_manager_group", choices = opts$manager_groups, selected = sel_intersect_choices(fmg, opts$manager_groups))
    ivd_perf_elapsed(t0_fs, "observe gantt_filter updateSelectInput×9")
  })

  # 新建会议：甘特同款维度筛「选择项目」下拉选项
  meeting_new_gantt_filter_options <- reactive({
    input$mtg_refresh
    input$mtg_new_dim_refresh %||% 0
    meeting_force_refresh()
    auth <- current_user_auth()
    fetch_gantt_dim_filter_options(pg_con, auth)
  })

  observe({
    opts <- meeting_new_gantt_filter_options()
    if (is.null(opts)) return(NULL)
    isolate({
      ft <- input$mtg_new_filter_type
      fn <- input$mtg_new_filter_name
      fm <- input$mtg_new_filter_manager
      fp <- input$mtg_new_filter_participant
      fi <- input$mtg_new_filter_importance
      frg <- input$mtg_new_filter_research_group
      fh <- input$mtg_new_filter_hospital
    })
    updateSelectInput(session, "mtg_new_filter_type", choices = opts$types, selected = sel_intersect_choices(ft, opts$types))
    updateSelectInput(session, "mtg_new_filter_name", choices = opts$names, selected = sel_intersect_choices(fn, opts$names))
    updateSelectInput(session, "mtg_new_filter_manager", choices = opts$managers, selected = sel_intersect_choices(fm, opts$managers))
    updateSelectInput(session, "mtg_new_filter_participant", choices = opts$participants, selected = sel_intersect_choices(fp, opts$participants))
    updateSelectInput(session, "mtg_new_filter_importance", choices = opts$importance, selected = sel_intersect_choices(fi, opts$importance))
    updateSelectInput(session, "mtg_new_filter_research_group", choices = opts$research_groups, selected = sel_intersect_choices(frg, opts$research_groups))
    updateSelectInput(session, "mtg_new_filter_hospital", choices = opts$hospitals, selected = sel_intersect_choices(fh, opts$hospitals))
  })

  meeting_new_filter_state <- reactive({
    meeting_force_refresh()
    input$mtg_refresh
    input$mtg_new_dim_refresh %||% 0
    list(
      type = if (is.null(input$mtg_new_filter_type)) character(0) else input$mtg_new_filter_type,
      name = if (is.null(input$mtg_new_filter_name)) character(0) else input$mtg_new_filter_name,
      manager = if (is.null(input$mtg_new_filter_manager)) character(0) else input$mtg_new_filter_manager,
      participant = if (is.null(input$mtg_new_filter_participant)) character(0) else input$mtg_new_filter_participant,
      importance = if (is.null(input$mtg_new_filter_importance)) character(0) else input$mtg_new_filter_importance,
      research_group = if (is.null(input$mtg_new_filter_research_group)) character(0) else input$mtg_new_filter_research_group,
      hospital = if (is.null(input$mtg_new_filter_hospital)) character(0) else input$mtg_new_filter_hospital,
      include_archived = isTRUE(input$mtg_new_filter_include_archived),
      combine_mode = {
        v <- input$mtg_new_filter_combine_mode
        if (is.null(v) || !v %in% c("and", "or")) "and" else v
      }
    )
  })
  meeting_new_filter_state_db <- debounce(meeting_new_filter_state, 400)

  meeting_new_project_choices_filtered <- reactive({
    st <- meeting_new_filter_state_db()
    auth <- current_user_auth()
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con) || is.null(auth) || auth$allow_none) {
      return(meeting_new_build_project_choices(pg_con, auth))
    }
    base <- meeting_new_build_project_choices(pg_con, auth)
    ids <- meeting_new_project_ids_by_gantt_dims(
      pg_con, auth,
      st$type, st$name, st$manager, st$participant, st$importance, st$hospital, st$research_group,
      st$include_archived, st$combine_mode
    )
    meeting_new_intersect_project_choices(base, ids)
  })

  # ---------- 会议决策数据查询 ----------

  # 会议数据：10 表；项目名称由首条 12→11→04 解析，无关联则为「共性决策」
  meeting_data <- reactive({
    meeting_force_refresh()
    input$mtg_filter_name %||% NULL
    input$mtg_filter_date_start %||% NULL
    input$mtg_filter_date_end %||% NULL
    input$mtg_filter_project %||% NULL
    input$mtg_filter_executor %||% NULL
    input$mtg_filter_combine_mode
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(data.frame())
    auth <- current_user_auth()
    if (auth$allow_none) return(data.frame())
    tryCatch({
      and_auth <- meeting_decision_auth_sql(auth)
      proj_sub <- paste0(
        "(SELECT g2.\"项目名称\" FROM public.\"12会议决策关联问题表\" lxp ",
        "INNER JOIN public.\"11阶段问题反馈表\" fx ON fx.id = lxp.\"11阶段问题反馈表_id\" ",
        "INNER JOIN public.\"04项目总表\" g2 ON g2.id = fx.\"04项目总表_id\" ",
        "WHERE lxp.\"10会议决策表_id\" = t.id ORDER BY lxp.id LIMIT 1)"
      )
      q <- paste0(
        "SELECT t.id, t.\"会议名称\", t.\"会议时间\", t.\"决策内容\", ",
        "t.\"决策执行人及执行确认\"::text AS \"决策执行人及执行确认\", ",
        "t.created_at, t.updated_at, t.created_by, t.updated_by, ",
        "COALESCE(", proj_sub, ", '共性决策') AS \"项目名称\" ",
        "FROM public.\"10会议决策表\" t WHERE 1=1",
        and_auth,
        " ORDER BY t.\"会议时间\" DESC, t.id"
      )
      df <- DBI::dbGetQuery(pg_con, q)

      fn <- if (is.null(input$mtg_filter_name)) character(0) else as.character(input$mtg_filter_name)
      fp <- if (is.null(input$mtg_filter_project)) character(0) else as.character(input$mtg_filter_project)
      fe <- if (is.null(input$mtg_filter_executor)) character(0) else as.character(input$mtg_filter_executor)
      df <- filter_meeting_decisions_by_dims(df, fn, fp, fe, input$mtg_filter_combine_mode)

      if (!is.null(input$mtg_filter_date_start) && !is.na(input$mtg_filter_date_start)) {
        ds <- as.POSIXct(paste0(as.Date(input$mtg_filter_date_start), " 00:00:00"), tz = APP_TZ_CN)
        df <- df[!is.na(df[["会议时间"]]) & df[["会议时间"]] >= ds, , drop = FALSE]
      }
      if (!is.null(input$mtg_filter_date_end) && !is.na(input$mtg_filter_date_end)) {
        de <- as.POSIXct(paste0(as.Date(input$mtg_filter_date_end), " 23:59:59"), tz = APP_TZ_CN)
        df <- df[!is.na(df[["会议时间"]]) & df[["会议时间"]] <= de, , drop = FALSE]
      }
      df
    }, error = function(e) data.frame())
  })

  # 会议筛选器选项
  meeting_filter_options <- reactive({
    meeting_force_refresh()
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(list(meeting_names = character(0), projects = character(0), executors = character(0)))
    auth <- current_user_auth()
    if (auth$allow_none) return(list(meeting_names = character(0), projects = character(0), executors = character(0)))
    tryCatch({
      and_auth <- meeting_decision_auth_sql(auth)
      proj_sub <- paste0(
        "(SELECT g2.\"项目名称\" FROM public.\"12会议决策关联问题表\" lxp ",
        "INNER JOIN public.\"11阶段问题反馈表\" fx ON fx.id = lxp.\"11阶段问题反馈表_id\" ",
        "INNER JOIN public.\"04项目总表\" g2 ON g2.id = fx.\"04项目总表_id\" ",
        "WHERE lxp.\"10会议决策表_id\" = t.id ORDER BY lxp.id LIMIT 1)"
      )
      names_q <- paste0('SELECT DISTINCT t."会议名称" FROM public."10会议决策表" t WHERE t."会议名称" IS NOT NULL', and_auth, ' ORDER BY 1')
      meeting_names <- DBI::dbGetQuery(pg_con, names_q)[[1]]

      proj_q <- paste0(
        "SELECT DISTINCT COALESCE(", proj_sub, ", '共性决策') AS \"项目名称\" ",
        "FROM public.\"10会议决策表\" t WHERE 1=1",
        and_auth,
        " ORDER BY 1"
      )
      projects <- DBI::dbGetQuery(pg_con, proj_q)[[1]]

      # 执行人列表从所有决策的JSON中提取
      exec_q <- paste0('SELECT DISTINCT t."决策执行人及执行确认"::text AS "决策执行人及执行确认" FROM public."10会议决策表" t WHERE t."决策执行人及执行确认" IS NOT NULL', and_auth)
      exec_df <- DBI::dbGetQuery(pg_con, exec_q)
      executor_set <- character(0)
      for (js in exec_df[[1]]) {
        if (is.na(js) || !nzchar(as.character(js))) next
        parsed <- parse_executor_json(js)
        if (nrow(parsed) > 0) executor_set <- unique(c(executor_set, parsed$key))
      }
      executor_set <- sort(executor_set)

      list(meeting_names = meeting_names, projects = projects, executors = executor_set)
    }, error = function(e) list(meeting_names = character(0), projects = character(0), executors = character(0)))
  })

  gantt_data_db <- reactive({
    t0_gd <- proc.time()
    ivd_perf_ts("gantt_data_db BEGIN")
    on.exit(ivd_perf_ts(paste0("gantt_data_db END elapsed_s=", round((proc.time() - t0_gd)[3], 3))), add = TRUE)
    today <- today_beijing()   # 每次执行取北京日历日
    current_user_auth()
    gantt_db_error(NULL)
    bundle <- gantt_sql_filter_bundle()
    if (!isTRUE(bundle$ok)) {
      if (identical(bundle$err, "no_db")) {
      gantt_db_error("无法连接数据库，请检查 PG_HOST/PG_PORT/PG_DBNAME/PG_USER/PG_PASSWORD 或先启动数据库服务")
      }
      return(NULL)
    }
    ss <- stage_catalog()$sync_stages
    tryCatch({
      sql_stage <- paste(
        'SELECT * FROM public."v_项目阶段甘特视图"',
        'WHERE', paste(bundle$where_stage, collapse = ' AND '),
        'ORDER BY project_id, stage_ord, site_name'
      )
      ps <- bundle$params_stage
      ivd_perf_ts("gantt_data_db before SQL v_项目阶段甘特视图")
      if (length(ps) > 0) {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage, params = ps)
      } else {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage)
      }
      ivd_perf_ts(paste0("gantt_data_db after SQL nrow=", nrow(stage_rows)))
      t_fn <- proc.time()
      out_gd <- finalize_gantt_stage_rows(stage_rows, today, ss, drop_stage_ord = TRUE)
      ivd_perf_elapsed(t_fn, "gantt_data_db finalize_gantt_stage_rows")
      out_gd
    }, error = function(e) {
      gantt_db_error(paste0("数据库加载失败: ", conditionMessage(e)))
      NULL
    })
  })

  # 甘特数据源（含未激活阶段），用于自由里程碑展示与占位，使未勾选 is_active 的阶段仍可显示/编辑里程碑
  gantt_data_all_stages <- reactive({
    t0_ga <- proc.time()
    ivd_perf_ts("gantt_data_all_stages BEGIN")
    on.exit(ivd_perf_ts(paste0("gantt_data_all_stages END elapsed_s=", round((proc.time() - t0_ga)[3], 3))), add = TRUE)
    today <- today_beijing()   # 每次执行取北京日历日
    current_user_auth()
    bundle <- gantt_sql_filter_bundle()
    if (!isTRUE(bundle$ok)) return(NULL)
    ss <- stage_catalog()$sync_stages
    tryCatch({
      sql_stage <- paste(
        'SELECT * FROM public."v_项目阶段甘特视图_全部"',
        'WHERE', paste(bundle$where_stage, collapse = ' AND '),
        'ORDER BY project_id, stage_ord, site_name'
      )
      ps <- bundle$params_stage
      ivd_perf_ts("gantt_data_all_stages before SQL v_项目阶段甘特视图_全部")
      if (length(ps) > 0) {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage, params = ps)
      } else {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage)
      }
      ivd_perf_ts(paste0("gantt_data_all_stages after SQL nrow=", nrow(stage_rows)))
      t_fn <- proc.time()
      out_ga <- finalize_gantt_stage_rows(stage_rows, today, ss, drop_stage_ord = TRUE)
      ivd_perf_elapsed(t_fn, "gantt_data_all_stages finalize_gantt_stage_rows")
      out_ga
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

  # ----- 甘特图数据变换（Tab1 / Tab2 共用） -----
  process_gantt_items <- function(gd, gd_all, ss, today) {
    if (is.null(gd) || nrow(gd) == 0) {
      return(list(items = data.frame(), groups = data.frame()))
    }
    json_cols <- c("remark_json", "contributors_json", "milestones_json", "sample_json")
    for (c in json_cols) if (c %in% names(gd)) gd[[c]] <- as.character(gd[[c]])
    if (!"is_unplanned" %in% names(gd)) gd$is_unplanned <- FALSE
    if (!"project_type" %in% names(gd)) gd$project_type <- NA_character_
    if (!"manager_name" %in% names(gd)) gd$manager_name <- NA_character_
    gd_milestone <- if (is.null(gd_all) || nrow(gd_all) == 0) gd else gd_all
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
        content = sprintf("<div style='padding:2px 6px; font-size:13px;'>%s</div>", site_name)
      )
    if (!is.null(gd_milestone) && nrow(gd_milestone) > 0) {
      existing_center_ids <- paste0(groups_centers$project_id, "_", groups_centers$site_name)
      missing_centers <- gd_milestone %>%
        filter(!task_name %in% ss) %>%
        mutate(center_id = paste0(project_id, "_", site_name)) %>%
        filter(!center_id %in% existing_center_ids) %>%
        group_by(project_id, site_name) %>%
        summarise(
          avg_p = 0,
          project_type = first(project_type),
          importance = { x <- suppressWarnings(na.omit(`重要紧急程度`)); if (length(x) == 0) NA_character_ else x[1] },
          .groups = "drop"
        ) %>%
        mutate(
          id = paste0(project_id, "_", site_name),
          content = sprintf("<div style='padding:2px 6px; font-size:13px;'>%s</div>", site_name)
        )
      if (nrow(missing_centers) > 0) {
        groups_centers <- bind_rows(groups_centers, missing_centers) %>%
          arrange(project_id, site_name)
      }
    }
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
        mgr_line = if_else(
          !is.na(manager_name) & nzchar(trimws(as.character(manager_name))),
          sprintf("<br><span style='color:#888; font-size:12px;'>项目负责人：%s</span>",
                  as.character(manager_name)), ""),
        content = sprintf(
          "<div style='padding:2px 6px; font-size:13px; line-height:1.35;'>各中心同步阶段%s</div>",
          mgr_line
        )
      ) %>%
      select(-mgr_line)
    groups_data <- bind_rows(groups_sync, groups_centers) %>%
      arrange(project_id, desc(id == paste0(project_id, "_同步阶段")), site_name)
    proj_headers <- groups_sync %>%
      mutate(
        orig_pid    = project_id,
        hdr_line1   = if_else(!is.na(project_type) & nzchar(project_type),
                              paste0(project_type, "-", project_id), project_id),
        hdr_imp_txt = if_else(!is.na(importance) & nzchar(importance),
                              paste0(" \u00b7 ", importance), ""),
        hdr_mgr_txt = if_else(!is.na(manager_name) & nzchar(trimws(as.character(manager_name))),
                              paste0(" \u00b7 \u8d1f\u8d23\u4eba\uff1a", as.character(manager_name)), ""),
        title       = paste0(hdr_line1, hdr_imp_txt, hdr_mgr_txt),
        hdr_display = sapply(hdr_line1, truncate_label, USE.NAMES = FALSE),
        content     = sprintf(
          "<div style='padding:0;margin:0;font-size:14px;line-height:1.2;white-space:nowrap;'><strong>%s</strong></div>",
          hdr_display
        ),
        className   = "gantt-row-sep",
        style       = "font-weight: 900; font-size: 14px; background-color: #B7BEC7; color: #1F2933;",
        id          = paste0("MOCK_SEPARATOR_", project_id),
        project_id  = paste0("HDR_", orig_pid),
        avg_p       = 0,
        site_name   = NA_character_
      ) %>%
      select(orig_pid, project_id, id, content, title, className, style, avg_p, site_name)
    unique_projects <- unique(groups_data$project_id)
    grp_ri <- split(seq_len(nrow(groups_data)), factor(groups_data$project_id, levels = unique_projects))
    hdr_i <- match(unique_projects, proj_headers$orig_pid)
    n_up <- length(unique_projects)
    parts <- vector("list", 2L * n_up)
    pi <- 0L
    hdr_drop <- setdiff(names(proj_headers), "orig_pid")
    for (k in seq_len(n_up)) {
      hi <- hdr_i[k]
      if (!is.na(hi)) {
        pi <- pi + 1L
        parts[[pi]] <- proj_headers[hi, hdr_drop, drop = FALSE]
      }
      pi <- pi + 1L
      parts[[pi]] <- groups_data[grp_ri[[k]], , drop = FALSE]
    }
    groups_data <- bind_rows(parts[seq_len(pi)])
    groups_data <- groups_data %>% mutate(
      className = case_when(
        !is.na(className) & nzchar(className) ~ className,
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
        planned_duration = dplyr::if_else(is_unplanned, 30.0, as.numeric(planned_end_date - planned_start_date)),
        is_completed = !is.na(actual_end_date),
        plan_no_actual_start = !is_unplanned & is.na(actual_start_date) &
          !is.na(planned_start_date) & !is.na(planned_end_date) & is.na(actual_end_date),
        planned_progress = ifelse(
          plan_no_actual_start,
          0.0,
          ifelse(
            is_completed,
            ifelse(planned_duration > 0,
                   as.numeric(actual_end_date - start_date) / planned_duration,
                   1.0),
            ifelse(planned_duration > 0,
                   as.numeric(today - start_date) / planned_duration,
                   ifelse(today >= start_date, 1.0, 0.0))
          )
        ),
        actual_progress = ifelse(is_completed, 1.0, progress),
        progress_diff = actual_progress - planned_progress,
        color_level = case_when(
          progress_diff < -0.5 ~ "落后50%以上",
          progress_diff >= -0.5 & progress_diff < -0.3 ~ "落后30%-50%",
          progress_diff >= -0.3 & progress_diff < -0.15 ~ "落后15%-30%",
          progress_diff >= -0.15 & progress_diff < -0.05 ~ "落后5%-15%",
          progress_diff >= -0.05 & progress_diff < 0.1 ~ "偏差5%以内/超前10%以内",
          progress_diff >= 0.1 & progress_diff < 0.25 ~ "超前10%-25%",
          progress_diff >= 0.25 ~ "超前25%以上"
        ),
        is_not_started = plan_no_actual_start |
          (!is_unplanned & today < start_date & progress == 0 & is.na(actual_end_date)),
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
        border_color = if_else(is_unplanned, "#D0D0D0", bg_color),
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
          sprintf("background-color: %s; border-color: %s; color: %s; border-width: 1px; border-style: solid;", bg_color, border_color, text_color),
          sprintf("background-color: %s; border-color: %s; color: %s; border-width: 2px;", bg_color, border_color, text_color)),
        type = "range",
        start = dplyr::if_else(is.na(start_date), today, start_date),
        theoretical_end = dplyr::case_when(
          is_unplanned ~ planned_end_date,
          !is.na(actual_start_date) & !is.na(planned_start_date) & !is.na(planned_end_date) ~
            actual_start_date + as.numeric(planned_end_date - planned_start_date),
          TRUE ~ planned_end_date
        ),
        end = dplyr::case_when(
          is_unplanned ~ as.character(dplyr::coalesce(planned_end_date, start_date + 30L)),
          !is.na(actual_end_date) ~ as.character(actual_end_date),
          today > theoretical_end ~ as.character(today),
          TRUE ~ as.character(theoretical_end)
        ),
        content = paste0(
          ifelse(!is.na(task_display_name) & nzchar(as.character(task_display_name)),
                 as.character(task_display_name),
                 vapply(task_name, stage_label_for_key, character(1))),
          ifelse(is_unplanned, " (未制定计划)", ""), " ", ifelse(!is.na(actual_end_date), 100, round(progress * 100, 0)), "%")
      )
    items_sync <- gd %>%
      filter(task_name %in% ss) %>%
      group_by(project_id, task_name) %>%
      summarise(
        start_date = first(start_date),
        actual_start_date = first(actual_start_date),
        planned_start_date = first(planned_start_date),
        planned_end_date = first(planned_end_date),
        actual_end_date = first(actual_end_date),
        progress = first(progress),
        task_type = first(task_type),
        is_unplanned = first(is_unplanned),
        task_display_name = first(task_display_name),
        .groups = 'drop'
      ) %>%
      mutate(
        id = max(items_centers$id, 0) + row_number(),
        group = paste0(project_id, "_同步阶段"),
        planned_duration = dplyr::if_else(is_unplanned, 30.0, as.numeric(planned_end_date - planned_start_date)),
        is_completed = !is.na(actual_end_date),
        plan_no_actual_start = !is_unplanned & is.na(actual_start_date) &
          !is.na(planned_start_date) & !is.na(planned_end_date) & is.na(actual_end_date),
        planned_progress = ifelse(
          plan_no_actual_start,
          0.0,
          ifelse(
            is_completed,
            ifelse(planned_duration > 0,
                   as.numeric(actual_end_date - start_date) / planned_duration,
                   1.0),
            ifelse(planned_duration > 0,
                   as.numeric(today - start_date) / planned_duration,
                   ifelse(today >= start_date, 1.0, 0.0))
          )
        ),
        actual_progress = ifelse(is_completed, 1.0, progress),
        progress_diff = actual_progress - planned_progress,
        color_level = case_when(
          progress_diff < -0.5 ~ "落后50%以上",
          progress_diff >= -0.5 & progress_diff < -0.3 ~ "落后30%-50%",
          progress_diff >= -0.3 & progress_diff < -0.15 ~ "落后15%-30%",
          progress_diff >= -0.15 & progress_diff < -0.05 ~ "落后5%-15%",
          progress_diff >= -0.05 & progress_diff < 0.1 ~ "偏差5%以内/超前10%以内",
          progress_diff >= 0.1 & progress_diff < 0.25 ~ "超前10%-25%",
          progress_diff >= 0.25 ~ "超前25%以上"
        ),
        is_not_started = plan_no_actual_start |
          (!is_unplanned & today < start_date & progress == 0 & is.na(actual_end_date)),
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
        border_color = if_else(is_unplanned, "#D0D0D0", bg_color),
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
          sprintf("background-color: %s; border-color: %s; color: %s; border-width: 1px; border-style: solid;", bg_color, border_color, text_color),
          sprintf("background-color: %s; border-color: %s; color: %s; border-width: 2px;", bg_color, border_color, text_color)),
        type = "range",
        start = dplyr::if_else(is.na(start_date), today, start_date),
        theoretical_end = dplyr::case_when(
          is_unplanned ~ planned_end_date,
          !is.na(actual_start_date) & !is.na(planned_start_date) & !is.na(planned_end_date) ~
            actual_start_date + as.numeric(planned_end_date - planned_start_date),
          TRUE ~ planned_end_date
        ),
        end = dplyr::case_when(
          is_unplanned ~ as.character(dplyr::coalesce(planned_end_date, start_date + 30L)),
          !is.na(actual_end_date) ~ as.character(actual_end_date),
          today > theoretical_end ~ as.character(today),
          TRUE ~ as.character(theoretical_end)
        ),
        content = paste0(
          ifelse(!is.na(task_display_name) & nzchar(as.character(task_display_name)),
                 as.character(task_display_name),
                 vapply(task_name, stage_label_for_key, character(1))),
          ifelse(is_unplanned, " (未制定计划)", ""), " ", ifelse(!is.na(actual_end_date), 100, round(progress * 100, 0)), "%")
      )
    if (nrow(items_centers) == 0) {
      items_data <- items_sync
    } else if (nrow(items_sync) == 0) {
      items_data <- items_centers
    } else {
      items_data <- bind_rows(items_centers, items_sync)
    }
    if (length(unique_projects) > 1) {
      all_dates <- c(items_data$start, as.Date(items_data$end[!is.na(items_data$end)]))
      min_date <- min(all_dates, na.rm = TRUE)
      max_date <- max(all_dates, na.rm = TRUE)
      mock_items <- groups_data %>%
        filter(grepl("MOCK_SEPARATOR", id)) %>%
        mutate(
          new_id = max(items_data$id, 0) + row_number(),
          new_group = id,
          new_start = min_date,
          new_end = as.character(max_date),
          new_content = "",
          new_style = "background-color: transparent !important; border: none !important; pointer-events: none !important; opacity: 0 !important;",
          new_type = "range"
        ) %>%
        select(id = new_id, group = new_group, start = new_start, end = new_end,
               content = new_content, style = new_style, type = new_type)
      if (nrow(mock_items) > 0) {
        items_data <- bind_rows(items_data, mock_items)
      }
    }
    milestone_items <- list()
    if (nrow(gd_milestone) > 0) {
      cur_ms_id <- if (nrow(items_data) > 0) max(items_data$id, na.rm = TRUE) + 1L else 1L
      seen_sync_e <- new.env(parent = emptyenv(), hash = TRUE)
      seen_site_e <- new.env(parent = emptyenv(), hash = TRUE)
      for (ri in seq_len(nrow(gd_milestone))) {
        row <- gd_milestone[ri, ]
        pid   <- row$project_id
        sname <- row$site_name
        is_sync_row <- row$task_name %in% ss
        if (is_sync_row) {
          kpr <- as.character(pid)
          if (!is.null(seen_sync_e[[kpr]])) next
          seen_sync_e[[kpr]] <- TRUE
        } else {
          site_key <- paste(pid, sname, sep = "||")
          if (!is.null(seen_site_e[[site_key]])) next
          seen_site_e[[site_key]] <- TRUE
        }
        group_id  <- if (is_sync_row) paste0(pid, "_同步阶段") else paste0(pid, "_", sname)
        ms_json   <- if ("milestones_json" %in% names(row)) row$milestones_json else NULL
        if (!is.null(ms_json) && !is.na(ms_json)) {
          ms_df_current <- parse_milestone_json_to_df(ms_json)
          for (mi in seq_len(nrow(ms_df_current))) {
            ms_name  <- ms_df_current$name[mi]
            plan_str <- ifelse(nzchar(ms_df_current$plan[mi]),   ms_df_current$plan[mi],   "无")
            act_str  <- ifelse(nzchar(ms_df_current$actual[mi]), ms_df_current$actual[mi], "无")
            if (!identical(plan_str, "无")) {
              milestone_items[[length(milestone_items) + 1]] <- data.frame(
                id = cur_ms_id, group = group_id,
                start = plan_str, end = NA,
                content = paste0(ms_name, "（计划）"),
                style = "color:#0D47A1; border-color:#1976D2; background-color:#BBDEFB;",
                type = "point", milestone_name = ms_name, milestone_kind = "plan",
                milestone_stage_key = NA_character_, stringsAsFactors = FALSE
              )
              cur_ms_id <- cur_ms_id + 1L
            }
            if (!identical(act_str, "无")) {
              milestone_items[[length(milestone_items) + 1]] <- data.frame(
                id = cur_ms_id, group = group_id,
                start = act_str, end = NA,
                content = paste0(ms_name, "（实际）"),
                style = "color:#1B5E20; border-color:#388E3C; background-color:#C8E6C9;",
                type = "point", milestone_name = ms_name, milestone_kind = "actual",
                milestone_stage_key = NA_character_, stringsAsFactors = FALSE
              )
              cur_ms_id <- cur_ms_id + 1L
            }
          }
        }
      }
    }
    if (length(milestone_items) > 0) {
      ms_df <- bind_rows(milestone_items)
      if ("start" %in% names(items_data)) items_data$start <- as.character(items_data$start)
      if ("end" %in% names(items_data))   items_data$end   <- as.character(items_data$end)
      if ("start" %in% names(ms_df))      ms_df$start      <- as.character(ms_df$start)
      if ("end" %in% names(ms_df))        ms_df$end        <- as.character(ms_df$end)
      items_data <- bind_rows(items_data, ms_df)
    }
    list(items = items_data, groups = groups_data)
  }

  # ----- 甘特图 -----
  processed_data <- reactive({
    today <- today_beijing()
    gd <- current_gantt_data()
    ss <- stage_catalog()$sync_stages
    gd_all <- gantt_data_all_stages()
    process_gantt_items(gd, gd_all, ss, today)
  })
  
  output$my_gantt <- renderTimevis({
    if (is.null(.ivd_sess_once$gantt_tv)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|output$my_gantt renderTimevis FIRST")
      .ivd_sess_once$gantt_tv <- TRUE
    }
    t0_tv <- proc.time()
    ivd_perf_ts("renderTimevis(my_gantt) BEGIN")
    data <- gantt_init_data()
    ivd_perf_ts(paste0(
      "renderTimevis(my_gantt) after gantt_init_data() elapsed_s=", round((proc.time() - t0_tv)[3], 3),
      " nrow_items=", if (is.null(data)) 0 else nrow(data$items), " nrow_groups=", if (is.null(data)) 0 else nrow(data$groups)
    ))
    req(data)
    t_tv <- proc.time()
    today <- today_beijing()
    tv <- timevis(
      data = data$items,
      groups = data$groups,
      height = "100%",
      options = list(
        start    = format(seq(today, by = "-4 month", length.out = 2L)[2L], "%Y-%m-%d"),
        end      = format(seq(today, by = "8 month",  length.out = 2L)[2L], "%Y-%m-%d"),
        min      = format(seq(today, by = "-24 month", length.out = 2L)[2L], "%Y-%m-%d"),
        max      = format(seq(today, by = "36 month",  length.out = 2L)[2L], "%Y-%m-%d"),
        format = list(
          minorLabels = list(month = "M月", year = "YYYY年"),
          majorLabels = list(month = "YYYY年M月", year = "YYYY年")
        ),
        showCurrentTime = TRUE,
        orientation = "top",
        verticalScroll = TRUE,
        zoomKey = "altKey",
        margin = list(item = list(vertical = 2, horizontal = 4), axis = 4)
      )
    )
    ivd_perf_elapsed(t_tv, "renderTimevis timevis() widget build only")
    ivd_perf_ts(paste0("renderTimevis(my_gantt) after timevis() elapsed_s=", round((proc.time() - t0_tv)[3], 3)))
    tv
  })

  # 甘特数据更新：首次写入 gantt_init_data 触发 renderTimevis 创建 widget；后续用 proxy 增量更新，保留用户视口
  observe({
    d <- processed_data()
    if (is.null(d) || is.null(d$items) || nrow(d$items) == 0) return()
    if (is.null(.ivd_sess_once$gantt_proxy_ready)) {
      .ivd_sess_once$gantt_proxy_ready <- TRUE
      gantt_init_data(d)
      return()
    }
    # 后续更新：根据来源决定模式
    # 用户交互（保存/编辑）→ incremental（保留滚动位置）；筛选器 → full（完全重建）
    use_incremental <- isolate(gantt_use_incremental())
    if (use_incremental) gantt_use_incremental(FALSE)  # 重置
    session$sendCustomMessage("ganttSetUpdateMode", if (use_incremental) "incremental" else "full")
    setItems("my_gantt", d$items)
    setGroups("my_gantt", d$groups)
  })

  # ========== Tab2：未参与/负责项目甘特图（只读） ==========

  # Tab2 初始化
  gantt2_init_data <- reactiveVal(NULL)
  gantt2_force_refresh <- reactiveVal(0L)
  gantt2_skip_query_debounce_once <- reactiveVal(TRUE)

  # Tab2 筛选器状态
  gantt2_filter_state <- reactive({
    list(
      refresh = input$gantt2_refresh,
      type = input$gantt2_filter_type,
      name = input$gantt2_filter_name,
      manager = input$gantt2_filter_manager,
      participant = input$gantt2_filter_participant,
      importance = input$gantt2_filter_importance,
      research_group = input$gantt2_filter_research_group,
      hospital = input$gantt2_filter_hospital,
      manager_group = input$gantt2_filter_manager_group,
      include_archived = input$gantt2_filter_include_archived,
      combine_mode = {
        v <- input$gantt2_filter_combine_mode
        if (is.null(v) || !v %in% c("and", "or")) "and" else v
      }
    )
  })
  gantt2_filter_state_debounced <- debounce(gantt2_filter_state, 400)
  gantt2_filter_state_for_query <- reactive({
    st <- gantt2_filter_state()
    if (isTRUE(shiny::isolate(gantt2_skip_query_debounce_once()))) {
      gantt2_skip_query_debounce_once(FALSE)
      return(st)
    }
    gantt2_filter_state_debounced()
  })

  # Tab2 SQL 筛选包（使用 disallowed_subquery — 补集）
  gantt2_sql_filter_bundle <- reactive({
    input$gantt2_refresh
    gantt2_force_refresh()
    state <- gantt2_filter_state_for_query()
    auth <- current_user_auth()
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(list(ok = FALSE, err = "no_db"))
    if (auth$allow_all || auth$allow_none) return(list(ok = FALSE, err = "no_complement"))
    ft <- if (is.null(state$type)) character(0) else state$type
    fn <- if (is.null(state$name)) character(0) else state$name
    fm <- if (is.null(state$manager)) character(0) else state$manager
    fp <- if (is.null(state$participant)) character(0) else state$participant
    fi <- if (is.null(state$importance)) character(0) else state$importance
    fh <- if (is.null(state$hospital)) character(0) else state$hospital
    frg <- if (is.null(state$research_group)) character(0) else state$research_group
    fmg <- if (is.null(state$manager_group)) character(0) else state$manager_group
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
    proj_ids_rg <- integer(0)
    if (length(frg) > 0) {
      qrg <- paste0('SELECT id FROM public."04项目总表" WHERE "课题组" IN (', paste(rep("$", length(frg)), seq_along(frg), sep = "", collapse = ","), ")")
      proj_ids_rg <- DBI::dbGetQuery(pg_con, qrg, params = as.list(frg))$id
    }
    manager_group_ids <- integer(0)
    if (length(fmg) > 0) {
      qmg <- paste0('SELECT id FROM public."05人员表" WHERE "组别" IN (', paste(rep("$", length(fmg)), seq_along(fmg), sep = "", collapse = ","), ")")
      manager_group_ids <- DBI::dbGetQuery(pg_con, qmg, params = as.list(fmg))$id
    }
    where_stage <- c('project_id IS NOT NULL', 'project_type IS NOT NULL')
    # 使用 disallowed_subquery（补集）
    where_stage <- c(where_stage, paste0("project_db_id IN (", auth$disallowed_subquery, ")"))
    dim_built <- build_gantt_filter_dimension_parts(ft, fn, fi, manager_ids, proj_ids_participant, proj_ids_hosp, proj_ids_rg, manager_group_ids)
    combine_mode <- if (is.null(state$combine_mode)) "and" else state$combine_mode
    if (length(dim_built$dim_parts) > 0L) {
      if (identical(combine_mode, "or")) {
        where_stage <- c(where_stage, paste0("(", paste(dim_built$dim_parts, collapse = " OR "), ")"))
      } else {
        where_stage <- c(where_stage, dim_built$dim_parts)
      }
    }
    if (!isTRUE(state$include_archived)) {
      where_stage <- c(where_stage, 'COALESCE(project_is_active, true) = true')
    }
    list(ok = TRUE, where_stage = where_stage, params_stage = dim_built$params_stage)
  })

  # Tab2 筛选器选项（基于补集项目）
  gantt2_filter_options <- reactive({
    input$gantt2_refresh
    auth <- current_user_auth()
    fetch_gantt_dim_filter_options(pg_con, auth, complement = TRUE)
  })
  observe({
    opts <- gantt2_filter_options()
    if (is.null(opts)) return(NULL)
    isolate({
      ft2 <- input$gantt2_filter_type
      fn2 <- input$gantt2_filter_name
      fm2 <- input$gantt2_filter_manager
      fp2 <- input$gantt2_filter_participant
      fi2 <- input$gantt2_filter_importance
      frg2 <- input$gantt2_filter_research_group
      fh2 <- input$gantt2_filter_hospital
      fmg2 <- input$gantt2_filter_manager_group
    })
    updateSelectInput(session, "gantt2_filter_type", choices = opts$types, selected = sel_intersect_choices(ft2, opts$types))
    updateSelectInput(session, "gantt2_filter_name", choices = opts$names, selected = sel_intersect_choices(fn2, opts$names))
    updateSelectInput(session, "gantt2_filter_manager", choices = opts$managers, selected = sel_intersect_choices(fm2, opts$managers))
    updateSelectInput(session, "gantt2_filter_participant", choices = opts$participants, selected = sel_intersect_choices(fp2, opts$participants))
    updateSelectInput(session, "gantt2_filter_importance", choices = opts$importance, selected = sel_intersect_choices(fi2, opts$importance))
    updateSelectInput(session, "gantt2_filter_research_group", choices = opts$research_groups, selected = sel_intersect_choices(frg2, opts$research_groups))
    updateSelectInput(session, "gantt2_filter_hospital", choices = opts$hospitals, selected = sel_intersect_choices(fh2, opts$hospitals))
    updateSelectInput(session, "gantt2_filter_manager_group", choices = opts$manager_groups, selected = sel_intersect_choices(fmg2, opts$manager_groups))
  })

  # Tab2 数据源
  gantt2_data_db <- reactive({
    today <- today_beijing()
    bundle <- gantt2_sql_filter_bundle()
    if (!isTRUE(bundle$ok)) return(NULL)
    ss <- stage_catalog()$sync_stages
    tryCatch({
      sql_stage <- paste(
        'SELECT * FROM public."v_项目阶段甘特视图"',
        'WHERE', paste(bundle$where_stage, collapse = ' AND '),
        'ORDER BY project_id, stage_ord, site_name'
      )
      ps <- bundle$params_stage
      if (length(ps) > 0) {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage, params = ps)
      } else {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage)
      }
      finalize_gantt_stage_rows(stage_rows, today, ss, drop_stage_ord = TRUE)
    }, error = function(e) NULL)
  })
  gantt2_data_all_stages <- reactive({
    today <- today_beijing()
    bundle <- gantt2_sql_filter_bundle()
    if (!isTRUE(bundle$ok)) return(NULL)
    ss <- stage_catalog()$sync_stages
    tryCatch({
      sql_stage <- paste(
        'SELECT * FROM public."v_项目阶段甘特视图_全部"',
        'WHERE', paste(bundle$where_stage, collapse = ' AND '),
        'ORDER BY project_id, stage_ord, site_name'
      )
      ps <- bundle$params_stage
      if (length(ps) > 0) {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage, params = ps)
      } else {
        stage_rows <- DBI::dbGetQuery(pg_con, sql_stage)
      }
      finalize_gantt_stage_rows(stage_rows, today, ss, drop_stage_ord = TRUE)
    }, error = function(e) NULL)
  })

  # Tab2 数据处理（复用 process_gantt_items）
  processed_data_tab2 <- reactive({
    req(identical(input$gantt_sub_tabs, "gantt_view2"))
    today <- today_beijing()
    gd <- gantt2_data_db()
    ss <- stage_catalog()$sync_stages
    gd_all <- gantt2_data_all_stages()
    process_gantt_items(gd, gd_all, ss, today)
  })

  # Tab2 渲染
  output$my_gantt2 <- renderTimevis({
    data <- gantt2_init_data()
    req(data)
    today <- today_beijing()
    timevis(
      data = data$items,
      groups = data$groups,
      height = "100%",
      options = list(
        start    = format(seq(today, by = "-4 month", length.out = 2L)[2L], "%Y-%m-%d"),
        end      = format(seq(today, by = "8 month",  length.out = 2L)[2L], "%Y-%m-%d"),
        min      = format(seq(today, by = "-24 month", length.out = 2L)[2L], "%Y-%m-%d"),
        max      = format(seq(today, by = "36 month",  length.out = 2L)[2L], "%Y-%m-%d"),
        format = list(
          minorLabels = list(month = "M月", year = "YYYY年"),
          majorLabels = list(month = "YYYY年M月", year = "YYYY年")
        ),
        showCurrentTime = TRUE,
        orientation = "top",
        verticalScroll = TRUE,
        zoomKey = "altKey",
        margin = list(item = list(vertical = 2, horizontal = 4), axis = 4)
      )
    )
  })

  # Tab2 数据更新（始终 full 模式，因为只读无增量更新）
  gantt2_proxy_ready <- reactiveVal(FALSE)
  observe({
    d <- processed_data_tab2()
    if (is.null(d) || is.null(d$items) || nrow(d$items) == 0) return()
    if (!isTRUE(gantt2_proxy_ready())) {
      gantt2_proxy_ready(TRUE)
      gantt2_init_data(d)
      return()
    }
    session$sendCustomMessage("ganttSetUpdateMode", "full")
    setItems("my_gantt2", d$items)
    setGroups("my_gantt2", d$groups)
  })

  # ========== End Tab2 ==========

  # ---- 任务详情弹窗：共享渲染函数（甘特点击 / 消息阶段点击 / 消息甘特点击 共用） ----
  .render_task_detail_modal <- function(
    task_info_row, proj_row_id, stage_instance_id,
    raw_planned_start, raw_actual_start, raw_planned_end, actual_end_date,
    is_unplanned, is_s09_task,
    actual_progress, planned_p, diff_p, delay_days,
    importance_display, importance_color,
    manager_display, participants_display,
    diagnostic_text, can_edit,
    read_only = FALSE
  ) {
    auth <- current_user_auth()
    showModal(modalDialog(
      title = "任务详细进度核查",
      size = "l",
      easyClose = TRUE,
      footer = if (read_only) tagList(
        modalButton("关闭")
      ) else tagList(
        tags$span(style = "float: left;",
          if (!is.na(proj_row_id) && (isTRUE(auth$can_manage_project) || is_current_user_project_manager(proj_row_id)))
            actionButton("btn_edit_personnel_center", "修改项目基础信息", class = "btn-warning",
                         style = "margin-right: 6px;", title = "管理项目参与人员与临床中心"),
          {
            is_proj_mgr <- !is.na(proj_row_id) && is_current_user_project_manager(proj_row_id)
            if (isTRUE(auth$is_super_admin) || isTRUE(is_proj_mgr))
              actionButton("btn_stage_maintain", "阶段维护", class = "btn-default",
                           title = if (isTRUE(auth$is_super_admin))
                                   "管理 08 阶段定义与 09 阶段实例"
                                  else
                                   "管理 09 阶段实例")
          }
        ),
        if (can_edit) tagList(
          actionButton("btn_edit_task", "修改/更新数据", class = "btn-primary"),
          actionButton("btn_edit_contrib", "修改贡献者信息", class = "btn-default"),
          actionButton("btn_open_milestone_from_task", "自由里程碑", class = "btn-default",
                       title = "管理本阶段计划/实际达成时间等（原甘特占位入口）")
        ),
        modalButton("关闭")
      ),

      fluidRow(
        column(6,
               p(tags$b("项目："), task_info_row$project_id),
               p(tags$b("中心："), task_info_row$site_name),
               p(tags$b("阶段："), display_name_for_ctx(list(task_name = task_info_row$task_name, task_display_name = if ("task_display_name" %in% names(task_info_row)) as.character(task_info_row$task_display_name[1]) else NA_character_))),
               p(tags$b("项目紧急程度："), tags$span(importance_display, style = sprintf("color: %s; font-weight: bold;", importance_color))),
               p(tags$b("项目负责人："), manager_display),
               p(tags$b("项目参与人员名单："),
                 if (length(participants_display) > 0)
                   tags$div(style = "white-space: pre-wrap; margin-top: 4px;", paste(participants_display, collapse = "\n"))
                 else tags$span("（无）", style = "color: #999;")),
               p(tags$b("计划开始日期："), if (length(raw_planned_start) > 0 && !is.na(raw_planned_start)) as.character(raw_planned_start) else "无"),
               p(tags$b("实际开始日期："), if (length(raw_actual_start) > 0 && !is.na(raw_actual_start)) as.character(raw_actual_start) else "无"),
               p(tags$b("计划结束日期："), if (length(raw_planned_end) > 0 && !is.na(raw_planned_end)) as.character(raw_planned_end) else "无"),
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
               p(tags$b("项目要点：")),
               {
                 remark_df <- if (!is.null(task_edit_context())) task_edit_context()$remark_entries else empty_remark_df()
                 if (!is.null(remark_df) && nrow(remark_df) > 0L) {
                   rdf <- ensure_remark_display_columns(
                     remark_df,
                     fixed_stage = display_name_for_ctx(list(task_name = task_info_row$task_name, task_display_name = if ("task_display_name" %in% names(task_info_row)) as.character(task_info_row$task_display_name[1]) else NA_character_)),
                     fixed_site = as.character(task_info_row$site_name %||% ""),
                     fixed_task_key = as.character(task_info_row$task_name[1])
                   )
                   dec_by_fid <- fetch_decisions_linked_to_feedback_ids(pg_con, rdf$fb_id)
                   tags$div(
                     style = "white-space: normal; word-break: break-word; overflow-wrap: anywhere; max-height: 42vh; overflow-y: auto; padding-right: 4px;",
                     ui_gantt_feedback_with_decisions(auth, pg_con, rdf, dec_by_fid)
                   )
                 } else {
                   tags$span("（无）", style = "color: #999;")
                 }
               },
               if (is_s09_task) {
                 tagList(
                   tags$hr(),
                   p(tags$b("样本来源与数量：")),
                   {
                     sp <- if (!is.null(task_edit_context())) task_edit_context()$samples else NULL
                     if (!is.null(sp) && nrow(sp) > 0) {
                       tags$pre(
                         style = "white-space: pre-wrap; background: #f5f5f5; padding: 8px; border-radius: 4px; font-size: 14px;",
                         paste(
                           vapply(seq_len(nrow(sp)), function(i) {
                             sprintf("%s：%s", as.character(sp$hospital[i]), as.character(sp$count[i]))
                           }, character(1)),
                           collapse = "\n"
                         )
                       )
                     } else {
                       tags$span("（暂无样本来源与数量记录）", style = "color: #999;")
                     }
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
                       note_txt <- if ("note" %in% names(row) && nzchar(as.character(row$note %||% ""))) paste0("【", row$note, "】") else ""
                       role_txt <- trimws(as.character(row$role %||% ""))
                       tags$li(
                         style = "color: #333;",
                         row$person, "：",
                         tags$span(
                           style = paste0("color: ", contrib_role_color(role_txt), "; font-weight: 600;"),
                           if (nzchar(role_txt)) role_txt else "（空）"
                         ),
                         " - ", row$work,
                         if (nzchar(as.character(row$amount %||% ""))) paste0("（数量：", row$amount, "）") else "",
                         note_txt
                       )
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
        if(!is.na(planned_p) && planned_p > 1.0) tags$span(" (已超过计划完成时间)", style="color:orange; font-size:0.9em;")),
      p(tags$b("实际提报进度："), tags$span(sprintf("%.1f%%", actual_progress * 100), style="font-weight:bold;")),
      p(tags$b("执行差值 (实际-计划)："), sprintf("%.1f%%", diff_p * 100)),
          p(tags$b("系统诊断结果："), diagnostic_text)
        )
      }
    ))
  }

  observeEvent(input$my_gantt_selected, {
    today <- today_beijing()   # 每次执行取北京日历日（任务详情进度诊断基准）
    req(input$my_gantt_selected)
    # 每次处理完毕（无论正常返回还是 return() 提前退出）都重置选中状态，
    # 使下次点击同一控件仍能触发 observeEvent（value NULL → id 视为变化）
    on.exit(session$sendCustomMessage("resetGanttSelection", "my_gantt"), add = TRUE)
    task_id <- as.numeric(input$my_gantt_selected)
    data <- processed_data()
    task_info <- data$items[data$items$id == task_id, ]

    # 如果点击的是自由里程碑相关的点（计划/实际），且该行里确实标记了 milestone_kind，
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
      has_stage_key <- !is.na(stage_key_clicked) && nzchar(stage_key_clicked)
      # 用含未激活阶段的数据源查找阶段行，使未勾选 is_active 的阶段仍能点开里程碑编辑
      gd_lookup <- gantt_data_all_stages()
      if (is.null(gd_lookup) || nrow(gd_lookup) == 0) gd_lookup <- current_gantt_data()
      if (is.null(gd_lookup) || nrow(gd_lookup) == 0) return(NULL)
      if (is_sync) {
        original_task <- gd_lookup %>%
          filter(project_id == project_id_from_group, task_name %in% ss) %>%
          { if (has_stage_key) filter(., task_name == stage_key_clicked) else . } %>%
          slice(1)
      } else {
        original_task <- gd_lookup %>%
          filter(project_id == project_id_from_group, site_name == site_name_from_group, !task_name %in% ss) %>%
          { if (has_stage_key) filter(., task_name == stage_key_clicked) else . } %>%
          slice(1)
      }
      if (nrow(original_task) == 0) return(NULL)

      proj_row_id <- if ("proj_row_id" %in% names(original_task)) original_task$proj_row_id[1] else NA_integer_
      site_row_id <- if ("site_row_id" %in% names(original_task)) original_task$site_row_id[1] else NA_integer_
      stage_instance_id <- if ("stage_instance_id" %in% names(original_task)) original_task$stage_instance_id[1] else NA_integer_

      ms_json <- if ("milestones_json" %in% names(original_task)) original_task$milestones_json[1] else NULL
      ms_df <- if (!is.null(ms_json) && !is.na(ms_json)) parse_milestone_json_to_df(ms_json) else empty_milestone_df()
      ms_task_name <- as.character(original_task$task_name[1])
      milestone_row_count(nrow(ms_df))
      task_edit_context(list(
        project_id = project_id_from_group,
        site_name = if (is_sync) "各中心同步阶段" else site_name_from_group,
        task_name = ms_task_name,
        is_sync = is_sync,
        proj_row_id = proj_row_id,
        site_row_id = site_row_id,
        stage_instance_id = stage_instance_id,
        table_name = "09项目阶段实例表",
        milestone_table = if (is_sync) "04项目总表" else "03医院_项目表",
        milestone_row_id = if (is_sync) proj_row_id else as.integer(site_row_id),
        milestones = ms_df,
        milestone_raw = ms_json,
        milestone_stage_key = ms_task_name
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

    snapshot_cols <- c("planned_start_date", "actual_start_date", "planned_end_date", "actual_end_date", "remark_json", "progress", "contributors_json", "sample_json", "row_version")
    ms_tbl_fetch <- if (is_sync) "04项目总表" else "03医院_项目表"
    ms_id_fetch  <- if (is_sync) proj_row_id else as.integer(site_row_id)
    pt_for_lookup <- if ("project_type" %in% names(original_task)) as.character(original_task$project_type[1]) else NA_character_
    is_s09_task <- stage_supports_sample(as.character(task_info$task_name[1]), pt_for_lookup) && is_sync
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
    remarks_df <- if (!is.na(stage_instance_id) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      fetch_11_feedback_df(pg_con, stage_instance_id)
    } else {
      parse_remark_json_to_df(remark_raw)
    }
    snapshot_feedback_map <- if (!is.na(stage_instance_id) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      fetch_11_feedback_map(pg_con, stage_instance_id)
    } else {
      sort_feedback_map(parse_named_json_map(remark_raw))
    }

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
    milestone_raw <- if (!is.na(ms_id_fetch) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      tryCatch({
        r <- fetch_row_snapshot(pg_con, ms_tbl_fetch, ms_id_fetch, "milestones_json", lock = FALSE)
        if (!is.null(r)) r[["milestones_json"]] else NA_character_
      }, error = function(e) NA_character_)
    } else { NA_character_ }
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
        task_display_name = if ("task_display_name" %in% names(task_info)) as.character(task_info$task_display_name[1]) else NA_character_,
        is_sync = is_sync,
        supports_sample = is_s09_task,
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
        milestone_table = ms_tbl_fetch,
        milestone_row_id = ms_id_fetch,
        samples = sample_pairs,
        contributors = contrib_df,
        milestones = milestones_df,
        milestone_raw = milestone_raw,
        milestone_stage_key = tn,
        note_col = note_col,
        contrib_col = contrib_col,
        snapshot_row = snapshot_row,
        snapshot_project_row = snapshot_project_row,
        snapshot_feedback_map = snapshot_feedback_map
      ))
    } else {
      task_edit_context(NULL)
    }
    
    # ---- 与甘特图颜色逻辑保持完全一致 ----
    # 分母：计划周期 = 计划结束 - 计划开始（固定基准，不随实际开始日期变化）
    planned_duration <- as.numeric(planned_end_date - raw_planned_start)
    # 分子起点：实际开始有则用，否则退回计划开始（与 start_date = COALESCE(actual_start, planned_start) 一致）
    eff_start_detail <- if (!is.na(raw_actual_start)) raw_actual_start else raw_planned_start
    # 已制定计划起止但未填实际开始：与甘特「未开始」一致，不按日历推进理论进度
    no_actual_but_plan <- !is_unplanned && is.na(raw_actual_start) && !is.na(raw_planned_start) &&
      !is.na(raw_planned) && is.na(actual_end_date)
    # 理论计划进度 = 实际周期 / 计划周期（无实际开始时理论进度按 0）
    planned_p <- if (isTRUE(no_actual_but_plan)) {
      0
    } else {
      ifelse(
        is_completed,
        ifelse(!is.na(planned_duration) && planned_duration > 0,
               as.numeric(actual_end_date - eff_start_detail) / planned_duration,
               1.0),
        ifelse(!is.na(planned_duration) && planned_duration > 0,
               as.numeric(today - eff_start_detail) / planned_duration,
               ifelse(!is.na(eff_start_detail) && today >= eff_start_detail, 1.0, 0.0))
      )
    }

    # 计划/实际完成日期差异（仅在有 actual_end_date 时计算）
    delay_days <- if (!is.na(actual_end_date)) {
      as.numeric(actual_end_date - planned_end_date)
    } else {
      NA_real_
    }

    # 颜色用数值差额计算：实际完成度 - 理论计划进度（无实际开始时差值按 0，避免误报超前/落后）
    diff_p <- if (isTRUE(no_actual_but_plan)) 0 else (actual_progress - planned_p)

    # 特殊状态判定：与甘特图 is_not_started / missing_actual_done 逻辑一致
    # is_not_started: 今天 < 有效开始日期 且 进度为0 且 未完成（与 Gantt bar 判定完全对齐）
    is_not_started_detail <- !isTRUE(no_actual_but_plan) &&
      !is.na(eff_start_detail) &&
      isTRUE(today < eff_start_detail) &&
      isTRUE(actual_progress == 0) &&
      is.na(actual_end_date)
    missing_actual_done <- is.na(actual_end_date) && reported_progress >= 1.0

    diagnostic_text <- if (isTRUE(no_actual_but_plan)) {
      span("⏳ 尚未开始：已制定计划起止，但未填写实际开始日期；理论计划进度按 0% 计，直至填写实际开始。", style = "color: #757575; font-weight: bold;")
    } else if (is_not_started_detail) {
      span("⏳ 尚未开始：未到计划启动时间。", style = "color: #757575; font-weight: bold;")
    } else if (isTRUE(missing_actual_done)) {
      span("ℹ️ 已报完成，但未填写实际完成日期，请补充“实际完成时间”。", 
           style = "color: #1976D2; font-weight: bold;")
    } else if (is.na(diff_p)) {
      span("⏳ 尚未开始：计划日期未设置。", style = "color: #757575; font-weight: bold;")
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

    .render_task_detail_modal(
      task_info_row = task_info,
      proj_row_id = proj_row_id,
      stage_instance_id = stage_instance_id,
      raw_planned_start = raw_planned_start,
      raw_actual_start = raw_actual_start,
      raw_planned_end = raw_planned,
      actual_end_date = actual_end_date,
      is_unplanned = is_unplanned,
      is_s09_task = is_s09_task,
      actual_progress = actual_progress,
      planned_p = planned_p,
      diff_p = diff_p,
      delay_days = delay_days,
      importance_display = importance_display,
      importance_color = importance_color,
      manager_display = manager_display,
      participants_display = participants_display,
      diagnostic_text = diagnostic_text,
      can_edit = can_edit
    )
  })

  # ---- Tab2 点击事件：只读模式 ----
  observeEvent(input$my_gantt2_selected, {
    today <- today_beijing()
    req(input$my_gantt2_selected)
    on.exit(session$sendCustomMessage("resetGanttSelection", "my_gantt2"), add = TRUE)
    task_id <- as.numeric(input$my_gantt2_selected)
    data <- processed_data_tab2()
    if (is.null(data) || is.null(data$items) || nrow(data$items) == 0) return(NULL)
    task_info <- data$items[data$items$id == task_id, ]
    if (nrow(task_info) == 0) return(NULL)

    # 里程碑点击 — 只读概览
    if ("milestone_kind" %in% names(task_info) && nrow(task_info) == 1 && !is.na(task_info$milestone_kind[1])) {
      ms_row <- task_info[1, ]
      project_id_from_group <- sub("_.*$", "", ms_row$group)
      site_name_from_group <- sub("^[^_]+_", "", ms_row$group)
      is_sync <- grepl("同步阶段", ms_row$group)
      ss <- sync_stages_current()
      stage_key_clicked <- if ("milestone_stage_key" %in% names(ms_row) && !is.na(ms_row$milestone_stage_key[1])) ms_row$milestone_stage_key[1] else NA_character_
      has_stage_key <- !is.na(stage_key_clicked) && nzchar(stage_key_clicked)
      gd_lookup <- gantt2_data_all_stages()
      if (is.null(gd_lookup) || nrow(gd_lookup) == 0) gd_lookup <- gantt2_data_db()
      if (is.null(gd_lookup) || nrow(gd_lookup) == 0) return(NULL)
      if (is_sync) {
        original_task <- gd_lookup %>%
          filter(project_id == project_id_from_group, task_name %in% ss) %>%
          { if (has_stage_key) filter(., task_name == stage_key_clicked) else . } %>%
          slice(1)
      } else {
        original_task <- gd_lookup %>%
          filter(project_id == project_id_from_group, site_name == site_name_from_group, !task_name %in% ss) %>%
          { if (has_stage_key) filter(., task_name == stage_key_clicked) else . } %>%
          slice(1)
      }
      if (nrow(original_task) == 0) return(NULL)
      proj_row_id <- if ("proj_row_id" %in% names(original_task)) original_task$proj_row_id[1] else NA_integer_
      site_row_id <- if ("site_row_id" %in% names(original_task)) original_task$site_row_id[1] else NA_integer_
      stage_instance_id <- if ("stage_instance_id" %in% names(original_task)) original_task$stage_instance_id[1] else NA_integer_
      ms_json <- if ("milestones_json" %in% names(original_task)) original_task$milestones_json[1] else NULL
      ms_df <- if (!is.null(ms_json) && !is.na(ms_json)) parse_milestone_json_to_df(ms_json) else empty_milestone_df()
      ms_task_name <- as.character(original_task$task_name[1])
      task_edit_context(list(
        project_id = project_id_from_group,
        site_name = if (is_sync) "各中心同步阶段" else site_name_from_group,
        task_name = ms_task_name,
        is_sync = is_sync,
        proj_row_id = proj_row_id,
        site_row_id = site_row_id,
        stage_instance_id = stage_instance_id,
        table_name = "09项目阶段实例表",
        milestone_table = if (is_sync) "04项目总表" else "03医院_项目表",
        milestone_row_id = if (is_sync) proj_row_id else as.integer(site_row_id),
        milestones = ms_df,
        milestone_raw = ms_json,
        milestone_stage_key = ms_task_name
      ))
      showModal(modalDialog(
        title = "自由里程碑概览",
        size = "l",
        easyClose = TRUE,
        footer = modalButton("关闭"),
        uiOutput("milestone_viewer")
      ))
      return(NULL)
    }

    # 普通进度条点击 — 只读详情弹窗
    project_id_from_group <- sub("_.*$", "", task_info$group)
    site_name_from_group <- sub("^[^_]+_", "", task_info$group)
    is_sync <- grepl("同步阶段", task_info$group)
    gd <- gantt2_data_db()
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
    planned_end_date <- original_task$planned_end_date
    actual_end_date  <- original_task$actual_end_date
    is_unplanned <- if ("is_unplanned" %in% names(original_task)) isTRUE(original_task$is_unplanned[1]) else FALSE
    start_for_calc <- as.Date(task_info$start)
    proj_row_id <- if ("proj_row_id" %in% names(original_task)) original_task$proj_row_id[1] else NA_integer_
    site_row_id <- if ("site_row_id" %in% names(original_task)) original_task$site_row_id[1] else NA_integer_
    progress_match <- regmatches(task_info$content, regexpr("\\d+%", task_info$content))
    reported_progress <- if (length(progress_match) > 0) as.numeric(sub("%", "", progress_match)) / 100 else original_task$progress
    is_completed <- !is.na(actual_end_date)
    actual_progress <- ifelse(is_completed, 1.0, reported_progress)
    raw_planned_start <- if ("raw_planned_start_date" %in% names(original_task)) original_task$raw_planned_start_date[1] else (if (is_unplanned) as.Date(NA) else start_for_calc)
    raw_actual_start  <- if ("raw_actual_start_date"  %in% names(original_task)) original_task$raw_actual_start_date[1]  else as.Date(NA)
    raw_planned <- if ("raw_planned_end_date" %in% names(original_task)) original_task$raw_planned_end_date[1] else (if (is_unplanned) as.Date(NA) else planned_end_date)
    tn <- as.character(task_info$task_name[1])
    stage_instance_id <- if ("stage_instance_id" %in% names(original_task)) original_task$stage_instance_id[1] else NA_integer_
    pt_for_lookup <- if ("project_type" %in% names(original_task)) as.character(original_task$project_type[1]) else NA_character_
    is_s09_task <- stage_supports_sample(as.character(task_info$task_name[1]), pt_for_lookup) && is_sync
    snapshot_cols <- c("planned_start_date", "actual_start_date", "planned_end_date", "actual_end_date", "remark_json", "progress", "contributors_json", "sample_json", "row_version")
    ms_tbl_fetch <- if (is_sync) "04项目总表" else "03医院_项目表"
    ms_id_fetch  <- if (is_sync) proj_row_id else as.integer(site_row_id)
    snapshot_row <- if (!is.na(stage_instance_id) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      tryCatch(fetch_row_snapshot(pg_con, "09项目阶段实例表", stage_instance_id, snapshot_cols, lock = FALSE), error = function(e) NULL)
    } else { NULL }
    remark_raw <- if (!is.null(snapshot_row)) snapshot_row[["remark_json"]] else NA_character_
    remarks_df <- if (!is.na(stage_instance_id) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      fetch_11_feedback_df(pg_con, stage_instance_id)
    } else {
      parse_remark_json_to_df(remark_raw)
    }
    sample_pairs <- if (is_s09_task && !is.null(snapshot_row)) {
      parse_sample_df(snapshot_row[["sample_json"]])
    } else { empty_sample_df() }
    contrib_df <- if (!is.null(snapshot_row) && "contributors_json" %in% names(snapshot_row)) {
      parse_contrib_json_to_df(snapshot_row[["contributors_json"]])
    } else { empty_contrib_df() }
    milestone_raw <- if (!is.na(ms_id_fetch) && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
      tryCatch({
        r <- fetch_row_snapshot(pg_con, ms_tbl_fetch, ms_id_fetch, "milestones_json", lock = FALSE)
        if (!is.null(r)) r[["milestones_json"]] else NA_character_
      }, error = function(e) NA_character_)
    } else { NA_character_ }
    can_edit <- FALSE  # Tab2 只读

    task_edit_context(list(
      project_id = task_info$project_id[1],
      site_name = task_info$site_name[1],
      task_name = tn,
      is_sync = is_sync,
      supports_sample = is_s09_task,
      project_type = pt_for_lookup,
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
      col_map = list(
        planned_start = "planned_start_date", actual_start  = "actual_start_date",
        plan = "planned_end_date", act = "actual_end_date",
        note = "remark_json", progress = "progress"
      ),
      table_name = "09项目阶段实例表",
      milestone_table = ms_tbl_fetch,
      milestone_row_id = ms_id_fetch,
      samples = sample_pairs,
      contributors = contrib_df,
      milestones = parse_milestone_json_to_df(milestone_raw),
      milestone_raw = milestone_raw,
      milestone_stage_key = tn,
      note_col = "remark_json",
      contrib_col = "contributors_json",
      snapshot_row = snapshot_row
    ))

    # 计算进度诊断（与 Tab1 逻辑一致）
    planned_duration <- as.numeric(planned_end_date - raw_planned_start)
    eff_start_detail <- if (!is.na(raw_actual_start)) raw_actual_start else raw_planned_start
    no_actual_but_plan <- !is_unplanned && is.na(raw_actual_start) && !is.na(raw_planned_start) &&
      !is.na(raw_planned) && is.na(actual_end_date)
    planned_p <- if (isTRUE(no_actual_but_plan)) {
      0
    } else {
      ifelse(
        is_completed,
        ifelse(!is.na(planned_duration) && planned_duration > 0,
               as.numeric(actual_end_date - eff_start_detail) / planned_duration, 1.0),
        ifelse(!is.na(planned_duration) && planned_duration > 0,
               as.numeric(today - eff_start_detail) / planned_duration,
               ifelse(!is.na(eff_start_detail) && today >= eff_start_detail, 1.0, 0.0))
      )
    }
    delay_days <- if (!is.na(actual_end_date)) as.numeric(actual_end_date - planned_end_date) else NA_real_
    diff_p <- if (isTRUE(no_actual_but_plan)) 0 else (actual_progress - planned_p)
    is_not_started_detail <- !isTRUE(no_actual_but_plan) &&
      !is.na(eff_start_detail) && isTRUE(today < eff_start_detail) &&
      isTRUE(actual_progress == 0) && is.na(actual_end_date)
    missing_actual_done <- is.na(actual_end_date) && reported_progress >= 1.0
    diagnostic_text <- if (isTRUE(no_actual_but_plan)) {
      span("⏳ 尚未开始：已制定计划起止，但未填写实际开始日期。", style = "color: #757575; font-weight: bold;")
    } else if (is_not_started_detail) {
      span("⏳ 尚未开始：未到计划启动时间。", style = "color: #757575; font-weight: bold;")
    } else if (isTRUE(missing_actual_done)) {
      span("ℹ️ 已报完成，但未填写实际完成日期。", style = "color: #1976D2; font-weight: bold;")
    } else if (is.na(diff_p)) {
      span("⏳ 尚未开始：计划日期未设置。", style = "color: #757575; font-weight: bold;")
    } else if(diff_p < -0.5) {
      span("❌ 严重落后（落后50%以上）。", style = "color: #D32F2F; font-weight: bold;")
    } else if(diff_p < -0.3) {
      span("⚠️ 明显滞后（落后30%-50%）。", style = "color: #F44336; font-weight: bold;")
    } else if(diff_p < -0.15) {
      span("⚠️ 轻微滞后（落后15%-30%）。", style = "color: #FF6F00;")
    } else if(diff_p < -0.05) {
      span("⚠️ 轻微滞后（落后5%-15%）。", style = "color: #FFB300;")
    } else if(diff_p < 0.1) {
      span("ℹ️ 进度基本匹配（偏差5%以内/超前10%以内）。", style = "color: #558B2F;")
    } else if(diff_p >= 0.25) {
      span("✅ 大幅超前（超前25%以上）。", style = "color: #2E7D32; font-weight: bold;")
    } else if(diff_p >= 0.1) {
      span("✅ 明显超前（超前10%-25%）。", style = "color: #4CAF50; font-weight: bold;")
    } else {
      span("🏃 进度平稳：符合预期。", style = "color: #1565C0;")
    }

    # 查询项目负责人和参与人员
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
    importance_level <- if ("重要紧急程度" %in% names(original_task)) as.character(original_task[["重要紧急程度"]][1]) else NA_character_
    importance_display <- if (is.na(importance_level) || !nzchar(trimws(importance_level))) "（未设置）" else trimws(importance_level)
    importance_color <- switch(
      trimws(importance_level),
      "重要紧急" = "#C62828", "重要不紧急" = "#F57C00", "紧急不重要" = "#1976D2", "不重要不紧急" = "#616161", "#757575"
    )

    .render_task_detail_modal(
      task_info_row = task_info,
      proj_row_id = proj_row_id,
      stage_instance_id = stage_instance_id,
      raw_planned_start = raw_planned_start,
      raw_actual_start = raw_actual_start,
      raw_planned_end = raw_planned,
      actual_end_date = actual_end_date,
      is_unplanned = is_unplanned,
      is_s09_task = is_s09_task,
      actual_progress = actual_progress,
      planned_p = planned_p,
      diff_p = diff_p,
      delay_days = delay_days,
      importance_display = importance_display,
      importance_color = importance_color,
      manager_display = manager_display,
      participants_display = participants_display,
      diagnostic_text = diagnostic_text,
      can_edit = can_edit,
      read_only = TRUE
    )
  })
  # ---- End Tab2 点击事件 ----

  observeEvent(input$project_header_clicked, {
    ev <- input$project_header_clicked
    req(is.list(ev), !is.null(ev$project_id))
    display_pid <- as.character(ev$project_id)
    req(nzchar(display_pid))
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return(invisible(NULL))
    auth <- current_user_auth()
    if (isTRUE(auth$allow_none)) return(invisible(NULL))

    proj_db <- resolve_project_db_id_from_display(pg_con, display_pid)
    if (is.na(proj_db)) {
      showNotification("未找到该项目，无法打开汇总。", type = "warning")
      return(invisible(NULL))
    }
    if (!isTRUE(auth$allow_all) && nzchar(auth$allowed_subquery)) {
      chk <- tryCatch(
        DBI::dbGetQuery(pg_con, paste0(
          'SELECT 1 AS ok FROM public."04项目总表" WHERE id = $1 AND (id IN (', auth$allowed_subquery, ')',
          if (!is.null(auth$disallowed_subquery) && nzchar(auth$disallowed_subquery)) paste0(' OR id IN (', auth$disallowed_subquery, ')'),
          ')'
        ), params = list(as.integer(proj_db))),
        error = function(e) data.frame(ok = integer(0))
      )
      if (nrow(chk) == 0L) {
        showNotification("无权查看该项目汇总。", type = "warning")
        return(invisible(NULL))
      }
    }

    proj_row <- tryCatch(
      DBI::dbGetQuery(pg_con,
        'SELECT p."项目类型" AS project_type, COALESCE(NULLIF(p."项目名称", \'\'), \'项目-\' || p.id::text) AS display_name,
                p."重要紧急程度" AS importance, mgr."姓名" AS manager_name
         FROM public."04项目总表" p
         LEFT JOIN public."05人员表" mgr ON mgr.id = p."05人员表_id"
         WHERE p.id = $1',
        params = list(as.integer(proj_db))
      ),
      error = function(e) NULL
    )
    if (is.null(proj_row) || nrow(proj_row) == 0L) {
      showNotification("加载项目信息失败。", type = "error")
      return(invisible(NULL))
    }

    participants_display <- character(0)
    manager_display <- "（无）"
    tryCatch({
      mgr <- DBI::dbGetQuery(pg_con,
        'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."04项目总表" proj ON proj."05人员表_id" = p.id WHERE proj.id = $1',
        params = list(as.integer(proj_db)))
      if (nrow(mgr) > 0) {
        pos <- if (is.na(mgr[["岗位"]][1]) || !nzchar(trimws(as.character(mgr[["岗位"]][1])))) "" else as.character(mgr[["岗位"]][1])
        nm <- if (is.na(mgr[["姓名"]][1])) "" else as.character(mgr[["姓名"]][1])
        manager_display <- if (nzchar(pos)) paste0(pos, "-", nm) else (if (nzchar(nm)) nm else "（无）")
      }
      parts <- DBI::dbGetQuery(pg_con,
        'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."05人员表_id" = p.id WHERE m."04项目总表_id" = $1',
        params = list(as.integer(proj_db)))
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
    }, error = function(e) { })

    inst_sql_from <-
      'FROM public."09项目阶段实例表" si
         JOIN public."08项目阶段定义表" d ON d.id = si.stage_def_id
         LEFT JOIN public."03医院_项目表" s ON s.id = si.site_project_id
         LEFT JOIN public."01医院信息表" h ON h.id = s."01_hos_resource_table医院信息表_id"
         WHERE si.project_id = $1'
    inst_sql_select <-
      'SELECT si.id AS stage_instance_id,
                d.stage_key AS task_name, d.stage_order AS stage_ord,
                COALESCE(si."阶段实例自定义名称", d.stage_name) AS task_display_name,
                CASE WHEN d.stage_scope = \'sync\'
                     THEN \'所有中心（同步）\'
                     ELSE COALESCE(NULLIF(h."医院名称", \'\'), \'中心-\' || COALESCE(s.id, 0)::text) END AS site_name,
                si.remark_json::text AS remark_json,
                si.contributors_json::text AS contributors_json,
                COALESCE(si.progress, 0)::numeric AS progress_pct,
                si.planned_start_date AS planned_start_date,
                si.planned_end_date AS planned_end_date,
                si.actual_start_date AS actual_start_date,
                si.actual_end_date AS actual_end_date,
                COALESCE(si.is_active, TRUE) AS si_is_active'
    inst <- tryCatch(
      DBI::dbGetQuery(pg_con,
        paste0(inst_sql_select, ' ', inst_sql_from, ' ORDER BY d.stage_order, site_name'),
        params = list(as.integer(proj_db))
      ),
      error = function(e) NULL
    )
    if (is.null(inst)) {
      showNotification("加载阶段实例失败。", type = "error")
      return(invisible(NULL))
    }
    # 与甘特视图一致：仅 COALESCE(si.is_active, TRUE)=TRUE 的 09 行参与贡献者与「按阶段」罗列（在库内过滤，避免 R 端布尔类型歧义）
    inst_act <- tryCatch(
      DBI::dbGetQuery(pg_con,
        paste0(inst_sql_select, ' ', inst_sql_from, ' AND COALESCE(si.is_active, TRUE) = TRUE ORDER BY d.stage_order, site_name'),
        params = list(as.integer(proj_db))
      ),
      error = function(e) NULL
    )
    if (is.null(inst_act)) {
      inst_act <- inst[0L, , drop = FALSE]
    }
    if (nrow(inst) > 0L) {
      inst$planned_start_date <- as.Date(inst$planned_start_date)
      inst$planned_end_date <- as.Date(inst$planned_end_date)
      inst$actual_start_date <- as.Date(inst$actual_start_date)
      inst$actual_end_date <- as.Date(inst$actual_end_date)
    }
    if (nrow(inst_act) > 0L) {
      inst_act$planned_start_date <- as.Date(inst_act$planned_start_date)
      inst_act$planned_end_date <- as.Date(inst_act$planned_end_date)
      inst_act$actual_start_date <- as.Date(inst_act$actual_start_date)
      inst_act$actual_end_date <- as.Date(inst_act$actual_end_date)
    }

    # effective_center_progress_pct_for_summary / fmt_amt 已迁至 R/ivd_server_helpers.R

    remark_rows <- list()
    contrib_all <- list()
    fb_proj <- fetch_11_feedback_all_for_project_df(pg_con, proj_db)
    if (!is.null(fb_proj) && nrow(fb_proj) > 0L) {
      for (j in seq_len(nrow(fb_proj))) {
        tk <- as.character(fb_proj$task_key_raw[j])
        sn <- as.character(fb_proj$site_name[j])
        rj <- fb_proj$fb_rj[j]
        reps <- tryCatch({
          jj <- jsonlite::fromJSON(rj, simplifyVector = TRUE)
          if (is.null(jj)) character(0) else as.character(jj)
        }, error = function(e) character(0))
        reps <- unique(trimws(reps[nzchar(trimws(reps))]))
        reporter <- paste(reps, collapse = "、")
        st_lab <- if (identical(tk, "__project_scope__")) {
          "项目要点"
        } else {
          dn_fb <- if ("task_display_name" %in% names(fb_proj)) as.character(fb_proj$task_display_name[j]) else NA_character_
          if (!is.na(dn_fb) && nzchar(dn_fb)) dn_fb else stage_label_for_key(tk)
        }
            remark_rows[[length(remark_rows) + 1L]] <- data.frame(
          type = trimws(as.character(fb_proj$fb_type[j] %||% "")),
          updated_at = as.character(fb_proj$fb_updated[j] %||% ""),
          reporter = reporter,
          content = as.character(fb_proj$fb_content[j] %||% ""),
          stage_label = st_lab,
              site_name = sn,
          fb_id = suppressWarnings(as.integer(fb_proj$id[j])),
          task_key_raw = tk,
              stringsAsFactors = FALSE
            )
      }
    }
    if (nrow(inst_act) > 0L) {
      for (ri in seq_len(nrow(inst_act))) {
        tk <- as.character(inst_act$task_name[ri])
        sn <- as.character(inst_act$site_name[ri])
        cj <- inst_act$contributors_json[ri]
        if (is.na(cj)) cj <- ""
        cdf <- parse_contrib_json_to_df(cj)
        if (nrow(cdf) > 0L) {
          for (j in seq_len(nrow(cdf))) {
            contrib_all[[length(contrib_all) + 1L]] <- data.frame(
              person = trimws(as.character(cdf$person[j] %||% "")),
              role = trimws(as.character(cdf$role[j] %||% "")),
              work = trimws(as.character(cdf$work[j] %||% "")),
              amount = as.character(cdf$amount[j] %||% ""),
              task_name = tk,
              site_name = sn,
              progress_pct = effective_center_progress_pct_for_summary(
                inst_act$planned_start_date[ri],
                inst_act$planned_end_date[ri],
                inst_act$actual_start_date[ri],
                inst_act$actual_end_date[ri],
                inst_act$progress_pct[ri]
              ),
              stringsAsFactors = FALSE
            )
          }
        }
      }
    }

    remark_df <- if (length(remark_rows) > 0L) bind_rows(remark_rows) else empty_remark_df()
    if (nrow(remark_df) > 0L) {
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
      remark_df <- remark_df %>%
        arrange(.data$type_rank, desc(.data$sort_date), desc(.data$reporter))
    }

    contrib_df <- if (length(contrib_all) > 0L) {
      bind_rows(contrib_all)
    } else {
      data.frame(
        person = character(0), role = character(0), work = character(0), amount = character(0),
        task_name = character(0), site_name = character(0), progress_pct = numeric(0),
        stringsAsFactors = FALSE
      )
    }

    importance_level <- as.character(proj_row$importance[1] %||% "")
    importance_display <- if (is.na(importance_level) || !nzchar(trimws(importance_level))) "（未设置）" else trimws(importance_level)
    importance_color <- switch(
      trimws(importance_level),
      "重要紧急" = "#C62828",
      "重要不紧急" = "#F57C00",
      "紧急不重要" = "#1976D2",
      "不重要不紧急" = "#616161",
      "#757575"
    )

    ui_remarks <- if (nrow(remark_df) == 0L) {
      tags$span("（暂无）", style = "color:#999;")
    } else {
      dec_by_fid <- fetch_decisions_linked_to_feedback_ids(pg_con, remark_df$fb_id)
      ui_gantt_feedback_with_decisions(auth, pg_con, remark_df, dec_by_fid)
    }

    # merge_contrib_totals 已迁至 R/ivd_server_helpers.R

    ui_contrib_by_person <- local({
      if (nrow(contrib_df) == 0L) {
        return(tags$span("（暂无进度贡献者记录）", style = "color:#999;"))
      }
      m <- merge_contrib_totals(contrib_df %>% select(person, role, work, amount))
      persons <- sort(unique(m$person))
      tags$div(
        style = "display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); column-gap: 16px; row-gap: 4px; align-items: start;",
        lapply(persons, function(pn) {
          sub <- m %>% filter(person == pn)
          tags$div(
            style = "min-width: 0;",
            tagList(
              tags$div(style = "font-weight: 700; margin-top: 10px; color: #1a237e;", pn),
              tagList(lapply(seq_len(nrow(sub)), function(i) {
                contrib_summary_line_tag(
                  sub$role[i], sub$work[i], fmt_amt(sub$total_amt[i]),
                  font_size = "14px"
                )
              }))
            )
          )
        })
      )
    })

    ui_contrib_by_stage <- local({
      if (nrow(inst_act) == 0L) {
        return(tags$span("（无已激活阶段，或暂无阶段数据）", style = "color:#999;"))
      }
      st_ord <- inst_act %>% distinct(task_name, stage_ord) %>% arrange(stage_ord)
      tags$div(
        style = "display: flex; flex-direction: row; flex-wrap: nowrap; gap: 12px; align-items: flex-start; overflow-x: auto; padding-bottom: 6px; -webkit-overflow-scrolling: touch;",
        lapply(seq_len(nrow(st_ord)), function(si) {
          tk <- as.character(st_ord$task_name[si])
          sub_inst <- inst_act %>% filter(.data$task_name == tk)
          if (nrow(sub_inst) > 0L) {
            sub_inst$display_progress_pct <- vapply(seq_len(nrow(sub_inst)), function(ii) {
              effective_center_progress_pct_for_summary(
                sub_inst$planned_start_date[ii],
                sub_inst$planned_end_date[ii],
                sub_inst$actual_start_date[ii],
                sub_inst$actual_end_date[ii],
                sub_inst$progress_pct[ii]
              )
            }, numeric(1))
          }
          prog_lines <- if (nrow(sub_inst) == 0L) {
            character(0)
          } else {
            sub_inst %>%
              group_by(site_name) %>%
              summarise(display_progress_pct = max(display_progress_pct), .groups = "drop") %>%
              arrange(site_name) %>%
              mutate(s = sprintf("%s：%.0f%%", site_name, display_progress_pct)) %>%
              pull(s)
          }
          prog_block <- if (length(prog_lines) == 0L) {
            "各中心进度：\n（无）"
          } else {
            paste(c("各中心进度：", paste0(prog_lines, "；")), collapse = "\n")
          }
          cdf_s <- contrib_df %>% filter(.data$task_name == tk)
          body <- if (nrow(cdf_s) == 0L) {
            tags$span("（本阶段无贡献者记录）", style = "color:#888; font-size:13px;")
          } else {
            m <- merge_contrib_totals(cdf_s %>% select(person, role, work, amount))
            persons <- sort(unique(m$person))
            tagList(lapply(persons, function(pn) {
              sub <- m %>% filter(person == pn)
              tagList(
                tags$div(style = "font-weight: 600; margin-top: 6px; color: #333;", pn),
                tagList(lapply(seq_len(nrow(sub)), function(i) {
                  contrib_summary_line_tag(
                    sub$role[i], sub$work[i], fmt_amt(sub$total_amt[i]),
                    font_size = "13px"
                  )
                }))
              )
            }))
          }
          tags$div(
            style = "flex: 0 0 auto; min-width: 220px; max-width: 300px; padding: 10px; background: #fafafa; border-radius: 6px; border: 1px solid #e0e0e0;",
            tags$div(
              style = "font-weight: 800; color: #263238; margin-bottom: 4px;",
              {
                dn_s <- if ("task_display_name" %in% names(sub_inst)) as.character(sub_inst$task_display_name[1]) else NA_character_
                if (!is.na(dn_s) && nzchar(dn_s)) dn_s else stage_label_for_key(tk)
              }
            ),
            tags$div(
              style = "font-size: 13px; color: #546e7a; margin-bottom: 8px; white-space: pre-line; word-break: break-word;",
              prog_block
            ),
            body
          )
        })
      )
    })

    # 存储项目汇总上下文（供"管理项目要点"按钮使用）
    proj_summary_ctx(list(
      proj_db = proj_db,
      display_name = as.character(proj_row$display_name[1]),
      remark_df = remark_df
    ))

    showModal(modalDialog(
      title = sprintf("项目汇总 — %s", as.character(proj_row$display_name[1])),
      size = "l",
      easyClose = TRUE,
      footer = tagList(
        if (!identical(input$gantt_sub_tabs, "gantt_view2"))
          actionButton("btn_proj_summary_manage_points", "添加/管理项目要点", class = "btn-primary"),
        modalButton("关闭")
      ),
      tags$div(
        style = "max-height: 72vh;",
        tabsetPanel(
          id = "project_summary_tabs",
          type = "tabs",
          tabPanel(
            title = "基础信息",
            tags$div(
              class = "well",
              style = "margin-top: 12px; margin-bottom: 0; padding: 16px 18px; border-radius: 8px; background: #fafafa; border: 1px solid #e8e8e8;",
              fluidRow(
                column(6,
                  p(tags$b("项目类型："), as.character(proj_row$project_type[1] %||% "（空）")),
                  p(tags$b("项目名称："), as.character(proj_row$display_name[1])),
                  p(tags$b("项目负责人："), manager_display)
                ),
                column(6,
                  p(tags$b("重要紧急程度："),
                    tags$span(importance_display, style = sprintf("color: %s; font-weight: bold;", importance_color))),
                  p(tags$b("参与人员："),
                    if (length(participants_display) > 0)
                      tags$div(style = "white-space: pre-wrap; margin-top: 4px;", paste(participants_display, collapse = "\n"))
                    else
                      tags$span("（无）", style = "color: #999;"))
                )
              )
            )
          ),
          tabPanel(
            title = "项目要点",
            tags$div(
              class = "well",
              style = "margin-top: 12px; margin-bottom: 0; padding: 16px 18px; border-radius: 8px; background: #fafafa; border: 1px solid #e8e8e8; max-height: 58vh; overflow-y: auto;",
              ui_remarks
            )
          ),
          tabPanel(
            title = "贡献者",
            tags$div(
              style = "margin-top: 8px;",
              tags$div(
                class = "well",
                style = "margin-bottom: 0; padding: 16px 18px; border-radius: 8px; background: #fafafa; border: 1px solid #e8e8e8; max-height: 54vh; overflow-y: auto;",
                tags$div(
                  style = "display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; margin-bottom: 10px;",
                  tags$div(
                    style = "flex: 1; min-width: 0;",
                    tags$div(id = "proj_summary_contrib_hint_person"),
                    tags$div(
                      id = "proj_summary_contrib_hint_stage",
                      style = "display: none;"
                    )
                  ),
                  tags$button(
                    type = "button",
                    id = "proj_summary_contrib_toggle_btn",
                    class = "btn btn-default",
                    style = "font-size: 13px; padding: 5px 14px; line-height: 1.35; font-weight: 700; flex-shrink: 0; margin: 0;",
                    "按阶段"
                  )
                ),
                tags$div(id = "proj_summary_view_person", ui_contrib_by_person),
                tags$div(id = "proj_summary_view_stage", style = "display: none;", ui_contrib_by_stage)
              ),
              tags$script(HTML('
                setTimeout(function() {
                  var btn = $("#proj_summary_contrib_toggle_btn");
                  if (!btn.length) return;
                  var hintP = $("#proj_summary_contrib_hint_person");
                  var hintS = $("#proj_summary_contrib_hint_stage");
                  function showPerson() {
                    $("#proj_summary_view_person").show();
                    $("#proj_summary_view_stage").hide();
                    hintP.show();
                    hintS.hide();
                    btn.text("按阶段");
                  }
                  function showStage() {
                    $("#proj_summary_view_person").hide();
                    $("#proj_summary_view_stage").show();
                    hintP.hide();
                    hintS.show();
                    btn.text("按人员");
                  }
                  btn.off("click.projSummaryContrib").on("click.projSummaryContrib", function() {
                    if ($("#proj_summary_view_person").is(":visible")) showStage(); else showPerson();
                  });
                }, 0);
              '))
            )
          )
        )
      )
    ))
  }, ignoreNULL = TRUE)

  # ---------- 项目汇总 — 管理项目要点弹窗 ----------

  output$proj_summary_remark_editor <- renderUI({
    proj_summary_remark_refresh()
    ctx <- proj_summary_ctx()
    if (is.null(ctx)) return(NULL)
    n_rows <- proj_summary_remark_row_count()
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
        typ_show <- trimws(type_val)
        bracket_lbl <- if (nzchar(typ_show)) paste0("【", typ_show, i, "】") else paste0("【要点", i, "】")
        tags$div(
          style = "margin-bottom: 12px;",
          tags$div(style = "font-weight: 700; color: #37474f; margin-bottom: 6px; font-size: 13px;", bracket_lbl),
          fluidRow(
            column(
              3,
              selectizeInput(
                paste0("ps_remark_type_", i),
                if (i == 1L) "类型" else NULL,
                choices = type_choices,
                selected = if (nzchar(type_val)) type_val else NULL,
                options = list(create = TRUE)
              )
            ),
            column(
              9,
              textAreaInput(
                paste0("ps_remark_content_", i),
                if (i == 1L) sprintf("内容（反馈人：%s）", current_reporter) else NULL,
                value = content,
                rows = 3,
                placeholder = "留空则删除该条"
              )
            )
          )
        )
      })
    } else {
      list(tags$p("当前暂无项目要点。", style = "color:#777;"))
    }
    tagList(
      tags$b("项目要点（不分中心/阶段）"),
      tags$p(tags$small(sprintf("反馈人将自动写入当前登录帐号：%s；留空内容的要点将在保存时删除。", current_reporter))),
      tags$div(style = "margin-top:10px;", rows_ui)
    )
  })

  observeEvent(input$btn_proj_summary_manage_points, {
    ctx <- proj_summary_ctx()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    auth <- current_user_auth()
    if (isTRUE(auth$allow_none)) return

    proj_db <- ctx$proj_db
    # 从数据库重新加载项目级要点
    remarks_df <- fetch_project_scope_feedback_11(pg_con, proj_db)
    ctx$remark_entries <- remarks_df
    proj_summary_ctx(ctx)
    proj_summary_remark_row_count(nrow(remarks_df))
    proj_summary_remark_refresh(proj_summary_remark_refresh() + 1L)

    showModal(modalDialog(
      title = sprintf("管理项目要点 — %s", ctx$display_name),
      size = "l",
      easyClose = TRUE,
      footer = tagList(
        actionButton("btn_proj_summary_add_remark", "新增要点", class = "btn-success"),
        actionButton("btn_proj_summary_save_remarks", "保存", class = "btn-primary"),
        modalButton("关闭")
      ),
      uiOutput("proj_summary_remark_editor")
    ))
  })

  observeEvent(input$btn_proj_summary_add_remark, {
    ctx <- proj_summary_ctx()
    req(ctx)
    n_cur <- proj_summary_remark_row_count()
    if (is.null(n_cur) || n_cur < 0L) n_cur <- 0L
    remark_df <- ctx$remark_entries
    if (is.null(remark_df)) remark_df <- empty_remark_df()
    if (!"fb_id" %in% names(remark_df)) remark_df$fb_id <- rep(NA_integer_, nrow(remark_df))
    auth <- current_user_auth()
    current_reporter <- normalize_text(auth$name)
    if (is.na(current_reporter)) current_reporter <- normalize_text(auth$work_id)
    if (is.na(current_reporter)) current_reporter <- "未知用户"
    if (n_cur > 0L) {
      for (i in seq_len(n_cur)) {
        type_val <- tryCatch(trimws(as.character(input[[paste0("ps_remark_type_", i)]])), error = function(e) "")
        content <- tryCatch(trimws(as.character(input[[paste0("ps_remark_content_", i)]])), error = function(e) "")
        if (i <= nrow(remark_df)) {
          remark_df$reporter[i] <- current_reporter
          remark_df$updated_at[i] <- if (i <= nrow(remark_df) && nzchar(as.character(remark_df$updated_at[i] %||% ""))) as.character(remark_df$updated_at[i]) else ""
          remark_df$type[i] <- type_val
          remark_df$content[i] <- content
        }
      }
    }
    new_key <- sprintf("__proj_%d_%s_%06d", ctx$proj_db, format(Sys.time(), "%Y%m%d%H%M%S", tz = APP_TZ_CN), sample.int(999999L, 1L))
    new_row <- data.frame(
      entry_key = new_key,
      reporter = current_reporter,
      updated_at = now_beijing_str("%Y-%m-%d %H:%M"),
      type = "",
      content = "",
      fb_id = NA_integer_,
      stringsAsFactors = FALSE
    )
    remark_df <- rbind(remark_df, new_row)
    ctx$remark_entries <- remark_df
    proj_summary_ctx(ctx)
    proj_summary_remark_row_count(nrow(remark_df))
    proj_summary_remark_refresh(proj_summary_remark_refresh() + 1L)
  })

  observeEvent(input$btn_proj_summary_save_remarks, {
    ctx <- proj_summary_ctx()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    auth <- current_user_auth()
    if (isTRUE(auth$allow_none)) return

    remark_df <- ctx$remark_entries
    if (is.null(remark_df)) remark_df <- empty_remark_df()
    if (!"fb_id" %in% names(remark_df)) remark_df$fb_id <- rep(NA_integer_, nrow(remark_df))
    n_rows <- proj_summary_remark_row_count()
    if (is.null(n_rows) || n_rows < 0L) n_rows <- 0L

    current_reporter <- normalize_text(auth$name)
    if (is.na(current_reporter)) current_reporter <- normalize_text(auth$work_id)
    if (is.na(current_reporter)) current_reporter <- "未知用户"

    if (n_rows > 0L) {
      for (i in seq_len(n_rows)) {
        type_val <- tryCatch(trimws(as.character(input[[paste0("ps_remark_type_", i)]])), error = function(e) "")
        content <- tryCatch(trimws(as.character(input[[paste0("ps_remark_content_", i)]])), error = function(e) "")
        if (i <= nrow(remark_df)) {
          remark_df$type[i] <- type_val
          remark_df$content[i] <- content
          remark_df$reporter[i] <- current_reporter
        }
      }
    }

    # 构建 merged_named_list：仅保留有内容的行
    merged <- list()
    for (i in seq_len(nrow(remark_df))) {
      cont <- trimws(as.character(remark_df$content[i]))
      if (!nzchar(cont)) next
      key <- as.character(remark_df$entry_key[i])
      merged[[key]] <- list(
        type = trimws(as.character(remark_df$type[i])),
        content = cont,
        reporters = c(trimws(as.character(remark_df$reporter[i]))),
        updated_at = if (nzchar(as.character(remark_df$updated_at[i] %||% ""))) as.character(remark_df$updated_at[i]) else now_beijing_str("%Y-%m-%d %H:%M")
      )
    }

    tryCatch({
      apply_merged_project_scope_feedback_11(pg_con, ctx$proj_db, merged, auth$work_id, auth$name)
      insert_audit_log(pg_con, auth$work_id, auth$name,
        "UPDATE", "11阶段问题反馈表", ctx$proj_db,
        sprintf("项目汇总管理要点[%s]", ctx$display_name),
        "项目级要点批量更新", NULL, NULL, NULL)
      showNotification("项目要点已保存", type = "message")
      removeModal()
      # 刷新项目汇总弹窗（重新触发点击）
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) {
      showNotification(paste("保存失败:", e$message), type = "error")
    })
  })

  open_task_edit_modal <- function(ctx) {
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    removeModal()
    psd <- ctx$planned_start_date
    asd <- ctx$actual_start_date
    pd <- ctx$planned_end_date
    ad <- ctx$actual_end_date
    proj_type <- ctx$project_type
    # ---- 上阶段信息（参考）----
    prev_name <- "（无）"
    prev_ps <- prev_as <- prev_pe <- prev_ae <- prev_prog <- "（无）"
    prev_site_hint <- NULL  # 若上一阶段为子中心阶段且选取了某特定中心，记录中心名用于提示
    tryCatch({
      gd_for_prev <- current_gantt_data()
      if (!is.null(gd_for_prev) && nrow(gd_for_prev) > 0) {
        ss_local <- sync_stages_current()
        # 取本项目全部已激活阶段行（同步+子中心混合；同步阶段已按中心展开）
        gd_proj <- gd_for_prev %>% filter(project_id == ctx$project_id)
        if (nrow(gd_proj) > 0) {
          # 以 08 表 stage_order 为序，获取去重后的活跃阶段键列表（同步与子中心混排）
          stage_order_map <- stage_catalog()$defs %>%
            { if (!is.na(proj_type) && nzchar(proj_type)) filter(., project_type == proj_type) else . } %>%
            select(stage_key, stage_order) %>%
            distinct(stage_key, .keep_all = TRUE)
          active_keys <- gd_proj %>%
            distinct(task_name) %>%
            left_join(stage_order_map, by = c("task_name" = "stage_key")) %>%
            arrange(coalesce(stage_order, 999999L)) %>%
            pull(task_name)
          cur_idx <- which(active_keys == ctx$task_name)[1]
          if (!is.na(cur_idx) && cur_idx > 1L) {
            prev_key  <- active_keys[cur_idx - 1L]
            prev_name <- {
              pdn <- gd_proj %>% filter(task_name == prev_key) %>% slice(1) %>% pull(task_display_name)
              if (!is.na(pdn) && nzchar(as.character(pdn))) as.character(pdn) else stage_label_for_key(prev_key)
            }
            prev_is_sync <- prev_key %in% ss_local
            prev_rows <- gd_proj %>% filter(task_name == prev_key)
            if (nrow(prev_rows) > 0) {
              cur_is_sync  <- isTRUE(ctx$is_sync)
              same_scope   <- (cur_is_sync == prev_is_sync)
              # 选取代表行 -------------------------------------------------------
              # ① 上一阶段是同步阶段：各行数据相同（展开副本），直接取首行
              # ② 上下阶段 scope 相同（都是子中心阶段）：直接取本中心自己的行，无需跨中心比较
              # ③ 上下阶段 scope 不同（当前为同步，上一阶段为子中心）：跨中心选出最慢进度的代表行
              chosen <- if (prev_is_sync || nrow(prev_rows) == 1L) {
                prev_rows[1L, ]
              } else if (same_scope) {
                # 同 scope：只看本中心
                same_site <- prev_rows %>% filter(site_name == ctx$site_name)
                if (nrow(same_site) > 0) same_site[1L, ] else prev_rows[1L, ]
              } else {
                # 跨 scope（当前同步 → 上一子中心）：按四级优先级选代表中心
                # 优先级：① 未完成+有计划 → ② 已完成+有计划 → ③ 未完成+无计划 → ④ 已完成+无计划
                # "有计划" = raw_planned_start_date 与 raw_planned_end_date 均非 NA
                ps_col_name <- if ("raw_planned_start_date" %in% names(prev_rows)) "raw_planned_start_date" else "planned_start_date"
                pe_col_name <- if ("raw_planned_end_date"   %in% names(prev_rows)) "raw_planned_end_date"   else "planned_end_date"
                hp <- !is.na(suppressWarnings(as.Date(prev_rows[[ps_col_name]]))) &
                      !is.na(suppressWarnings(as.Date(prev_rows[[pe_col_name]])))
                dn <- !is.na(prev_rows$actual_end_date)
                pool_1 <- prev_rows[!dn &  hp, ]   # 未完成+有计划
                pool_2 <- prev_rows[ dn &  hp, ]   # 已完成+有计划
                pool_3 <- prev_rows[!dn & !hp, ]   # 未完成+无计划
                pool_4 <- prev_rows[ dn & !hp, ]   # 已完成+无计划（兜底）
                # 选第一个非空池
                if (nrow(pool_1) > 0) {
                  # 未完成+有计划：取进度最慢；并列取计划结束最晚
                  prog_v <- suppressWarnings(as.numeric(pool_1$progress))
                  min_p  <- if (any(!is.na(prog_v))) min(prog_v, na.rm = TRUE) else 0
                  cands  <- pool_1[is.na(prog_v) | (!is.na(prog_v) & prog_v <= min_p + 1e-9), ]
                  pe_v   <- suppressWarnings(as.Date(cands[[pe_col_name]]))
                  cands[order(-as.numeric(pe_v), na.last = TRUE)[1L], ]
                } else if (nrow(pool_2) > 0) {
                  # 已完成+有计划：取实际结束最晚
                  ae_v <- suppressWarnings(as.Date(pool_2$actual_end_date))
                  pool_2[order(-as.numeric(ae_v), na.last = TRUE)[1L], ]
                } else if (nrow(pool_3) > 0) {
                  # 未完成+无计划：取进度最慢（通常都是 0，再按计划结束排）
                  prog_v <- suppressWarnings(as.numeric(pool_3$progress))
                  min_p  <- if (any(!is.na(prog_v))) min(prog_v, na.rm = TRUE) else 0
                  cands  <- pool_3[is.na(prog_v) | (!is.na(prog_v) & prog_v <= min_p + 1e-9), ]
                  pe_v   <- suppressWarnings(as.Date(cands[[pe_col_name]]))
                  cands[order(-as.numeric(pe_v), na.last = TRUE)[1L], ]
                } else {
                  # 已完成+无计划：取实际结束最晚
                  ae_v <- suppressWarnings(as.Date(pool_4$actual_end_date))
                  pool_4[order(-as.numeric(ae_v), na.last = TRUE)[1L], ]
                }
              }
              # 跨 scope 且多中心时，记录显示的是哪个中心
              if (!prev_is_sync && !same_scope && nrow(prev_rows) > 1L && "site_name" %in% names(chosen)) {
                prev_site_hint <- as.character(chosen$site_name[1L])
              }
              # 提取四个日期与进度（全部容错）-------------------------------------
              safe_date <- function(x) tryCatch({
                d <- suppressWarnings(as.Date(x[1L]))
                if (length(d) == 0L || is.na(d)) "（无）" else format(d, "%Y-%m-%d")
              }, error = function(e) "（无）")
              prev_ps   <- safe_date(if ("raw_planned_start_date" %in% names(chosen)) chosen$raw_planned_start_date else chosen$planned_start_date)
              prev_as   <- safe_date(if ("raw_actual_start_date"  %in% names(chosen)) chosen$raw_actual_start_date  else chosen$actual_start_date)
              prev_pe   <- safe_date(if ("raw_planned_end_date"   %in% names(chosen)) chosen$raw_planned_end_date   else chosen$planned_end_date)
              prev_ae   <- safe_date(chosen$actual_end_date)
              prev_prog <- tryCatch({
                is_done_prev <- !is.na(chosen$actual_end_date[1L])
                p <- suppressWarnings(as.numeric(chosen$progress[1L]))
                if (is_done_prev) "100%" else if (is.na(p)) "（无）" else paste0(round(p * 100, 0L), "%")
              }, error = function(e) "（无）")
            }
          }
        }
      }
    }, error = function(e) NULL)  # 整体兜底：任何异常保持默认"（无）"
    edit_title <- sprintf(
      "%s-%s-%s-修改/更新数据",
      if (!is.na(ctx$project_type) && nzchar(ctx$project_type)) as.character(ctx$project_type) else "未知",
      ctx$project_id,
      if (ctx$is_sync) "各中心同步阶段" else ctx$site_name
    )
    if (isTRUE(ctx$supports_sample)) {
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
            p(tags$b("阶段："), prev_name,
              if (!is.null(prev_site_hint)) tags$span(style = "color:#888; font-size:12px;", paste0("（", prev_site_hint, "）")) else NULL),
            p(tags$b("计划开始："), prev_ps),
            p(tags$b("实际开始："), prev_as),
            p(tags$b("计划结束："), prev_pe),
            p(tags$b("实际结束："), prev_ae),
            p(tags$b("当前进度："), prev_prog)
          ),
          p(tags$span(display_name_for_ctx(ctx), style = "color: #1976D2; font-weight: bold;")),
          textInput("edit_planned_start_date", "计划开始日期：", value = if (length(psd) > 0 && !is.na(psd)) format(psd, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未制定计划"),
          textInput("edit_actual_start_date",  "实际开始日期：", value = if (length(asd) > 0 && !is.na(asd)) format(asd, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，已开始则填写，优先用于位置与色彩计算"),
          textInput("edit_planned_date", "计划结束日期：", value = if (length(pd) > 0 && !is.na(pd)) format(pd, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未制定计划"),
          textInput("edit_actual_date", "实际结束日期：", value = if (length(ad) > 0 && !is.na(ad)) format(ad, "%Y-%m-%d") else "", placeholder = "YYYY-MM-DD，留空表示未完成"),
          tags$p(tags$small("（日期格式：YYYY-MM-DD；实际开始日期已填时优先参与甘特位置与色彩计算）")),
          sliderInput("edit_progress", "调整当前实际进度：", 0, 100, round((if (is.null(ctx$progress)) 0 else ctx$progress) * 100), post = "%")
        ),
        column(6,
          uiOutput("remark_editor"),
          actionButton("btn_add_remark_row", "新增问题/卡点/经验分享", class = "btn-success"),
          uiOutput("sample_pairs_editor")
        )
      )
    ))
  }

  refresh_task_context_from_db <- function(ctx) {
    main_cols <- c(ctx$col_map$planned_start, ctx$col_map$actual_start, ctx$col_map$plan, ctx$col_map$act, ctx$col_map$note, ctx$col_map$progress)
    if (isTRUE(ctx$supports_sample)) {
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
    ctx$remark_entries <- fetch_11_feedback_df(pg_con, ctx$stage_instance_id)
    ctx$snapshot_feedback_map <- fetch_11_feedback_map(pg_con, ctx$stage_instance_id)
    if (isTRUE(ctx$supports_sample)) {
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
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))

    # 权限检查：super_admin 或项目负责人
    if (!isTRUE(auth$is_super_admin)) {
      if (!is_current_user_project_manager(ctx$proj_row_id)) return()
    }

    pt <- ctx$project_type
    req(!is.na(pt) && nzchar(pt))
    stage_maintain_context(list(
      proj_row_id = ctx$proj_row_id,
      site_row_id = ctx$site_row_id,
      project_id = ctx$project_id,
      site_name = ctx$site_name,
      project_type = pt,
      is_sync = ctx$is_sync,
      is_super_admin = auth$is_super_admin,
      can_manage_project = isTRUE(auth$can_manage_project)
    ))

    # 根据权限动态构建 tabsetPanel
    sm_ctx <- stage_maintain_context()
    showModal(modalDialog(
      title = "阶段维护",
      size = "l",
      easyClose = TRUE,
      footer = modalButton("关闭"),
      tabsetPanel(
        id = "stage_maintain_tabs",
        # 仅 super_admin 可见 08 tab
        if (isTRUE(sm_ctx$is_super_admin))
          tabPanel("08 阶段定义", value = "tab1", uiOutput("stage_maintain_tab1_ui")),
        # 所有权限用户都可见 09 tab
        tabPanel("09 阶段实例", value = "tab2", uiOutput("stage_maintain_tab2_ui"))
      )
    ))
  })

  # ---------- 修改项目基础信息：打开弹窗 ----------
  observeEvent(input$btn_edit_personnel_center, {
    auth <- current_user_auth()
    ctx <- task_edit_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    proj_id <- ctx$proj_row_id
    req(!is.na(proj_id))
    if (!isTRUE(auth$can_manage_project) && !is_current_user_project_manager(proj_id)) return()
    personnel_center_context(list(
      proj_row_id = as.integer(proj_id),
      project_id  = ctx$project_id
    ))
    pc_center_refresh(0L)
    showModal(modalDialog(
      title = sprintf("修改项目基础信息 — %s", ctx$project_id),
      size = "l",
      easyClose = TRUE,
      footer = modalButton("关闭"),
      tabsetPanel(
        id = "pc_tabs",
        tabPanel("参与人员管理", value = "tab_personnel", uiOutput("pc_personnel_ui")),
        tabPanel("临床中心管理", value = "tab_center",    uiOutput("pc_center_ui")),
        tabPanel("修改项目名称/课题组/重要紧急程度", value = "tab_proj_name", uiOutput("pc_proj_name_ui")),
        tabPanel("其他", value = "tab_other", uiOutput("pc_other_ui"))
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
        samp <- isTRUE(as.logical(df$supports_sample[i]))
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
      gantt_use_incremental(TRUE)
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
        column(2, tags$div(style = "margin-top: 25px;", checkboxInput("new_stage_active", "模板启用", value = TRUE))),
        column(2, tags$div(style = "margin-top: 25px;", checkboxInput("new_stage_supports_sample", "支持样本追踪", value = FALSE)))
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
        isTRUE(input$new_stage_supports_sample), isTRUE(input$new_stage_active), cfg
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
      gantt_use_incremental(TRUE)
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
      q09 <- 'SELECT stage_def_id, is_active, "阶段实例自定义名称" FROM public."09项目阶段实例表" WHERE project_id = $1 AND scope_row_id = $2'
      inst <- DBI::dbGetQuery(pg_con, q09, params = list(proj_id, scope_row))
      is_active_true <- nrow(inst) > 0L & !is.na(inst$is_active) & (inst$is_active == TRUE | as.character(inst$is_active) == "t")
      active_ids <- if (nrow(inst) > 0L) as.integer(inst$stage_def_id[is_active_true]) else integer(0)
      # 仅「验证」项目，且仅为该项目 04 表负责人（或 super_admin）；不按 can_manage_project 放开
      can_rename <- pt == "验证" && (isTRUE(ctx$is_super_admin) || is_current_user_project_manager(ctx$proj_row_id))
      tagList(
        tags$p(tags$b("项目："), ctx$project_id, " | ", tags$b("中心："), ctx$site_name),
        tags$p(tags$small("勾选表示该维度下该阶段实例参与甘特展示；取消勾选仅置 is_active=FALSE，不删行。")),
        tags$hr(),
        lapply(seq_len(nrow(defs)), function(i) {
          d <- defs[i, ]
          did <- as.integer(d$id)
          checked <- did %in% active_ids
          sid <- paste0("sm09_", did)
          ex <- inst %>% filter(stage_def_id == did)
          custom_nm <- if (nrow(ex) > 0 && !is.na(ex$"阶段实例自定义名称"[1]) && nzchar(as.character(ex$"阶段实例自定义名称"[1])))
            as.character(ex$"阶段实例自定义名称"[1]) else ""
          rename_ui <- NULL
          if (can_rename) {
            rename_ui <- tagList(
              if (nzchar(custom_nm)) tags$span(style = "color:#1565C0;font-weight:600;margin-left:6px;", custom_nm) else NULL,
              actionButton(paste0("sm09_rename_", did), "重命名",
                           class = "btn btn-xs btn-default", style = "margin-left:6px;")
            )
          }
          tags$div(
            style = "margin-bottom: 6px; display:flex; align-items:center;",
            checkboxInput(sid, paste0(d$stage_name, " (", d$stage_key, ")"), value = checked),
            rename_ui
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
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("保存失败：", conditionMessage(e)), type = "error"))
  })

  # ---------- 09阶段实例：重命名 ----------
  observeEvent(input$sm09_rename_trigger, {
    raw <- input$sm09_rename_trigger
    did <- suppressWarnings(as.integer(sub("_.*$", "", as.character(raw))))
    if (is.na(did)) return()
    ctx <- stage_maintain_context(); req(ctx)
    if (ctx$project_type != "验证") return()
    auth <- current_user_auth()
    if (!isTRUE(auth$is_super_admin) && !is_current_user_project_manager(ctx$proj_row_id)) return()
    scope_row <- if (ctx$is_sync) 0L else as.integer(ctx$site_row_id)
    cur <- tryCatch({
      r <- DBI::dbGetQuery(pg_con,
        'SELECT "阶段实例自定义名称" FROM public."09项目阶段实例表" WHERE project_id = $1 AND scope_row_id = $2 AND stage_def_id = $3',
        params = list(as.integer(ctx$proj_row_id), scope_row, did))
      if (nrow(r) > 0 && !is.na(r$"阶段实例自定义名称"[1])) as.character(r$"阶段实例自定义名称"[1]) else ""
    }, error = function(e) "")
    sm09_rename_def_id(did)
    showModal(modalDialog(
      title = "重命名阶段实例",
      size = "s",
      easyClose = TRUE,
      textInput("sm09_rename_input", "自定义名称：", value = cur,
                placeholder = "留空则使用默认阶段名称"),
      tags$p(tags$small("留空保存将恢复为默认阶段名称。")),
      footer = tagList(
        actionButton("btn_sm09_save_rename", "保存", class = "btn-primary"),
        modalButton("取消")
      )
    ))
  }, ignoreInit = TRUE)

  observeEvent(input$btn_sm09_save_rename, {
    did <- sm09_rename_def_id()
    if (is.null(did)) return()
    ctx <- stage_maintain_context(); req(ctx)
    auth <- current_user_auth()
    if (ctx$project_type != "验证") return()
    if (!isTRUE(auth$is_super_admin) && !is_current_user_project_manager(ctx$proj_row_id)) return()
    new_nm <- trimws(as.character(input$sm09_rename_input %||% ""))
    if (!nzchar(new_nm)) new_nm <- NULL
    scope_row <- if (ctx$is_sync) 0L else as.integer(ctx$site_row_id)
    site_id <- if (ctx$is_sync) NULL else as.integer(ctx$site_row_id)
    tryCatch({
      ex <- DBI::dbGetQuery(pg_con,
        'SELECT id FROM public."09项目阶段实例表" WHERE project_id = $1 AND scope_row_id = $2 AND stage_def_id = $3',
        params = list(as.integer(ctx$proj_row_id), scope_row, did))
      if (nrow(ex) > 0) {
        DBI::dbExecute(pg_con,
          'UPDATE public."09项目阶段实例表" SET "阶段实例自定义名称" = $1 WHERE id = $2',
          params = list(new_nm, ex$id[1]))
      } else {
        DBI::dbExecute(pg_con,
          'INSERT INTO public."09项目阶段实例表" (project_id, scope_row_id, site_project_id, stage_def_id, is_active, "阶段实例自定义名称") VALUES ($1, $2, $3, $4, TRUE, $5)',
          params = list(as.integer(ctx$proj_row_id), scope_row, site_id, did, new_nm))
      }
      showNotification("自定义名称已保存。", type = "message")
      removeModal()
      # 刷新 09 tab
      stage_maintain_context(stage_maintain_context())
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("保存失败：", conditionMessage(e)), type = "error"))
  })

  # ==================== 编辑人员、中心 功能 ====================

  # ---------- Tab1: 参与人员管理 ----------
  output$pc_personnel_ui <- renderUI({
    ctx <- personnel_center_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    proj_id <- ctx$proj_row_id
    tryCatch({
      all_staff <- DBI::dbGetQuery(pg_con,
        'SELECT id, "姓名", "岗位" FROM public."05人员表" WHERE "人员状态" = \'在职\' ORDER BY "岗位", "姓名"')
      staff_choices <- setNames(
        as.character(all_staff$id),
        vapply(seq_len(nrow(all_staff)), function(i) {
          pos <- trimws(as.character(if (is.na(all_staff[["岗位"]][i])) "" else all_staff[["岗位"]][i]))
          nm  <- as.character(if (is.na(all_staff[["姓名"]][i])) "" else all_staff[["姓名"]][i])
          if (nzchar(pos)) paste0(pos, "-", nm) else nm
        }, character(1))
      )
      mgr_row <- DBI::dbGetQuery(pg_con,
        'SELECT "05人员表_id" FROM public."04项目总表" WHERE id = $1',
        params = list(as.integer(proj_id)))
      cur_mgr_id <- if (nrow(mgr_row) > 0 && !is.na(mgr_row[["05人员表_id"]][1]))
                      as.character(mgr_row[["05人员表_id"]][1]) else ""
      parts_row <- DBI::dbGetQuery(pg_con,
        'SELECT "05人员表_id" FROM public."_nc_m2m_04项目总表_05人员表" WHERE "04项目总表_id" = $1',
        params = list(as.integer(proj_id)))
      cur_part_ids <- if (nrow(parts_row) > 0) as.character(parts_row[["05人员表_id"]]) else character(0)
      tagList(
        tags$p(style = "color:#666; font-size:13px; margin-bottom:12px;",
               "修改后点击「保存人员设置」使更改生效。只能在在职人员中选择。"),
        fluidRow(
          column(6,
            selectInput("pc_manager_id", "项目负责人",
              choices = c("（无）" = "", staff_choices),
              selected = cur_mgr_id, width = "100%")
          ),
          column(6,
            selectizeInput("pc_participant_ids", "参与人员（可多选）",
              choices = staff_choices,
              selected = cur_part_ids,
              multiple = TRUE,
              options = list(placeholder = "请选择参与人员..."),
              width = "100%")
          )
        ),
        actionButton("btn_save_personnel", "保存人员设置", class = "btn-primary")
      )
    }, error = function(e) tags$p(style = "color:red;", paste0("加载失败：", conditionMessage(e))))
  })

  observeEvent(input$btn_save_personnel, {
    auth <- current_user_auth()
    ctx <- personnel_center_context()
    if (!isTRUE(auth$can_manage_project) && !is_current_user_project_manager(ctx$proj_row_id)) return()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    proj_id <- as.integer(ctx$proj_row_id)
    new_mgr_str  <- input$pc_manager_id
    new_part_ids <- input$pc_participant_ids
    tryCatch({
      new_mgr_int <- if (!is.null(new_mgr_str) && nzchar(trimws(new_mgr_str)))
                       suppressWarnings(as.integer(new_mgr_str)) else NA_integer_
      old_mgr_row <- DBI::dbGetQuery(pg_con,
        'SELECT "05人员表_id" FROM public."04项目总表" WHERE id = $1',
        params = list(proj_id))
      old_mgr_id <- if (nrow(old_mgr_row) > 0) old_mgr_row[["05人员表_id"]][1] else NA_integer_
      if (is.na(new_mgr_int)) {
        DBI::dbExecute(pg_con,
          'UPDATE public."04项目总表" SET "05人员表_id" = NULL WHERE id = $1',
          params = list(proj_id))
      } else {
        DBI::dbExecute(pg_con,
          'UPDATE public."04项目总表" SET "05人员表_id" = $1 WHERE id = $2',
          params = list(new_mgr_int, proj_id))
      }
      old_parts <- DBI::dbGetQuery(pg_con,
        'SELECT "05人员表_id" FROM public."_nc_m2m_04项目总表_05人员表" WHERE "04项目总表_id" = $1',
        params = list(proj_id))
      old_part_ids <- if (nrow(old_parts) > 0) as.integer(old_parts[["05人员表_id"]]) else integer(0)
      new_part_ints <- if (!is.null(new_part_ids) && length(new_part_ids) > 0)
        suppressWarnings(as.integer(new_part_ids[nzchar(new_part_ids)])) else integer(0)
      to_add    <- setdiff(new_part_ints, old_part_ids)
      to_remove <- setdiff(old_part_ids, new_part_ints)
      for (rid in to_remove) {
        DBI::dbExecute(pg_con,
          'DELETE FROM public."_nc_m2m_04项目总表_05人员表" WHERE "04项目总表_id" = $1 AND "05人员表_id" = $2',
          params = list(proj_id, as.integer(rid)))
      }
      for (aid in to_add) {
        DBI::dbExecute(pg_con,
          'INSERT INTO public."_nc_m2m_04项目总表_05人员表" ("04项目总表_id", "05人员表_id") VALUES ($1, $2) ON CONFLICT DO NOTHING',
          params = list(proj_id, as.integer(aid)))
      }
      insert_audit_log(
        pg_con, auth$work_id, auth$name,
        "UPDATE", "04项目总表+_nc_m2m_04项目总表_05人员表", proj_id,
        sprintf("修改项目 %s 的人员配置", ctx$project_id),
        sprintf("负责人: %s -> %s; 参与人员 +%d -%d", old_mgr_id, new_mgr_int, length(to_add), length(to_remove)),
        list(manager_id = old_mgr_id, participants = old_part_ids),
        list(manager_id = new_mgr_int, participants = new_part_ints)
      )
      showNotification("人员设置已保存！", type = "message")
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("保存失败：", conditionMessage(e)), type = "error"))
  })

  # ---------- Tab2: 临床中心管理 ----------
  output$pc_center_ui <- renderUI({
    pc_center_refresh()
    ctx <- personnel_center_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    proj_id <- ctx$proj_row_id
    tryCatch({
      linked <- DBI::dbGetQuery(pg_con,
        paste0('SELECT t03.id AS link_id, h."医院名称"',
               ' FROM public."03医院_项目表" t03',
               ' LEFT JOIN public."01医院信息表" h ON h.id = t03."01_hos_resource_table医院信息表_id"',
               ' WHERE t03."project_table 项目总表_id" = $1',
               ' ORDER BY h."医院名称"'),
        params = list(as.integer(proj_id)))
      avail <- DBI::dbGetQuery(pg_con,
        paste0('SELECT h.id, h."医院名称"',
               ' FROM public."01医院信息表" h',
               ' WHERE h.id NOT IN (',
               '   SELECT t03."01_hos_resource_table医院信息表_id"',
               '   FROM public."03医院_项目表" t03',
               '   WHERE t03."project_table 项目总表_id" = $1',
               '   AND t03."01_hos_resource_table医院信息表_id" IS NOT NULL',
               ' )',
               ' ORDER BY h."医院名称"'),
        params = list(as.integer(proj_id)))
      # setNames 的 names 不能含 NA（医院名称在库中可能为 NULL），否则报错 NAs are not allowed in subscripted assignments
      avail_choices <- if (nrow(avail) > 0) {
        ids <- as.character(avail$id)
        raw_nm <- as.character(avail[["医院名称"]])
        lbl <- vapply(seq_along(raw_nm), function(i) {
          x <- raw_nm[i]
          if (is.na(x) || !nzchar(trimws(x))) {
            paste0("（医院名称未填 id=", avail$id[i], "）")
          } else {
            x
          }
        }, character(1))
        if (any(duplicated(lbl))) {
          lbl <- make.unique(lbl, sep = " ")
        }
        setNames(ids, lbl)
      } else {
        character(0)
      }
      tagList(
        tags$p(tags$b("当前已关联临床中心："), style = "margin-bottom:6px;"),
        if (nrow(linked) == 0) {
          tags$p("（暂无关联中心）", style = "color:#999;")
        } else {
          tags$ul(style = "list-style: none; padding-left: 0;",
            lapply(seq_len(nrow(linked)), function(i) {
              nm <- linked[["医院名称"]][i]
              display_nm <- if (is.na(nm) || !nzchar(trimws(as.character(nm))))
                paste0("（医院名称未填，link_id=", linked$link_id[i], "）") else as.character(nm)
              tags$li(style = "margin-bottom: 4px;",
                tags$span(display_nm),
                tags$a(href = "#", class = "pc-del-center-btn",
                  `data-id` = as.character(linked$link_id[i]),
                  style = "margin-left:8px; color:#dc3545; font-size:0.85em; text-decoration:none;",
                  "删除")
              )
            })
          )
        },
        tags$hr(),
        tags$p(tags$b("新增临床中心："),
               tags$small("（触发器将自动维护 09 阶段实例）"),
               style = "margin-bottom:6px;"),
        if (length(avail_choices) == 0) {
          tags$p("（所有医院已关联本项目，无可新增中心）", style = "color:#999;")
        } else {
          tagList(
            selectInput("pc_new_center_id", "选择要新增的医院",
              choices = c("请选择..." = "", avail_choices),
              width = "100%"),
            actionButton("btn_add_center", "新增中心", class = "btn-success")
          )
        }
      )
    }, error = function(e) tags$p(style = "color:red;", paste0("加载失败：", conditionMessage(e))))
  })

  observeEvent(input$btn_add_center, {
    auth <- current_user_auth()
    ctx <- personnel_center_context()
    if (!isTRUE(auth$can_manage_project) && !is_current_user_project_manager(ctx$proj_row_id)) return()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    hosp_id_str <- input$pc_new_center_id
    if (is.null(hosp_id_str) || !nzchar(trimws(hosp_id_str))) {
      showNotification("请先选择要新增的医院。", type = "warning")
      return()
    }
    hosp_id <- suppressWarnings(as.integer(hosp_id_str))
    if (is.na(hosp_id)) {
      showNotification("医院选择无效。", type = "error")
      return()
    }
    proj_id <- as.integer(ctx$proj_row_id)
    tryCatch({
      existing <- DBI::dbGetQuery(pg_con,
        'SELECT id FROM public."03医院_项目表" WHERE "project_table 项目总表_id" = $1 AND "01_hos_resource_table医院信息表_id" = $2',
        params = list(proj_id, hosp_id))
      if (nrow(existing) > 0) {
        showNotification("该医院已与本项目关联。", type = "warning")
        return()
      }
      work_id_str <- if (!is.null(auth$work_id) && nzchar(auth$work_id)) auth$work_id else ""
      DBI::dbExecute(pg_con,
        'INSERT INTO public."03医院_项目表" ("project_table 项目总表_id", "01_hos_resource_table医院信息表_id", created_at1, created_by1) VALUES ($1, $2, NOW(), $3)',
        params = list(proj_id, hosp_id, work_id_str))
      hosp_name_row <- DBI::dbGetQuery(pg_con,
        'SELECT "医院名称" FROM public."01医院信息表" WHERE id = $1',
        params = list(hosp_id))
      hosp_name <- if (nrow(hosp_name_row) > 0) as.character(hosp_name_row[["医院名称"]][1]) else as.character(hosp_id)
      insert_audit_log(
        pg_con, auth$work_id, auth$name,
        "INSERT", "03医院_项目表", proj_id,
        sprintf("项目 %s 新增临床中心 %s", ctx$project_id, hosp_name),
        sprintf("新增医院 id=%d (%s)", hosp_id, hosp_name),
        NULL,
        list(proj_id = proj_id, hosp_id = hosp_id, hosp_name = hosp_name)
      )
      showNotification(sprintf("已成功新增中心：%s（触发器将自动创建相关阶段实例）", hosp_name), type = "message")
      pc_center_refresh(pc_center_refresh() + 1L)
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("新增中心失败：", conditionMessage(e)), type = "error"))
  })

  # ---------- Tab2: 删除中心（事件委托 → 确认弹窗） ----------
  observeEvent(input$pc_del_center_trigger, {
    raw <- input$pc_del_center_trigger
    link_id <- suppressWarnings(as.integer(sub("_.*$", "", as.character(raw))))
    if (is.na(link_id)) return()
    ctx <- personnel_center_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    tryCatch({
      center_row <- DBI::dbGetQuery(pg_con,
        'SELECT t03."01_hos_resource_table医院信息表_id", h."医院名称" FROM public."03医院_项目表" t03 LEFT JOIN public."01医院信息表" h ON h.id = t03."01_hos_resource_table医院信息表_id" WHERE t03.id = $1',
        params = list(link_id))
      if (nrow(center_row) == 0) {
        showNotification("未找到该中心关联记录。", type = "warning")
        return()
      }
      center_name <- if (!is.na(center_row[["医院名称"]][1]) && nzchar(trimws(center_row[["医院名称"]][1])))
        center_row[["医院名称"]][1] else paste0("id=", link_id)
      session$userData$pending_del_center <- list(
        link_id = link_id,
        center_name = center_name
      )
      showModal(modalDialog(
        title = "确认删除临床中心",
        size = "m", easyClose = TRUE,
        tags$div(
          tags$p(sprintf("是否删除临床中心「%s」？", center_name)),
          tags$p("删除后将移除该中心的所有阶段实例，但该中心的问题/卡点/项目要点将保留（不再关联具体阶段实例）。", style = "color:#666;")
        ),
        footer = tagList(
          actionButton("btn_confirm_del_center", "确认删除", class = "btn-danger"),
          modalButton("取消")
        )
      ))
    }, error = function(e) showNotification(paste0("查询失败：", conditionMessage(e)), type = "error"))
  })

  observeEvent(input$btn_confirm_del_center, {
    pending <- session$userData$pending_del_center
    if (is.null(pending)) { removeModal(); return() }
    auth <- current_user_auth()
    ctx <- personnel_center_context()
    if (!isTRUE(auth$can_manage_project) && !is_current_user_project_manager(ctx$proj_row_id)) { removeModal(); return() }
    req(!is.null(pg_con), DBI::dbIsValid(pg_con))
    link_id <- pending$link_id
    center_name <- pending$center_name
    session$userData$pending_del_center <- NULL
    removeModal()
    tryCatch({
      pool::poolWithTransaction(pg_con, function(conn) {
        # 1. 断开 11→09 关联（保留 11 行，置 NULL）
        DBI::dbExecute(conn,
          'UPDATE public."11阶段问题反馈表" SET "09项目阶段实例表_id" = NULL WHERE "09项目阶段实例表_id" IN (SELECT id FROM public."09项目阶段实例表" WHERE site_project_id = $1)',
          params = list(link_id))
        # 2. 删除 03 行 → CASCADE 自动删除关联 09 行（site_project_id FK）
        DBI::dbExecute(conn,
          'DELETE FROM public."03医院_项目表" WHERE id = $1',
          params = list(link_id))
      })
      insert_audit_log(
        pg_con, auth$work_id, auth$name,
        "DELETE", "03医院_项目表", link_id,
        sprintf("项目 %s 删除临床中心 %s", ctx$project_id, center_name),
        sprintf("删除中心 link_id=%d (%s)", link_id, center_name),
        NULL, NULL
      )
      showNotification(sprintf("已删除临床中心：%s", center_name), type = "message")
      pc_center_refresh(pc_center_refresh() + 1L)
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("删除中心失败：", conditionMessage(e)), type = "error"))
  })

  # ---------- Tab3: 修改项目名称 / 课题组 / 重要紧急程度 ----------
  output$pc_proj_name_ui <- renderUI({
    ctx <- personnel_center_context()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    auth <- current_user_auth()
    and_04 <- if (isTRUE(auth$allow_all)) "" else paste0(" AND id IN (", auth$allowed_subquery, ")")
    tryCatch({
      cur_row <- DBI::dbGetQuery(pg_con,
        'SELECT "项目名称", "课题组", "重要紧急程度" FROM public."04项目总表" WHERE id = $1',
        params = list(as.integer(ctx$proj_row_id)))
      cur_name <- if (nrow(cur_row) > 0 && !is.na(cur_row[["项目名称"]][1]))
                    as.character(cur_row[["项目名称"]][1]) else ""
      cur_rg <- if (nrow(cur_row) > 0 && "课题组" %in% names(cur_row) && !is.na(cur_row[["课题组"]][1]))
                    trimws(as.character(cur_row[["课题组"]][1])) else "无"
      cur_imp <- if (nrow(cur_row) > 0 && !is.na(cur_row[["重要紧急程度"]][1]))
                    trimws(as.character(cur_row[["重要紧急程度"]][1])) else ""
      imp_choices <- c("重要紧急", "重要不紧急", "紧急不重要", "不重要不紧急")
      if (!nzchar(cur_imp) || !(cur_imp %in% imp_choices)) cur_imp <- "重要紧急"

      rg_dist <- DBI::dbGetQuery(pg_con,
        paste0('SELECT DISTINCT "课题组" AS v FROM public."04项目总表" WHERE "课题组" IS NOT NULL', and_04, " ORDER BY 1"))
      rg_choices <- if (nrow(rg_dist) > 0) as.character(rg_dist$v) else character(0)
      if (length(rg_choices) == 0L) rg_choices <- "无"
      if (!(cur_rg %in% rg_choices)) rg_choices <- unique(c(rg_choices, cur_rg))

      tagList(
        tags$p(style = "color:#666; font-size:13px; margin-bottom:12px;",
               "修改后点击「保存」使更改生效。"),
        fluidRow(
          column(12,
            textInput("pc_proj_name_input", "项目名称",
                      value = cur_name, width = "100%",
                      placeholder = "请输入项目名称")
          ),
          column(12,
            selectInput("pc_proj_research_group", "课题组",
                        choices = rg_choices, selected = cur_rg, width = "100%")
          ),
          column(12,
            selectInput("pc_proj_importance", "重要紧急程度",
                        choices = imp_choices, selected = cur_imp, width = "100%")
          )
        ),
        actionButton("btn_save_proj_name", "保存", class = "btn-primary")
      )
    }, error = function(e) tags$p(style = "color:red;", paste0("加载失败：", conditionMessage(e))))
  })

  observeEvent(input$btn_save_proj_name, {
    auth <- current_user_auth()
    ctx <- personnel_center_context()
    if (!isTRUE(auth$can_manage_project) && !is_current_user_project_manager(ctx$proj_row_id)) return()
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    proj_id <- as.integer(ctx$proj_row_id)
    new_name <- trimws(input$pc_proj_name_input %||% "")
    new_rg <- trimws(as.character(input$pc_proj_research_group %||% ""))
    new_imp <- trimws(as.character(input$pc_proj_importance %||% ""))
    imp_choices <- c("重要紧急", "重要不紧急", "紧急不重要", "不重要不紧急")
    if (!nzchar(new_name)) {
      showNotification("项目名称不能为空。", type = "warning")
      return()
    }
    if (!(new_imp %in% imp_choices)) new_imp <- "重要紧急"
    if (!nzchar(new_rg)) new_rg <- "无"

    tryCatch({
      dup <- DBI::dbGetQuery(pg_con,
        'SELECT id FROM public."04项目总表" WHERE "项目名称" = $1 AND id != $2',
        params = list(new_name, proj_id))
      if (nrow(dup) > 0) {
        showNotification("已存在同名项目，请使用不同的项目名称。", type = "warning")
        return()
      }

      result <- pool::poolWithTransaction(pg_con, function(conn) {
        locked <- fetch_row_snapshot(conn, "04项目总表", proj_id,
          c("项目名称", "课题组", "重要紧急程度"), lock = TRUE)
        if (is.null(locked)) stop("项目不存在。")
        updates <- list()
        if (!identical(normalize_text(new_name), normalize_text(locked[["项目名称"]]))) updates[["项目名称"]] <- new_name
        if (!identical(normalize_text(new_rg), normalize_text(locked[["课题组"]]))) updates[["课题组"]] <- new_rg
        if (!identical(normalize_text(new_imp), normalize_text(locked[["重要紧急程度"]]))) updates[["重要紧急程度"]] <- new_imp
        if (length(updates) == 0L) return(list(skip = TRUE))

        execute_updates(conn, "04项目总表", proj_id, updates)
        old_audit <- list(
          项目名称 = locked[["项目名称"]],
          课题组 = locked[["课题组"]],
          重要紧急程度 = locked[["重要紧急程度"]]
        )
        new_audit <- old_audit
        for (nm in names(updates)) new_audit[[nm]] <- updates[[nm]]
        list(skip = FALSE, old_audit = old_audit, new_audit = new_audit, updates = updates, new_name = new_name)
      })

      if (isTRUE(result$skip)) {
        showNotification("未检测到变更。", type = "message")
        return()
      }

      parts <- character(0)
      if ("项目名称" %in% names(result$updates)) {
        parts <- c(parts, sprintf(
          "项目名称 %s→%s",
          ifelse(is.na(result$old_audit$项目名称), "（空）", result$old_audit$项目名称),
          ifelse(is.na(result$new_audit$项目名称), "（空）", result$new_audit$项目名称)))
      }
      if ("课题组" %in% names(result$updates)) {
        parts <- c(parts, sprintf(
          "课题组 %s→%s",
          ifelse(is.na(result$old_audit$课题组), "（空）", result$old_audit$课题组),
          ifelse(is.na(result$new_audit$课题组), "（空）", result$new_audit$课题组)))
      }
      if ("重要紧急程度" %in% names(result$updates)) {
        parts <- c(parts, sprintf(
          "重要紧急程度 %s→%s",
          ifelse(is.na(result$old_audit$重要紧急程度), "（空）", result$old_audit$重要紧急程度),
          ifelse(is.na(result$new_audit$重要紧急程度), "（空）", result$new_audit$重要紧急程度)))
      }
      summary_txt <- paste(parts, collapse = "；")

      insert_audit_log(
        pg_con, auth$work_id, auth$name,
        "UPDATE", "04项目总表", proj_id,
        "项目基本信息", summary_txt,
        result$old_audit, result$new_audit
      )

      personnel_center_context(list(
        proj_row_id = proj_id,
        project_id  = result$new_name
      ))
      showNotification("项目信息已保存。", type = "message")
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("保存失败：", conditionMessage(e)), type = "error"))
  })

  # ---------- Tab4: 其他（删除项目） ----------
  output$pc_other_ui <- renderUI({
    ctx <- personnel_center_context()
    req(ctx)
    tagList(
      tags$div(style = "padding: 15px; border: 1px solid #dee2e6; border-radius: 5px;",
        tags$h4("危险操作", style = "color: #dc3545;"),
        tags$p("以下操作不可撤销，请谨慎执行。"),
        tags$hr(),
        actionButton("btn_delete_project", "删除项目",
                     class = "btn-danger",
                     style = "background-color: #dc3545; color: white;"),
        tags$span(" 删除后该项目所有信息将无法恢复", style = "color: #999;")
      )
    )
  })

  observeEvent(input$btn_delete_project, {
    ctx <- personnel_center_context()
    req(ctx)
    auth <- current_user_auth()
    if (!isTRUE(auth$can_manage_project) && !is_current_user_project_manager(ctx$proj_row_id)) return()
    showModal(modalDialog(
      title = "确认删除项目",
      size = "m", easyClose = FALSE,
      tags$div(
        tags$p(strong("是否删除项目？该操作会删除关于该项目的所有信息，包括项目要点、会议记录、个人贡献等所有内容。")),
        tags$p("此操作不可撤销。", style = "color: #dc3545;")
      ),
      footer = tagList(
        actionButton("btn_delete_project_confirm", "确定", class = "btn-danger"),
        modalButton("取消")
      )
    ))
  })

  observeEvent(input$btn_delete_project_confirm, {
    auth <- current_user_auth()
    ctx <- personnel_center_context()
    if (!isTRUE(auth$can_manage_project) && !is_current_user_project_manager(ctx$proj_row_id)) { removeModal(); return() }
    req(ctx, !is.null(pg_con), DBI::dbIsValid(pg_con))
    proj_id <- as.integer(ctx$proj_row_id)
    proj_name <- ctx$project_id
    removeModal()
    tryCatch({
      pool::poolWithTransaction(pg_con, function(conn) {
        # 1. 删除人员关联（_nc_m2m）
        DBI::dbExecute(conn,
          'DELETE FROM public."_nc_m2m_04项目总表_05人员表" WHERE "04项目总表_id" = $1',
          params = list(proj_id))
        # 2. 处理项目级 11 行（09项目阶段实例_id IS NULL，不被 CASCADE 覆盖）
        DBI::dbExecute(conn,
          'DELETE FROM public."12会议决策关联问题表" WHERE "11阶段问题反馈表_id" IN (SELECT id FROM public."11阶段问题反馈表" WHERE "04项目总表_id" = $1 AND "09项目阶段实例表_id" IS NULL)',
          params = list(proj_id))
        DBI::dbExecute(conn,
          'DELETE FROM public."11阶段问题反馈表" WHERE "04项目总表_id" = $1 AND "09项目阶段实例表_id" IS NULL',
          params = list(proj_id))
        # 3. 删除 03 行 → CASCADE: 09(site_project_id) → 11 → 12
        DBI::dbExecute(conn,
          'DELETE FROM public."03医院_项目表" WHERE "project_table 项目总表_id" = $1',
          params = list(proj_id))
        # 4. 删除 04 行 → CASCADE: 09(project_id) → 11 → 12
        DBI::dbExecute(conn,
          'DELETE FROM public."04项目总表" WHERE id = $1',
          params = list(proj_id))
        # 5. 清理孤立的 10 行（不再有任何 12 关联的会议决策）
        DBI::dbExecute(conn,
          'DELETE FROM public."10会议决策表" WHERE id NOT IN (SELECT DISTINCT "10会议决策表_id" FROM public."12会议决策关联问题表")')
      })
      insert_audit_log(
        pg_con, auth$work_id, auth$name,
        "DELETE", "04项目总表", proj_id,
        sprintf("删除项目 %s 及其所有关联数据", proj_name),
        sprintf("级联删除项目 id=%d (%s)，含 03/09/11/12 关联行及孤立10行", proj_id, proj_name),
        NULL, NULL
      )
      showNotification(sprintf("项目「%s」已删除。", proj_name), type = "message")
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
    }, error = function(e) showNotification(paste0("删除项目失败：", conditionMessage(e)), type = "error"))
  })

  # ==================== 编辑人员、中心 功能 end ====================

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
        typ_show <- trimws(type_val)
        bracket_lbl <- if (nzchar(typ_show)) paste0("【", typ_show, i, "】") else paste0("【要点", i, "】")
        tags$div(
          style = "margin-bottom: 12px;",
          tags$div(style = "font-weight: 700; color: #37474f; margin-bottom: 6px; font-size: 13px;", bracket_lbl),
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
        )
      })
    } else {
      list(tags$p("当前暂无备注条目。", style = "color:#777;"))
    }
    tagList(
      tags$hr(),
      tags$b("项目要点"),
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
    if (!"fb_id" %in% names(remark_df)) remark_df$fb_id <- rep(NA_integer_, nrow(remark_df))
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
          remark_df[i, ] <- list("", current_reporter, "", type_val, content, NA_integer_)
        }
      }
    }
    remark_df[nrow(remark_df) + 1L, ] <- list("", current_reporter, "", "问题", "", NA_integer_)
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
        person <- role <- work <- note_val <- ""
        amount <- 1
        if (!is.null(existing) && nrow(existing) >= i) {
          person <- as.character(existing$person[i])
          role <- as.character(existing$role[i])
          work <- as.character(existing$work[i])
          amount <- suppressWarnings(as.numeric(existing$amount[i]))
          if (is.na(amount) || amount < 1) amount <- 1
          if ("note" %in% names(existing)) {
            note_val <- as.character(existing$note[i] %||% "")
            if (is.na(note_val)) note_val <- ""
          }
        }
        role_choices_i <- c("主导", "参与", "协助")
        if (nzchar(role) && !(role %in% role_choices_i)) role_choices_i <- c(role_choices_i, role)
        work_choices_i <- work_choices
        if (nzchar(work) && !(work %in% work_choices_i)) work_choices_i <- c(work_choices_i, work)
        fluidRow(
          column(
            2,
            selectInput(
              paste0("contrib_person_", i),
              if (i == 1L) "人员" else NULL,
              choices = c("（请选择）" = "", setNames(person_choices, person_choices)),
              selected = if (nzchar(person)) person else ""
            )
          ),
          column(
            2,
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
            2,
            numericInput(
              paste0("contrib_amount_", i),
              if (i == 1L) "数量" else NULL,
              value = amount,
              min = 1,
              width = "100%"
            )
          ),
          column(
            3,
            textInput(
              paste0("contrib_note_", i),
              if (i == 1L) "备注（≤10字）" else NULL,
              value = note_val,
              placeholder = "选填，限10字"
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
        tags$b("阶段："), display_name_for_ctx(ctx)
      ),
      tags$hr(),
      tags$b("进度贡献者"),
      tags$p(tags$small("每行：人员（从人员表选择） + 参与度（主导/参与/协助，可自定义） + 工作内容（阶段相关，可自定义） + 数量（>=1 的数字） + 备注（选填，限10字）。")),
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
        note_i   <- tryCatch(trimws(as.character(input[[paste0("contrib_note_", i)]])), error = function(e) "")
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
          contrib_df$note[i] <- note_i
        } else {
          contrib_df[i, ] <- list(entry_key_i, person_i, role_i, work_i, as.character(amt_num), note_i)
        }
      }
    }
    contrib_df[nrow(contrib_df) + 1L, ] <- list("", "", "", "", "1", "")
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
        n_val <- tryCatch(trimws(as.character(input[[paste0("contrib_note_", i)]])), error = function(e) "")
        if (is.na(n_val)) n_val <- ""
        if (nchar(n_val, type = "chars") > 10) {
          showNotification(sprintf("第 %d 行备注超过 10 个字，请缩减。", i), type = "error")
          return()
        }
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
          contrib_df$note[i] <- n_val
        } else {
          contrib_df[i, ] <- list(entry_key_i, p, r, w, as.character(amt_num), n_val)
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
      gantt_use_incremental(TRUE)
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
             " / ", display_name_for_ctx(ctx)),
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
    latest_row <- fetch_row_snapshot(pg_con, ctx$milestone_table, ctx$milestone_row_id, "milestones_json", lock = FALSE)
    if (is.null(latest_row)) return(NULL)
    ctx$milestone_raw <- latest_row[["milestones_json"]]
    ctx$milestones <- parse_milestone_json_to_df(latest_row[["milestones_json"]])
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
        if (i <= nrow(ms_df)) {
          ms_df$entry_key[i] <- entry_key_i
          ms_df$name[i] <- nm
          ms_df$plan[i] <- pl
          ms_df$actual[i] <- ac
          ms_df$note[i] <- nt
        } else {
          ms_df[i, ] <- list(entry_key_i, nm, pl, ac, nt)
        }
      }
    }
    # 新增一空行
    ms_df[nrow(ms_df) + 1, ] <- list("", "", "", "", "")
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
          ms_df$name[i] <- nm
          ms_df$plan[i] <- pl
          ms_df$actual[i] <- ac
          ms_df$note[i] <- nt
        } else {
          ms_df[i, ] <- list(entry_key_i, nm, pl, ac, nt)
        }
      }
    }
    if (n_rows < nrow(ms_df)) ms_df <- ms_df[seq_len(n_rows), , drop = FALSE]

    # 里程碑现在是项目/子中心维度，用户编辑的即全量；直接用用户结果替换整个 JSON
    snapshot_ms_map <- parse_named_json_map(ctx$milestone_raw)
    user_ms_map <- build_milestone_map_from_df(ms_df, actor = actor_name)

    tbl <- ctx$milestone_table
    row_id <- ctx$milestone_row_id
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
      task_edit_context(ctx)
      milestone_row_count(nrow(ctx$milestones))
      gantt_use_incremental(TRUE)
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

  observeEvent(input$btn_open_milestone_from_task, {
    ctx <- task_edit_context()
    req(ctx)
    msdf <- ctx$milestones
    if (is.null(msdf)) msdf <- empty_milestone_df()
    milestone_row_count(as.integer(nrow(msdf)))
    open_milestone_edit_modal(ctx)
  })

  # S09“验证试验开展与数据管理”样本来源与数量编辑区
  output$sample_pairs_editor <- renderUI({
    ctx <- task_edit_context()
    if (is.null(ctx) || !isTRUE(ctx$supports_sample)) {
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
    if (!isTRUE(ctx$supports_sample)) return()
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
    # validate_date_input 已迁至 R/ivd_server_helpers.R
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
    auth <- current_user_auth()
    actor_name <- normalize_text(auth$name)
    if (is.na(actor_name)) actor_name <- normalize_text(auth$work_id)
    if (is.na(actor_name)) actor_name <- "未知用户"

    # 备注 JSON：逐条读取并生成用户版本
    remark_df <- ctx$remark_entries
    if (is.null(remark_df)) remark_df <- empty_remark_df()
    if (!"fb_id" %in% names(remark_df)) remark_df$fb_id <- rep(NA_integer_, nrow(remark_df))
    n_remark_rows <- remark_row_count()
    if (is.null(n_remark_rows) || n_remark_rows < 0L) n_remark_rows <- 0L
    if (n_remark_rows > 0L) {
      for (i in seq_len(n_remark_rows)) {
        type_val <- tryCatch(trimws(as.character(input[[paste0("remark_type_", i)]])), error = function(e) "")
        content <- tryCatch(trimws(as.character(input[[paste0("remark_content_", i)]])), error = function(e) "")
        entry_key_i <- if (nrow(remark_df) >= i) as.character(remark_df$entry_key[i]) else ""
        updated_at_i <- if (nrow(remark_df) >= i) as.character(remark_df$updated_at[i] %||% "") else ""
        fb_id_i <- if (nrow(remark_df) >= i) suppressWarnings(as.integer(remark_df$fb_id[i])) else NA_integer_
        if (is.na(fb_id_i)) fb_id_i <- NA_integer_
        if (i <= nrow(remark_df)) {
          remark_df$entry_key[i] <- entry_key_i
          remark_df$reporter[i] <- actor_name
          remark_df$updated_at[i] <- updated_at_i
          remark_df$type[i] <- type_val
          remark_df$content[i] <- content
          remark_df$fb_id[i] <- fb_id_i
        } else {
          remark_df[i, ] <- list(entry_key_i, actor_name, updated_at_i, type_val, content, fb_id_i)
        }
      }
    }
    if (n_remark_rows < nrow(remark_df)) remark_df <- remark_df[seq_len(n_remark_rows), , drop = FALSE]
    user_remark_map <- build_remark_map_from_df(remark_df, actor = actor_name)

    # 针对 S09“验证试验开展与数据管理”的样本来源与数量特殊处理
    is_s09_task <- isTRUE(ctx$supports_sample)
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
    snapshot_note_map <- sort_feedback_map(ctx$snapshot_feedback_map %||% list())
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
          locked_fb_before <- fetch_11_feedback_map(conn, row_id)
          note_merge <- merge_remark_field(
            snapshot_note_map,
            locked_fb_before,
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

          all_conflicts <- c(main_scalar$conflicts, note_merge$conflicts, sample_merge$conflicts)
          if (!isTRUE(overwrite_conflicts) && !isTRUE(allow_partial_save) && length(all_conflicts) > 0) {
            return(list(status = "conflict", conflicts = all_conflicts))
          }

          updates_main <- main_scalar$changed_updates
          proj_for_11 <- suppressWarnings(as.integer(ctx$proj_row_id))
          if (is.na(proj_for_11)) {
            prr <- DBI::dbGetQuery(conn, 'SELECT project_id FROM public."09项目阶段实例表" WHERE id = $1', list(as.integer(row_id)))
            if (!is.null(prr) && nrow(prr) > 0) proj_for_11 <- as.integer(prr$project_id[1])
          }
          if (!is.na(proj_for_11)) {
            apply_merged_feedback_to_11(conn, row_id, proj_for_11, note_merge$merged, auth$work_id, auth$name)
          }
          updates_main[[ctx$note_col]] <- "{}"
          if (is_s09_task && !identical(normalize_text(sample_merge$merged_json, empty_as_na = FALSE), normalize_text(locked_main_row[[sample_col]], empty_as_na = FALSE))) {
            updates_main[[sample_col]] <- sample_merge$merged_json
          }
          if (length(updates_main) > 0) {
            execute_updates(conn, tbl, row_id, updates_main)
          }

          new_main_row <- locked_main_row
          for (nm in names(updates_main)) new_main_row[[nm]] <- updates_main[[nm]]

          old_audit <- list(
            计划开始时间 = locked_main_row[[cm$planned_start]],
            实际开始时间 = locked_main_row[[cm$actual_start]],
            计划完成时间 = locked_main_row[[cm$plan]],
            实际完成时间 = locked_main_row[[cm$act]],
            备注 = locked_fb_before,
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
        biz_desc = "阶段时间与进度", summary = "修改阶段时间、进度、备注",
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
      ctx$remark <- ctx$snapshot_row[[ctx$note_col]]
      ctx$remark_entries <- fetch_11_feedback_df(pg_con, row_id)
      ctx$snapshot_feedback_map <- fetch_11_feedback_map(pg_con, row_id)
      if (is_s09_task) ctx$samples <- parse_sample_df(ctx$snapshot_row[[sample_col]])
      task_edit_context(ctx)
      remark_row_count(nrow(ctx$remark_entries))
      if (is_s09_task) sample_row_count(nrow(ctx$samples))
      gantt_use_incremental(TRUE)
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
      fetch_11_feedback_map(pg_con, row_id),
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
    pre_conflicts <- c(pre_main_scalar$conflicts, pre_note_merge$conflicts, pre_sample_merge$conflicts)
    stale_any <- state_changed(snapshot_main_row, current_main_row) ||
      !identical(sort_feedback_map(ctx$snapshot_feedback_map %||% list()), fetch_11_feedback_map(pg_con, row_id)) ||
      (is_s09_task && state_changed(snapshot_sample_map, parse_sample_map(current_main_row[[sample_col]])))
    auto_merge_items <- character(0)
    if (stale_any) {
      auto_merge_items <- c(
        pre_main_scalar$changed_labels,
        format_json_auto_merge_items("问题/卡点/经验分享", pre_note_merge$changed_keys),
        if (is_s09_task) format_json_auto_merge_items("样本来源与数", pre_sample_merge$changed_keys) else character(0)
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

  # ==================== 会议决策 renderUI ====================
  # 与甘特图相同：左侧可收起侧栏 + 右侧主区；侧栏用 conditionalPanel 按子 Tab 切换（控件仍在 DOM，切换不丢状态）。
  # 历史：筛选与刷新 + 日期 + meeting_filter_controls（不随 meeting_data 重建）；新建：与甘特同款维度筛选「选择项目」。

  output$meeting_tab_ui <- renderUI({
    # 首屏默认「项目甘特图」：勿在首轮 flush 构建会议 Tab（含 meeting_filter_options / meeting_data 等），切到本 Tab 再渲染
    req(identical(input$main_tabs, "tab_meeting"))
    if (is.null(.ivd_sess_once$mtg_tab)) {
      ivd_perf_elapsed(t_ivd_sess0, "sess|output$meeting_tab_ui renderUI first")
      .ivd_sess_once$mtg_tab <- TRUE
    }
    ed <- today_beijing()
    st <- seq(ed, by = "-1 month", length.out = 4L)[4L]
    tagList(
      fluidRow(
        column(
          12,
          tags$div(
            class = "gantt-row-flex",
            tags$div(
              id = "meeting_sidebar_col",
              class = "gantt-sidebar-wrap",
              tags$button(
                type = "button",
                id = "meeting_sidebar_toggle",
                class = "btn btn-default btn-sm",
                title = "收起筛选",
                style = "width: 100%; margin-bottom: 10px; white-space: nowrap;",
                "◀ 收起筛选"
              ),
              tags$div(
                class = "gantt-sidebar-body",
                conditionalPanel(
                  condition = "typeof input.meeting_sub_tabs === 'undefined' || !input.meeting_sub_tabs || input.meeting_sub_tabs == 'mtg_history'",
                  tags$div(
                    class = "panel panel-default",
                    style = "margin-bottom: 8px;",
                    tags$div(class = "panel-heading", style = "padding: 8px 12px; font-size: 14px;", tags$b("筛选与刷新")),
                    tags$div(
                      class = "panel-body",
                      style = "padding: 10px 12px;",
                      tags$div(
                        style = "display: flex; flex-direction: row; align-items: center; flex-wrap: wrap; gap: 10px; margin-bottom: 12px;",
                        actionButton("mtg_refresh", "🔄 从数据库刷新", class = "btn btn-primary", style = "margin: 0;"),
                        tags$button(
                          type = "button",
                          id = "mtg_filter_combine_btn",
                          class = "btn btn-default",
                          style = "font-size: 13px; padding: 5px 14px; line-height: 1.35; font-weight: 700; flex-shrink: 0; margin: 0;",
                          `data-mode` = "and",
                          "且条件"
                        )
                      ),
                      dateInput("mtg_filter_date_start", "开始日期", value = st, width = "100%"),
                      dateInput("mtg_filter_date_end", "结束日期", value = ed, width = "100%"),
                      uiOutput("meeting_filter_controls")
                    )
                  )
                ),
                conditionalPanel(
                  condition = "input.meeting_sub_tabs == 'mtg_new'",
                  tags$div(
                    class = "panel panel-default",
                    style = "margin-bottom: 8px;",
                    tags$div(class = "panel-heading", style = "padding: 8px 12px; font-size: 14px;", tags$b("筛选与刷新")),
                    tags$div(
                      class = "panel-body",
                      style = "padding: 10px 12px;",
                      tags$div(
                        style = "display: flex; flex-direction: row; align-items: center; flex-wrap: wrap; gap: 10px; margin-bottom: 12px;",
                        actionButton("mtg_new_dim_refresh", "🔄 从数据库刷新", class = "btn btn-primary", style = "margin: 0;"),
                        tags$button(
                          type = "button",
                          id = "mtg_new_filter_combine_btn",
                          class = "btn btn-default",
                          style = "font-size: 13px; padding: 5px 14px; line-height: 1.35; font-weight: 700; flex-shrink: 0; margin: 0;",
                          `data-mode` = "and",
                          "且条件"
                        )
                      ),
                      selectInput("mtg_new_filter_type", "项目类型", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("mtg_new_filter_name", "项目名称", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("mtg_new_filter_manager", "项目负责人", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("mtg_new_filter_participant", "项目参与人员", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("mtg_new_filter_importance", "重要紧急程度", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("mtg_new_filter_research_group", "课题组", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      selectInput("mtg_new_filter_hospital", "相关医院（有中心）", choices = character(0), multiple = TRUE, selectize = TRUE, width = "100%"),
                      div(
                        style = "margin-top: 6px; font-size: 14px;",
                        checkboxInput("mtg_new_filter_include_archived", "包含已结题项目", value = FALSE)
                      )
                    )
                  )
                )
              )
            ),
            tags$div(
              class = "gantt-main-wrap",
              tabsetPanel(
                id = "meeting_sub_tabs",
                type = "tabs",
                tabPanel("历史会议", value = "mtg_history", uiOutput("meeting_history_list")),
                tabPanel("新建会议", value = "mtg_new", uiOutput("meeting_new_ui"))
              )
            )
          )
        )
      )
    )
  })

  output$meeting_filter_controls <- renderUI({
    req(identical(input$main_tabs, "tab_meeting"))
    opts <- meeting_filter_options()
    if (is.null(opts)) {
      return(tags$p(style = "color: #888; font-size: 13px; margin-top: 8px;", "筛选选项加载中…"))
    }
    tagList(
      selectInput("mtg_filter_name", "会议名称", choices = opts$meeting_names, multiple = TRUE, selectize = TRUE, width = "100%"),
      selectInput("mtg_filter_project", "决策项目", choices = opts$projects, multiple = TRUE, selectize = TRUE, width = "100%"),
      selectInput("mtg_filter_executor", "执行人", choices = opts$executors, multiple = TRUE, selectize = TRUE, width = "100%")
    )
  })

  # ==================== 个人消息 Tab ====================

  personal_msg_data <- reactive({
    msg_force_refresh()
    auth <- current_user_auth()
    days_back <- 14L
    if (!is.null(input$msg_days_back)) {
      db <- suppressWarnings(as.integer(input$msg_days_back))
      if (!is.na(db) && db > 0L) days_back <- db
    }
    fetch_personal_messages(pg_con, auth, days_back)
  })

  # 布局壳：侧边栏（共用刷新+回溯天数）+ 子 tab 内容区
  output$personal_msg_ui <- renderUI({
    req(identical(input$main_tabs, "tab_messages"))
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return(tags$div(class = "alert alert-warning", "请先登录"))

    tagList(
      tags$div(
        class = "gantt-row-flex",
        tags$div(
          id = "msg_sidebar_col",
          class = "gantt-sidebar-wrap",
          tags$button(
            type = "button", id = "msg_sidebar_toggle",
            class = "btn btn-default btn-sm",
            title = "收起筛选",
            style = "width: 100%; margin-bottom: 10px; white-space: nowrap;",
            "◀ 收起筛选"
          ),
          tags$div(
            class = "gantt-sidebar-body",
            tags$div(
              class = "panel panel-default",
              style = "margin-bottom: 8px;",
              tags$div(class = "panel-heading", style = "padding: 8px 12px; font-size: 14px;", tags$b("筛选与刷新")),
              tags$div(
                class = "panel-body",
                style = "padding: 10px 12px;",
                actionButton("msg_refresh", "刷新", class = "btn btn-primary", style = "width: 100%; margin-bottom: 10px;"),
                selectInput("msg_days_back", "回溯天数",
                  choices = c("7天" = "7", "14天" = "14", "30天" = "30", "90天" = "90"),
                  selected = "14", width = "100%")
              )
            ),
            conditionalPanel(
              condition = "input.msg_sub_tabs == 'msg_progress'",
              tags$div(
                class = "panel panel-default",
                style = "margin-bottom: 8px;",
                tags$div(class = "panel-heading", style = "padding: 8px 12px; font-size: 14px;", tags$b("进度消息类型")),
                tags$div(
                  class = "panel-body",
                  style = "padding: 10px 12px;",
                  checkboxGroupInput("msg_cats_progress", "显示类型",
                    choices = c("阶段落后或逾期" = "阶段落后或逾期", "未启动" = "未启动", "久未更新" = "久未更新"),
                    selected = c("阶段落后或逾期", "未启动", "久未更新"),
                    width = "100%")
                )
              )
            ),
            conditionalPanel(
              condition = "input.msg_sub_tabs == 'msg_issues'",
              tags$div(
                class = "panel panel-default",
                style = "margin-bottom: 8px;",
                tags$div(class = "panel-heading", style = "padding: 8px 12px; font-size: 14px;", tags$b("问题消息类型")),
                tags$div(
                  class = "panel-body",
                  style = "padding: 10px 12px;",
                  checkboxGroupInput("msg_cats_issues", "显示类型",
                    choices = c("近期卡点" = "近期卡点", "近期问题" = "近期问题", "待执行决策" = "待执行决策"),
                    selected = c("近期卡点", "近期问题", "待执行决策"),
                    width = "100%")
                )
              )
            )
          )
        ),
        tags$div(
          class = "gantt-main-wrap",
          tabsetPanel(
            id = "msg_sub_tabs",
            type = "tabs",
            tabPanel("项目进度消息", value = "msg_progress", uiOutput("msg_progress_content")),
            tabPanel("卡点、问题和决策", value = "msg_issues", uiOutput("msg_issues_content"))
          )
        )
      )
    )
  })

  # 子 tab 1：项目进度消息
  output$msg_progress_content <- renderUI({
    msgs <- personal_msg_data()
    progress_cats <- c("阶段落后或逾期", "未启动", "久未更新")
    sel <- input$msg_cats_progress
    if (!is.null(sel) && length(sel) > 0L) progress_cats <- sel else if (!is.null(sel) && length(sel) == 0L) progress_cats <- character(0)
    msgs <- msgs[msgs$category %in% progress_cats, , drop = FALSE]
    if (nrow(msgs) == 0L) {
      return(tags$div(style = "text-align: center; padding: 40px 20px; color: #888;",
        tags$h4("暂无项目进度消息"), tags$p("所有阶段进度正常。")))
    }
    .build_msg_panels(msgs, panel_mode = "progress")
  })

  # 子 tab 2：卡点、问题和决策
  output$msg_issues_content <- renderUI({
    msgs <- personal_msg_data()
    issue_cats <- c("近期卡点", "近期问题", "待执行决策")
    sel <- input$msg_cats_issues
    if (!is.null(sel) && length(sel) > 0L) issue_cats <- sel else if (!is.null(sel) && length(sel) == 0L) issue_cats <- character(0)
    msgs <- msgs[msgs$category %in% issue_cats, , drop = FALSE]
    if (nrow(msgs) == 0L) {
      return(tags$div(style = "text-align: center; padding: 40px 20px; color: #888;",
        tags$h4("暂无卡点、问题或待执行决策"), tags$p("一切正常。")))
    }
    fb_ids <- unique(suppressWarnings(as.integer(msgs$feedback_id)))
    fb_ids <- fb_ids[!is.na(fb_ids) & fb_ids > 0L]
    dec_by_fid <- fetch_decisions_linked_to_feedback_ids(pg_con, fb_ids)
    .build_msg_panels(msgs, panel_mode = "issues", dec_by_fid = dec_by_fid)
  })

  # 通用消息渲染函数：按优先级分组 + 可折叠 + 下钻交互
  .build_msg_panels <- function(msgs, panel_mode = c("progress", "issues"), dec_by_fid = NULL) {
    panel_mode <- match.arg(panel_mode)
    auth <- current_user_auth()
    seen_decisions <- integer(0)
    pri_order <- c("critical", "high", "medium", "low")
    sections <- lapply(pri_order, function(pk) {
      sub <- msgs[msgs$priority == pk, , drop = FALSE]
      if (nrow(sub) == 0L) return(NULL)
      info <- msg_priority_info(pk)
      cards <- lapply(seq_len(nrow(sub)), function(i) {
        has_proj_id <- !is.na(sub$project_db_id[i]) && nzchar(as.character(sub$project_db_id[i]))
        has_stage_id <- !is.na(sub$stage_instance_id[i]) && nzchar(as.character(sub$stage_instance_id[i]))

        # 项目名称：有 project_db_id 时渲染为可点击链接
        proj_el <- if (has_proj_id) {
          tags$a(
            class = "msg-project-link", href = "javascript:void(0)",
            style = "font-weight: 600; color: #1565C0; cursor: pointer;",
            `data-project-db-id` = as.character(sub$project_db_id[i]),
            sub$project_name[i]
          )
        } else {
          tags$span(style = "font-weight: 600;", sub$project_name[i])
        }

        # 阶段名称：有 stage_instance_id 时渲染为可点击链接
        stage_el <- if (!nzchar(sub$stage_name[i])) {
          NULL
        } else if (has_stage_id) {
          tags$a(
            class = "msg-stage-link", href = "javascript:void(0)",
            style = "font-size: 12px; color: #1565C0; cursor: pointer;",
            `data-stage-instance-id` = as.character(sub$stage_instance_id[i]),
            sub$stage_name[i]
          )
        } else {
          tags$span(style = "font-size: 12px; color: #666;", sub$stage_name[i])
        }

        body_el <- if (identical(panel_mode, "issues")) {
          fid <- suppressWarnings(as.integer(sub$feedback_id[i]))
          ftyp <- trimws(as.character(sub$feedback_type[i] %||% ""))
          fcontent <- as.character(sub$feedback_content[i] %||% "")
          freporter <- as.character(sub$feedback_reporters[i] %||% "")
          fupdated <- as.character(sub$feedback_updated_at[i] %||% "")
          dec_df <- if (!is.null(dec_by_fid) && !is.na(fid) && !is.null(dec_by_fid[[as.character(fid)]])) {
            dec_by_fid[[as.character(fid)]]
          } else {
            data.frame(
              decision_id = integer(0),
              meeting_label = character(0),
              decision_content = character(0),
              exec_json = character(0),
              meeting_time = as.POSIXct(character(0)),
              stringsAsFactors = FALSE
            )
          }
          dec_nodes <- if (nrow(dec_df) > 0L) {
            lapply(seq_len(nrow(dec_df)), function(di) {
              did <- suppressWarnings(as.integer(dec_df$decision_id[di]))
              allow_link <- !is.na(did) && !(did %in% seen_decisions)
              if (allow_link) seen_decisions <<- c(seen_decisions, did)
              tags$div(
                style = "margin-top: 8px; margin-left: 4px; padding-left: 10px; border-left: 2px solid #e0e0e0;",
                tags$div(
                  style = "font-size: 13px; color: #37474f; margin-bottom: 2px;",
                  tags$span(style = "font-weight: 600;", sprintf("决策%d：", di)),
                  tags$span(as.character(dec_df$decision_content[di] %||% ""))
                ),
                if (nzchar(as.character(dec_df$meeting_label[di] %||% ""))) {
                  tags$div(style = "font-size: 12px; color: #78909c; margin-bottom: 4px;", as.character(dec_df$meeting_label[di]))
                } else NULL,
                gantt_executor_block_tags(
                  auth,
                  did,
                  as.character(dec_df$exec_json[di] %||% "{}"),
                  allow_exec_link = allow_link
                )
              )
            })
          } else {
            list(tags$div(style = "font-size: 12px; color: #999; margin-top: 6px;", "暂无关联会议决策"))
          }
          tagList(
            tags$div(
              style = "margin-bottom: 6px; line-height: 1.5;",
              tags$span(
                style = "font-weight: 600;",
                paste0(if (nzchar(ftyp)) ftyp else "要点", if (isTRUE(sub$is_decision_driven[i])) "（决策关注）" else "")
              ),
              tags$span(": "),
              tags$span(if (nzchar(fcontent)) fcontent else "（无内容）")
            ),
            tags$div(
              style = "font-size: 12px; color: #607d8b; margin-bottom: 4px;",
              paste0(
                if (nzchar(freporter)) paste0("反馈人：", freporter) else "反馈人：-",
                "；更新日期：", if (nzchar(fupdated)) fupdated else "-"
              )
            ),
            dec_nodes
          )
        } else {
          sub$message_text[i]
        }

        tags$div(
          class = "msg-panel",
          tags$div(
            class = "msg-panel-heading",
            tags$div(
              tags$span(class = paste0("msg-badge msg-badge-", pk), info$label),
              tags$span(class = "msg-category-tag", sub$category[i]),
              proj_el
            ),
            stage_el
          ),
          tags$div(class = "msg-panel-body", body_el)
        )
      })
      tagList(
        tags$div(
          class = "msg-section-header",
          style = paste0("color: ", info$color, "; border-bottom-color: ", info$color, ";"),
          sprintf("%s优先级 (%d条)", info$label, nrow(sub)),
          tags$span(class = "msg-section-arrow", " ▼")
        ),
        tags$div(class = "msg-section-body", cards)
      )
    })
    tagList(sections)
  }

  observeEvent(input$msg_refresh, {
    msg_force_refresh(msg_force_refresh() + 1L)
  })

  # ---------- 个人消息：阶段名称点击 → 任务详情弹窗 ----------
  observeEvent(input$msg_stage_clicked, {
    raw <- input$msg_stage_clicked
    sid_str <- sub("_.*$", "", as.character(raw))
    sid <- suppressWarnings(as.integer(sid_str))
    if (is.na(sid)) return
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return

    today <- today_beijing()

    # 从 09+08+04+03+01+05 联表查询阶段完整数据
    original_task <- tryCatch(
      DBI::dbGetQuery(pg_con, paste0(
        "SELECT si.id AS stage_instance_id, si.project_id,",
        " si.planned_start_date, si.actual_start_date, si.planned_end_date, si.actual_end_date,",
        " si.progress, si.remark_json::text AS remark_json,",
        " si.contributors_json::text AS contributors_json, si.sample_json::text AS sample_json,",
        ' si.\"阶段实例自定义名称\", d.stage_key AS task_name, d.stage_name, d.stage_scope,',
        " COALESCE(si.\"阶段实例自定义名称\", d.stage_name) AS task_display_name,",
        " d.stage_order AS stage_ord,",
        " g.id AS proj_row_id, g.\"项目类型\" AS project_type, g.\"重要紧急程度\",",
        " g.\"项目名称\" AS project_id,",
        " s.id AS site_row_id,",
        " COALESCE(NULLIF(h.\"医院名称\", ''), '中心-' || COALESCE(s.id,0)::text) AS site_name,",
        ' si.is_active',
        " FROM public.\"09项目阶段实例表\" si",
        " JOIN public.\"08项目阶段定义表\" d ON d.id = si.stage_def_id",
        " JOIN public.\"04项目总表\" g ON g.id = si.project_id",
        " LEFT JOIN public.\"03医院_项目表\" s ON s.id = si.site_project_id",
        " LEFT JOIN public.\"01医院信息表\" h ON h.id = s.\"01_hos_resource_table医院信息表_id\"",
        " WHERE si.id = $1"
      ), params = list(sid)),
      error = function(e) NULL
    )
    if (is.null(original_task) || nrow(original_task) == 0) return

    ot <- original_task[1, ]
    is_sync <- identical(as.character(ot$stage_scope), "sync")
    stage_instance_id <- sid
    proj_row_id <- suppressWarnings(as.integer(ot$proj_row_id))
    site_row_id <- suppressWarnings(as.integer(ot$site_row_id))
    tn <- as.character(ot$task_name[1])

    raw_planned_start <- as.Date(ot$planned_start_date)
    raw_actual_start  <- as.Date(ot$actual_start_date)
    raw_planned       <- as.Date(ot$planned_end_date)
    actual_end_date   <- as.Date(ot$actual_end_date)
    progress_val <- suppressWarnings(as.numeric(ot$progress))
    if (is.na(progress_val)) progress_val <- 0

    is_unplanned <- is.na(raw_planned_start) || is.na(raw_planned)
    is_completed <- !is.na(actual_end_date)
    reported_progress <- progress_val / 100
    actual_progress <- ifelse(is_completed, 1.0, reported_progress)

    # 计划进度
    planned_duration <- as.numeric(raw_planned - raw_planned_start)
    eff_start <- if (!is.na(raw_actual_start)) raw_actual_start else raw_planned_start
    plan_no_actual_start <- !is_unplanned && is.na(raw_actual_start) &&
      !is.na(raw_planned_start) && !is.na(raw_planned) && is.na(actual_end_date)
    planned_p <- if (isTRUE(plan_no_actual_start)) {
      0
    } else {
      ifelse(is_completed,
             ifelse(!is.na(planned_duration) && planned_duration > 0,
                    as.numeric(actual_end_date - eff_start) / planned_duration, 1.0),
             ifelse(!is.na(planned_duration) && planned_duration > 0,
                    as.numeric(today - eff_start) / planned_duration,
                    ifelse(!is.na(eff_start) && today >= eff_start, 1.0, 0.0)))
    }
    diff_p <- if (isTRUE(plan_no_actual_start)) 0 else (actual_progress - planned_p)
    delay_days <- if (!is.na(actual_end_date)) as.numeric(actual_end_date - raw_planned) else NA_real_

    # 诊断
    is_not_started_detail <- !isTRUE(plan_no_actual_start) &&
      !is.na(eff_start) && isTRUE(today < eff_start) &&
      isTRUE(actual_progress == 0) && is.na(actual_end_date)
    missing_actual_done <- is.na(actual_end_date) && reported_progress >= 1.0

    diagnostic_text <- if (isTRUE(plan_no_actual_start)) {
      span("⏳ 尚未开始：已制定计划起止，但未填写实际开始日期；理论计划进度按 0% 计，直至填写实际开始。", style = "color: #757575; font-weight: bold;")
    } else if (is_not_started_detail) {
      span("⏳ 尚未开始：未到计划启动时间。", style = "color: #757575; font-weight: bold;")
    } else if (isTRUE(missing_actual_done)) {
      span("ℹ️ 已报完成，但未填写实际完成日期，请补充\u201c实际完成时间\u201d。", style = "color: #1976D2; font-weight: bold;")
    } else if (is.na(diff_p)) {
      span("⏳ 尚未开始：计划日期未设置。", style = "color: #757575; font-weight: bold;")
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

    # 重要紧急程度
    importance_level <- as.character(ot[["重要紧急程度"]])
    importance_display <- if (is.na(importance_level) || !nzchar(trimws(importance_level))) "（未设置）" else trimws(importance_level)
    importance_color <- switch(trimws(importance_level), "重要紧急" = "#C62828", "重要不紧急" = "#F57C00", "紧急不重要" = "#1976D2", "不重要不紧急" = "#616161", "#757575")

    # 负责人 + 参与人员
    manager_display <- "（无）"
    participants_display <- character(0)
    tryCatch({
      mgr <- DBI::dbGetQuery(pg_con,
        'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."04项目总表" proj ON proj."05人员表_id" = p.id WHERE proj.id = $1',
        params = list(proj_row_id))
      if (nrow(mgr) > 0) {
        pos <- if (is.na(mgr[["岗位"]][1]) || !nzchar(trimws(as.character(mgr[["岗位"]][1])))) "" else as.character(mgr[["岗位"]][1])
        nm <- if (is.na(mgr[["姓名"]][1])) "" else as.character(mgr[["姓名"]][1])
        manager_display <- if (nzchar(pos)) paste0(pos, "-", nm) else (if (nzchar(nm)) nm else "（无）")
      }
      parts <- DBI::dbGetQuery(pg_con,
        'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."05人员表_id" = p.id WHERE m."04项目总表_id" = $1',
        params = list(proj_row_id))
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
    }, error = function(e) { })

    # 获取要点、贡献者、样本、里程碑
    snapshot_cols <- c("planned_start_date", "actual_start_date", "planned_end_date", "actual_end_date", "remark_json", "progress", "contributors_json", "sample_json", "row_version")
    snapshot_row <- tryCatch(fetch_row_snapshot(pg_con, "09项目阶段实例表", stage_instance_id, snapshot_cols, lock = FALSE), error = function(e) NULL)
    remark_raw <- if (!is.null(snapshot_row)) snapshot_row[["remark_json"]] else NA_character_
    remarks_df <- fetch_11_feedback_df(pg_con, stage_instance_id)
    snapshot_feedback_map <- fetch_11_feedback_map(pg_con, stage_instance_id)
    pt_for_lookup <- as.character(ot$project_type)
    is_s09_task <- stage_supports_sample(tn, pt_for_lookup) && is_sync
    sample_pairs <- if (is_s09_task && !is.null(snapshot_row)) parse_sample_df(snapshot_row[["sample_json"]]) else empty_sample_df()
    contrib_df <- if (!is.null(snapshot_row)) parse_contrib_json_to_df(snapshot_row[["contributors_json"]]) else empty_contrib_df()

    ms_tbl_fetch <- if (is_sync) "04项目总表" else "03医院_项目表"
    ms_id_fetch  <- if (is_sync) proj_row_id else as.integer(site_row_id)
    milestone_raw <- tryCatch({
      r <- fetch_row_snapshot(pg_con, ms_tbl_fetch, ms_id_fetch, "milestones_json", lock = FALSE)
      if (!is.null(r)) r[["milestones_json"]] else NA_character_
    }, error = function(e) NA_character_)
    milestones_df <- parse_milestone_json_to_df(milestone_raw)

    can_edit <- !is.na(stage_instance_id)

    if (can_edit) {
      if (is_s09_task) sample_row_count(if (is.null(sample_pairs)) 0L else nrow(sample_pairs)) else sample_row_count(0L)
      contrib_row_count(nrow(contrib_df))
      remark_row_count(nrow(remarks_df))
      task_edit_context(list(
        project_id = as.character(ot$project_id),
        site_name = as.character(ot$site_name),
        task_name = tn,
        task_display_name = as.character(ot$task_display_name),
        is_sync = is_sync,
        supports_sample = is_s09_task,
        project_type = pt_for_lookup,
        importance = importance_level,
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
        col_map = list(planned_start = "planned_start_date", actual_start = "actual_start_date", plan = "planned_end_date", act = "actual_end_date", note = "remark_json", progress = "progress"),
        table_name = "09项目阶段实例表",
        milestone_table = ms_tbl_fetch,
        milestone_row_id = ms_id_fetch,
        samples = sample_pairs,
        contributors = contrib_df,
        milestones = milestones_df,
        milestone_raw = milestone_raw,
        milestone_stage_key = tn,
        note_col = "remark_json",
        contrib_col = "contributors_json",
        snapshot_row = snapshot_row,
        snapshot_project_row = if (!is.na(proj_row_id)) tryCatch(fetch_row_snapshot(pg_con, "04项目总表", proj_row_id, "重要紧急程度", lock = FALSE), error = function(e) NULL) else NULL,
        snapshot_feedback_map = snapshot_feedback_map
      ))
    }

    task_info_row <- data.frame(
      project_id = as.character(ot$project_id),
      site_name = as.character(ot$site_name),
      task_name = tn,
      task_display_name = as.character(ot$task_display_name),
      stringsAsFactors = FALSE
    )

    .render_task_detail_modal(
      task_info_row = task_info_row,
      proj_row_id = proj_row_id,
      stage_instance_id = stage_instance_id,
      raw_planned_start = raw_planned_start,
      raw_actual_start = raw_actual_start,
      raw_planned_end = raw_planned,
      actual_end_date = actual_end_date,
      is_unplanned = is_unplanned,
      is_s09_task = is_s09_task,
      actual_progress = actual_progress,
      planned_p = planned_p,
      diff_p = diff_p,
      delay_days = delay_days,
      importance_display = importance_display,
      importance_color = importance_color,
      manager_display = manager_display,
      participants_display = participants_display,
      diagnostic_text = diagnostic_text,
      can_edit = can_edit
    )
  }, ignoreInit = TRUE)

  # ---------- 个人消息：项目名称点击 → 单项目甘特图弹窗 ----------
  output$msg_proj_gantt <- renderTimevis({
    pid <- msg_gantt_project_id()
    if (is.null(pid)) return(timevis(data.frame(), options = list()))
    sc <- stage_catalog()
    data <- build_single_project_gantt_data(pg_con, pid, sc$sync_stages, stage_label_for_key)
    if (is.null(data) || nrow(data$items) == 0) return(timevis(data.frame(), options = list()))
    timevis(data$items, data$groups,
      options = list(
        stack = TRUE,
        verticalScroll = TRUE,
        zoomMin = 7 * 24 * 60 * 60 * 1000,
        orientation = "top",
        margin = list(item = list(horizontal = 5)),
        locale = "zh",
        format = list(minorLabels = list(day = "MM/DD"), majorLabels = list(day = "YYYY/MM"))
      )
    )
  })

  observeEvent(input$msg_project_clicked, {
    raw <- input$msg_project_clicked
    pid_str <- sub("_.*$", "", as.character(raw))
    pid <- suppressWarnings(as.integer(pid_str))
    if (is.na(pid)) return
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return
    if (!auth$allow_all && nzchar(auth$allowed_subquery)) {
      chk <- tryCatch(
        DBI::dbGetQuery(pg_con, paste0(
          'SELECT 1 AS ok FROM public."04项目总表" WHERE id = $1 AND id IN (', auth$allowed_subquery, ')'
        ), params = list(pid)),
        error = function(e) data.frame(ok = integer(0))
      )
      if (nrow(chk) == 0L) return
    }

    # 获取项目名称
    proj_name <- tryCatch({
      r <- DBI::dbGetQuery(pg_con, 'SELECT "项目名称" FROM public."04项目总表" WHERE id = $1', params = list(pid))
      if (nrow(r) > 0) as.character(r[["项目名称"]][1]) else ""
    }, error = function(e) "")

    msg_gantt_project_id(pid)

    legend_html <- "<div style='margin-bottom:10px;font-size:12px;color:#555;'>
      <b>进度诊断图例：</b>
      <span style='margin-left:15px;padding:2px 8px;background:#EDEDED;border:1px solid #D0D0D0;border-radius:3px;color:#555;'>未制定计划</span>
      <span style='margin-left:15px;padding:2px 8px;background:#9E9E9E;border:2px solid #9E9E9E;border-radius:3px;color:white;'>未开始</span>
      <span style='margin-left:15px;padding:2px 8px;background:#F44336;border:2px solid #F44336;border-radius:3px;color:white;'>落后50%以上</span>
      <span style='margin-left:10px;padding:2px 8px;background:#FF6F00;border:2px solid #FF6F00;border-radius:3px;color:white;'>落后30%-50%</span>
      <span style='margin-left:10px;padding:2px 8px;background:#FFC107;border:2px solid #FFC107;border-radius:3px;color:white;'>落后15%-30%</span>
      <span style='margin-left:10px;padding:2px 8px;background:#FFEB3B;border:2px solid #FFEB3B;border-radius:3px;color:#333;'>落后5%-15%</span>
      <span style='margin-left:10px;padding:2px 8px;background:#CDDC39;border:2px solid #CDDC39;border-radius:3px;color:#333;'>偏差5%以内</span>
      <span style='margin-left:10px;padding:2px 8px;background:#8BC34A;border:2px solid #8BC34A;border-radius:3px;color:#333;'>超前10%-25%</span>
      <span style='margin-left:10px;padding:2px 8px;background:#4CAF50;border:2px solid #4CAF50;border-radius:3px;color:white;'>超前25%以上</span>
    </div>"

    showModal(modalDialog(
      title = paste0("项目甘特图", if (nzchar(proj_name)) paste0(" — ", proj_name) else ""),
      size = "l",
      easyClose = TRUE,
      footer = modalButton("关闭"),
      HTML(legend_html),
      timevisOutput("msg_proj_gantt", height = "400px")
    ))
  }, ignoreInit = TRUE)

  # 弹窗甘特图内点击进度条 → 任务详情
  observeEvent(input$msg_proj_gantt_selected, {
    req(input$msg_proj_gantt_selected)
    on.exit(session$sendCustomMessage("resetGanttSelection", "msg_proj_gantt"), add = TRUE)
    task_id <- as.numeric(input$msg_proj_gantt_selected)
    pid <- msg_gantt_project_id()
    if (is.null(pid) || is.na(task_id)) return
    sc <- stage_catalog()
    data <- build_single_project_gantt_data(pg_con, pid, sc$sync_stages, stage_label_for_key)
    if (is.null(data)) return
    task_info <- data$items[data$items$id == task_id, ]
    if (nrow(task_info) == 0) return

    # 从 group 中提取 project_id 和 site_name
    project_id_from_group <- sub("_.*$", "", task_info$group)
    site_name_from_group <- sub("^[^_]+_", "", task_info$group)
    is_sync <- grepl("同步阶段", task_info$group)

    # 获取原始甘特数据行（用 all_stages 避免主 tab 筛选器干扰）
    gd <- gantt_data_all_stages()
    if (is.null(gd) || nrow(gd) == 0) gd <- current_gantt_data()
    ss <- sc$sync_stages
    if (is_sync) {
      original_task <- gd %>%
        filter(project_id == project_id_from_group, task_name %in% ss) %>%
        filter(task_name == task_info$task_name | grepl(sub("\\s+\\d+%$", "", as.character(task_info$content)), task_name, fixed = TRUE)) %>%
        slice(1)
      task_info$site_name <- "所有中心（同步）"
      task_info$project_id <- project_id_from_group
    } else {
      original_task <- gd %>%
        filter(project_id == project_id_from_group, site_name == site_name_from_group, !task_name %in% ss) %>%
        filter(task_name == task_info$task_name | grepl(sub("\\s+\\d+%$", "", as.character(task_info$content)), task_name, fixed = TRUE)) %>%
        slice(1)
    }
    if (nrow(original_task) == 0) return

    today <- today_beijing()
    planned_end_date <- original_task$planned_end_date[1]
    actual_end_date  <- original_task$actual_end_date[1]
    is_unplanned <- if ("is_unplanned" %in% names(original_task)) isTRUE(original_task$is_unplanned[1]) else FALSE
    start_for_calc <- as.Date(task_info$start)
    proj_row_id <- if ("proj_row_id" %in% names(original_task)) original_task$proj_row_id[1] else NA_integer_
    site_row_id <- if ("site_row_id" %in% names(original_task)) original_task$site_row_id[1] else NA_integer_
    progress_match <- regmatches(task_info$content, regexpr("\\d+%", task_info$content))
    reported_progress <- if (length(progress_match) > 0) as.numeric(sub("%", "", progress_match)) / 100 else original_task$progress
    is_completed <- !is.na(actual_end_date)
    actual_progress <- ifelse(is_completed, 1.0, reported_progress)
    raw_planned_start <- if ("raw_planned_start_date" %in% names(original_task)) original_task$raw_planned_start_date[1] else (if (is_unplanned) as.Date(NA) else start_for_calc)
    raw_actual_start  <- if ("raw_actual_start_date"  %in% names(original_task)) original_task$raw_actual_start_date[1]  else as.Date(NA)
    raw_planned       <- if ("raw_planned_end_date" %in% names(original_task)) original_task$raw_planned_end_date[1] else (if (is_unplanned) as.Date(NA) else planned_end_date)
    tn <- as.character(task_info$task_name[1])
    stage_instance_id <- if ("stage_instance_id" %in% names(original_task)) original_task$stage_instance_id[1] else NA_integer_

    planned_duration <- as.numeric(raw_planned - raw_planned_start)
    eff_start_detail <- if (!is.na(raw_actual_start)) raw_actual_start else raw_planned_start
    plan_no_actual_start <- !is_unplanned && is.na(raw_actual_start) && !is.na(raw_planned_start) && !is.na(raw_planned) && is.na(actual_end_date)

    planned_p <- if (isTRUE(plan_no_actual_start)) 0 else ifelse(is_completed, ifelse(!is.na(planned_duration) && planned_duration > 0, as.numeric(actual_end_date - eff_start_detail) / planned_duration, 1.0), ifelse(!is.na(planned_duration) && planned_duration > 0, as.numeric(today - eff_start_detail) / planned_duration, ifelse(!is.na(eff_start_detail) && today >= eff_start_detail, 1.0, 0.0)))
    diff_p <- if (isTRUE(plan_no_actual_start)) 0 else (actual_progress - planned_p)
    delay_days <- if (!is.na(actual_end_date)) as.numeric(actual_end_date - raw_planned) else NA_real_

    is_not_started_detail <- !isTRUE(plan_no_actual_start) && !is.na(eff_start_detail) && isTRUE(today < eff_start_detail) && isTRUE(actual_progress == 0) && is.na(actual_end_date)
    missing_actual_done <- is.na(actual_end_date) && reported_progress >= 1.0

    diagnostic_text <- if (isTRUE(plan_no_actual_start)) {
      span("⏳ 尚未开始：已制定计划起止，但未填写实际开始日期；理论计划进度按 0% 计，直至填写实际开始。", style = "color: #757575; font-weight: bold;")
    } else if (is_not_started_detail) {
      span("⏳ 尚未开始：未到计划启动时间。", style = "color: #757575; font-weight: bold;")
    } else if (isTRUE(missing_actual_done)) {
      span("ℹ️ 已报完成，但未填写实际完成日期，请补充\u201c实际完成时间\u201d。", style = "color: #1976D2; font-weight: bold;")
    } else if (is.na(diff_p)) {
      span("⏳ 尚未开始：计划日期未设置。", style = "color: #757575; font-weight: bold;")
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

    # 获取负责人 + 参与人员
    manager_display <- "（无）"; participants_display <- character(0)
    tryCatch({
      proj_db <- suppressWarnings(as.integer(proj_row_id))
      if (!is.na(proj_db)) {
        mgr <- DBI::dbGetQuery(pg_con, 'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."04项目总表" proj ON proj."05人员表_id" = p.id WHERE proj.id = $1', params = list(proj_db))
        if (nrow(mgr) > 0) { pos <- if (is.na(mgr[["岗位"]][1]) || !nzchar(trimws(as.character(mgr[["岗位"]][1])))) "" else as.character(mgr[["岗位"]][1]); nm <- if (is.na(mgr[["姓名"]][1])) "" else as.character(mgr[["姓名"]][1]); manager_display <- if (nzchar(pos)) paste0(pos, "-", nm) else (if (nzchar(nm)) nm else "（无）") }
        parts <- DBI::dbGetQuery(pg_con, 'SELECT p."岗位", p."姓名" FROM public."05人员表" p INNER JOIN public."_nc_m2m_04项目总表_05人员表" m ON m."05人员表_id" = p.id WHERE m."04项目总表_id" = $1', params = list(proj_db))
        if (nrow(parts) > 0) { pos_order <- c("工程师"=1L,"CRA"=2L,"统计师"=3L,"实习生"=4L); ord <- vapply(seq_len(nrow(parts)), function(i) { pos <- if (is.na(parts[["岗位"]][i]) || !nzchar(trimws(as.character(parts[["岗位"]][i])))) "" else trimws(as.character(parts[["岗位"]][i])); if (pos %in% names(pos_order)) pos_order[[pos]] else 99L }, integer(1)); parts <- parts[order(ord, parts[["姓名"]]),,drop=FALSE]; participants_display <- vapply(seq_len(nrow(parts)), function(i) { pos <- if (is.na(parts[["岗位"]][i]) || !nzchar(trimws(as.character(parts[["岗位"]][i])))) "" else as.character(parts[["岗位"]][i]); nm <- if (is.na(parts[["姓名"]][i])) "" else as.character(parts[["姓名"]][i]); if (nzchar(pos)) paste0(pos, "-", nm) else (if (nzchar(nm)) nm else "") }, character(1)); participants_display <- participants_display[nzchar(participants_display)] }
      }
    }, error = function(e) {})

    importance_level <- if ("重要紧急程度" %in% names(original_task)) as.character(original_task[["重要紧急程度"]][1]) else NA_character_
    importance_display <- if (is.na(importance_level) || !nzchar(trimws(importance_level))) "（未设置）" else trimws(importance_level)
    importance_color <- switch(trimws(importance_level), "重要紧急"="#C62828","重要不紧急"="#F57C00","紧急不重要"="#1976D2","不重要不紧急"="#616161","#757575")

    pt_for_lookup <- if ("project_type" %in% names(original_task)) as.character(original_task$project_type[1]) else NA_character_
    is_s09_task <- stage_supports_sample(tn, pt_for_lookup) && is_sync

    snapshot_cols <- c("planned_start_date","actual_start_date","planned_end_date","actual_end_date","remark_json","progress","contributors_json","sample_json","row_version")
    snapshot_row <- if (!is.na(stage_instance_id)) tryCatch(fetch_row_snapshot(pg_con, "09项目阶段实例表", stage_instance_id, snapshot_cols, lock=FALSE), error=function(e) NULL) else NULL
    remark_raw <- if (!is.null(snapshot_row)) snapshot_row[["remark_json"]] else NA_character_
    remarks_df <- if (!is.na(stage_instance_id)) fetch_11_feedback_df(pg_con, stage_instance_id) else parse_remark_json_to_df(remark_raw)
    snapshot_feedback_map <- if (!is.na(stage_instance_id)) fetch_11_feedback_map(pg_con, stage_instance_id) else sort_feedback_map(parse_named_json_map(remark_raw))
    sample_pairs <- if (is_s09_task && !is.null(snapshot_row)) parse_sample_df(snapshot_row[["sample_json"]]) else empty_sample_df()
    contrib_df <- if (!is.null(snapshot_row)) parse_contrib_json_to_df(snapshot_row[["contributors_json"]]) else empty_contrib_df()

    can_edit <- !is.na(stage_instance_id)
    if (can_edit) {
      if (is_s09_task) sample_row_count(if (is.null(sample_pairs)) 0L else nrow(sample_pairs)) else sample_row_count(0L)
      contrib_row_count(nrow(contrib_df)); remark_row_count(nrow(remarks_df))
      task_edit_context(list(
        project_id = task_info$project_id[1], site_name = task_info$site_name[1], task_name = tn,
        task_display_name = if ("task_display_name" %in% names(task_info)) as.character(task_info$task_display_name[1]) else NA_character_,
        is_sync = is_sync, supports_sample = is_s09_task, project_type = pt_for_lookup, importance = importance_level,
        planned_start_date = raw_planned_start, actual_start_date = raw_actual_start, planned_end_date = raw_planned, actual_end_date = actual_end_date,
        progress = actual_progress, stage_instance_id = stage_instance_id, remark = remark_raw, remark_entries = remarks_df,
        proj_row_id = proj_row_id, site_row_id = site_row_id,
        col_map = list(planned_start="planned_start_date",actual_start="actual_start_date",plan="planned_end_date",act="actual_end_date",note="remark_json",progress="progress"),
        table_name = "09项目阶段实例表",
        milestone_table = if (is_sync) "04项目总表" else "03医院_项目表",
        milestone_row_id = if (is_sync) proj_row_id else as.integer(site_row_id),
        samples = sample_pairs, contributors = contrib_df,
        milestones = empty_milestone_df(), milestone_raw = NA_character_, milestone_stage_key = tn,
        note_col = "remark_json", contrib_col = "contributors_json",
        snapshot_row = snapshot_row,
        snapshot_project_row = if (!is.na(proj_row_id)) tryCatch(fetch_row_snapshot(pg_con, "04项目总表", proj_row_id, "重要紧急程度", lock=FALSE), error=function(e) NULL) else NULL,
        snapshot_feedback_map = snapshot_feedback_map
      ))
    }

    .render_task_detail_modal(
      task_info_row = task_info,
      proj_row_id = proj_row_id,
      stage_instance_id = stage_instance_id,
      raw_planned_start = raw_planned_start,
      raw_actual_start = raw_actual_start,
      raw_planned_end = raw_planned,
      actual_end_date = actual_end_date,
      is_unplanned = is_unplanned,
      is_s09_task = is_s09_task,
      actual_progress = actual_progress,
      planned_p = planned_p,
      diff_p = diff_p,
      delay_days = delay_days,
      importance_display = importance_display,
      importance_color = importance_color,
      manager_display = manager_display,
      participants_display = participants_display,
      diagnostic_text = diagnostic_text,
      can_edit = can_edit
    )
  }, ignoreInit = TRUE)

  # ---------- 历史会议列表（筛选在左侧侧栏，避免与 meeting_data 同块 render 导致控件被重建） ----------
  output$meeting_history_list <- renderUI({
    req(identical(input$main_tabs, "tab_meeting"))
    meeting_data() # 依赖
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return(tags$div(class = "alert alert-warning", "请先登录"))

    df <- meeting_data()
    if (nrow(df) == 0) {
      return(tags$div(style = "padding: 40px; text-align: center; color: #888;",
        tags$h4("暂无会议决策记录"),
        tags$p("点击「新建会议」标签创建第一个会议决策。")))
    }

    # 按 (会议名称, 会议时间) 分组
    df$group_key <- paste0(df[["会议名称"]], "||", format(df[["会议时间"]], "%Y-%m-%d %H:%M", tz = APP_TZ_CN))
    # split() 对字符会按因子**字母序**排面板，导致像按标题排；先按会议时间降序再固定 levels = 首次出现顺序
    ord <- order(df[["会议时间"]], df$id, decreasing = TRUE, na.last = TRUE)
    df <- df[ord, , drop = FALSE]
    groups <- split(df, factor(df$group_key, levels = unique(df$group_key)))

    panel_list <- lapply(names(groups), function(gk) {
      gdf <- groups[[gk]]
      mtg_name <- gdf[["会议名称"]][1]
      mtg_time <- gdf[["会议时间"]][1]
      if (is.na(mtg_time)) {
        mtg_time_str <- "时间未定"
      } else {
        mtg_time_str <- format(mtg_time, "%Y-%m-%d %H:%M", tz = APP_TZ_CN)
      }

      link_df <- data.frame(
        did = integer(0), id = integer(0),
        fb_type = character(0), fb_content = character(0),
        fb_updated = character(0), fb_rj = character(0),
        fb_created = as.POSIXct(character(0)),
        task_key_raw = character(0), site_name = character(0),
        task_display_name = character(0),
        stringsAsFactors = FALSE
      )
      did <- unique(as.integer(gdf$id))
      if (length(did) > 0L && !is.null(pg_con) && DBI::dbIsValid(pg_con)) {
        link_df <- tryCatch({
          q <- sprintf(
            'SELECT l."10会议决策表_id" AS did,
                    f.id,
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
             FROM public."12会议决策关联问题表" l
             INNER JOIN public."11阶段问题反馈表" f ON f.id = l."11阶段问题反馈表_id"
             LEFT JOIN public."09项目阶段实例表" si ON si.id = f."09项目阶段实例表_id"
             LEFT JOIN public."08项目阶段定义表" d ON d.id = si.stage_def_id
             LEFT JOIN public."03医院_项目表" s ON s.id = si.site_project_id
             LEFT JOIN public."01医院信息表" h ON h.id = s."01_hos_resource_table医院信息表_id"
             WHERE l."10会议决策表_id" IN (%s)
             ORDER BY l.id',
            paste(did, collapse = ","))
          DBI::dbGetQuery(pg_con, q)
        }, error = function(e) data.frame())
        if (is.null(link_df) || nrow(link_df) == 0L) {
          link_df <- data.frame(
            did = integer(0), id = integer(0),
            fb_type = character(0), fb_content = character(0),
            fb_updated = character(0), fb_rj = character(0),
            fb_created = as.POSIXct(character(0)),
            task_key_raw = character(0), site_name = character(0),
            task_display_name = character(0),
            stringsAsFactors = FALSE
          )
        }
      }

      # 按项目分组（名称顺序即展示顺序）
      proj_groups <- split(gdf, ifelse(is.na(gdf[["项目名称"]]), "(未关联项目)", gdf[["项目名称"]]))
      proj_names_ordered <- names(proj_groups)
      proj_n_counter <- 0L
      proj_sections <- vector("list", length(proj_names_ordered))
      for (j in seq_along(proj_names_ordered)) {
        pname <- proj_names_ordered[j]
        pdf <- proj_groups[[pname]]
        is_common <- identical(pname, "共性决策")
        proj_header <- if (is_common) pname else {
          proj_n_counter <- proj_n_counter + 1L
          paste0("项目", proj_n_counter, " ", pname)
        }

        decision_items <- lapply(seq_len(nrow(pdf)), function(ri) {
          row <- pdf[ri, ]
          dec_id <- row$id
          dec_content <- if (is.na(row[["决策内容"]])) "" else as.character(row[["决策内容"]])
          executor_json <- as.character(row[["决策执行人及执行确认"]])
          exec_df <- parse_executor_json(executor_json)

          exec_block <- NULL
          if (nrow(exec_df) > 0) {
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
              is_current_user <- grepl(paste0("-", auth$work_id, "$"), ek)

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
            exec_block <- tags$div(
              class = "meeting-exec-wrap",
              tags$span(style = "font-weight: 600; color: #424242;", "执行人："),
              tags$div(class = "meeting-exec-grid", exec_cells)
            )
          }

          fb_sub <- NULL
          if (nrow(link_df) > 0L) {
            lf <- link_df[link_df$did == dec_id, , drop = FALSE]
            if (nrow(lf) > 0L) {
              need_cols <- c("id", "fb_type", "fb_content", "fb_updated", "fb_rj", "task_key_raw", "site_name", "fb_created")
              miss <- setdiff(need_cols, names(lf))
              if (length(miss) == 0L) {
                fb_part <- lf[, need_cols, drop = FALSE]
                flat <- meeting_new_flat_feedback_rows(fb_part, stage_label_fn = stage_label_for_key)
                if (nrow(flat) > 0L) {
                  fb_sub <- tags$div(
                    style = "margin-bottom: 10px; font-size: 13px; line-height: 1.45; color: #333;",
                    tags$span(style = "font-weight: 600; color: #424242;", "项目要点："),
                    tags$ul(
                      style = "margin: 4px 0 0 18px; padding: 0; list-style: disc;",
                      lapply(seq_len(nrow(flat)), function(li) {
                        tags$li(
                          style = "margin-bottom: 6px;",
                          meeting_new_pt_line_tags(
                            flat$stage_label[li], flat$site_name[li], flat$typ_show[li],
                            flat$updated_at[li], flat$reporter[li], flat$content[li],
                            flat$task_key_raw[li]
                          )
                        )
                      })
                    )
                  )
                }
              }
            }
          }

          tags$div(
            class = "meeting-decision-item",
            if (!is.null(fb_sub)) fb_sub,
            tags$div(
              style = "font-size: 14px; line-height: 1.5; color: #333;",
              tags$span(style = "font-weight: 600; color: #424242;", paste0("决策内容", ri, "：")),
              tags$span(dec_content)
            ),
            exec_block
          )
        })

        proj_sections[[j]] <- tags$div(
          class = "meeting-project-group",
          tags$div(style = "font-weight: bold; color: #1565C0; margin-bottom: 6px;", proj_header),
          decision_items
        )
      }

      # 查找该组最早的 id 用于编辑按钮
      first_id <- min(gdf$id)

      tags$div(class = "meeting-panel",
        tags$div(class = "meeting-panel-heading",
          tags$span(paste0(mtg_name, "（", mtg_time_str, "）")),
          tags$button(
            type = "button", class = "btn btn-sm btn-default btn-edit-mtg", style = "margin-left: 12px;",
            `data-mtg-id` = as.character(first_id),
            "编辑"
          )
        ),
        tags$div(class = "meeting-panel-body", proj_sections)
      )
    })

    panel_list
  })

  # ---------- 新建会议 UI ----------
  output$meeting_new_ui <- renderUI({
    req(identical(input$main_tabs, "tab_meeting"))
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return(tags$div(class = "alert alert-warning", "请先登录"))

    time_val <- now_beijing_str("%Y-%m-%d %H:%M")
    ch <- meeting_new_build_project_choices(pg_con, auth)
    sel_proj <- isolate({
      v <- input$mtg_new_proj
      if (is.null(v) || length(v) == 0L) NULL else as.character(v)[1]
    })

    tagList(
      tags$div(style = "max-width: 920px; margin: 0 auto; padding: 20px;",
        tags$h3("新建会议决策", style = "margin-top: 0;"),
        tags$p(style = "color:#888; font-size: 13px; margin: 0 0 12px 0;",
          "项目筛选在左侧侧栏（与项目甘特图相同维度）；不选任何维度时「选择项目」列出全部可选项目。"
        ),
        tags$div(style = "display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap;",
          tags$div(style = "flex: 1; min-width: 200px;",
            textInput("mtg_new_name", "会议名称", value = "", width = "100%", placeholder = "输入会议名称...")
          ),
          tags$div(style = "width: 200px;",
            textInput("mtg_new_time", "会议时间", value = time_val, width = "100%", placeholder = "YYYY-MM-DD HH:MM")
          )
        ),
        tags$p(style = "color:#666; font-size: 13px; margin: 8px 0 12px 0;",
          "先填写会议名称与时间，再选择项目。具体项目下：逐条点击「要点」卡片打开弹窗，针对该要点填写决策并保存（直接写入会议决策表）。共性决策仍用下方按钮打开弹窗，不关联要点。"
        ),
        selectizeInput(
          "mtg_new_proj", "选择项目",
          choices = ch, selected = sel_proj, width = "100%",
          options = list(placeholder = "共性决策 或 具体项目…")
        ),
        tags$style(HTML(
          ".meeting-pt-line-readonly{border:1px solid #e8e8e8;border-radius:6px;padding:8px 12px;margin-bottom:8px;background:#fafbfd;font-size:13px;line-height:1.45;word-break:break-word;}",
          ".meeting-pt-click-card{border:1px solid #e0e0e0;border-radius:8px;padding:10px 14px;margin-bottom:10px;background:#fafbfd;cursor:pointer;transition:background .15s,border-color .15s;}",
          ".meeting-pt-click-card:hover{background:#f0f4fb;border-color:#bdc4e0;}"
        )),
        uiOutput("meeting_new_proj_sheet")
      )
    )
  })

  output$meeting_new_proj_sheet <- renderUI({
    meeting_force_refresh()
    meeting_new_modal_refresh()
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return(NULL)
    pv <- input$mtg_new_proj
    pvs <- if (is.null(pv) || length(pv) == 0L) "" else trimws(as.character(pv)[1])

    if (!nzchar(pvs)) {
      return(tags$p(style = "color:#888; margin-top: 12px;", "请在上方选择「共性决策」或具体项目。"))
    }
    if (identical(pvs, "__common__")) {
      return(tags$div(
        style = "margin-top: 16px; padding: 16px; border: 2px dashed #1565C0; border-radius: 8px; background: #f8fbff;",
        tags$p(style = "margin-bottom: 12px; color: #37474f;", "共性决策不关联项目登记要点。"),
        actionButton("mtg_btn_open_common_decisions", "登记共性会议决策", class = "btn btn-success", style = "font-weight: 600; white-space: normal; height: auto; padding: 10px 16px;")
      ))
    }
    pid <- suppressWarnings(as.integer(pvs))
    if (is.na(pid)) {
      return(tags$p(style = "color:#888; margin-top: 12px;", "项目无效。"))
    }
    raw <- fetch_11_feedback_all_for_project_df(pg_con, pid)
    flat <- meeting_new_flat_feedback_rows(raw, stage_label_fn = stage_label_for_key)
      tagList(
      tags$div(style = "margin-top: 16px; display: flex; flex-wrap: wrap; gap: 10px; align-items: center;",
        actionButton("mtg_btn_add_point_sheet", "添加要点", class = "btn btn-sm btn-default")
      ),
      if (nrow(flat) == 0L) {
        tags$p(style = "color:#999; margin-top: 12px;", "（本项目暂无已登记要点，可先点「添加要点」或在甘特中登记）")
      } else {
        tagList(
          tags$div(
            style = "font-weight: 600; margin: 12px 0 8px 0; color: #1565C0;",
            "已登记要点（按登记时间从新到旧，点击整条卡片登记针对该要点的决策）"
          ),
          lapply(seq_len(nrow(flat)), function(i) {
            iid <- as.integer(flat$id[i])
            tags$div(
              class = "meeting-pt-click-card",
              onclick = htmltools::HTML(sprintf(
                "Shiny.setInputValue('mtg_pt_pick', {id:%d, ts:(new Date()).getTime()}, {priority:'event'});",
                iid
              )),
              tags$div(
                style = "font-size:12px;color:#78909c;margin-bottom:4px;",
                sprintf("要点 id：%s · 点击此卡片", iid)
              ),
              meeting_new_pt_line_tags(
                flat$stage_label[i], flat$site_name[i], flat$typ_show[i],
                flat$updated_at[i], flat$reporter[i], flat$content[i],
                flat$task_key_raw[i]
              )
            )
          })
        )
      }
    )
  })

  observe({
    meeting_new_project_choices_filtered()
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return()
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return()
    ch <- meeting_new_project_choices_filtered()
    cur <- isolate(input$mtg_new_proj)
    updateSelectizeInput(session, "mtg_new_proj", choices = ch, selected = cur, server = FALSE)
  })

  # meeting_edit_feed_N 的 output$ 已改为按需注册（在 .show_edit_meeting_modal 内），
  # 不再在 server() 启动时预建 3000 个闭包

  output$mp_type_custom_wrap <- renderUI({
    v <- input$mp_type_preset %||% "问题"
    if (identical(as.character(v)[1], "自定义…")) {
      textInput("mp_type_custom", "自定义类型名称", value = "", width = "100%")
    } else {
      NULL
    }
  })

  output$meeting_new_add_point_inner <- renderUI({
    meeting_new_modal_refresh()
    pid <- meeting_new_add_point_pid()
    if (is.null(pid)) return(NULL)
    auth <- current_user_auth()
    if (auth$allow_none) return(NULL)
    # 限制为项目参与人员+负责人（姓名-工号去重并集）
    staff_choices <- project_member_choices(pg_con, pid)
    staff_labs <- names(staff_choices)
    # 报告人选择需要"姓名-工号"作为值（reporters_json 存储格式）
    staff_ch <- if (length(staff_labs) > 0L) setNames(staff_labs, staff_labs) else character(0)
    tagList(
      tags$p(style = "color:#555; font-size: 13px;", "新增记录为项目级要点（不分中心/阶段），保存后将出现在列表最上方。"),
      selectInput("mp_type_preset", "类型", choices = c("问题", "卡点", "经验分享", "自定义…"), selected = "问题", width = "100%"),
      uiOutput("mp_type_custom_wrap"),
      textAreaInput("mp_new_content", "内容", value = "", width = "100%", rows = 4, placeholder = "填写要点内容…"),
      if (length(staff_ch) > 0L) {
        selectizeInput(
          "mp_new_reporters",
          "反馈/填报人（可多选）",
          choices = staff_ch, selected = NULL, multiple = TRUE, width = "100%",
          options = list(placeholder = "可选")
        )
      } else {
        tags$p(tags$small("无法加载在职人员列表"), style = "color:#c00;")
      },
      tags$div(style = "margin-top: 12px;",
        actionButton("btn_meeting_new_add_point_submit", "保存要点", class = "btn btn-sm btn-success")
      )
    )
  })

  output$meeting_new_bulk_modal_inner <- renderUI({
    meeting_new_modal_refresh()
    meeting_new_bulk_nblocks()
    meeting_new_pt_nblocks()
    auth <- current_user_auth()
    if (is.null(auth) || auth$allow_none) return(NULL)
    mode <- meeting_new_bulk_mode()
    if (identical(mode, "none")) return(NULL)

    all_persons <- character(0)
    # 根据当前选择的项目筛选执行人范围：项目参与人员+负责人
    pv_proj <- input$mtg_new_proj
    pvs_proj <- if (is.null(pv_proj) || length(pv_proj) == 0L) "" else trimws(as.character(pv_proj)[1])
    if (nzchar(pvs_proj) && !identical(pvs_proj, "__common__")) {
      pid_proj <- suppressWarnings(as.integer(pvs_proj))
      if (!is.na(pid_proj)) {
        all_persons <- project_member_choices(pg_con, pid_proj)
      }
    }
    if (length(all_persons) == 0L) {
      # 共性决策或无项目上下文时，仍显示全部在职人员
      tryCatch({
        ap <- DBI::dbGetQuery(pg_con, 'SELECT id, "姓名", "工号" FROM public."05人员表" WHERE "人员状态" = \'在职\' ORDER BY "姓名"')
        if (nrow(ap) > 0) all_persons <- setNames(as.character(ap$id), paste0(ap[["姓名"]], "-", ap[["工号"]]))
      }, error = function(e) {})
    }

    block_ui_common <- function(bi) {
      tags$div(
        class = "well",
        style = "margin-bottom: 14px; padding: 12px 14px;",
        tags$div(style = "font-weight: 700; color: #37474f; margin-bottom: 8px;", sprintf("决策 %d", bi)),
        textAreaInput(
          paste0("mtg_bct_", bi),
          "决策内容",
          value = "",
          width = "100%",
          rows = 4,
          placeholder = "填写决策内容…"
        ),
        selectizeInput(
          paste0("mtg_bex_", bi),
          "执行人",
          choices = all_persons,
          selected = NULL,
          multiple = TRUE,
          width = "100%",
          options = list(placeholder = "选择执行人…", plugins = list("remove_button"))
        )
      )
    }

    block_ui_spt <- function(bi) {
      tags$div(
        class = "well",
        style = "margin-bottom: 14px; padding: 12px 14px;",
        tags$div(style = "font-weight: 700; color: #37474f; margin-bottom: 8px;", sprintf("决策 %d（均关联上方要点）", bi)),
        textAreaInput(
          paste0("mtg_spt_bct_", bi),
          "决策内容",
          value = "",
          width = "100%",
          rows = 4,
          placeholder = "填写针对该要点的决策内容…"
        ),
        selectizeInput(
          paste0("mtg_spt_bex_", bi),
          "执行人",
          choices = all_persons,
          selected = NULL,
          multiple = TRUE,
          width = "100%",
          options = list(placeholder = "选择执行人…", plugins = list("remove_button"))
        )
      )
    }

    if (identical(mode, "common")) {
      nb <- meeting_new_bulk_nblocks()
      if (!is.numeric(nb) || length(nb) != 1L || is.na(nb) || nb < 1L) nb <- 1L
      nb <- as.integer(min(25L, max(1L, nb)))
      return(tagList(
        tags$p(style = "color:#555; margin-bottom: 12px;", "不关联项目登记要点；可添加多条决策，保存时逐条写入数据库。"),
        lapply(seq_len(nb), function(bi) block_ui_common(bi)),
        if (nb < 25L) {
          actionButton("mtg_bulk_add_row", "+ 添加一条决策", class = "btn btn-default btn-sm")
        } else NULL
      ))
    }

    if (identical(mode, "pt_single")) {
      fid <- meeting_new_pt_fb_id()
      pid <- meeting_new_pt_proj_id()
      if (is.na(fid) || is.na(pid)) {
        return(tags$p(style = "color:#c00;", "要点或项目无效。"))
      }
      raw <- fetch_11_feedback_all_for_project_df(pg_con, pid)
      flat <- meeting_new_flat_feedback_rows(raw, stage_label_fn = stage_label_for_key)
      hit <- flat[flat$id == fid, , drop = FALSE]
      if (nrow(hit) == 0L) {
        return(tags$p(style = "color:#c00;", "该要点已不存在或已不属于当前项目。"))
      }
      i <- 1L
      nb <- meeting_new_pt_nblocks()
      if (!is.numeric(nb) || length(nb) != 1L || is.na(nb) || nb < 1L) nb <- 1L
      nb <- as.integer(min(25L, max(1L, nb)))
      return(tagList(
        tags$p(style = "color:#555; margin-bottom: 10px;", "以下决策在保存时均写入同一条登记要点（可多填几条会议决策行）。"),
        tags$div(
          class = "meeting-pt-line-readonly",
          style = "margin-bottom: 14px;",
          meeting_new_pt_line_tags(
            hit$stage_label[i], hit$site_name[i], hit$typ_show[i],
            hit$updated_at[i], hit$reporter[i], hit$content[i],
            hit$task_key_raw[i]
          )
        ),
        lapply(seq_len(nb), function(bi) block_ui_spt(bi)),
        if (nb < 25L) {
          actionButton("mtg_spt_add_row", "+ 添加一条决策", class = "btn btn-default btn-sm")
        } else NULL
      ))
    }

    NULL
  })

  observeEvent(input$mtg_btn_add_point_sheet, {
    req(is.numeric(input$mtg_btn_add_point_sheet) && input$mtg_btn_add_point_sheet > 0)
    auth <- current_user_auth()
    req(!auth$allow_none)
    pv <- input$mtg_new_proj
    pvs <- if (is.null(pv) || length(pv) == 0L) "" else as.character(pv)[1]
    if (!nzchar(pvs) || identical(pvs, "__common__")) {
      showNotification("请选择具体项目（非共性决策）后再添加要点", type = "warning")
      return()
    }
    pid <- suppressWarnings(as.integer(pvs))
    if (is.na(pid)) return()
    if (!auth$allow_all) {
      ok <- tryCatch({
        chk <- DBI::dbGetQuery(pg_con,
          paste0("SELECT 1 AS o FROM public.\"04项目总表\" WHERE id = $1 AND id IN (", auth$allowed_subquery, ")"),
          params = list(pid))
        nrow(chk) > 0L
      }, error = function(e) FALSE)
      if (!ok) {
        showNotification("无权为此项目添加要点", type = "warning")
        return()
      }
    }
    pn <- tryCatch({
      r <- DBI::dbGetQuery(pg_con, 'SELECT "项目名称" FROM public."04项目总表" WHERE id = $1', params = list(pid))
      if (is.null(r) || nrow(r) == 0L) paste0("项目 ", pid) else as.character(r[[1]][1])
    }, error = function(e) paste0("项目 ", pid))
    meeting_new_add_point_pid(pid)
    meeting_new_modal_refresh(meeting_new_modal_refresh() + 1L)
    showModal(modalDialog(
      title = paste0("添加要点 — ", pn),
      size = "m",
      easyClose = TRUE,
      footer = modalButton("关闭"),
      uiOutput("meeting_new_add_point_inner")
    ))
  }, ignoreInit = TRUE)

  observeEvent(input$btn_meeting_new_add_point_submit, {
    pid <- meeting_new_add_point_pid()
    req(!is.null(pid))
    auth <- current_user_auth()
    req(!auth$allow_none)
    preset <- as.character(input$mp_type_preset %||% "问题")[1]
    typ_ <- if (identical(preset, "自定义…")) trimws(input$mp_type_custom %||% "") else preset
    if (!nzchar(typ_)) {
      showNotification("请填写类型", type = "warning")
      return()
    }
    cont <- trimws(input$mp_new_content %||% "")
    if (!nzchar(cont)) {
      showNotification("请填写内容", type = "warning")
      return()
    }
    reps <- input$mp_new_reporters
    rvec <- if (is.null(reps)) character(0) else as.character(reps)
    ek <- sprintf("__proj_%d_%s_%06d", as.integer(pid), format(Sys.time(), "%Y%m%d%H%M%S", tz = APP_TZ_CN), sample.int(999999L, 1L))
    insert_project_scope_feedback_11(
      pg_con, pid, ek, typ_, cont, rvec,
      now_beijing_str("%Y-%m-%d %H:%M"),
      auth$work_id, auth$name
    )
    nid <- tryCatch({
      r <- DBI::dbGetQuery(pg_con,
        'SELECT id FROM public."11阶段问题反馈表" WHERE "04项目总表_id" = $1 AND "条目键" = $2 AND "09项目阶段实例表_id" IS NULL',
        params = list(as.integer(pid), ek))
      if (is.null(r) || nrow(r) == 0L) NA_integer_ else suppressWarnings(as.integer(r$id[1]))
    }, error = function(e) NA_integer_)
    if (!is.na(nid)) {
      insert_audit_log(
        pg_con, auth$work_id, auth$name,
        "INSERT", "11阶段问题反馈表", nid,
        sprintf("新建会议页添加项目级要点: %s", substr(cont, 1, 40)),
        sprintf("11 id=%s", nid),
        NULL, list(类型 = typ_, 内容 = cont), NULL
      )
    }
    meeting_new_add_point_pid(NULL)
    meeting_force_refresh(meeting_force_refresh() + 1L)
    removeModal()
    showNotification("已添加要点", type = "message")
  })

  open_meeting_new_bulk_modal <- function(title_txt) {
    meeting_new_modal_refresh(meeting_new_modal_refresh() + 1L)
    showModal(modalDialog(
      title = title_txt,
      size = "l",
      easyClose = TRUE,
      footer = tagList(
        actionButton("mtg_bulk_save", "保存到数据库", class = "btn btn-primary btn-sm"),
        modalButton("取消")
      ),
      uiOutput("meeting_new_bulk_modal_inner")
    ))
  }

  observeEvent(input$mtg_btn_open_common_decisions, {
    req(is.numeric(input$mtg_btn_open_common_decisions) && input$mtg_btn_open_common_decisions > 0)
    auth <- current_user_auth()
    req(!auth$allow_none)
    pv <- input$mtg_new_proj
    pvs <- if (is.null(pv) || length(pv) == 0L) "" else as.character(pv)[1]
    if (!identical(pvs, "__common__")) {
      showNotification("请先在上方选择「共性决策」", type = "warning")
      return()
    }
    meeting_new_bulk_mode("common")
    meeting_new_bulk_proj_id(NA_integer_)
    meeting_new_bulk_nblocks(1L)
    open_meeting_new_bulk_modal("登记共性会议决策")
  })

  observeEvent(input$mtg_pt_pick, {
    pk <- input$mtg_pt_pick
    if (is.null(pk)) return()
    fid <- if (is.list(pk) && !is.null(pk$id)) suppressWarnings(as.integer(pk$id)) else NA_integer_
    if (is.na(fid) || fid < 1L) return()
    auth <- current_user_auth()
    req(!auth$allow_none)
    pv <- input$mtg_new_proj
    pvs <- if (is.null(pv) || length(pv) == 0L) "" else trimws(as.character(pv)[1])
    if (identical(pvs, "__common__") || !nzchar(pvs)) {
      showNotification("请先选择具体项目，再点击要点卡片", type = "warning")
      return()
    }
    pid <- suppressWarnings(as.integer(pvs))
    if (is.na(pid)) return()
    raw <- fetch_11_feedback_all_for_project_df(pg_con, pid)
    flat <- meeting_new_flat_feedback_rows(raw, stage_label_fn = stage_label_for_key)
    if (!fid %in% flat$id) {
      showNotification("该要点已不属于当前所选项目，请刷新后重试", type = "warning")
      return()
    }
    meeting_new_pt_fb_id(fid)
    meeting_new_pt_proj_id(pid)
    meeting_new_bulk_mode("pt_single")
    meeting_new_pt_nblocks(1L)
    open_meeting_new_bulk_modal("登记会议决策（针对所选要点）")
  }, ignoreNULL = TRUE)

  observeEvent(input$mtg_bulk_add_row, {
    req(is.numeric(input$mtg_bulk_add_row) && input$mtg_bulk_add_row > 0)
    n <- meeting_new_bulk_nblocks()
    if (n >= 25L) {
      showNotification("单次最多添加 25 条决策", type = "warning")
      return()
    }
    meeting_new_bulk_nblocks(n + 1L)
  })

  observeEvent(input$mtg_spt_add_row, {
    req(is.numeric(input$mtg_spt_add_row) && input$mtg_spt_add_row > 0)
    n <- meeting_new_pt_nblocks()
    if (n >= 25L) {
      showNotification("单次最多添加 25 条决策", type = "warning")
      return()
    }
    meeting_new_pt_nblocks(n + 1L)
  })

  observeEvent(input$mtg_bulk_save, {
    auth <- current_user_auth()
    req(!auth$allow_none)

    mtg_name <- trimws(input$mtg_new_name %||% "")
    mtg_time_str <- trimws(input$mtg_new_time %||% "")
    if (!nzchar(mtg_name)) {
      showNotification("请填写会议名称", type = "warning")
      return()
    }
    mtg_time <- parse_datetime_beijing(mtg_time_str)
    if (is.null(mtg_time)) {
      showNotification("会议时间格式错误，请使用 YYYY-MM-DD HH:MM", type = "warning")
      return()
    }

    mode <- meeting_new_bulk_mode()
    if (!identical(mode, "common") && !identical(mode, "pt_single")) {
      showNotification("弹窗状态异常，请关闭后重试", type = "warning")
      return()
    }
    nb <- if (identical(mode, "common")) {
      meeting_new_bulk_nblocks()
    } else {
      meeting_new_pt_nblocks()
    }
    if (!is.numeric(nb) || length(nb) != 1L || is.na(nb) || nb < 1L) nb <- 1L
    nb <- as.integer(min(25L, max(1L, nb)))

    n_ok <- 0L
    tryCatch({
      for (bi in seq_len(nb)) {
        if (identical(mode, "common")) {
          content <- trimws(as.character(input[[paste0("mtg_bct_", bi)]] %||% ""))
          exec_in <- input[[paste0("mtg_bex_", bi)]]
          fb_int <- integer(0)
          actual_proj_id <- NA_integer_
        } else {
          content <- trimws(as.character(input[[paste0("mtg_spt_bct_", bi)]] %||% ""))
          exec_in <- input[[paste0("mtg_spt_bex_", bi)]]
          actual_proj_id <- meeting_new_pt_proj_id()
          fid0 <- meeting_new_pt_fb_id()
          if (is.na(actual_proj_id) || is.na(fid0) || fid0 < 1L) {
            showNotification("要点或项目信息已失效，请关闭弹窗后重新点击要点卡片", type = "warning")
      return()
    }
          fb_int <- as.integer(fid0)
          chk <- check_meeting_feedback_matches_project(pg_con, fb_int, actual_proj_id)
          if (!is.na(chk)) {
            showNotification(chk, type = "warning")
            return()
          }
        }
        if (!nzchar(content)) next
        exec_json <- build_executor_json(pg_con, exec_in)

        new_id <- meeting_next_id(pg_con)
        q <- paste0(
          "INSERT INTO public.\"10会议决策表\" ",
          "(\"id\", \"会议名称\", \"会议时间\", \"决策内容\", \"决策执行人及执行确认\", \"created_by\", \"updated_by\") ",
          "VALUES ($1, $2, $3, $4, $5::json, $6, $7)"
        )
        DBI::dbExecute(pg_con, q, params = list(
          new_id, mtg_name, mtg_time, content,
          exec_json, auth$work_id, auth$work_id))

        if (length(fb_int) > 0L) {
          q12 <- paste0(
            'INSERT INTO public."12会议决策关联问题表" ("10会议决策表_id", "11阶段问题反馈表_id") ',
            'VALUES ($1, $2) ON CONFLICT ("10会议决策表_id", "11阶段问题反馈表_id") DO NOTHING'
          )
          for (fid in fb_int) {
            DBI::dbExecute(pg_con, q12, params = list(new_id, as.integer(fid)))
          }
        }

        insert_audit_log(pg_con, auth$work_id, auth$name,
          "INSERT", "10会议决策表", new_id,
          sprintf("新增会议决策[%s]: %s", mtg_name, substr(content, 1, 50)),
          sprintf("新增决策 id=%d", new_id),
          NULL, list(会议名称 = mtg_name, 决策内容 = content), NULL)
        n_ok <- n_ok + 1L
      }

      if (n_ok == 0L) {
        showNotification("请至少填写一条非空的决策内容", type = "warning")
        return()
      }

      removeModal()
      meeting_new_bulk_mode("none")
      meeting_new_bulk_nblocks(1L)
      meeting_new_pt_nblocks(1L)
      meeting_new_pt_fb_id(NA_integer_)
      meeting_new_pt_proj_id(NA_integer_)
      meeting_force_refresh(meeting_force_refresh() + 1L)
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
      showNotification(sprintf("已写入 %d 条会议决策", n_ok), type = "message")
    }, error = function(e) {
      showNotification(paste("保存失败:", e$message), type = "error")
    })
  })

  # ==================== 会议决策 observeEvent ====================

  # 刷新
  observeEvent(input$mtg_refresh, {
    meeting_force_refresh(meeting_force_refresh() + 1L)
  })

  observeEvent(input$mtg_new_dim_refresh, {
    meeting_force_refresh(meeting_force_refresh() + 1L)
  }, ignoreInit = TRUE)

  # 点击执行人状态 -> 弹出修改模态框
  # JS 事件委托替代 5000 个 observeEvent：前端统一设 input$exec_status_clicked，此处单一 observeEvent 处理
  observeEvent(input$exec_status_clicked, {
    raw <- input$exec_status_clicked
    ddi <- as.integer(sub("_.*$", "", as.character(raw)))
    if (is.na(ddi)) return
        if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return
        tryCatch({
          row <- DBI::dbGetQuery(pg_con,
            'SELECT id, "决策内容", "决策执行人及执行确认"::text AS "决策执行人及执行确认" FROM public."10会议决策表" WHERE id = $1',
        params = list(as.integer(ddi)))
          if (nrow(row) == 0) return
      executor_modal_ctx(list(decision_id = as.integer(ddi), content = row[["决策内容"]][1], executor_json = as.character(row[["决策执行人及执行确认"]][1]), nonce = as.character(Sys.time())))
        }, error = function(e) {})
      }, ignoreInit = TRUE)

  # 执行状态修改模态框
  observeEvent(executor_modal_ctx(), {
    ctx <- executor_modal_ctx()
    if (is.null(ctx)) return
    auth <- current_user_auth()
    if (auth$allow_none) return

    exec_df <- parse_executor_json(ctx$executor_json)
    # 找到当前用户
    my_row <- exec_df[grepl(paste0("-", auth$work_id, "$"), exec_df$key), , drop = FALSE]
    if (nrow(my_row) == 0) return

    my_key <- my_row$key[1]
    my_status <- my_row$状态[1]
    my_note <- my_row$说明[1]

    showModal(modalDialog(
      title = "更新执行状态",
      tags$div(
        tags$p(strong("决策内容: "), ctx$content),
        tags$hr(),
        tags$p(strong("执行人: "), executor_display_name_from_key(my_key)),
        radioButtons("executor_new_status", "执行状态",
          choices = c("未执行", "已执行"),
          selected = my_status, inline = TRUE),
        textInput("executor_new_note", "说明", value = my_note, width = "100%", placeholder = "填写执行说明...")
      ),
      footer = tagList(
        actionButton("btn_save_executor_status", "保存", class = "btn btn-sm btn-primary", style = "min-width: 80px;"),
        modalButton("取消")
      ),
      easyClose = TRUE
    ))
  })

  # 保存执行状态
  observeEvent(input$btn_save_executor_status, {
    auth <- current_user_auth()
    req(!auth$allow_none)
    ctx <- executor_modal_ctx()
    if (is.null(ctx)) return

    tryCatch({
      row <- DBI::dbGetQuery(pg_con,
        'SELECT "决策执行人及执行确认"::text AS "决策执行人及执行确认" FROM public."10会议决策表" WHERE id = $1',
        params = list(ctx$decision_id))
      if (nrow(row) == 0) return

      old_json <- as.character(row[["决策执行人及执行确认"]][1])
      exec_list <- jsonlite::fromJSON(old_json, simplifyVector = FALSE)

      # 找到当前用户的key
      my_key <- NULL
      for (k in names(exec_list)) {
        if (grepl(paste0("-", auth$work_id, "$"), k)) { my_key <- k; break }
      }
      if (is.null(my_key)) return

      old_status <- exec_list[[my_key]][["状态"]] %||% "未执行"
      old_note <- exec_list[[my_key]][["说明"]] %||% ""

      exec_list[[my_key]][["状态"]] <- input$executor_new_status
      exec_list[[my_key]][["说明"]] <- trimws(input$executor_new_note %||% "")
      new_json <- jsonlite::toJSON(exec_list, auto_unbox = TRUE)

      DBI::dbExecute(pg_con,
        'UPDATE public."10会议决策表" SET "决策执行人及执行确认" = $1::json, "updated_by" = $2 WHERE id = $3',
        params = list(as.character(new_json), auth$work_id, ctx$decision_id))

      insert_audit_log(pg_con, auth$work_id, auth$name,
        "UPDATE", "10会议决策表", ctx$decision_id,
        sprintf("更新执行状态[%s]: %s -> %s", my_key, old_status, input$executor_new_status),
        sprintf("执行状态变更: %s", my_key),
        list(状态 = old_status, 说明 = old_note),
        list(状态 = input$executor_new_status, 说明 = trimws(input$executor_new_note %||% "")),
        NULL)

      removeModal()
      showNotification("执行状态已更新", type = "message")
      meeting_force_refresh(meeting_force_refresh() + 1L)
      gantt_use_incremental(TRUE)
      gantt_force_refresh(gantt_force_refresh() + 1L)
      executor_modal_ctx(NULL)
    }, error = function(e) {
      showNotification(paste("更新失败:", e$message), type = "error")
    })
  })

  # 编辑会议（弹出模态框）— JS 事件委托，替代 100 个 observeEvent
  observeEvent(input$mtg_edit_clicked, {
    raw <- input$mtg_edit_clicked
    if (is.null(raw) || is.na(raw)) return
    id <- as.integer(sub("_.*$", "", as.character(raw)))
    if (!is.na(id)) .show_edit_meeting_modal(id)
      }, ignoreInit = TRUE)

  .show_edit_meeting_modal <- function(first_id) {
    if (is.null(pg_con) || !DBI::dbIsValid(pg_con)) return
    auth <- current_user_auth()
    if (auth$allow_none) return

    tryCatch({
      # 获取同一场会议的所有决策行
      target <- DBI::dbGetQuery(pg_con,
        'SELECT id, "会议名称", "会议时间" FROM public."10会议决策表" WHERE id = $1',
        params = list(first_id))
      if (nrow(target) == 0) return
      mtg_name <- target[["会议名称"]][1]
      mtg_time <- target[["会议时间"]][1]

      all_rows <- DBI::dbGetQuery(pg_con,
        'SELECT t.id, t."决策内容", t."决策执行人及执行确认"::text AS "决策执行人及执行确认" FROM public."10会议决策表" t WHERE t."会议名称" = $1 AND t."会议时间" = $2 ORDER BY t.id',
        params = list(mtg_name, mtg_time))

      # 获取项目选项 + "共性决策"（与新建会议一致；落库仅通过 12→11）
      project_choices <- c("共性决策" = "__common__")
      and_auth <- if (auth$allow_all) "" else paste0(' AND id IN (', auth$allowed_subquery, ')')
      pq <- paste0('SELECT id, "项目名称" FROM public."04项目总表" WHERE "项目名称" IS NOT NULL', and_auth, ' ORDER BY "项目名称"')
      pdf <- DBI::dbGetQuery(pg_con, pq)
      if (nrow(pdf) > 0) project_choices <- c(project_choices, setNames(as.character(pdf$id), pdf[["项目名称"]]))

      orig_fb <- vector("list", nrow(all_rows))
      names(orig_fb) <- as.character(all_rows$id)
      orig_proj_sel <- character(nrow(all_rows))
      names(orig_proj_sel) <- as.character(all_rows$id)
      for (i in seq_len(nrow(all_rows))) {
        rid <- all_rows$id[i]
        fq <- DBI::dbGetQuery(pg_con,
          'SELECT "11阶段问题反馈表_id" AS i FROM public."12会议决策关联问题表" WHERE "10会议决策表_id" = $1 ORDER BY id',
          params = list(rid))
        orig_fb[[as.character(rid)]] <- if (nrow(fq) > 0L) as.character(fq$i) else character(0)
        pr_q <- DBI::dbGetQuery(pg_con,
          'SELECT f."04项目总表_id" AS pid FROM public."12会议决策关联问题表" l INNER JOIN public."11阶段问题反馈表" f ON f.id = l."11阶段问题反馈表_id" WHERE l."10会议决策表_id" = $1 ORDER BY l.id LIMIT 1',
          params = list(rid))
        orig_proj_sel[as.character(rid)] <- if (nrow(pr_q) == 0L || is.na(pr_q$pid[1])) "__common__" else as.character(as.integer(pr_q$pid[1]))
      }

      all_persons <- character(0)
      # 收集本会议涉及的全部项目ID，仅展示其参与人员+负责人
      involved_pids <- unique(suppressWarnings(as.integer(orig_proj_sel[orig_proj_sel != "__common__"])))
      involved_pids <- involved_pids[!is.na(involved_pids)]
      if (length(involved_pids) > 0L) {
        all_persons <- project_member_choices(pg_con, involved_pids)
      }
      if (length(all_persons) == 0L) {
        # 兜底：全部在职人员（共性决策或无法确定项目时）
        tryCatch({
          ap <- DBI::dbGetQuery(pg_con, 'SELECT id, "姓名", "工号" FROM public."05人员表" WHERE "人员状态" = \'在职\' ORDER BY "姓名"')
          if (nrow(ap) > 0) all_persons <- setNames(as.character(ap$id), paste0(ap[["姓名"]], "-", ap[["工号"]]))
        }, error = function(e) {})
      }

      n_dec <- nrow(all_rows)
      decision_blocks <- lapply(seq_len(n_dec), function(ri) {
        row <- all_rows[ri, , drop = FALSE]
        rid <- as.integer(row$id[1])
        sel_val <- as.character(orig_proj_sel[as.character(rid)])[1]
        ej <- as.character(row[["决策执行人及执行确认"]])[1]
        exec_sel <- person_ids_from_executor_json(pg_con, ej)
        tags$div(
          class = "meeting-new-slot-block",
          style = "margin-bottom: 28px; padding-bottom: 20px; border-bottom: 1px solid #e8e8e8;",
          selectizeInput(
            paste0("edit_mtg_proj_", rid),
            label = if (n_dec == 1L) "选择项目" else sprintf("项目 %d", ri),
              choices = project_choices,
              selected = sel_val,
            width = "100%",
            options = list(placeholder = "共性决策 或 具体项目…")
          ),
          uiOutput(paste0("meeting_edit_feed_", rid)),
          textAreaInput(
            paste0("edit_mtg_content_", rid),
            "决策内容",
            value = if (is.na(row[["决策内容"]])) "" else as.character(row[["决策内容"]])[1],
            width = "100%", rows = 4, placeholder = "填写决策内容…"
          ),
          selectizeInput(
            paste0("edit_mtg_exec_", rid),
            "执行人",
            choices = all_persons,
            selected = exec_sel,
            multiple = TRUE,
            width = "100%",
            options = list(placeholder = "选择执行人…", plugins = list("remove_button"))
          )
        )
      })

      meeting_edit_ctx(list(
        first_id = first_id,
        mtg_name = mtg_name,
        mtg_time = mtg_time,
        row_ids = all_rows$id,
        original_data = all_rows,
        orig_feedback = orig_fb,
        orig_proj_sel = orig_proj_sel
      ))
      meeting_edit_nonce(meeting_edit_nonce() + 1L)

      # 按需注册编辑弹窗中用到的 output[["meeting_edit_feed_<rid>"]]
      # 只为本次会议实际包含的决策行注册闭包（通常 1~20 个），而非预建 3000 个
      for (.ri in seq_len(nrow(all_rows))) {
        local({
          .rid <- as.integer(all_rows$id[.ri])
          output[[paste0("meeting_edit_feed_", .rid)]] <- renderUI({
            meeting_edit_nonce()
            ctx <- meeting_edit_ctx()
            if (is.null(ctx) || !(.rid %in% ctx$row_ids)) return(NULL)
            auth <- current_user_auth()
            if (is.null(auth) || auth$allow_none) return(NULL)

            pv <- input[[paste0("edit_mtg_proj_", .rid)]]
            pvs <- if (is.null(pv) || length(pv) == 0L || !nzchar(trimws(as.character(pv)[1]))) {
              as.character(ctx$orig_proj_sel[as.character(.rid)])[1]
            } else {
              as.character(pv)[1]
            }
            if (is.na(pvs) || !nzchar(pvs %||% "")) pvs <- "__common__"

            orig_fb <- ctx$orig_feedback[[as.character(.rid)]] %||% character(0)
            orig_fb <- as.character(orig_fb)[nzchar(as.character(orig_fb))]

            if (identical(pvs, "__common__")) {
              return(tags$p(style = "color:#888;", "已选「共性决策」：无需关联项目要点。"))
            }
            pid <- suppressWarnings(as.integer(pvs))
            if (is.na(pid)) {
              return(tags$p(style = "color:#888;", "项目无效。"))
            }
            raw <- fetch_11_feedback_all_for_project_df(pg_con, pid)
            flat <- meeting_new_flat_feedback_rows(raw, stage_label_fn = stage_label_for_key)
            if (nrow(flat) == 0L) {
              return(tags$p(style = "color:#999;", "（本项目暂无已登记要点）"))
            }
            ch_fb <- meeting_feedback_id_choices(flat)
            valid_ids <- as.character(flat$id)
            sel_fb <- as.character(orig_fb)[as.character(orig_fb) %in% valid_ids]
            tagList(
              tags$div(
                style = "font-weight: 600; margin-bottom: 8px; color: #1565C0;",
                "登记要点（按登记时间从新到旧，供对照）"
              ),
              lapply(seq_len(nrow(flat)), function(i) {
                tags$div(
                  class = "meeting-pt-line-readonly",
                  style = "margin-bottom: 8px;",
                  meeting_new_pt_line_tags(
                    flat$stage_label[i], flat$site_name[i], flat$typ_show[i],
                    flat$updated_at[i], flat$reporter[i], flat$content[i],
                    flat$task_key_raw[i]
                  )
                )
              }),
              selectizeInput(
                paste0("edit_mtg_fb_", .rid),
                "本决策关联的要点（多选）",
                choices = ch_fb,
                selected = sel_fb,
                multiple = TRUE,
                width = "100%",
                options = list(placeholder = "选择关联的登记要点…", plugins = list("remove_button"))
              )
            )
          })
        })
      }

      showModal(modalDialog(
        title = "编辑会议决策",
        size = "l", easyClose = TRUE,
        tags$div(
          style = "max-width: 920px; margin: 0 auto;",
          tags$style(HTML(
            ".meeting-pt-line-readonly{border:1px solid #e8e8e8;border-radius:6px;padding:8px 12px;margin-bottom:8px;background:#fafbfd;font-size:13px;line-height:1.45;word-break:break-word;}"
          )),
          tags$div(style = "display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap;",
            tags$div(style = "flex: 1; min-width: 200px;",
            textInput("edit_mtg_name", "会议名称", value = mtg_name, width = "100%")
          ),
          tags$div(style = "width: 200px;",
            textInput("edit_mtg_time", "会议时间",
                value = if (is.na(mtg_time)) "" else format(mtg_time, "%Y-%m-%d %H:%M", tz = APP_TZ_CN),
                width = "100%", placeholder = "YYYY-MM-DD HH:MM")
            )
          ),
          tags$p(style = "color:#666; font-size: 13px; margin: 8px 0 12px 0;",
            "与「新建会议」一致：具体项目请在「本决策关联的要点」多选框中选择；共性决策无需关联。保存时执行人调整会合并进原有 JSON，保留已填状态与说明。"
          ),
          tagList(decision_blocks)
        ),
        footer = tagList(
          actionButton("btn_save_mtg_edit", "保存修改", class = "btn btn-sm btn-primary", style = "min-width: 100px;"),
          modalButton("取消")
        )
      ))
    }, error = function(e) {
      showNotification(paste("加载会议数据失败:", e$message), type = "error")
    })
  }

  # 保存编辑后的会议
  observeEvent(input$btn_save_mtg_edit, {
    auth <- current_user_auth()
    req(!auth$allow_none)
    ctx <- meeting_edit_ctx()
    if (is.null(ctx)) return

    new_name <- trimws(input$edit_mtg_name %||% "")
    new_time_str <- trimws(input$edit_mtg_time %||% "")
    if (!nzchar(new_name)) {
      showNotification("会议名称不能为空", type = "warning")
      return()
    }
    new_time <- parse_datetime_beijing(new_time_str)
    if (is.null(new_time)) {
      showNotification("时间格式错误", type = "warning")
      return()
    }

    tryCatch({
      for (rid in ctx$row_ids) {
        new_proj <- input[[paste0("edit_mtg_proj_", rid)]]
        new_content <- trimws(input[[paste0("edit_mtg_content_", rid)]] %||% "")
        is_common <- is.null(new_proj) || !nzchar(as.character(new_proj)[1]) || as.character(new_proj)[1] == "__common__"
        actual_proj_id <- if (is_common) NA_integer_ else suppressWarnings(as.integer(new_proj))

        fb_int <- integer(0)
        if (!is_common && !is.na(actual_proj_id)) {
          fb_raw <- input[[paste0("edit_mtg_fb_", rid)]]
          if (is.null(fb_raw)) fb_raw <- character(0)
          fb_int <- suppressWarnings(as.integer(fb_raw))
          fb_int <- unique(fb_int[!is.na(fb_int) & fb_int > 0L])
        }
        if (!is_common && length(fb_int) == 0L) {
          showNotification(sprintf("决策 id=%s：请在「本决策关联的要点」中至少选择一条", rid), type = "warning")
          return()
        }

        chk <- check_meeting_feedback_matches_project(pg_con, fb_int, actual_proj_id)
        if (!is.na(chk)) {
          showNotification(chk, type = "warning")
          return()
        }

        exec_raw <- input[[paste0("edit_mtg_exec_", rid)]]
        exec_ids <- if (is.null(exec_raw)) integer(0) else suppressWarnings(as.integer(exec_raw))
        exec_ids <- unique(exec_ids[!is.na(exec_ids) & exec_ids > 0L])

        orig_row <- ctx$original_data[ctx$original_data$id == rid, , drop = FALSE]
        orig_fb <- ctx$orig_feedback[[as.character(rid)]] %||% character(0)
        orig_proj <- ctx$orig_proj_sel[[as.character(rid)]] %||% "__common__"
        new_proj_disp <- if (is_common) "__common__" else as.character(as.integer(new_proj))
        orig_ex <- if (nrow(orig_row) > 0L) as.character(orig_row[["决策执行人及执行确认"]])[1] else "{}"
        new_exec_json <- merge_executor_json_for_edit(pg_con, exec_ids, orig_ex)

        changes <- list()
        if (new_name != ctx$mtg_name) changes <- c(changes, sprintf("会议名称: [%s]->[%s]", ctx$mtg_name, new_name))
        if (!identical(new_time, ctx$mtg_time)) changes <- c(changes, "会议时间已变更")
        if (new_proj_disp != orig_proj) changes <- c(changes, sprintf("项目关联: [%s]->[%s]", orig_proj, new_proj_disp))
        orig_content <- if (nrow(orig_row) == 0L || is.na(orig_row[["决策内容"]])) "" else as.character(orig_row[["决策内容"]])[1]
        if (new_content != orig_content) changes <- c(changes, sprintf("内容: [%s]->[%s]", substr(orig_content, 1, 30), substr(new_content, 1, 30)))
        if (!identical(sort(as.character(orig_fb)), sort(as.character(fb_int)))) {
          changes <- c(changes, "项目要点已更新")
        }
        if (!identical(trimws(new_exec_json), trimws(orig_ex %||% ""))) {
          changes <- c(changes, "执行人已调整")
        }

        DBI::dbExecute(pg_con,
          'DELETE FROM public."12会议决策关联问题表" WHERE "10会议决策表_id" = $1',
          params = list(rid))
        if (length(fb_int) > 0L) {
          q12 <- paste0(
            'INSERT INTO public."12会议决策关联问题表" ("10会议决策表_id", "11阶段问题反馈表_id") ',
            'VALUES ($1, $2) ON CONFLICT ("10会议决策表_id", "11阶段问题反馈表_id") DO NOTHING'
          )
          for (fid in fb_int) {
            DBI::dbExecute(pg_con, q12, params = list(rid, as.integer(fid)))
          }
        }

        DBI::dbExecute(pg_con,
          'UPDATE public."10会议决策表" SET "会议名称" = $1, "会议时间" = $2, "决策内容" = $3, "决策执行人及执行确认" = $4::json, "updated_by" = $5 WHERE id = $6',
          params = list(new_name, new_time, new_content, new_exec_json, auth$work_id, rid))

        if (length(changes) > 0) {
          insert_audit_log(pg_con, auth$work_id, auth$name,
            "UPDATE", "10会议决策表", rid,
            sprintf("编辑会议决策[%s]: %s", new_name, paste(changes, collapse = "; ")),
            sprintf("编辑决策 id=%d", rid),
            list(会议名称 = ctx$mtg_name), list(会议名称 = new_name), NULL)
        }
      }

      removeModal()
      showNotification("会议修改已保存", type = "message")
      meeting_force_refresh(meeting_force_refresh() + 1L)
      meeting_edit_ctx(NULL)
    }, error = function(e) {
      showNotification(paste("保存失败:", e$message), type = "error")
    })
  })

  ivd_perf_elapsed(t_ivd_sess0, "sess|server() body finished registering (about to return from server fn)")
}


ivd_perf_ts("GLOBAL before shinyApp() return (app.R source 即将结束)")

shinyApp(ui = ui, server = server)
