# -*- coding: utf-8 -*-
"""
生成《观合科技 · 二级市场投资研究平台 系统介绍》10 页 PPT。
服务于二级市场投资者，覆盖五大模块：Daily / Feeds / Upload Sources / Watchlist / Agent MCP。
运行：python3 scripts/make_intro_ppt.py
输出：项目根目录 观合科技_系统介绍.pptx
"""
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE
from pptx.oxml.ns import qn

# ---------- 配色 ----------
DEEP      = RGBColor(0x0F, 0x1B, 0x2D)   # 深藏青（封面/封底背景）
NAVY      = RGBColor(0x16, 0x27, 0x47)   # 次深蓝
ACCENT    = RGBColor(0xC9, 0xA9, 0x61)   # 品牌金
ACCENT2   = RGBColor(0x4A, 0x90, 0xE2)   # 点缀蓝
LIGHT_BG  = RGBColor(0xF6, 0xF7, 0xFA)   # 浅灰背景
CARD      = RGBColor(0xFF, 0xFF, 0xFF)
CARD_LINE = RGBColor(0xE2, 0xE6, 0xEE)
TEXT_DARK = RGBColor(0x16, 0x1C, 0x28)
TEXT_MID  = RGBColor(0x52, 0x5C, 0x6E)
TEXT_MUT  = RGBColor(0x8A, 0x93, 0xA3)
WHITE     = RGBColor(0xFF, 0xFF, 0xFF)
RED_DOT   = RGBColor(0xD9, 0x53, 0x4F)

# ---------- 字体 ----------
F_LATIN = 'Arial'
F_EA    = '微软雅黑'

EMU_W, EMU_H = 13.333, 7.5  # 16:9


# ---------- 基础 helper ----------
def set_font(run, size=14, color=TEXT_DARK, bold=False,
             name_latin=F_LATIN, name_ea=F_EA):
    f = run.font
    f.size = Pt(size); f.bold = bold; f.color.rgb = color
    f.name = name_latin
    rPr = run._r.get_or_add_rPr()
    for tag in ('a:ea', 'a:cs'):
        for e in rPr.findall(qn(tag)):
            rPr.remove(e)
    for tag in ('a:ea', 'a:cs'):
        el = rPr.makeelement(qn(tag), {'typeface': name_ea})
        rPr.append(el)


def textbox(slide, l, t, w, h, anchor='top'):
    tb = slide.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = {'top': MSO_ANCHOR.TOP, 'mid': MSO_ANCHOR.MIDDLE,
                          'bot': MSO_ANCHOR.BOTTOM}[anchor]
    tf.margin_left = Pt(2); tf.margin_right = Pt(2)
    tf.margin_top = Pt(1); tf.margin_bottom = Pt(1)
    return tb, tf


def para(tf, text, size, color, bold=False, align='left',
         space_after=4, space_before=0, first=False,
         name_latin=F_LATIN, name_ea=F_EA):
    p = tf.paragraphs[0] if first else tf.add_paragraph()
    p.alignment = {'left': PP_ALIGN.LEFT, 'center': PP_ALIGN.CENTER,
                   'right': PP_ALIGN.RIGHT}[align]
    p.space_after = Pt(space_after); p.space_before = Pt(space_before)
    run = p.add_run(); run.text = text
    set_font(run, size, color, bold, name_latin, name_ea)
    return p


def rect(slide, l, t, w, h, fill, line=None, line_w=0.75,
         rounded=False, radius=0.05):
    shp = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE if rounded else MSO_SHAPE.RECTANGLE,
        Inches(l), Inches(t), Inches(w), Inches(h))
    if fill is None:
        shp.fill.background()
    else:
        shp.fill.solid(); shp.fill.fore_color.rgb = fill
    if line is None:
        shp.line.fill.background()
    else:
        shp.line.color.rgb = line; shp.line.width = Pt(line_w)
    shp.shadow.inherit = False
    if rounded:
        try: shp.adjustments[0] = radius
        except Exception: pass
    return shp


def oval(slide, l, t, w, h, fill, line=None, line_w=0.75):
    shp = slide.shapes.add_shape(MSO_SHAPE.OVAL,
        Inches(l), Inches(t), Inches(w), Inches(h))
    shp.fill.solid(); shp.fill.fore_color.rgb = fill
    if line is None: shp.line.fill.background()
    else: shp.line.color.rgb = line; shp.line.width = Pt(line_w)
    shp.shadow.inherit = False
    return shp


def card(slide, l, t, w, h, title, desc, accent=ACCENT,
         title_size=15, desc_size=11.5):
    """白底圆角卡片：左侧金色竖条 + 标题 + 描述。"""
    rect(slide, l, t, w, h, CARD, line=CARD_LINE, line_w=0.75, rounded=True)
    rect(slide, l + 0.06, t + 0.14, 0.085, h - 0.28, accent, rounded=False)
    tb, tf = textbox(slide, l + 0.32, t + 0.16, w - 0.5, h - 0.32, anchor='top')
    para(tf, title, title_size, DEEP, bold=True, first=True, space_after=3)
    para(tf, desc, desc_size, TEXT_MID, space_after=0)


def page_bg(slide, color=LIGHT_BG):
    rect(slide, -0.06, -0.06, EMU_W + 0.12, EMU_H + 0.12, color)


def top_bar(slide, color=ACCENT):
    rect(slide, 0, 0, EMU_W, 0.12, color)


def header_module(slide, number, name, tag):
    """模块页统一页眉：编号 + 名称 + 右侧定位小标签。"""
    tb, tf = textbox(slide, 0.7, 0.45, 4.2, 0.8)
    para(tf, number, 34, ACCENT, bold=True, first=True)
    tb2, tf2 = textbox(slide, 1.75, 0.62, 6.0, 0.6)
    para(tf2, name, 24, DEEP, bold=True, first=True)
    # 右侧 tag
    rect(slide, 8.7, 0.72, 3.95, 0.42, WHITE, line=CARD_LINE, rounded=True, radius=0.5)
    tb3, tf3 = textbox(slide, 8.85, 0.74, 3.7, 0.4, anchor='mid')
    para(tf3, tag, 11, TEXT_MID, first=True, align='left')
    # 分隔线
    rect(slide, 0.7, 1.42, EMU_W - 1.4, 0.012, CARD_LINE)


def footer_scene(slide, text):
    rect(slide, 0.7, 6.82, EMU_W - 1.4, 0.012, CARD_LINE)
    tb, tf = textbox(slide, 0.7, 6.9, EMU_W - 1.4, 0.35)
    para(tf, text, 10.5, TEXT_MUT, first=True)


def module_page(prs, number, name, tag, lead, items, scene):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    page_bg(s); top_bar(s)
    header_module(s, number, name, tag)
    # 大定位语
    tb, tf = textbox(s, 0.7, 1.62, EMU_W - 1.4, 0.7)
    para(tf, lead, 19, NAVY, bold=True, first=True)
    # 2x2 卡片
    xs = [0.7, 6.83]; ys = [2.45, 4.62]
    cw, ch = 5.8, 2.0
    idx = 0
    for ry in ys:
        for rx in xs:
            title, desc = items[idx]
            card(s, rx, ry, cw, ch, title, desc)
            idx += 1
    footer_scene(s, scene)
    return s


# =====================================================================
# 逐页构建
# =====================================================================
def build():
    prs = Presentation()
    prs.slide_width = Inches(EMU_W)
    prs.slide_height = Inches(EMU_H)

    # ---------- P1 封面 ----------
    s = prs.slides.add_slide(prs.slide_layouts[6])
    page_bg(s, DEEP)
    rect(s, 0, 0, EMU_W, 0.14, ACCENT)
    # 右侧装饰：错落金色细条
    for i, (x, y, hh) in enumerate(
            [(11.6, 1.0, 1.4), (12.05, 2.2, 2.2), (11.6, 4.6, 1.0), (12.5, 5.5, 1.6)]):
        rect(s, x, y, 0.06, hh, ACCENT)
    oval(s, 11.3, 6.0, 0.9, 0.9, NAVY, line=ACCENT, line_w=1)
    # 品牌
    tb, tf = textbox(s, 0.85, 0.7, 8, 0.5)
    para(tf, 'GUANHE CAPITAL  ·  观合科技', 14, ACCENT, bold=True, first=True)
    # 主标题
    tb, tf = textbox(s, 0.85, 2.45, 11, 1.2)
    para(tf, '观合科技', 46, WHITE, bold=True, first=True)
    # 副标题
    tb, tf = textbox(s, 0.9, 3.55, 11, 0.7)
    para(tf, 'AI 驱动的二级市场投资研究平台', 24, RGBColor(0xCF, 0xD6, 0xE2), first=True)
    # 标语
    tb, tf = textbox(s, 0.9, 4.45, 11, 0.6)
    para(tf, '让信息过载时代的投资决策  ·  更高效 · 更深度 · 更智能',
         16, ACCENT, bold=True, first=True)
    # 五模块药丸
    pills = ['Daily', 'Feeds', 'Upload Sources', 'Watchlist', 'Agent MCP']
    px = 0.9
    for name in pills:
        w = 0.42 + len(name) * 0.115
        rect(s, px, 5.35, w, 0.46, NAVY, line=ACCENT, line_w=0.75, rounded=True, radius=0.5)
        tb, tf = textbox(s, px, 5.36, w, 0.44, anchor='mid')
        para(tf, name, 11.5, ACCENT, bold=True, first=True, align='center')
        px += w + 0.18
    # 底部
    tb, tf = textbox(s, 0.9, 6.75, 11, 0.4)
    para(tf, '系统介绍  ·  服务于二级市场投资者', 12,
         RGBColor(0x8F, 0x99, 0xAB), first=True)

    # ---------- P2 痛点与回答 ----------
    s = prs.slides.add_slide(prs.slide_layouts[6])
    page_bg(s); top_bar(s)
    tb, tf = textbox(s, 0.7, 0.55, 11.9, 0.7)
    para(tf, '今日投资者的困境，与观合的回答', 24, DEEP, bold=True, first=True)
    rect(s, 0.7, 1.3, 5.6, 0.012, ACCENT)
    # 左栏 痛点
    pains = [
        ('信息分散', 'Twitter、YouTube、研报、行情散落十几个终端，反复切换、顾此失彼。'),
        ('信号淹没', '刷屏两小时，关键信息被噪音盖过，复盘时想不起细节。'),
        ('知识难沉淀', '研报、电话会、笔记随手丢，用时找不到、对不上。'),
        ('行情与资讯割裂', '自选股、新闻、研报之间断层，无法在一个上下文里看全。'),
        ('AI 拿不到私有数据', '通用 AI 不懂你的持仓与研报库，答得泛而无据。'),
    ]
    tb, tf = textbox(s, 0.7, 1.6, 6.0, 0.5)
    para(tf, '困境', 15, TEXT_MUT, bold=True, first=True)
    y = 2.05
    for title, desc in pains:
        oval(s, 0.72, y + 0.10, 0.18, 0.18, RED_DOT)
        tb, tf = textbox(s, 1.05, y, 5.65, 0.95)
        para(tf, title, 14.5, DEEP, bold=True, first=True, space_after=2)
        para(tf, desc, 11, TEXT_MID, space_after=0)
        y += 0.93
    # 右栏 深色回答卡
    rect(s, 7.05, 1.6, 5.6, 5.05, DEEP, rounded=True, radius=0.03)
    rect(s, 7.05, 1.6, 0.1, 5.05, ACCENT, rounded=False)
    tb, tf = textbox(s, 7.4, 1.85, 5.0, 0.5)
    para(tf, '观合的回答', 15, ACCENT, bold=True, first=True)
    tb, tf = textbox(s, 7.4, 2.35, 5.0, 0.9)
    para(tf, '一个平台  ·  五大模块  ·  AI 原生', 18, WHITE, bold=True, first=True)
    answers = [
        ('Feeds', '把关注源持续聚合到统一收件箱'),
        ('Upload Sources', '把研究资产沉淀成可检索的知识库'),
        ('Daily', '每日跨源信息中枢，10 分钟取代 2 小时'),
        ('Watchlist', '个股研究终端，行情与资讯打通'),
        ('Agent MCP', '把平台能力装进你的 AI 助手'),
    ]
    y = 3.35
    for name, desc in answers:
        rect(s, 7.4, y + 0.04, 0.12, 0.12, ACCENT)
        tb, tf = textbox(s, 7.7, y - 0.05, 4.8, 0.5)
        para(tf, name, 13.5, ACCENT, bold=True, first=True, space_after=1)
        para(tf, desc, 11, RGBColor(0xCF, 0xD6, 0xE2), space_after=0)
        y += 0.6
    rect(s, 7.4, 6.18, 4.9, 0.012, RGBColor(0x33, 0x44, 0x5E))
    tb, tf = textbox(s, 7.4, 6.22, 5.0, 0.4)
    para(tf, '让投资决策更高效、更深度、更智能。', 12, ACCENT, bold=True, first=True)

    # ---------- P3 平台总览 ----------
    s = prs.slides.add_slide(prs.slide_layouts[6])
    page_bg(s); top_bar(s)
    tb, tf = textbox(s, 0.7, 0.55, 11.9, 0.7)
    para(tf, '平台总览  ·  五大模块构成完整投研闭环', 24, DEEP, bold=True, first=True)
    rect(s, 0.7, 1.3, 5.6, 0.012, ACCENT)
    modules = [
        ('01', 'Feeds', '多渠道订阅聚合'),
        ('02', 'Upload Sources', '私人投研资料库'),
        ('03', 'Daily', '每日信息中枢'),
        ('04', 'Watchlist', '自选股研究终端'),
        ('05', 'Agent MCP', 'AI 能力对外输出'),
    ]
    n = len(modules)
    margin = 0.7; gap = 0.2
    cw = (EMU_W - 2 * margin - (n - 1) * gap) / n
    cx = margin; cy = 1.85; chh = 2.15
    for num, name, role in modules:
        rect(s, cx, cy, cw, chh, WHITE, line=CARD_LINE, rounded=True)
        rect(s, cx, cy, cw, 0.12, ACCENT, rounded=False)
        tb, tf = textbox(s, cx + 0.18, cy + 0.28, cw - 0.3, 0.5)
        para(tf, num, 22, ACCENT, bold=True, first=True)
        tb, tf = textbox(s, cx + 0.18, cy + 0.78, cw - 0.3, 0.7)
        para(tf, name, 14.5, DEEP, bold=True, first=True, space_after=2)
        para(tf, role, 10.5, TEXT_MID, space_after=0)
        cx += cw + gap
    # 汇流行
    tb, tf = textbox(s, 0.7, 4.2, EMU_W - 1.4, 0.5, anchor='mid')
    para(tf, '五大模块共享同一套 AI 引擎与数据底座 —— 输入有源、研究有库、决策有据',
         13.5, NAVY, bold=True, first=True, align='center')
    rect(s, 0.7, 4.78, EMU_W - 1.4, 0.012, CARD_LINE)
    # 底部双 banner
    rect(s, 0.7, 4.95, 5.9, 1.65, NAVY, rounded=True, radius=0.04)
    tb, tf = textbox(s, 0.95, 5.15, 5.4, 1.4)
    para(tf, 'AI 引擎层', 14, ACCENT, bold=True, first=True, space_after=4)
    para(tf, '统一 LLM 框架 · Jinja2 模板系统 · 多用户配置 · 限流 · 向量 RAG',
         11.5, RGBColor(0xCF, 0xD6, 0xE2), space_after=0)
    rect(s, 6.73, 4.95, 5.9, 1.65, DEEP, rounded=True, radius=0.04)
    rect(s, 6.73, 4.95, 0.1, 1.65, ACCENT)
    tb, tf = textbox(s, 6.98, 5.15, 5.4, 1.4)
    para(tf, '数据接入层', 14, ACCENT, bold=True, first=True, space_after=4)
    para(tf, 'EODHD 全球行情基本面 · 阿里云向量库 · Twitter / YouTube / 微博 / RSS',
         11.5, RGBColor(0xCF, 0xD6, 0xE2), space_after=0)
    footer_scene(s, '覆盖输入 · 沉淀 · 中枢 · 研究 · AI 输出 的完整投研工作流')

    # ---------- P4 Daily ----------
    module_page(prs, '01', 'Daily', '每日信息中枢  ·  登录落地页',
        '每天 10 分钟取代 2 小时跨平台刷信息——上传文件与订阅 Feed 合并为一条跨源时间线。',
        [
            ('跨源统一时间线',
             '上传文件 + 订阅 Feed 通过跨源合并成一条按时间倒序的卡片流，9+ 来源统一徽章，告别多平台切换。'),
            ('AI 摘要 + 智能标签',
             '每张卡片自带 50–200 字摘要、GICS 行业、Ticker、关键人物与关联国家，扫一眼即知是否值得深读。'),
            ('就地预览抽屉',
             '点击卡片不跳页，右侧抽屉展开原文与摘要，支持 PDF / 图片 / 音频 / 视频 / 字幕等 9 种渲染，键盘翻阅全日内容。'),
            ('结构化每日日报',
             'Bloomberg / CNBC / Finviz 四 Tab 市场复盘 + 主题报告 + 周报，盘后自动生成、随时回看。'),
        ],
        '典型场景｜早间 5 分钟扫隔夜要闻，盘后看 Daily Market Report 复盘主线与个股多空。')

    # ---------- P5 Feeds ----------
    module_page(prs, '02', 'Feeds', '多渠道智能订阅',
        '把散落各平台的关注源，被动、持续地同步到一个统一收件箱——不再怕漏看。',
        [
            ('多源统一订阅',
             'Twitter / X、YouTube、新闻、微博、知识星球、播客、RSS，一个界面统一管理与启停。'),
            ('定时自动抓取',
             'YouTube 每小时、Twitter 每 2 小时、新闻每 30 分钟自动同步，连续失败自动降级保护。'),
            ('跨用户内容复用',
             '同一频道同一条内容只入库一次，多人订阅自动合并、标签全局共享，不浪费 API 额度。'),
            ('AI 打标 + 智能日报',
             '抓取后自动 LLM 打标（行业 / Ticker / 概念），并按作者或频道两阶段生成当日精华日报。'),
        ],
        '典型场景｜跟踪 50+ 财经 KOL，每天早晨看平台生成的全网观点日报。')

    # ---------- P6 Upload Sources ----------
    module_page(prs, '03', 'Upload Sources', '私人投研资料库  ·  AI 深度分析',
        '一键把外部研报、财报、音视频、网页沉淀进自己的知识库——AI 自动命名、打标、归类、分析。',
        [
            ('多格式一键上传',
             'PDF / Word / Excel / 图片 / 音频 / 视频 / 网页 / 笔记 等 10+ 类输入，自动提取正文入库。'),
            ('四路并行 AI 自动化',
             '自动命名 + 自动打标 + 自动归类入文件夹 + 自动深度分析，四路 Celery 并行、零等待。'),
            ('向量语义检索 RAG',
             '长文档切块向量化入库，百万字电子书也能一句话问出"价值投资方法论在哪一章"。'),
            ('交易报告结构化',
             '自动识别 IB 活动账单 CSV，解析每笔成交，前端 K 线标注 + 复盘视图。'),
        ],
        '典型场景｜研报财报沉淀、电话会音频转写、网页剪藏、个人投研 Wiki。')

    # ---------- P7 Watchlist ----------
    module_page(prs, '04', 'Watchlist', '自选股研究终端',
        '以 EODHD 全球数据为底座，把行情、K 线、基本面、财报、资讯自动聚合到每一只关注的股票。',
        [
            ('全球多市场自选',
             '美股 / 港股 / A股统一管理，支持按行业分组、1–3 星优先级、拖拽排序。'),
            ('实时行情 + 多周期 K 线',
             '日 / 周 / 月 EOD 数据实时刷新，新加入自选股自动回拉 10 年历史 K 线。'),
            ('深度基本面 17 大类',
             '三大财务报表、估值、分析师评级、机构持仓、内部人交易、分红、技术指标，可导出 Excel。'),
            ('财报日历 + 资讯聚合',
             '财报 / IPO 日历提前预警，点开个股即可看到平台内全部相关研报与新闻。'),
        ],
        '典型场景｜持仓盯盘、财报季准备、新标的深度尽调。')

    # ---------- P8 Agent MCP ----------
    module_page(prs, '05', 'Agent MCP', '把平台能力装进你的 AI 助手',
        '通过 MCP 开放协议，把整个研究库以只读接口暴露给 Cursor / Claude Desktop 等 AI 工具。',
        [
            ('什么是 MCP',
             'AI 工具连接外部数据源的"通用插座"——配置一次，即可在你的 AI 助手里直接提问平台研究库。'),
            ('Feed 流多角度检索',
             '按时间 / 关键词 / Ticker / 人物 / 概念过滤，支持多角度并行搜索并跨组去重。'),
            ('语义 + 混合向量检索',
             '按"意思"而非字面检索研报段落，RRF 算法融合精准关键词与语义命中两类结果。'),
            ('报告随取随用',
             '日报 / 周报 / 主题报告一键取回，返回公开链接，浏览器免登录即开即看。'),
        ],
        '安全设计｜全链路只读 + PAT 个人令牌 + 细粒度作用域权限 + 独立速率限制。')

    # ---------- P9 技术亮点 ----------
    s = prs.slides.add_slide(prs.slide_layouts[6])
    page_bg(s); top_bar(s)
    tb, tf = textbox(s, 0.7, 0.55, 11.9, 0.7)
    para(tf, '技术亮点与差异化', 24, DEEP, bold=True, first=True)
    rect(s, 0.7, 1.3, 5.6, 0.012, ACCENT)
    tb, tf = textbox(s, 0.7, 1.45, 11.9, 0.4)
    para(tf, '支撑五大模块稳定、安全、低成本运行的底层工程能力', 13, TEXT_MID, first=True)
    techs = [
        ('统一 AI 框架',
         '系统级 + 用户级 Jinja2 模板、多用户 provider / model 配置、限流，兼顾一致性与个性化。'),
        ('向量 RAG 检索',
         '长文档切块 + 阿里云向量库 + RRF 混合排序，支持概念级提问与章节级定位。'),
        ('两阶段日报',
         'Phase A 全局共享 / Phase B 个性化拼接，同一内容跨用户复用，token 成本大幅可控。'),
        ('UTC 时区契约',
         '存储全 UTC、传输带时区、展示按用户时区，报告日期对齐北京自然日，杜绝跨时区错位。'),
        ('AI 原生接口',
         'MCP 工具自带决策树、渐进式精度与零结果诊断，让 AI 像研究员一样高效使用平台。'),
        ('全链路只读安全',
         '对外接口纯查询，PAT 令牌 + 作用域权限 + 速率限制，从架构上保证数据不被篡改外泄。'),
    ]
    xs = [0.7, 4.75, 8.8]; ys = [2.1, 4.55]; cw, chh = 3.8, 2.25
    i = 0
    for ry in ys:
        for rx in xs:
            title, desc = techs[i]
            card(s, rx, ry, cw, chh, title, desc, title_size=14.5, desc_size=11)
            i += 1
    footer_scene(s, '工程化的 AI：稳定、安全、成本可控，并能持续接入新的数据源与模型')

    # ---------- P10 投资者的一天 + 收尾 ----------
    s = prs.slides.add_slide(prs.slide_layouts[6])
    page_bg(s, DEEP)
    rect(s, 0, 0, EMU_W, 0.14, ACCENT)
    tb, tf = textbox(s, 0.85, 0.55, 11, 0.7)
    para(tf, '投资者的完整一天', 26, WHITE, bold=True, first=True)
    rect(s, 0.88, 1.3, 4.6, 0.014, ACCENT)
    # 时间轴
    line_y = 2.55
    rect(s, 1.4, line_y, 10.6, 0.03, RGBColor(0x33, 0x44, 0x5E))
    nodes = [
        ('06:30', '早间', '打开 Daily，Preset 只看 Twitter + News，5 分钟掌握隔夜要闻与 KOL 观点。'),
        ('09:30', '盘中', '搜索 Ticker 或人名，即时调出全部订阅源相关内容，就地深读不离开时间线。'),
        ('16:00', '盘后', 'Daily Market Report 四 Tab 复盘：主线叙事、个股多空、量化面板、宏观催化。'),
        ('深度', '研究', '上传研报 → MCP 在 Cursor 里提问 → 向量检索精确定位章节段落。'),
    ]
    nx = [2.05, 5.0, 7.95, 10.9]
    for (t, sub, desc), x in zip(nodes, nx):
        oval(s, x - 0.16, line_y - 0.14, 0.32, 0.32, ACCENT)
        tb, tf = textbox(s, x - 1.1, line_y + 0.3, 2.2, 0.4, anchor='mid')
        para(tf, t, 16, ACCENT, bold=True, first=True, align='center')
        para(tf, sub, 11, RGBColor(0x9F, 0xA9, 0xBC), align='center', space_after=0)
        rect(s, x - 1.15, 3.55, 2.3, 1.95, NAVY, line=RGBColor(0x33, 0x44, 0x5E), rounded=True)
        rect(s, x - 1.15, 3.55, 2.3, 0.08, ACCENT)
        tb, tf = textbox(s, x - 1.0, 3.75, 2.0, 1.7)
        para(tf, desc, 11.5, RGBColor(0xDD, 0xE2, 0xEC), first=True, space_after=0)
    # slogan
    rect(s, 0.85, 5.95, EMU_W - 1.7, 0.014, RGBColor(0x33, 0x44, 0x5E))
    tb, tf = textbox(s, 0.85, 6.15, EMU_W - 1.7, 0.7, anchor='mid')
    para(tf, '观合科技  ——  一个属于你的 AI 投研工作台',
         22, ACCENT, bold=True, first=True, align='center')
    tb, tf = textbox(s, 0.85, 6.95, EMU_W - 1.7, 0.4, anchor='mid')
    para(tf, '让信息过载时代的投资决策，更高效 · 更深度 · 更智能',
         13, RGBColor(0x9F, 0xA9, 0xBC), first=True, align='center')

    out = '观合科技_系统介绍.pptx'
    prs.save(out)
    print('已生成:', out, ' 共', len(prs.slides.__iter__.__self__._sldIdLst), '页')


if __name__ == '__main__':
    build()
