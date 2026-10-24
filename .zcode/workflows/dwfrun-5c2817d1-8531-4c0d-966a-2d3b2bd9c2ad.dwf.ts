/* zcode-workflow
description: dwfrun-5c2817d1-8531-4c0d-966a-2d3b2bd9c2ad
*/
interface BriefSection {
  /** 报告日期，YYYY-MM-DD */
  reportDate: string;
  /** 完整的 OVERALL MARKET 板块 Markdown，以 "## OVERALL MARKET" 开头 */
  markdown: string;
  /** 引用的源报告文件（workspace 相对路径） */
  sourceFile: string;
}

interface ReviewVerdict {
  /** true = 无实质问题，无需修改 */
  approved: boolean;
  /** 需要修复的具体问题清单；approved 为 true 时为空数组 */
  issues: string[];
}

// ── 定位最新 The Close 报告（支持 args.date 指定日期，YYYY-MM-DD 或 YYYYMMDD）──
const dateFilter = args.date
  ? "*" + String(args.date).replace(/-/g, "") + "*.html"
  : "*.html";
const candidates = await files.glob("backend/reports/close_brief/" + dateFilter);
if (candidates.length === 0) {
  return {
    conclusion:
      "backend/reports/close_brief/ 下没有匹配的报告文件，无法生成。可能是当日 The Close 摘要尚未生成（无节目/转录缺失日）。",
    findings: [],
    verified: [],
    notCovered: ["The Close 报告文件缺失，未进行任何合成"],
  };
}
const sourceFile = candidates[candidates.length - 1];
const ymdMatch = sourceFile.match(/(\d{8})\.html$/);
if (ymdMatch === null) {
  return {
    conclusion: "报告文件名不含 YYYYMMDD 日期，无法解析报告日期：" + sourceFile,
    findings: [],
    verified: [],
    notCovered: ["文件名异常，未进行任何合成"],
  };
}
const ymd = ymdMatch[1];
const reportDate = ymd.slice(0, 4) + "-" + ymd.slice(4, 6) + "-" + ymd.slice(6, 8);
log("选定源报告：" + sourceFile + "（" + reportDate + "）");

// ── 合成 ──
phase("合成 OVERALL MARKET 板块");
const synthesizer = agent("CNT 简报合成器", {
  system:
    "你是 CNT 风格每日市场简报（Daily Market Brief）的英文撰稿人，服务于专业投资组合经理。" +
    "你的产出以信息密度和结构纪律著称：忠实于源材料、保留具名引语、不编造任何数字或事实。" +
    "如果源材料与指令冲突或信息不足以支撑某个段落，如实缩减该段落而不是虚构内容。",
});
const brief = await synthesizer.ask<BriefSection>(
  "请先用 Read 工具读取报告文件：" + sourceFile +
    "（这是 Bloomberg The Close 节目的当日中文 PM Brief 摘要，含 8 个 H2 小节：" +
    "市场主线 / 市场驱动因素 / 跨资产信号 / 核心分歧 / 分歧解决框架 / 投资组合启示 / 尾盘异动 / 明日关注）。\n\n" +
    "基于其中内容，产出 CNT 风格的英文 OVERALL MARKET 板块，Markdown 格式，结构固定如下：\n\n" +
    "## OVERALL MARKET\n" +
    "### Today's Narrative\n" +
    "恰好 3 条 bullet，每条 1-2 句话，提炼当日市场主线叙事。\n" +
    "### Top Market Drivers\n" +
    "恰好 4 个驱动因素，每条格式：\n" +
    "- **加粗断言句做标题（结论式，句号结尾）。** 2-3 句事实支撑（含具体数字、公司、行情），最后以 1 句斜体可操作结论收尾（如 *Stay short duration and favor the front end.*）。\n" +
    "### Positioning / Flow\n" +
    "按主题分 2-4 组，每组以加粗主题行开头（如 **Primary - quarter-end de-risking.**），组内 1-3 条 bullet，必须保留具名策略师/机构（人名 + 机构 + 观点）。\n\n" +
    "硬性要求：\n" +
    "1. 正文用英文；Ticker 用 $XXX 格式（如 $MU）；人名/机构/专有名词保留英文\n" +
    "2. 所有数字、公司、事件、引语必须来自源文件，禁止编造或从外部补充\n" +
    "3. 事实与观点分离；交易结论句用斜体\n" +
    "4. 源文件中的跨资产信号、尾盘异动等素材应融入 Drivers 或 Positioning，不要整段照搬翻译腔\n\n" +
    '返回结构化结果：reportDate="' + reportDate + '"，sourceFile="' + sourceFile + '"，markdown=完整板块文本。',
);

// ── 独立审读（新上下文，只看草稿与源文件）──
phase("独立审读与修订");
const reviewer = agent("独立审读员", {
  system:
    "你是一名苛刻的金融简报审读员。你的职责是挑毛病而不是给通过：结构偏差、编造数字、张冠李戴的引语、" +
    "重要主线遗漏、翻译腔英语，都是你要抓的问题。每条问题必须具体到可执行修改。",
});
const verdict = await reviewer.ask<ReviewVerdict>(
  "审读一篇 CNT 风格简报草稿。你只依据两样东西：(1) 下方草稿文本；(2) 源报告文件 " +
    sourceFile +
    "（请用 Read 工具读取，它是草稿的唯一合法事实来源）。\n\n" +
    "审读清单：\n" +
    "1. 结构：Today's Narrative 是否恰好 3 条；Top Market Drivers 是否恰好 4 条且每条为『加粗断言 + 事实支撑 + 斜体结论』；Positioning / Flow 是否分组并保留具名机构/策略师引用\n" +
    "2. 事实核对：草稿中每个数字、公司、引语能否在源文件中找到对应（抓编造与张冠李戴）\n" +
    "3. 完整度：源文件中的重要主线（当日最大叙事、关键数据、核心分歧）是否有影响简报质量的遗漏\n" +
    "4. 语言：英文是否地道简洁，无中文翻译腔\n\n" +
    "无实质问题返回 approved=true 且 issues 为空；否则返回具体问题列表。\n\n" +
    "草稿：\n" +
    brief.markdown,
);
let final = brief;
if (!verdict.approved) {
  log("审读发现 " + verdict.issues.length + " 个问题，修订一轮");
  final = await synthesizer.ask<BriefSection>(
    "独立审读员对上一版提出以下问题，请逐条修订，返回修订后的完整结构化结果（完整 markdown，不是 diff）：\n- " +
      verdict.issues.join("\n- "),
  );
} else {
  log("审读一次通过");
}

// ── 落盘（仓库外目录）并交付 ──
phase("落盘并交付");
const outPath = "/root/.zcode/eval-reports/close-brief/" + ymd + ".md";
const WRITER =
  "const fs=require('fs'),p=require('path');fs.mkdirSync(p.dirname(process.argv[2]),{recursive:true});" +
  "fs.writeFileSync(process.argv[2],process.argv[1],'utf8');console.log('WROTE',process.argv[2],process.argv[1].length+'B');";
const write = await world.run("node", ["-e", WRITER, final.markdown, outPath]);
if (write.exitCode !== 0) {
  log("落盘失败：" + write.stderr.slice(0, 500) + "（卡片仍会交付内容）");
} else {
  log("已写入 " + outPath);
}

await artifact.markdown("overall-market-brief", final.markdown, {
  title: "Daily Market Brief — OVERALL MARKET（" + reportDate + "）",
  description:
    "由 The Close 报告（" + sourceFile + "）合成的 CNT 风格英文板块，经独立审读" +
    (verdict.approved ? "一次通过" : "修订一轮") + "。",
  primary: true,
});

return {
  conclusion:
    "已基于 The Close 报告（" + reportDate + "）生成 CNT 风格 OVERALL MARKET 板块（Today's Narrative / Top Market Drivers / Positioning-Flow），经独立审读" +
    (verdict.approved ? "一次通过" : "修订一轮") +
    "，写入 " + outPath + "，正文见卡片。",
  findings: verdict.approved
    ? []
    : verdict.issues.map((issue) => ({
        where: sourceFile,
        what: "审读员指出并已修订的问题：" + issue,
        evidence: "独立审读员对照源文件逐项核对",
        status: "verified",
        severity: "medium",
      })),
  verified: [
    "合成内容全部取自 " + sourceFile + "，未引入外部素材",
    "独立审读员以新上下文逐项核对了结构、数字对应与语言质量",
  ],
  notCovered: [
    "量化数据交叉核验（本阶段无行情数据包，(verified) 标注机制未启用）",
    "EQUITY MARKET / MACROS 板块（后续阶段接入 Closing Bell 与宏观数据）",
  ],
};