#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电子书解析压力测试脚本
测试对象: "The Nvidia Way" PDF
输出: 解析时间、内存峰值、字符数、chunks 数量、向量化可行性评估
"""
import sys, os, time, gc
sys.path.insert(0, '/opt/guanhetech')

PDF = "/opt/guanhetech/he Nvidia Way_ Jensen Huang and the Making of a Tech Giant (2024, W. W. Norton & Company) - libgen.li.pdf"

# ─────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────
def hr(title=""):
    print(f"\n{'='*60}")
    if title:
        print(f"  {title}")
        print('='*60)

def mem_mb():
    """读取当前进程 RSS（MB）"""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        return 0.0

# ─────────────────────────────────────────────────────────────
# 1. 文件基本信息
# ─────────────────────────────────────────────────────────────
hr("1. 文件基本信息")
size_mb = os.path.getsize(PDF) / 1024 / 1024
print(f"  文件大小  : {size_mb:.1f} MB")

import pymupdf
doc = pymupdf.open(PDF)
page_count = doc.page_count
print(f"  总页数    : {page_count} 页")

# 抽样估算字符数（前10页 + 中间5页 + 末5页）
sample_idx = (
    list(range(0, min(10, page_count)))
    + list(range(max(0, page_count//2-2), min(page_count//2+3, page_count)))
    + list(range(max(0, page_count-5), page_count))
)
sample_idx = sorted(set(sample_idx))
sample_chars = sum(len(doc[i].get_text()) for i in sample_idx)
avg_per_page = sample_chars / len(sample_idx)
est_total_chars = int(avg_per_page * page_count)
est_tokens = int(est_total_chars * 0.4)  # 英文约 0.35-0.5 token/char
doc.close()

print(f"  每页均字符: {avg_per_page:.0f}")
print(f"  预估总字符: {est_total_chars:,}（约 {est_total_chars//10000} 万字）")
print(f"  预估 tokens: {est_tokens:,}（英文 ~0.4 token/char）")

# 模型上下文对比
print(f"\n  模型上下文容量参考:")
print(f"    qwen-turbo  128K tokens  → 容纳 {128000/max(est_tokens,1)*100:.0f}% 全书内容")
print(f"    qwen3-max   128K tokens  → 容纳 {128000/max(est_tokens,1)*100:.0f}% 全书内容")
print(f"    qwen-long   1M  tokens   → 容纳 {1000000/max(est_tokens,1)*100:.0f}% 全书内容")
print(f"  （超出 100% 表示无法一次性塞入模型）")

# ─────────────────────────────────────────────────────────────
# 2. Quick Parse (全量一次性) — 计时 + 内存峰值
# ─────────────────────────────────────────────────────────────
hr("2. Quick Parse 全量解析（当前生产方式）")

gc.collect()
mem_before = mem_mb()
t0 = time.time()

import pymupdf4llm
md_full = pymupdf4llm.to_markdown(PDF)

elapsed_full = time.time() - t0
mem_after = mem_mb()
chars_full = len(md_full)
lines_full = md_full.count('\n')

print(f"  解析耗时  : {elapsed_full:.1f} 秒")
print(f"  内存峰值  : {mem_after - mem_before:.0f} MB（进程增量）")
print(f"  输出字符数: {chars_full:,}（约 {chars_full//10000} 万字）")
print(f"  输出行数  : {lines_full:,}")

# 前 300 字符预览
print(f"\n  内容开头预览（前300字）:")
print("  " + md_full[:300].replace('\n', '\n  '))

# ─────────────────────────────────────────────────────────────
# 3. 分页流式解析 — 每批 50 页
# ─────────────────────────────────────────────────────────────
hr("3. 分页流式解析（建议方案，每批 50 页）")

BATCH = 50
gc.collect()
mem_before2 = mem_mb()
t1 = time.time()

doc2 = pymupdf.open(PDF)
total_pages = doc2.page_count
doc2.close()

batches = []
peak_batch_mem = 0
for start in range(0, total_pages, BATCH):
    end = min(start + BATCH, total_pages)
    pages = list(range(start, end))
    t_batch = time.time()
    chunk_md = pymupdf4llm.to_markdown(PDF, pages=pages)
    batch_elapsed = time.time() - t_batch
    cur_mem = mem_mb()
    peak_batch_mem = max(peak_batch_mem, cur_mem - mem_before2)
    batches.append({
        "pages": f"{start+1}-{end}",
        "chars": len(chunk_md),
        "elapsed": batch_elapsed,
    })
    print(f"  批次 {start//BATCH+1:3d}  页 {start+1:4d}-{end:4d}  {len(chunk_md):>8,} chars  {batch_elapsed:.1f}s")
    del chunk_md
    gc.collect()

elapsed_stream = time.time() - t1
total_chars_stream = sum(b["chars"] for b in batches)

print(f"\n  总耗时         : {elapsed_stream:.1f} 秒")
print(f"  批次内存峰值   : {peak_batch_mem:.0f} MB（单批增量，可释放）")
print(f"  总字符数       : {total_chars_stream:,}")
print(f"  全量 vs 分页   : 全量 {elapsed_full:.1f}s / {mem_after-mem_before:.0f}MB  ←→  "
      f"分页 {elapsed_stream:.1f}s / {peak_batch_mem:.0f}MB/批")

# ─────────────────────────────────────────────────────────────
# 4. 分块（chunking_service）评估
# ─────────────────────────────────────────────────────────────
hr("4. 分块（RAG chunking）评估")

from backend.services.chunking_service import chunk_text
t2 = time.time()
chunks = chunk_text(md_full, file_type="pdf")
elapsed_chunk = time.time() - t2

chunk_lens = [len(c.text) for c in chunks]
chapters = list({c.chapter for c in chunks if c.chapter})

print(f"  分块耗时    : {elapsed_chunk:.2f} 秒")
print(f"  总 chunks 数: {len(chunks)}")
print(f"  平均 chunk  : {sum(chunk_lens)//max(len(chunk_lens),1)} chars")
print(f"  最短 chunk  : {min(chunk_lens)} chars")
print(f"  最长 chunk  : {max(chunk_lens)} chars")
print(f"  识别章节数  : {len(chapters)}（H1/H2 标题）")
if chapters[:10]:
    print(f"  前10章标题  : {chapters[:10]}")

# ─────────────────────────────────────────────────────────────
# 5. 向量化成本估算
# ─────────────────────────────────────────────────────────────
hr("5. 向量化成本估算（Qwen text-embedding-v3）")

total_embed_tokens = sum(c.token_count for c in chunks)
# qwen text-embedding-v3: ~0.0007 RMB / 1000 tokens
embed_cost = total_embed_tokens / 1000 * 0.0007

print(f"  总 embedding tokens: {total_embed_tokens:,}")
print(f"  预估向量化成本    : {embed_cost:.4f} RMB（text-embedding-v3 @ 0.0007/1k）")

# 对比: 一次性塞入 qwen3-max 全文分析
qwen3_input_cost = est_tokens / 1000 * 0.004   # qwen3-max input ~0.004/1k
qwen3_output_cost = 2000 / 1000 * 0.016        # 输出约 2000 tokens
print(f"\n  对比: 一次性 qwen3-max 全文分析（若模型能塞下）:")
print(f"    input tokens  {est_tokens:,} × 0.004/1k = {qwen3_input_cost:.2f} RMB")
print(f"    output ~2k    tokens × 0.016/1k = {qwen3_output_cost:.3f} RMB")
print(f"    合计                             = {qwen3_input_cost + qwen3_output_cost:.2f} RMB（每次重分析）")

# ─────────────────────────────────────────────────────────────
# 6. Map-Reduce 摘要成本估算
# ─────────────────────────────────────────────────────────────
hr("6. Map-Reduce 章节摘要成本估算（qwen-turbo）")

TARGET_CHUNK_CHARS = 600
SUMMARY_OUTPUT_TOKENS = 300
# map 阶段：每 chunk ~600 chars → ~240 tokens input，300 tokens output
map_input = total_embed_tokens
map_output = len(chunks) * SUMMARY_OUTPUT_TOKENS
# qwen-turbo: 0.0003/1k input, 0.0006/1k output
map_cost = map_input / 1000 * 0.0003 + map_output / 1000 * 0.0006
# reduce 阶段：把所有 chapter_summary 拼接（上限32k tokens）→ qwen-turbo
reduce_input_tokens = min(len(chapters) * SUMMARY_OUTPUT_TOKENS, 32000)
reduce_cost = reduce_input_tokens / 1000 * 0.0003 + 1000 / 1000 * 0.0006
print(f"  Map  ({len(chunks)} chunks × ~{SUMMARY_OUTPUT_TOKENS} output tokens):")
print(f"    input  {map_input:,} tokens × 0.0003/1k = {map_input/1000*0.0003:.4f} RMB")
print(f"    output {map_output:,} tokens × 0.0006/1k = {map_output/1000*0.0006:.4f} RMB")
print(f"    subtotal = {map_cost:.4f} RMB")
print(f"  Reduce (合并 {len(chapters)} 章摘要 → 全书概览):")
print(f"    input  ~{reduce_input_tokens:,} tokens × 0.0003/1k = {reduce_input_tokens/1000*0.0003:.4f} RMB")
print(f"    output ~1k tokens × 0.0006/1k = {0.0006:.4f} RMB")
print(f"    subtotal = {reduce_cost:.4f} RMB")
total_mr_cost = map_cost + reduce_cost + embed_cost
print(f"\n  向量化 + Map-Reduce 合计: {total_mr_cost:.4f} RMB")
print(f"  vs 一次性全文塞入:        {qwen3_input_cost + qwen3_output_cost:.2f} RMB")
print(f"  节省:                     {(qwen3_input_cost + qwen3_output_cost)/max(total_mr_cost,0.0001):.0f}x")

# ─────────────────────────────────────────────────────────────
# 7. Ebook 判定阈值建议
# ─────────────────────────────────────────────────────────────
hr("7. Ebook 判定阈值建议")
print(f"  本书实测:")
print(f"    页数: {page_count}  字符: {chars_full:,}")
print(f"  建议阈值 (满足其一即认定为 ebook-class):")
print(f"    EBOOK_MIN_PAGES  = 80  (本书: {'超过 ✓' if page_count > 80 else '未超过'})")
print(f"    EBOOK_MIN_CHARS  = 100000  (本书: {'超过 ✓' if chars_full > 100000 else '未超过'})")
print(f"  建议 hard limit (避免 OOM):")
print(f"    MAX_PAGES_HARD_LIMIT = 2000  (本书: {page_count} 页, 在限制内 ✓)")

hr("测试完成")
print()
