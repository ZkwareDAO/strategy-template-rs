#!/usr/bin/env python3
"""check_doc_refs.py — 引用坐标与日志锚串的 CI 校验（第四步门禁）。

为什么需要它：仓库既有三门（fmt / clippy / test）都只看代码，而本仓大量文档
以「每处 `文件:行` 已核对」「锚串直接取自代码」为卖点。这类坐标会在**任意一次
上方编辑后静默作废**，三门一个都抓不到。2026-11-09 实测暴露三族失效：
  ① 坐标越界/歧义（design-package 13 处；retirement 引 :943 而文件 910 行）
  ② 日志锚前缀改名未同步（acceptance 14 处 SPI 前缀 → engine 前缀）
  ③ 漏检教训：日志字面量常与宏不在同一行（`info!(` 起于 N 行、字面量在 N+8 行），
     按"同一行含 info!"过滤会把**真实存在**的锚误判为不存在——故本脚本一律在源码
     全文里按字面量本体匹配，不分宏行。

两类校验
--------
A. 坐标（`path.ext:NNN` / `:NNN-MMM`）
   - 解析顺序：给定路径 → `crates/<给定路径>` → 全仓唯一同名文件；
   - 裸同名文件在仓内有多份 ⇒ **歧义即失败**（`manager.rs` 那类）；
   - 行号超出文件实际行数 ⇒ 失败。
B. 日志锚（仅对声明了 anchors 的文件启用）
   - 文中反引号内含中文、且非路径/配置键/参数名的串，必须在 `crates/**/*.rs`
     的源码文本中出现；不出现 ⇒ 失败（防止"照着 grep 抓不到东西"）。

豁免机制（显式、可 grep，不允许静默）
------------------------------------
- 行级：行内任意位置写 `refcheck:ignore` ⇒ 跳过该行（历史叙述、已删实现坐标）。
- 文件级：文件前 40 行内写 `refcheck:coords=off` 关闭坐标校验；写
  `refcheck:anchors=on` 才启用锚串校验。**退出时会打印关闭清单**，让债务可见。

用法
----
  python3 -I scripts/check_doc_refs.py                 # 全量，CI 用
  python3 -I scripts/check_doc_refs.py --report-only   # 只报告不判失败
  python3 -I scripts/check_doc_refs.py --max-details 100

输出为机器可读友好：每类一行摘要 + 逐条明细（最多 --max-details 条）。
"""

from __future__ import annotations

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 行内的 `path.ext:NNN[-MMM]`
COORD_RE = re.compile(
    r"([A-Za-z0-9_\-./一-鿿]*\.(?:rs|py|sh|toml|yaml|yml|md|csv|json))"
    r":(\d{1,5})(?:-(\d{1,5}))?"
)
BACKTICK_RE = re.compile(r"`([^`\n]{6,})`")
ANCHOR_CJK = re.compile(r"[一-鿿]")
# 反引号里这些形态不是日志锚：路径、CLI 参数、含赋值/标点的散文片段、带 <> 的模板占位
NOT_ANCHOR = re.compile(r"(\.(rs|py|sh|toml|yaml|yml|md|csv|json)|^--|/|:|=|，|。|、|[<>])")

IGNORE = "refcheck:ignore"
# 坐标后紧跟的「（`符号`）」声称——用于语义校验。注意闭反引号先于括号出现：
# 文档形态为「坐标行号 + 闭反引号 + 全角括号 + 符号名」，故先容忍反引号/空格再匹配括号。
CLAIM_RE = re.compile(r"^[`\s]*[（(]\s*`?([A-Za-z_][A-Za-z0-9_]{3,})`?")
OFF_COORDS = "refcheck:coords=off"
ON_ANCHORS = "refcheck:anchors=on"
OFF_ANCHORS = "refcheck:anchors=off"


def read(p: str) -> str:
    with open(os.path.join(ROOT, p), encoding="utf-8", errors="replace") as f:
        return f.read()


def iter_files(subdirs, suffixes):
    out = []
    for sub in subdirs:
        base = os.path.join(ROOT, sub)
        if not os.path.isdir(base):
            continue
        for dp, dns, fns in os.walk(base):
            dns[:] = [d for d in dns if d not in ("target", ".git", "node_modules")]
            for fn in fns:
                if any(fn.endswith(s) for s in suffixes):
                    out.append(os.path.relpath(os.path.join(dp, fn), ROOT))
    return sorted(out)


def basename_index():
    idx: dict[str, list[str]] = {}
    for rel in iter_files(["crates"], (".rs",)):
        idx.setdefault(os.path.basename(rel), []).append(rel)
    return idx


def make_line_counter():
    cache: dict[str, int] = {}

    def get(rel: str) -> int:
        if rel not in cache:
            try:
                with open(os.path.join(ROOT, rel), encoding="utf-8", errors="replace") as f:
                    cache[rel] = sum(1 for _ in f)
            except OSError:
                cache[rel] = -1
        return cache[rel]

    return get


def resolve(raw: str, idx, get):
    # 本仓文档引用惯例：`store/lib.rs` / `signals/manager.rs` 指 crates/<crate>/src/<file>
    cands = [raw, "crates/" + raw]
    parts = raw.split("/")
    if len(parts) >= 2:
        cands.append(f"crates/{parts[0]}/src/{'/'.join(parts[1:])}")
    for cand in cands:
        if get(cand) >= 0:
            return cand
    hits = idx.get(os.path.basename(raw), [])
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        # 裸名前缀匹配（`positions/manager.rs` 型简写）：限定同 crate 唯一命中
        if len(parts) >= 2:
            hits = [h for h in hits if f"/{parts[0]}/" in h]
            if len(hits) == 1:
                return hits[0]
        return "AMBIG:" + ", ".join(hits)
    return None


def comment_part(line: str) -> str:
    """.rs/.py/.sh 行只取注释段——字符串字面量里的伪坐标（形如 名字.csv:数字）是
    断言内容或示例文本，不是引用坐标。简化处理：从首个注释标记起算。"""
    for marker in ("//", "#"):
        i = line.find(marker)
        if i >= 0:
            return line[i:]
    return ""


def check_coords(path: str, idx, get, counter):
    txt = read(path)
    lines_cache: dict[str, list[str]] = {}
    for lineno, line in enumerate(txt.splitlines(), 1):
        if line_ignored(path, line):
            continue
        hay = comment_part(line) if path.endswith((".rs", ".py", ".sh")) else line
        if not hay:
            continue
        for m in COORD_RE.finditer(hay):
            raw, a, b = m.group(1), int(m.group(2)), m.group(3)
            hi = int(b) if b else a
            counter["refs"] += 1
            real = resolve(raw, idx, get)
            if real is None:
                counter["bad"] += 1
                yield f"{path}:{lineno}: 文件不可解析 `{raw}:{a}`"
            elif real.startswith("AMBIG:"):
                counter["bad"] += 1
                yield f"{path}:{lineno}: 裸名歧义 `{raw}:{a}` → {real[6:]}"
            else:
                n = get(real)
                if a < 1 or hi > n:
                    counter["bad"] += 1
                    yield f"{path}:{lineno}: 越界 `{real}:{a}-{hi}`（文件 {n} 行）"
                    continue
                # 语义校验：坐标后紧跟「（`符号`）」时，该符号必须真的出现在被引行内。
                # 动机（2026-11-09 实测）：删除 6 行代码就让 runtime.rs 下方的全部引用
                # 偏移，行号仍在上限内 ⇒ 纯边界检查放过，靠人才抓到。这条把它变成闸。
                for claim in CLAIM_RE.finditer(hay[m.end():m.end() + 40]):
                    sym = claim.group(1)
                    if sym.lower() in ("refcheck",):
                        continue
                    if real not in lines_cache:
                        try:
                            lines_cache[real] = read(real).splitlines()
                        except OSError:
                            lines_cache[real] = []
                    body = lines_cache[real]
                    window = " ".join(body[max(0, a - 1): min(len(body), hi + 1)])
                    if sym not in window:
                        counter["bad"] += 1
                        yield (
                            f"{path}:{lineno}: 语义错位 `{real}:{a}` 声称 `{sym}`，"
                            f"该行实为「{(body[a - 1][:44] if a <= len(body) else '…')}」"
                        )


def check_anchors(path: str, blob: str):
    txt = read(path)
    for lineno, line in enumerate(txt.splitlines(), 1):
        if line_ignored(path, line):
            continue
        for m in BACKTICK_RE.finditer(line):
            t = m.group(1).strip()
            if not ANCHOR_CJK.search(t) or NOT_ANCHOR.search(t) or len(t) < 6:
                continue
            stem = re.split(r"[（(；;]", t)[0].strip()
            if t in blob or (len(stem) >= 6 and stem in blob):
                continue
            yield f"{path}:{lineno}: 日志锚在代码中不存在 `{t[:44]}`"


def file_flags(path: str):
    """文件级开关。**只认独立指令行**：形如 `<!-- refcheck:coords=off ... -->` 且该行
    以 <!-- 开头。正文/表格里*提及*标记名（例如 CONTRIBUTING 解释用法）不得改变校验
    范围——否则文档一写用法就把自己关掉，属"改尺子"缺陷。非 markdown 文件：坐标开、
    锚串关。"""
    if not path.endswith(".md"):
        return True, False
    head = read(path).splitlines()[:40]
    dirs = set()
    for line in head:
        m = re.match(r"^<!--\s*refcheck:(coords=off|anchors=on|anchors=off)\b", line.strip())
        if m:
            dirs.add(m.group(1))
    return "coords=off" not in dirs, ("anchors=on" in dirs and "anchors=off" not in dirs)


def line_ignored(path: str, line: str) -> bool:
    """行级豁免的显式形式：markdown 必须写 `<!-- refcheck:ignore -->`（防止正文/表格
    里*提到*标记名就自我豁免）；代码文件在注释段内出现 `refcheck:ignore` 即可。"""
    if path.endswith(".md"):
        return re.search(r"<!--\s*refcheck:ignore\s*-->", line) is not None
    return IGNORE in comment_part(line)


def main() -> int:
    ap = argparse.ArgumentParser(description="文档引用坐标与日志锚 CI 校验")
    ap.add_argument("--report-only", action="store_true", help="只报告，不以失败退出")
    ap.add_argument("--max-details", type=int, default=60)
    ap.add_argument("--only", nargs="*", default=[], help="只校验这些路径（调试用）")
    args = ap.parse_args()

    idx = basename_index()
    get = make_line_counter()
    targets = args.only or (
        iter_files(["docs"], (".md",))
        + iter_files(["crates"], (".rs",))
        + iter_files(["scripts"], (".py", ".sh"))
        + (["README.md"] if os.path.isfile(os.path.join(ROOT, "README.md")) else [])
    )
    blob = "\n".join(read(p) for p in iter_files(["crates"], (".rs",)))

    coord_fail: list[str] = []
    anchor_fail: list[str] = []
    counter = {"refs": 0, "bad": 0}
    coords_off: list[str] = []
    anchors_on_list: list[str] = []
    n_coords = 0
    for p in targets:
        coords_on, anchors_on = file_flags(p)
        if not coords_on:
            coords_off.append(p)
        if coords_on:
            n_coords += 1
            coord_fail += list(check_coords(p, idx, get, counter))
        if anchors_on:
            anchors_on_list.append(p)
            anchor_fail += list(check_anchors(p, blob))

    print(f"校验文件 {len(targets)} 个（坐标启用 {n_coords}，锚串启用 {len(anchors_on_list)}）")
    print(f"坐标引用 {counter['refs']} 处 → 失效 {counter['bad']}；日志锚失效 {len(anchor_fail)}")
    for line in (coord_fail + anchor_fail)[: args.max_details]:
        print("  FAIL " + line)
    extra = len(coord_fail) + len(anchor_fail) - args.max_details
    if extra > 0:
        print(f"  …另有 {extra} 条未列出（--max-details 调大）")
    if anchors_on_list:
        print("锚串校验中的文件：" + ", ".join(anchors_on_list))
    if coords_off:
        print(f"⚠️ 坐标校验关闭清单（账本/历史文件，{len(coords_off)} 个）：")
        for p in coords_off:
            print("   OFF  " + p)

    if args.report_only:
        print("--report-only：忽略失败计数")
        return 0
    return 1 if (coord_fail or anchor_fail) else 0


if __name__ == "__main__":
    sys.exit(main())
