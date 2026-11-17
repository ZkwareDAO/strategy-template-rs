//! roll_build — 通用期货主力连续 1m 构造工具（不含策略专用元数据）
//!
//! 输入（外部原始归档契约；模板仓库内不含原始样本，格式取自 CTP 参考仓库
//! `crates/data/src/bin/spread_prep.rs` 头注释与解析逻辑）：
//!   `--in` 目录递归扫描文件名为 `{PRODUCT}{YYMM}.csv` 的合约文件（大小写不敏感，
//!   其它文件忽略）。两种已知布局均自动识别，任意嵌套深度均会扫描：
//!     1. 全生命周期：`2005-2024/SHFE/CU/CU2612.csv`
//!     2. 逐交易日：  `2025/202511/20251104/cu2612.csv`
//!   必需列头（名称大小写不敏感、列序任意，多余列忽略）：
//!     `symbol, bob(或 open_time), open, high, low, close, volume, position`
//!   - `position` = 持仓量(OI)，选主依据；**不得用 volume 代替**。
//!   - bob 时间戳：`%Y-%m-%d %H:%M:%S%:z` 或 RFC3339。
//!   - 行内 symbol 必须与文件名合约一致，否则报错（带文件与行号）。
//!   - OHLC 须为有限正数，volume/position 须为有限非负数，否则报错。
//!   - 去重按 (合约, 时刻)，先到先得；文件按路径字典序读取，结果确定。
//!
//! 选主（每个 CTP 交易日，`strategy_core::contract::ctp_trading_day`）：
//!   取各合约**当日最后一根 bar** 的持仓量；剔除 YYYYMM ≤ 当前交易月的合约
//!   （通用规则：进入交割月即剔除，不同交易所的个别规则未覆盖，无逐品种覆盖）；
//!   在持仓量 > 0 的候选中取最大者，持平取合约码字典序较小者。
//!   **因果口径**：D 日收盘观测到的 OI 只在 D 之后的下一个交易日生效（避免用
//!   收盘后才知道的信息回填当日盘中 bar）；首个观测日无生效主力，其 bar 不输出。
//!   注意：节假日表未实现，`ctp_trading_day` 仅按周末回落；某交易日若没有
//!   日盘 bar（可能是节假日伪交易日），会打印 warning。
//!
//! 输出：
//!   `{out}/1m/{PRODUCT}_CONT_1m.csv`：模板 10 列标准格式（经 `csv_store::save_csv`）。
//!     每个交易日仅取被选中主力合约的 bar；OHLCV 来自该合约同一 bar；
//!     quote_volume / trade_num / active_buy_* 恒为 0（原始输入无此列）。
//!     不做复权（`--backadjust none`），换月处存在价格断档。
//!   `{out}/roll_events.csv`：表头 `date,new_symbol,old_symbol`，每次主力切换一行，
//!     date 为新主力首次被选中的交易日；首个主力不计为事件。
//!     同一 `--out` 目录只能用于一个品种（roll_events.csv 会被覆盖）。
//!
//! 用法：
//!   cargo run -p strategy-data --bin roll_build -- \
//!     --in <dir> --product CU --out <dir> [--backadjust none]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Timelike, Utc};
use strategy_core::candle::Candle;
use strategy_core::contract::ctp_trading_day;
use strategy_data::csv_store::save_csv;

const REQUIRED_HEADERS: &str =
    "symbol, bob (or open_time), open, high, low, close, volume, position";

const USAGE: &str = "\
roll_build — build a main-continuous 1m series for one product

USAGE:
  roll_build --in <DIR> --product <PRODUCT> --out <DIR> [--backadjust none]

OPTIONS:
  --in <DIR>          Root directory, scanned recursively for <PRODUCT><YYMM>.csv files
  --product <CODE>    Product code, letters only, e.g. CU (case-insensitive)
  --out <DIR>         Output directory: 1m/<PRODUCT>_CONT_1m.csv and roll_events.csv
  --backadjust <MODE> none (default). ratio is NOT implemented and is rejected with an error.
  -h, --help          Show this help

INPUT (raw per-contract 1m CSV; external archive contract, not shipped with this template):
  Two layouts are recognised, any nesting depth is scanned:
    1. full lifecycle:  2005-2024/SHFE/CU/CU2612.csv
    2. daily:           2025/202511/20251104/cu2612.csv
  Only files named <PRODUCT><YYMM>.csv are read; other files are ignored.
  Required header columns (case-insensitive, any order, extra columns ignored):
    symbol, bob (or open_time), open, high, low, close, volume, position
  - position is open interest (OI) and is the selection key. volume is NOT used as OI.
  - bob: \"YYYY-MM-DD HH:MM:SS+08:00\" or RFC3339.
  - symbol must equal the file's contract code; a mismatch fails the run with file:line.
  - Duplicate (symbol, bob) rows: the first occurrence in sorted path order wins.
  - Non-finite or non-positive OHLC, negative volume/position: the run fails with file:line.

MAIN SELECTION (per CTP trading day; weekends only, no holiday table):
  - Rank contracts by position on their last bar of the day (position > 0).
  - Exclude contracts with YYYYMM <= current trading month (generic delivery rule).
  - Highest position wins; ties go to the smaller contract code.
  - CAUSAL: the close-of-day observation on day D takes effect on the NEXT trading
    day. The first observed day therefore has no effective main and contributes no
    bars. This avoids using information that was not yet known intraday.
  - If a trading day has no day-session bars (possible holiday pseudo-day under the
    weekend-only calendar), a warning is printed.

OUTPUT:
  <out>/1m/<PRODUCT>_CONT_1m.csv   10 columns: timestamp,open,high,low,close,volume,
                                   quote_volume,trade_num,active_buy_volume,active_buy_quote_volume
                                   (quote_volume, trade_num, active_buy_* are 0: not in input)
  <out>/roll_events.csv            header: date,new_symbol,old_symbol (one row per switch)
  Use a separate --out directory per product (roll_events.csv is overwritten).
";

// ---------------------------------------------------------------------------
// 选主纯函数
// ---------------------------------------------------------------------------

/// 当日选主：`rows` 为 (合约码, 该合约当日末根 bar 的持仓量)。
///
/// 剔除持仓量非正/非有限的候选，以及 YYYYMM ≤ `today` 所在交易月的合约（进入交割月即剔除），
/// 余下取持仓量最大者；持平取合约码字典序较小者。
pub fn pick_main<'a>(
    rows: impl Iterator<Item = (&'a str, f64)>,
    today: NaiveDate,
) -> Option<&'a str> {
    let current = (today.year(), today.month());
    rows.filter(|&(symbol, oi)| {
        oi.is_finite() && oi > 0.0 && yymm_of(symbol).is_some_and(|ym| ym > current)
    })
    .max_by(|a, b| a.1.total_cmp(&b.1).then_with(|| b.0.cmp(a.0)))
    .map(|(symbol, _)| symbol)
}

/// 合约码 → (年, 月)；取末 4 位为 YYMM，月须为 1..=12。
fn yymm_of(symbol: &str) -> Option<(i32, u32)> {
    let tail = symbol.len().checked_sub(4).and_then(|i| symbol.get(i..))?;
    if !tail.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let yy: i32 = tail[..2].parse().ok()?;
    let mm: u32 = tail[2..].parse().ok()?;
    (1..=12).contains(&mm).then_some((2000 + yy, mm))
}

/// 文件名主干 → 合约码（大写）。须为 `{product}` + 4 位 YYMM（月 1..=12）。
fn contract_of_filename(stem: &str, product: &str) -> Option<String> {
    let product = product.to_ascii_uppercase();
    let upper = stem.trim().to_ascii_uppercase();
    let tail = upper.strip_prefix(product.as_str())?;
    if tail.len() != 4 {
        return None;
    }
    yymm_of(&upper)?;
    Some(upper)
}

// ---------------------------------------------------------------------------
// 逐交易日、逐合约的末根持仓量
// ---------------------------------------------------------------------------

/// (交易日, 合约) → 该合约当日最大时刻 bar 的持仓量。
///
/// 同一 (合约, 时刻) 的重复观测先到先得（严格晚于已记录时刻才覆盖），与观测顺序无关。
#[derive(Debug, Default)]
pub struct DayBook {
    days: BTreeMap<NaiveDate, BTreeMap<String, (DateTime<Utc>, f64)>>,
    /// 该交易日是否出现过日盘 bar（09:00-15:00 北京时间）；用于识别
    /// 仅含夜盘 bar 的假日伪交易日并告警。
    day_session: BTreeMap<NaiveDate, bool>,
}

impl DayBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记录一根 bar 的 (交易日, 合约, 时刻, 持仓量)。
    pub fn observe(&mut self, day: NaiveDate, symbol: &str, t: DateTime<Utc>, oi: f64) {
        let slot = self.day_session.entry(day).or_insert(false);
        if is_day_session_bar(t) {
            *slot = true;
        }
        let contracts = self.days.entry(day).or_default();
        match contracts.get_mut(symbol) {
            Some(slot) => {
                if t > slot.0 {
                    *slot = (t, oi);
                }
            }
            None => {
                contracts.insert(symbol.to_string(), (t, oi));
            }
        }
    }

    /// 全部观测过的交易日（升序，含无合格候选的日子）。
    pub fn all_days(&self) -> impl Iterator<Item = &NaiveDate> {
        self.days.keys()
    }

    /// 只有夜盘 bar（无 09:00-15:00 日盘 bar）的交易日：周末日历无法识别节假日，
    /// 这些日子可能是伪交易日，调用方应告警。
    pub fn days_without_day_session(&self) -> Vec<NaiveDate> {
        self.day_session
            .iter()
            .filter(|(_, has_day)| !**has_day)
            .map(|(day, _)| *day)
            .collect()
    }

    /// 某交易日各合约的末根持仓量，按合约码排序。
    pub fn candidates(&self, day: NaiveDate) -> Vec<(&str, f64)> {
        self.days
            .get(&day)
            .map(|contracts| {
                contracts
                    .iter()
                    .map(|(symbol, &(_, oi))| (symbol.as_str(), oi))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 每个有候选的交易日选出的主力合约（无合格候选的日子不出现）。
    pub fn selection(&self) -> BTreeMap<NaiveDate, String> {
        self.days
            .keys()
            .filter_map(|&day| {
                pick_main(self.candidates(day).into_iter(), day)
                    .map(|symbol| (day, symbol.to_string()))
            })
            .collect()
    }
}

/// 是否日盘 bar（北京时间 09:00-14:59）。
fn is_day_session_bar(t: DateTime<Utc>) -> bool {
    let cst = FixedOffset::east_opt(8 * 3600).expect("UTC+8");
    (9..15).contains(&t.with_timezone(&cst).hour())
}

/// 因果选主：D 日收盘观测到的 OI 只在 D 之后的下一个交易日生效。
///
/// 逐交易日推进：当日生效主力 = 最近一次收盘观测（严格早于本日）得出的主力；
/// 首个观测日之前没有任何可用信息，故不产生生效映射（该日 bar 不进入连续序列）。
pub fn effective_selection(
    book: &DayBook,
    observed: &BTreeMap<NaiveDate, String>,
) -> BTreeMap<NaiveDate, String> {
    let mut out = BTreeMap::new();
    let mut last: Option<&String> = None;
    for day in book.all_days() {
        if let Some(main) = last {
            out.insert(*day, main.clone());
        }
        if let Some(main) = observed.get(day) {
            last = Some(main);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// 解析
// ---------------------------------------------------------------------------

/// 一根已校验的原始 1m bar。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawBar {
    pub t: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub position: f64,
}

/// 必需列在表头中的下标。
struct Columns {
    symbol: usize,
    bob: usize,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    volume: usize,
    position: usize,
}

impl Columns {
    fn resolve(headers: &csv::StringRecord, label: &str) -> anyhow::Result<Self> {
        let names: Vec<String> = headers
            .iter()
            .map(|h| {
                h.trim()
                    .trim_start_matches('\u{feff}')
                    .trim()
                    .to_ascii_lowercase()
            })
            .collect();
        let find = |aliases: &[&str]| names.iter().position(|h| aliases.contains(&h.as_str()));
        let slots = [
            ("symbol", find(&["symbol"])),
            ("bob|open_time", find(&["bob", "open_time"])),
            ("open", find(&["open"])),
            ("high", find(&["high"])),
            ("low", find(&["low"])),
            ("close", find(&["close"])),
            ("volume", find(&["volume"])),
            ("position", find(&["position"])),
        ];
        let missing: Vec<&str> = slots
            .iter()
            .filter(|(_, idx)| idx.is_none())
            .map(|(name, _)| *name)
            .collect();
        ensure!(
            missing.is_empty(),
            "{label}: missing required column(s) [{}]; required headers: {REQUIRED_HEADERS}",
            missing.join(", ")
        );
        let [symbol, bob, open, high, low, close, volume, position] =
            slots.map(|(_, idx)| idx.unwrap_or(usize::MAX));
        Ok(Columns {
            symbol,
            bob,
            open,
            high,
            low,
            close,
            volume,
            position,
        })
    }
}

/// bob 时间戳：`%Y-%m-%d %H:%M:%S%:z` 或 RFC3339，统一转 UTC。
fn parse_bob(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%:z")
        .or_else(|_| DateTime::parse_from_rfc3339(raw))
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// 有限浮点数（拒绝 NaN/inf）。
fn parse_num(raw: &str) -> Option<f64> {
    raw.parse::<f64>().ok().filter(|v| v.is_finite())
}

/// 解析单个合约文件（路径形式），逐行回调。
pub fn scan_contract_file(
    path: &Path,
    contract: &str,
    f: impl FnMut(RawBar),
) -> anyhow::Result<()> {
    let file = File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    scan_contract_reader(
        BufReader::new(file),
        &path.display().to_string(),
        contract,
        f,
    )
}

/// 解析合约 CSV（任意 Read），逐行回调。`label` 仅用于错误信息。
pub fn scan_contract_reader<R: Read>(
    reader: R,
    label: &str,
    contract: &str,
    mut f: impl FnMut(RawBar),
) -> anyhow::Result<()> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(reader);
    let headers = rdr
        .headers()
        .with_context(|| format!("{label}: cannot read CSV header"))?
        .clone();
    let c = Columns::resolve(&headers, label)?;
    for rec in rdr.records() {
        let rec = rec.with_context(|| format!("{label}: malformed CSV record"))?;
        let line = rec.position().map_or(0, |p| p.line());
        let loc = || format!("{label}:{line}");
        let get = |i: usize| rec.get(i).unwrap_or("").trim();

        ensure!(
            get(c.symbol).eq_ignore_ascii_case(contract),
            "{}: symbol {:?} does not match file contract {}",
            loc(),
            get(c.symbol),
            contract
        );
        let t = parse_bob(get(c.bob)).with_context(|| {
            format!(
                "{}: bad bob timestamp {:?} (expected 'YYYY-MM-DD HH:MM:SS+08:00' or RFC3339)",
                loc(),
                get(c.bob)
            )
        })?;
        let positive = |i: usize, name: &str| -> anyhow::Result<f64> {
            match parse_num(get(i)) {
                Some(v) if v > 0.0 => Ok(v),
                _ => bail!(
                    "{}: column {name} must be a positive finite number, got {:?}",
                    loc(),
                    get(i)
                ),
            }
        };
        let non_negative = |i: usize, name: &str| -> anyhow::Result<f64> {
            match parse_num(get(i)) {
                Some(v) if v >= 0.0 => Ok(v),
                _ => bail!(
                    "{}: column {name} must be a non-negative finite number, got {:?}",
                    loc(),
                    get(i)
                ),
            }
        };
        f(RawBar {
            t,
            open: positive(c.open, "open")?,
            high: positive(c.high, "high")?,
            low: positive(c.low, "low")?,
            close: positive(c.close, "close")?,
            volume: non_negative(c.volume, "volume")?,
            position: non_negative(c.position, "position")?,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 目录布局
// ---------------------------------------------------------------------------

/// 一个合约文件（合约码取自文件名）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractFile {
    pub contract: String,
    pub path: PathBuf,
}

/// 递归收集 `{product}{YYMM}.csv`，按路径字典序排序。
pub fn collect_contract_files(root: &Path, product: &str) -> anyhow::Result<Vec<ContractFile>> {
    let product = product.to_ascii_uppercase();
    let mut out = Vec::new();
    collect_into(root, &product, &mut out)?;
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

fn collect_into(dir: &Path, product: &str, out: &mut Vec<ContractFile>) -> anyhow::Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("cannot read directory {}", dir.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue; // 不跟随符号链接，避免环/越界扫描
        }
        let path = entry.path();
        if file_type.is_dir() {
            collect_into(&path, product, out)?;
            continue;
        }
        let is_csv = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("csv"));
        if !is_csv {
            continue;
        }
        let contract = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|stem| contract_of_filename(stem, product));
        if let Some(contract) = contract {
            out.push(ContractFile { contract, path });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 构造
// ---------------------------------------------------------------------------

/// 一次主力切换。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollEvent {
    pub date: NaiveDate,
    pub new_symbol: String,
    pub old_symbol: String,
}

/// 构造结果。
#[derive(Debug)]
pub struct Continuous {
    /// 按时间升序的主力 bar（仅被选中主力在其交易日内的 bar）。
    pub bars: Vec<Candle>,
    /// 主力切换事件（按日期升序）。
    pub events: Vec<RollEvent>,
    /// 交易日 → 选中的主力合约。
    pub selection: BTreeMap<NaiveDate, String>,
}

/// 两遍扫描构造主力连续序列。
///
/// 第一遍：全部文件 → `DayBook`（每日各合约末根持仓量）→ 当日收盘选主。
/// 因果映射：D 日收盘选出的主力在下一交易日生效（首个观测日无生效主力）。
/// 第二遍：重新扫描文件，仅保留每日**生效主力**的 bar；按时间稳定排序后去重（先到先得）。
pub fn build_continuous(files: &[ContractFile], product: &str) -> anyhow::Result<Continuous> {
    ensure!(
        !files.is_empty(),
        "no {product} contract files found (expected {product}YYMM.csv under the input directory)"
    );

    // 第一遍：每日各合约末根持仓量 → 当日收盘选主
    let mut book = DayBook::new();
    for file in files {
        scan_contract_file(&file.path, &file.contract, |bar| {
            book.observe(ctp_trading_day(bar.t), &file.contract, bar.t, bar.position);
        })?;
    }
    let observed = book.selection();
    ensure!(
        !observed.is_empty(),
        "no main contract could be selected for {product}: no contract has positive position outside the delivery-month exclusion"
    );
    // 周末日历无法识别节假日：仅含夜盘 bar 的交易日可能是伪交易日，显式告警。
    for day in book.days_without_day_session() {
        eprintln!(
            "roll_build: warning: trading day {day} has no day-session bars; \
             holiday calendar is weekend-only, so this may be a holiday pseudo-day"
        );
    }
    // 因果口径：D 日收盘 OI → 下一交易日生效（避免用收盘后信息回填当日盘中）
    let selection = effective_selection(&book, &observed);
    ensure!(
        !selection.is_empty(),
        "no effective main contract for {product}: need at least two observed trading days \
         (close-of-day D selects the main effective on the next trading day)"
    );

    // 第二遍：仅保留每日被选中主力的 bar，按时间稳定排序后去重（先到先得）
    let mut bars: Vec<Candle> = Vec::new();
    for file in files {
        scan_contract_file(&file.path, &file.contract, |bar| {
            let day = ctp_trading_day(bar.t);
            if selection
                .get(&day)
                .is_some_and(|main| *main == file.contract)
            {
                bars.push(Candle {
                    open_time: bar.t,
                    open: bar.open,
                    high: bar.high,
                    low: bar.low,
                    close: bar.close,
                    volume: bar.volume,
                    quote_volume: 0.0,
                    trade_num: 0,
                    active_buy_volume: 0.0,
                    active_buy_quote_volume: 0.0,
                    is_closed: true,
                });
            }
        })?;
    }
    bars.sort_by_key(|c| c.open_time);
    bars.dedup_by_key(|c| c.open_time);

    let mut events = Vec::new();
    let mut prev: Option<&str> = None;
    for (day, main) in &selection {
        if let Some(old) = prev {
            if old != main.as_str() {
                events.push(RollEvent {
                    date: *day,
                    new_symbol: main.clone(),
                    old_symbol: old.to_string(),
                });
            }
        }
        prev = Some(main.as_str());
    }
    Ok(Continuous {
        bars,
        events,
        selection,
    })
}

// ---------------------------------------------------------------------------
// 输出
// ---------------------------------------------------------------------------

/// 写出 `{out}/1m/{PRODUCT}_CONT_1m.csv` 与 `{out}/roll_events.csv`。
pub fn write_outputs(out: &Path, product: &str, c: &Continuous) -> anyhow::Result<()> {
    let bar_path = out.join("1m").join(format!("{product}_CONT_1m.csv"));
    save_csv(&bar_path, &c.bars).with_context(|| format!("cannot write {}", bar_path.display()))?;

    let events_path = out.join("roll_events.csv");
    let mut wtr = csv::Writer::from_path(&events_path)
        .with_context(|| format!("cannot write {}", events_path.display()))?;
    wtr.write_record(["date", "new_symbol", "old_symbol"])?;
    for e in &c.events {
        wtr.write_record([
            e.date.format("%Y-%m-%d").to_string(),
            e.new_symbol.clone(),
            e.old_symbol.clone(),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

/// 复权模式。当前仅实现 `none`；`ratio` 在参数解析阶段报错。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backadjust {
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub input: PathBuf,
    /// 大写品种码
    pub product: String,
    pub out: PathBuf,
    pub backadjust: Backadjust,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Help,
    Run(Args),
}

fn parse_backadjust(value: &str) -> Result<Backadjust, String> {
    match value {
        "none" => Ok(Backadjust::None),
        "ratio" => Err(
            "ratio backadjust not implemented: only --backadjust none is supported (ratio is rejected, not ignored)"
                .to_string(),
        ),
        other => Err(format!("unknown --backadjust value {other:?}: expected none")),
    }
}

/// 解析命令行参数（不含程序名）。
pub fn parse_args(argv: &[String]) -> Result<Command, String> {
    let mut input: Option<PathBuf> = None;
    let mut product: Option<String> = None;
    let mut out: Option<PathBuf> = None;
    let mut backadjust = Backadjust::None;

    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].as_str();
        if flag == "-h" || flag == "--help" {
            return Ok(Command::Help);
        }
        if !matches!(flag, "--in" | "--product" | "--out" | "--backadjust") {
            return Err(format!("unknown argument: {flag}"));
        }
        let value = argv
            .get(i + 1)
            .filter(|v| !v.starts_with("--"))
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag {
            "--in" => input = Some(PathBuf::from(value)),
            "--product" => product = Some(value.clone()),
            "--out" => out = Some(PathBuf::from(value)),
            _ => backadjust = parse_backadjust(value)?,
        }
        i += 2;
    }

    let input = input.ok_or("missing required --in <DIR>")?;
    let product = product.ok_or("missing required --product <CODE>")?;
    if product.is_empty() || !product.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(format!(
            "invalid --product {product:?}: expected letters only, e.g. CU"
        ));
    }
    let out = out.ok_or("missing required --out <DIR>")?;
    Ok(Command::Run(Args {
        input,
        product: product.to_ascii_uppercase(),
        out,
        backadjust,
    }))
}

fn run(args: &Args) -> anyhow::Result<()> {
    match args.backadjust {
        Backadjust::None => {}
    }
    ensure!(
        args.input.is_dir(),
        "input directory does not exist: {}",
        args.input.display()
    );
    let files = collect_contract_files(&args.input, &args.product)?;
    eprintln!(
        "roll_build: {} {} contract file(s) found",
        files.len(),
        args.product
    );
    let continuous = build_continuous(&files, &args.product)?;
    write_outputs(&args.out, &args.product, &continuous)?;
    eprintln!(
        "roll_build: {} bar(s), {} roll event(s) -> {}",
        continuous.bars.len(),
        continuous.events.len(),
        args.out.display()
    );
    Ok(())
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match parse_args(&argv) {
        Ok(Command::Help) => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Ok(Command::Run(args)) => match run(&args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("roll_build: error: {e:#}");
                ExitCode::FAILURE
            }
        },
        Err(msg) => {
            eprintln!("roll_build: {msg}\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod roll_build_tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone};

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    /// 2026 年 11 月某日 `h:m` 北京时间 → UTC。
    fn at(day: u32, h: u32, m: u32) -> DateTime<Utc> {
        FixedOffset::east_opt(8 * 3600)
            .unwrap()
            .with_ymd_and_hms(2026, 11, day, h, m, 0)
            .unwrap()
            .with_timezone(&Utc)
    }

    const HDR: &str = "symbol,bob,open,high,low,close,volume,position,extra\n";

    /// 行：(symbol, bob, close, position)；OHLC 同价，volume 固定 10。
    fn csv_text(rows: &[(&str, &str, f64, f64)]) -> String {
        let mut s = String::from(HDR);
        for (sym, bob, close, oi) in rows {
            s.push_str(&format!(
                "{sym},{bob},{close},{close},{close},{close},10,{oi},x\n"
            ));
        }
        s
    }

    fn write(root: &Path, rel: &str, text: &str) {
        let p = root.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, text).unwrap();
    }

    /// 两种布局混合的合成数据（均为日盘，交易日 = 自然日）：
    /// - 11-02 主力 CU2612（末根 OI 90 > CU2701 的 40）
    /// - 11-03 主力切为 CU2701（末根 OI 60 > CU2612 的 30；早盘 CU2612 的 100 会误导首根口径）
    /// - 11-04 CU2611 为交割月（OI 999 最大但剔除），主力 CU2701
    /// - 11-05 仅剩 CU2611 行 → 无合格候选，无 bar、无事件
    fn scenario(root: &Path) {
        write(
            root,
            "2005-2024/SHFE/CU/CU2612.csv",
            &csv_text(&[
                ("CU2612", "2026-11-02 09:00:00+08:00", 70000.0, 100.0),
                ("CU2612", "2026-11-02 09:01:00+08:00", 70010.0, 90.0),
                ("CU2612", "2026-11-03 09:00:00+08:00", 70100.0, 100.0),
                ("CU2612", "2026-11-03 09:01:00+08:00", 70110.0, 30.0),
            ]),
        );
        write(
            root,
            "2025-2026/SHFE/CU/CU2701.csv",
            &csv_text(&[
                ("CU2701", "2026-11-02 09:00:00+08:00", 70400.0, 50.0),
                ("CU2701", "2026-11-02 09:01:00+08:00", 70410.0, 40.0),
                ("CU2701", "2026-11-03 09:00:00+08:00", 70500.0, 10.0),
                ("CU2701", "2026-11-03 09:01:00+08:00", 70510.0, 60.0),
                ("CU2701", "2026-11-04 09:00:00+08:00", 70600.0, 5.0),
                ("CU2701", "2026-11-04 09:01:00+08:00", 70610.0, 5.0),
            ]),
        );
        write(
            root,
            "2026/202611/cu2611.csv",
            &csv_text(&[
                ("CU2611", "2026-11-04 09:00:00+08:00", 99999.0, 999.0),
                ("CU2611", "2026-11-05 09:00:00+08:00", 99998.0, 500.0),
            ]),
        );
    }

    fn scenario_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        scenario(dir.path());
        dir
    }

    fn build_scenario(root: &Path) -> Continuous {
        let files = collect_contract_files(root, "CU").unwrap();
        build_continuous(&files, "CU").unwrap()
    }

    // ---- 选主（任务指定名称） ----

    #[test]
    fn pick_main_uses_last_bar_oi() {
        let today = d(2026, 11, 4);
        let mut book = DayBook::new();
        // 先喂末根再喂早盘：验证与观测顺序无关，按时间取末根
        book.observe(today, "CU2612", at(4, 14, 59), 20.0);
        book.observe(today, "CU2701", at(4, 14, 59), 60.0);
        // 早盘 CU2612 持仓更大（若按首根/全天最大，会误选 CU2612）
        book.observe(today, "CU2612", at(4, 9, 0), 100.0);
        book.observe(today, "CU2701", at(4, 9, 0), 10.0);

        assert_eq!(
            book.candidates(today),
            vec![("CU2612", 20.0), ("CU2701", 60.0)]
        );
        assert_eq!(
            pick_main(book.candidates(today).into_iter(), today),
            Some("CU2701")
        );
    }

    #[test]
    fn pick_main_skips_delivery_month() {
        let rows = [
            ("CU2610", 900.0),
            ("CU2611", 800.0),
            ("CU2612", 300.0),
            ("CU2701", 200.0),
        ];
        // 2026-11-04 当月为 2611：2610/2611 均已进入或超过交割月，剔除
        assert_eq!(pick_main(rows.into_iter(), d(2026, 11, 4)), Some("CU2612"));
        // 对照：2026-10-30 当月为 2610，CU2611 此时尚未进入交割月，可选
        assert_eq!(pick_main(rows.into_iter(), d(2026, 10, 30)), Some("CU2611"));
    }

    #[test]
    fn pick_main_ignores_nonpositive_oi_and_breaks_ties_by_symbol() {
        let today = d(2026, 11, 4);
        let rows = [
            ("CU2612", 0.0),
            ("CU2701", 5.0),
            ("CU2705", 5.0),
            ("CU2709", f64::NAN),
        ];
        assert_eq!(pick_main(rows.into_iter(), today), Some("CU2701"));
        assert_eq!(pick_main([("CU2612", 0.0)].into_iter(), today), None);
    }

    // ---- 解析 ----

    #[test]
    fn parser_reports_missing_required_columns() {
        let no_position =
            "symbol,bob,open,high,low,close,volume\nCU2612,2026-11-02 09:00:00+08:00,1,1,1,1,1\n";
        let err = scan_contract_reader(no_position.as_bytes(), "a.csv", "CU2612", |_| {})
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("missing required column(s) [position]"),
            "{err}"
        );

        let no_bob = "symbol,open,high,low,close,volume,position\nCU2612,1,1,1,1,1,1\n";
        let err = scan_contract_reader(no_bob.as_bytes(), "a.csv", "CU2612", |_| {})
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("missing required column(s) [bob|open_time]"),
            "{err}"
        );
    }

    #[test]
    fn bom_prefixed_header_is_accepted() {
        let text = "\u{feff}symbol,bob,open,high,low,close,volume,position\n\
            CU2612,2026-11-02 09:00:00+08:00,1,1,1,1,1,1\n";
        let mut n = 0;
        scan_contract_reader(text.as_bytes(), "bom.csv", "CU2612", |_| n += 1).unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn parser_accepts_open_time_alias_and_both_timestamp_formats() {
        let text = "symbol,open_time,open,high,low,close,volume,position\n\
            CU2612,2026-11-02 09:00:00+08:00,1,1,1,1,1,1\n\
            CU2612,2026-11-02T09:01:00+08:00,1,1,1,1,1,1\n";
        let mut got = Vec::new();
        scan_contract_reader(text.as_bytes(), "t.csv", "CU2612", |b| got.push(b.t)).unwrap();
        assert_eq!(got, vec![at(2, 9, 0), at(2, 9, 1)]);
    }

    #[test]
    fn parser_rejects_symbol_mismatch_with_location() {
        let text = "symbol,bob,open,high,low,close,volume,position\n\
            CU2701,2026-11-02 09:00:00+08:00,1,1,1,1,1,1\n";
        let err = scan_contract_reader(text.as_bytes(), "a.csv", "CU2612", |_| {})
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("CU2701") && err.contains("CU2612") && err.contains("a.csv:2"),
            "{err}"
        );
    }

    #[test]
    fn parser_rejects_nonpositive_or_nonfinite_values() {
        let head = "symbol,bob,open,high,low,close,volume,position\n";
        let cases = [
            ("CU2612,2026-11-02 09:00:00+08:00,0,1,1,1,1,1\n", "open"),
            (
                "CU2612,2026-11-02 09:00:00+08:00,1,1,1,1,1,NaN\n",
                "position",
            ),
            (
                "CU2612,2026-11-02 09:00:00+08:00,1,1,1,1,1,-1\n",
                "position",
            ),
            ("CU2612,2026-11-02 09:00:00+08:00,1,1,1,1,-1,1\n", "volume"),
            ("CU2612,not-a-time,1,1,1,1,1,1\n", "bob"),
        ];
        for (row, field) in cases {
            let text = format!("{head}{row}");
            let err = scan_contract_reader(text.as_bytes(), "a.csv", "CU2612", |_| {})
                .unwrap_err()
                .to_string();
            assert!(err.contains(field), "{row}: {err}");
        }
    }

    // ---- 目录布局 / 文件名 ----

    #[test]
    fn layout_recognition_collects_both_layouts_only() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let body = csv_text(&[("CU2612", "2026-11-02 09:00:00+08:00", 1.0, 1.0)]);
        write(root, "2005-2024/SHFE/CU/CU2612.csv", &body);
        write(root, "2025/202511/20251104/cu2701.csv", &body);
        // 以下均应被忽略
        for junk in [
            "notes.txt",
            "CU8888.csv",
            "CU2613.csv",
            "CUP2612.csv",
            "rb2612.csv",
            "1m/CU_CONT_1m.csv",
            "CU2612.csv.tmp",
        ] {
            write(root, junk, "junk,not,a,contract\n");
        }

        let files = collect_contract_files(root, "CU").unwrap();
        let names: Vec<&str> = files.iter().map(|f| f.contract.as_str()).collect();
        assert_eq!(names, vec!["CU2612", "CU2701"]);
        assert_eq!(
            files[0].path.strip_prefix(root).unwrap(),
            Path::new("2005-2024/SHFE/CU/CU2612.csv")
        );
    }

    #[test]
    fn dedup_keeps_first_occurrence_in_sorted_path_order() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // 11-02 建立收盘观测（CU2612 为主力）；11-03 才进入生效序列。
        // 11-03 09:00 的重复行按排序路径先出现的文件胜出（2005-2024 目录在前）。
        write(
            root,
            "2005-2024/SHFE/CU/CU2612.csv",
            &csv_text(&[
                ("CU2612", "2026-11-02 09:00:00+08:00", 69990.0, 90.0),
                ("CU2612", "2026-11-03 09:00:00+08:00", 70000.0, 90.0),
                ("CU2612", "2026-11-03 09:01:00+08:00", 70010.0, 80.0),
            ]),
        );
        write(
            root,
            "2026/202611/20261103/cu2612.csv",
            &csv_text(&[("CU2612", "2026-11-03 09:00:00+08:00", 71111.0, 5.0)]),
        );
        write(
            root,
            "2025-2026/SHFE/CU/CU2701.csv",
            &csv_text(&[("CU2701", "2026-11-02 09:01:00+08:00", 70400.0, 10.0)]),
        );

        let c = build_scenario(root);
        let closes: Vec<f64> = c.bars.iter().map(|b| b.close).collect();
        assert_eq!(closes, vec![70000.0, 70010.0]);
    }

    // ---- 构造 ----

    #[test]
    fn build_selects_main_bars_only() {
        let dir = scenario_dir();
        let c = build_scenario(dir.path());

        // 因果口径：首日（11-02）无生效主力，不输出；11-03 生效 CU2612，
        // 11-04 生效 CU2701（11-03 收盘选出）。
        let closes: Vec<f64> = c.bars.iter().map(|b| b.close).collect();
        assert_eq!(closes, vec![70100.0, 70110.0, 70600.0, 70610.0]);

        let days: Vec<NaiveDate> = c.selection.keys().copied().collect();
        assert_eq!(days, vec![d(2026, 11, 3), d(2026, 11, 4), d(2026, 11, 5)]);
        let mains: Vec<&str> = c.selection.values().map(String::as_str).collect();
        assert_eq!(mains, vec!["CU2612", "CU2701", "CU2701"]);
    }

    #[test]
    fn roll_events_record_transitions() {
        let dir = scenario_dir();
        let c = build_scenario(dir.path());
        assert_eq!(
            c.events,
            vec![RollEvent {
                date: d(2026, 11, 4),
                new_symbol: "CU2701".to_string(),
                old_symbol: "CU2612".to_string(),
            }]
        );
    }

    #[test]
    fn write_outputs_emits_10_column_bars_and_roll_events() {
        let dir = scenario_dir();
        let c = build_scenario(dir.path());
        let out = tempfile::tempdir().unwrap();
        write_outputs(out.path(), "CU", &c).unwrap();

        let bars = fs::read_to_string(out.path().join("1m").join("CU_CONT_1m.csv")).unwrap();
        let mut lines = bars.lines();
        assert_eq!(
            lines.next().unwrap(),
            "timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume"
        );
        assert_eq!(
            lines.next().unwrap(),
            "2026-11-03 01:00:00+00:00,70100,70100,70100,70100,10,0,0,0,0"
        );
        assert_eq!(bars.lines().count(), 1 + 4);

        let ev = fs::read_to_string(out.path().join("roll_events.csv")).unwrap();
        let ev: Vec<&str> = ev.lines().collect();
        assert_eq!(
            ev,
            vec!["date,new_symbol,old_symbol", "2026-11-04,CU2701,CU2612"]
        );
    }

    // ---- 因果选主 / 夜盘交易日映射 ----

    #[test]
    fn causal_selection_lags_one_trading_day() {
        let mut book = DayBook::new();
        book.observe(d(2026, 11, 2), "CU2612", at(2, 9, 0), 100.0);
        book.observe(d(2026, 11, 3), "CU2701", at(3, 9, 0), 200.0);
        book.observe(d(2026, 11, 4), "CU2701", at(4, 9, 0), 200.0);
        let observed = book.selection();
        let effective = effective_selection(&book, &observed);
        // D 日收盘选主 → 下一交易日生效；首个观测日无映射
        assert_eq!(effective.get(&d(2026, 11, 2)), None);
        assert_eq!(effective.get(&d(2026, 11, 3)), Some(&"CU2612".to_string()));
        assert_eq!(effective.get(&d(2026, 11, 4)), Some(&"CU2701".to_string()));
    }

    #[test]
    fn night_session_bar_belongs_to_next_trading_day_via_build() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            root,
            "A/SHFE/CU/CU2612.csv",
            &csv_text(&[
                ("CU2612", "2026-11-02 09:00:00+08:00", 70000.0, 100.0),
                // 夜盘 21:00 → 交易日 11-03
                ("CU2612", "2026-11-02 21:00:00+08:00", 70050.0, 90.0),
            ]),
        );
        write(
            root,
            "B/SHFE/CU/CU2701.csv",
            &csv_text(&[
                ("CU2701", "2026-11-02 09:01:00+08:00", 70400.0, 10.0),
                ("CU2701", "2026-11-03 09:00:00+08:00", 70500.0, 200.0),
            ]),
        );
        let c = build_scenario(root);
        // 11-02 收盘选 CU2612 → 11-03 生效；11-02 21:00 的夜盘 bar 属于 11-03，
        // 随生效主力 CU2612 输出；11-03 收盘 CU2701 尚无后续交易日可生效。
        let bars: Vec<(i64, f64)> = c
            .bars
            .iter()
            .map(|b| (b.open_time.timestamp(), b.close))
            .collect();
        assert_eq!(bars, vec![(at(2, 21, 0).timestamp(), 70050.0)]);
        assert_eq!(
            c.selection.get(&d(2026, 11, 3)),
            Some(&"CU2612".to_string())
        );
        assert!(c.events.is_empty());
    }

    #[test]
    fn friday_night_bar_maps_to_monday() {
        assert_eq!(ctp_trading_day(at(6, 21, 0)), d(2026, 11, 9));
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            root,
            "A/SHFE/CU/CU2612.csv",
            &csv_text(&[
                ("CU2612", "2026-11-06 09:00:00+08:00", 70000.0, 100.0),
                // 周五夜盘 → 下周一交易日
                ("CU2612", "2026-11-06 21:00:00+08:00", 70050.0, 90.0),
            ]),
        );
        write(
            root,
            "B/SHFE/CU/CU2701.csv",
            &csv_text(&[
                ("CU2701", "2026-11-06 09:01:00+08:00", 70400.0, 10.0),
                ("CU2701", "2026-11-09 09:00:00+08:00", 70500.0, 200.0),
            ]),
        );
        let c = build_scenario(root);
        let bars: Vec<(i64, f64)> = c
            .bars
            .iter()
            .map(|b| (b.open_time.timestamp(), b.close))
            .collect();
        assert_eq!(bars, vec![(at(6, 21, 0).timestamp(), 70050.0)]);
        assert_eq!(
            c.selection.get(&d(2026, 11, 9)),
            Some(&"CU2612".to_string())
        );
    }

    #[test]
    fn days_without_day_session_flags_pseudo_days() {
        let mut book = DayBook::new();
        let night = at(3, 21, 0);
        book.observe(ctp_trading_day(night), "CU2612", night, 10.0);
        book.observe(d(2026, 11, 5), "CU2612", at(5, 9, 0), 10.0);
        // 11-03 21:00 属于 11-04，该交易日只有夜盘 bar
        assert_eq!(book.days_without_day_session(), vec![d(2026, 11, 4)]);
    }

    // ---- CLI ----

    fn argv(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_args_defaults_to_none_backadjust() {
        let got = parse_args(&argv(&[
            "--in",
            "/data/in",
            "--product",
            "cu",
            "--out",
            "/data/out",
        ]))
        .unwrap();
        assert_eq!(
            got,
            Command::Run(Args {
                input: "/data/in".into(),
                product: "CU".into(),
                out: "/data/out".into(),
                backadjust: Backadjust::None,
            })
        );
    }

    #[test]
    fn parse_args_rejects_ratio_backadjust() {
        let err = parse_args(&argv(&[
            "--in",
            "/i",
            "--product",
            "CU",
            "--out",
            "/o",
            "--backadjust",
            "ratio",
        ]))
        .unwrap_err();
        assert!(err.contains("ratio backadjust not implemented"), "{err}");
    }

    #[test]
    fn parse_args_rejects_unknown_backadjust_missing_flags_and_bad_product() {
        let err = parse_args(&argv(&[
            "--in",
            "/i",
            "--product",
            "CU",
            "--out",
            "/o",
            "--backadjust",
            "bogus",
        ]))
        .unwrap_err();
        assert!(err.contains("bogus"), "{err}");

        let err = parse_args(&argv(&["--in", "/i", "--product", "CU"])).unwrap_err();
        assert!(err.contains("--out"), "{err}");

        let err =
            parse_args(&argv(&["--in", "/i", "--product", "C_U", "--out", "/o"])).unwrap_err();
        assert!(err.contains("--product"), "{err}");
    }

    #[test]
    fn parse_args_help_flag_returns_help() {
        assert_eq!(parse_args(&argv(&["--help"])).unwrap(), Command::Help);
        assert_eq!(parse_args(&argv(&["-h"])).unwrap(), Command::Help);
    }

    #[test]
    fn usage_documents_required_headers_and_backadjust_policy() {
        assert!(USAGE.contains("position"));
        assert!(USAGE.contains("open_time"));
        assert!(USAGE.contains("--backadjust"));
        assert!(USAGE.contains("ratio is NOT implemented"));
    }
}
