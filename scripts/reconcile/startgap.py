"""History a backfill would fetch only for retention to delete it again.

A plugin backfills from its configured start: START_DATE in the instance's config.env when
it has one, otherwise the start_time it wrote to data.watermark when the table was created
(plugins never update that column afterwards, so the config wins). Retention keeps the last
keep_days. Everything between the start and the cutoff is downloaded, written and deleted
again — and the database file it grew does not shrink back when the rows go.

Nothing here knows a particular plugin: a table belongs to the instance named in its
watermark row (`ingestor`). A table nobody has written yet has no such row -- the plugin
creates it on its first start, just before backfilling -- so for an instance that has
never run (or one that just gained a data source) the policy tables without a row are
checked against that instance's START_DATE instead, marked `assumed`. A gap only counts when the history thrown away would be more
than what is kept, and at least MIN_GAP_DAYS. A START_DATE set close to the window, as
upgrade-1.1.md recommends, therefore does not warn as soon as the cutoff moves on.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

MIN_GAP_DAYS = 30
MARGIN_DAYS = 7  # upgrade-1.1.md: START_DATE = today - keep_days - 7


@dataclass
class Gap:
    table: str
    instance: Optional[str]
    start: date
    source: str      # "START_DATE" (instance config) or "start_time" (data.watermark)
    keep_days: int
    cutoff: date
    fresh: bool      # no duck_time yet: the instance's next start backfills from `start`
    assumed: bool = False  # nobody has written the table yet; `instance` is only a candidate

    @property
    def days(self) -> int:
        return (self.cutoff - self.start).days

    @property
    def suggested_start(self) -> date:
        return self.cutoff - timedelta(days=MARGIN_DAYS)

    def describe(self) -> List[str]:
        """Three lines: the gap, what it costs, what to change (the table name is the caller's)."""
        where = f"duckport config {self.instance}" if self.instance else "写入这张表的插件的配置"
        fix = f"把起始日期（START_DATE）调到 {self.suggested_start} 或之后：{where}"
        if self.assumed:
            return [
                f"还没有实例写入过；若由实例 {self.instance} 写入，会从它的 START_DATE {self.start} 补起，"
                f"而 retention 只保留 {self.keep_days} 天（cutoff {self.cutoff}），相差 {self.days:,} 天",
                f"首次启动时这 {self.days:,} 天的数据会被下载、写入，再被 retention 删掉，删除后库文件不会变小",
                fix,
            ]
        src = f"实例 {self.instance} 的 START_DATE" if self.source == "START_DATE" else "data.watermark.start_time"
        when = (f"这张表还没有 duck_time，实例下次启动就会从 {self.start} 补起" if self.fresh
                else "下次补历史（loadhist、重建表）时")
        return [
            f"起始 {self.start}（{src}），retention 只保留 {self.keep_days} 天"
            f"（cutoff {self.cutoff}），相差 {self.days:,} 天",
            f"{when}，这 {self.days:,} 天的数据会被下载、写入，再被 retention 删掉，删除后库文件不会变小",
            fix,
        ]


def threshold(keep_days: int) -> int:
    return max(MIN_GAP_DAYS, keep_days)


def _as_date(value) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip().strip("\"'")[:10])
    except ValueError:
        return None


def gaps(policies: List[dict], watermark: Dict[str, dict], start_dates: Dict[str, str],
         today: Optional[date] = None, candidates: Iterable[str] = ()) -> List[Gap]:
    """Enabled policies whose table starts far before the retention cutoff.

    policies: rows of data.retention_policy; watermark: table -> row of data.watermark;
    start_dates: instance -> START_DATE from its config.env (absent when it has none);
    candidates: instances that may write a policy table nobody has written yet. Such a
    table is reported once per candidate with a START_DATE, marked `assumed`; a candidate
    without one is skipped, since only the plugin knows its default.
    """
    today = today or datetime.now(timezone.utc).date()
    out = []
    for p in policies:
        if not p.get("enabled"):
            continue
        table, keep = p["table_name"], int(p["keep_days"])
        cutoff = today - timedelta(days=keep)
        wm = watermark.get(table)
        if wm is None:
            for name in candidates:
                start = _as_date(start_dates.get(name))
                if start is not None:
                    out.append(Gap(table, name, start, "START_DATE", keep, cutoff, fresh=True, assumed=True))
            continue
        instance = wm.get("ingestor") or None
        start, source = _as_date(start_dates.get(instance)) if instance else None, "START_DATE"
        if start is None:
            start, source = _as_date(wm.get("start_time")), "start_time"
        if start is not None:
            out.append(Gap(table, instance, start, source, keep, cutoff, fresh=not wm.get("duck_time")))
    return sorted((g for g in out if g.days > threshold(g.keep_days)),
                  key=lambda g: (g.table, g.instance or ""))


def load(dp):
    """(policies, watermark) read straight from the two system tables."""
    policies = dp.query("SELECT table_name, keep_days, enabled FROM data.retention_policy")
    watermark = {r["table_name"]: r for r in dp.query(
        "SELECT table_name, ingestor, start_time, duck_time FROM data.watermark")}
    return policies, watermark
