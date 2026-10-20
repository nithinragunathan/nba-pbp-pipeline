"""Cross-source event matching.

Every source event is assigned a cluster_id: the set of source rows that describe the same
real event on the court. Two steps per game:

1. Score anchors. A made shot or free throw is the only event that changes the score, and the
   score only goes up, so "the event that made it 45-41 in period 2" is the same event in every
   source no matter what the clock says. Those pairs are fixed first.
2. Sequence alignment. Between two consecutive anchors each source has a short run of misses,
   rebounds, turnovers and fouls. The runs are aligned the way `diff` aligns two files: an
   order-preserving pairing that maximizes a similarity score (same event type, same player,
   close clock), where skipping an event is allowed. Order matters: a miss, an offensive rebound,
   a putback miss and a second rebound can all happen within two seconds, which defeats a plain
   clock window but not an alignment.

Anything left unpaired becomes a singleton cluster ("only one source saw this").

NBA is the pivot: cluster ids are built from the NBA event id when there is one, so they stay the
same across reruns. `clock_window_match` is the simple baseline used in the evaluation.
"""
from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass

import pandas as pd

SCORING_CLASSES = ("shot", "free_throw")


@dataclass(frozen=True)
class Weights:
    base: float = 3.0
    same_player: float = 2.0
    different_player: float = -1.5
    both_team_level: float = 1.0          # e.g. two team rebounds
    one_team_level: float = -0.5
    same_result: float = 1.0
    different_result: float = -2.0
    same_value: float = 0.5
    same_rebound_type: float = 0.5
    per_clock_second: float = -0.25
    min_score: float = 0.5


W = Weights()


def _similarity(a: dict, b: dict, max_clock_tenths: int) -> float | None:
    """How alike two events are, or None when they cannot be the same event."""
    if a["event_class"] != b["event_class"] or a["period"] != b["period"]:
        return None
    dclock = abs(a["clock_tenths"] - b["clock_tenths"])
    if dclock > max_clock_tenths:
        return None
    if a["team_key"] and b["team_key"] and a["team_key"] != b["team_key"]:
        return None
    s = W.base + W.per_clock_second * dclock / 10
    pa, pb = a["player_key"], b["player_key"]
    if pa and pb:
        s += W.same_player if pa == pb else W.different_player
    elif not pa and not pb:
        s += W.both_team_level
    else:
        s += W.one_team_level
    if a["event_class"] in SCORING_CLASSES:
        s += W.same_result if a["is_made"] == b["is_made"] else W.different_result
        if a["shot_value"] == b["shot_value"]:
            s += W.same_value
    if a["ft_number"] and b["ft_number"] and a["ft_number"] != b["ft_number"]:
        return None                       # "1 of 2" is never "2 of 2"
    if a["event_class"] == "rebound" and a["rebound_type"] == b["rebound_type"]:
        s += W.same_rebound_type
    return s if s >= W.min_score else None


def align(xs: list[dict], ys: list[dict], max_clock_tenths: int) -> list[tuple[int, int, float]]:
    """Order-preserving pairing of xs and ys with the highest total similarity (gaps are free)."""
    n, m = len(xs), len(ys)
    if n == 0 or m == 0:
        return []
    best = [[0.0] * (m + 1) for _ in range(n + 1)]
    move = [[0] * (m + 1) for _ in range(n + 1)]   # 0 = skip x, 1 = skip y, 2 = pair
    sims: dict[tuple[int, int], float] = {}
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            skip_x, skip_y = best[i - 1][j], best[i][j - 1]
            if skip_x >= skip_y:
                best[i][j], move[i][j] = skip_x, 0
            else:
                best[i][j], move[i][j] = skip_y, 1
            s = _similarity(xs[i - 1], ys[j - 1], max_clock_tenths)
            if s is not None and best[i - 1][j - 1] + s > best[i][j]:
                best[i][j], move[i][j] = best[i - 1][j - 1] + s, 2
                sims[(i - 1, j - 1)] = s
    pairs = []
    i, j = n, m
    while i > 0 and j > 0:
        if move[i][j] == 2:
            pairs.append((i - 1, j - 1, sims[(i - 1, j - 1)]))
            i, j = i - 1, j - 1
        elif move[i][j] == 0:
            i -= 1
        else:
            j -= 1
    return pairs[::-1]


def _longest_increasing(pairs: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Keep the largest subset of (pivot_pos, other_pos) pairs that is ordered in both sources."""
    pairs = sorted(pairs)
    tails: list[int] = []
    tail_idx: list[int] = []
    prev = [-1] * len(pairs)
    for k, (_, pos) in enumerate(pairs):
        t = bisect_left(tails, pos)
        if t == len(tails):
            tails.append(pos)
            tail_idx.append(k)
        else:
            tails[t] = pos
            tail_idx[t] = k
        prev[k] = tail_idx[t - 1] if t > 0 else -1
    out, k = [], tail_idx[-1] if tail_idx else -1
    while k >= 0:
        out.append(pairs[k])
        k = prev[k]
    return out[::-1]


def _different_players(a: dict, b: dict) -> bool:
    return bool(a["player_key"] and b["player_key"] and a["player_key"] != b["player_key"])


def _score_anchors(xs: list[dict], ys: list[dict], max_clock_tenths: int,
                   strict: bool = True) -> list[tuple[int, int]]:
    """Pair made shots and free throws on the score after the event.

    The key also carries the scoring team and the points, and the pair must pass the normal
    similarity check (clock within the limit). Score alone is not enough: when one feed's running
    score is briefly off (seen in real data: ESPN two points behind NBA for part of a quarter),
    the score after a Gobert dunk in one feed can equal the score after a Middleton three in the
    other. Requiring team and points makes such a collision impossible. Two different players are
    never anchored either: the NBA feed can carry a later score on free throws it logs out of
    order (a technical between two shots), so 'the free throw that made it 96-94' is Queta's in
    one feed and Pritchard's in the other. `strict=False` keys on the score alone; it exists only
    so the evaluation can show the difference."""
    def keyed(evs):
        seen: dict[tuple, list[int]] = {}
        for i, e in enumerate(evs):
            if e["event_class"] in SCORING_CLASSES and e["is_made"]:
                key = (e["period"], e["score_home"], e["score_away"])
                if strict:
                    key += (e["event_class"], e["team_key"], e["shot_value"])
                seen.setdefault(key, []).append(i)
        return {k: v[0] for k, v in seen.items() if len(v) == 1}   # ambiguous keys are left to alignment
    kx, ky = keyed(xs), keyed(ys)
    pairs = [(kx[k], ky[k]) for k in kx.keys() & ky.keys()
             if not strict or (_similarity(xs[kx[k]], ys[ky[k]], max_clock_tenths) is not None
                               and not _different_players(xs[kx[k]], ys[ky[k]]))]
    return _longest_increasing(pairs)


def match_pair(xs: list[dict], ys: list[dict], max_clock_tenths: int,
               strict_anchors: bool = True) -> list[tuple[int, int, str, float | None]]:
    """Match two sources' events for one game. Both lists must be in game order.
    Returns (x_index, y_index, method, score) for every matched pair."""
    anchors = _score_anchors(xs, ys, max_clock_tenths, strict_anchors)
    out: list[tuple[int, int, str, float | None]] = [(i, j, "score_anchor", None) for i, j in anchors]
    bounds = [(-1, -1)] + anchors + [(len(xs), len(ys))]
    for (i0, j0), (i1, j1) in zip(bounds, bounds[1:]):
        out += _align_by_class(xs, ys, range(i0 + 1, i1), range(j0 + 1, j1), max_clock_tenths)
    # Safety net: an event logged at the same clock as an anchor can sit on opposite sides of it
    # in the two sources. Align whatever is still unpaired, period by period.
    done_x = {i for i, *_ in out}
    done_y = {j for _, j, *_ in out}
    for period in sorted({e["period"] for e in xs} | {e["period"] for e in ys}):
        left_x = [i for i, e in enumerate(xs) if i not in done_x and e["period"] == period]
        left_y = [j for j, e in enumerate(ys) if j not in done_y and e["period"] == period]
        out += _align_by_class(xs, ys, left_x, left_y, max_clock_tenths)
    return out


def _align_by_class(xs, ys, idx_x, idx_y, max_clock_tenths):
    """Align each event class on its own. Only the order within a class carries information
    (two rebounds in a row); a foul and a rebound can never be the same event, so their
    relative order should not constrain the alignment."""
    out = []
    for cls in {xs[i]["event_class"] for i in idx_x}:
        cx = [i for i in idx_x if xs[i]["event_class"] == cls]
        cy = [j for j in idx_y if ys[j]["event_class"] == cls]
        for a, b, s in align([xs[i] for i in cx], [ys[j] for j in cy], max_clock_tenths):
            out.append((cx[a], cy[b], "seq_align", s))
    return out


def clock_window_match(xs: list[dict], ys: list[dict], window_tenths: int = 20) -> list[tuple[int, int, str, float | None]]:
    """Baseline: same period, type and team, nearest clock within a window, greedy in pivot order."""
    by_bucket: dict[tuple, list[int]] = {}
    for j, b in enumerate(ys):
        by_bucket.setdefault((b["period"], b["event_class"]), []).append(j)
    used: set[int] = set()
    out = []
    for i, a in enumerate(xs):
        best_j, best_d = None, None
        for j in by_bucket.get((a["period"], a["event_class"]), []):
            b = ys[j]
            if j in used or (a["team_key"] and b["team_key"] and a["team_key"] != b["team_key"]):
                continue
            d = abs(a["clock_tenths"] - b["clock_tenths"])
            if d <= window_tenths and (best_d is None or d < best_d):
                best_j, best_d = j, d
        if best_j is not None:
            used.add(best_j)
            out.append((i, best_j, "clock_window", None))
    return out


EVENT_COLUMNS = ["source", "source_event_id", "game_key", "period", "clock_tenths", "seq_in_period",
                 "event_class", "is_made", "shot_value", "ft_number", "rebound_type", "player_key", "team_key",
                 "score_home", "score_away"]


def _records(df: pd.DataFrame) -> list[dict]:
    """Events in game order: period, then game clock (counting down), then the source's own
    sequence. Clock comes before sequence because ESPN sometimes appends late-entered plays out
    of order (a foul at 5:22 listed before a turnover at 5:49); sequence still breaks ties within
    the same tenth of a second, which is where alignment matters."""
    df = df.assign(_neg_clock=-df["clock_tenths"]).sort_values(["period", "_neg_clock", "seq_in_period"])
    df = df.astype(object).where(df.notna(), None)
    return df[EVENT_COLUMNS].to_dict("records")


def match_events(events: pd.DataFrame, max_clock_diff_s: float = 12, pivot: str = "nba",
                 other: str = "espn", matcher=match_pair) -> pd.DataFrame:
    """One output row per input event: cluster_id, match_method, clock_diff (other - pivot, tenths)."""
    max_tenths = int(max_clock_diff_s * 10)
    rows = []
    for game_key, g in events.groupby("game_key", sort=True):
        xs = _records(g[g["source"] == pivot])
        ys = _records(g[g["source"] == other])
        kwargs = {} if matcher is clock_window_match else {"max_clock_tenths": max_tenths}
        pairs = matcher(xs, ys, **kwargs)
        paired_y = {}
        for i, j, method, score in pairs:
            a, b = xs[i], ys[j]
            cid = f"{game_key}:{pivot[0].upper()}{a['source_event_id'].rsplit('-', 1)[-1]}"
            diff = b["clock_tenths"] - a["clock_tenths"]
            rows.append((cid, game_key, pivot, a["source_event_id"], a["event_class"], method, diff, score))
            rows.append((cid, game_key, other, b["source_event_id"], b["event_class"], method, diff, score))
            paired_y[j] = True
        paired_x = {i for i, _, _, _ in pairs}
        for i, a in enumerate(xs):
            if i not in paired_x:
                cid = f"{game_key}:{pivot[0].upper()}{a['source_event_id'].rsplit('-', 1)[-1]}"
                rows.append((cid, game_key, pivot, a["source_event_id"], a["event_class"], "singleton", None, None))
        for j, b in enumerate(ys):
            if j not in paired_y:
                cid = f"{game_key}:{other[0].upper()}{b['source_event_id']}"
                rows.append((cid, game_key, other, b["source_event_id"], b["event_class"], "singleton", None, None))
    out = pd.DataFrame(rows, columns=["cluster_id", "game_key", "source", "source_event_id", "event_class",
                                      "match_method", "clock_diff", "align_score"])
    out["clock_diff"] = out["clock_diff"].astype("Int64")
    out["align_score"] = out["align_score"].astype("Float64")
    return out
