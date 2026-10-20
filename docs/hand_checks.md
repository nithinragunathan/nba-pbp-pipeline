# Hand-checked disagreements

The Wizards' 82 games of 2025-26 are checked case by case below. The league-wide build has 293
NBA clusters in 169 games where the feeds disagree; they were reviewed by pattern, in
[League-wide review](#league-wide-review) at the end. The WNBA is ESPN only, so it has nothing to
compare.

Every place where NBA and ESPN disagree, or only one of them recorded an event, across the 82
Wizards games: **19 cases out of about 33,350 events per source.** One (#5) turned out to be a
mapping bug and is fixed, so `marts.rpt_disagreements`, regenerated on every run, now lists 18.

Each case was read from the two play descriptions and the plays around it (a first pass drafted
with Claude Code). The notes say what kind of discrepancy it is, not which feed is
right: that needs video or the official box score.

What the cases show:

- **Neither feed is always right.** ESPN contradicts itself twice (#3: made flag vs its own score;
  #1: text vs structured player id). NBA has events ESPN lacks (#10-11, #13-16).
- **Most disagreements are about credit, not existence.** The matcher paired the events; the feeds
  differ on who gets the credit. That is why gold keeps an agreement flag (`agree_player`) instead
  of silently taking one side.
- **Some gaps are structural.** Only NBA flags dead-ball rebounds.

| # | Game | Time left | Disagreement | NBA | ESPN | Read |
|---|---|---|---|---|---|---|
| 1 | 20251030_WAS_at_OKC | Q2 1:28.0 | player (turnover) | B. Coulibaly bad pass TURNOVER (2 TO) | Bilal Coulibaly bad pass (Luguentz Dort steals) | ESPN contradicts itself: the text says Coulibaly, the structured participant id is Alex Sarr (1 of 1,453 steal plays). The pipeline uses ids, so it surfaces here. NBA is consistent. |
| 2 | 20251105_WAS_at_BOS | Q3 11:08.0 | team_vs_player_rebound (rebound) | J. Champagnie REBOUND (Off:1 Def:2) | Wizards defensive team rebound | NBA credits Champagnie, ESPN a Wizards team rebound. NBA's player credit is the official stat. |
| 3 | 20251107_CLE_at_WAS | Q2 8:15.0 | result (free_throw) | MISS T. Johnson Free Throw 1 of 2 | Tre Johnson makes free throw 1 of 2 | ESPN contradicts itself: the text and made flag say made, but ESPN's own running score does not move. NBA's MISS agrees with both running scores (game ends 114-148). ESPN's header says 115. Recorded in `known_data_issues`. |
| 4 | 20251107_CLE_at_WAS | Q3 0:50.8 | player (shot) | MISS J. Allen 9' driving Hook | Donovan Mitchell misses 9-foot two point shot | Missed 9-footer credited to Jarrett Allen (NBA) vs Donovan Mitchell (ESPN). Shooter attribution differs. |
| 5 | 20251201_MIL_at_WAS | Q2 5:38.0 | nba_only (turnover) | G. Antetokounmpo 10-second-freethrow-shooter TURNOVER (2 TO) | — | **Resolved.** ESPN does record it, as an untyped turnover (type 61, "No Turnover"), which the seed used to drop as `ignore`. It is mapped to `turnover` now and the two pair, so this case no longer appears in `rpt_disagreements`. |
| 6 | 20251201_MIL_at_WAS | Q4 1:23.0 | team_vs_player_rebound (rebound) | TEAM offensive REBOUND | Marvin Bagley III offensive rebound | NBA says team offensive rebound, ESPN credits Bagley. Same pattern as #9, other direction from #2. |
| 7 | 20260102_BKN_at_WAS | Q4 8:25.0 | player (rebound) | B. Coulibaly REBOUND (Off:3 Def:4) | Alex Sarr offensive rebound | Two rebounds 1.9 s apart credited to different Wizards in each feed (Coulibaly, Carrington vs Sarr, Coulibaly). Looks like an attribution shifted by one in one feed; needs video. |
| 8 | 20260102_BKN_at_WAS | Q4 8:06.0 | player (rebound) | B. Carrington REBOUND (Off:0 Def:4) | Bilal Coulibaly defensive rebound | Second of the pair in #7. |
| 9 | 20260106_ORL_at_WAS | Q4 4:23.0 | team_vs_player_rebound (rebound) | TEAM defensive REBOUND | Alex Sarr defensive rebound | The reverse: NBA says team rebound, ESPN credits Sarr. |
| 10 | 20260124_WAS_at_CHA | Q2 7:15.0 | nba_only (rebound) | B. Miller REBOUND (Off:1 Def:1) | — | A tip-in miss and the rebound before it exist only in NBA; ESPN's neighbouring plays all match. Likely an ESPN omission. |
| 11 | 20260124_WAS_at_CHA | Q2 7:14.0 | nba_only (shot) | MISS B. Miller tip Layup | — | Second of the pair in #10. |
| 12 | 20260129_MIL_at_WAS | Q4 6:59.0 | player (turnover) | A. Green bad pass TURNOVER (1 TO) | Ryan Rollins bad pass turnover (Kyshawn George steals) | Bad pass credited to A. Green (NBA) vs Ryan Rollins (ESPN), both Bucks, same steal by Kyshawn George. |
| 13 | 20260201_SAC_at_WAS | Q1 10:50.0 | nba_only (shot) | MISS M. Bagley III putback Layup - blocked | — | Blocked putback and its rebound exist only in NBA. Same pattern as #10-11. |
| 14 | 20260201_SAC_at_WAS | Q1 10:49.0 | nba_only (rebound) | M. Bagley III REBOUND (Off:2 Def:0) | — | Second of the pair in #13. |
| 15 | 20260203_NYK_at_WAS | Q3 1:10.0 | nba_only (shot) | MISS J. Clarkson 11' fadeaway Shot | — | Miss plus the rebound after it, only in NBA. Same pattern. |
| 16 | 20260203_NYK_at_WAS | Q3 1:08.0 | nba_only (rebound) | K. Towns REBOUND (Off:2 Def:11) | — | Second of the pair in #15. |
| 17 | 20260211_WAS_at_CLE | Q4 4:50.0 | player (turnover) | T. Proctor bad pass TURNOVER (1 TO) | Keon Ellis bad pass turnover (Keshon Gilbert steals) | Bad pass credited to T. Proctor (NBA) vs Keon Ellis (ESPN) on the same steal. Teammate credit differs, as in #12. |
| 18 | 20260219_IND_at_WAS | Q3 0:31.0 | team_vs_player_rebound (rebound) | J. Walker REBOUND (Off:2 Def:8) | Pacers offensive team rebound | NBA credits Jarace Walker, ESPN a Pacers team rebound. Same pattern as #2. |
| 19 | 20260319_DET_at_WAS | Q2 7:06.0 | player (shot) | MISS A. Thompson driving floating Shot | Caris LeVert misses driving floating jump shot | Missed floater credited to Amen Thompson (NBA) vs Caris LeVert (ESPN). Shooter attribution differs, as in #4. |

## League-wide review

Every 2025-26 NBA game in both feeds, reviewed on 2026-10-19 by disagreement type and then by
pattern, not case by case. **293 NBA clusters in 169 games (out of about 492,000 events per
source).** The NBA Cup final, which only ESPN has, is left out: every event in it is single-source
by definition.

The review found two pipeline bugs, both in `event_type_map` and both fixed:

- **Double fouls.** NBA logs a double foul as two `personal` (or `technical`) fouls with
  descriptor `double`; the seed ignores double fouls, but only matched NBA's rare
  `double personal` sub-type. 58 NBA rows had no ESPN partner. `stg_nba` now types them
  `foul:double personal`, so both feeds ignore them.
- **ESPN's "No Turnover" (type 61) is an untyped turnover,** like "No Shot (Default Shot)" for
  shots. It was mapped to `ignore`; it now pairs with NBA's untyped turnovers and 10-second
  free-throw violations (#5 above). ESPN's "No Foul" (46) is likely an untyped foul too, but all
  three rows are in LAC at CHI's degraded 4th quarter (known issue 14) and one has no team or
  player, so it stays `ignore`.

What remains is real disagreement between the feeds:

| Pattern | NBA clusters (approx.) | Read |
|---|---|---|
| Credit to a different player, nearly always a teammate (rebound, shot, turnover, free throw, foul) | 95 | Needs video. Gold takes NBA's credit and keeps `agree_player = false`. |
| ESPN's text names NBA's player, but its structured participant id is a teammate | 14 | ESPN contradicts itself, as in #1; NBA is consistent with ESPN's own text. |
| A miss only one feed logged, plus the rebound after it | 60 | One-feed omissions, as in #10-16. Includes last-second shots at 0.0 that NBA doesn't record at all (no row before "Period End") while ESPN has the shot and a team rebound. |
| Same play logged as a shot in one feed and a turnover in the other | 9 pairs | ESPN: blocked or missed layup; NBA: lost ball (often out of bounds). Different event classes can't pair. |
| Team rebound credited to opposite teams | 8 pairs | NBA "TEAM offensive REBOUND" vs ESPN "<other team> defensive team rebound". The matcher never pairs across teams, so each side is a singleton. |
| Player vs team rebound | 18 | As #2, #6, #9. |
| 2 vs 3 points on a miss | 16 | No effect on points. Several are clear ESPN errors (NBA "25' pullup 3PT" vs ESPN "4-foot jumper"; a blocked dunk as a 25-foot three); the rest are 21-23 ft shots. Gold takes NBA's value. |
| Known ESPN data issues (DEN at MIN, LAC at CHI, OKC at NOP) | about 40 | Already in `seeds/known_data_issues.csv`. |

Single cases worth noting:

- **PHI at NOP, Q2 6:26** (2026-02-21): Bey's technical free throw and Queen's free throw 1 of 2
  are cross-paired, because the feeds order the two differently and alignment allows a different
  shooter. Both are made, so points are unaffected. A small matcher weakness.
- **CHI at CLE, Q3** (2025-11-08): one Vučević miss is logged 14 s apart, just past the matcher's
  12 s limit, so it appears as two singletons.
- **MIN at SAC, OT 0:19.2** (2025-11-24): ESPN logs a Conley "turnover" where NBA has his take
  foul.
