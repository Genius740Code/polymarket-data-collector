# NEW-BOX MIGRATION PROMPT — read this first, do everything in order

## 0. Machine (operator does this in the Cloud Console, not you)

- **Instance: `n2d-standard-4` (4 vCPU, 16 GB AMD), region `us-central1` (Iowa),
  50 GB balanced persistent disk, Ubuntu 24.04 LTS.** (~$99/mo + ~$3 disk;
  $300 free-trial credit ≈ 3 months. Do NOT use e2-micro free tier — 1 GB RAM
  cannot run this. Do NOT use Spot for the collector — preemptions cause gaps.)
- Why N2D: the collector pins CPU 50–100% 24/7 (E2 burstable throttles sustained
  load); N2D is ~13% cheaper than Intel N2 at equal-or-better pandas throughput.
- Sizing math (measured 2026-09-08): 14 lanes ≈ 420 MB steady, 35 lanes ≈ 1 GB;
  backfill cron spikes to 1.4 GB; OS+agents ≈ 1 GB → 16 GB comfortable, 8 GB thin.

## 1. Goal (unchanged)

Run **5m + 15m + 1h + 4h on all 7 assets** (BTC/ETH/SOL/HYPE/BNB/XRP/DOGE),
**no data leaks, no fabricated data, honest NULLs/gaps**. **1d stays OFF**:
no up/down daily series exists on Gamma (the daily product is the price-touch
ladder — a different market type, out of scope until the operator says otherwise).

## 2. Setup (fresh Ubuntu — run verbatim)

```bash
sudo apt update && sudo apt install -y git python3-venv python3-pip nodejs npm
git clone https://github.com/Genius740Code/polymarket-data-collector.git \
  && cd polymarket-data-collector
git log --oneline -3          # expect d9ca7bd or newer on master
python3 -m venv .venv && .venv/bin/pip install -e ".[dev,kaggle]"
npm install -g pm2 && pm2 --version
cp config/collector.example.yaml config/collector.yaml   # ONLY if collector.yaml missing
.venv/bin/python -m pytest tests/ -q   # GATE: expect 147 passed, else STOP and report
```

## 3. Credentials + data migration (from the OLD box — operator assists)

⛔ NEVER paste secret VALUES into chat, git, or this file — transfer FILES only:

```bash
# run on the NEW box (old-box reachable as oldbox):
scp -p oldbox:~/.kaggle/kaggle.json oldbox:~/.kaggle/access_token ~/.kaggle/ 2>/dev/null || \
  scp -pr oldbox:~/.kaggle/ ~/.kaggle/
scp -p oldbox:~/.netrc ~/ 2>/dev/null; scp -p oldbox:~/.opencode.env ~/ 2>/dev/null
chmod 600 ~/.kaggle/* ~/.netrc ~/.opencode.env 2>/dev/null
```
- What lives where on the old box: `~/.kaggle/kaggle.json` (+`access_token`) =
  Kaggle API; `~/.netrc` = GitHub auth; `~/.opencode.env` holds ONE line
  `OPENCODE_SERVER_PASSWORD=...` (OpenCode web login — keep it, do not regenerate
  unless you want a new password). There is NO repo `.env` and NO Alchemy key on
  the old box — C2 on-chain uses the free public RPC, nothing to copy.
- Verify: `.venv/bin/python -c "from polymarket_collector.storage.export import _validate_kaggle_config; print(_validate_kaggle_config())"` → True.
- Hive continuity (else Kaggle history resets): on OLD box
  `pm2 stop polymarket-collector polymarket-resolution-backfill`, then
  `rsync -az oldbox:~/polymarket-collector/data/ ./data/` (~1–2 GB, 48 h window).
- ⚠️ **Two collectors must NEVER upload to the same Kaggle slugs.**
  Old-box uploads stop BEFORE the new box's first hourly upload.

## 3b. OpenCode web + Cloudflare tunnel (remote access, mirrors the old box)

```bash
curl -fsSL https://opencode.ai/install | bash   # → ~/.opencode/bin/opencode
npm install -g pm2
# tunnel binary (pick one):
sudo dpkg -i /tmp/cloudflared.deb   # from https://github.com/cloudflare/cloudflared/releases (amd64 .deb)
# same working dir + password file already scp'd in §3 (~/.opencode.env):
cd ~ && pm2 start ~/.opencode/bin/opencode --interpreter none \
  --name opencode --cwd /home/$USER -- web --port 4096 --hostname 127.0.0.1
pm2 start /usr/local/bin/cloudflared --interpreter none --name cloud \
  --cwd /home/$USER -- tunnel --url http://127.0.0.1:4096
pm2 save && curl -s -o /dev/null --max-time 10 http://127.0.0.1:4096/ && echo WEB_OK
pm2 logs cloud --lines 5 --nostream | grep -oE "https://[a-z0-9-]+\.trycloudflare\.com" | head -n 1
# ^ your remote URL (login with OPENCODE_SERVER_PASSWORD). NOTE: quick-tunnel
# URLs ROTATE on every restart — read the fresh URL from `pm2 logs cloud`.
# For a STABLE url later: `cloudflared tunnel login` (browser) →
# `cloudflared tunnel create <name>` → route your domain → switch the pm2
# `cloud` app to `tunnel run <name>`.
```
- Self-healing (same as old box) — install `~/healthcheck.sh` with this content,
  then `crontab -e`:
```bash
#!/bin/bash
LOG="$HOME/.healthcheck.log"; STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if ! curl -s -o /dev/null --max-time 10 http://127.0.0.1:4096/; then
  echo "$STAMP ALERT port 4096 down, restarting opencode" >> "$LOG"
  /usr/local/bin/pm2 restart opencode >> "$LOG" 2>&1
else
  MEM="$(free -m | awk '/^Mem:/{print "mem_used="$3"MB_avail="$7"MB"}')"
  echo "$STAMP OK $MEM load:$(cut -d' ' -f1-3 /proc/loadavg)" >> "$LOG"
fi
tail -n 200 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
```
```
*/5 * * * * /home/$USER/healthcheck.sh
0 4 * * * /usr/local/bin/pm2 restart opencode >/dev/null 2>&1
```

## 4. Staged rollout (cursors persist per lane — enabling is lossless)

1. `timeframes: [5m]` → `pm2 start ecosystem.config.js && pm2 save && pm2 startup`
   (run the command it prints). Soak 1 h: RSS flat, flushes flowing, gaps 0.
2. Add `15m` → full-file `pm2 restart ecosystem.config.js && pm2 save`.
   Gate: 7/7 lanes live, `series_id == "{ASSET}-15m"` pure, Kaggle 15m version ready.
3. Add `1h` (ET-slug discovery, live-validated 2026-09-08) → same gates, `"{ASSET}-1h"`.
4. Add `4h` → same gates. Each step: commit the config change. Do NOT push unless asked.

## 5. Standing rules (repo law — violations corrupt data)

- Real data only. Missing stays NULL. Gaps stay honest (never backfill invented rows).
- No manual deletes of `data/` (only `run_2x5min_test.py` wipes, and only in test mode).
- `polymarket-compact` stays STOPPED (compact cron off by design).
- Disk gate 150 MB (`df -h /`); box reboot check via `uptime` (silent deaths).
- Always use `.venv/bin/python`, never system python.
- Leak canary: `pm2 stop polymarket-collector` →
  `.venv/bin/python leak_probe.py --duration-sec 780 --sample-sec 60` →
  external `ps -o rss=` sampling must be flat (≈ +0 MB/min; pre-fix was +110).
  Full-file restart + `pm2 save` afterwards.
- Wallets: Data-API both-legs + on-chain C2 receipt pass run on the 15-min backfill
  cron automatically; multi-party txs stay NULL (never guessed).
- Resolutions: in-run inference in seconds; official winner via 15-min cron
  (newest-first) — worst case ≈ 30 min after end.

## 6. Current repo state (all on master, pushed)

- `b99dc0f` rollout step 2 (5m+15m) · `6da36c8` 1h ET-slug discovery + C2 on-chain
  wallets + newest-first backfill (pytest 147/147) · `3849fd9` P0-leak VPS verify
  (probe flat) · `5aa2feb` leak-container bounds + 8 regression tests.
- Known-good: pytest 147/147; VPS probe flat; 15m live 7/7 (2026-09-08).
- Open: 1h/4h soak on big disk; `markets_log.flush_staging` retry hardening;
  1d ladder (scoped out); NegRisk-exchange wallet coverage (unverified signatures).
