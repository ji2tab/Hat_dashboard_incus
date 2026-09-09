#!/usr/bin/env python3
"""mmdvm-dash 単独編: MMDVM-Host の MQTT (host/json) を購読して表示するダッシュボード

母艦編(incus exec 経由で各コンテナの mosquitto を購読)を、コンテナ非依存の
「ローカル mosquitto 直結」に差し替えた単独ノード版。API と画面は母艦編と同一。

  GET /api/instances                 インスタンス一覧
  GET /api/{name}/status             現在の状態(受信中/待機中)
  GET /api/{name}/lastheard?limit=N  Last heard(start〜end をまとめたもの)
  GET /                              static/index.html

母艦編との違いはこの2点のみ:
  - subscribe_loop: `incus exec <c> -- mosquitto_sub` → `mosquitto_sub -h 127.0.0.1 ...`(認証付き)
  - read_station_info: `incus exec <c> -- cat ini` → ローカルの ini を直接 open
"""

import configparser
import json
import os
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

CONFIG_PATH = os.environ.get("MMDVM_DASH_CONFIG", "/etc/mmdvm-dash/config.yaml")
MAX_HEARD = 100          # インスタンスごとに保持する Last heard 件数
ACTIVE_TIMEOUT = 15      # start 後この秒数 end が来なければ待機中に戻す

app = FastAPI(title="mmdvm-dash")


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not cfg or "instances" not in cfg:
        raise RuntimeError(f"{CONFIG_PATH}: 'instances' が定義されていません")
    return cfg


CFG = load_config()
INSTANCES: dict = CFG["instances"]

# 表示タイムゾーン(既定 JST)。systemd の環境に依存せず明示変換する。
_tz_name = CFG.get("display", {}).get("timezone", "Asia/Tokyo")
try:
    DISPLAY_TZ = ZoneInfo(_tz_name)
except Exception:  # noqa: BLE001
    DISPLAY_TZ = None  # フォールバック: システムローカル

# ID → コールサイン対応表(自局など Text が来ない ID を補完)。キーは文字列。
CALLSIGN_MAP: dict = {str(k): str(v) for k, v in (CFG.get("callsigns", {}) or {}).items()}

# DMR ID データベース。ID(str) -> {"call","name","loc"}
# 対応形式: RadioID.net user.csv(ヘッダ付きCSV)/ dmrid.dat(空白区切り)
DMR_DB: dict = {}
DMR_DB_LOCK = threading.Lock()
DMR_DB_INFO = {"path": None, "format": None, "count": 0, "loaded_at": None}


def _load_user_csv(path: str) -> dict:
    import csv
    db = {}
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        # 列位置をヘッダから決定(将来の列追加に強い)
        idx = {name: i for i, name in enumerate(h.strip().upper() for h in (header or []))}
        c_id = idx.get("RADIO_ID", 0)
        c_call = idx.get("CALLSIGN", 1)
        c_first = idx.get("FIRST_NAME")
        c_city = idx.get("CITY")
        c_country = idx.get("COUNTRY")
        for row in reader:
            if len(row) <= c_call:
                continue
            did = row[c_id].strip()
            call = row[c_call].strip()
            if not did.isdigit() or not call:
                continue
            name = row[c_first].strip() if c_first is not None and len(row) > c_first else ""
            city = row[c_city].strip() if c_city is not None and len(row) > c_city else ""
            country = row[c_country].strip() if c_country is not None and len(row) > c_country else ""
            loc = ", ".join(x for x in (city, country) if x)
            db[did] = {"call": call, "name": name, "loc": loc}
    return db


def _load_dat(path: str) -> dict:
    db = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2 or not parts[0].isdigit():
                continue
            name = parts[2] if len(parts) > 2 else ""
            db[parts[0]] = {"call": parts[1], "name": name, "loc": ""}
    return db


def load_dmr_db(path: str) -> int:
    """DB を読み込む。拡張子/先頭行から形式を自動判定。件数を返す。"""
    try:
        fmt = "csv" if path.lower().endswith(".csv") else "dat"
        if fmt == "dat":
            with open(path, encoding="utf-8", errors="replace") as f:
                first = f.readline()
            if "," in first and "RADIO_ID" in first.upper():
                fmt = "csv"
        db = _load_user_csv(path) if fmt == "csv" else _load_dat(path)
    except OSError:
        return 0
    with DMR_DB_LOCK:
        DMR_DB.clear()
        DMR_DB.update(db)
        DMR_DB_INFO.update({
            "path": path, "format": fmt, "count": len(db),
            "loaded_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        })
    return len(db)


_dmr_db_path = CFG.get("dmr_ids")
# 設定パスが無ければ app_solo.py と同じディレクトリの user.csv を試す
# (git clone 直下起動など、配置場所が config の絶対パスと異なる場合の保険)
if _dmr_db_path and not os.path.isfile(_dmr_db_path):
    _fallback_csv = str(Path(__file__).parent / "user.csv")
    if os.path.isfile(_fallback_csv):
        _dmr_db_path = _fallback_csv
if _dmr_db_path:
    load_dmr_db(_dmr_db_path)


def resolve_call(src_id) -> str | None:
    """ID からコールサインを解決。config 上書き → DB の順。"""
    src = str(src_id or "")
    if not src:
        return None
    if src in CALLSIGN_MAP:
        return CALLSIGN_MAP[src]
    with DMR_DB_LOCK:
        rec = DMR_DB.get(src)
    return rec["call"] if rec else None


def resolve_full(src_id):
    """ID から {"call","name","loc"} を返す(無ければ None)。"""
    src = str(src_id or "")
    if src in CALLSIGN_MAP:
        return {"call": CALLSIGN_MAP[src], "name": "", "loc": ""}
    with DMR_DB_LOCK:
        rec = DMR_DB.get(src)
    return dict(rec) if rec else None


def format_callsign(src_id, alias) -> str:
    """表示用コールサイン。src_id の DB/config 解決を主にし、
    Text alias が食い違う場合のみ ' / alias:XXX' を併記する。"""
    src = str(src_id or "")
    resolved = resolve_call(src)                 # config → DB
    alias = (alias or "").strip()
    alias_call = alias.split()[0] if alias else ""
    if resolved:
        if alias_call and alias_call.upper() != resolved.upper():
            return f"{resolved} / alias:{alias_call}"
        return resolved
    # DB 未解決: alias があればそれ(全体)を、無ければ数字IDを出す
    if alias:
        return alias
    return src or "?"


# ---------------------------------------------------------------- 状態保持

def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def fmt_time(iso_utc: str) -> str:
    """MQTT の UTC タイムスタンプ → 表示TZ(既定JST)の HH:MM:SS"""
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        return dt.astimezone(DISPLAY_TZ).strftime("%H:%M:%S")
    except (ValueError, AttributeError):
        return ""


class InstanceState:
    def __init__(self, name: str, cfg: dict):
        self.name = name
        # label は config で明示された時のみ固定。未指定なら ini のコールサインを
        # 起動時に自動導出する(コールサイン・周波数をソース/設定に直書きしない)。
        self._label_from_config = bool(cfg.get("label"))
        self.label = cfg.get("label") or name
        self.heard = deque(maxlen=MAX_HEARD)   # 新しいものが先頭
        self.open_tx = {}                       # slot -> 進行中の start 情報
        self.last_msg_at = None                 # 最後に MQTT を受けた時刻(監視用)
        self.connected = False
        self.error = None
        self.lock = threading.Lock()

    # ---- MQTT メッセージ処理 ----------------------------------------

    def handle(self, payload: dict):
        with self.lock:
            self.last_msg_at = time.monotonic()
            if "DMR" in payload:
                self._dmr(payload["DMR"])
            elif "Text" in payload:
                self._text(payload["Text"])
            # 他モード(YSF/D-Star 等)も同様の形なら将来ここに追加

    def _dmr(self, d: dict):
        action = d.get("action")
        slot = d.get("slot")
        if action in ("start", "late_entry"):
            self.open_tx[slot] = {
                "time": d.get("timestamp", ""),
                "_mono": time.monotonic(),
                "mode": f"DMR TS{slot}" if slot else "DMR",
                "source": "RF" if d.get("source") == "rf" else "NET",
                "src_id": str(d.get("src_id", "")),
                "callsign": d.get("src_info") or str(d.get("src_id", "?")),
                "dest": f"TG {d.get('dst_id')}" if d.get("dst_id") is not None else "?",
                "late": action == "late_entry",
            }
        elif action in ("end", "lost"):
            entry = self.open_tx.pop(slot, None)
            if entry is None:
                # start を取りこぼした end/lost。最低限の行を作る
                entry = {
                    "time": d.get("timestamp", ""), "mode": f"DMR TS{slot}",
                    "source": "", "src_id": "", "callsign": "?", "dest": "?",
                    "alias": "", "late": False,
                }
            entry["duration"] = d.get("duration")
            entry["ber"] = d.get("ber")
            entry["loss"] = d.get("loss")           # lost 時はパケットロス%が入る
            entry["lost"] = (action == "lost")
            entry["ended"] = d.get("timestamp", "")
            self.heard.appendleft(entry)

    def _text(self, t: dict):
        """Text メッセージ(talker alias)を別枠で保持する。callsign は上書きしない。
        src_id の DB 解決を主とし、alias は食い違うときだけ併記に使う。"""
        value = (t.get("value") or "").strip()
        if not value:
            return
        slot = t.get("slot")
        target = self.open_tx.get(slot)
        if target is not None:
            target["alias"] = value
            return
        # 進行中が無ければ、直近で alias 未設定のエントリに付与
        for e in self.heard:
            if not e.get("alias"):
                e["alias"] = value
                break

    # ---- API 向けスナップショット ----------------------------------

    def snapshot_status(self):
        with self.lock:
            now = time.monotonic()
            active = None
            for slot, tx in list(self.open_tx.items()):
                if now - tx.get("_mono", now) > ACTIVE_TIMEOUT:
                    # end が来なかった送信。記録に落として active から外す
                    tx = {k: v for k, v in tx.items() if k != "_mono"}
                    tx["duration"] = None
                    tx["ber"] = None
                    tx["ended"] = tx["time"]
                    self.heard.appendleft(tx)
                    self.open_tx.pop(slot, None)
                    continue
                active = {
                    "callsign": format_callsign(tx.get("src_id"), tx.get("alias")),
                    "dest": tx["dest"],
                    "mode": tx["mode"], "source": tx["source"],
                }
            return {
                "connected": self.connected,
                "active": active,
                "error": self.error,
                "last_msg_ago": (
                    round(time.monotonic() - self.last_msg_at, 1)
                    if self.last_msg_at else None
                ),
            }

    def snapshot_heard(self, limit: int):
        with self.lock:
            out = []
            for e in list(self.heard)[:limit]:
                rec = resolve_full(e.get("src_id"))
                out.append({
                    "time": fmt_time(e.get("ended") or e.get("time")),
                    "mode": e["mode"], "source": e["source"],
                    "callsign": format_callsign(e.get("src_id"), e.get("alias")),
                    "name": (rec or {}).get("name", ""),
                    "loc": (rec or {}).get("loc", ""),
                    "dest": e["dest"],
                    "duration": (
                        round(e["duration"], 1) if isinstance(e.get("duration"), (int, float)) else None
                    ),
                    "ber_pct": (
                        round(e["ber"], 2) if isinstance(e.get("ber"), (int, float)) else None
                    ),
                    "loss_pct": (
                        round(e["loss"], 1) if isinstance(e.get("loss"), (int, float)) else None
                    ),
                    "lost": e.get("lost", False),
                    "late": e.get("late", False),
                })
            return out


STATES: dict[str, InstanceState] = {
    name: InstanceState(name, cfg) for name, cfg in INSTANCES.items()
}


# ---------------------------------------------------------------- 局情報(ini)
# ヘッダ表示用にコールサイン・周波数・DMR ID を ini から一度だけ読む(任意)。
# 単独編: incus exec を使わず、ローカルの ini を直接読む。

def read_station_info(cfg: dict) -> dict:
    ini_path = cfg.get("ini")
    if not ini_path:
        return {}
    try:
        with open(ini_path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError:
        return {}

    cp = configparser.ConfigParser(strict=False, interpolation=None)
    try:
        cp.read_file(StringIO(text))
    except configparser.Error:
        return {}

    def get(section, key):
        return cp.get(section, key, fallback=None)

    info = {
        "callsign": get("General", "Callsign"),
        "dmr_id": get("General", "Id") or get("DMR", "Id"),
    }
    for section in ("Info", "Modem"):  # 2026-07-13 変更: [Info] → [Modem]
        rx, tx = get(section, "RXFrequency"), get(section, "TXFrequency")
        if rx or tx:
            info["rx_freq"], info["tx_freq"] = rx, tx
            break
    info["modes"] = {
        m: (get(m, "Enable") == "1")
        for m in ("D-Star", "DMR", "System Fusion", "P25", "NXDN", "M17")
        if cp.has_section(m)
    }
    return info


# ---------------------------------------------------------------- MQTT 購読
# 単独編: ローカル mosquitto へ直接 mosquitto_sub を常駐させ、1行ずつ受け取る。
# 出力形式: `-v` なので「host/json {json}」の行が届く。

def subscribe_loop(state: InstanceState):
    inst = INSTANCES[state.name]
    host = inst.get("mqtt_host", "127.0.0.1")
    port = str(inst.get("mqtt_port", 1883))
    user = inst.get("mqtt_user")
    pw = inst.get("mqtt_pass")
    topic = inst.get("topic", "host/#")
    while True:
        cmd = ["mosquitto_sub", "-h", host, "-p", port, "-t", topic, "-v"]
        if user:
            cmd += ["-u", str(user)]
        if pw:
            cmd += ["-P", str(pw)]
        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
        except FileNotFoundError:
            state.error = "'mosquitto_sub' が見つかりません (apt install mosquitto-clients)"
            time.sleep(10)
            continue

        state.connected = True
        state.error = None
        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                # "host/json {json}" を分割
                _, _, body = line.partition(" ")
                body = body.strip()
                if not body.startswith("{"):
                    continue
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError:
                    continue
                state.handle(payload)
        except Exception as e:  # noqa: BLE001
            state.error = str(e)
        finally:
            state.connected = False
            err = proc.stderr.read() if proc.stderr else ""
            if err.strip():
                state.error = err.strip().splitlines()[-1]
            proc.wait()
        time.sleep(5)  # 切れたら再接続


STATION_INFO: dict[str, dict] = {}


def dmr_db_updater():
    """設定間隔ごとに DB をダウンロードして再読み込みする(任意)。"""
    upd = CFG.get("dmr_ids_update") or {}
    url = upd.get("url")
    path = CFG.get("dmr_ids")
    interval_h = float(upd.get("interval_hours", 24))
    if not url or not path:
        return
    import urllib.request
    while True:
        time.sleep(interval_h * 3600)
        tmp = f"{path}.tmp"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "mmdvm-dash"})
            with urllib.request.urlopen(req, timeout=60) as r, open(tmp, "wb") as out:
                out.write(r.read())
            os.replace(tmp, path)
            n = load_dmr_db(path)
            print(f"[dmr-db] updated from {url}: {n} entries")
        except Exception as e:  # noqa: BLE001
            print(f"[dmr-db] update failed: {e}")
            try:
                os.remove(tmp)
            except OSError:
                pass


@app.on_event("startup")
def start_subscribers():
    for name, state in STATES.items():
        info = read_station_info(INSTANCES[name])
        STATION_INFO[name] = info
        # label 未指定なら ini のコールサインをタブ名に採用(直書きを避ける)
        if not state._label_from_config:
            cs = (info.get("callsign") or "").strip()
            if cs:
                state.label = cs
        t = threading.Thread(target=subscribe_loop, args=(state,), daemon=True)
        t.start()
    threading.Thread(target=dmr_db_updater, daemon=True).start()


# ---------------------------------------------------------------- API

def get_state(name: str) -> InstanceState:
    st = STATES.get(name)
    if st is None:
        raise HTTPException(404, f"instance '{name}' not found")
    return st


@app.get("/api/dmrdb")
def api_dmrdb():
    with DMR_DB_LOCK:
        return dict(DMR_DB_INFO)


@app.get("/api/instances")
def api_instances():
    return {"instances": [
        {"name": n, "label": s.label} for n, s in STATES.items()
    ]}


@app.get("/api/{name}/status")
def api_status(name: str):
    st = get_state(name)
    snap = st.snapshot_status()
    return {"name": name, "label": st.label,
            "info": STATION_INFO.get(name, {}), **snap}


@app.get("/api/{name}/lastheard")
def api_lastheard(name: str, limit: int = 20):
    st = get_state(name)
    return {"lastheard": st.snapshot_heard(max(1, min(limit, MAX_HEARD)))}


# 画面(index.html)は static/ を優先し、無ければ app_solo.py と同階層を使う
# (repo の solo/ 直下に index.html がある構成でも clone 一発で動くように)
_static_dir = Path(__file__).parent / "static"
if not (_static_dir / "index.html").is_file():
    _static_dir = Path(__file__).parent
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
