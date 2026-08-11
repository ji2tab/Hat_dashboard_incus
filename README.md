# mmdvm-dash (MQTT購読版 / DMR ID DB対応)

MMDVM-Host が mosquitto に流す `host/json` を母艦で購読し、Last heard と
現在状態を表示するダッシュボード。ログファイルのパース不要。

## 主な機能
- hat1 / hat2 をタブ切替で監視(config 追加で拡張可)
- Last heard: 時刻(JST)・モード・RF/NET・コールサイン・宛先・秒・BER
- DMR ID データベース(RadioID.net user.csv / dmrid.dat)で ID→コールサイン
  ＋名前・地域を解決。数字のみの発信元は「CALL / ID」で併記
- DB 自動更新(任意, 既定24時間ごと)
- 局情報(コールサイン・周波数・DMR ID・有効モード)は ini から表示

## 配置(母艦)
| パス | 役割 |
|---|---|
| /opt/mmdvm-dash/app.py | 本体 |
| /opt/mmdvm-dash/static/index.html | 画面 |
| /opt/mmdvm-dash/venv/ | Python仮想環境 |
| /etc/mmdvm-dash/config.yaml | 設定 |
| /etc/mmdvm-dash/user.csv | DMR ID DB |
| /etc/systemd/system/mmdvm-dash.service | サービス定義 |
| /etc/systemd/system/mmdvm-dash.service.d/home.conf | HOME/ProtectHome 上書き |

## セットアップ(母艦・初回)
```bash
apt install -y python3-venv
useradd -r -s /usr/sbin/nologin mmdvmdash
usermod -aG incus-admin mmdvmdash
mkdir -p /opt/mmdvm-dash /etc/mmdvm-dash /home/mmdvmdash
chown mmdvmdash:mmdvmdash /home/mmdvmdash

cp app.py /opt/mmdvm-dash/
cp -r static /opt/mmdvm-dash/
cp config.yaml /etc/mmdvm-dash/
curl -fL https://radioid.net/static/user.csv -o /etc/mmdvm-dash/user.csv

python3 -m venv /opt/mmdvm-dash/venv
/opt/mmdvm-dash/venv/bin/pip install fastapi uvicorn pyyaml
chown -R mmdvmdash:mmdvmdash /opt/mmdvm-dash /etc/mmdvm-dash

cp mmdvm-dash.service /etc/systemd/system/
mkdir -p /etc/systemd/system/mmdvm-dash.service.d
cat > /etc/systemd/system/mmdvm-dash.service.d/home.conf <<'DROPIN'
[Service]
Environment=HOME=/home/mmdvmdash
ProtectHome=false
DROPIN

# mmdvmdash に incus クライアント設定を生成させる(初回だけ)
sudo -u mmdvmdash env HOME=/home/mmdvmdash incus list >/dev/null 2>&1

systemctl daemon-reload
systemctl enable --now mmdvm-dash
```

## 更新(既存環境へ差し替え)
```bash
sudo cp app.py      /opt/mmdvm-dash/app.py
sudo cp config.yaml /etc/mmdvm-dash/config.yaml
sudo cp index.html  /opt/mmdvm-dash/static/index.html   # tar内は static/index.html
sudo chown -R mmdvmdash:mmdvmdash /opt/mmdvm-dash
sudo systemctl restart mmdvm-dash
```

## 確認
```bash
curl -s localhost:8080/api/dmrdb   | python3 -m json.tool   # DB件数
curl -s localhost:8080/api/hat2/status | python3 -m json.tool
curl -s "localhost:8080/api/hat2/lastheard?limit=5" | python3 -m json.tool
```
ブラウザ: http://192.168.1.149:8080/

## config のポイント
- `display.timezone`: 時刻表示(既定 Asia/Tokyo)
- `dmr_ids`: DBファイルのパス(user.csv / dmrid.dat)
- `dmr_ids_update.url` / `interval_hours`: 自動更新(不要ならブロックごと削除)
- `callsigns`: 手動の上書き(DBより優先)。DBが誤り/未登録のIDだけ書く

## MQTT メッセージ形式(実測)
トピック `host/json`。DMR は start/end/late_entry、Text はトーカーエイリアス。
