# mmdvm-dash 単独編 (母艦編と同一UI / ローカルmosquitto直結)

母艦編(incus exec 経由)を、コンテナ非依存の「ローカル mosquitto 直結」に差し替えた単独ノード版。
画面(static/index.html)と API は母艦編と完全同一。app.py の2点だけ変更:
- subscribe_loop: incus exec → mosquitto_sub -h 127.0.0.1 -u mmdvm -P mmdvm(認証付き直結)
- read_station_info: incus exec cat → ローカル ini を直接 open

## ファイル
- app_solo.py              本体(単独版)
- index.html          画面(母艦編と同一, 配置先は static/index.html)
- config_solo.yaml         設定(単独1インスタンス jr2dhr)
- install_solo.sh     上記を同梱した自己完結インストーラ(base64+SHA256)

## 導入(JR2DHR)
    scp install_solo.sh asai@100.108.135.13:~/
    bash ~/install_solo.sh test      # 8080で前景起動して確認(sudo不要)
    #  http://192.168.0.62:8080/  または  http://100.108.135.13:8080/
確認後、install_solo.sh が表示する sudo ブロックで systemd 常駐化。

## config のポイント
- instances.jr2dhr.ini: /opt/MMDVMHost/MMDVM-Host.ini (局情報表示に使用)
- mqtt_user/mqtt_pass: mmdvm/mmdvm
- dmr_ids: /home/asai/mmdvm-dash/user.csv (RadioID, 24hで自動更新)
- 複数ノードを1画面のタブで見るなら instances に追記(母艦編と同じ構造)
