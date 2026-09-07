#!/usr/bin/env bash
# shared helpers for g8 steady bench — sourced by g8_steady_bench.sh
overview_slots() {
  python3 -c '
import json,urllib.request
try:
  d=json.load(urllib.request.urlopen("http://127.0.0.1:8081/overview", timeout=5))
  print(d.get("slots-total",0), d.get("taskmanagers",0), d.get("jobs-running",0))
except Exception:
  print("0 0 0")
' 2>/dev/null || echo "0 0 0"
}

read_kafka_ads() {
  local dau_file=/tmp/g8s_kafka_dau.jsonl
  local pay_file=/tmp/g8s_kafka_pay.jsonl
  rm -f "$dau_file" "$pay_file"
  timeout 10 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_DAU" \
    --from-beginning --property print.key=true --property key.separator="|" \
    --timeout-ms 7000 > "$dau_file" 2>/dev/null || true
  timeout 10 docker exec gs-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_PAY" \
    --from-beginning --property print.key=true --property key.separator="|" \
    --timeout-ms 7000 > "$pay_file" 2>/dev/null || true
  DT="$DT" SERVER_ID="$SERVER_ID" python3 - <<'PY'
import json, os
from pathlib import Path
dt, sid = os.environ["DT"], int(os.environ["SERVER_ID"])

def latest(path):
    latest=None
    p=Path(path)
    if not p.exists():
        return None
    for line in p.read_text().splitlines():
        line=line.strip()
        if not line:
            continue
        val=line.split("|",1)[1] if "|" in line else line
        try:
            o=json.loads(val)
        except Exception:
            continue
        odt=str(o.get("dt",""))[:10]
        try:
            osid=int(o.get("server_id"))
        except Exception:
            continue
        if odt==dt and osid==sid:
            latest=o
    return latest

dau_o=latest("/tmp/g8s_kafka_dau.jsonl")
pay_o=latest("/tmp/g8s_kafka_pay.jsonl")
dau = dau_o.get("dau") if dau_o else None
pay = pay_o.get("pay_users") if pay_o else None
rate = pay_o.get("pay_rate") if pay_o else None
pdau = pay_o.get("dau") if pay_o else None
if dau is None and pdau is not None:
    dau=pdau
print(f"{dau if dau is not None else 'NULL'} {pay if pay is not None else 'NULL'} {rate if rate is not None else 'NULL'}")
PY
}

materialize_doris() {
  local raw dau pay rate
  raw=$(read_kafka_ads)
  dau=$(echo "$raw" | awk '{print $1}')
  pay=$(echo "$raw" | awk '{print $2}')
  rate=$(echo "$raw" | awk '{print $3}')
  if [[ "$dau" == "NULL" || "$pay" == "NULL" || -z "$rate" || "$rate" == "NULL" ]]; then
    echo "[g8s] materialize skip: kafka ads incomplete ($raw)" >&2
    return 1
  fi
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "
INSERT INTO ads.ads_dau_di (dt, server_id, dau, metric_id)
VALUES ('${DT}', ${SERVER_ID}, ${dau}, 'ads_dau_di');
INSERT INTO ads.ads_pay_rate_di (dt, server_id, dau, pay_users, pay_rate, metric_id)
VALUES ('${DT}', ${SERVER_ID}, ${dau}, ${pay}, ${rate}, 'ads_pay_rate_di');
" >/dev/null
  echo "$raw"
}

doris_ads_row() {
  docker exec gs-doris-fe mysql -h127.0.0.1 -P9030 -uroot -N -e "
SELECT CONCAT_WS(' ',
  IFNULL((SELECT dau FROM ads.ads_dau_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL'),
  IFNULL((SELECT pay_users FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL'),
  IFNULL((SELECT pay_rate FROM ads.ads_pay_rate_di WHERE dt='${DT}' AND server_id=${SERVER_ID}), 'NULL')
);" 2>/dev/null | tr -d '\r' | tail -n 1
}

sample_lag() {
  local label="$1" outf="$2"
  {
    echo "=== lag $label $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
    docker exec gs-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 --describe --group "$G8_GROUP" 2>&1 || echo "consumer-groups describe failed"
  } | tee "$outf"
}

produce_file() {
  local file="$1"
  docker exec -i gs-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC_IN" < "$file"
}
