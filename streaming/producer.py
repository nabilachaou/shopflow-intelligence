import argparse, json, time
import pandas as pd
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--servers", default="localhost:9092")
parser.add_argument("--topic", default="orders")
parser.add_argument("--rate", type=float, default=5.0, help="commandes par seconde")
parser.add_argument("--limit", type=int, default=200, help="nombre de commandes (0 = toutes)")
args = parser.parse_args()

df = pd.read_csv("data/test_replay.csv").sort_values("purchase_ts")
if args.limit > 0:
    df = df.head(args.limit)

producer = KafkaProducer(
    bootstrap_servers=args.servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all")

sent = 0
for _, row in df.iterrows():
    event = {k: (None if pd.isna(v) else v) for k, v in row.items()}
    event["is_late_truth"] = int(event.pop("is_late"))   # vérité terrain, jamais une entrée du modèle
    event["event_time"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    producer.send(args.topic, key=event["order_id"], value=event)
    sent += 1
    if sent % 50 == 0:
        print(f"{sent} commandes envoyées")
    time.sleep(1.0 / args.rate)

producer.flush()
print(f"Terminé : {sent} commandes envoyées vers {args.topic}")
