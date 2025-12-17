import os 
import time
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
import redis

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT_NUM", "6379"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI()

HTTP_REQS = Counter('http_requests_total', 'HTTP requests', ['route', 'method', 'status'])
HTTP_LAT = Histogram('http_request_duration_seconds', 'Request latency', ['route'])
EVENTS_INGESTED = Counter('events_ingested_total', 'Events ingested', ['tenant'])
REDIS_OPS = Counter('redis_ops_total', 'Redis operations', ['op'])
REDIS_LAT = Histogram('redis_latency_seconds', 'Redis latencty', ['op'])

class Event(BaseModel):
    tenant: str
    user: str
    path: str
    ts: int | None = None

def utc_minute_key(ts: int):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y%m%d-%H%M")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(data, media_type=CONTENT_TYPE_LATEST)

@app.post("/ingest")
def ingest(ev: Event):
    route = "/ingest"
    start = time.time()
    status = "200"

    try:
        ts = ev.ts or int(time.time())
        minute = utc_minute_key(ts)
        k_cnt = f"events:tenant:{ev.tenant}:minute:{minute}"

        day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%d")

        k_hll = f"users:tenant:{ev.tenant}:day:{day}"

        k_top = f"top_paths:tenant:{ev.tenant}"

        t0 = time.time()
        r.incr(k_cnt)
        r.expire(k_cnt, 60*60)
        REDIS_OPS.labels("incr_expire").inc()
        REDIS_LAT.labels("incr_expire").observe(time.time() - t0)

        t1 = time.time()
        r.pfadd(k_hll, ev.user)
        r.expire(k_hll, 48*3600)
        REDIS_OPS.labels("pfadd").inc()
        REDIS_LAT.labels("pfadd").observe(time.time() - t1)

        t2 = time.time()
        r.zincrby(k_top, 1, ev.path)
        REDIS_OPS.labels("zincrby").inc()
        REDIS_LAT.labels("zincrby").observe(time.time() - t2)

        EVENTS_INGESTED.labels(ev.tenant).inc()
        return {"ok": True}
    except Exception as e:
        status = "500"
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        HTTP_REQS.labels(route, "POST", status).inc()
        HTTP_LAT.labels(route).observe(time.time() - start)


@app.get("/stats")
def stats(tenant: str):
    route = "/stats"
    start = time.time()
    status = "200"
    
    try:
        now = int(time.time())
        minute = utc_minute_key(now)
        day = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y%m%d")

        k_cnt = f"events:tenant:{tenant}:minute:{minute}"
        k_hll = f"uusers:tenant:{tenant}:day:{day}"

        t0 = time.time()
        cnt = r.get(k_cnt) or "0"
        REDIS_OPS.labels("get").inc()
        REDIS_LAT.labels("get").observe(time.time() - t0)

        t1 = time.time()
        u = r.pfcount(k_hll)
        REDIS_OPS.labels("pfcount").inc()
        REDIS_LAT.labels("pfcount").observe(time.time() - t1)

        return {"tenant": tenant, "minute": minute, "events_this_minure": int(cnt), "unique_users_today": u}
    except Exception as e:
        status = "500"
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        HTTP_REQS.labels(route, "GET", status).inc()
        HTTP_LAT.labels(route).observe(time.time() - start)

@app.get("/top-paths")
def top_paths(tenant: str, n: int = 10):
    route = "/top-paths"
    start = time.time()
    status = "200"

    try:
        k_top = f"top_paths:tenant:{tenant}"
        t0 = time.time()
        items = r.zrevrange(k_top, 0, max(n-1, 0), withscores=True) or list()
        REDIS_OPS.labels("zrevrange").inc()
        REDIS_LAT.labels("zrevrange").observe(time.time() - t0)

        return[{"path": p, "count": int(c)} for p, c in items] 
    
    except Exception as e:
        status = "500"
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        HTTP_REQS.labels(route, "GET", status).inc()
        HTTP_LAT.labels(route).observe(time.time() - start)





