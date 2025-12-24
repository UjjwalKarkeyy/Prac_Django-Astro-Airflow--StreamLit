import redis

def get_redis():
    return redis.Redis(
        host="redis",
        port=6379,
        decode_responses=True # accept normal python strings not raw bytes
    )