# from fastapi import FastAPI
# import time, random
# from otel_setup import setup_otel

# app = FastAPI()
# setup_otel(app, "user-service")

# @app.get("/users/{user_id}")
# def get_user(user_id: int):
#     time.sleep(random.uniform(0.1, 0.3))
#     return {
#         "id": user_id,
#         "name": "Lee"
#     }

from fastapi import FastAPI, HTTPException
import random, time
from otel_setup import setup_otel

app = FastAPI()
setup_otel(app, "user-service")

@app.get("/user/{user_id}")
def get_user(user_id: int):
    time.sleep(random.uniform(0.05, 0.25))
    r = random.random()

    if r < 0.15:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if r < 0.30:
        raise HTTPException(status_code=403, detail="User banned")

    if r < 0.40:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "user_id": user_id,
        "status": "active"
    }
