# from fastapi import FastAPI
# import httpx
# from otel_setup import setup_otel

# app = FastAPI()
# setup_otel(app, "order-service")

# @app.post("/order")
# async def create_order():
#     async with httpx.AsyncClient() as client:
#         payment = await client.post("http://localhost:8003/pay")

#     return {
#         "order": "created",
#         "payment": payment.json()
#     }


from fastapi import FastAPI, HTTPException
import time, random
from otel_setup import setup_otel

app = FastAPI()
setup_otel(app, "order-service")

@app.post("/order")
def create_order(user_id: int):
    time.sleep(random.uniform(0.05, 0.3))
    r = random.random()

    if r < 0.15:
        raise HTTPException(status_code=400, detail="Invalid order data")

    if r < 0.30:
        raise HTTPException(status_code=409, detail="Out of stock")

    if r < 0.40:
        raise HTTPException(status_code=500, detail="Order DB error")

    return {
        "order_id": random.randint(1000, 9999),
        "status": "created"
    }

@app.get("/order/{order_id}")
def get_order(order_id: int):
    time.sleep(random.uniform(0.05, 0.2))

    if random.random() < 0.2:
        raise HTTPException(status_code=404, detail="Order not found")

    return {"order_id": order_id, "status": "created"}
