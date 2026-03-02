# from fastapi import FastAPI
# import httpx
# from otel_setup import setup_otel

# app = FastAPI()
# setup_otel(app, "gateway-service")

# @app.get("/checkout/{user_id}")
# async def checkout(user_id: int):
#     async with httpx.AsyncClient() as client:
#         user = await client.get(f"http://localhost:8001/users/{user_id}")
#         order = await client.post("http://localhost:8002/order")

#     return {
#         "user": user.json(),
#         "order": order.json()
#     }


from fastapi import FastAPI, HTTPException
import requests, random, time
from otel_setup import setup_otel

app = FastAPI()
setup_otel(app, "gateway-service")

USER_URL = "http://localhost:8001"
ORDER_URL = "http://localhost:8002"
PAY_URL = "http://localhost:8003"

@app.post("/checkout")
def checkout(user_id: int):
    if random.random() < 0.1:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        user = requests.get(f"{USER_URL}/user/{user_id}", timeout=1).json()
        order = requests.post(f"{ORDER_URL}/order", params={"user_id": user_id}, timeout=1).json()
        payment = requests.post(f"{PAY_URL}/pay", timeout=1).json()

        return {
            "user": user,
            "order": order,
            "payment": payment
        }

    except requests.exceptions.Timeout:
        raise HTTPException(status_code=504, detail="Upstream timeout")

    except Exception:
        raise HTTPException(status_code=502, detail="Bad gateway")
