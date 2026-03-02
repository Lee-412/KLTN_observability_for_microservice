# from fastapi import FastAPI, HTTPException
# import time, random
# from otel_setup import setup_otel
# from opentelemetry import trace
# from opentelemetry.trace import SpanKind

# app = FastAPI()
# setup_otel(app, "payment-service")

# tracer = trace.get_tracer("payment-service")

# @app.post("/pay")
# def pay():
#     with tracer.start_as_current_span(
#         "POST /pay",
#         kind=SpanKind.SERVER
#     ):
#         time.sleep(random.uniform(0.2, 0.5))
#         if random.random() < 0.3:
#             raise HTTPException(status_code=500, detail="Payment failed")
#         return {"status": "paid"}

from fastapi import FastAPI, HTTPException
import time, random
from otel_setup import setup_otel

app = FastAPI()
setup_otel(app, "payment-service")

@app.post("/pay")
def pay(amount: float = 100):
    delay = random.uniform(0.1, 0.6)
    time.sleep(delay)

    r = random.random()

    if r < 0.15:
        raise HTTPException(status_code=400, detail="Invalid payment request")

    if r < 0.30:
        raise HTTPException(status_code=402, detail="Insufficient funds")

    if r < 0.45:
        raise HTTPException(status_code=503, detail="Payment gateway timeout")

    if r < 0.55:
        raise HTTPException(status_code=500, detail="Internal payment error")

    return {
        "status": "paid",
        "amount": amount,
        "latency": delay
    }
