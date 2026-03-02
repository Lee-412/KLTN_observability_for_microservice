from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor


def setup_otel(app, service_name: str):
    resource = Resource.create({
        "service.name": service_name
    })

    # ===== TRACE =====
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)

    exporter = OTLPSpanExporter(
        endpoint="http://localhost:4318/v1/traces"
    )

    tracer_provider.add_span_processor(
        BatchSpanProcessor(exporter)
    )

    # ===== METRICS (Prometheus) =====
    reader = PrometheusMetricReader()
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[reader]
    )
    metrics.set_meter_provider(meter_provider)

    # ===== Instrument =====
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()
