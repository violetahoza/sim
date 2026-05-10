from __future__ import annotations
import ssl
from sqlalchemy import Column, DateTime, Float, Integer, Boolean, Text, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from .utils import read_env_file


def make_engine(db_url: str | None = None):
    url = db_url or read_env_file().get("DB_URL", "")
    if not url:
        return None

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    return create_engine(
        url,
        connect_args={"ssl": ctx},
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False,
    )


class Base(DeclarativeBase):
    pass


class ScenarioRun(Base):
    __tablename__ = "scenario_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    scenario_name = Column(String(120), nullable=False, index=True)
    protocol = Column(String(10), nullable=False)
    architecture = Column(String(30), nullable=False)
    traffic_level = Column(String(10), nullable=False)
    num_spots = Column(Integer, nullable=False)
    sim_duration_s = Column(Float, nullable=False)
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    config_json = Column(Text)

    latency_mean_ms = Column(Float)
    latency_p50_ms = Column(Float)
    latency_p95_ms = Column(Float)
    latency_p99_ms = Column(Float)
    latency_min_ms = Column(Float)
    latency_max_ms = Column(Float)
    latency_mean_ms_with_warmup = Column(Float)
    warmup_s = Column(Float)
    warmup_events_excluded = Column(Integer)

    sensor_to_edge_msgs = Column(Integer)
    edge_to_cloud_msgs = Column(Integer)

    sensor_to_edge_delivery_ratio = Column(Float)
    edge_to_cloud_delivery_ratio = Column(Float)
    end_to_end_delivery_ratio = Column(Float)

    aggregation_ratio = Column(Float)
    filtered_events = Column(Integer)
    anomalies_detected = Column(Integer)
    adaptive_mode_switches = Column(Integer)

    edge_cpu_pct = Column(Float)
    edge_mem_mb = Column(Float)
    cloud_cpu_pct = Column(Float)
    cloud_mem_mb = Column(Float)

    broker_overhead_score = Column(Float)


class ParkingSpot(Base):
    __tablename__ = "parking_spots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, nullable=False, index=True)
    spot_id = Column(Integer, nullable=False)
    state = Column(String(10), nullable=False)
    last_updated = Column(Float, nullable=False)
    received_at = Column(Float, nullable=False)


class LatencyRecord(Base):
    __tablename__ = "latency_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, nullable=False, index=True)
    spot_id = Column(Integer, nullable=False)
    sequence = Column(Integer, nullable=False)
    protocol = Column(String(10), nullable=False)
    architecture = Column(String(30), nullable=False)
    sent_at = Column(Float, nullable=False)
    received_at = Column(Float, nullable=False)
    latency_ms = Column(Float, nullable=False)
    is_warmup = Column(Boolean, nullable=False, default=False)


def init_schema(engine) -> None:
    Base.metadata.create_all(engine)


def make_session(engine) -> Session:
    factory = sessionmaker(bind=engine)
    return factory()