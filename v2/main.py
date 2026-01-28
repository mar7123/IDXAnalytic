from data_loader import get_engine
from risk_engine.risk_engine import risk_engine_main


engine = get_engine()

risk_engine_output = risk_engine_main(engine)
