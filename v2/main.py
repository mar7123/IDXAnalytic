from data_loader import get_engine
from risk_engine.risk_engine import risk_engine_main
from stock.stock import stock_main
from shutd import shudtd


engine = get_engine()
# risk_engine_output = risk_engine_main(engine)
stock_output = stock_main(engine)
shudtd()
