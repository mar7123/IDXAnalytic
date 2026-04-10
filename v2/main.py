from data_loader import get_engine
from stock.stock import stock_main, stock_short_term
from shutd import shudtd


engine = get_engine()
# risk_engine_output = risk_engine_main(engine)
stock_output = stock_main(engine)
# stock_short_term(engine=engine)
shudtd()
