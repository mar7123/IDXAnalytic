from dataclasses import dataclass
from typing import List, Optional


@dataclass
class StockSummaryResponseData:
    No: int
    IDStockSummary: int
    Date: str
    StockCode: str
    StockName: str
    Remarks: str
    Previous: Optional[float]
    OpenPrice: Optional[float]
    FirstTrade: Optional[float]
    High: Optional[float]
    Low: Optional[float]
    Close: Optional[float]
    Change: Optional[float]
    Volume: Optional[float]
    Value: Optional[float]
    Frequency: Optional[float]
    IndexIndividual: Optional[float]
    Offer: Optional[float]
    OfferVolume: Optional[float]
    Bid: Optional[float]
    BidVolume: Optional[float]
    ListedShares: Optional[float]
    TradebleShares: Optional[float]
    WeightForIndex: Optional[float]
    ForeignSell: Optional[float]
    ForeignBuy: Optional[float]
    DelistingDate: Optional[str]
    NonRegularVolume: Optional[float]
    NonRegularValue: Optional[float]
    NonRegularFrequency: Optional[float]
    persen: Optional[float]
    percentage: Optional[float]


@dataclass
class IndexSummaryResponseData:
    No: int
    IndexSummaryID: int
    Date: str
    IndexCode: str
    Previous: Optional[float]
    Highest: Optional[float]
    Lowest: Optional[float]
    Close: Optional[float]
    NumberOfStock: Optional[float]
    Change: Optional[float]
    Volume: Optional[float]
    Value: Optional[float]
    Frequency: Optional[float]
    MarketCapital: Optional[float]


@dataclass
class BaseSummaryResponse:
    draw: int
    recordsTotal: int
    recordsFiltered: int


@dataclass
class StockSummaryResponse(BaseSummaryResponse):
    data: List[StockSummaryResponseData]

    @staticmethod
    def from_json(json: any):
        data = [StockSummaryResponseData(**item) for item in json.get("data", [])]
        return StockSummaryResponse(
            draw=json["draw"],
            recordsTotal=json["recordsTotal"],
            recordsFiltered=json["recordsFiltered"],
            data=data,
        )


@dataclass
class IndexSummaryResponse(BaseSummaryResponse):
    data: List[IndexSummaryResponseData]

    @staticmethod
    def from_json(json: any):
        data = [IndexSummaryResponseData(**item) for item in json.get("data", [])]
        return IndexSummaryResponse(
            draw=json["draw"],
            recordsTotal=json["recordsTotal"],
            recordsFiltered=json["recordsFiltered"],
            data=data,
        )
