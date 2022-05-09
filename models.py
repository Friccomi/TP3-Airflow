from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Date
import db


class stock(db.Base):
    """Describe Daily Stock

    Args:
        symbol (String):   product which stock will be regist per day
        date   (Date):     date of the daily stock
        open   (Integer):  Quantity of product when day opens
        high   (Integer):  Maximun quantity of stock in the day  (Integer)
        low    (Integer):  Minimum quantity of stock in the day (Integer)
        close  (Integer):  Quantity of stock at the end of the day

    Returns:
        String: that shows : symbol, date, open, high, low, close
    """

    __tablename__ = "stock"
    __table_args__ = {"schema": "stock"}

    symbol = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)

    def __init__(self, symbol, date, open, high, low, close) -> None:
        """[summary]

        Args:
            symbol ([str]): name of the product
            date ([str]): date of the stock
            open ([number]): price when the stock opens
            high ([number]): high price in date
            low ([number]):  low price in date
            close ([number]): price when the stock closes
        """
        self.symbol = symbol
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close

    def __repr__(self):
        """Constructor"""
        return f"Stock({self.symbol}, {self.date},{self.open},{self.high},{self.low},{self.close})"

    def __str__(self):
        """Return all values"""
        return f"{self.symbol}, {self.date},{self.open},{self.high},{self.low},{self.close}"
