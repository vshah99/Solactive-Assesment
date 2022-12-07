import datetime as dt
import pandas as pd

class IndexModel:
    def __init__(self) -> None:
        # To be implemented
        self.stock_prices = pd.read_csv("data_sources/stock_prices.csv")
        self.stock_prices["Date"] = pd.to_datetime(self.stock_prices["Date"], dayfirst=True)

        # GroupBy M/Y, filter for first item in each group, remove first (Dec 2019)
        first_of_month = self.stock_prices.groupby(
            [(self.stock_prices["Date"].dt.year), (self.stock_prices["Date"].dt.month)]).first()
        first_of_month.drop(index=first_of_month.index[0], axis=0, inplace=True)
        self.rebal = first_of_month.loc[:, "Date"].values

        #Empty index_levels
        self.calc_index_level(dt.date(year=2020, month=1, day=1), dt.date(year=2020, month=12, day=31))

        #Empty index_samples
        self.index_samples = pd.DataFrame({"Date": [], "Index Level": []})

    def get_prices_dt(self, d):
        #Returns last 2 prices for each stock on date d
        prices = self.stock_prices[self.stock_prices.Date <= d].iloc[-2:, :]
        return prices

    def generate_index_baskets(self, d: dt.date, curr_basket: pd.DataFrame) -> (float, pd.DataFrame):
        prices = self.get_prices_dt(d)
        if curr_basket is None:
            curr_value = 100
        else:
            basket = pd.Series(prices.iloc[1, 1:], name="Price_Curr").to_frame().join(curr_basket)
            curr_value = sum(basket.Price_Curr * basket.Shares)

        if d not in self.rebal:
            return curr_value, curr_basket
        else:
            top_prices_prev = pd.Series(prices.iloc[0, 1:], name="Price_Prev").sort_values().to_frame()
            top_prices_curr = pd.Series(prices.iloc[1, 1:], name="Price_Curr").sort_values().to_frame()
            top_prices = top_prices_prev.join(top_prices_curr)
            top_prices['Weight'] = [0 for _ in range(7)] + [0.25, 0.25, 0.5]
            next_basket = pd.Series((top_prices.Weight * curr_value) / top_prices.Price_Curr, name="Shares")
            return curr_value, next_basket

    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:
        # To be implemented
        values = []
        dates = []
        curr_basket = None
        for d in pd.date_range(start=start_date, end=end_date):
            value, curr_basket = self.generate_index_baskets(d, curr_basket)
            values.append(value)
            dates.append(d)

        self.index_levels =\
            pd.DataFrame({"Date": dates, "Index Level": values})
        print(self.index_levels)

    def export_values(self, file_name: str) -> None:
        # To Csv
        self.index_levels.to_csv(file_name, index=False)

if __name__ == "__main__":
    IndexModel()