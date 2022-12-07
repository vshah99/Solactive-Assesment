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
        self.index_levels = pd.DataFrame(columns=["Date", "Index Level"])
        #Pre-calculate index levels on all data for O(1) retrieval
        self.precalc_index_level()

        #Empty index_samples for export
        self.index_samples = None


    def generate_index_baskets(self, d: dt.date, curr_basket: pd.DataFrame) -> (float, pd.DataFrame):
        # Returns last 2 prices for each stock on date d
        prices = self.stock_prices[self.stock_prices.Date <= d].iloc[-2:, :]

        #Jan 1 current basket is None
        if curr_basket is None:
            curr_value = 100
        else:
            basket = pd.Series(prices.iloc[1, 1:], name="Price_Curr").to_frame().join(curr_basket)
            curr_value = sum(basket.Price_Curr * basket.Shares)

        if d not in self.rebal:
            #Not a rebalancing date, no change in basket
            return curr_value, curr_basket
        else:
            #Rebalance
            top_prices_prev = pd.Series(prices.iloc[0, 1:], name="Price_Prev").sort_values().to_frame()
            top_prices_curr = pd.Series(prices.iloc[1, 1:], name="Price_Curr").sort_values().to_frame()
            top_prices = top_prices_prev.join(top_prices_curr)
            #Weighted bottom 3, next_basket calculates shares for each stock
            top_prices['Weight'] = [0 for _ in range(7)] + [0.25, 0.25, 0.5]
            next_basket = pd.Series((top_prices.Weight * curr_value) / top_prices.Price_Curr, name="Shares")
            return curr_value, next_basket

    def precalc_index_level(self) -> None:
        values = []
        dates = []
        curr_basket = None
        for d in self.stock_prices.Date:
            #get value and update basket
            value, curr_basket = self.generate_index_baskets(d, curr_basket)
            values.append(value)
            dates.append(d)

        #pd for easy sample and export
        self.index_levels =\
            pd.DataFrame({"Date": dates, "Index Level": values})

    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:
        self.index_samples = self.index_levels[(self.index_levels.Date >= pd.to_datetime(start_date)) & (self.index_levels.Date <= pd.to_datetime(end_date))]

    def export_values(self, file_name: str) -> None:
        self.index_samples.to_csv(file_name, index=False)

if __name__ == "__main__":
    IndexModel()