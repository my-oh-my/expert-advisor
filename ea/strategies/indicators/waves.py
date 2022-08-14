import numpy as np
import pandas as pd
import plotly.graph_objects as go
from pandas import DataFrame
from plotly.subplots import make_subplots
from xtb.wrapper.chart_last_request import ChartLastRequest


class Waves:
    def __init__(self, settings):
        self.settings = settings

    # https://towardsdatascience.com/simple-linear-regression-in-python-numpy-only-130a988c0212
    def get_beta(self, x, y):
        x_mean = x.mean()
        y_mean = y.mean()

        B1_num = ((x - x_mean) * (y - y_mean)).sum()
        B1_den = ((x - x_mean) ** 2).sum()

        B1 = None
        if B1_den != 0:
            B1 = B1_num / B1_den
        else:
            B1 = 0

        B0 = y_mean - (B1 * x_mean)

        return B0, B1

    def info_on_candidate(self):
        pass

    def get_wave_candle(self, side, dataframe: DataFrame):
        # instead of copy
        dicts = dataframe.to_dict('records')
        local_df = pd.DataFrame(dicts)

        sorted_df = local_df.sort_values(by=['date_time'], ascending=False)

        data = None
        if side == 'bullish':
            # search for local max
            column_high = sorted_df['high']
            index = column_high.idxmax()

            candle = local_df.iloc[index].to_dict()
            data = dict(
                date_time=sorted_df['date_time'].iloc[0],
                confirmation_price=sorted_df['close'].iloc[0],
                break_time=candle['date_time'],
                break_value=candle['high'],
                market='bearish'
            )

        elif side == 'bearish':
            # search for local min
            column_low = sorted_df['low']
            index = column_low.idxmin()

            candle = local_df.iloc[index].to_dict()
            data = dict(
                date_time=sorted_df['date_time'].iloc[0],
                confirmation_price=sorted_df['close'].iloc[0],
                break_time=candle['date_time'],
                break_value=candle['low'],
                market='bullish'
            )

        return data

    def get_waves(self, dataframe, distance):
        copied_df = dataframe.copy()
        collection = dataframe.to_dict('index')

        wave_candles = []

        market = None

        starting_index = 0

        regression_elements = []

        current_extremum = None
        current_extremum_break = None

        for key in collection:
            # establish current trend
            # current_date = collection[key]['date_time']

            current_low = collection[key]['low']
            current_high = collection[key]['high']
            current_close = collection[key]['close']
            medium_value = current_high - (current_high - current_low) / 2

            regression_elements.append(medium_value)
            regression_elements_count = len(regression_elements)

            beta = self.get_beta(
                x=np.arange(1, (regression_elements_count + 1)),
                y=np.array(regression_elements)
            )[1]

            if current_extremum is not None:
                if (beta > 0) & (current_high > current_extremum) & (current_close > current_extremum_break):
                    if market == 'bearish':
                        wave_candles.append(self.get_wave_candle(market, copied_df.iloc[starting_index:(key + 1)]))

                        starting_index = key
                        regression_elements = [medium_value]

                        self.info_on_candidate()

                    market = 'bullish'

                    current_extremum = current_high
                    current_extremum_break = current_low
                elif (beta > 0) & (market == 'bullish') & (
                        ((current_high > current_extremum) & (current_close < current_extremum_break))
                        | (current_close < current_extremum_break)
                        | (current_extremum - current_close > distance)):
                    current_extremum = current_low
                    current_extremum_break = current_high

                    wave_candles.append(self.get_wave_candle(market, copied_df.iloc[starting_index:(key + 1)]))

                    starting_index = key
                    regression_elements = [medium_value]

                    market = 'bearish'

                    self.info_on_candidate()
                elif (beta < 0) & (current_low < current_extremum) & (current_close < current_extremum_break):
                    if market == 'bullish':
                        wave_candles.append(self.get_wave_candle(market, copied_df.iloc[starting_index:(key + 1)]))

                        starting_index = key
                        regression_elements = [medium_value]

                        self.info_on_candidate()

                    market = 'bearish'

                    current_extremum = current_low
                    current_extremum_break = current_high
                elif (beta < 0) & (market == 'bearish') & (
                        ((current_low < current_extremum) & (current_close > current_extremum_break))
                        | (current_close > current_extremum_break)
                        | (current_close - current_extremum > distance)):
                    current_extremum = current_high
                    current_extremum_break = current_low

                    wave_candles.append(self.get_wave_candle(market, copied_df.iloc[starting_index:key + 1]))

                    starting_index = key
                    regression_elements = [medium_value]

                    market = 'bullish'

                    self.info_on_candidate()
                else:
                    pass  # print("Somehow here")
            else:
                if beta > 0:
                    current_extremum = current_high
                    current_extremum_break = current_low
                else:
                    current_extremum = current_low
                    current_extremum_break = current_high

        return pd.DataFrame(wave_candles)

    def analyze(self, dataframe: DataFrame) -> DataFrame:
        local_df = dataframe.copy()

        waves_df = self.get_waves(dataframe=local_df, distance=self.settings['distance'])
        # final result
        with_waves_df = None
        waves_history = len(waves_df)
        if waves_history != 0:
            with_waves_df = pd.merge(local_df, waves_df, how='left')

        return with_waves_df

    @staticmethod
    def plot_chart(symbol, dataframe):
        return None
        draw_df = dataframe.copy()

        fig = make_subplots(rows=1, cols=1, shared_xaxes=True, specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Candlestick(x=draw_df["date_time"],
                                     open=draw_df["open"],
                                     high=draw_df["high"],
                                     low=draw_df["low"],
                                     close=draw_df["close"],
                                     name=symbol),
                      secondary_y=True)

        fig.update_layout(xaxis_rangeslider_visible=False)

        fig.add_trace(
            go.Scatter(
                x=draw_df["break_time"],
                y=draw_df["break_value"],
                mode='lines+markers',
                line=dict(color='black', width=2),
                connectgaps=True,
                name="waves"
            ),
            secondary_y=True
        )

        fig.add_trace(go.Bar(
            x=draw_df['date_time'],
            y=draw_df['volume']),
            secondary_y=False)
        fig.layout.yaxis2.showgrid = False

        fig.show()

    @staticmethod
    def collect_from_api(client, symbol, period) -> DataFrame:
        return ChartLastRequest(client).collect_from_api(symbol, period)

    def run_scenario(self, dataframe, symbol, plot_func=None) -> DataFrame:
        result_df = self.analyze(dataframe)

        if (not result_df.empty) and (plot_func is not None):
            plot_func(symbol, result_df)

        return result_df
