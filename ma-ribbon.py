""" Include data sets and pipeline """

import numpy as np
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data import morningstar
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, SimpleMovingAverage
from quantopian.pipeline.filters.morningstar import Q1500US
from quantopian.pipeline.factors import CustomFactor

class MarketCap(CustomFactor):

    inputs = [morningstar.valuation.shares_outstanding, USEquityPricing.close]
    window_length = 1

    def compute(self, today, assets, out, shares, close_price):
        # Calculation : shares * price/share = total price = market cap
        out[:] = shares * close_price

class WeeklyReturn(CustomFactor):

    inputs = [USEquityPricing.close]
    window_length = 2

    def compute(self, today, assets, out, close):
        out[:] = (close[-1]/close[0] - 1) * 100

def initialize(context):

    # Setting commission and slippage
    # set_commission(commission.PerTrade(cost=6.75))
    # set_slippage(slippage.VolumeShareSlippage(volume_limit=0.025, price_impact=0.1))

    # Rebalance every day, 1 hour after market open.
    schedule_function(my_rebalance, date_rules.every_day(), time_rules.market_open(hours=1))

    # Record tracking variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())

    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'my_pipeline')

def make_pipeline():

    # Base universe set to the Q500US
    base_universe = Q1500US()

    # Factor of yesterday's OCHLV values
    yesterday_close = USEquityPricing.close.latest
    yesterday_volume = USEquityPricing.volume.latest
    weekly_return = WeeklyReturn()

    # Filters for universe of securities
    above_micro = (MarketCap() >= 50000000)
    price_range = (USEquityPricing.close.latest >= 1 and USEquityPricing.close.latest <= 15.00)
    highly_liquid = yesterday_volume >= 1000000

    # Classifiers for long / short positions
    long_position = weekly_return > 0
    short_position = weekly_return < 0

    # Combine filters to screen tradable securities
    is_tradable = above_micro & price_range & highly_liquid & (long_position | short_position)

    pipe = Pipeline(
        screen = base_universe & is_tradable,
        columns = {
            'close': yesterday_close,
            'volume': yesterday_volume,
            'longs': long_position,
            'shorts': short_position
        }
    )

    return pipe

def before_trading_start(context, data):
    """ Called every day before market open. """

    context.benchmark = symbol('SPY')
    mean_group = {}
    sum_distance = []
    allocations = []

    for i in range(2,25,2):
        # Get the price history of the SPY
        benchmark_history = data.history(context.benchmark, fields='price', bar_count=i, frequency='1d' )
        current_mean = benchmark_history.mean()
        mean_group[i] = current_mean

    for i in range(2,25,2):
        if i < 23:
            # Sum the distance between each moving average and append to the sum_distance array
            sum_distance.append((mean_group[i]) - (mean_group[i + 2]))

    high_low = np.percentile(sum_distance, [0, 100])
    abs_diff = sum(abs(high_low))

    for i in range(0, len(sum_distance)):
        allocate = (sum_distance[i] / abs_diff)
        allocations.append(allocate)

    # The % allocations for long / short orders and position sizing
    long_allocations = 0.5 + np.average(allocations)
    short_allocations = 1 - long_allocations

    # Calculated allocations based on
    context.allocations = np.average(allocations)
    context.long_allocations = long_allocations
    context.short_allocations = short_allocations

    # These are the securities that we are interested in trading each day
    context.output = pipeline_output('my_pipeline')
    context.security_list = context.output.index
    context.longs = context.output[context.output['longs']].index
    context.shorts = context.output[context.output['shorts']].index


def my_assign_weights(context, data):
    """ Assign weights to securities that we want to order. """
    pass

def my_rebalance(context,data):
    """ Execute orders according to our schedule_function() timing. """

    my_positions = context.portfolio.positions

    if (len(context.longs) > 0) and (len(context.shorts) > 0):

        long_weight = context.long_allocations/len(context.longs)
        short_weight = context.short_allocations/len(context.shorts)

        # Open long positions in our high weekly return stocks.
        for security in context.longs:
            if data.can_trade(security):
                if security not in my_positions:
                    order_target_percent(security, long_weight)

        # Open short positions in our low weekly return stocks.
        for security in context.shorts:
            if data.can_trade(security):
                if security not in my_positions:
                    order_target_percent(security, -short_weight)

    daily_clean(context, data)

# make sure all untradeable securities are sold off each day
def daily_clean(context, data):
    """ Removing any stocks that don't meet the criteria set out in our pipeline. """

    for stock in context.portfolio.positions:
        if stock not in context.security_list and data.can_trade(stock):
            order_target_percent(stock, 0)

def my_record_vars(context, data):
    """ Plot variables at the end of each day. """

    record(allocations=context.allocations)
    record(long_allocations=context.long_allocations)
    record(short_allocations=context.short_allocations)
    record(number_of_longs=len(context.longs))
    record(number_of_shorts=len(context.shorts))

    
