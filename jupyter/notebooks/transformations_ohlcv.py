from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *


def transformation_ohlcv(df):
    """
    Transforms the OHLCV DataFrame by calculating additional features.
    
    Parameters:
    df (pd.DataFrame): DataFrame containing OHLCV data with columns 'Open', 'High', 'Low', 'Close', 'Volume'.
    
    """
    # 1. BASIC PRICE TRANSFORMATIONS
    df_basic = df.select(
        "*",
        # Daily price change
        (col("close") - col("open")).alias("daily_change"),
        ((col("close") - col("open")) / col("open") * 100).alias("daily_change_pct"),
        
        # Price range and volatility
        (col("high") - col("low")).alias("daily_range"),
        ((col("high") - col("low")) / col("open") * 100).alias("daily_range_pct"),
        
        # Typical price (used in technical analysis)
        ((col("high") + col("low") + col("close")) / 3).alias("typical_price"),
        
        # OHLC4 average
        ((col("open") + col("high") + col("low") + col("close")) / 4).alias("ohlc4"),
        
        # Volume-weighted price
        (col("close") * col("volume")).alias("volume_weighted_price")
    )

    # 2. TECHNICAL INDICATORS (Window Functions)
    window_spec = Window.partitionBy().orderBy("date")

    df_technical = df_basic.select(
        "*",
        # Simple Moving Averages
        avg("close").over(window_spec.rowsBetween(-6, 0)).alias("sma_7"),
        avg("close").over(window_spec.rowsBetween(-19, 0)).alias("sma_20"),
        avg("close").over(window_spec.rowsBetween(-49, 0)).alias("sma_50"),
        
        # Exponential Moving Average (approximation)
        avg("volume").over(window_spec.rowsBetween(-19, 0)).alias("avg_volume_20"),
        
        # Price momentum
        (col("close") - lag("close", 1).over(window_spec)).alias("price_momentum_1d"),
        (col("close") - lag("close", 7).over(window_spec)).alias("price_momentum_7d"),
        
        # Relative Strength Index components
        greatest(col("close") - lag("close", 1).over(window_spec), lit(0)).alias("gain"),
        greatest(lag("close", 1).over(window_spec) - col("close"), lit(0)).alias("loss"),
        
        # Bollinger Bands components
        stddev("close").over(window_spec.rowsBetween(-19, 0)).alias("price_stddev_20"),
        
        # Volume analysis
        (col("volume") / avg("volume").over(window_spec.rowsBetween(-19, 0))).alias("volume_ratio"),
        
        # High/Low analysis
        max("high").over(window_spec.rowsBetween(-19, 0)).alias("highest_20d"),
        min("low").over(window_spec.rowsBetween(-19, 0)).alias("lowest_20d")
    )

    # 3. ADVANCED ANALYTICS TRANSFORMATIONS
    df_advanced = df_technical.select(
        "*",
        # Bollinger Bands
        (col("sma_20") + 2 * col("price_stddev_20")).alias("bollinger_upper"),
        (col("sma_20") - 2 * col("price_stddev_20")).alias("bollinger_lower"),
        
        # %K Stochastic Oscillator
        ((col("close") - col("lowest_20d")) / (col("highest_20d") - col("lowest_20d")) * 100).alias("stochastic_k"),
        
        # Price position within range
        ((col("close") - col("lowest_20d")) / (col("highest_20d") - col("lowest_20d"))).alias("price_position"),
        
        # Volume surge detection
        when(col("volume_ratio") > 2, "High Volume")
        .when(col("volume_ratio") > 1.5, "Above Average")
        .otherwise("Normal").alias("volume_category"),
        
        # Trend identification
        when(col("close") > col("sma_7"), 1)
        .when(col("close") < col("sma_7"), -1)
        .otherwise(0).alias("short_trend"),
        
        when(col("sma_7") > col("sma_20"), 1)
        .when(col("sma_7") < col("sma_20"), -1)
        .otherwise(0).alias("medium_trend")
    )

    # 4. TIME-BASED AGGREGATIONS
    # Weekly aggregations
    df_weekly = df.withColumn("week", date_trunc("week", col("date"))) \
        .groupBy("week") \
        .agg(
            first("open").alias("week_open"),
            max("high").alias("week_high"),
            min("low").alias("week_low"),
            last("close").alias("week_close"),
            sum("volume").alias("week_volume"),
            count("*").alias("trading_days")
        ) \
        .withColumn("weekly_change_pct", 
                    ((col("week_close") - col("week_open")) / col("week_open") * 100))

    # Monthly aggregations
    df_monthly = df.withColumn("month", date_trunc("month", col("date"))) \
        .groupBy("month") \
        .agg(
            first("open").alias("month_open"),
            max("high").alias("month_high"),
            min("low").alias("month_low"),
            last("close").alias("month_close"),
            sum("volume").alias("month_volume"),
            avg("close").alias("month_avg_price"),
            stddev("close").alias("month_volatility")
        )

    # 5. STATISTICAL TRANSFORMATIONS
    # Rolling correlations and statistics
    window_30d = Window.partitionBy().orderBy("date").rowsBetween(-29, 0)

    df_stats = df_advanced.select(
        "*",
        # Rolling statistics
        skewness("close").over(window_30d).alias("price_skewness_30d"),
        kurtosis("close").over(window_30d).alias("price_kurtosis_30d"),
        
        # Price quartiles
        expr("percentile_approx(close, 0.25)").over(window_30d).alias("price_q1_30d"),
        expr("percentile_approx(close, 0.75)").over(window_30d).alias("price_q3_30d"),
        
        # Z-score for outlier detection
        ((col("close") - col("sma_20")) / col("price_stddev_20")).alias("price_zscore"),
        
        # Volume Z-score
        ((col("volume") - col("avg_volume_20")) / 
        stddev("volume").over(window_spec.rowsBetween(-19, 0))).alias("volume_zscore")
    )

    # 6. FEATURE ENGINEERING FOR ML
    df_ml_features = df_stats.select(
        "*",
        # Lag features for time series prediction
        lag("close", 1).over(window_spec).alias("close_lag_1"),
        lag("close", 2).over(window_spec).alias("close_lag_2"),
        lag("close", 3).over(window_spec).alias("close_lag_3"),
        
        # Rolling features
        (col("close") / col("sma_7") - 1).alias("price_deviation_sma7"),
        (col("close") / col("sma_20") - 1).alias("price_deviation_sma20"),
        
        # Categorical features
        when(col("daily_change_pct") > 5, "Strong_Up")
        .when(col("daily_change_pct") > 2, "Up")
        .when(col("daily_change_pct") > -2, "Neutral")
        .when(col("daily_change_pct") > -5, "Down")
        .otherwise("Strong_Down").alias("price_movement_category"),
        
        # Binary features
        (col("close") > col("open")).cast("int").alias("is_green_candle"),
        (col("volume") > col("avg_volume_20")).cast("int").alias("is_high_volume"),
        
        # Target variable for prediction (next day return)
        ((lead("close", 1).over(window_spec) - col("close")) / col("close")).alias("next_day_return")
    )

    # 7. ANOMALY DETECTION
    df_anomalies = df_ml_features.select(
        "*",
        # Price anomalies
        when(abs(col("price_zscore")) > 2, "Price_Anomaly").otherwise("Normal").alias("price_anomaly_flag"),
        
        # Volume anomalies
        when(abs(col("volume_zscore")) > 2, "Volume_Anomaly").otherwise("Normal").alias("volume_anomaly_flag"),
        
        # Combined anomaly score
        (abs(col("price_zscore")) + abs(col("volume_zscore"))).alias("combined_anomaly_score")
    )

    # 8. MARKET REGIME DETECTION
    df_regime = df_anomalies.select(
        "*",
        # Volatility regime
        when(col("price_stddev_20") > 
            avg("price_stddev_20").over(window_spec.rowsBetween(-99, 0)), "High_Volatility")
        .otherwise("Low_Volatility").alias("volatility_regime"),
        
        # Trend regime
        when((col("short_trend") == 1) & (col("medium_trend") == 1), "Uptrend")
        .when((col("short_trend") == -1) & (col("medium_trend") == -1), "Downtrend")
        .otherwise("Sideways").alias("trend_regime")
    )

    # 9. PERFORMANCE METRICS
    def calculate_performance_metrics(df):
        return df.select(
            "*",
            # Cumulative returns
            sum(col("daily_change_pct")).over(window_spec).alias("cumulative_return"),
            
            # Maximum drawdown calculation
            (col("close") / max("close").over(window_spec.rowsBetween(Window.unboundedPreceding, 0)) - 1).alias("drawdown"),
            
            # Sharpe ratio components (simplified)
            avg("daily_change_pct").over(window_spec.rowsBetween(-29, 0)).alias("avg_return_30d"),
            stddev("daily_change_pct").over(window_spec.rowsBetween(-29, 0)).alias("return_volatility_30d")
        )

    df_performance = calculate_performance_metrics(df_regime)

    return df_performance