with source as (
    select
        *
    from {{ ref("test_overview") }}
)

, standardized as (
    select
        {{ standardize_varchar('symbol', 5) }} as symbol
        , {{ standardize_varchar('asset_type', 32) }} as asset_type
        , {{ standardize_varchar('name', 64) }} as name
        , {{ standardize_varchar('description', 2056) }} as description
        , {{ standardize_varchar('central_index_key', 8) }} as central_index_key
        , {{ standardize_varchar('exchange', 8) }} as exchange
        , {{ standardize_varchar('currency', 5) }} as currency
        , {{ standardize_varchar('country', 5) }} as country
        , {{ standardize_varchar('sector', 32) }} as sector
        , {{ standardize_varchar('industry', 32) }} as industry
        , {{ standardize_varchar('address', 64) }} as address
        , {{ standardize_varchar('fiscal_year_end', 16) }} as fiscal_year_end
        , {{ standardize_date('latest_quarter') }} as latest_quarter
        , {{ standardize_number('market_capitalization', 'bigint') }} as market_capitalization
        , {{ standardize_number('interest_before_interest_taxes_amortization', 'bigint') }} as interest_before_interest_taxes_amortization
        , {{ standardize_number('price_earnings_ratio', 'float') }} as price_earnings_ratio
        , {{ standardize_number('price_earnings_growth_ratio', 'float') }} as price_earnings_growth_ratio
        , {{ standardize_number('book_value', 'float') }} as book_value
        , {{ standardize_number('dividend_per_share', 'float') }} as dividend_per_share
        , {{ standardize_number('dividend_yield', 'float') }} as dividend_yield
        , {{ standardize_number('earnings_per_share', 'float') }} as earnings_per_share
        , {{ standardize_number('revenue_per_share_trailing_twelve_months', 'float') }} as revenue_per_share_trailing_twelve_months
        , {{ standardize_number('profit_margin', 'float') }} as profit_margin
        , {{ standardize_number('operating_margin_trailing_twelve_months', 'float') }} as operating_margin_trailing_twelve_months
        , {{ standardize_number('return_on_assets_trailing_twelve_months', 'float') }} as return_on_assets_trailing_twelve_months
        , {{ standardize_number('return_on_equity_trailing_twelve_months', 'float') }} as return_on_equity_trailing_twelve_months
        , {{ standardize_number('revenue_trailing_twelve_months', 'bigint') }} as revenue_trailing_twelve_months
        , {{ standardize_number('gross_profit_trailing_twelve_months', 'bigint') }} as gross_profit_trailing_twelve_months
        , {{ standardize_number('diluted_earnings_per_share_trailing_twelve_months', 'float') }} as diluted_earnings_per_share_trailing_twelve_months
        , {{ standardize_number('quarterly_earnings_growth_year_on_year', 'float') }} as quarterly_earnings_growth_year_on_year
        , {{ standardize_number('quarterly_revenue_growth_year_on_year', 'float') }} as quarterly_revenue_growth_year_on_year
        , {{ standardize_number('analyst_target_price', 'float') }} as analyst_target_price
        , {{ standardize_number('trailing_price_to_earnings_ratio', 'float') }} as trailing_price_to_earnings_ratio
        , {{ standardize_number('forward_price_to_earnings_ratio', 'float') }} as forward_price_to_earnings_ratio
        , {{ standardize_number('price_to_sales_ratio_trailing_twelve_months', 'float') }} as price_to_sales_ratio_trailing_twelve_months
        , {{ standardize_number('price_to_book_ratio', 'float') }} as price_to_book_ratio
        , {{ standardize_number('enterprise_value_to_revenue_ratio', 'float') }} as enterprise_value_to_revenue_ratio
        , {{ standardize_number('enterprise_value_to_interest_before_interest_taxes_amortization_ratio', 'float') }} as enterprise_value_to_interest_before_interest_taxes_amortization_ratio
        , {{ standardize_number('beta', 'float') }} as beta
        , {{ standardize_number('fifty_two_week_high', 'float') }} as fifty_two_week_high
        , {{ standardize_number('fifty_two_week_low', 'float') }} as fifty_two_week_low
        , {{ standardize_number('fifty_day_moving_average', 'float') }} as fifty_day_moving_average
        , {{ standardize_number('two_hundred_day_moving_average', 'float') }} as two_hundred_day_moving_average
        , {{ standardize_number('shares_outstanding', 'bigint') }} as shares_outstanding
        , {{ standardize_date('dividend_date') }} as dividend_date
        , {{ standardize_date('ex_dividend_date') }} as ex_dividend_date
        , {{ standardize_datetime('execution_time') }} as execution_time
    from source
)

select * from standardized
