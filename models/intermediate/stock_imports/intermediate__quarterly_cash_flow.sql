with source as (
    select
        *
    from {{ ref("source__quarterly_cash_flow") }}
)

, standardized as (
    select
        {{ standardize_date('fiscal_date_ending') }} as fiscal_date_ending
        , {{ standardize_varchar('symbol', 5) }} as symbol
        , {{ standardize_varchar('reported_currency', 5) }} as reported_currency
        , {{ standardize_number('operating_cashflow', 'bigint') }} as operating_cashflow
        , {{ standardize_number('payments_for_operating_activities', 'bigint') }} as payments_for_operating_activities
        , {{ standardize_number('proceeds_from_operating_activities', 'bigint') }} as proceeds_from_operating_activities
        , {{ standardize_number('change_in_operating_liabilities', 'bigint') }} as change_in_operating_liabilities
        , {{ standardize_number('change_in_operating_assets', 'bigint') }} as change_in_operating_assets
        , {{ standardize_number('depreciation_depletion_and_amortization', 'bigint') }} as depreciation_depletion_and_amortization
        , {{ standardize_number('captial_expenditures', 'bigint') }} as captial_expenditures
        , {{ standardize_number('change_in_receivables', 'bigint') }} as change_in_receivables
        , {{ standardize_number('change_in_inventory', 'bigint') }} as change_in_inventory
        , {{ standardize_number('profit_loss', 'bigint') }} as profit_loss
        , {{ standardize_number('cashflow_from_investment', 'bigint') }} as cashflow_from_investment
        , {{ standardize_number('cashflow_from_financing', 'bigint') }} as cashflow_from_financing
        , {{ standardize_number('proceeds_from_repayments_of_short_term_debt', 'bigint') }} as proceeds_from_repayments_of_short_term_debt
        , {{ standardize_number('payments_for_repurchase_of_common_stock', 'bigint') }} as payments_for_repurchase_of_common_stock
        , {{ standardize_number('payments_for_repurchase_of_equity', 'bigint') }} as payments_for_repurchase_of_equity
        , {{ standardize_number('payments_for_repurchase_of_preferred_stock', 'bigint') }} as payments_for_repurchase_of_preferred_stock
        , {{ standardize_number('dividend_payout', 'bigint') }} as dividend_payout
        , {{ standardize_number('dividend_payout_common_stock', 'bigint') }} as dividend_payout_common_stock
        , {{ standardize_number('dividend_payout_preferred_stock', 'bigint') }} as dividend_payout_preferred_stock
        , {{ standardize_number('proceeds_from_issuance_of_common_stock', 'bigint') }} as proceeds_from_issuance_of_common_stock
        , {{ standardize_number('proceeds_from_issuance_of_long_term_debt_and_capital_securities_net', 'bigint') }} as proceeds_from_issuance_of_long_term_debt_and_capital_securities_net
        , {{ standardize_number('proceeds_from_issuance_of_preferred_stock', 'bigint') }} as proceeds_from_issuance_of_preferred_stock
        , {{ standardize_number('proceeds_from_repurchase_of_equity', 'bigint') }} as proceeds_from_repurchase_of_equity
        , {{ standardize_number('proceeds_from_sale_of_treasury_stock', 'bigint') }} as proceeds_from_sale_of_treasury_stock
        , {{ standardize_number('change_in_cash_and_cash_equivalents', 'bigint') }} as change_in_cash_and_cash_equivalents
        , {{ standardize_number('change_in_exchange_rate', 'bigint') }} as change_in_exchange_rate
        , {{ standardize_number('net_income', 'bigint') }} as net_income
        , {{ standardize_datetime('execution_time') }} as execution_time
    from source
)

, deduped as (
    select
        fiscal_date_ending
        , symbol
        , reported_currency
        , operating_cashflow
        , payments_for_operating_activities
        , proceeds_from_operating_activities
        , change_in_operating_liabilities
        , change_in_operating_assets
        , depreciation_depletion_and_amortization
        , captial_expenditures
        , change_in_receivables
        , change_in_inventory
        , profit_loss
        , cashflow_from_investment
        , cashflow_from_financing
        , proceeds_from_repayments_of_short_term_debt
        , payments_for_repurchase_of_common_stock
        , payments_for_repurchase_of_equity
        , payments_for_repurchase_of_preferred_stock
        , dividend_payout
        , dividend_payout_common_stock
        , dividend_payout_preferred_stock
        , proceeds_from_issuance_of_common_stock
        , proceeds_from_issuance_of_long_term_debt_and_capital_securities_net
        , proceeds_from_issuance_of_preferred_stock
        , proceeds_from_repurchase_of_equity
        , proceeds_from_sale_of_treasury_stock
        , change_in_cash_and_cash_equivalents
        , change_in_exchange_rate
        , net_income
        , execution_time
    from {{ dedupe_multiple("standardized", "fiscal_date_ending", "symbol", "execution_time") }}
)

select * from deduped
