with source as (
    select
        *
    from {{ ref("source__annual_balance_sheet") }}
)

, standardized as (
    select
        {{ standardize_date('fiscal_date_ending') }} as fiscal_date_ending
        , {{ standardize_varchar('symbol', 5) }} as symbol
        , {{ standardize_varchar('reported_currency', 5) }} as reported_currency
        , {{ standardize_number('total_assets', 'bigint') }} as total_assets
        , {{ standardize_number('total_current_assets', 'bigint') }} as total_current_assets
        , {{ standardize_number('cash_and_cash_equivalents_at_carrying_value', 'bigint') }} as cash_and_cash_equivalents_at_carrying_value
        , {{ standardize_number('cash_and_short_term_investments', 'bigint') }} as cash_and_short_term_investments
        , {{ standardize_number('inventory', 'bigint') }} as inventory
        , {{ standardize_number('current_net_receivables', 'bigint') }} as current_net_receivables
        , {{ standardize_number('total_non_current_assets', 'bigint') }} as total_non_current_assets
        , {{ standardize_number('property_plant_equipment', 'bigint') }} as property_plant_equipment
        , {{ standardize_number('accumulated_depreciation_amortization_property_plant_equipment', 'bigint') }} as accumulated_depreciation_amortization_property_plant_equipment
        , {{ standardize_number('intangible_assets', 'bigint') }} as intangible_assets
        , {{ standardize_number('intangible_assets_excluding_goodwill', 'bigint') }} as intangible_assets_excluding_goodwill
        , {{ standardize_number('goodwill', 'bigint') }} as goodwill
        , {{ standardize_number('investments', 'bigint') }} as investments
        , {{ standardize_number('long_term_investments', 'bigint') }} as long_term_investments
        , {{ standardize_number('short_term_investments', 'bigint') }} as short_term_investments
        , {{ standardize_number('other_current_assets', 'bigint') }} as other_current_assets
        , {{ standardize_number('other_non_current_assets', 'bigint') }} as other_non_current_assets
        , {{ standardize_number('total_liabilities', 'bigint') }} as total_liabilities
        , {{ standardize_number('total_current_liabilities', 'bigint') }} as total_current_liabilities
        , {{ standardize_number('current_accounts_payable', 'bigint') }} as current_accounts_payable
        , {{ standardize_number('deferred_revenue', 'bigint') }} as deferred_revenue
        , {{ standardize_number('current_debt', 'bigint') }} as current_debt
        , {{ standardize_number('short_term_debt', 'bigint') }} as short_term_debt
        , {{ standardize_number('total_non_current_liabilities', 'bigint') }} as total_non_current_liabilities
        , {{ standardize_number('capital_lease_obligations', 'bigint') }} as capital_lease_obligations
        , {{ standardize_number('long_term_debt', 'bigint') }} as long_term_debt
        , {{ standardize_number('current_long_term_debt', 'bigint') }} as current_long_term_debt
        , {{ standardize_number('long_term_debt_non_current', 'bigint') }} as long_term_debt_non_current
        , {{ standardize_number('short_long_term_debt_total', 'bigint') }} as short_long_term_debt_total
        , {{ standardize_number('other_current_liabilities', 'bigint') }} as other_current_liabilities
        , {{ standardize_number('other_non_current_liabilities', 'bigint') }} as other_non_current_liabilities
        , {{ standardize_number('total_shareholder_equity', 'bigint') }} as total_shareholder_equity
        , {{ standardize_number('treasury_stock', 'bigint') }} as treasury_stock
        , {{ standardize_number('retained_earnings', 'bigint') }} as retained_earnings
        , {{ standardize_number('common_stock', 'bigint') }} as common_stock
        , {{ standardize_number('common_stock_shares_outstanding', 'bigint') }} as common_stock_shares_outstanding
        , {{ standardize_datetime('execution_time') }} as execution_time
    from source
)

, deduped as (
    select
        *
    from {{ dedupe_multiple("standardized", "fiscal_date_ending", "symbol", "execution_time") }}
)

select * from deduped
