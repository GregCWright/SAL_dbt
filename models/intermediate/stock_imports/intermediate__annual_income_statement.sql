with source as (
    select
        *
    from {{ ref("source__annual_income_statement") }}
)

, standardized as (
    select
        {{ standardize_date('fiscal_date_ending') }} as fiscal_date_ending
        , {{ standardize_varchar('symbol', 5) }} as symbol
        , {{ standardize_varchar('reported_currency', 5) }} as reported_currency
        , {{ standardize_number('gross_profit', 'bigint') }} as gross_profit
        , {{ standardize_number('total_revenue', 'bigint') }} as total_revenue
        , {{ standardize_number('cost_of_revenue', 'bigint') }} as cost_of_revenue
        , {{ standardize_number('cost_of_goods_and_services_sold', 'bigint') }} as cost_of_goods_and_services_sold
        , {{ standardize_number('operating_income', 'bigint') }} as operating_income
        , {{ standardize_number('selling_general_and_administrative', 'bigint') }} as selling_general_and_administrative
        , {{ standardize_number('research_and_development', 'bigint') }} as research_and_development
        , {{ standardize_number('operating_expenses', 'bigint') }} as operating_expenses
        , {{ standardize_number('investment_income_net', 'bigint') }} as investment_income_net
        , {{ standardize_number('net_interest_income', 'bigint') }} as net_interest_income
        , {{ standardize_number('interest_income', 'bigint') }} as interest_income
        , {{ standardize_number('interest_expense', 'bigint') }} as interest_expense
        , {{ standardize_number('non_interest_income', 'bigint') }} as non_interest_income
        , {{ standardize_number('other_non_operating_income', 'bigint') }} as other_non_operating_income
        , {{ standardize_number('depreciation', 'bigint') }} as depreciation
        , {{ standardize_number('depreciation_and_amortization', 'bigint') }} as depreciation_and_amortization
        , {{ standardize_number('income_before_tax', 'bigint') }} as income_before_tax
        , {{ standardize_number('income_tax_expense', 'bigint') }} as income_tax_expense
        , {{ standardize_number('interest_and_dept_expense', 'bigint') }} as interest_and_dept_expense
        , {{ standardize_number('net_income_from_continuting_operations', 'bigint') }} as net_income_from_continuting_operations
        , {{ standardize_number('comprehensive_income_net_of_tax', 'bigint') }} as comprehensive_income_net_of_tax
        , {{ standardize_number('earnings_before_interest_taxes', 'bigint') }} as earnings_before_interest_taxes
        , {{ standardize_number('earnings_before_interest_taxes_depreciation_amortization', 'bigint') }} as earnings_before_interest_taxes_depreciation_amortization
        , {{ standardize_number('net_income', 'bigint') }} as net_income
        , {{ standardize_datetime('execution_time') }} as execution_time
    from source
)

, deduped as (
    select
        fiscal_date_ending
        , symbol
        , reported_currency
        , gross_profit
        , total_revenue
        , cost_of_revenue
        , cost_of_goods_and_services_sold
        , operating_income
        , selling_general_and_administrative
        , research_and_development
        , operating_expenses
        , investment_income_net
        , net_interest_income
        , interest_income
        , interest_expense
        , non_interest_income
        , other_non_operating_income
        , depreciation
        , depreciation_and_amortization
        , income_before_tax
        , income_tax_expense
        , interest_and_dept_expense
        , net_income_from_continuting_operations
        , comprehensive_income_net_of_tax
        , earnings_before_interest_taxes
        , earnings_before_interest_taxes_depreciation_amortization
        , net_income
        , execution_time
    from {{ dedupe_multiple("standardized", "fiscal_date_ending", "symbol", "execution_time") }}
)

select * from deduped
