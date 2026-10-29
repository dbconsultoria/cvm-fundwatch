"""Median monthly return per peer group, in BRL and in USD (PTAX)."""

from _reporting import emit_csv

emit_csv("""
    SELECT strptime(ano_mes, '%Y%m')::DATE AS month, peer_group, n_funds,
           median_ret_brl, p25_ret_brl, p75_ret_brl, median_ret_usd
    FROM returns_monthly
    ORDER BY month, peer_group
""")
