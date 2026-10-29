"""Reports CVM corrected after publishing, per reference month, and how many were flagged."""

from _reporting import emit_csv

emit_csv("""
    SELECT strptime(ano_mes, '%Y%m')::DATE AS month,
           sum(changed_reports) AS changed_reports, sum(flagged_before) AS flagged_before
    FROM restatements_monthly
    GROUP BY 1
    ORDER BY 1
""")
