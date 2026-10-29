"""USD/BRL PTAX (sell) per day."""

from _reporting import emit_csv

emit_csv("SELECT dt, sell FROM ptax_daily ORDER BY dt")
