// Observable Framework configuration (ADR 0034).
// Data loaders are Python: CVM_PYTHON points at the project virtualenv interpreter and
// REPORTING_DIR at a folder holding gold/reporting/*.parquet (see dashboard/README.md).
export default {
  title: "cvm-fundwatch",
  root: "src",
  pages: [
    {name: "Returns in BRL and USD", path: "/returns"},
    {name: "Anomalies", path: "/anomalies"},
    {name: "About the data", path: "/about"}
  ],
  interpreters: {
    ".py": [process.env.CVM_PYTHON ?? "python3"]
  },
  theme: ["air", "near-midnight"],
  search: false,
  footer:
    "Data: CVM and Banco Central do Brasil open data, republished as is. " +
    'Code: <a href="https://github.com/dbconsultoria/cvm-fundwatch">github.com/dbconsultoria/cvm-fundwatch</a>.'
};
