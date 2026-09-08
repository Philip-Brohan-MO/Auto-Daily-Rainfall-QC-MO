# Authors and acknowledgements

This documentation and codebase are currently maintained by
[Philip Brohan](https://brohan.org/) (Met Office).

While the human maintainer is entirely responsible for the project, almost all the work was done by AI: [GitHub Copilot](https://github.com/features/copilot) wrote all the code and almost all the documentation. The secondary QC (neighbour checking) method was much influenced by suggestions from [ChatGPT](https://chatgpt.com/).

If you spot a bug or have a suggestion, please use the issue tracker:
[Raise an issue](https://github.com/Philip-Brohan/Auto-Daily-Rainfall-QC/issues/new).

All errors are the responsibility of the maintainer; credit is widely shared.

- This project was created by Philip Brohan (philip.brohan @ metoffice.gov.uk). He was funded by the Met Office Hadley Centre Climate Programme funded by BEIS, and by the Met Office Weather and Climate Science for Service Partnership (WCSSP) South Africa as part of the Newton Fund.
- This project is the quality-control follow-on to
  [Auto Daily Rainfall](https://brohan.org/Auto-Daily-Rainfall/),
  which produced the ensemble daily transcriptions.
- The station metadata and monthly rainfall records used to georeference daily
  transcriptions come from
  [Rainfall Rescue](https://github.com/ed-hawkins/rainfall-rescue), led by
  [Ed Hawkins](https://climatelabbook.substack.com/p/rainfall-rescue-5-years-on)
  and supported by volunteer transcribers. Ed also suggested using Rainfall Rescue monthly averages to geolocate the daily data, and provided help with the use of RR's outputs.
- Core analysis and pipeline implementation rely on open-source tools including
  [Python](https://www.python.org/),
  [Conda](https://docs.conda.io/en/latest/),
  [DuckDB](https://duckdb.org/),
  [PyArrow](https://arrow.apache.org/docs/python/),
  [XGBoost](https://xgboost.readthedocs.io/),
  [Matplotlib](https://matplotlib.org/), and
  [Plotly](https://plotly.com/python/).
- Documentation is built with
  [Sphinx](https://www.sphinx-doc.org/) and
  [MyST Markdown](https://myst-parser.readthedocs.io/), and hosted through
  [GitHub](https://github.com/) and
  [GitHub Pages](https://pages.github.com/).

Appearance on this page does not imply endorsement by any person or
organization.
