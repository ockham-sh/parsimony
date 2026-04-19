# parsimony-sdmx

SDMX connectors for [parsimony](https://parsimony.dev) — typed access to
ECB, Eurostat, IMF, World Bank, BIS and other SDMX agencies.

## Install

```bash
uv pip install parsimony-sdmx
```

## Usage

The package self-registers with parsimony core via the
`parsimony.providers` entry-point group, so connectors appear automatically
in `build_connectors_from_env()`.

Direct use:

```python
from parsimony_sdmx import sdmx, SdmxFetchParams

result = await sdmx(
    SdmxFetchParams(dataset_key="ECB-YC", series_key="B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y")
)
```

## What's included

| Connector | Purpose |
|---|---|
| `sdmx` | Fetch series data |
| `sdmx_list_datasets` | List dataflows for an agency |
| `sdmx_dsd` | Inspect a dataset's Data Structure Definition |
| `sdmx_codelist` | Resolve a single codelist for a dimension |
| `sdmx_series_keys` | Enumerate series keys for a dataset |
| `enumerate_sdmx_dataset_codelists` | Index-time helper: one table per codelist |

## License

Apache-2.0
