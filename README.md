# MORED WPILGT converter for NILMTK

Convert the whole-premises and individual-load ground-truth (WPILGT) portion of
[MORED](https://moredataset.github.io/MORED/) to a NILMTK HDF5 dataset. The converter
supports all ten premises, or any subset of complete premises. It does not
convert the separate WP-only or high-frequency ILS releases.

## Install and run

Use Python 3.11 or later with a working installation of
[NILMTK](https://github.com/nilmtk/nilmtk). For a reproducible test environment:

```bash
uv venv --python 3.11
uv pip install -r requirements-test.txt
.venv/bin/python -m pytest
```

Download the WPILGT files from the dataset website. Arrange the CSVs as shown in
[structure.txt](structure.txt), retaining the original names, for example
`data/Premises_1/Mains.csv`. Event annotations are not meter readings and are not
converted. Run from any working directory:

```bash
python /path/to/MORED-NILMTK-Converter/converter_mored.py /path/to/data mored.h5
```

Or import the converter from this repository:

```python
from converter_mored import convert_mored
from nilmtk import DataSet

convert_mored("/path/to/data", "mored.h5")
with DataSet("mored.h5") as dataset:
    print(dataset.buildings)
```

The output filename follows NILMTK's write-mode convention: an existing HDF5
file is replaced. Input files are checked before opening the output, and the
store is closed even if conversion fails. A failed conversion may leave a
partial HDF5 file; remove it or rerun the conversion before using the result.

## Data interpretation

- CSV columns are selected by name: `timestamp`, `Vrms` (volts), and `real_power`
  (watts). Their order in the source file does not change their meaning.
- Measurement columns use NILMTK's two-level labels and `float32` values.
- Timestamps are sorted and unique. The first valid reading at a duplicate
  timestamp is retained. Rows without a timestamp or without any finite
  measurement are discarded; a missing voltage does not discard valid power.
- Both `day/month/year hour:minute:second` and the English-month format used by
  `Premises_10/TV.csv` are supported. Invalid nonempty timestamps raise an error.
- There is no resampling, interpolation, clipping, or synthesized aggregate.
- Premises 9 contains only meters 2-6: the authors did not provide mains data.
  `dataset.buildings[9].elec.mains()` returns `None`. Mains-based metrics and
  wiring queries requiring a measured upstream meter cannot be used there.
- Metadata describe only the premises present in the input. The freezer in
  premises 7 is read from `Premises_7/Freezer.csv`.

### Timezone assumption

The published timestamps have no UTC offset. The original converter interpreted
them as UTC; that behavior is preserved and output is represented in
`Africa/Casablanca`, matching the dataset metadata's IANA timezone. The authors'
metadata say `GMT+01:00` but do not establish whether the CSV wall times are UTC
or local. The absolute UTC interpretation is therefore **not independently
confirmed**. If the source export is known to use local time, select it explicitly:

```python
convert_mored("/path/to/data", "mored.h5", source_timezone="Africa/Casablanca")
```

The command-line equivalent is `--source-timezone Africa/Casablanca`. Ambiguous
or nonexistent local times raise rather than silently shifting data.

The [dataset paper, section 3.3.2](https://doi.org/10.3390/en13246737)
says the acquisition devices synchronize with the country's clock. This suggests
local acquisition time, but does not specify the timezone used when exporting
CSV strings. The source metadata and this statement do not independently verify
the inherited UTC default. Events are derived from the same measurements and
cannot establish an absolute timezone independently.

### Output format

Only HDF5 is supported (`format="HDF"`). The current NILMTK CSV reader does not
reliably round-trip empty measurement subtypes and mixed-precision zoned
timestamps; CSV is rejected before opening the output.

## Validation and provenance

The tests generate small synthetic datasets for all 53 meters in all ten
premises, reopen the HDF5 result with `DataSet`, and check measurements, meter
paths, metadata, timestamps, subsets, and error cleanup. No raw measurements are
redistributed in the tests. Dataset YAML is adapted from the authors' metadata;
their other appliance descriptions (including washer/dryer classifications)
are retained rather than inferred from filenames alone.

A full WPILGT download was validated on 2026-09-08 using an installed NILMTK
integration wheel: all 44,172,557 readings across 53 meters matched an independent
CSV-to-HDF5 comparison exactly. Loading/resampling, energy and good-section
statistics were exercised for every meter. A two-appliance CO train/predict run
on household 1 also passed; this is an integration check, not an accuracy study.
The timestamp comparison uses the documented UTC source-time assumption.

The complete official `Premises_9/TV.csv` and `Premises_9/Refrigerator.csv` files
were byte-identical (6,707,937 bytes each). Both published channels are preserved;
the converter cannot infer which source label or recording should be corrected.

A fresh download from the current official folder reproduced this result using
two distinct Drive file IDs. Both files have SHA-256
`4da063176dccb8febd240b12ec192326c498c91e3c45f5b4c726522163fb795b`.
The published events also label TV and refrigerator at exactly the same 200,686
timestamps. The provider's [2022 link fix](https://github.com/MOREDataset/MORED/commit/94e525c589c638b14e00cdc593591cba5f319616)
corrected a link pointing to premises 2; the validation already uses the corrected
premises-9 folder. No replacement recording was identified in the reviewed
provider repository, history, or public issue. Treat these two channels as
potentially duplicated source data when choosing evaluation appliances.


Dataset reference: Ahajjam et al. (2020),
[MORED: A Moroccan Buildings' Electricity Consumption Dataset](https://doi.org/10.3390/en13246737).
Converter author: Camilo Mariño. Downloaded data remain subject to the dataset
publisher's terms.

## Code license

Copyright 2026 Camilo Mariño. The converter code, tests, and original
documentation are licensed under [Apache License 2.0](LICENSE), matching NILMTK.
This permits use, modification, and redistribution, including commercial use,
subject to the license terms.

This license grant does not cover third-party datasets or metadata adapted from
the dataset authors. Their original terms and attribution remain applicable;
conversion does not relicense those materials.
