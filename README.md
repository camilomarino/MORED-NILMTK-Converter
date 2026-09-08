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

Dataset reference: Ahajjam et al. (2020),
[MORED: A Moroccan Buildings' Electricity Consumption Dataset](https://doi.org/10.3390/en13246737).
Converter author: Camilo Mariño. Downloaded data remain subject to the dataset
publisher's terms. This repository does not currently declare a separate license
for converter code; confirm contribution licensing before submitting upstream.
