# Evidence: the two copies of the source CSV

| | A: authoritative (Kaggle) | B: copy inside the old project zip |
|---|---|---|
| File | `industrial_fire_risk_data.csv` | `industrial_fire_risk_data.csv` |
| SHA-256 | `5db607c512fad903bcfb6e242e2dcfc6ba8f9de091becf6169e6ae5414f309ba` | `6457d041b0215399f867ee0855aa136f859ff541714ebb8fed56cb4a43b44536` |
| Size (bytes) | 11,564,951 | 11,311,983 |
| Line endings | CRLF | CRLF |
| First data line | `2021-01-01,00:00:00,Factory_A,Urban,Day,7.0,Junior,Yes,26.49,25.31,54.18,1.63,1007.06,17,202,8.9,1,Off,56.47,0` | `01/01/2021,0:00:00,Factory_A,Urban,Day,7,Junior,Yes,26.49,25.31,54.18,1.63,1007.06,17,202,8.9,1,Off,56.47,0` |
| Last data line | `2023-11-08,15:45:00,Factory_D,Industrial_Zone,Night,23.0,Senior,Yes,16.7,16.22,79.56,1.94,1389.73,17,231,6.55,3,Off,38.8,0` | `08/11/2023,15:45:00,Factory_D,Industrial_Zone,Night,23,Senior,Yes,16.7,16.22,79.56,1.94,1389.73,17,231,6.55,3,Off,38.8,0` |

| Record-level check | Result |
|---|---|
| Same column names and order | yes |
| Same row count | yes |
| Same timestamps (A parsed as `%Y-%m-%d %H:%M:%S`, B as `%d/%m/%Y %H:%M:%S`) | yes |
| All other 18 columns equal value by value (NaN = NaN) | yes |

**Conclusion:** the files are different bytes but the same logical records. Copy B was re-saved by a spreadsheet program (dates became DD/MM/YYYY, hours lost the leading zero, whole numbers lost the trailing `.0`). The pipeline uses copy A and its checksum is the one recorded in `metadata/source_manifest.yml`.
