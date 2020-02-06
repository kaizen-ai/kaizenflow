<!--ts-->
Table of Contents
=================

   * [WIND Automax](#wind-automax)
      * [The flow](#the-flow)
         * [1. automax](#1-automax)
         * [2. OCR for Automax results](#2-ocr-for-automax-results)
         * [3. automax_indicators](#3-automax_indicators)
         * [4. OCR for automax_indicators](#4-ocr-for-automax_indicators)

<!--te-->

# WIND Automax

The goal was to obtain WIND Commodity DB metadata. WIND has a restriction on
the number of metadata files a user can load, so we automated the process of
"walking" WIND menu and taking screenshots of the metadata, and used OCR to
extract text from the screenshots.

## The flow

### 1. automax

Take screenshots of the first two levels of the metadata using [PyAutoGUI](https://pyautogui.readthedocs.io/en/latest/).

The code is located at [vendors/wind/automax.py](https://github.com/ParticleDev/commodity_research/blob/master/vendors/wind/automax.py).
- Up to `Commodity Index` and `Commodity Market` categories (they have
  different structure)
  - [WIND: Create Excel files with metadata for Commodity DB #757](https://github.com/ParticleDev/commodity_research/issues/757)
- `Commodity Index` and `Commodity Market` categories
  - [WIND: Create Excel files with metadata for Commodity Index and Market #758](https://github.com/ParticleDev/commodity_research/issues/758)

The screenshots are located in [`Wind_terminal/screensots_indicators_good`](https://drive.google.com/drive/u/0/folders/1jTrkc_P2xy4TLO1zzXB6MsTfSQInpw4F).

### 2. OCR for Automax results

We tried Tesseract and Amazon's Textract for OCR. Textract showed much better
results, so we used it for recognizing WIND data here and below.
- Extract text from the screenshots in [vendors/wind/extract_text_from_images.py](https://github.com/ParticleDev/commodity_research/blob/master/vendors/wind/extract_text_from_images.py)
- Combine the extracted data in [vendors/wind/PartTask799_DATA_wind_combine_data_screenshots_indicators.ipynb](https://github.com/ParticleDev/commodity_research/blob/master/vendors/wind/PartTask799_DATA_wind_combine_data_screenshots_indicators.ipynb)

The recognized data is located at `Wind_terminal/screenshots_indicators_good_csv/all_commodities_mapped_v3.csv`
  - GitHub reference: [WIND: OCR to build mapping commodity -> data #799 (comment)](https://github.com/ParticleDev/commodity_research/issues/799#issuecomment-567579131)

### 3. automax_indicators

Using the mapping obtained above, take screenshots of four levels of Commodity
DB menu (up to `Commodity Index`and `Commodity Market`).

The code is located at [vendors/wind/automax_indicators.py](https://github.com/ParticleDev/commodity_research/blob/master/vendors/wind/automax_indicators.py).

WIND terminal stops responding after running for several hours, so we had to run
the script multiple times. The history of the runs:
- [WIND: OCR to build mapping commodity -> data #799 (comment #1)](https://github.com/ParticleDev/commodity_research/issues/799#issuecomment-568909587)
- [WIND: OCR to build mapping commodity -> data #799 (comment #2)](https://github.com/ParticleDev/commodity_research/issues/799#issuecomment-569682734)

The screenshots are located in the following directories:
 - `/s3/default00-bucket/wind/datasets/Wind_terminal/screenshots_PartTask799_4levels_20191225`
 - `/s3/default00-bucket/wind/datasets/Wind_terminal/screenshots_PartTask799_4levels_20191225_20191230_missing_files/`

### 4. OCR for automax_indicators

- GitHub reference: 
   - [WIND: OCR to build mapping commodity -> data #799 (comment)](https://github.com/ParticleDev/commodity_research/issues/799#issuecomment-569095135)
   - [WIND: Extract metadata through screenshots #774 (linked comment and down)](https://github.com/ParticleDev/commodity_research/issues/774#issuecomment-580356330)
- The code: [vendors/wind/extract_tables_from_images.py](https://github.com/ParticleDev/commodity_research/blob/master/vendors/wind/extract_tables_from_images.py)

The resulting .csv that combines data from screenshots in both aforementioned directories is located at:
 - S3 bucket: `/s3/default00-bucket/wind/datasets/Wind_terminal/WIND_metadata_table.csv`
 - [Google Drive](https://docs.google.com/spreadsheets/d/1i_H1N4E81oFUB6O1Y8rJ95q4UFEpIIdv_Bhim-4Z4kE/edit#gid=1549615534)
 