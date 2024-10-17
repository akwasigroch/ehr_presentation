1. Export OMOP data to csv using the `export_omop.py` script. You need to change the `WHERE` clause in SQL queries to select PERSON_IDs.
```bash
git clone https://github.com/akwasigroch/ehr_presentation
```
3. Transform the data to MEDS format.  Don't forget to install the library with the specified version from the tag

```bash 
git clone https://github.com/akwasigroch/meds_etl.git 
cd meds_etl 
git checkout tags/0.1.4
pip install -e .
python src/meds_etl/omop.py ./source_omop/ output_meds --num_proc 4

```
3. Install the FEMR package
```bash
pip install femr
pip install torch
ml gcc/11.2.0 # package needed for xformers installation
pip install xformers
```
4. Download the model from `https://huggingface.co/StanfordShahLab/clmbr-t-base`
5. Run the `run_transformer.py` script. Specify the model path, MEDS path, and labels path. Here is an example of the labels file


| PERSON_ID | RETURNED | VISIT_END_DATETIME  |
| --------- | -------- | ------------------- |
| 0         | 0        | 2020-05-18 16:10:00 |
| 1         | 0        | 2020-12-20 16:26:00 |
| 2         | 1        | 2021-01-02 15:38:00 |
| 3         | 0        | 2021-01-18 17:18:00 |
