
import shutil
import torch
import os
import femr.models.tokenizer
import femr.models.transformer
import pyarrow.csv
import datasets
from datetime import datetime, timedelta
import random
import pickle
import polars as pl
import pandas as pd




# First, we compute our features

MODEL_PATH = '~/work/data/model/clmbr-t-base'
MEDS_PATH = '~/ehr_models/model/meds/data/*'
LABELS_PATH = '~/ehr_models/model/visit_labels.csv'
# DEVICE = torch.device('cuda')
DEVICE = torch.device('cpu')

if __name__ == "__main__":

    dataset = datasets.Dataset.from_parquet(MEDS_PATH)

    labels = pd.read_csv(LABELS_PATH)
    labels = pd.read_csv(LABELS_PATH)
    labels = labels[labels['PERSON_ID'].isin(dataset['patient_id'])]
    labels['VISIT_END_DATETIME'] = pd.to_datetime(labels['VISIT_END_DATETIME'])
    labels['RETURNED'] =  labels['RETURNED'].astype('bool')
    labels = labels.rename(columns={'PERSON_ID': 'patient_id', 'RETURNED': 'boolean_value', 'VISIT_END_DATETIME': 'prediction_time'})
    labels = labels.to_dict(orient='records')


    features = femr.models.transformer.compute_features(dataset,MODEL_PATH, labels, num_proc=12, tokens_per_batch=5952, device=DEVICE)

    with open('patient_embbedings.pkl', 'wb') as f:
        pickle.dump(features, f)


