# This file contains code for suporting addressing questions in the data

from . import assess

from datetime import timedelta
import pandas as pd
import numpy as np



def prepare_data(date, latitude, longitude, property_type, nodes, saler, pca):
    """Prepares input data for training by scaling it according to the training data"""
    date_rel = pd.DataFrame([[1.0 * (date - datetime(1995, 1, 1)).days]], columns=['date_of_transfer'])
    onehot_features_input = pd.DataFrame([[1.0]], columns=['property_type_' + property_type])
    # Add missing one-hot columns
    for col in onehot_features.columns:
        if col not in onehot_features_input.columns:
            onehot_features_input.insert(0, col, [0.0])

    input_osm_features = assess.get_closest_features(pd.DataFrame([[latitude, longitude]], columns=['latitude', 'longitude']), nodes)
    features = pd.concat([date_rel, pd.DataFrame([[latitude, longitude]], columns=['latitude', 'longitude']), onehot_features_input, input_osm_features])
    scaled_data = scaler.transform(features)
    scaled_features = pd.DataFrame(scaled_data, columns=features.columns)

    pca_osm = pca.transform(scaled_features[input_osm_features.columns])
    pca_features = pd.DataFrame(pc_osm, columns=[f'PC{i+1}' for i in range(len(pc_osm[0]))])

    input_X = pd.concat([scaled_features.drop(input_osm_features.columns, axis=1), pca_features], axis=1)
  
    return input_X
