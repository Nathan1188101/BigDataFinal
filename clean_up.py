import numpy as np
import pandas as pd

import os
#take in the data and clean it up from all the data we do not need to load it quicker later.
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Books_rating.csv'))

df = pd.read_csv(file_path)

columns_to_drop = ['price', 'review/text', 'review/summary', 'review/time', 'review/helpfullness']
df_final = df.drop(columns=columns_to_drop, errors='ignore')

output_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Books_rating_cleaned.csv'))
df_final.to_csv(output_file_path, index=False)


file_path2 = os.path.abspath(os.path.join(os.path.dirname(__file__), 'books_data.csv'))

df2 = pd.read_csv(file_path2)
columns_to_drop2 = ['description','image','previewLink','publishedDate','infoLink']
df_final2 = df2.drop(columns=columns_to_drop2, errors='ignore')

output_file_path2 = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Books_data_cleaned.csv'))
df_final2.to_csv(output_file_path2, index=False)