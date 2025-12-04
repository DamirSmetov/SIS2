import pandas as pd
import os
import re
import uuid
from deep_translator import GoogleTranslator
import logging

def slugify(s: str) -> str:
    """Convert product name into clean slug."""
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s or "unknown"

def make_product_id(model_name: str) -> str:
    base = slugify(model_name)
    short_uuid = uuid.uuid4().hex[:8]
    return f"{base}_{short_uuid}"

def translate_text(text: str, target_language: str = "en") -> str:
    try:
        return GoogleTranslator(source='auto', target=target_language).translate(text)
    except Exception as e:
        print(f"Translation error for '{text}': {e}")
        return text


def clean_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    df = raw_data.copy()

    # Removing duplicates
    df = df.drop_duplicates()

    # Handling missing values
    text_cols = ['name', 'memory', 'year', 'color']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()

    df['price'] = df['price'].fillna('0').astype(str).str.strip()

    # Normalizing text data
    for col in text_cols:
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True)

    # Converting types
    # price to int
    df['price'] = df['price'].str.replace(r'[^\d]', '', regex=True)
    df['price'] = df['price'].replace('', '0').astype(int)

    # year to int
    df['year'] = df['year'].str.extract(r'(\d{4})')[0]
    df['year'] = df['year'].fillna('0').astype(int)

    # memory to int(GB)
    df['memory_gb'] = df['memory'].str.extract(r'(\d+)')[0].fillna(0).astype(int)

    # translating 'color' to English
    df['color'] = df['color'].apply(lambda x: translate_text(x, target_language='en') if x else x)
    
    # Renaming columns
    df = df.rename(columns={
        'name': 'model_name',
        'memory': 'memory_str',
        'year': 'release_year',
        'color': 'color_en',
    })
     # creating product_id
    df['product_id'] = df['model_name'].apply(make_product_id)
    cols = ['product_id'] + [c for c in df.columns if c != 'product_id']
    df = df[cols]

    return df


def main():
    input_path = "/workspaces/SIS2/data/raw_data.parquet"
    output_path = "/workspaces/SIS2/data/cleaned_data.parquet"

    if not os.path.exists(input_path):
        print(f"Input file {input_path} does not exist.")
        return

    df_raw = pd.read_parquet(input_path, engine="pyarrow")
    print(f"Loaded {len(df_raw)} raw items from {input_path}")

    df_cleaned = clean_data(df_raw)
    df_cleaned.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"Saved cleaned data with {len(df_cleaned)} items to {output_path}")
    logging.info(f"Data cleaning completed, {len(df_cleaned)} items saved.")


if __name__ == "__main__":
    main()
