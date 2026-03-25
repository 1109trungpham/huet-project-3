import pandas as pd
from pathlib import Path
import spacy
import contractions
from concurrent.futures import ProcessPoolExecutor
import numpy as np
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk


# --- CẤU HÌNH ĐƯỜNG DẪN ---
BASE_DIR = Path('/Users/trung/Desktop/huet-project-3')
DATA_PATH = BASE_DIR / 'data/TripAdvisor_Data_Cleaned_Hotel_English_Hue.json'
OUTPUT_PATH = BASE_DIR / 'data/clean_data.parquet'
NLTK_DATA_PATH = BASE_DIR / 'nltk_data'


# --- KHỞI TẠO NLTK TẠI THƯ MỤC ROOT ---
NLTK_DATA_PATH.mkdir(exist_ok=True)
nltk.data.path.append(str(NLTK_DATA_PATH))

try:
    nltk.data.find('sentiment/vader_lexicon.zip', paths=[str(NLTK_DATA_PATH)])
except LookupError:
    nltk.download('vader_lexicon', download_dir=str(NLTK_DATA_PATH))


# --- CÁC HÀM XỬ LÝ ---
def data_ingestion(path):
    if path.exists():
        return pd.read_json(path)
    raise FileNotFoundError(f"Không tìm thấy file tại: {path}")

def date_processing(df):
    print("Đang tách dữ liệu ngày, tháng ...")
    df[['day', 'month']] = df['visit_date'].str.split('-', expand=True)
    # expand=True sẽ tách kết quả thành các cột riêng biệt
    return df

# Hàm xử lý song song xử lý viết tắt
def contractions_func(text):
    return contractions.fix(str(text))

def apply_parallel(df_col, func, workers=8):
    with ProcessPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(func, df_col))

def detect_sentiment_conflicts(df):
    print("Đang kiểm tra mâu thuẫn giữa nội dung và số sao...")
    sid = SentimentIntensityAnalyzer()
    
    # Tính điểm VADER trên text CÓ viết hoa/thường để tăng độ chính xác
    df['vader_score'] = df['review_full'].apply(lambda x: sid.polarity_scores(str(x))['compound'])

    # Logic mâu thuẫn
    condition_a = (df['vader_score'] > 0.6) & (df['star'] <= 2)
    condition_b = (df['vader_score'] < -0.6) & (df['star'] >= 4)

    df['is_conflict'] = False
    df.loc[condition_a | condition_b, 'is_conflict'] = True

    conflict_count = df['is_conflict'].sum()
    print(f"Phát hiện {conflict_count} trường hợp mâu thuẫn ({conflict_count/len(df)*100:.2f}%).")
    return df

if __name__ == '__main__':
    # 1. Ingestion
    df = data_ingestion(DATA_PATH)

    # 2. Tạo review_full thô
    df['review_full'] = df['title'].fillna('') + " " + df['comment'].fillna('')

    # 3. Xử lý dữ liệu ngày tháng
    df = date_processing(df)

    # 4. Multiprocessing sửa viết tắt
    print("Đang chạy Multiprocessing cho contractions...")
    df['review_full'] = apply_parallel(df['review_full'], contractions_func, workers=8)

    # 5. Kiểm tra mâu thuẫn
    df = detect_sentiment_conflicts(df)

    # 6. Gán nhãn Sentiment ban đầu dựa trên cột `star`
    df['sentiment'] = 'neutral'
    df.loc[df['star'] >= 4, 'sentiment'] = 'positive'
    df.loc[df['star'] <= 2, 'sentiment'] = 'negative'

    # 7. SpaCy Optimized
    print("Đang chạy SpaCy nlp.pipe...")
    nlp = spacy.load("en_core_web_sm", disable=['ner', 'parser', 'tok2vec'])

    texts_to_process = [str(text).lower() for text in df['review_full']]
    docs = nlp.pipe(texts_to_process, batch_size=1000, n_process=-1)
    
    df['cleaned_comment'] = [
        " ".join([t.lemma_ for t in doc if not t.is_stop and not t.is_punct]) 
        for doc in docs
    ]

    # 8. Lưu dạng Parquet
    print(f"Đang lưu dữ liệu dạng Parquet tại: {OUTPUT_PATH}")
    df.to_parquet(OUTPUT_PATH, index=False, engine='pyarrow')
    
    print("Hoàn thành! Toàn bộ quy trình đã được tối ưu.")



# Output:
# [nltk_data] Downloading package vader_lexicon to
# [nltk_data]     /Users/trung/Desktop/huet-project-3/nltk_data...
# Đang tách dữ liệu ngày, tháng ...
# Đang chạy Multiprocessing cho contractions...
# Đang kiểm tra mâu thuẫn giữa nội dung và số sao...
# Phát hiện 646 trường hợp mâu thuẫn (1.25%).
# Đang chạy SpaCy nlp.pipe...
# Đang lưu dữ liệu dạng Parquet tại: /Users/trung/Desktop/huet-project-3/data/clean_data.parquet
# Hoàn thành! Toàn bộ quy trình đã được tối ưu.