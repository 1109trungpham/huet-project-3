### Docs:
https://docs.google.com/document/d/1t4Liok8LiwfOVcOjujt9Ec-2bNlIxdEzXPH6_pD1Gdk/edit?tab=t.0#heading=h.2t5nigp69x75

### Tạo môi trường ảo Python
Tạo môi trường ảo:
```bash
python3 -m venv venv
```

Kích hoạt môi trường ảo (tuỳ vào hệ điều hành):
- macOS/Linux	bash/zsh
```bash
source venv/bin/activate
```

- Windows	Command Prompt:
```bash
venv\Scripts\activate.bat
```

- Windows	PowerShell:
```bash
.\\venv\\Scripts\\Activate.ps1
```

### Cài đặt các thư viện sau:
```bash
pip install spacy
python -m spacy download en_core_web_sm
pip install contractions
pip install pyarrow
pip install nltk
pip install duckdb
```

### Data Processing
Các phương pháp xử lý tối ưu:
- Multiprocessing: Giúp tận dụng toàn bộ nhân CPU cho các tác vụ tính toán nặng (như contractions.fix hay xử lý văn bản thô).
- Parquet: Lưu trữ dạng cột (columnar), giúp file nhẹ hơn 5-10 lần và tốc độ đọc/ghi nhanh hơn CSV gấp nhiều lần.
- DuckDB: Nó đọc Parquet cực nhanh (nhanh hơn Pandas rất nhiều khi file lớn).
          Nó cho phép bạn dùng SQL thuần thục để lọc dữ liệu ngay lập tức.
