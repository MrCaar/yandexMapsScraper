# Yandex Maps Yorum Scraper

Yandex Haritalar üzerindeki işletme sayfalarından yorumları yüksek kaliteli ve tekrarları azaltılmış şekilde çeken gelişmiş bir Playwright tabanlı scraper. Ayrıca çıkan verileri temizlemek ve tekrarlardan arındırmak için bir veri temizleyici (data_cleaner.py) içerir.

## Özellikler
- Yorumları sürekli kaydırma ve farklı yöntemlerle yükleme
- "Diğer" (Devamını gör) butonlarını agresif biçimde açma
- Yazar adı, puan, tarih, yorum metni, fotoğraf varlığı ve işletme yanıtı çıkarımı
- Gelişmiş tekrar tespiti (hash’lenmiş normalize metin) ve filtreleme
- Otomatik aralıklarla JSON + CSV autosave (data/autosave)
- Çalışma günlükleri (scraper.log, data_cleaner.log)
- Sonuçları JSON (data/raw) ve CSV (data/processed) olarak kaydetme

## Klasör Yapısı
- `src/`, `docs/`, `tests/` vb. klasörler hazırdır; aktif kodlar kök dizindeki Python dosyalarındadır.
- Veriler ve loglar:
  - `data/raw/` — Ham JSON çıktı
  - `data/processed/` — Temiz CSV çıktı
  - `data/autosave/` — Otomatik aralıklı yedeklemeler
  - `logs/` — Ek loglar için (opsiyonel)
  - `scraper.log`, `data_cleaner.log` — Çalışma günlük dosyaları

## Gereksinimler
- Python 3.9+
- Playwright (Python)
- Google Chrome/Chromium (Playwright kendi indirtebilir)

## Kurulum
Aşağıdaki adımlar Linux Bash için örneklendirilmiştir.

```bash
# (Opsiyonel) Sanal ortam
python -m venv .venv
source .venv/bin/activate

# Gerekli Python paketleri
pip install --upgrade pip
pip install playwright pandas

# Playwright tarayıcıları yükle
python -m playwright install chromium
```

Not: Şirket ağı veya kısıtlı ortamda `--with-deps` gerekebilir: `python -m playwright install --with-deps chromium`.

## Kullanım
### 1) Yorumları çekme (pagination_scraper.py)
Scraper, ilk çalıştırmada sizden URL, maksimum yorum sayısı ve görünür/görünmez tarayıcı tercihi ister.

```bash
python pagination_scraper.py
```

- URL: Yandex Haritalar işletme sayfası URL’si (örnek varsayılan sağlanır).
- Maksimum yorum sayısı: `all`/`hepsi` tümü, aksi halde sayı girin.
- Tarayıcı görünürlüğü: 1 (headless) daha hızlı, 2 (görünür) CAPTCHA çözmek için ideal.

Çıktılar:
- `data/raw/yandex_reviews_enhanced_YYYYMMDD_HHMMSS.json`
- `data/processed/yandex_reviews_enhanced_YYYYMMDD_HHMMSS.csv`
- Otomatik yedek: `data/autosave/` içine hem JSON hem CSV

Alanlar (örnek):
- `review_id`, `author_name`, `rating`, `text_original`, `date`, `has_photos`, `business_reply`

### 2) Veri temizleme (data_cleaner.py)
`data/processed` veya proje kökünde bulunan CSV/JSON dosyalarınızı seçip temizler.

```bash
python data_cleaner.py
```

Yaptıkları:
- Tekrarlanan `review_id` ve (author_name + text_original) kayıtlarını çıkarır
- Boş yazar adlarını “Anonim Kullanıcı” ile doldurur
- Boş/eksik yorum ve tarih alanlarını işaretler
- >5 yıldız gibi geçersiz puanları temizler
- Temiz sonucu yeni bir CSV’ye yazar

## CAPTCHA İpuçları
- Yandex bazen CAPTCHA gösterebilir. Headless modda tespit edilirse, araç görünür tarayıcı açıp sizin çözmenizi ister.
- CAPTCHA sayfasında çözümü yaptıktan sonra konsolda Enter’a basmanız istenebilir.
- Çok agresif veya hızlı istekler CAPTCHA riskini artırır; gerekirse görünür mod ve daha düşük hız tercih edin.

## Sık Karşılaşılan Sorunlar
- Playwright tarayıcısı eksik: `python -m playwright install chromium`
- GL/Lib eksikleri (Linux): `--with-deps` ile kurulum yapın veya sistem bağımlılıklarını yükleyin.
- Boş çıktı/az yorum: Sayfa yapısı değişmiş olabilir; tekrar deneyin veya görünür modda gözlemleyin.
- Dil/yerel farklar: Selektörler çok dilli destek içerir ancak site güncellemelerinde uyarlama gerekebilir.

## Geliştirme Notları
- Kod başlıca dosyalar:
  - `pagination_scraper.py` — ana scraper akışı
  - `data_cleaner.py` — veri temizleyici
- Loglar: `scraper.log` ve `data_cleaner.log` dosyalarından süreç ayrıntılarını inceleyin.

## GitHub’a Yükleme Önerileri
- Bir `.gitignore` ekleyin (ör. büyük veri dosyalarını, `data/` altını, `*.log`, `.venv/` gibi dizinleri hariç tutun).
- README içeriğini güncel tutun ve örnek ekran görüntüleri ekleyebilirsiniz.
- Lisans seçin (MIT, Apache-2.0 vb.).

Örnek `.gitignore` (kısa):

```
.venv/
__pycache__/
*.log
*.csv
*.json
/data/
```

## Lisans
Varsayılan bir lisans belirtilmedi. Açık kaynak paylaşacaksanız `LICENSE` dosyası eklemenizi öneririz (örn. MIT).

---
Sorularınız ve katkılar için Issues/PR’lara açıktır. İyi analizler!
