#!/usr/bin/env python3
"""
Yandex Maps - Veri Temizleyici ve Duplicate Önleyici
Path: data_cleaner.py

Bu script, çekilen yorum verilerindeki sorunları temizler:
1. Tekrarlanan kayıtları çıkarır
2. Boş alanları düzeltir 
3. Veri kalitesini iyileştirir
"""

import os
import pandas as pd
import json
import re
from datetime import datetime
import logging

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_cleaner.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YandexDataCleaner:
    def __init__(self):
        self.input_file = None
        self.output_file = None
        self.df = None
        self.total_records = 0
        self.duplicate_count = 0
        self.empty_fields_count = {
            'author_name': 0,
            'text_original': 0,
            'date': 0,
            'rating': 0
        }
        
    def load_data(self, file_path):
        """CSV veya JSON dosyasından veri yükle"""
        logger.info(f"📂 Dosya yükleniyor: {file_path}")
        self.input_file = file_path
        
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.csv':
            self.df = pd.read_csv(file_path, encoding='utf-8-sig')
        elif file_ext == '.json':
            # JSON dosyasını aç
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # JSON içinden reviews listesini al
            if isinstance(data, dict) and 'reviews' in data:
                reviews = data['reviews']
            else:
                reviews = data
                
            # DataFrame'e çevir
            self.df = pd.DataFrame(reviews)
        else:
            raise ValueError(f"Desteklenmeyen dosya formatı: {file_ext}")
            
        self.total_records = len(self.df)
        logger.info(f"✅ {self.total_records} kayıt yüklendi")
        logger.info(f"📊 Sütunlar: {', '.join(self.df.columns)}")
        
        # Sütunların var olduğunu kontrol et
        required_columns = ['review_id', 'author_name', 'text_original', 'date', 'rating']
        for col in required_columns:
            if col not in self.df.columns:
                logger.warning(f"⚠️ '{col}' sütunu bulunamadı!")
                
        return self.total_records
        
    def analyze_data_quality(self):
        """Veri kalitesi analizi yap"""
        logger.info(f"🔍 Veri kalitesi analizi yapılıyor...")
        
        # Boş değerler
        for col in self.empty_fields_count.keys():
            if col in self.df.columns:
                empty_count = self.df[col].isna().sum()
                self.empty_fields_count[col] = empty_count
                empty_percent = (empty_count / self.total_records) * 100
                logger.info(f"  - {col}: {empty_count} boş değer (%{empty_percent:.1f})")
        
        # Tekrarlanan kayıtlar
        if 'review_id' in self.df.columns:
            duplicate_ids = self.df['review_id'].duplicated().sum()
            logger.info(f"  - Tekrarlanan review_id: {duplicate_ids}")
            
        # Diğer alanlar üzerinden tekrarları kontrol et
        if 'author_name' in self.df.columns and 'text_original' in self.df.columns:
            duplicate_content = self.df.duplicated(subset=['author_name', 'text_original']).sum()
            logger.info(f"  - İçerik bazlı tekrar: {duplicate_content}")
            
        # Ortalama metin uzunluğu
        if 'text_original' in self.df.columns:
            self.df['text_length'] = self.df['text_original'].fillna('').apply(len)
            avg_length = self.df['text_length'].mean()
            logger.info(f"  - Ortalama yorum uzunluğu: {avg_length:.1f} karakter")
            
        # Puanların dağılımı
        if 'rating' in self.df.columns:
            rating_counts = self.df['rating'].value_counts(dropna=False)
            logger.info(f"  - Puan dağılımı:\n{rating_counts}")
            
        return {
            'empty_fields': self.empty_fields_count,
            'duplicate_ids': duplicate_ids if 'review_id' in self.df.columns else 0,
            'duplicate_content': duplicate_content if 'author_name' in self.df.columns and 'text_original' in self.df.columns else 0,
            'avg_text_length': avg_length if 'text_original' in self.df.columns else 0
        }
    
    def clean_data(self):
        """Veriyi temizle"""
        logger.info(f"🧹 Veri temizleme işlemi başlatılıyor...")
        original_count = len(self.df)
        
        # 1. Tekrarlanan kayıtları çıkar
        if 'review_id' in self.df.columns:
            # review_id'ye göre tekrarları çıkar
            before_count = len(self.df)
            self.df = self.df.drop_duplicates(subset=['review_id'])
            removed = before_count - len(self.df)
            if removed > 0:
                logger.info(f"✓ {removed} tekrarlanan review_id çıkarıldı")
                self.duplicate_count += removed
        
        # 2. İçerik bazlı tekrarları çıkar
        if 'author_name' in self.df.columns and 'text_original' in self.df.columns:
            before_count = len(self.df)
            self.df = self.df.drop_duplicates(subset=['author_name', 'text_original'])
            removed = before_count - len(self.df)
            if removed > 0:
                logger.info(f"✓ {removed} içerik bazlı tekrar çıkarıldı")
                self.duplicate_count += removed
                
        # 3. Boş kullanıcı adlarını düzelt
        if 'author_name' in self.df.columns:
            empty_authors = self.df['author_name'].isna() | (self.df['author_name'] == '')
            self.df.loc[empty_authors, 'author_name'] = "Anonim Kullanıcı"
            logger.info(f"✓ {empty_authors.sum()} boş kullanıcı adı 'Anonim Kullanıcı' olarak değiştirildi")
            
        # 4. Boş yorumları filtrele veya işaretle
        if 'text_original' in self.df.columns:
            empty_text = self.df['text_original'].isna() | (self.df['text_original'] == '')
            # Seçenek 1: Boş yorumları çıkar
            # self.df = self.df[~empty_text]
            # logger.info(f"✓ {empty_text.sum()} boş yorumlu kayıt çıkarıldı")
            
            # Seçenek 2: Boş yorumları işaretle
            self.df.loc[empty_text, 'text_original'] = "[Boş yorum]"
            logger.info(f"✓ {empty_text.sum()} boş yorum işaretlendi")
        
        # 5. Metin içeriğini temizle
        if 'text_original' in self.df.columns:
            # Fazla boşlukları temizle
            self.df['text_original'] = self.df['text_original'].fillna('').apply(
                lambda x: re.sub(r'\s+', ' ', x).strip()
            )
            
            # Yaygın olarak bulunan gereksiz metinleri temizle
            common_texts = ["Abone ol", "Subscribe", "Varsayılan", "Default", "Deneyimini paylaş"]
            for text in common_texts:
                self.df['text_original'] = self.df['text_original'].str.replace(text, '', regex=False)
                
            logger.info(f"✓ Yorum metinleri temizlendi, gereksiz içerikler çıkarıldı")
        
        # 6. Puanları normalleştir
        if 'rating' in self.df.columns:
            # Mantıksız değerleri düzelt (örn. 66.0 gibi)
            invalid_ratings = self.df['rating'] > 5
            if invalid_ratings.any():
                logger.info(f"⚠️ {invalid_ratings.sum()} geçersiz puan değeri tespit edildi")
                
                # 5 üzeri puanları normalize et - örneğin 66.0 -> None
                self.df.loc[invalid_ratings, 'rating'] = None
                logger.info(f"✓ Geçersiz puanlar temizlendi")
        
        # 7. Tarih formatını normalize et
        if 'date' in self.df.columns:
            # Tarih formatını düzelt veya eksik tarihleri işaretle
            empty_dates = self.df['date'].isna() | (self.df['date'] == '')
            self.df.loc[empty_dates, 'date'] = 'Tarih belirtilmemiş'
            logger.info(f"✓ {empty_dates.sum()} boş tarih işaretlendi")
            
        logger.info(f"🎉 Veri temizleme tamamlandı! İlk: {original_count}, Son: {len(self.df)}, Fark: {original_count-len(self.df)}")
        
        return len(self.df)
    
    def export_clean_data(self, output_path=None):
        """Temizlenmiş veriyi dışa aktar"""
        if output_path:
            self.output_file = output_path
        else:
            # Varsayılan isim oluştur
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_base = os.path.splitext(os.path.basename(self.input_file))[0]
            self.output_file = f"{file_base}_clean_{timestamp}.csv"
            
        logger.info(f"💾 Temizlenmiş veri kaydediliyor: {self.output_file}")
        self.df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        
        # Özet bilgi yazdır
        print("\n" + "=" * 40)
        print(f"✅ Veri temizleme tamamlandı!")
        print(f"📊 İlk kayıt sayısı: {self.total_records}")
        print(f"🔍 Çıkarılan tekrar kayıt: {self.duplicate_count}")
        print(f"📋 Kalan kayıt sayısı: {len(self.df)}")
        print(f"💾 Temiz veri kaydedildi: {self.output_file}")
        print("=" * 40)
        
        return self.output_file

def main():
    print("\n🧹 Yandex Maps Veri Temizleyici")
    print("=" * 40)
    
    # Dosya seç
    print("\n📁 Lütfen temizlenecek veri dosyasını seçin (CSV veya JSON):")
    
    # Mevcut dizindeki CSV ve JSON dosyalarını listele
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, "data", "processed")
    
    if not os.path.exists(data_dir):
        data_dir = current_dir
    
    files = [f for f in os.listdir(data_dir) if f.endswith(('.csv', '.json'))]
    
    if not files:
        print("❌ Hiç CSV veya JSON dosyası bulunamadı!")
        return
    
    # Dosyaları numaralandırarak göster
    for i, file in enumerate(files, 1):
        print(f"  [{i}] {file}")
    
    # Dosya seçimi
    while True:
        try:
            choice = input("\nSeçiminiz (numara): ").strip()
            file_index = int(choice) - 1
            
            if 0 <= file_index < len(files):
                selected_file = os.path.join(data_dir, files[file_index])
                break
            else:
                print("⚠️ Lütfen listeden geçerli bir numara seçin!")
        except ValueError:
            print("⚠️ Lütfen bir sayı girin!")
    
    # Veri temizleyiciyi başlat
    cleaner = YandexDataCleaner()
    
    try:
        # Veriyi yükle
        cleaner.load_data(selected_file)
        
        # Veri kalitesi analizi yap
        cleaner.analyze_data_quality()
        
        # Veriyi temizle
        cleaner.clean_data()
        
        # Temizlenmiş veriyi dışa aktar
        cleaner.export_clean_data()
        
    except Exception as e:
        logger.error(f"💥 Hata oluştu: {e}")
        print(f"\n❌ Bir hata oluştu: {e}")

if __name__ == "__main__":
    main()