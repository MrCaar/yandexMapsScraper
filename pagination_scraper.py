#!/usr/bin/env python3
"""
Yandex Maps Geliştirilmiş Yorum Scraper
Path: enhanced_yandex_scraper.py

Veri kalitesi iyileştirilmiş, tekrarlar azaltılmış ve daha fazla veri elementi çıkaran scraper
"""

import asyncio
import json
import os
import re
import hashlib
from playwright.async_api import async_playwright, TimeoutError
from datetime import datetime
import pandas as pd
import logging
import time

# Klasörleri oluştur
os.makedirs('data/raw', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)
os.makedirs('data/autosave', exist_ok=True)  # Otomatik kayıtlar için yeni klasör

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YandexMapsScraper:
    def __init__(self):
        import collections
        self.base_url = "https://yandex.com.tr/maps"
        self.session_cookies = None
        self.page = None
        self.browser = None
        self.total_reviews = 0
        # Sadece son 30 yorumu kontrol etmek için collections.deque kullan
        self.recent_review_ids = collections.deque(maxlen=30)
        self.recent_content_hashes = collections.deque(maxlen=30)
        self.duplicate_count = 0  # Tekrar sayısını izlet()
        # Gelişmiş tekrar kontrolü için tüm yorumlar boyunca hash seti
        self.global_content_hashes = set()
        # Otomatik kaydetme için değişkenler
        self.auto_save_interval = 50  # Her 50 yorumda bir otomatik kaydetme yapılacak
        self.last_auto_save_count = 0
        self.business_id = None
        self.business_name = None
            
    async def start_browser(self):
        """Browser'ı başlat ve session kur"""
        self.playwright = await async_playwright().start()
        
        # Browser'ı yükle (headless=False olursa görünür olur)
        self.browser = await self.playwright.chromium.launch(
            headless=True,  # Performans için headless modda çalıştır
            args=[
                '--no-sandbox', 
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu'
            ]
        )
        
        context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        
        self.page = await context.new_page()
        
    async def navigate_to_place(self, business_url):
        """Yandex Maps'teki işletme sayfasına git"""
        logger.info(f"🌐 İşletme sayfasına yönlendiriliyor: {business_url}")
        
        # Ana sayfaya git
        await self.page.goto(business_url)
        await self.page.wait_for_load_state('networkidle')
        
        # CAPTCHA kontrolü
        if await self.check_and_handle_captcha():
            logger.info("✅ CAPTCHA işlemi tamamlandı, devam ediliyor...")
            
        # Yorumlar sekmesine geç
        if not await self.navigate_to_reviews_tab():
            logger.error("❌ Yorumlar sekmesine geçilemedi!")
            return None, None
            
        # İşletme bilgilerini topla
        business_name = await self.get_business_name()
        logger.info(f"🏢 İşletme Adı: {business_name}")
        
        # URL'den business ID çıkar
        business_id = self.extract_business_id(business_url)
        logger.info(f"🆔 Business ID: {business_id}")
        
        # Toplam yorum sayısını al
        self.total_reviews = await self.get_total_review_count()
        logger.info(f"📊 Toplam yorum sayısı: {self.total_reviews}")
        
        return business_id, business_name
        
    async def check_and_handle_captcha(self):
        """CAPTCHA sayfasını kontrol et ve yönet"""
        current_url = self.page.url
        page_title = await self.page.title()
        
        is_captcha = ('showcaptcha' in current_url or 
                      'captcha' in current_url.lower() or 
                      'robot' in page_title.lower() or
                      'Are you not a robot' in page_title)
        
        if is_captcha:
            logger.warning("⚠️ CAPTCHA tespit edildi! Lütfen tarayıcıda CAPTCHA'yı çözün.")
            logger.warning("⚠️ CAPTCHA çözüldükten sonra entere basın...")
            
            # headless=True ise CAPTCHA için tarayıcıyı görünür yap
            if self.browser:
                await self.browser.close()
                
            logger.info("🔄 CAPTCHA çözümü için görünür tarayıcı açılıyor...")
            self.browser = await self.playwright.chromium.launch(headless=False)
            context = await self.browser.new_context()
            self.page = await context.new_page()
            
            # CAPTCHA sayfasına git
            await self.page.goto(current_url)
            
            input("CAPTCHA çözüldüğünde Enter tuşuna basın...")
            
            # Sayfanın yüklenmesini bekle
            await self.page.wait_for_load_state('networkidle')
            return True
            
        return False
    
    def extract_business_id(self, url):
        """URL'den business ID'yi çıkarır"""
        match = re.search(r'/org/[^/]+/(\d+)', url)
        if match:
            return match.group(1)
        return None
    
    async def get_business_name(self):
        """Sayfa başlığından işletme adını al"""
        try:
            title = await self.page.title()
            # "İşletme Adı — Yandex Haritalar" formatından işletme adını çıkar
            if " — " in title:
                return title.split(" — ")[0]
            elif "robot" in title.lower() or "captcha" in title.lower():
                return "CAPTCHA Sayfası"
            return title
        except:
            return "Bilinmeyen İşletme"
    
    async def get_total_review_count(self):
        """Toplam yorum sayısını sayfadan çıkar"""
        try:
            # Yorum sayısını içeren metni bul
            review_count_text = await self.page.evaluate("""
                () => {
                    {
                    // 0. H2 özel başlık
                    const h2Review = document.querySelector('h2.card-section-header__title._wide');
                    if (h2Review) {
                        const text = h2Review.textContent || '';
                        const match = text.match(/(\d[\d\s,.]*\d+)/);
                        if (match) return match[0];
                    }                                     
                    
                    // 1. Yorumlar sekmesi üzerindeki sayı
                    const reviewTab = document.querySelector('[data-tab-name="reviews"], [role="tab"]:has-text("Yorumlar"), [role="tab"]:has-text("Reviews"), [role="tab"]:has-text("Отзывы")');
                    if (reviewTab) {
                        const text = reviewTab.textContent || '';
                        const match = text.match(/(\\d[\\d\\s,.]*\\d+)/);
                        if (match) return match[0];
                    }
                    
                    // 2. Yorumlar başlığındaki sayı
                    const headers = document.querySelectorAll('h1, h2, h3, .header, .title');
                    for (const header of headers) {
                        const text = header.textContent || '';
                        const match = text.match(/(\\d[\\d\\s,.]*\\d+)\\s*(yorum|отзыв|reviews)/i);
                        if (match) return match[1];
                    }
                    
                    // 3. Sayfa üzerinde herhangi bir yerdeki yorum sayısı
                    const elements = document.querySelectorAll('*');
                    for (const elem of elements) {
                        const text = elem.textContent || '';
                        const match = text.match(/(\\d[\\d\\s,.]*\\d+)\\s*(yorum|отзыв|reviews)/i);
                        if (match) return match[1];
                    }
                    
                    return null;
                }
            """)
            
            if review_count_text:
                # Sayıyı temizle (boşluklar, noktalama işaretleri vs.)
                count_str = re.sub(r'\D', '', review_count_text)
                if count_str.isdigit():
                    return int(count_str)
            
            logger.warning("⚠️ Toplam yorum sayısı belirlenemedi, varsayılan değer kullanılacak")
            return 1000  # Varsayılan bir değer
            
        except Exception as e:
            logger.error(f"❌ Yorum sayısı alınırken hata: {e}")
            return 1000  # Hata durumunda varsayılan bir değer
    
    async def navigate_to_reviews_tab(self):
        """Yorumlar sekmesine git"""
        logger.info("🔍 Yorumlar sekmesine geçiliyor...")
        
        try:
            # 1. Yorumlar sekmesini ara
            tabs = await self.page.query_selector_all('[role="tab"], [data-tab-name], li[class*="tab"]')
            
            for tab in tabs:
                text = await tab.text_content()
                if re.search(r'(yorumlar|reviews|отзывы|comments)', text, re.IGNORECASE):
                    logger.info(f"✅ Yorumlar sekmesi bulundu: '{text}'")
                    await tab.click()
                    await asyncio.sleep(2)
                    return True
            
            # 2. XPath ile ara
            review_tab_xpath = "//a[contains(text(), 'Yorumlar')] | //a[contains(text(), 'Reviews')] | //a[contains(text(), 'Отзывы')] | //div[contains(text(), 'Yorumlar')] | //div[contains(text(), 'Reviews')] | //div[contains(text(), 'Отзывы')]"
            review_tab = await self.page.query_selector(review_tab_xpath)
            
            if review_tab:
                logger.info("✅ Yorumlar sekmesi bulundu (XPath ile)")
                await review_tab.click()
                await asyncio.sleep(2)
                return True
                
            # 3. URL'de reviews kelimesi varsa zaten doğru sekmedeyiz
            current_url = self.page.url
            if 'reviews' in current_url or 'yorumlar' in current_url:
                logger.info("✅ Zaten yorumlar sekmesindeyiz")
                return True
            
            # 4. Kullanıcıdan manuel geçiş iste
            logger.warning("⚠️ Yorumlar sekmesi otomatik olarak bulunamadı.")
            logger.warning("ℹ️ Lütfen tarayıcıda 'Yorumlar' sekmesine manuel olarak tıklayın")
            input("Yorumlar sekmesine geçtikten sonra Enter tuşuna basın...")
            await asyncio.sleep(2)
            return True
            
        except Exception as e:
            logger.error(f"❌ Yorumlar sekmesine geçerken hata: {e}")
            return False
    
    async def expand_review_texts(self):
        """Yorumlardaki 'Diğer' butonlarına tıklayarak uzun yorumları tamamen genişletir (tüm butonlar bitene kadar)."""
        try:
            more_button_selectors = [
                "span.business-review-view__expand",
                "span.spoiler-view__button",
                "[aria-label='Diğer']",
                "[aria-label='More']",
                "[aria-label='Ещё']",
                "button:has-text('Diğer')",
                "button:has-text('More')",
                "button:has-text('Daha fazla')",
                "[role='button']:has-text('Diğer')",
                "[role='button']:has-text('More')"
            ]
            total_expanded = 0
            max_loops = 10  # Sonsuz döngüye girmemek için limit
            for _ in range(max_loops):
                expanded_this_round = 0
                for selector in more_button_selectors:
                    more_buttons = await self.page.query_selector_all(selector)
                    for button in more_buttons:
                        try:
                            is_visible = await button.is_visible()
                            if is_visible:
                                await button.click()
                                expanded_this_round += 1
                                await asyncio.sleep(0.2)
                        except Exception:
                            continue
                total_expanded += expanded_this_round
                if expanded_this_round == 0:
                    break  # Artık açılacak buton kalmadı
                await asyncio.sleep(0.3)
            if total_expanded > 0:
                logger.info(f"✅ {total_expanded} adet 'Diğer' butonu tıklandı, tüm uzun yorumlar genişletildi")
            return total_expanded
        except Exception as e:
            logger.error(f"❌ Yorum genişletme işlemi sırasında hata: {e}")
            return 0

    async def auto_save_reviews(self, all_reviews):
        """Belirli aralıklarla review'ları otomatik kaydet"""
        if len(all_reviews) == 0 or not self.business_id:
            return
            
        if len(all_reviews) - self.last_auto_save_count >= self.auto_save_interval:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Otomatik kaydedilen verileri hazırla
                data = {
                    'business_id': self.business_id,
                    'business_name': self.business_name,
                    'reviews': all_reviews,
                    'total_review_count': self.total_reviews,
                    'scraped_review_count': len(all_reviews),
                    'scrape_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'auto_save': True
                }
                
                # JSON dosyası olarak kaydet
                autosave_filename = f"data/autosave/yandex_reviews_{self.business_id}_autosave_{timestamp}.json"
                with open(autosave_filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                # CSV dosyası olarak kaydet
                df = pd.DataFrame(all_reviews)
                csv_filename = f"data/autosave/yandex_reviews_{self.business_id}_autosave_{timestamp}.csv"
                df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                
                logger.info(f"💾 Otomatik kayıt: {len(all_reviews)} yorum kaydedildi (her {self.auto_save_interval} yorumda bir)")
                
                # Son kayıt sayısını güncelle
                self.last_auto_save_count = len(all_reviews)
                
            except Exception as e:
                logger.error(f"❌ Otomatik kayıt sırasında hata: {e}")

    async def scrape_reviews_with_continuous_scroll(self, max_reviews=None):
        """Sürekli kaydırma ile yorumları çek (daha fazla scroll ve daha agresif 'Diğer' açma ile)."""
        if max_reviews is None:
            max_reviews = self.total_reviews

        logger.info(f"🔍 Yorumlar çekiliyor (hedef: {max_reviews})...")

        all_reviews = []
        last_height = 0
        no_new_content_count = 0

        best_selector = await self.find_best_review_selector()
        if not best_selector:
            logger.error("❌ Hiçbir yorum elementi bulunamadı!")
            return []

        logger.info(f"✅ En uygun selektör: {best_selector}")

        page_size_estimate = 15
        scroll_count = max(5, min(max_reviews // page_size_estimate, 20))  # Daha fazla scroll
        max_attempts = min(max_reviews // 5, 300)  # Daha fazla deneme

        attempts = 0
        while len(all_reviews) < max_reviews and attempts < max_attempts:
            # Scroll öncesi ve sonrası agresif şekilde tüm 'Diğer' butonlarını aç
            await self.expand_review_texts()

            for _ in range(scroll_count):
                await self.try_multiple_scroll_methods()
                await asyncio.sleep(1.5)
                await self.expand_review_texts()

            # Scroll sonrası tekrar tüm 'Diğer' butonlarını aç
            await self.expand_review_texts()

            current_height = await self.page.evaluate('document.body.scrollHeight')
            elements = await self.page.query_selector_all(best_selector)
            current_element_count = len(elements)

            logger.info(f"📜 Şu ana kadar bulunan yorum sayısı: {current_element_count}")

            start_index = len(all_reviews)
            if start_index < current_element_count:
                logger.info(f"✨ {current_element_count - start_index} yeni yorum bulundu")
                for i in range(start_index, current_element_count):
                    try:
                        review_data = await self.extract_review_data(elements[i])
                        if review_data and self.is_valid_review(review_data):
                            if not self.is_duplicate_review(review_data):
                                all_reviews.append(review_data)
                                if len(all_reviews) % 25 == 0:
                                    logger.info(f"✅ {len(all_reviews)} yorum işlendi")
                                await self.auto_save_reviews(all_reviews)
                    except Exception as e:
                        logger.error(f"❌ Yorum çıkarma hatası: {e}")
                no_new_content_count = 0
            else:
                no_new_content_count += 1
                logger.info(f"⚠️ Yeni yorum yüklenmedi. Deneme: {no_new_content_count}/3")

            if no_new_content_count >= 3 or (current_height == last_height and no_new_content_count >= 2):
                logger.info("🔄 Alternatif kaydırma yöntemleri deneniyor...")
                await self.try_alternative_loading_methods()
                await asyncio.sleep(3)
                await self.expand_review_texts()
                new_elements = await self.page.query_selector_all(best_selector)
                if len(new_elements) <= current_element_count:
                    logger.info("⚠️ Daha fazla yorum yüklenemedi, mevcut yorumlarla devam ediliyor")
                    break

            last_height = current_height

            if len(all_reviews) >= max_reviews:
                logger.info(f"🎯 Hedef yorum sayısına ulaşıldı: {max_reviews}")
                break

            attempts += 1

        logger.info(f"🎉 Toplam {len(all_reviews)} yorum başarıyla çekildi!")
        return all_reviews
    
    async def find_best_review_selector(self):
        """Sayfadaki en iyi yorum selektörünü bul"""
        review_selectors = [
            "div.spoiler-view__text span.spoiler-view__text-container",
            "div[class*='business-reviews-card']",
            "div[class*='review']",
            "[class*='review-item']",
            "li[class*='card']",
            "[class*='comment']",
            "div[class*='feed-item']",
            "div[class*='_card_']",
            "div[class*='rating-']"
        ]
        
        best_selector = None
        max_count = 0
        
        for selector in review_selectors:
            try:
                elements = await self.page.query_selector_all(selector)
                if len(elements) > max_count:
                    # İlk elementi kontrol et - gerçekten yorum mu?
                    if len(elements) > 0:
                        element_text = await elements[0].text_content()
                        # Yorum benzeri metin içeriyor mu?
                        if re.search(r'(star|puan|rating|yorum|review|отзыв)', element_text, re.IGNORECASE):
                            max_count = len(elements)
                            best_selector = selector
                            logger.info(f"Potansiyel selektör: '{selector}' ile {len(elements)} element bulundu")
            except Exception as e:
                logger.debug(f"Selektör '{selector}' hatası: {e}")
        
        # Eğer hiçbir selektör bulunamadıysa, sayfa HTML'ini analiz et
        if not best_selector:
            logger.info("🔍 Sayfa yapısı analiz ediliyor...")
            
            # Sayfa HTML'inden class isimlerini çıkart
            page_html = await self.page.content()
            
            # Yorum ile ilgili class isimlerini ara
            review_classes = []
            for keyword in ['review', 'comment', 'feed', 'card', 'rating', 'отзыв', 'yorum']:
                pattern = f'class="([^"]*{keyword}[^"]*)"'
                matches = re.findall(pattern, page_html, re.IGNORECASE)
                review_classes.extend(matches[:5])  # Her keyword için en fazla 5 match
            
            # En fazla 10 sınıf dene
            for class_name in review_classes[:10]:
                selector = f'[class*="{class_name}"]'
                elements = await self.page.query_selector_all(selector)
                if len(elements) > max_count:
                    max_count = len(elements)
                    best_selector = selector
                    logger.info(f"Bulunan selektör: '{selector}' ile {len(elements)} element bulundu")
        
        return best_selector
    
    async def try_multiple_scroll_methods(self):
        """Farklı kaydırma yöntemlerini dene"""
        try:
            # 1. JavaScript ile sayfayı kaydır
            await self.page.evaluate("window.scrollBy(0, 500)")
            await asyncio.sleep(0.5)
            
            # 2. End tuşu ile sayfa sonuna git
            await self.page.keyboard.press("End")
            await asyncio.sleep(0.5)
            
            # 3. PageDown tuşu ile aşağı in
            await self.page.keyboard.press("PageDown")
            
        except Exception as e:
            logger.debug(f"Kaydırma hatası: {e}")
    
    async def try_alternative_loading_methods(self):
        """Alternatif içerik yükleme yöntemlerini dene"""
        try:
            # 1. "Daha fazla göster" butonlarını ara
            load_more_selectors = [
                "button:has-text('Daha fazla')",
                "button:has-text('Load more')",
                "button:has-text('Show more')",
                "button:has-text('Еще')",
                "button:has-text('Показать еще')",
                "[class*='show-more']",
                "[class*='load-more']"
            ]
            
            for selector in load_more_selectors:
                try:
                    load_more = await self.page.query_selector(selector)
                    if load_more:
                        logger.info(f"✅ 'Daha fazla' butonu bulundu: {selector}")
                        await load_more.click()
                        await asyncio.sleep(2)
                        return True
                except:
                    continue
            
            # 2. JavaScript ile sayfayı yenile ve scroll event'i tetikle
            await self.page.evaluate("""
                () => {
                    window.dispatchEvent(new Event('scroll'));
                    window.scrollTo(0, document.body.scrollHeight);
                }
            """)
            await asyncio.sleep(2)
            
            # 3. Farklı kaydırma teknikleri
            for scroll_pos in [1000, 2000, 3000, 5000]:
                await self.page.evaluate(f"window.scrollTo(0, {scroll_pos})")
                await asyncio.sleep(0.5)
            
            # 4. Space tuşu ile kaydır
            for _ in range(5):
                await self.page.keyboard.press("Space")
                await asyncio.sleep(0.5)
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Alternatif yükleme yöntemleri hatası: {e}")
            return False
    
    def is_valid_review(self, review_data):
        """Yorumun geçerli olup olmadığını kontrol et"""
        # Minimum metin uzunluğu kontrolü
        if 'text_original' not in review_data or not review_data['text_original']:
            return False
            
        # Çok kısa yorumları filtrele 
        if len(review_data['text_original']) <= 1:
            return False
            
        # Yazar adı kontrolü
        if 'author_name' not in review_data or not review_data['author_name']:
            return False
            
        return True
    
    def is_duplicate_review(self, review_data):
        """Bir yorumu hem son 30 yorumda hem de tüm veri boyunca normalize edilmiş metin hash'iyle tekrar kontrol eder"""
        # Review ID'yi kontrol et (kısa vadeli tekrarlar için)
        if 'review_id' in review_data and review_data['review_id'] in self.recent_review_ids:
            self.duplicate_count += 1
            return True

        # Gelişmiş tekrar kontrolü: normalize edilmiş metin hash'i
        text = review_data.get('text_original', '')
        norm_text = self.normalize_review_text(text)
        content_hash = hashlib.md5(norm_text.encode()).hexdigest()

        # Tüm veri boyunca tekrar kontrolü
        if content_hash in self.global_content_hashes:
            self.duplicate_count += 1
            return True

        # Son 30 için de tutmaya devam et
        if 'review_id' in review_data:
            self.recent_review_ids.append(review_data['review_id'])
        self.recent_content_hashes.append(content_hash)
        self.global_content_hashes.add(content_hash)

        return False

    def normalize_review_text(self, text):
        """Yorum metnini normalize ederek tekrar kontrolünü iyileştirir (küçük harf, noktalama, gereksiz boşluk, baştaki/sondaki tarih/seviye/isim temizliği)."""
        import re
        if not text:
            return ''
        # Küçük harfe çevir
        text = text.lower()
        # Başta/sonda tarih, seviye, isim gibi tekrar eden yapıları temizle
        text = re.sub(r'^[^a-zA-Z0-9а-яА-Яçğıöşü]+', '', text)
        text = re.sub(r'\d+\.\s*şehir uzmanı', '', text)
        text = re.sub(r'\d+\.\s*level local guide', '', text)
        text = re.sub(r'\d+\s*(temmuz|nisan|mart|ocak|şubat|mayıs|haziran|temmuz|ağustos|eylül|ekim|kasım|aralık|january|february|march|april|may|june|july|august|september|october|november|december)\s*', '', text)
        text = re.sub(r'\d{1,2}\.\d{1,2}\.\d{4}', '', text)
        text = re.sub(r'\d{1,2}/\d{1,2}/\d{4}', '', text)
        # Noktalama ve fazla boşlukları kaldır
        text = re.sub(r'[\W_]+', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    async def extract_review_data(self, review_element):
        """Bir yorum elementinden veri çıkar - GELİŞTİRİLMİŞ VERSİYON"""
        try:
            # Element HTML ve metnini al (debug için)
            element_html = await self.page.evaluate('(element) => element.outerHTML', review_element)
            element_text = await review_element.text_content()
            
            # ---- YAZAR ADI ----
            author_name = await self.extract_author_name(review_element, element_text)
            
            # ---- PUAN ----
            rating = await self.extract_rating(review_element, element_html)
            
            # ---- YORUM METNİ ----
            text = await self.extract_text_content(review_element, element_text, author_name)
            
            # ---- TARİH ----
            date = await self.extract_date(review_element)
            
            # ---- FOTOĞRAFLAR ----
            has_photos = await self.has_photos(review_element)
            
            # ---- İŞLETME YANITI ----
            business_reply = await self.extract_business_reply(review_element)
            
            # ID oluştur
            review_id = hashlib.md5(f"{author_name}_{text}_{date}".encode()).hexdigest()
            
            return {
                'review_id': review_id,
                'author_name': author_name,
                'rating': rating,
                'text_original': text,
                'date': date,
                'has_photos': has_photos,
                'business_reply': business_reply
            }
            
        except Exception as e:
            logger.error(f"❌ Yorum veri çıkarma hatası: {e}")
            return None
    
    async def extract_author_name(self, review_element, element_text=None):
        """Yorum elementinden yazar adını çıkar"""
        author_name = "Anonim"
        
        # Adım 1: CSS seçicilerle ara
        author_selectors = [
            "[class*='user']", 
            "[class*='author']",
            "[class*='name']",
            "span[itemprop='name']",
            "a[href*='profile']",
            "[class*='profile']"
        ]
        
        for selector in author_selectors:
            try:
                author_element = await review_element.query_selector(selector)
                if author_element:
                    author_text = await author_element.text_content()
                    author_text = author_text.strip()
                    if author_text and len(author_text) > 0 and len(author_text) < 100:
                        # Yaygın gereksiz metinleri temizle
                        author_text = re.sub(r'(Abone ol|Subscribe|Follow|seviye|level|узнать|эксперт)', '', author_text, flags=re.IGNORECASE)
                        author_text = re.sub(r'\s+', ' ', author_text).strip()  # Fazla boşlukları temizle
                        
                        if author_text:  # Temizlemeden sonra hala metin varsa
                            author_name = author_text
                            break
            except:
                pass
        
        # Adım 2: Tipik yazar adı desenlerini regex ile ara
        if author_name == "Anonim" and element_text:
            author_patterns = [
                r'([A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü]{2,}\s+[A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü]{2,})\s+(\d+\.|\d+\s+seviye|level)',  # "John Smith 5. seviye"
                r'([A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü]{2,}\s+[A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü\.]{1,})\s+',  # "John Smith "
                r'^([A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü]{2,}\s+[A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü\.]{1,})'  # "John Smith" (başlangıçta)
            ]
            
            for pattern in author_patterns:
                match = re.search(pattern, element_text)
                if match:
                    author_name = match.group(1).strip()
                    break
        
        return author_name
    
    async def extract_rating(self, review_element, element_html=None):
        """Yorum elementinden puanı çıkar - güncel ve çok dilli"""
        rating = None

        # --- Adım 1: CSS seçicilerle ara ---
        rating_selectors = [
            "[class*='rating']",
            "[class*='score']",
            "[class*='stars']",
            "[class*='star']",
            "meta[itemprop='ratingValue']",
            # Senin verdiğin özel örnek class
            "div.business-rating-badge-view__stars"
        ]

        for selector in rating_selectors:
            try:
                rating_element = await review_element.query_selector(selector)
                if rating_element:
                    rating_text = await rating_element.get_attribute("aria-label") or await rating_element.text_content()
                    rating_match = re.search(r'(\d+(\.\d+)?)', rating_text)
                    if rating_match:
                        rating_value = float(rating_match.group(1))
                        if 0 <= rating_value <= 5:
                            rating = rating_value
                            break
            except:
                continue

        # --- Adım 2: HTML'de yıldız sayısını kontrol et ---
        if rating is None and element_html:
            star_count = element_html.count('★')
            if 0 < star_count <= 5:
                rating = float(star_count)
            else:
                # SVG veya aria-label üzerinden de dene
                aria_match = re.search(r'(Değerlendirme|Rating|Оценка)\s*(\d+)\s*/\s*5', element_html, re.IGNORECASE)
                if aria_match:
                    rating = float(aria_match.group(2))

        # --- Adım 3: Element metninde puan desenlerini ara ---
        if rating is None:
            element_text = await review_element.text_content()
            rating_patterns = [
                r'(\d+(\.\d+)?)\s*(?:out of|\/)\s*5',  # "4.5 out of 5" veya "4.5/5"
                r'rating\s*:?\s*(\d+(\.\d+)?)',       # "rating: 4.5" veya "Rating 4.5"
                r'(\d+(\.\d+)?)\s*stars?'             # "4.5 stars" veya "4.5 star"
            ]
            for pattern in rating_patterns:
                match = re.search(pattern, element_text, re.IGNORECASE)
                if match:
                    try:
                        rating_value = float(match.group(1))
                        if 0 <= rating_value <= 5:
                            rating = rating_value
                            break
                    except:
                        continue

        return rating

    
    async def extract_text_content(self, review_element, element_text=None, author_name=None):
        """Yorum elementinden metin içeriğini çıkar"""
        text = ""
        
        # Adım 1: CSS seçicilerle ara
        text_selectors = [
            "[class*='text']", 
            "[class*='content']", 
            "[class*='body']",
            "p", 
            "[class*='comment']",
            "[class*='message']",
            "span.spoiler-view__text-container",        # spoiler metinleri
            "[class*='description']",
            "div.spoiler-view__text._collapsed",       # collapse edilmiş div
            "div.business-review-view__text",          # yorum div'inin alternatif sınıfı
            "span.business-review-view__expand"        # genişletme butonlarıyla birlikte içerik
        ]
        
        for selector in text_selectors:
            try:
                text_element = await review_element.query_selector(selector)
                if text_element:
                    text_content = await text_element.text_content()
                    text_content = text_content.strip()
                    # En az 5 karakter ve yazar adından farklı olmalı
                    if len(text_content) > 5 and (not author_name or text_content != author_name):
                        text = text_content
                        break
            except:
                pass
        
        # Adım 2: Metin bulunamadıysa, tüm içeriği al ve temizle
        if not text or text == "Varsayılan" or len(text) < 10:
            if element_text:
                # Yazar adını ve yaygın metinleri çıkar
                full_text = element_text
                
                # Gereksiz metinleri çıkar
                if author_name:
                    full_text = full_text.replace(author_name, "")
                
                common_texts = [
                    "Varsayılan", "Default", "Abone ol", "Subscribe", "Follow",
                    "seviye şehir uzmanı", "level local guide", "yerel rehber",
                    "Yanıtla", "Reply", "Beğen", "Like", "Share", "Paylaş"
                ]
                
                for common in common_texts:
                    full_text = full_text.replace(common, "")
                
                # Fazla boşlukları temizle
                text = re.sub(r'\s+', ' ', full_text).strip()
                
                # Çok uzun metinleri kısalt (veri kalitesini artırmak için)
                if len(text) > 2000:
                    text = text[:2000] + "..."
        
        # Adım 3: İlave temizleme
        if text:
            # Başta ve sonda tek karakterleri temizle
            text = re.sub(r'^[^a-zA-Z0-9а-яА-ЯçğıöşüÇĞİÖŞÜ]+', '', text)
            text = re.sub(r'[^a-zA-Z0-9а-яА-ЯçğıöşüÇĞİÖŞÜ]+$', '', text)
            
            # İşletme yanıtını çıkar (genellikle "İşletme yanıtı: ..." formatında olur)
            text = re.sub(r'İşletme[^:]*:[^\n]*\n.*$', '', text, flags=re.DOTALL)
            text = re.sub(r'Business[^:]*:[^\n]*\n.*$', '', text, flags=re.DOTALL)
            text = re.sub(r'Owner[^:]*:[^\n]*\n.*$', '', text, flags=re.DOTALL)
        
        return text
    
    async def extract_date(self, review_element):
        """Yorum elementinden tarihi çıkar"""
        date = None
        
        # Adım 1: CSS seçicilerle ara
        date_selectors = [
            "[class*='date']", 
            "[class*='time']", 
            "time", 
            ".business-review-view__date",  # yeni eklendi
            "[class*='when']",
            "[class*='posted']",
            "meta[itemprop='datePublished']",     # meta etiketi ile tarih
            "span[aria-label*='Değerlendirme']"   # bazı sitelerde yıldız + tarih bilgisi aynı span içinde olabilir
        ]
        
        for selector in date_selectors:
            try:
                date_element = await review_element.query_selector(selector)
                if date_element:
                    date_content = await date_element.text_content()
                    date_content = date_content.strip()
                    if date_content and len(date_content) > 2 and len(date_content) < 50:
                        # Tarih formatını daha da doğrula
                        if re.search(r'\d', date_content):  # En azından bir rakam içermeli
                            date = date_content
                            break
            except:
                pass
        
        # Adım 2: Metnin içinde tarih formatını ara
        if not date:
            element_text = await review_element.text_content()
            date_patterns = [
                r'(\d{1,2}\s+[A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü]+\s+\d{4})',  # "15 Ocak 2023"
                r'(\d{1,2}\s+[A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü]+)',  # "15 Ocak"
                r'([A-Za-zА-Яа-яÇçĞğİıÖöŞşÜü]+\s+\d{1,2},\s*\d{4})',  # "Ocak 15, 2023"
                r'(\d{1,2}\.\d{1,2}\.\d{4})',  # "15.01.2023"
                r'(\d{1,2}/\d{1,2}/\d{4})'  # "15/01/2023"
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, element_text)
                if match:
                    date = match.group(1)
                    break
        
        return date
    
    async def has_photos(self, review_element):
        """Yorum elementinde fotoğraf olup olmadığını kontrol et"""
        # Fotoğraf göstergeleri
        photo_selectors = [
            "img[src*='review']",
            "img[class*='photo']", 
            "[class*='gallery']",
            "[class*='photo']",
            "[class*='image']",
            "[class*='media']"
        ]
        
        for selector in photo_selectors:
            try:
                photos = await review_element.query_selector_all(selector)
                if len(photos) > 0:
                    return True
            except:
                pass
        
        # HTML içinde fotoğraf göstergeleri ara
        element_html = await self.page.evaluate('(element) => element.outerHTML', review_element)
        
        # İçerikte fotoğraf referansı var mı?
        if re.search(r'(photo|image|picture|gallery|фото|resim)', element_html, re.IGNORECASE):
            return True
            
        return False
    
    async def extract_business_reply(self, review_element):
        """İşletme yanıtını çıkar"""
        # İşletme yanıtı seçicileri
        reply_selectors = [
            "[class*='reply']",
            "[class*='response']",
            "[class*='owner']",
            "[class*='business-comment']",
            ".spoiler-view__reply"
        ]
        
        for selector in reply_selectors:
            try:
                reply_element = await review_element.query_selector(selector)
                if reply_element:
                    reply_text = await reply_element.text_content()
                    reply_text = reply_text.strip()
                    
                    # Gereksiz metinleri temizle
                    reply_text = re.sub(r'(İşletme yanıtı|Business reply|Owner response|ответ владельца)[:\s]*', '', reply_text, flags=re.IGNORECASE)
                    reply_text = reply_text.strip()
                    
                    if reply_text and len(reply_text) > 5:
                        return reply_text
            except:
                pass
        
        return None
    
    async def scrape_all_reviews(self, business_url, max_reviews=None):
        """Tüm yorumları çek"""
        
        # Browser başlat
        await self.start_browser()
        
        try:
            # İşletme sayfasına git ve bilgileri al
            business_id, business_name = await self.navigate_to_place(business_url)
            
            # İş yeri bilgilerini kaydet (otomatik kaydetme için)
            self.business_id = business_id
            self.business_name = business_name
            
            if not business_id:
                logger.error("❌ İşletme bilgileri alınamadı!")
                return {
                    'business_id': None,
                    'business_name': None,
                    'reviews': [],
                    'scrape_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
            # Yorumları çek
            reviews = await self.scrape_reviews_with_continuous_scroll(max_reviews)
            
            # Sonuçları döndür
            return {
                'business_id': business_id,
                'business_name': business_name,
                'reviews': reviews,
                'total_review_count': self.total_reviews,
                'scraped_review_count': len(reviews),
                'scrape_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'scrape_url': business_url
            }
            
        except Exception as e:
            logger.error(f"💥 Scraping işlemi sırasında hata: {e}")
            return {
                'business_id': None,
                'business_name': None,
                'reviews': [],
                'scrape_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'error': str(e)
            }
        
        finally:
            await self.close()
    
    async def save_to_files(self, data, filename_base):
        """Verileri dosyalara kaydet"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # JSON kaydet
        json_filename = f"data/raw/{filename_base}_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # CSV kaydet
        if data and 'reviews' in data and data['reviews']:
            df = pd.DataFrame(data['reviews'])
            csv_filename = f"data/processed/{filename_base}_{timestamp}.csv"
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            
            logger.info(f"💾 Veriler kaydedildi:")
            logger.info(f"   JSON: {json_filename}")
            logger.info(f"   CSV:  {csv_filename}")
            
            return json_filename, csv_filename
        else:
            logger.warning("⚠️ Kaydedilecek yorum verisi bulunamadı")
            return None, None
    
    async def close(self):
        """Browser'ı kapat"""
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()

# Ana fonksiyon
async def main():
    """Ana scraping fonksiyonu"""
    
    # Kullanıcı tercihlerini sor
    print("\n🚀 Yandex Maps Geliştirilmiş Yorum Scraper")
    print("=" * 50)
    print("Bu script, Yandex Maps yorumlarını yüksek kalitede çeker")
    
    # URL bilgisi
    default_url = "https://yandex.com.tr/maps/org/istanbul_havalimani/85454152633/?ll=28.752054%2C41.279299&utm_campaign=desktop&utm_medium=search&utm_source=maps&z=13.65"
    print(f"\n📍 Hangi mekanın yorumlarını çekmek istiyorsunuz?")
    print(f"   Varsayılan: {default_url}")
    business_url = input("URL: ").strip() or default_url
    
    # Maksimum yorum sayısı
    print("\n📊 Maksimum kaç yorum çekilsin?")
    print("   (Varsayılan: 2000, 'all' tüm yorumlar için)")
    max_reviews_input = input("Maksimum yorum sayısı: ").strip() or "2000"
    
    if max_reviews_input.lower() in ["all", "tüm", "hepsi"]:
        max_reviews = None  # Tüm yorumlar
    else:
        try:
            max_reviews = int(max_reviews_input)
        except ValueError:
            print("⚠️ Geçersiz sayı, varsayılan değer (2000) kullanılıyor.")
            max_reviews = 2000
    
    # Headless mod ayarı
    print("\n🖥️ Tarayıcı görünürlüğü:")
    print("   [1] Görünmez mod (daha hızlı, arka planda çalışır)")
    print("   [2] Görünür mod (daha yavaş, tarayıcıyı görebilirsiniz)")
    headless_choice = input("Seçiminiz (1/2): ").strip() or "1"
    
    # Scraper'ı başlat
    scraper = YandexMapsScraper()
    
    try:
        # Başlangıç zamanını kaydet
        start_time = time.time()
        
        # Tarayıcı görünürlük ayarını güncelle
        if headless_choice == "2":
            print("✅ Görünür mod seçildi, tarayıcı penceresi açılacak.")
            # start_browser fonksiyonu çağrılmadan önce browser nesnesini güncelle
            scraper.browser = None  # İlk browser nesnesini temizle
            
        # Yorumları çek
        logger.info(f"🚀 Yorum toplama işlemi başlatılıyor: {business_url}")
        data = await scraper.scrape_all_reviews(
            business_url=business_url,
            max_reviews=max_reviews
        )
        
        # Bitiş zamanını kaydet ve süreyi hesapla
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # Dosyalara kaydet
        await scraper.save_to_files(data, "yandex_reviews_enhanced")
        
        # Özet bilgi
        if data and 'reviews' in data and data['reviews']:
            reviews = data['reviews']
            
            logger.info(f"\n📊 ÖZET:")
            logger.info(f"   İşletme: {data['business_name']}")
            logger.info(f"   ID: {data['business_id']}")
            logger.info(f"   Sitede gösterilen toplam yorum sayısı: {data.get('total_review_count', 'Belirsiz')}")
            logger.info(f"   Çekilen yorum sayısı: {len(reviews)}")
            logger.info(f"   Tekrar kontrolünden geçirilmiş veri")
            logger.info(f"   Geçen süre: {elapsed_time:.2f} saniye")
            
            valid_ratings = [r.get('rating') for r in reviews if r.get('rating') is not None]
            if valid_ratings:
                avg_rating = sum(valid_ratings) / len(valid_ratings)
                logger.info(f"   Ortalama puan: {avg_rating:.1f}⭐")
            
            print("\n" + "=" * 40)
            print(f"✅ İşlem tamamlandı!")
            print(f"📊 Toplam {len(reviews)} yorum çekildi (sitede gösterilen: {data.get('total_review_count', 'Belirsiz')})")
            print(f"⏱️ Geçen süre: {elapsed_time:.2f} saniye")
            print(f"💾 Veriler data/ klasörüne kaydedildi")
            print("=" * 40)
        else:
            logger.warning("⚠️ Hiç yorum bulunamadı!")
            print("\n❌ Hiç yorum bulunamadı!")
    
    except Exception as e:
        logger.error(f"💥 Hata oluştu: {e}")
        print(f"\n❌ Bir hata oluştu: {e}")
    
    finally:
        # Temizlik
        await scraper.close()
        print("\n👋 Yandex Maps Scraper kapatılıyor...")

if __name__ == "__main__":
    # Scripti çalıştır
    print("🚀 Yandex Maps Geliştirilmiş Yorum Scraper başlatılıyor...")
    asyncio.run(main())