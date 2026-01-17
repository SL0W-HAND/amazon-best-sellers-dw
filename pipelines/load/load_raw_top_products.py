"""
ETL: Load Raw Data
==================
Loads scraped data from best_sells_scraping into PostgreSQL raw tables.

Usage:
    python load_raw_data.py
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from extract.best_sells_scraping import (
    AmazonBestSellersScraper,
    ScraperConfig,
    ScrapingResult,
    Product,
)


# =============================================================================
# CONFIGURATION
# =============================================================================

class DatabaseConfig:
    """Database configuration loaded from environment variables."""
    
    def __init__(self, env_path: Optional[str] = None):
        """Load configuration from .env file."""
        if env_path:
            load_dotenv(env_path)
        else:
            # Default to credential.env in the project root directory
            default_env = Path(__file__).parent.parent.parent / "credential.env"
            load_dotenv(default_env)
        
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = os.getenv("POSTGRES_DB", "amazon_data")
        self.user = os.getenv("POSTGRES_USER", "amazon")
        self.password = os.getenv("POSTGRES_PASSWORD", "amazon123")
    
    @property
    def connection_params(self) -> dict:
        """Return connection parameters as a dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }


# =============================================================================
# DATABASE LOADER
# =============================================================================

class BestSellersLoader:
    """Handles loading scraped best sellers data into PostgreSQL."""
    
    BATCH_SIZE = 100
    
    INSERT_QUERY = """
        INSERT INTO raw.best_sellers (
            category,
            rank_position,
            asin,
            product_name,
            product_url,
            scraped_at
        ) VALUES (
            %(category)s,
            %(rank_position)s,
            %(asin)s,
            %(product_name)s,
            %(product_url)s,
            %(scraped_at)s
        )
    """
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.connection = None
        self.cursor = None
    
    def connect(self) -> None:
        """Establish database connection."""
        print("🔌 Conectando a PostgreSQL...")
        try:
            self.connection = psycopg2.connect(**self.db_config.connection_params)
            self.cursor = self.connection.cursor()
            print(f"   ✓ Conectado a {self.db_config.database}@{self.db_config.host}")
        except psycopg2.Error as e:
            print(f"   ✗ Error de conexión: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("🔌 Conexión cerrada")
    
    def load_products(
        self,
        products: list[Product],
        category: str,
        scraped_at: Optional[datetime] = None
    ) -> int:
        """
        Load a list of products into raw.best_sellers.
        
        Args:
            products: List of Product objects from the scraper
            category: Category name (e.g., 'electronics')
            scraped_at: Timestamp of the scrape (defaults to now)
        
        Returns:
            Number of records inserted
        """
        if not products:
            print("   ⚠ No hay productos para cargar")
            return 0
        
        scraped_at = scraped_at or datetime.now()
        
        # Transform products to database records
        records = [
            {
                "category": category,
                "rank_position": index + 1,  # 1-based ranking
                "asin": product.asin,
                "product_name": product.name,
                "product_url": product.url,
                "scraped_at": scraped_at,
            }
            for index, product in enumerate(products)
            if product.asin and product.asin != 'N/A'
        ]
        
        if not records:
            print("   ⚠ No hay productos válidos (todos tienen ASIN inválido)")
            return 0
        
        print(f"📥 Insertando {len(records)} productos en raw.best_sellers...")
        
        try:
            execute_batch(
                self.cursor,
                self.INSERT_QUERY,
                records,
                page_size=self.BATCH_SIZE
            )
            self.connection.commit()
            print(f"   ✓ {len(records)} registros insertados correctamente")
            return len(records)
        
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"   ✗ Error al insertar: {e}")
            raise
    
    def get_last_scrape_stats(self, category: str) -> dict:
        """Get statistics from the last scrape for a category."""
        query = """
            SELECT 
                COUNT(*) as total_products,
                MAX(scraped_at) as last_scrape,
                MIN(rank_position) as min_rank,
                MAX(rank_position) as max_rank
            FROM raw.best_sellers
            WHERE category = %s
            AND scraped_at = (
                SELECT MAX(scraped_at) 
                FROM raw.best_sellers 
                WHERE category = %s
            )
        """
        self.cursor.execute(query, (category, category))
        row = self.cursor.fetchone()
        
        return {
            "total_products": row[0],
            "last_scrape": row[1],
            "min_rank": row[2],
            "max_rank": row[3],
        }


# =============================================================================
# ETL PIPELINE
# =============================================================================

class BestSellersETL:
    """
    ETL pipeline that orchestrates scraping and loading.
    
    Extract: Run the Amazon Best Sellers scraper
    Transform: Clean and validate product data
    Load: Insert into PostgreSQL raw.best_sellers
    """
    
    def __init__(
        self,
        scraper_config: Optional[ScraperConfig] = None,
        db_config: Optional[DatabaseConfig] = None,
        amazon_url: str = "/Best-Sellers-Electronics/zgbs/electronics/",
        category: str = "electronics",
    ):
        self.scraper_config = scraper_config or ScraperConfig()
        # Set the start_path from amazon_url
        self.scraper_config.start_path = amazon_url
        self.db_config = db_config or DatabaseConfig()
        self.amazon_url = amazon_url
        self.category = category
        self.loader = BestSellersLoader(self.db_config)
    
    def run(self) -> dict:
        """
        Execute the full ETL pipeline.
        
        Returns:
            Dictionary with pipeline statistics
        """
        start_time = datetime.now()
        stats = {
            "started_at": start_time,
            "amazon_url": self.amazon_url,
            "category": self.category,
            "products_scraped": 0,
            "products_loaded": 0,
            "pages_processed": 0,
            "success": False,
            "error": None,
        }
        
        try:
            print("\n" + "=" * 80)
            print("🚀 INICIANDO ETL: Best Sellers → PostgreSQL")
            print("=" * 80)
            
            # --- EXTRACT ---
            print("\n📤 FASE 1: EXTRACCIÓN (Scraping)")
            print("-" * 40)
            
            scraper = AmazonBestSellersScraper(self.scraper_config)
            result = scraper.run()
            
            stats["products_scraped"] = result.total_products
            stats["pages_processed"] = result.pages_processed
            
            print(f"\n   Productos extraídos: {result.total_products}")
            print(f"   Páginas procesadas: {result.pages_processed}")
            
            # --- TRANSFORM ---
            print("\n🔄 FASE 2: TRANSFORMACIÓN")
            print("-" * 40)
            
            valid_products = self._filter_valid_products(result.products)
            print(f"   Productos válidos: {len(valid_products)}/{result.total_products}")
            
            # --- LOAD ---
            print("\n📥 FASE 3: CARGA (PostgreSQL)")
            print("-" * 40)
            
            self.loader.connect()
            
            try:
                loaded_count = self.loader.load_products(
                    products=valid_products,
                    category=self.category,
                    scraped_at=start_time
                )
                stats["products_loaded"] = loaded_count
                stats["success"] = True
                
            finally:
                self.loader.disconnect()
            
            # --- SUMMARY ---
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            stats["duration_seconds"] = duration
            
            self._print_summary(stats)
            
        except Exception as e:
            stats["success"] = False
            stats["error"] = str(e)
            print(f"\n❌ ERROR EN ETL: {e}")
            raise
        
        return stats
    
    def _filter_valid_products(self, products: list[Product]) -> list[Product]:
        """Filter out products with invalid or missing data."""
        valid = []
        
        for product in products:
            # Skip products without valid ASIN
            if not product.asin or product.asin == 'N/A':
                continue
            
            # Skip products without name
            if not product.name or product.name == 'N/A':
                continue
            
            valid.append(product)
        
        return valid
    
    def _print_summary(self, stats: dict) -> None:
        """Print ETL execution summary."""
        print("\n" + "=" * 80)
        print("📊 RESUMEN ETL")
        print("=" * 80)
        print(f"   Categoría: {stats['category']}")
        print(f"   Productos scrapeados: {stats['products_scraped']}")
        print(f"   Productos cargados: {stats['products_loaded']}")
        print(f"   Páginas procesadas: {stats['pages_processed']}")
        print(f"   Duración: {stats.get('duration_seconds', 0):.2f} segundos")
        print(f"   Estado: {'✓ ÉXITO' if stats['success'] else '✗ ERROR'}")
        print("=" * 80)


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Main entry point for the ETL pipeline."""
    # Configure the scraper
    scraper_config = ScraperConfig(
        headless=True,
        max_no_change_attempts=3,
    )
    
    # Load database configuration from credential.env
    db_config = DatabaseConfig()
    
    # Create and run ETL pipeline
    etl = BestSellersETL(
        scraper_config=scraper_config,
        db_config=db_config,
        amazon_url="/Best-Sellers-Electronics/zgbs/electronics/",
        category="electronics",
    )
    
    try:
        stats = etl.run()
        
        if stats["success"]:
            print("\n✅ ETL completado exitosamente")
            sys.exit(0)
        else:
            print(f"\n❌ ETL fallido: {stats.get('error')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠ ETL interrumpido por el usuario")
        sys.exit(130)
    
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
