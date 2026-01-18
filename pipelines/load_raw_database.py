from load.load_raw_top_products import BestSellersETL, ScraperConfig, DatabaseConfig

urls = [
    {'amazon_url': "/Best-Sellers-Electronics/zgbs/electronics/", 'category_name': "Electronics"},
    # Puedes agregar más categorías aquí
]

def main():
    scraper_config = ScraperConfig(
        headless=True,
        max_no_change_attempts=3,
    )
    db_config = DatabaseConfig()
    for item in urls:
        print(f"\n=== Procesando categoría: {item['category_name']} ===")
        etl = BestSellersETL(
            scraper_config=scraper_config,
            db_config=db_config,
            amazon_url=item['amazon_url'],
            category=item['category_name'],
        )
        try:
            stats = etl.run()
            if stats["success"]:
                print(f"   Productos scrapeados: {stats['products_scraped']}")
                print(f"\n✅ ETL completado para {item['category_name']}")
            else:
                print(f"\n❌ ETL fallido para {item['category_name']}: {stats.get('error')}")
        except KeyboardInterrupt:
            print("\n\n⚠ ETL interrumpido por el usuario")
            break
        except Exception as e:
            print(f"\n❌ Error fatal en {item['category_name']}: {e}")

if __name__ == "__main__":
    main()