import time
import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision


url = "http://localhost:8086"
token = "MN-Vj21QIiLCafivbctXjx_sM2gQrSPMfPbV9Dkxv__M9eDgYC9dl2UcKkpPPyOD0ete4z3B6yB9cPnBTtxKcQ=="
org = "c31d0f1a97dce5f5"
bucket = "ef1cc90cbeadbdd6"


doviz_api_key = "0908baf8abf3316f202514df"
doviz_api_url = f"https://v6.exchangerate-api.com/v6/{doviz_api_key}/latest/USD"
cekme_suresi_saniye = 30  


client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api()  

def doviz_verisi_cek_ve_yaz():
    try:
        response = requests.get(doviz_api_url)
        response.raise_for_status()  
        data = response.json()
        rates = data.get("conversion_rates", {})

        if not rates:
            print("[UYARI] API'den 'conversion_rates' verisi alınamadı.")
            return

        for currency, rate in rates.items():
            point = (
                Point("dovizdb")
                .tag("sembol", currency)
                .field("fiyat", float(rate))
                .time(time.time_ns(), WritePrecision.NS)
            )
            write_api.write(bucket=bucket, org=org, record=point)
            print(f"[Döviz] USD -> {currency}: {rate}")

    except requests.exceptions.RequestException as e:
        print("[HATA] Döviz API bağlantı hatası:", e)
    except Exception as e:
        print("[HATA] Döviz verisi işlenirken hata oluştu:", e)

def main():
    try:
        while True:
            print(" Döviz verisi çekiliyor...")
            doviz_verisi_cek_ve_yaz()
            time.sleep(cekme_suresi_saniye)
    except KeyboardInterrupt:
        print(" Program kullanıcı tarafından durduruldu.")
    finally:
        client.close()  

if __name__ == "__main__":
    main()
