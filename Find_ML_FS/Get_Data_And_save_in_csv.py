import time
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch

# กำหนด Elasticsearch URL ในรูปแบบ http://ip_address:port
es = Elasticsearch(['http://192.168.53.230:9200/'])

# สร้างฟังก์ชันเพื่อดึงข้อมูลและบันทึกเป็น CSV
def fetch_and_save_elasticsearch_data(es, index_name, csv_filename):
    # ใช้คำสั่ง search() เพื่อค้นหาข้อมูลใน index ที่ระบุ
    search_body = {
        "query": {
            "match_all": {}  # ค้นหาทุกเอกสารใน index
        },
        "size": 1000  # เพิ่มขนาดหน้าเป็น 1000 เร็คอร์ดต่อหน้า
    }

    response = es.search(index=index_name, body=search_body)

    # ดึงข้อมูลเอกสารจาก Elasticsearch response
    documents = [hit['_source'] for hit in response['hits']['hits']]

    if documents:
        # สร้าง DataFrame จากข้อมูล
        df = pd.DataFrame(documents)

        # บันทึก DataFrame เป็นไฟล์ CSV
        df.to_csv(csv_filename, index=False)
    else:
        # พิมพ์ข้อความเมื่อ Elasticsearch ไม่มีข้อมูล
        print("Elasticsearch ไม่มีข้อมูล")

# สร้างฟังก์ชันเพื่อตรวจสอบข้อมูลใหม่และดึงมาบันทึกเป็น CSV
def check_and_save_data_periodically(es, index_name, csv_filename, interval_seconds):
    while True:
        # เรียกใช้ฟังก์ชันเพื่อดึงข้อมูลและบันทึกเป็น CSV
        fetch_and_save_elasticsearch_data(es, index_name, csv_filename)

        # นอนเป็นระยะเวลา interval_seconds ก่อนที่จะทำการตรวจสอบข้อมูลใหม่อีกครั้ง
        time.sleep(interval_seconds)

if __name__ == "__main__":
    # ชื่อของ index ที่คุณต้องการดู
    today_date = datetime.now().strftime("%Y.%m.%d")
    index_name = f"itdml-{today_date}"

    # กำหนดชื่อไฟล์ CSV ที่ต้องการบันทึก
    csv_filename = "elasticsearch_data.csv"

    # กำหนดระยะเวลาที่ต้องการตรวจสอบข้อมูลใหม่ใน Elasticsearch (เช่น ทุก 1 วินาที)
    interval_seconds = 1  # 1 วินาที

    # เรียกใช้ฟังก์ชันเพื่อตรวจสอบและบันทึกข้อมูลอัตโนมัติ
    check_and_save_data_periodically(es, index_name, csv_filename, interval_seconds)
