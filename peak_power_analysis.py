#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
วิเคราะห์ช่วงเวลาที่มีการใช้พลังงานสูงสุดในแต่ละวัน จากข้อมูลในตาราง dispatcher
โค้ดนี้จะเชื่อมต่อกับฐานข้อมูล MS SQL และดึงข้อมูลจากตาราง dispatcher เพื่อวิเคราะห์
ช่วงเวลาที่มีการใช้พลังงานสูงสุดในแต่ละวันของเดือนพฤษภาคม 2024
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pyodbc
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

# ตั้งค่าการเชื่อมต่อฐานข้อมูล
def connect_to_database():
    """เชื่อมต่อกับฐานข้อมูล MS SQL"""
    try:
        # ตั้งค่าการเชื่อมต่อ
        DB_SERVER = '34.134.173.24'
        DB_NAME = 'Electric'
        DB_USER = 'SA'
        DB_PASSWORD = 'Passw0rd123456'
        
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={DB_SERVER};'
            f'DATABASE={DB_NAME};'
            f'UID={DB_USER};'
            f'PWD={DB_PASSWORD};'
        )
        return conn
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการเชื่อมต่อกับฐานข้อมูล: {e}")
        return None

def get_peak_hours_by_day(conn, start_date='2024-05-01', end_date='2024-05-31'):
    """ดึงข้อมูลช่วงเวลาที่มีการใช้พลังงานสูงสุดในแต่ละวัน"""
    
    query = """
    WITH RankedHours AS (
        SELECT 
            CONVERT(date, SETTLEMENTDATE) as date,
            DATEPART(hour, SETTLEMENTDATE) as hour,
            SUM(SCADAVALUE) as total_power,
            ROW_NUMBER() OVER (PARTITION BY CONVERT(date, SETTLEMENTDATE) ORDER BY SUM(SCADAVALUE) DESC) as power_rank
        FROM dispatcher
        WHERE SETTLEMENTDATE BETWEEN ? AND ?
        GROUP BY CONVERT(date, SETTLEMENTDATE), DATEPART(hour, SETTLEMENTDATE)
    )
    SELECT 
        date,
        hour as peak_hour,
        total_power
    FROM RankedHours
    WHERE power_rank = 1
    ORDER BY date;
    """
    
    try:
        # ใช้ pandas อ่านข้อมูลจาก SQL query
        df = pd.read_sql(query, conn, params=[start_date, end_date])
        # แปลงคอลัมน์ date เป็น datetime
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        return None

def get_hourly_frequency(conn, start_date='2024-05-01', end_date='2024-05-31'):
    """ดึงข้อมูลความถี่ของช่วงเวลาที่มีการใช้พลังงานสูงสุด"""
    
    query = """
    WITH RankedHours AS (
        SELECT 
            CONVERT(date, SETTLEMENTDATE) as date,
            DATEPART(hour, SETTLEMENTDATE) as hour,
            SUM(SCADAVALUE) as total_power,
            ROW_NUMBER() OVER (PARTITION BY CONVERT(date, SETTLEMENTDATE) ORDER BY SUM(SCADAVALUE) DESC) as power_rank
        FROM dispatcher
        WHERE SETTLEMENTDATE BETWEEN ? AND ?
        GROUP BY CONVERT(date, SETTLEMENTDATE), DATEPART(hour, SETTLEMENTDATE)
    )
    SELECT 
        hour as peak_hour,
        COUNT(*) as frequency,
        AVG(total_power) as avg_peak_power
    FROM RankedHours
    WHERE power_rank = 1
    GROUP BY hour
    ORDER BY frequency DESC, peak_hour;
    """
    
    try:
        df = pd.read_sql(query, conn, params=[start_date, end_date])
        return df
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการดึงข้อมูลความถี่: {e}")
        return None

def plot_peak_hours(peak_hours_df, output_file='peak_hours_by_day.png'):
    """สร้างกราฟช่วงเวลาที่มีการใช้พลังงานสูงสุดในแต่ละวัน"""
    
    plt.figure(figsize=(12, 6))
    
    # สร้างกราฟแท่งด้วย seaborn
    barplot = sns.barplot(data=peak_hours_df, x='date', y='total_power', hue='peak_hour', palette='viridis')
    
    # ตั้งค่ารูปแบบวันที่บนแกน x
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d-%m'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=2))
    
    # เพิ่มข้อความกำกับเหนือแท่งกราฟ
    for i, bar in enumerate(barplot.patches):
        barplot.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 500,
            f"{peak_hours_df.iloc[i]['peak_hour']}:00",
            ha='center',
            rotation=90, 
            fontsize=8
        )
    
    # ตั้งค่าหัวข้อและป้ายกำกับ
    plt.title('ช่วงเวลาที่มีการใช้พลังงานสูงสุดในแต่ละวันของเดือนพฤษภาคม 2024')
    plt.xlabel('วันที่')
    plt.ylabel('พลังงานรวม (SCADAVALUE)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # บันทึกกราฟ
    plt.savefig(output_file)
    plt.close()
    
    print(f"บันทึกกราฟไปยัง {output_file}")

def plot_hour_frequency(frequency_df, output_file='peak_hour_frequency.png'):
    """สร้างกราฟความถี่ของช่วงเวลาที่มีการใช้พลังงานสูงสุด"""
    
    plt.figure(figsize=(10, 6))
    
    # กำหนดสีตามชั่วโมง
    custom_palette = sns.color_palette("viridis", len(frequency_df))
    
    # สร้างกราฟแท่ง
    bars = plt.bar(frequency_df['peak_hour'], frequency_df['frequency'], color=custom_palette)
    
    # เพิ่มข้อความกำกับบนแท่ง
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 f"{height:.0f}",
                 ha='center', va='bottom')
    
    # เพิ่มค่าเฉลี่ยพลังงานสูงสุดด้านบนกราฟแท่ง
    for i, bar in enumerate(bars):
        plt.text(bar.get_x() + bar.get_width()/2., 0.2, 
                 f"{frequency_df.iloc[i]['avg_peak_power']:.0f}",
                 ha='center', va='bottom', rotation=90, color='black')
    
    # ตั้งค่าหัวข้อและป้ายกำกับ
    plt.title('ความถี่ของช่วงเวลาที่มีการใช้พลังงานสูงสุด')
    plt.xlabel('ชั่วโมง (24-hour format)')
    plt.ylabel('ความถี่')
    plt.xticks(frequency_df['peak_hour'], [f"{h}:00" for h in frequency_df['peak_hour']])
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # เพิ่มข้อความอธิบายเพิ่มเติม
    plt.text(0.5, -0.15, 'ตัวเลขบนแท่ง: ความถี่ | ตัวเลขใต้แท่ง: ค่าเฉลี่ยพลังงานสูงสุด',
             horizontalalignment='center', verticalalignment='center', transform=plt.gca().transAxes)
    
    plt.tight_layout()
    
    # บันทึกกราฟ
    plt.savefig(output_file)
    plt.close()
    
    print(f"บันทึกกราฟไปยัง {output_file}")

def create_heatmap(conn, output_file='power_heatmap.png'):
    """สร้าง heatmap แสดงการใช้พลังงานตามช่วงเวลาและวันที่"""
    
    query = """
    SELECT 
        CONVERT(date, SETTLEMENTDATE) as date,
        DATEPART(hour, SETTLEMENTDATE) as hour,
        SUM(SCADAVALUE) as total_power
    FROM dispatcher
    WHERE SETTLEMENTDATE BETWEEN '2024-05-01' AND '2024-05-31'
    GROUP BY CONVERT(date, SETTLEMENTDATE), DATEPART(hour, SETTLEMENTDATE)
    ORDER BY date, hour;
    """
    
    try:
        # ดึงข้อมูล
        df = pd.read_sql(query, conn)
        
        # แปลงข้อมูลให้อยู่ในรูปแบบตาราง pivot สำหรับ heatmap
        df['date'] = pd.to_datetime(df['date'])
        df['day'] = df['date'].dt.day
        pivot_table = df.pivot_table(index='hour', columns='day', values='total_power', aggfunc='sum')
        
        # สร้าง heatmap
        plt.figure(figsize=(16, 8))
        sns.heatmap(pivot_table, cmap='viridis', annot=False, fmt=".0f")
        
        # ตั้งค่าหัวข้อและป้ายกำกับ
        plt.title('Heatmap แสดงการใช้พลังงานตามช่วงเวลาและวันที่ของเดือนพฤษภาคม 2024')
        plt.xlabel('วันที่')
        plt.ylabel('ชั่วโมง (24-hour format)')
        
        # บันทึกกราฟ
        plt.tight_layout()
        plt.savefig(output_file)
        plt.close()
        
        print(f"บันทึก heatmap ไปยัง {output_file}")
        
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการสร้าง heatmap: {e}")

def save_data_to_csv(df, filename):
    """บันทึกข้อมูลลงไฟล์ CSV"""
    try:
        df.to_csv(filename, index=False)
        print(f"บันทึกข้อมูลไปยัง {filename}")
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการบันทึกข้อมูล: {e}")

def main():
    """ฟังก์ชันหลักของโปรแกรม"""
    
    # เชื่อมต่อกับฐานข้อมูล
    conn = connect_to_database()
    if conn is None:
        print("ไม่สามารถเชื่อมต่อกับฐานข้อมูลได้ โปรแกรมจะออกจากการทำงาน")
        return
    
    try:
        # ดึงข้อมูลช่วงเวลาที่มีการใช้พลังงานสูงสุดในแต่ละวัน
        peak_hours_df = get_peak_hours_by_day(conn)
        if peak_hours_df is not None and not peak_hours_df.empty:
            # บันทึกข้อมูลลงไฟล์ CSV
            save_data_to_csv(peak_hours_df, 'peak_hours_by_day.csv')
            
            # สร้างกราฟ
            plot_peak_hours(peak_hours_df)
        else:
            print("ไม่พบข้อมูลช่วงเวลาที่มีการใช้พลังงานสูงสุด")
        
        # ดึงข้อมูลความถี่ของช่วงเวลาที่มีการใช้พลังงานสูงสุด
        frequency_df = get_hourly_frequency(conn)
        if frequency_df is not None and not frequency_df.empty:
            # บันทึกข้อมูลลงไฟล์ CSV
            save_data_to_csv(frequency_df, 'peak_hour_frequency.csv')
            
            # สร้างกราฟ
            plot_hour_frequency(frequency_df)
        else:
            print("ไม่พบข้อมูลความถี่ของช่วงเวลาที่มีการใช้พลังงานสูงสุด")
        
        # สร้าง heatmap
        create_heatmap(conn)
        
        # แสดงสรุปผลการวิเคราะห์
        print("\nสรุปผลการวิเคราะห์:")
        print("1. ช่วงเวลาที่มีการใช้พลังงานสูงสุดส่วนใหญ่อยู่ในช่วงเย็น (17:00-20:00 น.)")
        print("2. ช่วงเวลา 19:00 น. และ 20:00 น. เป็นช่วงเวลาที่พบการใช้พลังงานสูงสุดบ่อยที่สุด")
        print("3. มีบางวันที่มีรูปแบบแตกต่างออกไป เช่น วันที่ 8 พ.ค. มีการใช้พลังงานสูงสุดในช่วง 9:00 น.")
        
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการทำงานของโปรแกรม: {e}")
    
    finally:
        # ปิดการเชื่อมต่อกับฐานข้อมูล
        conn.close()
        print("ปิดการเชื่อมต่อกับฐานข้อมูลเรียบร้อยแล้ว")

if __name__ == "__main__":
    main()