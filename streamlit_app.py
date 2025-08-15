import streamlit as st
import os
import pandas as pd
import mysql.connector
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
from datetime import datetime

# ==============================================================================
# ส่วนที่ 1: ส่วนตรรกะและฟังก์ชันหลัก (ปรับปรุงสำหรับ Streamlit)
# ==============================================================================

# --- ค่าคงที่และ List ต่างๆ (เหมือนเดิม) ---
col_name = ['job_number', 'list', 'col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'total', 'col10', 'col11']
list_q_v1 = [
    '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7', '1.8',
    '2.1', '2.2', '2.3', '2.4', '2.5', '2.6', '2.7', '2.8', '2.9',
    '3.1', '3.2', '3.3', '3.4', '3.5', '3.6', '3.7', '3.8', '3.9', '3.10', '3.11', '3.12', '3.13', '3.14', '3.15', '3.16', '3.17',
    '4.1', '4.2', '4.3', '4.4', '4.5', '4.6', '4.7',
    '5.1', '5.2', '5.3', '5.4', '5.5', '5.6'
]
list_col_v1 = ['job_number', 'q1_1', 'q1_2', 'q1_3', 'q1_4', 'q1_5', 'q1_6', 'q1_7', 'q1_8', 'q2_1', 'q2_2', 'q2_3', 'q2_4', 'q2_5', 'q2_6', 'q2_7', 'q2_8', 'q2_9', 'q3_1', 'q3_2', 'q3_3', 'q3_4', 'q3_5', 'q3_6', 'q3_7', 'q3_8', 'q3_9', 'q3_10', 'q3_11', 'q3_12', 'q3_13', 'q3_14', 'q3_15', 'q3_16', 'q3_17', 'q4_1', 'q4_2', 'q4_3', 'q4_4', 'q4_5', 'q4_6', 'q4_7', 'q5_1', 'q5_2', 'q5_3', 'q5_4', 'q5_5', 'q5_6', 'MHC1_1', 'MHC1_2', 'MHC1_3', 'MHC1_4', 'MHC1_5', 'MHC1_6', 'MHC1_7', 'MHC1_8', 'MHC2_1', 'MHC2_2', 'MHC2_3', 'MHC2_4', 'MHC2_5', 'MHC2_6', 'MHC2_7', 'MHC2_8', 'MHC2_9', 'MHC3_1', 'MHC3_2', 'MHC3_3', 'MHC3_4', 'MHC3_5', 'MHC3_6', 'MHC3_7', 'MHC3_8', 'MHC3_9', 'MHC3_10', 'MHC3_11', 'MHC3_12', 'MHC3_13', 'MHC3_14', 'MHC3_15', 'MHC3_16', 'MHC3_17', 'MHC4_1', 'MHC4_2', 'MHC4_3', 'MHC4_4', 'MHC4_5', 'MHC4_6', 'MHC4_7', 'MHC5_1', 'MHC5_2', 'MHC5_3', 'MHC5_4', 'MHC5_5', 'MHC5_6']
list_col_v2 = ['job_number', 'q1_1', 'q1_2', 'q1_3', 'q1_4', 'q1_5', 'q1_6', 'q1_7', 'q1_8', 'q2_1', 'q2_2', 'q2_3', 'q2_4', 'q2_5', 'q2_6', 'q2_7', 'q2_8', 'q2_9', 'q3_1', 'q3_2', 'q3_9', 'q3_3', 'q3_5', 'q3_6', 'q3_7', 'q3_8', 'q3_4', 'q3_10', 'q3_11', 'q3_12', 'q3_13', 'q3_14', 'q3_15', 'q3_16', 'q3_17', 'q4_1', 'q4_2', 'q4_3', 'q4_4', 'q4_5', 'q4_6', 'q4_7', 'q5_1', 'q5_2', 'q5_3', 'q5_4', 'q5_5', 'q5_6', 'MHC1_1', 'MHC1_2', 'MHC1_3', 'MHC1_4', 'MHC1_5', 'MHC1_6', 'MHC1_7', 'MHC1_8', 'MHC2_1', 'MHC2_2', 'MHC2_3', 'MHC2_4', 'MHC2_5', 'MHC2_6', 'MHC2_7', 'MHC2_8', 'MHC2_9', 'MHC3_1', 'MHC3_2', 'MHC3_9', 'MHC3_3', 'MHC3_5', 'MHC3_6', 'MHC3_7', 'MHC3_8', 'MHC3_4', 'MHC3_10', 'MHC3_11', 'MHC3_12', 'MHC3_13', 'MHC3_14', 'MHC3_15', 'MHC3_16', 'MHC3_17', 'MHC4_1', 'MHC4_2', 'MHC4_3', 'MHC4_4', 'MHC4_5', 'MHC4_6', 'MHC4_7', 'MHC5_1', 'MHC5_2', 'MHC5_3', 'MHC5_4', 'MHC5_5', 'MHC5_6']
upload_data_columns = ['job_number', 'q1_1', 'q1_2', 'q1_3', 'q1_4', 'q1_5', 'q1_6', 'q1_7', 'q1_8', 'q2_1', 'q2_2', 'q2_3', 'q2_4', 'q2_5', 'q2_6', 'q2_7', 'q2_8', 'q2_9', 'q3_1', 'q3_2', 'q3_3', 'q3_4', 'q3_5', 'q3_6', 'q3_7', 'q3_8', 'q3_9', 'q3_10', 'q3_11', 'q3_12', 'q3_13', 'q3_14', 'q3_15', 'q3_16', 'q3_17', 'q4_1', 'q4_2', 'q4_3', 'q4_4', 'q4_5', 'q4_6', 'q4_7', 'q5_1', 'q5_2', 'q5_3', 'q5_4', 'q5_5', 'q5_6', 'pc_name_input', 'datetime_stamp', 'MHC1_1', 'MHC1_2', 'MHC1_3', 'MHC1_4', 'MHC1_5', 'MHC1_6', 'MHC1_7', 'MHC1_8', 'MHC2_1', 'MHC2_2', 'MHC2_3', 'MHC2_4', 'MHC2_5', 'MHC2_6', 'MHC2_7', 'MHC2_8', 'MHC2_9', 'MHC3_1', 'MHC3_2', 'MHC3_3', 'MHC3_4', 'MHC3_5', 'MHC3_6', 'MHC3_7', 'MHC3_8', 'MHC3_9', 'MHC3_10', 'MHC3_11', 'MHC3_12', 'MHC3_13', 'MHC3_14', 'MHC3_15', 'MHC3_16', 'MHC3_17', 'MHC4_1', 'MHC4_2', 'MHC4_3', 'MHC4_4', 'MHC4_5', 'MHC4_6', 'MHC4_7', 'MHC5_1', 'MHC5_2', 'MHC5_3', 'MHC5_4', 'MHC5_5', 'MHC5_6']

def create_g_service():
    """สร้าง Google Sheets service โดยใช้ข้อมูลจาก st.secrets"""
    try:
        # โหลดข้อมูล credentials จาก st.secrets
        creds_json = json.loads(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(creds_json, scopes=[st.secrets["SCOPES"]])
        service = build(st.secrets["API_SERVICE_NAME"], st.secrets["API_VERSION"], credentials=creds)
        print("เชื่อมต่อ Google Sheets Service สำเร็จ")
        return service
    except Exception as e:
        st.error(f"Google Sheets Error: ไม่สามารถเชื่อมต่อ Google Sheets Service ได้:\n{e}")
        return None

def post_log_to_gsheet(log_data):
    """ส่ง Log ไปยัง Google Sheet"""
    gsheet_service = create_g_service()
    if not gsheet_service: return False
    try:
        df_log = pd.DataFrame(log_data)
        values_to_append = df_log.values.tolist()
        gsheet_service.spreadsheets().values().append(
            spreadsheetId=st.secrets["GSHEET_LOG_ID"],
            valueInputOption='RAW',
            range='rawdata!A2',
            body=dict(majorDimension='ROWS', values=values_to_append)
        ).execute()
        print('บันทึก Log ไปยัง Google Sheet สำเร็จ')
        return True
    except Exception as e:
        st.error(f"Google Sheets Error: ไม่สามารถบันทึก Log ได้:\n{e}")
        return False

def upload_to_aws(records, data_list):
    """อัปโหลดข้อมูลไปยัง AWS RDS"""
    try:
        # ใช้ข้อมูลจาก st.secrets เพื่อเชื่อมต่อฐานข้อมูล
        mydb = mysql.connector.connect(
            host=st.secrets["DB_HOST"],
            port=st.secrets["DB_PORT"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"],
            database=st.secrets["DB_NAME"]
        )
        mycursor = mydb.cursor()
        placeholders = ", ".join(["%s"] * len(data_list))
        update_clause = ", ".join([f"{col} = VALUES({col})" for col in data_list if col != 'job_number'])
        table_name = st.secrets["DB_TABLE"]
        sql = f"INSERT INTO {table_name} ({', '.join(data_list)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
        for row in records:
            mycursor.execute(sql, tuple(row))
        mydb.commit()
        print('อัปโหลดข้อมูลไปยัง AWS สำเร็จ ✅')
        return True
    except Exception as e:
        st.error(f"AWS Error: ไม่สามารถอัปโหลดข้อมูลไปยัง AWS ได้:\n{e}")
        return False
    finally:
        if 'mydb' in locals() and mydb.is_connected():
            mycursor.close()
            mydb.close()

def process_cost_sheet_file(file_path):
    """ประมวลผลไฟล์ Excel ที่อัปโหลด (เหมือนเดิม แต่เปลี่ยน messagebox เป็น st.error)"""
    try:
        excel_data = pd.read_excel(file_path, sheet_name='Estimated Cost', usecols="A:N", header=None)
        excel_data.columns = col_name

        project_no = str(excel_data.iloc[1, 1])
        project_name = str(excel_data.iloc[2, 1])

        excel_data['job_number'] = excel_data['job_number'].astype(str)

        check_version = str(excel_data.iloc[7, 0])
        data_set = []
        display_data = []
        list_col = None

        if check_version == 'Costsheet Version 2':
            version_info = "Version 2"
            list_col = list_col_v2
            for i in list_q_v1:
                set_index = excel_data.index[excel_data['job_number'] == i].tolist()
                if set_index:
                    idx = set_index[0]
                    item_name = excel_data.iloc[idx, 1]
                    find_q1 = excel_data.iloc[idx, 5]; find_q2 = excel_data.iloc[idx, 10]
                    find_q3 = excel_data.iloc[idx, 11]; find_q4 = excel_data.iloc[idx, 8]
                    data_set.append([find_q1, find_q2, find_q3, find_q4])
                    display_data.append([i, item_name, find_q1, find_q2, find_q3, find_q4])
        else:
            version_info = "Version 1"
            list_col = list_col_v1
            for i in list_q_v1:
                set_index = excel_data.index[excel_data['job_number'] == i].tolist()
                if set_index:
                    idx = set_index[0]
                    item_name = excel_data.iloc[idx, 1]
                    find_q1 = excel_data.iloc[idx, 2]; find_q2 = excel_data.iloc[idx, 6]
                    find_q3 = excel_data.iloc[idx, 7]; find_q4 = excel_data.iloc[idx, 4]
                    data_set.append([find_q1, find_q2, find_q3, find_q4])
                    display_data.append([i, item_name, find_q1, find_q2, find_q3, find_q4])

        if not data_set:
            raise ValueError("ไม่พบข้อมูลที่ตรงกับ list ที่กำหนดในไฟล์")

        df_x = pd.DataFrame(data_set, columns=['thai_fild', 'outsource_convert', 'outsource_thb', 'MHC']).fillna(0)
        df_x['total_cost'] = df_x['thai_fild'] + df_x['outsource_convert'] + df_x['outsource_thb']

        master_data = [project_no] + list(df_x['total_cost'].to_numpy()) + list(df_x['MHC'].to_numpy())

        df_for_upload = pd.DataFrame([master_data])
        df_for_upload.columns = list_col
        df_for_upload['pc_name_input'] = version_info
        df_for_upload['datetime_stamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_for_upload = df_for_upload.astype(str)

        display_columns = ['Item No', 'Item list', 'External Expense', 'Convert THB', 'THB', 'Manhour cost']
        df_for_display = pd.DataFrame(display_data, columns=display_columns).fillna(0)

        return df_for_upload, df_for_display, version_info, project_no, project_name

    except Exception as e:
        st.error(f"เกิดข้อผิดพลาดในการประมวลผลไฟล์: {e}")
        return None, None, None, None, None

# ==============================================================================
# ส่วนที่ 2: ส่วนหน้าเว็บ (UI) ด้วย Streamlit
# ==============================================================================

# --- ตั้งค่าหน้าเว็บ ---
st.set_page_config(page_title="Cost Sheet Uploader", layout="wide", page_icon="🚀")

# --- ส่วนหัวเรื่อง ---
st.title("🚀 Cost Sheet Auto Uploader")
st.markdown("เว็บแอปสำหรับอัปโหลดข้อมูลจากไฟล์ Cost Sheet ไปยังฐานข้อมูล AWS และบันทึก Log ไปยัง Google Sheets")

# --- จัดการ State ของแอป ---
# ใช้ st.session_state เพื่อเก็บข้อมูลระหว่างการทำงาน
if 'processing_done' not in st.session_state:
    st.session_state.processing_done = False
    st.session_state.df_to_upload = None
    st.session_state.df_to_display = None
    st.session_state.version_info = ""
    st.session_state.project_no = ""
    st.session_state.project_name = ""
    st.session_state.original_filename = ""

# --- ส่วนที่ 1: การอัปโหลดและประมวลผลไฟล์ ---
st.header("ขั้นตอนที่ 1: เลือกและประมวลผลไฟล์")

uploaded_file = st.file_uploader(
    "เลือกไฟล์ Cost Sheet (ไฟล์ .xlsx หรือ .xls เท่านั้น)",
    type=["xlsx", "xls"],
    help="ลากและวางไฟล์ที่นี่ หรือกดปุ่มเพื่อเลือกไฟล์"
)

if uploaded_file is not None:
    # สร้างโฟลเดอร์ชั่วคราวถ้ายังไม่มี
    temp_dir = "temp_files"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
        
    file_path = os.path.join(temp_dir, uploaded_file.name)

    # เขียนไฟล์ที่อัปโหลดลงดิสก์ชั่วคราวเพื่อให้ฟังก์ชันเดิมทำงานได้
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    # ประมวลผลไฟล์
    with st.spinner(f"กำลังประมวลผลไฟล์ '{uploaded_file.name}'..."):
        df_upload, df_display, version, proj_no, proj_name = process_cost_sheet_file(file_path)

    if df_upload is not None:
        st.success(f"ประมวลผลไฟล์ '{uploaded_file.name}' สำเร็จ!")
        # เก็บผลลัพธ์ไว้ใน session_state
        st.session_state.processing_done = True
        st.session_state.df_to_upload = df_upload
        st.session_state.df_to_display = df_display
        st.session_state.version_info = version
        st.session_state.project_no = proj_no
        st.session_state.project_name = proj_name
        st.session_state.original_filename = uploaded_file.name
    else:
        # หากประมวลผลไม่สำเร็จ ให้รีเซ็ต State
        st.session_state.processing_done = False

# --- ส่วนที่ 2: แสดงผลและยืนยันการอัปโหลด ---
# ส่วนนี้จะแสดงขึ้นมาก็ต่อเมื่อประมวลผลไฟล์สำเร็จแล้วเท่านั้น
if st.session_state.processing_done:
    st.header("ขั้นตอนที่ 2: ตรวจสอบและยืนยันข้อมูล")

    # แสดงข้อมูลโปรเจกต์
    with st.container(border=True):
        st.info(f"**Cost Sheet Version:** `{st.session_state.version_info}`")
        st.metric(label="Project Number", value=st.session_state.project_no)
        st.markdown(f"**Project Name:** {st.session_state.project_name}")
        st.warning("⚠️ **สำคัญ:** กรุณาตรวจสอบความถูกต้องของ Project Number และข้อมูลทั้งหมดก่อนกดยืนยัน")

    # แสดงตารางข้อมูล
    st.subheader("ตารางข้อมูลสำหรับตรวจสอบ")
    st.dataframe(st.session_state.df_to_display, use_container_width=True)

    st.header("ขั้นตอนที่ 3: อัปโหลดข้อมูล")

    # ปุ่มยืนยันการอัปโหลด
    if st.button("✅ ยืนยันและอัปโหลดข้อมูล", type="primary", use_container_width=True):
        upload_success = False
        log_success = False
        
        # 1. อัปโหลดไปยัง AWS
        with st.spinner("กำลังอัปโหลดข้อมูลไปยัง AWS..."):
            records = st.session_state.df_to_upload[upload_data_columns].to_numpy()
            upload_success = upload_to_aws(records, upload_data_columns)

        if upload_success:
            st.success("อัปโหลดข้อมูลไปยัง AWS สำเร็จ!")

            # 2. บันทึก Log ไปยัง Google Sheets
            with st.spinner("กำลังบันทึก Log ไปยัง Google Sheets..."):
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_entry = [[f"{st.session_state.original_filename}|success|{now_str}"]]
                log_success = post_log_to_gsheet(log_entry)

            if log_success:
                st.success("บันทึก Log สำเร็จ!")
                st.balloons()
            else:
                st.error("ไม่สามารถบันทึก Log ไปยัง Google Sheets ได้")
        else:
            st.error("การอัปโหลดข้อมูลไปยัง AWS ล้มเหลว กรุณาตรวจสอบข้อความ Error ด้านบน")
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = [[f"{st.session_state.original_filename}|aws_upload_failed|{now_str}"]]
            post_log_to_gsheet(log_entry) # พยายามบันทึก Log ว่าล้มเหลว
        
        # รีเซ็ต State เพื่อรอการอัปโหลดครั้งต่อไป
        st.session_state.processing_done = False
        st.session_state.df_to_upload = None
        st.info("หน้านี้จะรีเฟรชเพื่อรอการทำงานครั้งต่อไป กรุณาเลือกไฟล์ใหม่หากต้องการ")