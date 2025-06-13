import sys
import os
import sqlite3
import datetime
import secrets
import json
import threading
import socket
from io import BytesIO
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QGridLayout, 
                             QPushButton, QLabel, QVBoxLayout, QFrame,
                             QStackedWidget, QMessageBox, QTableView, QHeaderView,
                             QLineEdit, QDateEdit, QComboBox, QHBoxLayout,
                             QFormLayout, QDialog, QDialogButtonBox, QTextEdit,
                             QListWidget, QInputDialog, QFileDialog, QListWidgetItem,
                             QTextBrowser, QGroupBox)
from PyQt5.QtCore import Qt, QSize, pyqtSignal, QAbstractTableModel, QDate, QTime, QDateTime, QThread, QRectF, QSizeF
from PyQt5.QtGui import QIcon, QFont, QImage, QPixmap, QPainter, QColor
from PyQt5.QtPrintSupport import QPrinter, QPrintDialog

# --- Third-party library dependency check (will be handled in main) ---
try:
    import adif_io
    import serial
    import serial.tools.list_ports
    import nfc
    from nfc.clf import RemoteTarget
    import ndef
    import qrcode
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import mm
    from reportlab.lib.utils import ImageReader
    from reportlab.graphics.barcode import code128
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics import renderPDF
    from PIL import Image
    from PIL.ImageQt import ImageQt
    import fitz # PyMuPDF
except ImportError:
    pass

# --- Constants ---
DB_FILE = "database/qsl_manager.db"
LOGBOOK_FILE = "logbook.adi"
STYLE_SHEET_FILE = "assets/style.qss"
CONFIG_FILE = "config.json"
LABELS_DIR = "labels"
MODES_LIST = ["", "AM", "ARDOP", "ATV", "C4FM", "CHIP", "CLO", "CW", "DIGITALVOICE", "DOMINO", "DSTAR", "FAX", "FM", "FSK441", "FT8", "FT4", "HELL", "JT4", "JT6M", "JT9", "JT44", "JT65", "MFSK", "MSK144", "MT63", "OLIVIA", "OPERA", "PACKET", "PAX", "PSK", "PSK2K", "Q15", "QRA64", "ROS", "RTTY", "RTTYM", "SSB", "SSTV", "THOR", "THRB", "V4", "V5", "VOI", "WINMOR", "WSPR", "AMSS", "ASCI", "PCW", "EYEBALL"]


# --- Configuration Manager ---
class ConfigManager:
    @staticmethod
    def load_config():
        default_config = {"primary_callsign": "", "nfc_port": ""}
        if not os.path.exists(CONFIG_FILE): return default_config
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if 'nfc_port' not in config: config['nfc_port'] = ""
                return config
        except (IOError, json.JSONDecodeError): return default_config

    @staticmethod
    def save_config(config_data):
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f: json.dump(config_data, f, indent=4)
        except IOError as e: print(f"Error saving config: {e}")

    @staticmethod
    def get_config(key, default=""):
        return ConfigManager.load_config().get(key, default)

    @staticmethod
    def set_config(key, value):
        config = ConfigManager.load_config()
        config[key] = value
        ConfigManager.save_config(config)

# --- ADIF Handler ---
class ADIF_Handler:
    @staticmethod
    def qso_to_adif_record(qso_data: dict) -> str:
        adif_str = ""
        mapping = { 'station_callsign': 'CALL', 'qso_date': 'QSO_DATE', 'time_on': 'TIME_ON', 'band': 'BAND', 'band_rx': 'BAND_RX', 'mode': 'MODE', 'submode': 'SUBMODE', 'rst_sent': 'RST_SENT', 'rst_rcvd': 'RST_RCVD', 'freq': 'FREQ', 'freq_rx': 'FREQ_RX', 'my_callsign': 'OPERATOR', 'comment': 'COMMENT', 'qsl_sent': 'QSL_SENT', 'qsl_rcvd': 'QSL_RCVD', 'sat_name': 'SAT_NAME', 'prop_mode': 'PROP_MODE' }
        for key, adif_tag in mapping.items():
            value = qso_data.get(key)
            if value: adif_str += f"<{adif_tag}:{len(str(value))}>{value} "
        adif_str += "<EOR>\n\n"
        return adif_str

    @staticmethod
    def append_to_logbook(adif_record: str):
        try:
            with open(LOGBOOK_FILE, "a", encoding="utf-8") as f: f.write(adif_record)
        except IOError as e: print(f"Error writing to logbook file: {e}")

# --- Hardware Components ---
class NFCWriter(QThread):
    status_update = pyqtSignal(str)
    write_finished = pyqtSignal(bool, str) # success, message

    def __init__(self, port, data, parent=None):
        super().__init__(parent)
        self.port = port
        self.data_to_write = data
        self._is_running = False

    def run(self):
        self._is_running = True
        try:
            self.status_update.emit("正在连接NFC读写器...")
            with nfc.ContactlessFrontend(self.port) as clf:
                self.status_update.emit("连接成功，请放置NFC卡片...")
                target = clf.sense(RemoteTarget('106A'), RemoteTarget('106B'), RemoteTarget('212F'))
                while target is None and self._is_running:
                    target = clf.sense(RemoteTarget('106A'), RemoteTarget('106B'), RemoteTarget('212F'))
                
                if not self._is_running:
                    self.write_finished.emit(False, "操作已取消。")
                    return

                self.status_update.emit("发现卡片，正在写入...")
                tag = nfc.tag.activate(clf, target)
                if tag.ndef:
                    tag.ndef.records = [ndef.TextRecord(self.data_to_write)]
                    self.write_finished.emit(True, "NFC 标签写入成功！")
                else:
                    self.write_finished.emit(False, "错误：该卡片不支持NDEF格式。")

        except IOError as e:
            self.write_finished.emit(False, f"连接读写器失败: {e}")
        except Exception as e:
            self.write_finished.emit(False, f"写入失败: {e}")

    def stop(self):
        self._is_running = False

class NFCScanner(QThread):
    tag_read = pyqtSignal(str)
    status_update = pyqtSignal(str)

    def __init__(self, port, parent=None):
        super().__init__(parent)
        self.port = port
        self._is_running = False

    def run(self):
        self._is_running = True
        try:
            with nfc.ContactlessFrontend(self.port) as clf:
                self.status_update.emit("NFC 扫描已激活，请放置卡片。")
                while self._is_running:
                    target = clf.sense(RemoteTarget('106A'), RemoteTarget('106B'), RemoteTarget('212F'), interval=0.5)
                    if target:
                        try:
                            tag = nfc.tag.activate(clf, target)
                            if tag.ndef and tag.ndef.records:
                                record = ndef.message_decoder(tag.ndef.data)
                                if isinstance(record[0], ndef.TextRecord):
                                    self.tag_read.emit(record[0].text)
                        except Exception as e:
                            print(f"Error reading NFC tag: {e}")
                    QThread.msleep(100) 
        except IOError:
            self.status_update.emit(f"无法连接到NFC设备: {self.port}")
        except Exception as e:
            self.status_update.emit(f"NFC扫描出错: {e}")

    def stop(self):
        self._is_running = False

class LabelPrinter:
    @staticmethod
    def generate_label(qsl_id, log_data_list, parent_widget, output_mode="print"):
        os.makedirs(LABELS_DIR, exist_ok=True)
        pdf_buffer = BytesIO()
        try:
            # Create PDF in memory
            page_width, page_height = 40 * mm, 25 * mm
            c = canvas.Canvas(pdf_buffer, pagesize=(page_width, page_height))
            margin = 2 * mm

            # --- Page 1 ---
            c.setFont("Helvetica", 7)
            log_dict = dict(log_data_list[0])
            c.drawString(margin, 22*mm, f"To: {log_dict.get('station_callsign', '')}")
            c.drawString(margin, 19*mm, f"Fm: {log_dict.get('my_callsign', '')}")
            textobject = c.beginText(margin, 16*mm)
            textobject.setFont("Helvetica", 6)
            textobject.setLeading(7) # Line spacing
            
            if len(log_data_list) == 1:
                log = log_dict
                textobject.textLine(f"Date: {log.get('qso_date', '')} {log.get('time_on', '')}Z")
                textobject.textLine(f"Band: {log.get('band', '')} Mode: {log.get('mode', '')} RST: {log.get('rst_rcvd', '')}")
                if log.get('sat_name'):
                    textobject.textLine(f"SAT: {log.get('sat_name')}")
            else:
                textobject.textLine(f"Multiple QSOs ({len(log_data_list)}):")
                for i, log_row in enumerate(log_data_list[:3]):
                    log = dict(log_row)
                    textobject.textLine(f"-{log.get('qso_date','')} {log.get('band','')} {log.get('mode','')}")
                if len(log_data_list) > 3: textobject.textLine("...")
            c.drawText(textobject)
            
            qr_img = qrcode.make(qsl_id)
            qr_buffer = BytesIO()
            qr_img.save(qr_buffer, "PNG")
            qr_buffer.seek(0)
            c.drawImage(ImageReader(qr_buffer), 15*mm, 2*mm, width=10*mm, height=10*mm, preserveAspectRatio=True)
            
            c.showPage()

            # --- Page 2 ---
            c.setFont("Helvetica-Bold", 7)
            c.drawCentredString(page_width / 2, 22*mm, qsl_id)
            
            # QR Code (Center)
            qr_buffer.seek(0)
            qr_size = 12*mm # Slightly larger QR code
            c.drawImage(ImageReader(qr_buffer), (page_width - qr_size)/2, (page_height - qr_size)/2, width=qr_size, height=qr_size, preserveAspectRatio=True)

            c.save()

            # --- Output based on mode ---
            if output_mode == "print":
                pdf_path = os.path.join(LABELS_DIR, f"{qsl_id}.pdf")
                with open(pdf_path, "wb") as f:
                    f.write(pdf_buffer.getvalue())

                if sys.platform == "win32":
                    os.startfile(pdf_path, "print")
                else:
                    os.system(f"lp {pdf_path}")
                QMessageBox.information(parent_widget, "打印任务", f"已生成标签 {pdf_path} 并发送到打印机。")

            elif output_mode == "png":
                output_dir = os.path.join(LABELS_DIR, qsl_id)
                os.makedirs(output_dir, exist_ok=True)
                pdf_buffer.seek(0)
                
                doc = fitz.open("pdf", pdf_buffer.read())
                zoom_matrix = fitz.Matrix(10, 10)
                
                if len(doc) > 0:
                    page1 = doc.load_page(0)
                    pix1 = page1.get_pixmap(matrix=zoom_matrix)
                    pix1.save(os.path.join(output_dir, "QSL.png"))

                if len(doc) > 1:
                    page2 = doc.load_page(1)
                    pix2 = page2.get_pixmap(matrix=zoom_matrix)
                    pix2.save(os.path.join(output_dir, "Letter.png"))
                    
                doc.close()
                QMessageBox.information(parent_widget, "导出成功", f"标签已导出为PNG图片，保存在:\n{output_dir}")

        except Exception as e:
            QMessageBox.critical(None, "操作失败", f"生成或输出标签时出错: {e}")
            
# --- Dialogs ---
class OutputModeDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("选择输出模式")
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setFixedSize(350, 120)
        self.mode = None
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("请选择标签的输出方式："))
        
        self.print_button = QPushButton("直接打印")
        self.png_button = QPushButton("导出为PNG图片")
        
        self.print_button.clicked.connect(self.select_print)
        self.png_button.clicked.connect(self.select_png)
        
        layout.addWidget(self.print_button)
        layout.addWidget(self.png_button)

    def select_print(self):
        self.mode = "print"
        self.accept()

    def select_png(self):
        self.mode = "png"
        self.accept()

class BatchQslModeDialog(QDialog):
    def __init__(self, log_count, parent=None):
        super().__init__(parent)
        self.setWindowTitle("选择卡片模式")
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setFixedSize(700, 240)
        self.mode = None
        layout = QVBoxLayout(self); layout.addWidget(QLabel(f"您选择了 {log_count} 条日志，请选择处理模式："))
        self.multi_card_button = QPushButton("多卡模式 (为每条日志生成独立卡号)")
        self.single_card_button = QPushButton("单卡模式 (为所有日志生成一个卡号)")
        self.multi_card_button.clicked.connect(self.select_multi_card); self.single_card_button.clicked.connect(self.select_single_card)
        layout.addWidget(self.multi_card_button); layout.addWidget(self.single_card_button)
    def select_multi_card(self): self.mode = "multi"; self.accept()
    def select_single_card(self): self.mode = "single"; self.accept()

class CardActionDialog(QDialog):
    def __init__(self, title, cards, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setFixedSize(700, 240)
        self.selected_card_info = None
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("请选择要操作的卡片："))

        rc_card = next((c for c in cards if c['direction'] == 'RC'), None)
        tc_card = next((c for c in cards if c['direction'] == 'TC'), None)

        self.rc_button = QPushButton(f"收卡 (RC): {rc_card['qsl_id'] if rc_card else '无'}")
        self.rc_button.setEnabled(rc_card is not None)
        self.rc_button.clicked.connect(lambda: self.select_card(rc_card))
        
        self.tc_button = QPushButton(f"发卡 (TC): {tc_card['qsl_id'] if tc_card else '无'}")
        self.tc_button.setEnabled(tc_card is not None)
        self.tc_button.clicked.connect(lambda: self.select_card(tc_card))

        layout.addWidget(self.rc_button); layout.addWidget(self.tc_button)

    def select_card(self, card_info):
        self.selected_card_info = card_info
        self.accept()

class SettingsDialog(QDialog):
    data_changed = pyqtSignal()
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager; self.setWindowTitle("设置")
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setFixedSize(1000, 1000)
        main_layout = QVBoxLayout(self)
        callsign_frame = QFrame(); callsign_frame.setFrameShape(QFrame.StyledPanel); layout = QVBoxLayout(callsign_frame)
        self.callsign_list = QListWidget(); layout.addWidget(QLabel("我的呼号管理:")); layout.addWidget(self.callsign_list)
        btn_layout = QHBoxLayout(); self.add_btn = QPushButton("添加新呼号"); self.del_btn = QPushButton("删除选中呼号"); self.set_primary_btn = QPushButton("设为主要呼号")
        btn_layout.addWidget(self.add_btn); btn_layout.addWidget(self.del_btn); btn_layout.addWidget(self.set_primary_btn); layout.addLayout(btn_layout)
        
        danger_zone = QGroupBox("危险区域"); danger_layout = QVBoxLayout(danger_zone)
        self.reset_data_btn = QPushButton("重置全部卡片数据"); self.reset_data_btn.setStyleSheet("background-color: #c0392b;")
        danger_layout.addWidget(self.reset_data_btn)

        main_layout.addWidget(callsign_frame); main_layout.addWidget(danger_zone); main_layout.addStretch()
        self.close_button = QDialogButtonBox(QDialogButtonBox.Close); main_layout.addWidget(self.close_button)
        self.add_btn.clicked.connect(self.add_callsign); self.del_btn.clicked.connect(self.delete_callsign)
        self.set_primary_btn.clicked.connect(self.set_primary)
        self.reset_data_btn.clicked.connect(self.handle_reset_data)
        self.close_button.rejected.connect(self.reject); self.load_settings()
        
    def load_settings(self):
        self.callsign_list.clear(); primary_callsign = ConfigManager.get_config("primary_callsign"); callsigns = self.db_manager.get_all_my_callsigns()
        for callsign in callsigns:
            self.callsign_list.addItem(callsign)
            if callsign == primary_callsign: item = self.callsign_list.findItems(callsign, Qt.MatchExactly)[0]; item.setFont(QFont("Arial", 12, QFont.Bold)); item.setForeground(Qt.yellow)
        
    def handle_reset_data(self):
        reply = QMessageBox.question(self, "危险操作确认", "您确定要重置所有QSL卡片数据吗？\n\n此操作将 **永久删除** 所有已生成的卡号和出入库记录，并将所有日志状态重置为未收/发。\n\n**此操作不可恢复！**", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No: return
            
        password, ok = QInputDialog.getText(self, "密码确认", "请输入密码 'admin' 以确认操作:", QLineEdit.Password)
        if ok and password == "admin":
            if self.db_manager.reset_all_qsl_data():
                QMessageBox.information(self, "操作成功", "所有QSL卡片数据已重置。")
                self.data_changed.emit()
            else:
                QMessageBox.critical(self, "操作失败", "重置数据时发生错误。")
        elif ok:
            QMessageBox.warning(self, "密码错误", "密码不正确，操作已取消。")
            
    def add_callsign(self):
        text, ok = QInputDialog.getText(self, "添加呼号", "请输入您的呼号:")
        if ok and text:
            callsign = text.upper().strip()
            if self.db_manager.add_callsign(callsign): self.load_settings()
            else: QMessageBox.warning(self, "错误", f"无法添加呼号 '{callsign}'。可能已存在。")
    def delete_callsign(self):
        current_item = self.callsign_list.currentItem();
        if not current_item: QMessageBox.warning(self, "操作提示", "请先选择一个要删除的呼号。"); return
        callsign = current_item.text()
        reply = QMessageBox.question(self, "确认删除", f"您确定要删除呼号 '{callsign}' 吗？", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes and self.db_manager.delete_callsign(callsign):
            if callsign == ConfigManager.get_config("primary_callsign"): ConfigManager.set_config("primary_callsign", "")
            self.load_settings()
    def set_primary(self):
        current_item = self.callsign_list.currentItem()
        if not current_item: QMessageBox.warning(self, "操作提示", "请先选择一个要设为主用的呼号。"); return
        callsign = current_item.text(); ConfigManager.set_config("primary_callsign", callsign); self.load_settings(); QMessageBox.information(self, "成功", f"'{callsign}' 已被设为您的主要呼号。")
        
# --- Log Detail/Edit Dialog ---
class LogDetailDialog(QDialog):
    def __init__(self, db_manager, my_callsign, log_id=None, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager; self.log_id = log_id; self.my_callsign = my_callsign
        self.is_edit_mode = self.log_id is not None
        title = "编辑通联日志" if self.is_edit_mode else f"添加新通联日志 - [{self.my_callsign}]"
        self.setWindowTitle(title)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowContextHelpButtonHint)
        self.setFixedSize(1000, 1200)
        layout = QVBoxLayout(self)
        self.qso_type_combo = QComboBox(); self.qso_type_combo.addItems(["基础 (HF/VHF/UHF)", "卫星 (Satellite)", "中继 (Repeater)", "Eyeball"])
        self.qso_type_combo.currentIndexChanged.connect(self.update_form_layout)
        type_layout = QFormLayout(); type_layout.addRow("通联类型:", self.qso_type_combo); layout.addLayout(type_layout)
        self.form_layout = QFormLayout()
        self.callsign_input = QLineEdit(); self.qso_date_input = QDateEdit(); self.qso_date_input.setDisplayFormat("yyyy-MM-dd")
        self.time_on_input = QLineEdit(); self.band_input = QComboBox(); self.band_input.addItems(["", "160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m", "4m", "2m", "1.25m", "70cm", "33cm", "23cm", "13cm", "9cm", "6cm", "3cm", "1.25cm", "N/A"])
        self.band_rx_input = QComboBox(); self.band_rx_input.addItems(["", "160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m", "4m", "2m", "1.25m", "70cm", "33cm", "23cm", "13cm", "9cm", "6cm", "3cm", "1.25cm", "N/A"])
        self.freq_input = QLineEdit(); self.freq_rx_input = QLineEdit();
        self.mode_input = QComboBox(); self.mode_input.addItems(MODES_LIST)
        self.rst_sent_input = QLineEdit(); self.rst_rcvd_input = QLineEdit(); self.comment_input = QTextEdit()
        self.form_layout.addRow("对方呼号:", self.callsign_input); self.form_layout.addRow("日期 (UTC):", self.qso_date_input)
        self.form_layout.addRow("时间 (UTC):", self.time_on_input); self.form_layout.addRow("发射波段:", self.band_input)
        self.form_layout.addRow("接收波段:", self.band_rx_input); self.form_layout.addRow("发射频率 (MHz):", self.freq_input)
        self.form_layout.addRow("接收频率 (MHz):", self.freq_rx_input); self.form_layout.addRow("模式:", self.mode_input)
        self.form_layout.addRow("发送信号报告:", self.rst_sent_input); self.form_layout.addRow("接收信号报告:", self.rst_rcvd_input)
        self.satellite_frame = QFrame(); self.repeater_frame = QFrame(); self.eyeball_frame = QFrame()
        self.setup_dynamic_sections()
        self.form_layout.addRow(self.satellite_frame); self.form_layout.addRow(self.repeater_frame); self.form_layout.addRow(self.eyeball_frame)
        self.form_layout.addRow("备注:", self.comment_input); layout.addLayout(self.form_layout)
        if self.is_edit_mode:
            qsl_frame = QFrame(); qsl_frame.setFrameShape(QFrame.StyledPanel); qsl_layout = QFormLayout(qsl_frame)
            self.rc_card_label = QLineEdit("N/A"); self.rc_card_label.setReadOnly(True); self.tc_card_label = QLineEdit("N/A"); self.tc_card_label.setReadOnly(True)
            qsl_layout.addRow("收卡 (RC) 编号:", self.rc_card_label); qsl_layout.addRow("发卡 (TC) 编号:", self.tc_card_label); layout.addWidget(qsl_frame)
        self.buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept); self.buttons.rejected.connect(self.reject); layout.addWidget(self.buttons); self.populate_data()
    def setup_dynamic_sections(self):
        sat_layout = QFormLayout(self.satellite_frame); self.sat_name_input = QLineEdit(); self.prop_mode_input = QLineEdit()
        sat_layout.addRow("卫星名称:", self.sat_name_input); sat_layout.addRow("传播模式:", self.prop_mode_input)
        rep_layout = QFormLayout(self.repeater_frame); self.repeater_call_input = QLineEdit()
        rep_layout.addRow("中继呼号:", self.repeater_call_input)
        eye_layout = QFormLayout(self.eyeball_frame)
        self.eyeball_type_input = QComboBox(); self.eyeball_type_input.addItems(["线下聚会", "线上EYEBALL", "俱乐部活动", "其他"])
        eye_layout.addRow("Eyeball 类型:", self.eyeball_type_input)
    def update_form_layout(self):
        qso_type = self.qso_type_combo.currentText(); is_sat = "卫星" in qso_type; is_rep = "中继" in qso_type; is_eye = "Eyeball" in qso_type
        self.satellite_frame.setVisible(is_sat); self.repeater_frame.setVisible(is_rep); self.eyeball_frame.setVisible(is_eye)
        if is_eye:
            self.mode_input.setCurrentText("EYEBALL"); self.band_input.setCurrentText("N/A"); self.band_rx_input.setCurrentText("N/A")
            self.rst_sent_input.setText("59+"); self.rst_rcvd_input.setText("59+")
        else:
            if self.mode_input.currentText() == "EYEBALL": self.mode_input.setCurrentIndex(0)
    def populate_data(self):
        if not self.is_edit_mode:
            utc_now = QDateTime.currentDateTimeUtc()
            self.qso_date_input.setDateTime(utc_now)
            self.time_on_input.setText(utc_now.time().toString("hhmm"))
            self.rst_sent_input.setText("59"); self.rst_rcvd_input.setText("59"); self.update_form_layout(); return
        log_data = self.db_manager.get_log_details(self.log_id)
        if not log_data: QMessageBox.critical(self, "错误", "无法加载日志详情。"); self.reject(); return
        qso_type_to_set = "基础 (HF/VHF/UHF)"
        if log_data['sat_name']: 
            qso_type_to_set = "卫星 (Satellite)"
        elif log_data['mode'] == 'EYEBALL': 
            qso_type_to_set = "Eyeball"
        elif log_data['freq_rx'] and (log_data['mode'] or "").upper() == 'FM':
            qso_type_to_set = "中继 (Repeater)"
        self.qso_type_combo.blockSignals(True); self.qso_type_combo.setCurrentText(qso_type_to_set); self.qso_type_combo.blockSignals(False)
        self.callsign_input.setText(log_data['station_callsign'] or '')
        try:
            qso_date_str = log_data['qso_date']
            if qso_date_str: self.qso_date_input.setDate(QDate.fromString(qso_date_str, "yyyyMMdd"))
            else: self.qso_date_input.setDate(QDate.currentDate())
        except Exception: self.qso_date_input.setDate(QDate.currentDate())
        self.time_on_input.setText(log_data['time_on'] or ''); self.band_input.setCurrentText((log_data['band'] or '').lower())
        self.band_rx_input.setCurrentText((log_data['band_rx'] or '').lower()); self.freq_input.setText(str(log_data['freq'] or ''))
        self.freq_rx_input.setText(str(log_data['freq_rx'] or '')); self.mode_input.setCurrentText(log_data['mode'] or '')
        self.rst_sent_input.setText(log_data['rst_sent'] or ''); self.rst_rcvd_input.setText(log_data['rst_rcvd'] or '')
        self.comment_input.setPlainText(log_data['comment'] or ''); self.sat_name_input.setText(log_data['sat_name'] or ''); self.prop_mode_input.setText(log_data['prop_mode'] or '')
        if log_data['submode']: self.eyeball_type_input.setCurrentText(log_data['submode'])
        qsl_cards = self.db_manager.get_qsl_cards_for_log(self.log_id)
        for card in qsl_cards:
            if card['direction'] == 'RC': self.rc_card_label.setText(card['qsl_id'])
            elif card['direction'] == 'TC': self.tc_card_label.setText(card['qsl_id'])
        self.update_form_layout()
    def get_data(self) -> dict:
        data = { "station_callsign": self.callsign_input.text().upper(), "qso_date": self.qso_date_input.date().toString("yyyyMMdd"), "time_on": self.time_on_input.text(), "band": self.band_input.currentText(), "band_rx": self.band_rx_input.currentText(), "freq": self.freq_input.text(), "freq_rx": self.freq_rx_input.text(), "mode": self.mode_input.currentText(), "rst_sent": self.rst_sent_input.text(), "rst_rcvd": self.rst_rcvd_input.text(), "comment": self.comment_input.toPlainText(), "my_callsign": self.my_callsign, "sat_name": None, "prop_mode": None, "submode": None }
        qso_type = self.qso_type_combo.currentText()
        if "卫星" in qso_type: data.update({"sat_name": self.sat_name_input.text(), "prop_mode": self.prop_mode_input.text()})
        elif "中继" in qso_type:
            repeater_call = self.repeater_call_input.text()
            if repeater_call: data["comment"] = f"RPT: {repeater_call} | {data['comment']}"
        elif "Eyeball" in qso_type: data["submode"] = self.eyeball_type_input.currentText()
        return data

# --- Log Table Model ---
class LogTableModel(QAbstractTableModel):
    def __init__(self, data, headers):
        super().__init__()
        self._data = data
        self._headers = headers
        self._checked_states = [Qt.Unchecked] * len(self._data)
        self._qsl_sent_col_idx = headers.index("已发?") + 1
        self._qsl_rcvd_col_idx = headers.index("已收?") + 1
        self._special_cols = [self._qsl_sent_col_idx, self._qsl_rcvd_col_idx]

    def data(self, index, role):
        if not index.isValid():
            return None
            
        column = index.column()
        row = index.row()

        if role == Qt.CheckStateRole and column == 0:
            return self._checked_states[row]
            
        value = self._data[row][column - 1] if column > 0 else None

        if role == Qt.DisplayRole:
            if column in self._special_cols:
                return '✔' if value == 'Y' else '✖'
            if column > 0:
                return str(value or "")

        if role == Qt.ForegroundRole:
            if column in self._special_cols:
                return QColor("green") if value == 'Y' else QColor("red")

        if role == Qt.TextAlignmentRole:
            if column in self._special_cols:
                return Qt.AlignCenter
        
        return None
    
    def setData(self, index, value, role):
        if role == Qt.CheckStateRole and index.column() == 0:
            self._checked_states[index.row()] = value; self.dataChanged.emit(index, index); return True
        return super().setData(index, value, role)
    def flags(self, index):
        flags = super().flags(index)
        if index.column() == 0: flags |= Qt.ItemIsUserCheckable
        return flags
    def rowCount(self, p): return len(self._data)
    def columnCount(self, p): return len(self._headers) + 1
    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            if section == 0: return ""
            else: return self._headers[section - 1]
        return None
    def update_data(self, new_data):
        self.beginResetModel(); self._data = new_data; self._checked_states = [Qt.Unchecked] * len(self._data); self.endResetModel()
    def get_checked_log_ids(self): return [str(self._data[i][0]) for i, state in enumerate(self._checked_states) if state == Qt.Checked]

# --- Log Management Widget ---
class LogManagementWidget(QWidget):
    back_to_dashboard_signal = pyqtSignal(); data_changed_signal = pyqtSignal()
    def __init__(self, db_manager): super().__init__(); self.db_manager = db_manager; self.init_ui(); self.load_initial_data()
    def init_ui(self):
        main_layout = QVBoxLayout(self); filter_box = QFrame(); filter_box.setObjectName("filterBox")
        filter_layout = QFormLayout(filter_box)
        self.my_callsign_filter = QLineEdit(); self.my_callsign_filter.setPlaceholderText("实时过滤我方呼号...")
        self.callsign_filter = QLineEdit(); self.callsign_filter.setPlaceholderText("实时过滤对方呼号...")
        self.qsl_id_filter = QLineEdit(); self.qsl_id_filter.setPlaceholderText("通过QSL卡号精确查找...")
        self.mode_filter = QComboBox(); self.mode_filter.addItems(["全部模式"] + [m for m in MODES_LIST if m])
        filter_layout.addRow("我方呼号:", self.my_callsign_filter); filter_layout.addRow("对方呼号:", self.callsign_filter)
        filter_layout.addRow("QSL卡号:", self.qsl_id_filter); filter_layout.addRow("通联模式:", self.mode_filter)
        button_layout = QHBoxLayout(); self.reset_button = QPushButton("重置所有条件"); self.reorder_button = QPushButton("按时间重排")
        button_layout.addStretch(); button_layout.addWidget(self.reorder_button); button_layout.addWidget(self.reset_button)
        filter_layout.addRow(button_layout); main_layout.addWidget(filter_box)
        self.table_view = QTableView(); self.table_view.setAlternatingRowColors(True); self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setSortingEnabled(False)
        self.table_view.horizontalHeader().setStretchLastSection(True); self.table_view.setEditTriggers(QTableView.NoEditTriggers)
        main_layout.addWidget(self.table_view)
        bottom_button_layout = QHBoxLayout()
        self.card_in_button = QPushButton("确认收卡 (RC)"); self.card_out_button = QPushButton("确认发卡 (TC)")
        self.reprint_button = QPushButton("补打标签"); self.check_duplicates_button = QPushButton("检查并合并重复项")
        self.recycle_card_button = QPushButton("回收卡号"); self.delete_log_button = QPushButton("删除日志")
        bottom_button_layout.addWidget(self.card_in_button); bottom_button_layout.addWidget(self.card_out_button); bottom_button_layout.addWidget(self.reprint_button)
        bottom_button_layout.addWidget(self.check_duplicates_button); bottom_button_layout.addWidget(self.recycle_card_button); bottom_button_layout.addWidget(self.delete_log_button)
        bottom_button_layout.addStretch(); self.back_button = QPushButton("返回主菜单"); bottom_button_layout.addWidget(self.back_button)
        main_layout.addLayout(bottom_button_layout)
        self.my_callsign_filter.textChanged.connect(self.apply_filters); self.callsign_filter.textChanged.connect(self.apply_filters)
        self.qsl_id_filter.returnPressed.connect(self.apply_filters)
        self.mode_filter.currentIndexChanged.connect(self.apply_filters); self.reset_button.clicked.connect(self.reset_filters)
        self.reorder_button.clicked.connect(self.reorder_logs)
        self.card_in_button.clicked.connect(lambda: self.process_qsl_cards('RC')); self.card_out_button.clicked.connect(lambda: self.process_qsl_cards('TC'))
        self.reprint_button.clicked.connect(self.reprint_label); self.check_duplicates_button.clicked.connect(self.check_for_duplicates)
        self.delete_log_button.clicked.connect(self.delete_selected_logs); self.recycle_card_button.clicked.connect(self.recycle_selected_card)
        self.table_view.doubleClicked.connect(self.edit_selected_log); self.back_button.clicked.connect(self.back_to_dashboard_signal.emit)
            
    def load_initial_data(self):
        self.headers = ["ID", "我方呼号", "对方呼号", "日期", "时间", "TX 波段", "RX 波段", "TX 频率", "RX 频率", "模式", "已发?", "已收?", "备注"]
        self.model = LogTableModel([], self.headers); self.table_view.setModel(self.model); self.table_view.setColumnWidth(0, 40); self.apply_filters()
    def apply_filters(self):
        logs = self.db_manager.search_logs(my_callsign=self.my_callsign_filter.text(), station_callsign=self.callsign_filter.text(), mode=self.mode_filter.currentText(), qsl_id=self.qsl_id_filter.text())
        self.model.update_data(logs)
    def reset_filters(self):
        self.my_callsign_filter.clear(); self.callsign_filter.clear(); self.qsl_id_filter.clear(); self.mode_filter.setCurrentIndex(0); self.apply_filters()
    def reorder_logs(self):
        reply = QMessageBox.question(self, "确认操作", "此操作将根据通联时间重新排列所有日志的序号，并刷新列表。\n您确定要继续吗？", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            if self.db_manager.reorder_logs_by_time():
                QMessageBox.information(self, "成功", "日志已按时间重新排序。")
                self.apply_filters()
                self.data_changed_signal.emit()
            else:
                QMessageBox.critical(self, "失败", "重新排序时发生数据库错误。")
                
    def process_qsl_cards(self, direction):
        log_ids_to_process = []
        skipped_count = 0
        
        for log_id in self.model.get_checked_log_ids():
            log_details = self.db_manager.get_log_details(log_id)
            if not log_details: continue

            if direction == 'TC' and log_details['qsl_sent'] == 'Y':
                skipped_count += 1
                continue
            if direction == 'RC' and log_details['qsl_rcvd'] == 'Y':
                skipped_count += 1
                continue
            log_ids_to_process.append(log_id)
        
        if not log_ids_to_process:
            QMessageBox.information(self, "操作提示", f"所有勾选的日志都已经有相应的卡片记录，已全部跳过。")
            return

        mode = "multi"
        if len(log_ids_to_process) > 1:
            dialog = BatchQslModeDialog(len(log_ids_to_process), self)
            if dialog.exec_() == QDialog.Accepted: mode = dialog.mode
            else: return

        output_dialog = OutputModeDialog(self)
        if output_dialog.exec_() != QDialog.Accepted: return
        output_mode = output_dialog.mode

        processed_count = 0
        if mode == "single":
            qsl_id = QSL_ID_Generator.generate(self.db_manager, direction)
            if self.db_manager.add_qsl_card(qsl_id, log_ids_to_process, direction):
                processed_count = len(log_ids_to_process)
                log_data_list = [self.db_manager.get_log_details(log_id) for log_id in log_ids_to_process]
                LabelPrinter.generate_label(qsl_id, log_data_list, self, output_mode)
        else:
            for log_id in log_ids_to_process:
                qsl_id = QSL_ID_Generator.generate(self.db_manager, direction)
                if self.db_manager.add_qsl_card(qsl_id, [log_id], direction):
                    processed_count += 1
                    log_data_list = [self.db_manager.get_log_details(log_id)]
                    LabelPrinter.generate_label(qsl_id, log_data_list, self, output_mode)
        
        if processed_count > 0:
            msg = f"成功为 {processed_count} 条日志生成卡片。"
            if skipped_count > 0:
                msg += f"\n跳过 {skipped_count} 条已有记录的日志。"
            QMessageBox.information(self, "操作成功", msg)
        
        self.apply_filters()
        self.data_changed_signal.emit()

    def reprint_label(self):
        log_ids = self.model.get_checked_log_ids()

        if not log_ids:
            QMessageBox.warning(self, "操作提示", "请勾选一个日志进行标签补打。")
            return
        if len(log_ids) > 1:
            QMessageBox.warning(self, "操作无效", "请一次只选择一个日志进行标签补打。")
            return

        log_id = log_ids[0]
        
        cards = self.db_manager.get_qsl_cards_for_log(log_id)
        if not cards:
            QMessageBox.information(self, "提示", "所选日志没有关联的QSL卡。")
            return
            
        dialog = CardActionDialog("选择要补打的标签", cards, self)
        if dialog.exec_() == QDialog.Accepted and dialog.selected_card_info:
            qsl_id_to_print = dialog.selected_card_info['qsl_id']
            
            logs_for_this_card = self.db_manager.get_logs_for_qsl_card(qsl_id_to_print)
            if not logs_for_this_card:
                QMessageBox.critical(self, "数据库错误", f"找不到卡号 {qsl_id_to_print} 关联的日志。")
                return

            log_data_list = [self.db_manager.get_log_details(lid['log_id']) for lid in logs_for_this_card]
            
            output_dialog = OutputModeDialog(self)
            if output_dialog.exec_() == QDialog.Accepted:
                LabelPrinter.generate_label(qsl_id_to_print, log_data_list, self, output_dialog.mode)
            
    def edit_selected_log(self, index):
        log_id = self.model.data(self.model.index(index.row(), 1), Qt.DisplayRole)
        my_callsign = self.model.data(self.model.index(index.row(), 2), Qt.DisplayRole)
        dialog = LogDetailDialog(self.db_manager, my_callsign, log_id, self)
        if dialog.exec_() == QDialog.Accepted:
            updated_data = dialog.get_data()
            if self.db_manager.update_log_entry(log_id, updated_data): QMessageBox.information(self, "成功", f"日志 (ID: {log_id}) 已更新。"); self.apply_filters()
            else: QMessageBox.critical(self, "错误", "无法更新日志。")
    def search_by_qsl_id(self, qsl_id):
        self.qsl_id_filter.setText(qsl_id); self.apply_filters()
    def check_for_duplicates(self):
        QMessageBox.information(self, "开始查重", "将开始扫描整个数据库查找并合并重复日志，这可能需要一些时间。")
        duplicate_sets = self.db_manager.find_all_duplicates()
        if not duplicate_sets:
            QMessageBox.information(self, "查重完成", "未发现重复的日志记录。")
            return
        
        merged_count = 0
        for duplicate_set in duplicate_sets:
            log_ids = sorted(list(duplicate_set))
            master_log_id = log_ids[0]
            master_log_data = dict(self.db_manager.get_log_details(master_log_id))

            logs_to_delete = []
            
            for i in range(1, len(log_ids)):
                duplicate_log_id = log_ids[i]
                duplicate_log_data_row = self.db_manager.get_log_details(duplicate_log_id)
                if not duplicate_log_data_row: continue
                duplicate_log_data = dict(duplicate_log_data_row)

                needs_update = False
                for key in duplicate_log_data.keys():
                    if key in ['id', 'adif_blob']: continue
                    new_value = duplicate_log_data.get(key)
                    if new_value and not master_log_data.get(key):
                        master_log_data[key] = new_value
                        needs_update = True
                
                new_comment = duplicate_log_data.get('comment', ''); old_comment = master_log_data.get('comment', '')
                if new_comment and new_comment not in (old_comment or ""):
                    master_log_data['comment'] = f"{old_comment or ''} | MERGED: {new_comment}".strip(" | ")
                    needs_update = True
                
                logs_to_delete.append(duplicate_log_id)

            if needs_update:
                self.db_manager.update_log_entry(master_log_id, master_log_data)
            
            for log_id_to_delete in logs_to_delete:
                self.db_manager.delete_log(log_id_to_delete)
            
            merged_count += 1

        QMessageBox.information(self, "合并完成", f"已自动合并 {merged_count} 组重复的日志记录。")
        self.apply_filters()
    def delete_selected_logs(self):
        log_ids = self.model.get_checked_log_ids()
        if not log_ids: QMessageBox.warning(self, "操作提示", "请先在表格中勾选要删除的日志。"); return
        
        reply = QMessageBox.question(self, "确认删除", f"您确定要永久删除选中的 {len(log_ids)} 条日志吗？\n此操作不可恢复！", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            deleted_count = 0
            for log_id in log_ids:
                if self.db_manager.delete_log(log_id): deleted_count += 1
            QMessageBox.information(self, "操作完成", f"成功删除 {deleted_count} 条日志。")
            self.apply_filters(); self.data_changed_signal.emit()
    def recycle_selected_card(self):
        log_ids = self.model.get_checked_log_ids()
        if len(log_ids) != 1: QMessageBox.warning(self, "操作提示", "请勾选一条且仅一条日志来回收其关联的QSL卡号。"); return
        
        log_id = log_ids[0]
        cards = self.db_manager.get_qsl_cards_for_log(log_id)
        if not cards: QMessageBox.information(self, "提示", "该日志没有关联的QSL卡，无需回收。"); return
            
        dialog = CardActionDialog("选择要回收的卡号", cards, self)
        if dialog.exec_() == QDialog.Accepted and dialog.selected_card_info:
            direction = dialog.selected_card_info['direction']
            if self.db_manager.recycle_qsl_card(log_id, direction):
                QMessageBox.information(self, "操作成功", f"成功回收该日志的 {direction} 卡号。")
                self.apply_filters(); self.data_changed_signal.emit()
            else: QMessageBox.warning(self, "操作失败", "无法回收卡号。")

# --- Hardware Control Widget ---
class HardwareWidget(QWidget):
    back_to_dashboard_signal = pyqtSignal()
    search_qsl_id_signal = pyqtSignal(str)
    
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.nfc_writer_thread = None
        self.nfc_scanner_thread = None
        self.init_ui()

    def init_ui(self):
        top_level_layout = QVBoxLayout(self)
        main_layout = QHBoxLayout()
        main_layout.setSpacing(20)

        # --- Left Pane ---
        left_pane = QWidget()
        left_layout = QVBoxLayout(left_pane)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        # Manual Input Group
        input_zone = QGroupBox("手动输入")
        input_layout = QVBoxLayout(input_zone)
        self.manual_input = QLineEdit(); self.manual_input.setPlaceholderText("输入QSL卡号后按回车查询...")
        self.manual_input.returnPressed.connect(self.search_manual_code)
        input_layout.addWidget(self.manual_input)

        # NFC Group
        nfc_frame = QGroupBox("NFC 读/写")
        nfc_layout = QVBoxLayout(nfc_frame)
        self.nfc_status_label = QLabel("NFC 空闲")
        self.nfc_status_label.setAlignment(Qt.AlignCenter)
        self.nfc_write_button = QPushButton("将输入框内容写入NFC")
        
        # NFC Port Settings within the group
        nfc_port_layout = QHBoxLayout()
        self.nfc_port_combo = QComboBox()
        self.refresh_nfc_btn = QPushButton("刷新")
        nfc_port_layout.addWidget(QLabel("端口:"))
        nfc_port_layout.addWidget(self.nfc_port_combo, 1)
        nfc_port_layout.addWidget(self.refresh_nfc_btn)
        self.save_nfc_btn = QPushButton("保存")
        nfc_port_layout.addWidget(self.save_nfc_btn)
        
        nfc_layout.addLayout(nfc_port_layout)
        nfc_layout.addWidget(self.nfc_status_label)
        nfc_layout.addWidget(self.nfc_write_button)
        
        left_layout.addWidget(input_zone)
        left_layout.addWidget(nfc_frame)
        left_layout.addStretch(1)

        # --- Right Pane ---
        right_pane = QGroupBox("查询结果")
        right_layout = QVBoxLayout(right_pane)
        self.results_browser = QTextBrowser()
        self.results_browser.setStyleSheet("font-size: 28px;") # Increased font size
        right_layout.addWidget(self.results_browser)

        # --- Main Layout Assembly ---
        main_layout.addWidget(left_pane, 1)
        main_layout.addWidget(right_pane, 2)
        
        # --- Bottom Bar for Back Button ---
        back_button_layout = QHBoxLayout();
        back_button_layout.addStretch()
        self.back_button = QPushButton("返回主菜单");
        back_button_layout.addWidget(self.back_button)
        
        top_level_layout.addLayout(main_layout)
        top_level_layout.addLayout(back_button_layout)
        
        # Connections
        self.nfc_write_button.clicked.connect(self.write_nfc)
        self.back_button.clicked.connect(self.leave_view)
        self.back_button.clicked.connect(self.back_to_dashboard_signal.emit)
        self.refresh_nfc_btn.clicked.connect(self.populate_nfc_ports)
        self.save_nfc_btn.clicked.connect(self.save_nfc_settings)
        
    def populate_nfc_ports(self):
        self.nfc_port_combo.clear()
        try:
            ports = serial.tools.list_ports.comports()
            if not ports:
                self.nfc_port_combo.addItem("未找到串口设备")
            else:
                for port in ports: self.nfc_port_combo.addItem(port.device)
        except Exception as e:
            self.nfc_port_combo.addItem("无法扫描串口")
            print(f"Error scanning for serial ports: {e}")

    def save_nfc_settings(self):
        port = self.nfc_port_combo.currentText()
        if port and "未找到" not in port and "无法" not in port:
            ConfigManager.set_config("nfc_port", port)
            QMessageBox.information(self, "成功", f"NFC端口已保存为 {port}。")
            self.start_nfc_scan() # Restart scan with new port
        else:
            QMessageBox.warning(self, "无效端口", "请选择一个有效的端口。")

    def search_manual_code(self):
        code = self.manual_input.text().strip()
        if code:
            self._perform_search(code)
            self.manual_input.clear()

    def _perform_search(self, qsl_id):
        logs_for_card = self.db_manager.get_logs_for_qsl_card(qsl_id)
        if not logs_for_card:
            self.results_browser.setHtml(f"<h3>未找到与 QSL 卡号相关的日志:</h3><p>{qsl_id}</p>")
            return

        html = f"<h3>QSL 卡号: {qsl_id}</h3>"
        html += "<p>关联的通联日志:</p><ul>"
        for log_row in logs_for_card:
            log_details = self.db_manager.get_log_details(log_row['log_id'])
            if log_details:
                html += (
                    f"<li><b>呼号:</b> {log_details['station_callsign']} | "
                    f"<b>日期:</b> {log_details['qso_date']} | "
                    f"<b>时间:</b> {log_details['time_on']}Z | "
                    f"<b>波段:</b> {log_details['band']} | "
                    f"<b>模式:</b> {log_details['mode']}</li>"
                )
        html += "</ul>"
        self.results_browser.setHtml(html)

    def write_nfc(self):
        if self.nfc_writer_thread and self.nfc_writer_thread.isRunning():
            self.nfc_writer_thread.stop()
            self.nfc_writer_thread.wait()
            self.nfc_write_button.setText("将输入框内容写入NFC")
            self.nfc_status_label.setText("NFC 空闲")
            self.start_nfc_scan() # Restart scanning after write attempt
            return
        
        port = ConfigManager.get_config("nfc_port")
        data = self.manual_input.text()
        if not port:
            QMessageBox.warning(self, "NFC 未配置", "请先在“设置”中选择并保存PN532端口。")
            return
        if not data: 
            QMessageBox.warning(self, "信息不完整", "请先在输入框中输入要写入的数据。")
            return
        
        self.stop_nfc_scan() # Stop scanning to avoid conflict
        self.nfc_writer_thread = NFCWriter(port, data, self)
        self.nfc_writer_thread.status_update.connect(self.nfc_status_label.setText)
        self.nfc_writer_thread.write_finished.connect(self.on_nfc_write_finished)
        self.nfc_write_button.setText("停止写入")
        self.nfc_writer_thread.start()

    def on_nfc_write_finished(self, success, message):
        self.nfc_status_label.setText(message)
        if success:
            QMessageBox.information(self, "成功", message)
        else:
            QMessageBox.warning(self, "失败", message)
        self.nfc_write_button.setText("将输入框内容写入NFC")
        self.nfc_writer_thread = None
        self.start_nfc_scan()

    def enter_view(self):
        self.populate_nfc_ports()
        saved_port = ConfigManager.get_config("nfc_port")
        if saved_port and self.nfc_port_combo.findText(saved_port) != -1:
            self.nfc_port_combo.setCurrentText(saved_port)
        self.start_nfc_scan()
        self.manual_input.setFocus()

    def leave_view(self):
        self.stop_nfc_scan()
        if self.nfc_writer_thread and self.nfc_writer_thread.isRunning():
            self.nfc_writer_thread.stop()
            self.nfc_writer_thread.wait()
        self.manual_input.clear()
        self.results_browser.clear()
    
    def on_nfc_tag_read(self, text):
        self.manual_input.setText(text)
        QApplication.beep()
        self._perform_search(text)
        self.manual_input.clear()
        
    def start_nfc_scan(self):
        self.stop_nfc_scan() # Ensure any previous scan is stopped
        port = ConfigManager.get_config("nfc_port")
        if port:
            self.nfc_scanner_thread = NFCScanner(port, self)
            self.nfc_scanner_thread.tag_read.connect(self.on_nfc_tag_read)
            self.nfc_scanner_thread.status_update.connect(self.nfc_status_label.setText)
            self.nfc_scanner_thread.start()

    def stop_nfc_scan(self):
        if self.nfc_scanner_thread and self.nfc_scanner_thread.isRunning():
            self.nfc_scanner_thread.stop()
            self.nfc_scanner_thread.wait()
            self.nfc_scanner_thread = None
            self.nfc_status_label.setText("NFC 空闲")

# --- Main Application Window ---
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        os.makedirs("database", exist_ok=True)
        self.db_manager = DatabaseManager(DB_FILE); self.adif_handler = ADIF_Handler()
        self.init_database(); self.init_ui()
    def init_ui(self):
        self.setWindowTitle("QSL Card Manager"); self.setGeometry(100, 100, 1200, 800)
        self.statusBar().showMessage("准备就绪")
        try:
            with open(STYLE_SHEET_FILE, "r", encoding="utf-8") as f: self.setStyleSheet(f.read())
        except FileNotFoundError: print(f"Warning: Stylesheet '{STYLE_SHEET_FILE}' not found.")
        self.central_widget = QWidget(); self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget); self.stacked_widget = QStackedWidget()
        self.main_layout.addWidget(self.stacked_widget); self.create_dashboard_view()
        self.log_management_view = LogManagementWidget(self.db_manager); self.hardware_view = HardwareWidget(self.db_manager)
        self.log_management_view.back_to_dashboard_signal.connect(self.show_dashboard)
        self.log_management_view.data_changed_signal.connect(self.update_dashboard_stats)
        self.hardware_view.back_to_dashboard_signal.connect(self.show_dashboard); 
        self.stacked_widget.addWidget(self.log_management_view); self.stacked_widget.addWidget(self.hardware_view)
        self.stacked_widget.setCurrentWidget(self.dashboard_view)
    def show_dashboard(self):
        self.hardware_view.leave_view() # Ensure threads are stopped
        self.log_management_view.reset_filters()
        self.stacked_widget.setCurrentWidget(self.dashboard_view); self.update_dashboard_stats()
    def create_dashboard_view(self):
        self.dashboard_view = QWidget(); self.dashboard_view.setObjectName("dashboard_view"); main_hbox = QHBoxLayout(self.dashboard_view)
        tile_widget = QWidget(); tile_layout = QGridLayout(tile_widget)
        tiles = {"new_log": ("新通联日志", self.on_new_log_clicked), "log_management": ("日志管理", self.on_log_manage_clicked), "import_adif": ("导入ADIF", self.on_import_clicked), "hardware_scan": ("手动输入/NFC", self.on_scan_clicked), "settings": ("设置", self.on_settings_clicked)}
        positions = [(0,0), (0,1), (1,0), (1,1), (2,0)]
        for position, (key, (text, func)) in zip(positions, tiles.items()):
            button = QPushButton(text); button.setObjectName("tileButton")
            button.setMinimumSize(180, 120); button.clicked.connect(func); tile_layout.addWidget(button, *position)
        
        self.quit_button = QPushButton("退出系统"); self.quit_button.setObjectName("quitButton")
        self.quit_button.setMinimumSize(180, 120); self.quit_button.clicked.connect(self.close)
        tile_layout.addWidget(self.quit_button, 2, 1)

        main_hbox.addWidget(tile_widget, 1) # Changed stretch factor
        stats_widget = QFrame(); stats_widget.setFrameShape(QFrame.StyledPanel); stats_vbox = QVBoxLayout(stats_widget)
        stats_group_label = QLabel("数据统计"); stats_group_label.setObjectName("statsHeader")
        stats_form = QFormLayout()
        self.total_logs_label = QLabel("0"); self.sent_cards_label = QLabel("0"); self.received_cards_label = QLabel("0")
        stats_form.addRow("总日志数:", self.total_logs_label); stats_form.addRow("已发卡片:", self.sent_cards_label); stats_form.addRow("已收卡片:", self.received_cards_label)
        activity_group_label = QLabel("近期动态"); activity_group_label.setObjectName("statsHeader")
        self.activity_list = QListWidget()
        stats_vbox.addWidget(stats_group_label); stats_vbox.addLayout(stats_form); stats_vbox.addWidget(activity_group_label); stats_vbox.addWidget(self.activity_list)
        main_hbox.addWidget(stats_widget, 1) # Changed stretch factor
        self.stacked_widget.addWidget(self.dashboard_view); self.update_dashboard_stats()

    def update_dashboard_stats(self):
        log_count = self.db_manager.get_total_log_count(); sent_count = self.db_manager.get_qsl_count("TC"); rcvd_count = self.db_manager.get_qsl_count("RC")
        self.total_logs_label.setText(str(log_count)); self.sent_cards_label.setText(str(sent_count)); self.received_cards_label.setText(str(rcvd_count))
        self.activity_list.clear()
        recent_activity = self.db_manager.get_recent_qsl_activity()
        for activity in recent_activity:
            direction = "收到" if activity['direction'] == 'RC' else "寄出"; color = "#27ae60" if activity['direction'] == 'RC' else "#e67e22"
            item_text = f"<b>{direction}</b> {activity['station_callsign']} 的卡片"
            list_item = QListWidgetItem(); label = QLabel(item_text); label.setStyleSheet(f"color: {color};")
            self.activity_list.addItem(list_item); self.activity_list.setItemWidget(list_item, label)
    def init_database(self): self.db_manager.initialize_database(); print("Database initialized successfully.")
    def on_new_log_clicked(self):
        primary_callsign = ConfigManager.get_config("primary_callsign")
        if not primary_callsign: QMessageBox.information(self, "设置提示", "请先在“设置”中添加并设置一个主要呼号。"); self.on_settings_clicked(); return
        dialog = LogDetailDialog(self.db_manager, my_callsign=primary_callsign, parent=self)
        if dialog.exec_() == QDialog.Accepted:
            log_data = dialog.get_data()
            if self.db_manager.log_exists(log_data['station_callsign'], log_data['qso_date'], log_data['time_on'], log_data['band'], log_data['mode']):
                QMessageBox.warning(self, "重复日志", "该通联记录已存在，无法重复添加。"); return
            log_id = self.db_manager.add_log_entry(log_data)
            if log_id:
                log_data['id'] = log_id; adif_record = self.adif_handler.qso_to_adif_record(log_data)
                self.adif_handler.append_to_logbook(adif_record); QMessageBox.information(self, "成功", f"新日志已添加 (ID: {log_id}) 并已记录到 logbook.adi。")
                if self.stacked_widget.currentWidget() == self.log_management_view: self.log_management_view.apply_filters()
                self.update_dashboard_stats()
    def on_log_manage_clicked(self): self.log_management_view.load_initial_data(); self.stacked_widget.setCurrentWidget(self.log_management_view)
    def on_import_clicked(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "选择ADIF日志文件", "", "ADIF Files (*.adi);;All Files (*)")
        if not file_path: return
        try:
            qsos, _ = adif_io.read_from_file(file_path); imported_count, updated_count, duplicate_count = 0, 0, 0
            my_configured_callsigns = self.db_manager.get_all_my_callsigns(); primary_callsign = ConfigManager.get_config("primary_callsign")
            for qso in qsos:
                band = qso.get('BAND', '')
                if not all(k in qso for k in ['CALL', 'QSO_DATE', 'TIME_ON', 'BAND']): continue
                
                existing_log_id = self.db_manager.log_exists(qso['CALL'], qso['QSO_DATE'], qso['TIME_ON'], band, qso.get('MODE'))
                if existing_log_id:
                    existing_log = self.db_manager.get_log_details(existing_log_id)
                    merged_data = dict(existing_log); needs_update = False
                    
                    new_log_data = {'station_callsign': qso.get('CALL'), 'qso_date': qso.get('QSO_DATE'), 'time_on': qso.get('TIME_ON'), 'band': band, 'band_rx': qso.get('BAND_RX'), 'freq': qso.get('FREQ'), 'freq_rx': qso.get('FREQ_RX'), 'mode': qso.get('MODE'), 'rst_sent': qso.get('RST_SENT'), 'rst_rcvd': qso.get('RST_RCVD'), 'comment': qso.get('COMMENT', ''), 'my_callsign': qso.get('OPERATOR', primary_callsign), 'submode': qso.get('SUBMODE'), 'sat_name': qso.get('SAT_NAME'), 'prop_mode': qso.get('PROP_MODE')}

                    for key, new_value in new_log_data.items():
                        if new_value and not merged_data.get(key):
                            merged_data[key] = new_value; needs_update = True
                    
                    new_comment = new_log_data.get('comment', ''); old_comment = merged_data.get('comment', '')
                    if new_comment and new_comment not in (old_comment or ""):
                        merged_data['comment'] = f"{old_comment or ''} | IMPORTED: {new_comment}".strip(" | "); needs_update = True
                    
                    if needs_update:
                        self.db_manager.update_log_entry(existing_log_id, merged_data); updated_count += 1
                    else: duplicate_count += 1
                else:
                    operator = qso.get('OPERATOR', '').upper(); my_call = operator if operator in my_configured_callsigns else primary_callsign
                    log_data = { 'station_callsign': qso.get('CALL'), 'qso_date': qso.get('QSO_DATE'), 'time_on': qso.get('TIME_ON'), 'band': band, 'band_rx': qso.get('BAND_RX'), 'freq': qso.get('FREQ'), 'freq_rx': qso.get('FREQ_RX'), 'mode': qso.get('MODE'), 'rst_sent': qso.get('RST_SENT'), 'rst_rcvd': qso.get('RST_RCVD'), 'comment': qso.get('COMMENT', ''), 'my_callsign': my_call, 'submode': qso.get('SUBMODE'), 'sat_name': qso.get('SAT_NAME'), 'prop_mode': qso.get('PROP_MODE') }
                    self.db_manager.add_log_entry(log_data); imported_count += 1
            QMessageBox.information(self, "导入完成", f"成功导入 {imported_count} 条新日志。\n更新合并 {updated_count} 条已有日志。\n发现 {duplicate_count} 条完全重复日志已跳过。")
            if self.stacked_widget.currentWidget() == self.log_management_view: self.log_management_view.apply_filters()
            self.update_dashboard_stats()
        except Exception as e: QMessageBox.critical(self, "导入失败", f"无法解析或处理ADIF文件。\n错误: {e}")
    def on_scan_clicked(self): 
        self.hardware_view.enter_view()
        self.stacked_widget.setCurrentWidget(self.hardware_view)
    def on_settings_clicked(self): dialog = SettingsDialog(self.db_manager, self); dialog.data_changed.connect(self.update_dashboard_stats); dialog.exec_()
    def closeEvent(self, event): self.hardware_view.closeEvent(event); self.db_manager.close(); event.accept()
    def search_by_qsl_id(self, qsl_id):
        self.stacked_widget.setCurrentWidget(self.log_management_view); self.log_management_view.search_by_qsl_id(qsl_id)

# --- Database Manager ---
class DatabaseManager:
    def __init__(self, db_file):
        db_dir = os.path.dirname(db_file)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        self.conn = sqlite3.connect(db_file); self.conn.row_factory = sqlite3.Row; self.cursor = self.conn.cursor()
    def execute_query(self, query, params=()):
        try: self.cursor.execute(query, params); self.conn.commit(); return True
        except sqlite3.Error as e: print(f"Database error: {e}"); self.conn.rollback(); return False
    def fetch_one(self, query, params=()): self.cursor.execute(query, params); return self.cursor.fetchone()
    def fetch_all(self, query, params=()): self.cursor.execute(query, params); return self.cursor.fetchall()
    def get_all_my_callsigns(self): return [row['callsign'] for row in self.fetch_all("SELECT callsign FROM callsigns")]
    def add_callsign(self, callsign): return self.execute_query("INSERT OR IGNORE INTO callsigns (callsign) VALUES (?)", (callsign,))
    def delete_callsign(self, callsign): return self.execute_query("DELETE FROM callsigns WHERE callsign = ?", (callsign,))
    def add_log_entry(self, log_data):
        log_data['adif_blob'] = json.dumps(log_data)
        query = "INSERT INTO logs (my_callsign, station_callsign, qso_date, time_on, band, band_rx, freq, freq_rx, mode, rst_sent, rst_rcvd, comment, adif_blob, submode, sat_name, prop_mode) VALUES (:my_callsign, :station_callsign, :qso_date, :time_on, :band, :band_rx, :freq, :freq_rx, :mode, :rst_sent, :rst_rcvd, :comment, :adif_blob, :submode, :sat_name, :prop_mode)"
        if self.execute_query(query, log_data):
            log_id = self.cursor.lastrowid
            self.execute_query("UPDATE logs SET sort_id = ? WHERE id = ?", (log_id, log_id))
            return log_id
        return None
    def update_log_entry(self, log_id, log_data):
        log_data['adif_blob'] = json.dumps(log_data); log_data['log_id'] = log_id
        query = "UPDATE logs SET station_callsign=:station_callsign, qso_date=:qso_date, time_on=:time_on, band=:band, band_rx=:band_rx, freq=:freq, freq_rx=:freq_rx, mode=:mode, rst_sent=:rst_sent, rst_rcvd=:rst_rcvd, comment=:comment, adif_blob=:adif_blob, submode=:submode, sat_name=:sat_name, prop_mode=:prop_mode WHERE id=:log_id"
        return self.execute_query(query, log_data)
    def add_qsl_card(self, qsl_id, log_ids: list, direction):
        try:
            now = datetime.datetime.now().isoformat()
            self.cursor.execute("INSERT INTO qsl_cards (qsl_id, direction, status, created_at) VALUES (?, ?, ?, ?)", (qsl_id, direction, 'In Stock', now))
            for log_id in log_ids: self.cursor.execute("INSERT INTO qsl_log_link (qsl_id, log_id) VALUES (?, ?)", (qsl_id, log_id))
            status_field = "qsl_rcvd" if direction == 'RC' else "qsl_sent"
            placeholders = ', '.join('?' for _ in log_ids)
            self.cursor.execute(f"UPDATE logs SET {status_field} = 'Y' WHERE id IN ({placeholders})", log_ids)
            self.conn.commit(); return True
        except sqlite3.Error as e: print(f"Error adding QSL card: {e}"); self.conn.rollback(); return False
    def get_log_details(self, log_id): return self.fetch_one("SELECT * FROM logs WHERE id = ?", (log_id,))
    def get_qsl_cards_for_log(self, log_id): return self.fetch_all("SELECT q.* FROM qsl_cards q JOIN qsl_log_link ql ON q.qsl_id = ql.qsl_id WHERE ql.log_id = ?", (log_id,))
    def get_logs_for_qsl_card(self, qsl_id): return self.fetch_all("SELECT log_id FROM qsl_log_link WHERE qsl_id = ?", (qsl_id,))
    def get_total_log_count(self): return self.fetch_one("SELECT COUNT(id) FROM logs")[0]
    def get_qsl_count(self, direction): return self.fetch_one("SELECT COUNT(qsl_id) FROM qsl_cards WHERE direction = ?", (direction,))[0]
    def get_recent_qsl_activity(self, limit=10): return self.fetch_all("SELECT q.direction, l.station_callsign FROM qsl_cards q JOIN qsl_log_link ql ON q.qsl_id = ql.qsl_id JOIN logs l ON ql.log_id = l.id GROUP BY q.qsl_id ORDER BY q.created_at DESC LIMIT ?", (limit,))
    def search_logs(self, station_callsign=None, my_callsign=None, mode=None, qsl_id=None):
        params = []; db_columns = ["l.id", "l.my_callsign", "l.station_callsign", "l.qso_date", "l.time_on", "l.band", "l.band_rx", "l.freq", "l.freq_rx", "l.mode", "l.qsl_sent", "l.qsl_rcvd", "l.comment"]
        col_string = ", ".join(db_columns); base_query = f"SELECT DISTINCT {col_string} FROM logs l"; joins = ""; conditions = " WHERE 1=1"
        if qsl_id and qsl_id.strip(): joins += " JOIN qsl_log_link ql ON l.id = ql.log_id JOIN qsl_cards q ON ql.qsl_id = q.qsl_id"; conditions += " AND q.qsl_id LIKE ?"; params.append(f"%{qsl_id.strip()}%")
        if my_callsign and my_callsign.strip(): conditions += " AND l.my_callsign LIKE ?"; params.append(f"%{my_callsign.strip()}%")
        if station_callsign and station_callsign.strip(): conditions += " AND l.station_callsign LIKE ?"; params.append(f"%{station_callsign.strip()}%")
        if mode and mode != "全部模式": conditions += " AND l.mode = ?"; params.append(mode)
        final_query = base_query + joins + conditions + " ORDER BY l.sort_id DESC"
        return self.fetch_all(final_query, tuple(params))
    def log_exists(self, station_callsign, qso_date, time_on, band, mode):
        query = "SELECT id, time_on FROM logs WHERE UPPER(station_callsign)=? AND qso_date=? AND UPPER(band)=? AND UPPER(mode)=?"
        potential_duplicates = self.fetch_all(query, (station_callsign.upper(), qso_date, (band or "").upper(), (mode or "").upper()))
        if not potential_duplicates: return None
        try:
            new_qso_time_obj = datetime.datetime.strptime(time_on.zfill(6), '%H%M%S')
        except ValueError: return None
        time_window = datetime.timedelta(minutes=5)
        for row in potential_duplicates:
            try:
                existing_time_obj = datetime.datetime.strptime(row['time_on'].zfill(6), '%H%M%S')
                if abs(new_qso_time_obj - existing_time_obj) <= time_window:
                    return row['id']
            except ValueError: continue
        return None
    def find_all_duplicates(self):
        query = "SELECT station_callsign, qso_date, band, mode FROM logs GROUP BY UPPER(station_callsign), qso_date, UPPER(band), UPPER(mode) HAVING COUNT(id) > 1"
        candidate_groups = self.fetch_all(query)
        duplicate_sets = []
        time_window = datetime.timedelta(minutes=5)
        for group in candidate_groups:
            logs_in_group = self.fetch_all(
                "SELECT id, time_on FROM logs WHERE UPPER(station_callsign)=? AND qso_date=? AND UPPER(band)=? AND UPPER(mode)=? ORDER BY time_on",
                (group['station_callsign'].upper(), group['qso_date'], (group['band'] or "").upper(), (group['mode'] or "").upper())
            )
            if len(logs_in_group) < 2: continue
            
            visited_indices = set()
            for i in range(len(logs_in_group)):
                if i in visited_indices: continue
                current_set = {logs_in_group[i]['id']}
                try: time_i = datetime.datetime.strptime(logs_in_group[i]['time_on'].zfill(6), '%H%M%S')
                except (ValueError, AttributeError): continue
                
                for j in range(i + 1, len(logs_in_group)):
                    if j in visited_indices: continue
                    try:
                        time_j = datetime.datetime.strptime(logs_in_group[j]['time_on'].zfill(6), '%H%M%S')
                        if abs(time_j - time_i) <= time_window:
                            current_set.add(logs_in_group[j]['id']); visited_indices.add(j)
                    except (ValueError, AttributeError): continue
                if len(current_set) > 1:
                    duplicate_sets.append(current_set)
        return duplicate_sets
    def initialize_database(self):
        queries = ["CREATE TABLE IF NOT EXISTS callsigns (callsign TEXT PRIMARY KEY)", "CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, sort_id INTEGER, my_callsign TEXT, station_callsign TEXT, qso_date TEXT, time_on TEXT, band TEXT, band_rx TEXT, freq REAL, freq_rx REAL, mode TEXT, submode TEXT, rst_sent TEXT, rst_rcvd TEXT, comment TEXT, adif_blob TEXT, qsl_sent TEXT DEFAULT 'N', qsl_rcvd TEXT DEFAULT 'N', sat_name TEXT, prop_mode TEXT)", "CREATE TABLE IF NOT EXISTS qsl_cards (qsl_id TEXT PRIMARY KEY, direction TEXT NOT NULL, status TEXT, location TEXT, created_at TEXT NOT NULL)", "CREATE TABLE IF NOT EXISTS qsl_log_link (qsl_id TEXT NOT NULL, log_id INTEGER NOT NULL, PRIMARY KEY (qsl_id, log_id), FOREIGN KEY (qsl_id) REFERENCES qsl_cards (qsl_id) ON DELETE CASCADE, FOREIGN KEY (log_id) REFERENCES logs (id) ON DELETE CASCADE)"]
        for query in queries: self.execute_query(query)
        try:
            self.cursor.execute("SELECT sort_id FROM logs LIMIT 1")
        except sqlite3.OperationalError:
            self.execute_query("ALTER TABLE logs ADD COLUMN sort_id INTEGER")
            self.execute_query("UPDATE logs SET sort_id = id")
    def reorder_logs_by_time(self):
        try:
            logs = self.fetch_all("SELECT id, qso_date, time_on FROM logs ORDER BY qso_date, time_on")
            for new_index, log in enumerate(logs):
                self.execute_query("UPDATE logs SET sort_id = ? WHERE id = ?", (new_index + 1, log['id']))
            return True
        except sqlite3.Error as e:
            print(f"Error reordering logs: {e}")
            return False
    def delete_log(self, log_id):
        self.execute_query("DELETE FROM qsl_log_link WHERE log_id = ?", (log_id,))
        return self.execute_query("DELETE FROM logs WHERE id = ?", (log_id,))
    def recycle_qsl_card(self, log_id, direction):
        qsl_id_row = self.fetch_one("SELECT q.qsl_id FROM qsl_cards q JOIN qsl_log_link ql ON q.qsl_id = ql.qsl_id WHERE ql.log_id = ? AND q.direction = ?", (log_id, direction))
        if not qsl_id_row: return False
        qsl_id = qsl_id_row['qsl_id']
        status_field = "qsl_rcvd" if direction == 'RC' else "qsl_sent"
        self.execute_query(f"UPDATE logs SET {status_field} = 'N' WHERE id = ?", (log_id,))
        self.execute_query("DELETE FROM qsl_log_link WHERE qsl_id = ? AND log_id = ?", (qsl_id,))
        is_linked_elsewhere = self.fetch_one("SELECT 1 FROM qsl_log_link WHERE qsl_id = ?", (qsl_id,))
        if not is_linked_elsewhere: self.execute_query("DELETE FROM qsl_cards WHERE qsl_id = ?", (qsl_id,))
        return True
    def reset_all_qsl_data(self):
        try:
            self.execute_query("DELETE FROM qsl_log_link")
            self.execute_query("DELETE FROM qsl_cards")
            self.execute_query("UPDATE logs SET qsl_sent = 'N', qsl_rcvd = 'N'")
            return True
        except:
            return False
    def close(self): self.conn.close(); print("Database connection closed.")

# --- QSL ID Generator ---
class QSL_ID_Generator:
    @staticmethod
    def get_next_serial(db_manager, id_type):
        last_id_row = db_manager.fetch_one("SELECT qsl_id FROM qsl_cards WHERE direction = ? ORDER BY created_at DESC, qsl_id DESC LIMIT 1", (id_type,))
        if last_id_row:
            last_id = last_id_row['qsl_id']; last_year = last_id[0:2]; current_year = datetime.datetime.now().strftime('%y')
            if last_year == current_year: return int(last_id[2:8]) + 1
        return 1
    @staticmethod
    def generate(db_manager, id_type):
        if id_type not in ['RC', 'TC']: raise ValueError("Type must be 'RC' or 'TC'")
        year = datetime.datetime.now().strftime('%y'); serial = QSL_ID_Generator.get_next_serial(db_manager, id_type)
        random_hex = secrets.token_hex(8); return f"{year}{serial:06d}{id_type}{random_hex.upper()}"

if __name__ == '__main__':
    app = QApplication(sys.argv)
    
    missing_libs = []
    try: import adif_io
    except ImportError: missing_libs.append('adif-io')
    try: import serial
    except ImportError: missing_libs.append('pyserial')
    try: import nfc
    except ImportError: missing_libs.append('nfcpy')
    try: import qrcode
    except ImportError: missing_libs.append('qrcode')
    try: from reportlab.pdfgen import canvas
    except ImportError: missing_libs.append('reportlab')
    try: from PIL import Image
    except ImportError: missing_libs.append('Pillow')
    try: import fitz
    except ImportError: missing_libs.append('PyMuPDF')
        
    if missing_libs:
        QMessageBox.critical(None, "缺少依赖库", f"检测到缺少以下必要的库:\n\n{', '.join(missing_libs)}\n\n请在终端中运行 'pip install --upgrade <library_name>' 来安装或更新它们。")
        sys.exit(1)
        
    os.makedirs("assets", exist_ok=True)
    with open(STYLE_SHEET_FILE, "w", encoding="utf-8") as f:
        f.write("""
    QMainWindow, QDialog, QWidget { background-color: #2c3e50; color: #ecf0f1; font-size: 26px; }
    QWidget#dashboard_view { background-color: #34495e; }
    QGroupBox { border: 1px solid #7f8c8d; border-radius: 5px; margin-top: 1ex; }
    QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top center; padding: 0 3px; }
    QPushButton, QComboBox { background-color: #3498db; color: #ffffff; border: 1px solid #2980b9; border-radius: 4px; padding: 8px; min-height: 20px; }
    QPushButton:hover { background-color: #4aa3df; }
    QPushButton:pressed { background-color: #2980b9; }
    QPushButton#tileButton { background-color: #2c3e50; border: 2px solid #3498db; border-radius: 10px; padding: 20px; font-size: 30px; font-weight: bold; }
    QPushButton#tileButton:hover { background-color: #3498db; color: #ffffff; }
    QPushButton#quitButton { background-color: #c0392b; border: 2px solid #e74c3c; border-radius: 10px; padding: 20px; font-size: 30px; font-weight: bold; }
    QPushButton#quitButton:hover { background-color: #e74c3c; }
    QLineEdit, QDateEdit, QTextEdit, QListWidget { background-color: #2c3e50; border: 1px solid #7f8c8d; border-radius: 4px; padding: 8px; color: #ecf0f1; }
    QLineEdit:read-only { background-color: #34495e; }
    QFrame { border: 1px solid #7f8c8d; border-radius: 5px; }
    QTableView { border: 1px solid #7f8c8d; gridline-color: #7f8c8d; }
    QTableView::item { padding: 5px; }
    QTableView::item:alternate { background-color: #34495e; }
    QHeaderView::section { background-color: #3498db; color: white; padding: 5px; border: 1px solid #2980b9; font-weight: bold; }
    QDialog { border: 1px solid #7f8c8d; }
    QLabel#statsHeader { font-size: 16px; font-weight: bold; color: #3498db; margin-top: 10px; border: none; }
    QListWidget { border: none; }
    """)
    main_win = MainWindow()
    main_win.show()
    sys.exit(app.exec_())
