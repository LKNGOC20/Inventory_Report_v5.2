from PyQt5 import uic
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QApplication, QMessageBox, QDialog, QGridLayout, QPushButton, QLabel
import PyQt5.QtCore
from PyQt5.QtCore import Qt, QDateTime, QTimer, QSize
from PyQt5.QtGui import QFont, QCursor
import sys, os, time, psycopg2
import pandas as pd
import numpy as np
from datetime import datetime as dt
from pandas.tseries.offsets import Day
import shutil
from pyexcelerate import Workbook,Style,Color,Panes,Format
import xlwings as xw
import gc
import My_Custom_Class
import warnings
warnings.filterwarnings("ignore")
##################################################################################################################
class SubThread(PyQt5.QtCore.QThread):
	updated = PyQt5.QtCore.pyqtSignal(int)
	running = False
	def __init__(self, parent=None):
		super(SubThread, self).__init__(parent)
		self.progPercent = 0
		self.running = True

	def run(self):
		while self.running:
			global start_time, end_time
			start_time = time.time()

			self.progPercent = 1
			self.updated.emit(int(self.progPercent))

			if ".csv" in filename[0]:
				RawData_Obj.data = pd.read_csv(filename[0],usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12],low_memory=True)
				RawData_Obj.data['Price Value'] = RawData_Obj.data['Price Value'].str.replace(',', '')
				RawData_Obj.data['Price Value'] =RawData_Obj.data['Price Value'].astype(float)
				RawData_Obj.data['On Hand Qty'] = RawData_Obj.data['On Hand Qty'].str.replace(',', '')
				RawData_Obj.data['On Hand Qty'] =RawData_Obj.data['On Hand Qty'].astype(float)
			else:
				RawData_Obj.data = pd.read_excel(filename[0],usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12])
			# format date
			RawData_Obj.data['Valuation Date'] =pd.to_datetime(RawData_Obj.data['Valuation Date'], errors='coerce')
			RawData_Obj.data['Valuation Date'] = RawData_Obj.data['Valuation Date'].dt.strftime('%d/%m/%Y')
			Tool_Instance.Val_Date = dt.strptime(RawData_Obj.data.iloc[0,0], '%d/%m/%Y')
			
			RawData_Obj.data['Last Movement Date'] =pd.to_datetime(RawData_Obj.data['Last Movement Date'], errors='coerce')
			RawData_Obj.data['Last Movement Date'] = RawData_Obj.data['Last Movement Date'].dt.strftime('%d/%m/%Y')

			RawData_Obj.data['Last Material Receipt Date'] =pd.to_datetime(RawData_Obj.data['Last Material Receipt Date'], errors='coerce')
			RawData_Obj.data['Last Material Receipt Date'] = RawData_Obj.data['Last Material Receipt Date'].dt.strftime('%d/%m/%Y')
			# change column name
			RawData_Obj.data.columns = RawData_Obj.data.columns.str.replace(' ', '_')
			# format Price_Value
			RawData_Obj.data['Price_Value'] = RawData_Obj.data['Price_Value'].fillna(0)
			#RawData_Obj.data['Price_Value'] = RawData_Obj.data['Price_Value'].apply(lambda x: format(float(x),".2f"))
			# format On_Hand_Qty
			RawData_Obj.data['On_Hand_Qty'] = RawData_Obj.data['On_Hand_Qty'].fillna(0)
			
			RawData_Obj.data['Moving_Average_Price'] = RawData_Obj.data['Moving_Average_Price'].fillna(0)
	
			RightResult = pd.DataFrame(columns=['1-30_Days_Ago_Qty', '1-30_Days_Ago_Amount','31-60_Days_Ago_Qty','31-60_Days_Ago_Amount', 
												'61-90_Days_Ago_Qty','61-90_Days_Ago_Amount','91-120_Days_Ago_Qty', '91-120_Days_Ago_Amount',
												'121-150_Days_Ago_Qty', '121-150_Days_Ago_Amount','151-180_Days_Ago_Qty', '151-180_Days_Ago_Amount',
												'181-210_Days_Ago_Qty', '181-210_Days_Ago_Amount','211-240_Days_Ago_Qty', '211-240_Days_Ago_Amount',
												'241-270_Days_Ago_Qty', '241-270_Days_Ago_Amount','271-300_Days_Ago_Qty', '271-300_Days_Ago_Amount',
												'301-330_Days_Ago_Qty', '301-330_Days_Ago_Amount','331-360_Days_Ago_Qty', '331-360_Days_Ago_Amount',
												'Over_361_Days_Ago_Qty','Over_361_Days_Ago_Amount','Is_Over'])
			RawData_Obj.data = pd.concat([RawData_Obj.data, RightResult],
												axis="columns",copy=False)

			# 1-30
			self.progPercent = 2
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['1-30_Days_Ago_Qty'], RawData_Obj.data['Is_Over']  =RawData_Obj.using_pandas_vectorization_qty_thisMonth(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['1-30_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['1-30_Days_Ago_Qty'])
			#RawData_Obj.data['1-30_Days_Ago_Amount'] = RawData_Obj.data['1-30_Days_Ago_Amount'].astype('float').round(2)
			
			# 31-60
			self.progPercent = 3
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last1Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['31-60_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['31-60_Days_Ago_Qty'])
			
			# 61-90
			self.progPercent = 4
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last2Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['61-90_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['61-90_Days_Ago_Qty'])
			
			# 91-120
			self.progPercent = 5
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last3Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['91-120_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['91-120_Days_Ago_Qty'])
			
			# 121-150
			self.progPercent = 6
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last4Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['121-150_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['121-150_Days_Ago_Qty'])
			
			# 151-180
			self.progPercent = 7
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['151-180_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last5Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['121-150_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['151-180_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['151-180_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['151-180_Days_Ago_Qty'])
			
			# 181-210
			self.progPercent = 8
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['181-210_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last6Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['151-180_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['181-210_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['181-210_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['181-210_Days_Ago_Qty'])
			
			#211-240
			self.progPercent = 9
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['211-240_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last7Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['151-180_Days_Ago_Qty'],RawData_Obj.data['181-210_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['211-240_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['211-240_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['211-240_Days_Ago_Qty'])
			
			#241-270
			self.progPercent = 10
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['241-270_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last8Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['151-180_Days_Ago_Qty'],RawData_Obj.data['181-210_Days_Ago_Qty'],RawData_Obj.data['211-240_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['241-270_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['241-270_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['241-270_Days_Ago_Qty'])
			
			#271-300
			self.progPercent = 11
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['271-300_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last9Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['151-180_Days_Ago_Qty'],RawData_Obj.data['181-210_Days_Ago_Qty'],RawData_Obj.data['211-240_Days_Ago_Qty'],RawData_Obj.data['241-270_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['271-300_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['271-300_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['271-300_Days_Ago_Qty'])
			
			#301-330
			self.progPercent = 12
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['301-330_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last10Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['151-180_Days_Ago_Qty'],RawData_Obj.data['181-210_Days_Ago_Qty'],RawData_Obj.data['211-240_Days_Ago_Qty'],RawData_Obj.data['241-270_Days_Ago_Qty'],RawData_Obj.data['271-300_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['301-330_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['301-330_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['301-330_Days_Ago_Qty'])
			
			#331-360
			self.progPercent = 13
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['331-360_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_last11Month(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['151-180_Days_Ago_Qty'],RawData_Obj.data['181-210_Days_Ago_Qty'],RawData_Obj.data['211-240_Days_Ago_Qty'],RawData_Obj.data['241-270_Days_Ago_Qty'],RawData_Obj.data['271-300_Days_Ago_Qty'],RawData_Obj.data['301-330_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['331-360_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['331-360_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['331-360_Days_Ago_Qty'])

			# 361->
			self.progPercent = 14
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data['Over_361_Days_Ago_Qty'],RawData_Obj.data['Is_Over'] =RawData_Obj.using_pandas_vectorization_qty_over1Year(RawData_Obj.data['Item_No'],RawData_Obj.data['Locator'],RawData_Obj.data['On_Hand_Qty'],RawData_Obj.data['1-30_Days_Ago_Qty'],RawData_Obj.data['31-60_Days_Ago_Qty'],RawData_Obj.data['61-90_Days_Ago_Qty'],RawData_Obj.data['91-120_Days_Ago_Qty'],RawData_Obj.data['121-150_Days_Ago_Qty'],RawData_Obj.data['151-180_Days_Ago_Qty'],RawData_Obj.data['181-210_Days_Ago_Qty'],RawData_Obj.data['211-240_Days_Ago_Qty'],RawData_Obj.data['241-270_Days_Ago_Qty'],RawData_Obj.data['271-300_Days_Ago_Qty'],RawData_Obj.data['301-330_Days_Ago_Qty'],RawData_Obj.data['331-360_Days_Ago_Qty'],Tool_Instance.Val_Date,RawData_Obj.data['Over_361_Days_Ago_Qty'],RawData_Obj.data['Is_Over'])
			RawData_Obj.data['Over_361_Days_Ago_Amount'] = RawData_Obj.using_pandas_vectorization_amount(RawData_Obj.data['Over_361_Days_Ago_Qty'])
			
			# Prepare for display
			self.progPercent = 15
			self.updated.emit(int(self.progPercent))
			RawData_Obj.data.drop(columns=['Is_Over'],inplace=True)
			RawData_Obj.reduce_mem_usage(RawData_Obj.data)
			end_time = time.time()

			# End multi-thread
			self.progPercent = 16
			self.updated.emit(int(self.progPercent))
			Tool_Instance.ResultReport.setModel(My_Custom_Class.PandasModel(RawData_Obj.data,"ResultReport"))
			selectionModel = Tool_Instance.ResultReport.selectionModel() 
			selectionModel.selectionChanged.connect(Tool_Instance.selection_changed)
			Tool_Instance.ResultReport.resizeColumnsToContents()
			time.sleep(0.01)
			
	def stop(self):
		self.running = False

##################################################################################################################
class SubThread_Transaction(PyQt5.QtCore.QThread):
	updated = PyQt5.QtCore.pyqtSignal(int)
	running = False
	def __init__(self, parent=None):
		super(SubThread_Transaction, self).__init__(parent)
		self.progPercent = 0
		self.running = True
	
	def run(self):
		while self.running:
			temp_df = []
			i = 0
			for idx in range(0,len(Tool_Instance.Items)):
				condition= (All_Transaction_Obj.data['Product_Name']==Tool_Instance.Items[idx])&(All_Transaction_Obj.data['Locator']==Tool_Instance.Locators[idx])
				temp_df.append(All_Transaction_Obj.data[(condition)])
				i+=1
				self.progPercent =int(100*i/(len(Tool_Instance.Items)+1))
				self.updated.emit(int(self.progPercent))
			Select_Transaction_Obj.data = pd.concat(temp_df,ignore_index=True)
			del temp_df
			self.progPercent =100
			self.updated.emit(int(self.progPercent))
			time.sleep(0.01)
	
	def stop(self):
		self.running = False

##################################################################################################################
class SubThread_Export(PyQt5.QtCore.QThread):
	updated = PyQt5.QtCore.pyqtSignal(int)
	running = False
	def __init__(self, parent=None):
		super(SubThread_Export, self).__init__(parent)
		self.progPercent = 0
		self.running = True

	def stop(self):
		self.running = False
	
	def run(self):
		while self.running:
			self.progPercent = 1
			self.updated.emit(int(self.progPercent))
			with pd.ExcelWriter(report_file,mode="a",engine="openpyxl",if_sheet_exists="overlay") as writer: 
					RawDataPrint_Obj.data.to_excel(writer, sheet_name="Report",header=None, startcol=0,startrow=2,index=False)

			self.progPercent = 2
			self.updated.emit(int(self.progPercent))
			wb = Workbook()
			if Select_Transaction_Obj.data.empty:
				ws=wb.new_sheet("Transaction", data=[All_Transaction_Obj.data.columns] + list(All_Transaction_Obj.data.values))
			else:
				ws=wb.new_sheet("Transaction", data=[Select_Transaction_Obj.data.columns] + list(Select_Transaction_Obj.data.values))
			ws.range("A1","H1").style.font.bold = True
			ws.range("A1","H1").style.fill.background = Color(146, 208, 80, 0)
			ws.set_row_style(1, Style(size=36))
			ws.range("A1","H1").style.alignment.vertical  = 'center'
			ws.range("A1","H1").style.alignment.horizontal = 'center'				
			ws.set_col_style(1, Style(size=13.43))
			ws.set_col_style(2, Style(size=11.29,format=Format('dd/mm/yyyy')))
			ws.set_col_style(3, Style(size=12.14))
			ws.set_col_style(4, Style(size=21.43))
			ws.set_col_style(5, Style(size=15.86))
			ws.set_col_style(6, Style(size=15.57))
			ws.set_col_style(7, Style(size=15.43,format=Format('dd/mm/yyyy')))
			ws.set_col_style(8, Style(size=20.29))
			ws.range("A1","H1").style.borders.right.style = '_'
			ws.range("A1","H1").style.borders.left.style = '_'
			ws.range("A1","H1").style.borders.top.style = '_'
			ws.range("A1","H1").style.borders.bottom.style = '_'
			ws.panes = Panes(1, 1)
			wb.save("Transaction.xlsx")

			self.progPercent = 3
			self.updated.emit(int(self.progPercent))
			excel_app = xw.App(visible=False)
			excel_book = excel_app.books.open('Transaction.xlsx')
			sheet = excel_book.sheets[0]
			new_wb = excel_app.books.open(report_file)
			sheet.api.Copy(After=new_wb.sheets['Report'].api)
			new_wb.save()
			new_wb.close()
			excel_app.quit()
			os.remove('Transaction.xlsx')

			self.progPercent = 4
			self.updated.emit(int(self.progPercent))
			time.sleep(0.01)

##################################################################################################################
class DataFrame():
	def __init__(self, name,data):
		self.name = name
		self.data = data
		print("Phương thức khởi tạo của lớp DataFrame, đối tượng {} đã được gọi!".format(self.name))

	def __del__(self):
		print("Phương thức hủy của lớp Dataframe, đối tượng {} đã được gọi!".format(self.name))

	def __str__(self):
		return self.name
	
	def reduce_mem_usage(self,df):
		start_mem = df.memory_usage().sum() / 1024**2
		print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))
		for col in df.columns:
			col_type = df[col].dtype
		if col_type != object:
				c_min = df[col].min()
				c_max = df[col].max()
				if str(col_type)[:3] == 'int':
					if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
						df[col] = df[col].astype(np.int8)
					elif c_min > np.iinfo(np.uint8).min and c_max < np.iinfo(np.uint8).max:
						df[col] = df[col].astype(np.uint8)
					elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
						df[col] = df[col].astype(np.int16)
					elif c_min > np.iinfo(np.uint16).min and c_max < np.iinfo(np.uint16).max:
						df[col] = df[col].astype(np.uint16)
					elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
						df[col] = df[col].astype(np.int32)
					elif c_min > np.iinfo(np.uint32).min and c_max < np.iinfo(np.uint32).max:
						df[col] = df[col].astype(np.uint32)                    
					elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
						df[col] = df[col].astype(np.int64)
					elif c_min > np.iinfo(np.uint64).min and c_max < np.iinfo(np.uint64).max:
						df[col] = df[col].astype(np.uint64)
				elif str(col_type)[:5] == 'float':
					if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
						df[col] = df[col].astype(np.float16)
					elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
						df[col] = df[col].astype(np.float32)
					else:
						df[col] = df[col].astype(np.float64)
		end_mem = df.memory_usage().sum() / 1024**2
		print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
		print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
		return df
	#1-30
	def using_pandas_vectorization_qty_thisMonth(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,Val_Date,This_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))	
			This_Time_Qty = 0
			if OnHandQuantity <=0:
				RawData_Obj.data['Is_Over'].values[i] = "True"
			else:
				sqlCmd = """SELECT 
									m_transaction.m_transaction_id AS "Transaction_ID",
									m_transaction.updated AS "Update_Day", 
									ad_org.name AS "Organization",
									m_locator.value AS "Locator",
									m_product.name AS "Product_Name",
									m_transaction.movementtype AS "Movement_Type", 
									m_transaction.movementdate AS "Movement_Date", 
									m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
									adempiere.m_transaction, 
									adempiere.m_product, 
									adempiere.m_locator,
									adempiere.ad_org
							WHERE 
									ad_org.ad_org_id = m_transaction.ad_org_id AND
									m_product.m_product_id = m_transaction.m_product_id AND
									m_locator.m_locator_id = m_transaction.m_locator_id AND
									m_product.name = '{0}' AND m_locator.value = '{1}' AND
									m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
									m_transaction.movementdate>='{2}' AND 
									m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-30+interval),Val_Date)
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))	
				if not Transaction_df.empty:
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if This_Time_Qty == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		RawData_Obj.data['Is_Over'].values[i] = "True"
						# 		break
						# 	elif This_Time_Qty > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		RawData_Obj.data['Is_Over'].values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			This_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		return This_Month_Qty_Col,Is_Over_Col
	#31-60
	def using_pandas_vectorization_qty_last1Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Val_Date,Last_1_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-60+interval),Val_Date+ Day(-31+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-This_Month_Qty_Col.values[i]
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_1_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		return Last_1_Month_Qty_Col,Is_Over_Col
	#61-90
	def using_pandas_vectorization_qty_last2Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Val_Date,Last_2_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-90+interval),Val_Date+ Day(-61+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_2_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		return Last_2_Month_Qty_Col,Is_Over_Col
	#91-120
	def using_pandas_vectorization_qty_last3Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Val_Date,Last_3_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-120+interval),Val_Date+ Day(-91+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_3_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_3_Month_Qty_Col,Is_Over_Col
	#121-150
	def using_pandas_vectorization_qty_last4Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Val_Date,Last_4_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-150+interval),Val_Date+ Day(-121+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i] + Last_3_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_4_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_4_Month_Qty_Col,Is_Over_Col
	#151-180
	def using_pandas_vectorization_qty_last5Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Last_4_Month_Qty_Col,Val_Date,Last_5_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-180+interval),Val_Date+ Day(-151+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i] + Last_3_Month_Qty_Col.values[i] + Last_4_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_5_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_5_Month_Qty_Col,Is_Over_Col
	#181-210
	def using_pandas_vectorization_qty_last6Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Last_4_Month_Qty_Col,Last_5_Month_Qty_Col,Val_Date,Last_6_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-210+interval),Val_Date+ Day(-181+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i] + Last_3_Month_Qty_Col.values[i] + Last_4_Month_Qty_Col.values[i] + Last_5_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_6_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_6_Month_Qty_Col,Is_Over_Col
	#211-240
	def using_pandas_vectorization_qty_last7Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Last_4_Month_Qty_Col,Last_5_Month_Qty_Col,Last_6_Month_Qty_Col,Val_Date,Last_7_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-240+interval),Val_Date+ Day(-211+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i] + Last_3_Month_Qty_Col.values[i] + Last_4_Month_Qty_Col.values[i] + Last_5_Month_Qty_Col.values[i] + Last_6_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_7_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_7_Month_Qty_Col,Is_Over_Col
	#241-270
	def using_pandas_vectorization_qty_last8Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Last_4_Month_Qty_Col,Last_5_Month_Qty_Col,Last_6_Month_Qty_Col,Last_7_Month_Qty_Col,Val_Date,Last_8_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-270+interval),Val_Date+ Day(-241+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i] + Last_3_Month_Qty_Col.values[i] + Last_4_Month_Qty_Col.values[i] + Last_5_Month_Qty_Col.values[i] + Last_6_Month_Qty_Col.values[i] + Last_7_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_8_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_8_Month_Qty_Col,Is_Over_Col
	#271-300
	def using_pandas_vectorization_qty_last9Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Last_4_Month_Qty_Col,Last_5_Month_Qty_Col,Last_6_Month_Qty_Col,Last_7_Month_Qty_Col,Last_8_Month_Qty_Col,Val_Date,Last_9_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-300+interval),Val_Date+ Day(-271+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i] + Last_3_Month_Qty_Col.values[i] + Last_4_Month_Qty_Col.values[i] + Last_5_Month_Qty_Col.values[i] + Last_6_Month_Qty_Col.values[i] + Last_7_Month_Qty_Col.values[i] + Last_8_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_9_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_9_Month_Qty_Col,Is_Over_Col
	#301-330
	def using_pandas_vectorization_qty_last10Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Last_4_Month_Qty_Col,Last_5_Month_Qty_Col,Last_6_Month_Qty_Col,Last_7_Month_Qty_Col,Last_8_Month_Qty_Col,Last_9_Month_Qty_Col,Val_Date,Last_10_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-330+interval),Val_Date+ Day(-301+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i] + Last_3_Month_Qty_Col.values[i] + Last_4_Month_Qty_Col.values[i] + Last_5_Month_Qty_Col.values[i] + Last_6_Month_Qty_Col.values[i] + Last_7_Month_Qty_Col.values[i] + Last_8_Month_Qty_Col.values[i] + Last_9_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_10_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_10_Month_Qty_Col,Is_Over_Col
	#331-360
	def using_pandas_vectorization_qty_last11Month(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Last_4_Month_Qty_Col,Last_5_Month_Qty_Col,Last_6_Month_Qty_Col,Last_7_Month_Qty_Col,Last_8_Month_Qty_Col,Last_9_Month_Qty_Col,Last_10_Month_Qty_Col,Val_Date,Last_11_Month_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate>='{2}' AND m_transaction.movementdate<='{3}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-360+interval),Val_Date+ Day(-331+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i] + Last_3_Month_Qty_Col.values[i] + Last_4_Month_Qty_Col.values[i] + Last_5_Month_Qty_Col.values[i] + Last_6_Month_Qty_Col.values[i] + Last_7_Month_Qty_Col.values[i] + Last_8_Month_Qty_Col.values[i] + Last_9_Month_Qty_Col.values[i] + Last_10_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Last_11_Month_Qty_Col.at[i]=round(This_Time_Qty,2)
		
		return Last_11_Month_Qty_Col,Is_Over_Col
	#>361
	def using_pandas_vectorization_qty_over1Year(self,Item_No_Col,Locator_Col,On_Hand_Qty_Col,This_Month_Qty_Col,Last_1_Month_Qty_Col,Last_2_Month_Qty_Col,Last_3_Month_Qty_Col,Last_4_Month_Qty_Col,Last_5_Month_Qty_Col,Last_6_Month_Qty_Col,Last_7_Month_Qty_Col,Last_8_Month_Qty_Col,Last_9_Month_Qty_Col,Last_10_Month_Qty_Col,Last_11_Month_Qty_Col,Val_Date,Over_1_Year_Qty_Col,Is_Over_Col):
		for i in range(0,len(Item_No_Col)):
			This_Time_Qty = 0
			if Is_Over_Col.values[i] == "True":
				pass
			else:
				sqlCmd = """SELECT 
								m_transaction.m_transaction_id AS "Transaction_ID",
								m_transaction.updated AS "Update_Day", 
								ad_org.name AS "Organization",
								m_locator.value AS "Locator",
								m_product.name AS "Product_Name",
								m_transaction.movementtype AS "Movement_Type", 
								m_transaction.movementdate AS "Movement_Date", 
								m_transaction.movementqty AS "Movenment_Quantity"
							FROM 
								adempiere.m_transaction, 
								adempiere.m_product, 
								adempiere.m_locator,
								adempiere.ad_org
							WHERE 
								ad_org.ad_org_id = m_transaction.ad_org_id AND
								m_product.m_product_id = m_transaction.m_product_id AND
								m_locator.m_locator_id = m_transaction.m_locator_id AND
								m_product.name = '{0}' AND m_locator.value = '{1}' AND
								m_transaction.movementtype in ('V+','P+','M+','I+','C+') AND
								m_transaction.movementdate<='{2}'
							ORDER BY "Movement_Date" desc, "Transaction_ID" desc;""".format(Item_No_Col.values[i],Locator_Col.values[i],Val_Date + Day(-361+interval))
				Transaction_df = pd.read_sql_query(sqlCmd, conn)
				if not Transaction_df.empty:
					OnHandQuantity = float(str(On_Hand_Qty_Col.values[i]).replace(',', ''))-(This_Month_Qty_Col.values[i]+ Last_1_Month_Qty_Col.values[i]+ Last_2_Month_Qty_Col.values[i]+Last_3_Month_Qty_Col.values[i]+Last_4_Month_Qty_Col.values[i]+Last_5_Month_Qty_Col.values[i]+Last_6_Month_Qty_Col.values[i]+Last_7_Month_Qty_Col.values[i]+Last_8_Month_Qty_Col.values[i]+Last_9_Month_Qty_Col.values[i]+Last_10_Month_Qty_Col.values[i]+Last_11_Month_Qty_Col.values[i])
					sum_all = Transaction_df['Movenment_Quantity'].sum()
					if sum_all < OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
					elif sum_all == OnHandQuantity:
						This_Time_Qty = sum_all
						globals()['temp_transaction_df'].append(Transaction_df)	
						RawData_Obj.data['Is_Over'].values[i] = "True"
					else:
						This_Time_Qty = OnHandQuantity
						RawData_Obj.data['Is_Over'].values[i] = "True"
						count = len(Transaction_df.index)
						if count >=2:
							total = sum_all
							for row_Transaction in Transaction_df[::-1].values:
								count-= 1
								total-= row_Transaction[7]
								if total < OnHandQuantity:
									globals()['temp_transaction_df'].append(Transaction_df[0:count])
									row_Transaction_tmp = Transaction_df.iloc[count]
									row_Transaction_tmp['Movenment_Quantity'] = OnHandQuantity - Transaction_df['Movenment_Quantity'][0:count].sum()
									globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
									break
						else:
							Transaction_df['Movenment_Quantity'] = OnHandQuantity
							globals()['temp_transaction_df'].append(Transaction_df)
						# for row_Transaction in Transaction_df.values:
						# 	This_Time_Qty_Previous=This_Time_Qty
						# 	This_Time_Qty+=  row_Transaction[7]
						# 	if (This_Time_Qty) == OnHandQuantity:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	elif (This_Time_Qty) > OnHandQuantity:
						# 		This_Time_Qty = OnHandQuantity
						# 		row_Transaction_tmp=row_Transaction
						# 		row_Transaction_tmp[7]=OnHandQuantity-This_Time_Qty_Previous
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction_tmp], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))								
						# 		Is_Over_Col.values[i] = "True"
						# 		break
						# 	else:
						# 		globals()['temp_transaction_df'].append(pd.DataFrame([row_Transaction], columns = ['Transaction_ID','Update_Day','Organization','Locator','Product_Name','Movement_Type','Movement_Date','Movenment_Quantity']))	
			Over_1_Year_Qty_Col.at[i]=round(This_Time_Qty,2)
		return Over_1_Year_Qty_Col,Is_Over_Col
	# amount
	def using_pandas_vectorization_amount(self,Qty_Col):
		return (Qty_Col*RawData_Obj.data['Moving_Average_Price'])

##################################################################################################################
class SearchAdvanceDialog(QDialog):
	def __init__(self, parent=None):
		super().__init__(parent)
		self.setFixedWidth(350)
		self.setFixedHeight(100)
		self.setWindowTitle("Search Advance")
		self.setWindowFlag(Qt.WindowContextHelpButtonHint, False)  
		self.setStyleSheet("background-color: rgb(255, 255, 255);")

		self.Organization_Label = QLabel('Organization')
		self.Item_Category_Label = QLabel('Item_Category')
		self.Organization_ComboBox = My_Custom_Class.CheckableComboBox()
		self.Organization_ComboBox.setObjectName("Organization_ComboBox")
		self.Item_Category_ComboBox = My_Custom_Class.CheckableComboBox()
		self.Item_Category_ComboBox.setObjectName("Item_Category_ComboBox")

		grid = QGridLayout()

		grid.addWidget(self.Organization_Label, 0, 0)
		grid.addWidget(self.Organization_ComboBox, 0, 1)
		grid.addWidget(self.Item_Category_Label, 1, 0)
		grid.addWidget(self.Item_Category_ComboBox, 1, 1)

		self.SaveButton = QPushButton(self)
		self.SaveButton.setObjectName("SaveButton")
		self.SaveButton.setText("OK")
		self.SaveButton.setStyleSheet("""QPushButton#SaveButton {
										color: rgb(0, 0, 0);
										background-color: rgb(255, 255, 255);
											border-color: rgb(0, 0, 0);
										border-style: solid;
										border-width: 1px;
										border-radius: 5px;
										}

										QPushButton#SaveButton:pressed {
										padding-left:5px;
										padding-top:5px;
										}""")
		self.SaveButton.setFixedSize(QSize(91,21))
		self.SaveButton.setCursor(QCursor(Qt.PointingHandCursor))
		grid.addWidget(self.SaveButton, 2, 0, 1, 2, Qt.AlignCenter)
		self.setLayout(grid)  
		self.InitialValue()
		self.SignalSlot()
		
	def InitialValue(self):
		if "All" not in LIST_CATEGORY:
			LIST_CATEGORY.insert(0,"All")
		if "All" not in LIST_ORGANIZATION:
			LIST_ORGANIZATION.insert(0,"All")
		self.Organization_ComboBox.clear()
		self.Item_Category_ComboBox.clear()

		for index, item_name in enumerate(LIST_ORGANIZATION):
			self.Organization_ComboBox.addItem(str(item_name))
			item = self.Organization_ComboBox.model().item(index, 0)
			if item_name in MULTI_CHECK_ORGANIZATION:
				item.setCheckState(Qt.Checked)
			else:
				item.setCheckState(Qt.Unchecked)

		for index, item_name in enumerate(LIST_CATEGORY):
			self.Item_Category_ComboBox.addItem(str(item_name))
			item = self.Item_Category_ComboBox.model().item(index, 0)
			if item_name in MULTI_CHECK_CATEGORY:
				item.setCheckState(Qt.Checked)
			else:
				item.setCheckState(Qt.Unchecked)

	def SignalSlot(self):
		self.SaveButton.clicked.connect(self.SearchAdvance)
			
	def SearchAdvance(self):
		Organization = self.Organization_ComboBox.check_items()
		Category = self.Item_Category_ComboBox.check_items()
		globals()['MULTI_CHECK_ORGANIZATION'] = Organization
		globals()['MULTI_CHECK_CATEGORY'] = Category
		if (Organization == ["All"] or Organization == []) and (Category == ["All"] or Category == []):
			globals()['HAS_FILTER'] = False
			RawDataPrint_Obj.data = RawData_Obj.data
			pass
		elif (Organization == ["All"]  or Organization == []) and Category!= ["All"]:
			globals()['HAS_FILTER'] = True
			RawDataFilter_Obj.data = RawData_Obj.data[(RawData_Obj.data['Item_Category'].isin(Category))]
			if Tool_Instance.SearchItemlineEdit.text().replace(" ","") == "":
				RawDataPrint_Obj.data = RawDataFilter_Obj.data
				model = My_Custom_Class.PandasModel(RawDataFilter_Obj.data,"ResultReport")
				Tool_Instance.ResultReport.setModel(model)
				selectionModel = Tool_Instance.ResultReport.selectionModel() 
				selectionModel.selectionChanged.connect(Tool_Instance.selection_changed)
			Tool_Instance.SearchItemlineEdit.setFocus()
		elif Organization!= ["All"] and (Category == ["All"] or Category == []):
			globals()['HAS_FILTER'] = True
			RawDataFilter_Obj.data = RawData_Obj.data[(RawData_Obj.data['Organization'].isin(Organization))]
			if Tool_Instance.SearchItemlineEdit.text().replace(" ","") == "":
				RawDataPrint_Obj.data = RawDataFilter_Obj.data
				model = My_Custom_Class.PandasModel(RawDataFilter_Obj.data,"ResultReport")
				Tool_Instance.ResultReport.setModel(model)
				selectionModel = Tool_Instance.ResultReport.selectionModel() 
				selectionModel.selectionChanged.connect(Tool_Instance.selection_changed)
			Tool_Instance.SearchItemlineEdit.setFocus()
		else:
			globals()['HAS_FILTER'] = True
			RawDataFilter_Obj.data = RawData_Obj.data[(RawData_Obj.data['Organization'].isin(Organization))&(RawData_Obj.data['Item_Category'].isin(Category))]
			if Tool_Instance.SearchItemlineEdit.text().replace(" ","") == "":
				RawDataPrint_Obj.data = RawDataFilter_Obj.data
				model = My_Custom_Class.PandasModel(RawDataFilter_Obj.data,"ResultReport")
				Tool_Instance.ResultReport.setModel(model)
				selectionModel = Tool_Instance.ResultReport.selectionModel() 
				selectionModel.selectionChanged.connect(Tool_Instance.selection_changed)
			Tool_Instance.SearchItemlineEdit.setFocus()
		self.close()
		Tool_Instance.SearchItem()
##################################################################################################################
class Tool(QtWidgets.QMainWindow):
	def __init__(self):
		super(Tool,self).__init__()
		print("Phương thức khởi tạo của đối tượng Tool đã được gọi!")
		self.setAttribute(PyQt5.QtCore.Qt.WA_DeleteOnClose, True)
		fileh = PyQt5.QtCore.QFile(':/ui/ui/gui_reposnsive.ui')
		fileh.open(PyQt5.QtCore.QFile.ReadOnly)
		uic.loadUi(fileh, self)
		fileh.close()
		self.SignalSlot()
		global Items, Locators, Val_Date, idx, listType
		self.Items,self.Locators, self.listType = [],[],[]
		self.RunButton.setEnabled(False)
		self.GetTransactionButton.setEnabled(False)
		self.ExportExcelButton.setEnabled(False)
		self.SearchItemButton.setEnabled(False)
		self.ClearButton.setEnabled(False)
		self.AdvanceSearchItemButton.setEnabled(False)
		self.progressBar.setValue(0)
		self.progressBar.setCursor(PyQt5.QtCore.Qt.ArrowCursor)
		timer = QTimer(self)
		timer.timeout.connect(self.showTime)
		timer.start(1000) 
		self.showTime()

	def __del__(self):
		print("Phương thức hủy của đối tượng Tool đã được gọi!")
	
	def SignalSlot(self):
		self.ChooseFileButton.clicked.connect(self.ChooseFile)
		self.RunButton.clicked.connect(self.Run)
		self.SearchItemButton.clicked.connect(self.SearchItem)
		self.ClearButton.clicked.connect(self.Clear)
		self.GetTransactionButton.clicked.connect(self.GetTransaction)
		self.ExportExcelButton.clicked.connect(self.ExportExcel)
		self.AdvanceSearchItemButton.clicked.connect(self.AdvanceSearch)

	def closeEvent(self, event):
		respond=self.MessageBoxOKCancel("Confirm","Are you sure to exit the program?")
		if respond==QMessageBox.Yes:
			event.accept()
		else:
			event.ignore()

	def ChooseFile(self):
		globals()['temp_transaction_df'] = []
		self.progressBar.setValue(0)
		self.progress.setText("WAITING FOR THE INVENTORY FILE...")
		self.progressBar.setCursor(PyQt5.QtCore.Qt.ArrowCursor)
		defaultfolder='C:\\Users\\{0}\\Downloads'.format(os.getlogin())
		global filename
		filename = QtWidgets.QFileDialog.getOpenFileName(self, "Please choose a excel file", defaultfolder, "Excel files (*.csv *xls)")
		if not filename[0]=="":
			self.RunButton.setEnabled(True)
			self.GetTransactionButton.setEnabled(False)
			self.ExportExcelButton.setEnabled(False)
			self.progress.setText("{}".format(filename[0]))
			self.SearchItemlineEdit.setText("")
			self.ResultReport.setModel(None)
			self.TransactionDetail.setModel(None)
		else:
			self.RunButton.setEnabled(False)
	
	def Run(self):
		self.progressBar.setCursor(PyQt5.QtCore.Qt.WaitCursor)
		self.ExportExcelButton.setEnabled(False)
		self.RunButton.setEnabled(False)
		self.ChooseFileButton.setEnabled(False)
		self.GetTransactionButton.setEnabled(False)
		self.SearchItemButton.setEnabled(False)
		self.ClearButton.setEnabled(False)
		self.AdvanceSearchItemButton.setEnabled(False)
		
		self.tmr = SubThread(self)
		self.tmr.updated.connect(self.updateValue)
		self.tmr.start()

	def updateValue(self, data):
		match data:
			case 1:
				self.progress.setText("PROGRESSING...(1/15 TASKS):Reading Excel file, please wait!")
				self.progressBar.setValue(0)
			case 2:
				self.progress.setText("PROGRESSING...(2/15 TASKS):Caculating for 1-30 days ago, please wait!")
				self.progressBar.setValue(int(100*1/15))
			case 3:
				self.progress.setText("PROGRESSING...(3/15 TASKS):Caculating for 31-60 days ago, please wait!")
				self.progressBar.setValue(int(100*2/15))
			case 4:
				self.progress.setText("PROGRESSING...(4/15 TASKS):Caculating for 61-90 days ago, please wait!")
				self.progressBar.setValue(int(100*3/15))
			case 5:
				self.progress.setText("PROGRESSING...(5/15 TASKS):Caculating for 91-120 days ago, please wait!")
				self.progressBar.setValue(int(100*4/15))
			case 6:
				self.progress.setText("PROGRESSING...(6/15 TASKS):Caculating for 121-150 days ago, please wait!")
				self.progressBar.setValue(int(100*5/15))
			case 7:
				self.progress.setText("PROGRESSING...(7/15 TASKS):Caculating for 151-180 days ago, please wait!")
				self.progressBar.setValue(int(100*6/15))
			case 8:
				self.progress.setText("PROGRESSING...(8/15 TASKS):Caculating for 181-210 days ago, please wait!")
				self.progressBar.setValue(int(100*7/15))
			case 9:
				self.progress.setText("PROGRESSING...(9/15 TASKS):Caculating for 211-240 days ago, please wait!")
				self.progressBar.setValue(int(100*8/15))
			case 10:
				self.progress.setText("PROGRESSING...(10/15 TASKS):Caculating for 241-270 days ago, please wait!")
				self.progressBar.setValue(int(100*9/15))
			case 11:
				self.progress.setText("PROGRESSING...(11/15 TASKS):Caculating for 271-300 days ago, please wait!")
				self.progressBar.setValue(int(100*10/15))
			case 12:
				self.progress.setText("PROGRESSING...(12/15 TASKS):Caculating for 301-330 days ago, please wait!")
				self.progressBar.setValue(int(100*11/15))
			case 13:
				self.progress.setText("PROGRESSING...(13/15 TASKS):Caculating for 331-360 days ago, please wait!")
				self.progressBar.setValue(int(100*12/15))
			case 14:
				self.progress.setText("PROGRESSING...(14/15 TASKS):Caculating for over 361 days ago, please wait!")
				self.progressBar.setValue(int(100*13/15))
			case 15:
				self.progress.setText("PROGRESSING...(14/15 TASKS):Preparing for display, please wait!")
				self.progressBar.setValue(int(100*14/15))
			case 16:
				self.tmr.stop()
				List_Item_Obj = DataFrame("List_Item_Obj",RawData_Obj.data['Item_No'].drop_duplicates(keep = False, inplace = False)) 
				LIST_ITEM = List_Item_Obj.data.values.tolist()
				List_Organization_Obj = DataFrame("List_Organization_Obj", RawData_Obj.data['Organization'].drop_duplicates(inplace = False))
				
				globals()['LIST_ORGANIZATION'].clear()
				globals()['LIST_CATEGORY'].clear()
				globals()['LIST_ORGANIZATION'] = List_Organization_Obj.data.values.tolist()
				List_Category_Obj = DataFrame("List_Category_Obj", RawData_Obj.data['Item_Category'].drop_duplicates(inplace = False))
				globals()['LIST_CATEGORY'] = List_Category_Obj.data.values.tolist()
				RawDataPrint_Obj.data = RawData_Obj.data
				
				self.SearchItemlineEdit.setCompleter(My_Custom_Class.Completer(LIST_ITEM))
				self.progressBar.setCursor(PyQt5.QtCore.Qt.ArrowCursor)
				self.ExportExcelButton.setEnabled(True)
				self.ChooseFileButton.setEnabled(True)
				self.GetTransactionButton.setEnabled(True)
				self.SearchItemButton.setEnabled(True)
				self.ClearButton.setEnabled(True)
				self.AdvanceSearchItemButton.setEnabled(True)
				All_Transaction_Obj.data = pd.concat(globals()['temp_transaction_df'],ignore_index=True)
				All_Transaction_Obj.data = All_Transaction_Obj.data[All_Transaction_Obj.data['Movenment_Quantity']!=0]
				globals()['temp_transaction_df'] = []
				All_Transaction_Obj.reduce_mem_usage(All_Transaction_Obj.data)
				self.TransactionDetail.setModel(My_Custom_Class.PandasModel(All_Transaction_Obj.data,"TransactionDetail"))
				self.SearchItemlineEdit.setFocus()		
				self.progress.setText("PROGRESS FINISH IN ABOUT {} MINUTES!".format(round((end_time - start_time)/60,2)))
				self.progressBar.setValue(100)
				self.MessageBoxOK("Information","Announce","Progress finished!"+"The whole process took about {} minutes".format(round((end_time - start_time)/60,2)))
				gc.collect()
	
	def updateValue_Transaction(self,data):
		self.progressBar.setValue(data)
		self.progress.setText("PROGRESSING..., please wait!")
		if data == 100:
			self.tmr.stop()
			self.TransactionDetail.setModel(My_Custom_Class.PandasModel(Select_Transaction_Obj.data,"TransactionDetail"))
			self.TransactionDetail.resizeColumnsToContents()
			self.progress.setText("PROGRESSING FINISHED!")

	def updateValue_Export(self,data):
		match data:
			case 1:
				self.progress.setText("PROGRESSING...(1/3 TASKS):Exporting sheet Report, please wait!")
				self.progressBar.setValue(0)
			case 2:
				self.progress.setText("PROGRESSING...(2/3 TASKS):Exporting sheet Transaction, please wait!")
				self.progressBar.setValue(int(100*1/3))
			case 3:
				self.progress.setText("PROGRESSING...(3/3 TASKS):Merging sheets, please wait!")
				self.progressBar.setValue(int(100*2/3))
			case 4:
				self.tmr.stop()
				self.progress.setText("PROGRESSING FINISHED!")
				self.progressBar.setValue(100)
				respond = self.MessageBoxOKCancel("Announce","Export excel successfully in folder {}.\nDo you want to open it right now?".format(os.path.dirname(filename[0])))
				if respond == QMessageBox.Yes:	
					os.system('"start ' + report_file + '"')

	def SearchItem(self):
		SearchText = self.SearchItemlineEdit.text().replace(" ","")
		match HAS_FILTER:
			case False:
				match SearchText:
					case "":
						self.SearchItemlineEdit.setFocus()
						RawDataPrint_Obj.data = RawData_Obj.data
					case _:
						List_Item = SearchText.split(",")
						Search_Item_Obj = DataFrame("Search_Item_Obj",pd.DataFrame())
						for each_item in List_Item:
							Search_Absolute_Each_Item_Obj = DataFrame("Search_Absolute_Each_Item_Obj",RawData_Obj.data[(RawData_Obj.data['Item_No'] == each_item)])
							if Search_Absolute_Each_Item_Obj.data.empty:
								Search_Relative_Each_Item_Obj = DataFrame("Search_Relative_Each_Item_Obj",RawData_Obj.data[RawData_Obj.data['Item_No'].str.startswith(each_item.upper())])
								if not Search_Relative_Each_Item_Obj.data.empty:
									Search_Item_Obj.data = pd.concat([Search_Item_Obj.data,Search_Relative_Each_Item_Obj.data])
							else:
								Search_Item_Obj.data = pd.concat([Search_Item_Obj.data,Search_Absolute_Each_Item_Obj.data])
						if Search_Item_Obj.data.empty:
							self.MessageBoxOK("Information","Result","Not found with item name {}!".format(SearchText))
							self.progress.setText("CHECK FILTER OR SEARCH ANOTHER ITEM AGAIN!")
							self.SearchItemlineEdit.setText("")
							self.SearchItemlineEdit.setFocus()
							model = My_Custom_Class.PandasModel(RawData_Obj.data,"ResultReport")
							RawDataPrint_Obj.data = RawData_Obj.data
						else:
							model = My_Custom_Class.PandasModel(Search_Item_Obj.data,"ResultReport")
							self.MessageBoxOK("Information","Result","{} records were found!".format(len(Search_Item_Obj.data)))
							if len(Search_Item_Obj.data) >=2:
								self.progress.setText("{} RECORDS WERE FOUND!".format(len(Search_Item_Obj.data)))
							else:
								self.progress.setText("{} RECORD WERE FOUND!".format(len(Search_Item_Obj.data)))
							RawDataPrint_Obj.data = Search_Item_Obj.data
							del Search_Item_Obj
						self.ResultReport.setModel(model)
						selectionModel = Tool_Instance.ResultReport.selectionModel() 
						selectionModel.selectionChanged.connect(Tool_Instance.selection_changed)
			case True:
				match SearchText:
					case "":
						if len(RawDataFilter_Obj.data) >=2:
							self.progress.setText("{} RECORDS WERE FILTERED!".format(len(RawDataFilter_Obj.data)))
						else:
							self.progress.setText("{} RECORD WERE FILTERED!".format(len(RawDataFilter_Obj.data)))
						self.SearchItemlineEdit.setFocus()
					case _:
						List_Item = SearchText.split(",")
						Search_Item_Obj = DataFrame("Search_Item_Obj",pd.DataFrame())
						for each_item in List_Item:
							Search_Absolute_Each_Item_Obj = DataFrame("Search_Absolute_Each_Item_Obj",RawDataFilter_Obj.data[(RawDataFilter_Obj.data['Item_No'] == each_item)])
							if Search_Absolute_Each_Item_Obj.data.empty:
								Search_Relative_Each_Item_Obj = DataFrame("Search_Relative_Each_Item_Obj",RawDataFilter_Obj.data[RawDataFilter_Obj.data['Item_No'].str.startswith(each_item.upper())])
								if not Search_Relative_Each_Item_Obj.data.empty:
									Search_Item_Obj.data = pd.concat([Search_Item_Obj.data,Search_Relative_Each_Item_Obj.data])
							else:
								Search_Item_Obj.data = pd.concat([Search_Item_Obj.data,Search_Absolute_Each_Item_Obj.data])
						if Search_Item_Obj.data.empty:
							self.MessageBoxOK("Information","Result","Not found with item name {}!".format(SearchText))
							self.progress.setText("CHECK FILTER OR SEARCH ANOTHER ITEM AGAIN!")
							self.SearchItemlineEdit.setText("")
							self.SearchItemlineEdit.setFocus()
							model = My_Custom_Class.PandasModel(RawData_Obj.data,"ResultReport")
							RawDataPrint_Obj.data = RawData_Obj.data
						else:
							model = My_Custom_Class.PandasModel(Search_Item_Obj.data,"ResultReport")
							self.MessageBoxOK("Information","Result","{} records were found!".format(len(Search_Item_Obj.data)))
							if len(Search_Item_Obj.data) >=2:
								self.progress.setText("{} RECORDS WERE FOUND!".format(len(Search_Item_Obj.data)))
							else:
								self.progress.setText("{} RECORD WERE FOUND!".format(len(Search_Item_Obj.data)))
							RawDataPrint_Obj.data = Search_Item_Obj.data
							del Search_Item_Obj
						self.ResultReport.setModel(model)
						selectionModel = Tool_Instance.ResultReport.selectionModel() 
						selectionModel.selectionChanged.connect(Tool_Instance.selection_changed)
	
	def Clear(self):
		self.TransactionDetail.setModel(My_Custom_Class.PandasModel(All_Transaction_Obj.data,"TransactionDetail"))
		self.TransactionDetail.setModel(None)
		self.ResultReport.setModel(My_Custom_Class.PandasModel(RawData_Obj.data,"ResultReport"))
		selectionModel = Tool_Instance.ResultReport.selectionModel() 
		selectionModel.selectionChanged.connect(Tool_Instance.selection_changed)

		self.SearchItemlineEdit.setText("")
		self.SearchItemlineEdit.setFocus()
		self.Items, self.Locators = [],[]

		globals()['HAS_FILTER'] = False
		globals()['MULTI_CHECK_ORGANIZATION'] = []
		globals()['MULTI_CHECK_CATEGORY'] = []
		RawDataFilter_Obj.data = pd.DataFrame()
		Select_Transaction_Obj.data = pd.DataFrame()
		RawDataPrint_Obj.data = RawData_Obj.data
		self.progress.setText("ALL STATES WERE CLEARED!")
		gc.collect()
	
	def selection_changed(self):
		self.Items,self.Locators = [],[]
		for idx in self.ResultReport.selectionModel().selectedRows():
			self.Items.append(idx.sibling(idx.row(),7).data())
			self.Locators.append(idx.sibling(idx.row(),5).data())
	
	def select_column(self):
		for idx in self.TransactionDetail.selectionModel().selectedColumns():
			if idx.column() == 7:
				indexList = self.TransactionDetail.selectionModel().selectedIndexes()
				sum = 0
				for idx in indexList:
					sum+=float(idx.sibling(idx.row(),7).data())
				self.label.setText("INVENTORY: {}".format(sum))
			else:
				self.label.setText("ALL TRANSACTION DETAIL")
				
	def GetTransaction(self):
		if self.tabWidget.currentIndex() == 0:
			match len(self.Items):
				case 0:
					self.MessageBoxOK("Critical","Error","Please select at least one item!")
					self.SearchItemlineEdit.setFocus()
				case _:
					self.TransactionDetail.setModel(None)
					self.tabWidget.setCurrentIndex(1)
					self.tmr = SubThread_Transaction(self)
					self.tmr.updated.connect(self.updateValue_Transaction)
					self.tmr.start()
	
	def showTime(self):
		currentTime = QDateTime.currentDateTime()
		displayTxt = currentTime.toString('dd/MM/yyyy hh:mm:ss')
		self.clock.setText(displayTxt)
	
	def ExportExcel(self):
		folderName = os.path.dirname(filename[0])
		template_file = os.getcwd()+ r'\inventory_report_template.xlsx'
		global report_file
		report_file = folderName+r'\inventory_report_version_5.2_' + time.strftime("%Y-%m-%d-%H-%M-%S.xlsx")
		shutil.copy(template_file, report_file)
		respond = self.MessageBoxOKCancel("Your choice","Do you want to export with Transaction sheet?\nIt will take more time depending on the number of items you've selected!".format(folderName))
		match respond:
			case QMessageBox.Yes:
				self.tmr = SubThread_Export(self)
				self.tmr.updated.connect(self.updateValue_Export)
				self.tmr.start()
			case QMessageBox.No:
				with pd.ExcelWriter(report_file,mode="a",engine="openpyxl",if_sheet_exists="overlay") as writer: 
					RawDataPrint_Obj.data.to_excel(writer, sheet_name="Report",header=None, startcol=0,startrow=2,index=False)
				respond = self.MessageBoxOKCancel("Announce","Export excel successfully in folder {}.\nDo you want to open it right now?".format(folderName))
				if respond == QMessageBox.Yes:	
					os.system('"start ' + report_file + '"')
		self.Clear()
	
	def AdvanceSearch(self):
		SearchAdvancedlg = SearchAdvanceDialog(self)
		SearchAdvancedlg.exec()

	def MessageBoxOK(self,icon,flat,string):
		msg = QMessageBox()
		match icon:
			case "Information":
				msg.setIcon(QMessageBox.Information)
			case "Warning":
				msg.setIcon(QMessageBox.Warning)
			case "Critical":
				msg.setIcon(QMessageBox.Critical)
		font = QFont()
		font.setFamily("Yu Mincho")
		font.setPointSize(13)
		msg.setFont(font)
		msg.setText(string)
		msg.setStandardButtons(QMessageBox.Ok)
		msg.setWindowTitle(flat)
		QTimer.singleShot(5000, lambda : msg.done(0))
		msg.exec_()

	def MessageBoxOKCancel(self,flat,string):
		msg = QMessageBox()
		msg.setIcon(QMessageBox.Question)
		font = QFont()
		font.setFamily("Yu Mincho")
		font.setPointSize(13)
		msg.setFont(font)
		msg.setText(string)
		msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
		msg.setWindowTitle(flat)
		return msg.exec_()

	def centerWidgetOnScreen(self, widget):
		centerPoint = PyQt5.QtGui.QScreen.availableGeometry(QtWidgets.QApplication.primaryScreen()).center()
		fg = widget.frameGeometry()
		fg.moveCenter(centerPoint)
		widget.move(fg.topLeft())

	def event(self, e):
		if e.type() == e.WindowStateChange or e.type() == PyQt5.QtCore.QEvent.WindowDeactivate:
			# Lỗi khi kéo qua màn hình thứ 2
			#self.centerWidgetOnScreen(self)
			pass
		return super(QtWidgets.QMainWindow, self).event(e)
##################################################################################################################
##################################################################################################################
if __name__ == "__main__":
	app=QApplication(sys.argv)
	app.setWindowIcon(PyQt5.QtGui.QIcon(':icon/icon/inventory.ico'))
	interval =1
	RawData_Obj = DataFrame("RawData_Obj",pd.DataFrame())
	RawDataFilter_Obj = DataFrame("RawDataFilter_Obj",pd.DataFrame())
	RawDataPrint_Obj = DataFrame("RawDataPrint_Obj", pd.DataFrame())
	All_Transaction_Obj = DataFrame("All_Transaction_Obj",pd.DataFrame())
	Select_Transaction_Obj = DataFrame("Select_Transaction_Obj",pd.DataFrame())
	LIST_ITEM = []
	global LIST_ORGANIZATION, LIST_CATEGORY, temp_transaction_df, MULTI_CHECK_ORGANIZATION, MULTI_CHECK_CATEGORY
	LIST_ORGANIZATION, LIST_CATEGORY, temp_transaction_df, MULTI_CHECK_ORGANIZATION, MULTI_CHECK_CATEGORY = [], [], [], [], []
	HAS_FILTER = False
	Tool_Instance=Tool()
	Tool_Instance.setWindowTitle("AGING REPORT Version 5.2")
	Tool_Instance.show()
	try:
		conn = psycopg2.connect(database='lpa.41.0_smv_2023_02_10',user='postgres',password='postgres',host='localhost',port='5432')
	except Exception as e:
		Tool_Instance.MessageBoxOK("Critical","Error",str(e))
		Tool_Instance.deleteLater()
	try:
		app.closeEvent = lambda event:Tool.closeEvent(event)
		sys.exit(app.exec_())
	except SystemExit:
		print("đang thoát chương trình...")