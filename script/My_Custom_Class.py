from PyQt5.QtWidgets import QComboBox, QStylePainter, QStyleOptionComboBox, QStyle, QCompleter
from PyQt5.QtGui import QStandardItemModel, QPalette, QBrush, QColor
from PyQt5.QtCore import Qt, QAbstractTableModel, QModelIndex
import pandas as pd

class CheckableComboBox(QComboBox):
	def __init__(self):
		super(CheckableComboBox, self).__init__()
		self.view().pressed.connect(self.handle_item_pressed)
		self.setModel(QStandardItemModel(self))	

	def handle_item_pressed(self, index):
		item = self.model().itemFromIndex(index)
		if item.checkState() == Qt.Checked:
			item.setCheckState(Qt.Unchecked)
		else:
			if self.item_checked(0):
				self.model().item(0, 0).setCheckState(Qt.Unchecked)
			item.setCheckState(Qt.Checked)

	def item_checked(self, index):
		item = self.model().item(index, 0)
		return item.checkState() == Qt.Checked

	def check_items(self):
		checkedItems = []
		for i in range(self.count()):
			if self.item_checked(i):
				if i ==0:
					checkedItems.append(self.model().item(i, 0).text())
					for i_exclude in range(1,self.count()):
						item = self.model().item(i_exclude, 0)
						if item.checkState() == Qt.Checked:
							item.setCheckState(Qt.Unchecked)
				else:
					checkedItems.append(self.model().item(i, 0).text())
		return checkedItems

	def paintEvent(self, event):
		painter = QStylePainter(self)
		painter.setPen(self.palette().color(QPalette.Text))
		opt = QStyleOptionComboBox()
		self.initStyleOption(opt)
		opt.currentText = ",".join(self.check_items())
		painter.drawComplexControl(QStyle.CC_ComboBox, opt)
		painter.drawControl(QStyle.CE_ComboBoxLabel, opt)

class Completer(QCompleter):
	def __init__(self, *args, **kwargs):
		super(Completer, self).__init__(*args, **kwargs)

		self.setCaseSensitivity(Qt.CaseInsensitive)
		self.setCompletionMode(QCompleter.PopupCompletion)
		self.setWrapAround(False)

	def pathFromIndex(self, index):
		path = QCompleter.pathFromIndex(self, index)
		lst = str(self.widget().text()).split(',')
		if len(lst) > 1:
			path = '%s, %s' % (','.join(lst[:-1]), path)
		return path

	def splitPath(self, path):
		path = str(path.split(',')[-1]).lstrip(' ')
		return [path]

class PandasModel(QAbstractTableModel):

	def __init__(self, dataframe: pd.DataFrame, name,parent=None):
		QAbstractTableModel.__init__(self, parent)
		self._dataframe = dataframe
		self.name = name

	def rowCount(self, parent=QModelIndex()) -> int:
		if parent == QModelIndex():
			return len(self._dataframe)

		return 0

	def columnCount(self, parent=QModelIndex()) -> int:
		if parent == QModelIndex():
			return len(self._dataframe.columns)
		return 0

	def data(self, index: QModelIndex, role=Qt.ItemDataRole):
		if not index.isValid():
			return None

		if role == Qt.DisplayRole:
			return str(self._dataframe.iloc[index.row(), index.column()])
		
		elif role == Qt.BackgroundRole:
			if self.name == "ResultReport":
				if index.column() == 9:
					return QBrush(QColor(255, 192, 203))
				elif index.column() == 7:
					return QBrush(QColor(255, 228, 196)) 
				elif index.column() == 5:
					return QBrush(QColor(255, 255, 224))
				elif index.column() in [13,15,17,19,21,23,25,27,29,31,33,35,37] :
					return QBrush(QColor(127, 255, 212))

		# elif role == Qt.ForegroundRole:
		# 	if self._dataframe.iloc[index.row()][index.column()]!=0:
		# 		if index.column() == 25:
		# 			return QColor('red')
		# 		elif index.column() == 23 and self._dataframe.iloc[index.row()][25]==0:
		# 			return QColor('red')
		# 		elif index.column() == 21 and self._dataframe.iloc[index.row()][25]==0 and self._dataframe.iloc[index.row()][23]==0:
		# 			return QColor('red')
		# 		elif index.column() == 19 and self._dataframe.iloc[index.row()][25]==0 and self._dataframe.iloc[index.row()][23]==0 and self._dataframe.iloc[index.row()][21]==0:
		# 			return QColor('red')
		# 		elif index.column() == 17 and self._dataframe.iloc[index.row()][25]==0 and self._dataframe.iloc[index.row()][23]==0 and self._dataframe.iloc[index.row()][21]==0 and self._dataframe.iloc[index.row()][19]==0:
		# 			return QColor('red')
		# 		elif index.column() == 15 and self._dataframe.iloc[index.row()][25]==0 and self._dataframe.iloc[index.row()][23]==0 and self._dataframe.iloc[index.row()][21]==0 and self._dataframe.iloc[index.row()][19]==0 and self._dataframe.iloc[index.row()][17]==0:
		# 			return QColor('red')
		# 		elif index.column() == 13 and self._dataframe.iloc[index.row()][25]==0 and self._dataframe.iloc[index.row()][23]==0 and self._dataframe.iloc[index.row()][21]==0 and self._dataframe.iloc[index.row()][19]==0 and self._dataframe.iloc[index.row()][17]==0 and self._dataframe.iloc[index.row()][15]==0:
		# 			return QColor('red')
		return None

	def headerData(self, section: int, orientation: Qt.Orientation, role: Qt.ItemDataRole):
		if role == Qt.DisplayRole:
			if orientation == Qt.Horizontal:
				return str(self._dataframe.columns[section])

			if orientation == Qt.Vertical:
				return str(self._dataframe.index[section])

		return None