#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
设计：xmge
辅助AI：GLM-5-Turbo
支持网站：https://www.xmge.site 
Access → SQL Server 数据迁移工具
功能：字段类型映射、ID自增处理、断点续传、实时进度与日志
依赖安装：pip install pyodbc PyQt5
"""

import sys
import os
import json
import sqlite3
from datetime import datetime

import pyodbc
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QFormLayout, QGroupBox, QLabel, QLineEdit, QComboBox, QSpinBox,
    QPushButton, QRadioButton, QCheckBox, QListWidget, QListWidgetItem,
    QTableWidget, QTableWidgetItem, QHeaderView, QTabWidget, QTextEdit,
    QProgressBar, QFileDialog, QMessageBox, QDialog, QAbstractItemView,
    QSplitter, QSizePolicy
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QIcon

ACCESS_TO_SQL_DEFAULTS = {
    'COUNTER': 'INT', 'AUTOINCREMENT': 'INT',
    'INTEGER': 'INT', 'LONG': 'BIGINT', 'LONG INTEGER': 'BIGINT',
    'SMALLINT': 'SMALLINT', 'BYTE': 'TINYINT',
    'SINGLE': 'REAL', 'DOUBLE': 'FLOAT',
    'CURRENCY': 'MONEY', 'DECIMAL': 'DECIMAL(18,4)', 'NUMERIC': 'NUMERIC(18,4)',
    'DATETIME': 'DATETIME', 'DATE': 'DATE', 'TIME': 'TIME',
    'YESNO': 'BIT', 'BOOLEAN': 'BIT',
    'TEXT': 'NVARCHAR(255)', 'VARCHAR': 'NVARCHAR(255)',
    'CHAR': 'NCHAR(255)', 'MEMO': 'NVARCHAR(MAX)', 'LONGTEXT': 'NVARCHAR(MAX)',
    'OLEOBJECT': 'VARBINARY(MAX)', 'LONGBINARY': 'VARBINARY(MAX)',
    'GUID': 'UNIQUEIDENTIFIER', 'VARBINARY': 'VARBINARY(MAX)',
    'BINARY': 'BINARY(255)',
}

SQL_SERVER_TYPES = [
    'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'BIT',
    'FLOAT', 'REAL', 'DECIMAL(18,4)', 'NUMERIC(18,4)',
    'MONEY', 'SMALLMONEY',
    'CHAR(255)', 'VARCHAR(255)', 'NCHAR(255)', 'NVARCHAR(255)',
    'NVARCHAR(MAX)', 'TEXT', 'NTEXT',
    'DATE', 'TIME', 'DATETIME', 'DATETIME2', 'DATETIMEOFFSET',
    'BINARY(255)', 'VARBINARY(MAX)', 'IMAGE',
    'UNIQUEIDENTIFIER', 'SQL_VARIANT', 'XML',
]


class BreakpointManager:
    def __init__(self, db_path='migration_breakpoints.db'):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('''CREATE TABLE IF NOT EXISTS breakpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            last_pk_value TEXT,
            pk_column TEXT,
            rows_imported INTEGER DEFAULT 0,
            total_rows INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            updated_at TEXT,
            UNIQUE(task_id, table_name)
        )''')
        conn.commit()
        conn.close()

    def save(self, task_id, table_name, last_pk_value, pk_column,
             rows_imported, total_rows, status='in_progress'):
        pk_json = json.dumps(last_pk_value, default=str) if last_pk_value is not None else None
        conn = sqlite3.connect(self.db_path)
        conn.execute('''INSERT OR REPLACE INTO breakpoints
            (task_id, table_name, last_pk_value, pk_column, rows_imported, total_rows, status, updated_at)
            VALUES (?,?,?,?,?,?,?,?)''',
            (task_id, table_name, pk_json, pk_column, rows_imported, total_rows, status,
             datetime.now().isoformat()))
        conn.commit()
        conn.close()

    def get(self, task_id):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            'SELECT table_name, last_pk_value, pk_column, rows_imported, total_rows, status '
            'FROM breakpoints WHERE task_id=?', (task_id,)).fetchall()
        conn.close()
        result = []
        for r in rows:
            pk_val = json.loads(r[1]) if r[1] is not None else None
            result.append({
                'table': r[0], 'last_pk': pk_val, 'pk_col': r[2],
                'imported': r[3], 'total': r[4], 'status': r[5]
            })
        return result

    def delete(self, task_id):
        conn = sqlite3.connect(self.db_path)
        conn.execute('DELETE FROM breakpoints WHERE task_id=?', (task_id,))
        conn.commit()
        conn.close()

    def has_resumable(self, task_id):
        records = self.get(task_id)
        return any(r['status'] in ('in_progress', 'paused', 'error') and r['imported'] < r['total']
                   for r in records)


class AccessHelper:
    def __init__(self, driver, file_path):
        self.driver = driver
        self.file_path = file_path
        self.conn = None

    def connect(self):
        conn_str = f'DRIVER={{{self.driver}}};DBQ={self.file_path};'
        self.conn = pyodbc.connect(conn_str)
        return self

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_tables(self):
        return [row.table_name for row in self.conn.cursor().tables(tableType='TABLE')
                if not row.table_name.startswith('MSys') and not row.table_name.startswith('~')]

    def get_columns(self, table_name):
        cursor = self.conn.cursor()
        cols = []
        for row in cursor.columns(table=table_name):
            cols.append({
                'name': row.column_name,
                'type': row.type_name,
                'size': row.column_size,
                'nullable': bool(row.nullable),
                'is_auto': bool(getattr(row, 'auto_increment', False)),
            })
        return cols

    def get_primary_keys(self, table_name):
        try:
            # 尝试标准 ODBC 接口获取主键
            return [row.column_name for row in self.conn.cursor().primaryKeys(table=table_name)]
        except (pyodbc.InterfaceError, pyodbc.Error):
            # 如果驱动不支持 primaryKeys 接口（报错 IM001），降级处理：
            # 查找该表中的自增列（自增列通常就是主键）
            try:
                auto_cols = [
                    row.column_name 
                    for row in self.conn.cursor().columns(table=table_name) 
                    if getattr(row, 'auto_increment', False)
                ]
                if auto_cols:
                    return auto_cols
            except Exception:
                pass
            # 如果连自增列都找不到，返回空列表（程序会自动使用第一列作为排序依据）
            return []
            
    def get_row_count(self, table_name):
        cur = self.conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM [{table_name}]')
        return cur.fetchone()[0]

    def fetch_batch(self, table_name, pk_col, last_pk, batch_size):
        cur = self.conn.cursor()
        if pk_col and last_pk is not None:
            sql = f'SELECT * FROM [{table_name}] WHERE [{pk_col}] > ? ORDER BY [{pk_col}]'
            cur.execute(sql, (last_pk,))
        elif pk_col:
            cur.execute(f'SELECT * FROM [{table_name}] ORDER BY [{pk_col}]')
        else:
            cur.execute(f'SELECT * FROM [{table_name}]')
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchmany(batch_size)
        return col_names, rows, cur


class SQLServerHelper:
    def __init__(self, driver, server, database, username=None, password=None, windows_auth=False):
        self.driver = driver
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.windows_auth = windows_auth
        self.conn = None

    def connect(self):
        if self.windows_auth:
            cs = f'DRIVER={{{self.driver}}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;'
        else:
            cs = f'DRIVER={{{self.driver}}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};'
        self.conn = pyodbc.connect(cs, autocommit=False)
        return self

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def table_exists(self, name):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=? AND TABLE_TYPE='BASE TABLE'", (name,))
        return cur.fetchone()[0] > 0

    def create_table(self, name, col_defs, pks):
        parts = []
        for c in col_defs:
            s = f'[{c["name"]}] {c["sql_type"]}'
            if c.get('is_identity'):
                s += ' IDENTITY(1,1)'
            if not c.get('nullable', True):
                s += ' NOT NULL'
            parts.append(s)
        if pks:
            parts.append('PRIMARY KEY (' + ', '.join(f'[{k}]' for k in pks) + ')')
        sql = f'CREATE TABLE [{name}] ({", ".join(parts)})'
        self.conn.cursor().execute(sql)
        self.conn.commit()

    def truncate_table(self, name, reseed=False):
        cur = self.conn.cursor()
        cur.execute(f'DELETE FROM [{name}]')
        if reseed:
            try:
                cur.execute(f'DBCC CHECKIDENT ("[{name}]", RESEED, 0)')
            except Exception:
                pass
        self.conn.commit()

    def _has_identity(self, table_name):
        """动态检测目标表是否真正包含 IDENTITY 列"""
        try:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(?) AND is_identity = 1",
                (table_name,)
            )
            return cur.fetchone()[0] > 0
        except Exception:
            return False

    def insert_batch(self, table, columns, rows, identity_insert=False):
        """安全批量插入，避免使用 fast_executemany 防止脏数据导致底层闪退"""
        if not rows:
            return 0, []
        cur = self.conn.cursor()
        try:
            # 核心修复：只有当配置要求开启，且目标表确实有 IDENTITY 列时，才执行 SET
            if identity_insert and self._has_identity(table):
                cur.execute(f'SET IDENTITY_INSERT [{table}] ON')
                self.conn.commit()
                
            ph = ', '.join(['?'] * len(columns))
            col_s = ', '.join(f'[{c}]' for c in columns)
            sql = f'INSERT INTO [{table}] ({col_s}) VALUES ({ph})'
            
            try:
                cur.executemany(sql, rows)
                self.conn.commit()
                return len(rows), []
            except Exception:
                self.conn.rollback()
            
            # 如果普通批量执行也报错，降级为逐行插入，跳过有问题的行
            ok, fail = 0, []
            for i, r in enumerate(rows):
                try:
                    cur.execute(sql, r)
                    self.conn.commit()
                    ok += 1
                except Exception as e:
                    self.conn.rollback()
                    # 修改点：把具体的错误信息记录下来，而不是只记录索引
                    fail.append({'index': i, 'error': str(e)})
            return ok, fail
        finally:
            # 同样，只有确认表有 IDENTITY 列时，才去关闭
            if identity_insert and self._has_identity(table):
                try:
                    cur.execute(f'SET IDENTITY_INSERT [{table}] OFF')
                    self.conn.commit()
                except Exception:
                    pass

class MigrationWorker(QThread):
    progress = pyqtSignal(str, int, int)
    table_start = pyqtSignal(str)
    table_done = pyqtSignal(str, bool, str)
    log = pyqtSignal(str, str)
    overall = pyqtSignal(int, int)
    finished_signal = pyqtSignal(bool, str)

    def __init__(self, access_params, sql_params, configs, options, bp_mgr, task_id):
        super().__init__()
        self.access_params = access_params
        self.sql_params = sql_params
        self.configs = configs
        self.options = options
        self.bp_mgr = bp_mgr
        self.task_id = task_id
        self._stop = False

    def _clean_row(self, row):
        """清洗单行数据中的常见脏数据，防止 SQL Server 底层报错"""
        cleaned = list(row)
        for i, val in enumerate(cleaned):
            if val is None:
                continue
            # 1. SQL Server 的 NVARCHAR 绝对不允许存在 \x00（空字符），遇到直接剔除
            if isinstance(val, str) and '\x00' in val:
                cleaned[i] = val.replace('\x00', '')
            # 2. 处理 bytes 中的 \x00
            if isinstance(val, bytes) and b'\x00' in val:
                cleaned[i] = val.replace(b'\x00', b'')
            # 3. 处理 Access 的"幽灵空日期"（Access 中空日期底层往往会变成 1899-12-30）
            try:
                import datetime as dt
                if isinstance(val, dt.datetime) and val.year == 1899 and val.month == 12 and val.day == 30:
                    cleaned[i] = None
            except Exception:
                pass
        return tuple(cleaned)

    def stop(self):
        self._stop = True

    def run(self):
        access_h = SQLServer_h = None
        try:
            access_h = AccessHelper(**self.access_params).connect()
            SQLServer_h = SQLServerHelper(**self.sql_params).connect()
            total = len(self.configs)
            for idx, cfg in enumerate(self.configs):
                if self._stop:
                    self.log.emit('WARNING', '用户已中止迁移')
                    self.finished_signal.emit(False, '迁移已中止')
                    return
                self.overall.emit(idx, total)
                tbl = cfg['access_table']
                self.table_start.emit(tbl)
                ok, msg = self._migrate_one(access_h, SQLServer_h, cfg)
                self.table_done.emit(tbl, ok, msg)
                if not ok:
                    self.finished_signal.emit(False, f'表 {tbl} 迁移失败: {msg}')
                    return
            self.overall.emit(total, total)
            self.finished_signal.emit(True, '所有表迁移完成！')
        except Exception as e:
            self.log.emit('ERROR', f'迁移异常: {e}')
            self.finished_signal.emit(False, str(e))
        finally:
            if access_h:
                access_h.close()
            if SQLServer_h:
                SQLServer_h.close()

    def _migrate_one(self, ah, sh, cfg):
        tbl = cfg['access_table']
        sql_tbl = cfg.get('sql_table', tbl)
        col_cfgs = cfg['columns']
        col_names = [c['name'] for c in col_cfgs]
        id_cols = [c['name'] for c in col_cfgs if c.get('is_identity')]
        pks = cfg.get('primary_keys', [])
        keep_id = self.options.get('keep_identity', False)
        use_id_insert = keep_id and bool(id_cols)
        if_exists = self.options.get('if_exists', 'skip')
        batch_size = self.options.get('batch_size', 1000)

        total_rows = ah.get_row_count(tbl)
        if total_rows == 0:
            self.log.emit('INFO', f'表 {tbl} 无数据，跳过')
            self.progress.emit(tbl, 0, 0)
            return True, '空表已跳过'

        exists = sh.table_exists(sql_tbl)
        if exists and if_exists == 'skip':
            self.log.emit('WARNING', f'表 {sql_tbl} 已存在，跳过')
            self.progress.emit(tbl, total_rows, total_rows)
            return True, '已存在，已跳过'
        if exists and if_exists == 'clear':
            reseed = self.options.get('reseed_identity', True)
            self.log.emit('INFO', f'清空表 {sql_tbl}' + ('（含重置自增种子）' if reseed else ''))
            sh.truncate_table(sql_tbl, reseed=reseed)
        if not exists:
            self.log.emit('INFO', f'创建表 {sql_tbl}')
            defs = [{'name': c['name'], 'sql_type': c['sql_type'],
                     'nullable': c.get('nullable', True), 'is_identity': c.get('is_identity', False)}
                    for c in col_cfgs]
            sh.create_table(sql_tbl, defs, pks)

        pk_col = pks[0] if pks else col_names[0]
        last_pk = None
        imported = 0

        if self.options.get('resume'):
            for bp in self.bp_mgr.get(self.task_id):
                if bp['table'] == tbl and bp['imported'] < bp['total']:
                    last_pk = bp['last_pk']
                    imported = bp['imported']
                    pk_col = bp.get('pk_col', pk_col)
                    # 修复：如果不保留ID，断点续传会导致目标表ID错乱，强制放弃断点从头开始
                    if not use_id_insert:
                        last_pk = None
                        imported = 0
                        self.log.emit('WARNING', f'表 {tbl} 未启用保留ID，跳过断点从头导入')
                    else:
                        self.log.emit('INFO', f'断点续传 {tbl}: 已导入 {imported}/{bp["total"]} 行')
                    break

        col_names_set = set(col_names)
        col_indices = []
        self.progress.emit(tbl, imported, total_rows)
        try:
            # 修复核心逻辑：决定实际要插入 SQL Server 的列名
            # 如果不保留自增ID，必须从列列表中剔除自增列，让 SQL Server 自动生成
            insert_col_names = [c for c in col_names if not (not use_id_insert and c in id_cols)]
            
            src_cols, rows, cur = ah.fetch_batch(tbl, pk_col, last_pk, batch_size)
            # 只取需要插入的列对应的索引
            col_indices = [src_cols.index(c) for c in insert_col_names if c in src_cols]
            actual_cols = [insert_col_names[i] for i in range(len(insert_col_names)) if insert_col_names[i] in src_cols]

            while rows:
                if self._stop:
                    self.bp_mgr.save(self.task_id, tbl, last_pk, pk_col, imported, total_rows, 'paused')
                    return False, '已暂停，断点已保存'
                filtered = [self._clean_row(tuple(row[ci] for ci in col_indices)) for row in rows]
                ok_cnt, fail_rows = sh.insert_batch(sql_tbl, actual_cols, filtered, use_id_insert)
                if fail_rows:
                    err_msg = fail_rows[0].get('error', '未知错误')
                    self.log.emit('ERROR',
                                  f'表 {tbl} 本批 {len(filtered)} 行中有 {len(fail_rows)} 行失败！')
                    self.log.emit('ERROR', f'首条失败原因: {err_msg}')
                imported += ok_cnt
                if pk_col in src_cols:
                    pk_idx = src_cols.index(pk_col)
                    last_pk = rows[-1][pk_idx]
                self.progress.emit(tbl, imported, total_rows)
                if imported % (batch_size * 5) == 0:
                    self.bp_mgr.save(self.task_id, tbl, last_pk, pk_col, imported, total_rows, 'in_progress')
                rows = cur.fetchmany(batch_size)
            cur.close()
        except Exception as e:
            self.bp_mgr.save(self.task_id, tbl, last_pk, pk_col, imported, total_rows, 'error')
            return False, str(e)

        self.bp_mgr.save(self.task_id, tbl, last_pk, pk_col, imported, total_rows, 'completed')
        return True, f'成功导入 {imported} 行'

class FieldMappingDialog(QDialog):
    def __init__(self, table_name, access_columns, existing=None, parent=None):
        super().__init__(parent)
        # 标题中增加表名显示
        self.setWindowTitle(f'字段类型映射配置 - [{table_name}]')
        self.setMinimumSize(720, 420)
        self.resize(800, 500)
        self.table_name = table_name
        self.columns = access_columns
        self.result = []
        self._build(existing)

    def _build(self, existing):
        layout = QVBoxLayout(self)
        
        # 顶部提示增加表名显示
        info = QLabel(f'当前表: <b style="color:#d63031; font-size:14px;">{self.table_name}</b><br>'
                      '请为每个字段选择目标 SQL Server 数据类型。勾选"自增"将创建 IDENTITY(1,1) 字段。')
        info.setStyleSheet('color:#333;padding:6px; background:#f8f9fa; border-radius:4px;')
        info.setWordWrap(True)
        layout.addWidget(info)

        self.table = QTableWidget(len(self.columns), 5)
        self.table.setHorizontalHeaderLabels(['字段名', 'Access类型', '大小', 'SQL Server类型', '自增'])
        hh = self.table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(3, QHeaderView.Stretch)
        hh.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.table.verticalHeader().setVisible(False)

        for i, col in enumerate(self.columns):
            for j, val in enumerate([col['name'], col['type'], str(col.get('size', ''))]):
                it = QTableWidgetItem(val)
                it.setFlags(it.flags() & ~Qt.ItemIsEditable)
                self.table.setItem(i, j, it)
            
            cb = QComboBox()
            cb.addItems(SQL_SERVER_TYPES)
            
            is_id = col.get('is_auto', False)
            default_sql_type = 'NVARCHAR(255)'
            
            # 如果有历史自定义配置，优先使用历史配置
            if existing:
                for e in existing:
                    if e['name'] == col['name']:
                        default_sql_type = e['sql_type']
                        is_id = e.get('is_identity', is_id)
                        break
            
            # 如果没有历史配置，则进行智能默认映射
            if not existing or not any(e['name'] == col['name'] for e in existing):
                # 1. 强制修正：如果是自增字段，绝对不能是字符串类型，强制为 INT
                if is_id:
                    default_sql_type = 'INT'
                else:
                    # 2. 模糊匹配：去除 Access 类型名中多余的空格再匹配（如 "LONG INTEGER" -> "LONGINTEGER"）
                    clean_type = col['type'].upper().replace(' ', '')
                    if clean_type in ACCESS_TO_SQL_DEFAULTS:
                        default_sql_type = ACCESS_TO_SQL_DEFAULTS[clean_type]
                    else:
                        # 3. 精确匹配兜底
                        default_sql_type = ACCESS_TO_SQL_DEFAULTS.get(col['type'].upper(), 'NVARCHAR(255)')

            # 设置下拉框默认值
            idx = cb.findText(default_sql_type)
            if idx >= 0:
                cb.setCurrentIndex(idx)
            else:
                cb.setCurrentText(default_sql_type)
                
            self.table.setCellWidget(i, 3, cb)
            
            chk = QCheckBox()
            chk.setChecked(is_id)
            self.table.setCellWidget(i, 4, chk)
            
        layout.addWidget(self.table)

        btn_box = QHBoxLayout()
        btn_box.addStretch()
        ok_btn = QPushButton('确定')
        ok_btn.clicked.connect(self._ok)
        cancel_btn = QPushButton('取消')
        cancel_btn.clicked.connect(self.reject)
        btn_box.addWidget(ok_btn)
        btn_box.addWidget(cancel_btn)
        layout.addLayout(btn_box)

    def _ok(self):
        self.result = []
        for i in range(self.table.rowCount()):
            self.result.append({
                'name': self.table.item(i, 0).text(),
                'sql_type': self.table.cellWidget(i, 3).currentText(),
                'is_identity': self.table.cellWidget(i, 4).isChecked(),
                'nullable': self.columns[i].get('nullable', True),
            })
        self.accept()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Access → SQL Server 数据迁移工具')
        self.setMinimumSize(960, 680)
        self.resize(1150, 780)

        self.access_helper = None
        self.sql_helper = None
        self.worker = None
        self.bp_mgr = BreakpointManager()
        self.task_id = None
        self.table_mappings = {}

        self._build_ui()
        self._load_drivers()
        self._apply_style()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        self.tabs = QTabWidget()
        root.addWidget(self.tabs, stretch=4)
        self._tab_source()
        self._tab_target()
        self._tab_mapping()
        self._tab_migrate()
        self._tab_about()

        log_grp = QGroupBox('日志记录')
        log_lay = QVBoxLayout(log_grp)
        log_lay.setContentsMargins(4, 4, 4, 4)
        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        self.log_edit.setFont(QFont('Consolas', 9))
        log_lay.addWidget(self.log_edit)
        btn_clr = QPushButton('清空日志')
        btn_clr.setFixedWidth(80)
        btn_clr.clicked.connect(self.log_edit.clear)
        log_lay.addWidget(btn_clr, alignment=Qt.AlignRight)
        root.addWidget(log_grp, stretch=1)
        self.statusBar().showMessage('就绪')

    def _tab_source(self):
        w = QWidget()
        lay = QVBoxLayout(w)

        g1 = QGroupBox('Access ODBC 驱动')
        l1 = QVBoxLayout(g1)
        self.cb_access_drv = QComboBox()
        self.cb_access_drv.setMinimumWidth(400)
        l1.addWidget(self.cb_access_drv)
        l1.addWidget(QLabel('提示：列表为空请安装 Microsoft Access Database Engine（注意与 Python 位数一致）'))
        lay.addWidget(g1)

        g2 = QGroupBox('数据库文件')
        l2 = QHBoxLayout(g2)
        self.ed_mdb = QLineEdit()
        self.ed_mdb.setPlaceholderText('选择 .mdb / .accdb 文件...')
        l2.addWidget(self.ed_mdb)
        b = QPushButton('浏览...')
        b.clicked.connect(self._browse_mdb)
        l2.addWidget(b)
        lay.addWidget(g2)

        btn_conn = QPushButton('连接并加载表列表')
        btn_conn.setFixedHeight(36)
        btn_conn.clicked.connect(self._connect_access)
        lay.addWidget(btn_conn)

        g3 = QGroupBox('检测到的表')
        l3 = QVBoxLayout(g3)
        self.lst_tables = QListWidget()
        l3.addWidget(self.lst_tables)
        lay.addWidget(g3, stretch=1)
        self.tabs.addTab(w, '① 数据源配置')

    def _tab_target(self):
        w = QWidget()
        lay = QVBoxLayout(w)
        g = QGroupBox('SQL Server 连接配置')
        fl = QFormLayout(g)
        self.cb_sql_drv = QComboBox()
        fl.addRow('ODBC 驱动:', self.cb_sql_drv)
        self.ed_server = QLineEdit()
        self.ed_server.setPlaceholderText('例: localhost\\SQLEXPRESS 或 192.168.1.100')
        fl.addRow('服务器地址:', self.ed_server)
        self.ed_db = QLineEdit()
        self.ed_db.setPlaceholderText('目标数据库名称')
        fl.addRow('数据库名:', self.ed_db)

        hl = QHBoxLayout()
        self.rb_win = QRadioButton('Windows 身份验证')
        self.rb_sql = QRadioButton('SQL Server 身份验证')
        self.rb_sql.setChecked(True)
        self.rb_sql.toggled.connect(self._toggle_sql_auth)
        hl.addWidget(self.rb_win)
        hl.addWidget(self.rb_sql)
        fl.addRow('验证方式:', hl)

        self.ed_user = QLineEdit()
        self.ed_user.setEnabled(False)
        fl.addRow('用户名:', self.ed_user)
        self.ed_pwd = QLineEdit()
        self.ed_pwd.setEchoMode(QLineEdit.Password)
        self.ed_pwd.setEnabled(False)
        fl.addRow('密码:', self.ed_pwd)
        lay.addWidget(g)

        btn = QPushButton('测试连接')
        btn.setFixedHeight(36)
        btn.clicked.connect(self._test_sql)
        lay.addWidget(btn)
        self.lbl_sql_status = QLabel('')
        self.lbl_sql_status.setWordWrap(True)
        lay.addWidget(self.lbl_sql_status)
        lay.addStretch()
        self.tabs.addTab(w, '② 目标配置')

    def _tab_mapping(self):
        w = QWidget()
        lay = QVBoxLayout(w)
        top = QHBoxLayout()

        gl = QGroupBox('选择要迁移的表')
        ll = QVBoxLayout(gl)
        self.lst_check = QListWidget()
        ll.addWidget(self.lst_check)
        hb = QHBoxLayout()
        b1 = QPushButton('全选')
        b1.clicked.connect(lambda: self._sel_all(True))
        b2 = QPushButton('全不选')
        b2.clicked.connect(lambda: self._sel_all(False))
        hb.addWidget(b1)
        hb.addWidget(b2)
        ll.addLayout(hb)
        top.addWidget(gl, stretch=1)

        gr = QGroupBox('迁移选项')
        fr = QFormLayout(gr)
        self.cb_if_exist = QComboBox()
        self.cb_if_exist.addItems(['跳过已存在的表', '清空后重新导入', '追加数据'])
        fr.addRow('表已存在时:', self.cb_if_exist)
        self.chk_keep_id = QCheckBox('导入时保留ID自增值 (IDENTITY_INSERT)')
        fr.addRow('', self.chk_keep_id)
        self.chk_reseed = QCheckBox('清空表时重置自增种子 (DBCC RESEED)')
        self.chk_reseed.setChecked(True)
        fr.addRow('', self.chk_reseed)
        self.spn_batch = QSpinBox()
        self.spn_batch.setRange(100, 50000)
        self.spn_batch.setValue(1000)
        self.spn_batch.setSingleStep(500)
        self.spn_batch.setSuffix(' 行/批')
        fr.addRow('批量大小:', self.spn_batch)
        top.addWidget(gr, stretch=1)
        lay.addLayout(top, stretch=1)

        bl = QHBoxLayout()
        btn_def = QPushButton('使用默认类型映射（所有选中表）')
        btn_def.clicked.connect(self._default_mapping)
        bl.addWidget(btn_def)
        btn_cfg = QPushButton('自定义字段映射（逐表配置）')
        btn_cfg.clicked.connect(self._custom_mapping)
        bl.addWidget(btn_cfg)
        bl.addStretch()
        lay.addLayout(bl)

        self.lbl_map_status = QLabel('尚未配置字段映射')
        self.lbl_map_status.setWordWrap(True)
        self.lbl_map_status.setStyleSheet('color:#666;padding:5px;')
        lay.addWidget(self.lbl_map_status)
        self.tabs.addTab(w, '③ 表映射配置')

    def _tab_migrate(self):
        w = QWidget()
        lay = QVBoxLayout(w)

        gb = QGroupBox('断点信息')
        gbl = QVBoxLayout(gb)
        self.lbl_bp = QLabel('当前无断点记录')
        self.lbl_bp.setWordWrap(True)
        gbl.addWidget(self.lbl_bp)
        bpl = QHBoxLayout()
        self.btn_resume = QPushButton('从断点继续导入')
        self.btn_resume.setEnabled(False)
        self.btn_resume.clicked.connect(lambda: self._start(True))
        self.btn_del_bp = QPushButton('删除断点记录并重新导入')
        self.btn_del_bp.setEnabled(False)
        self.btn_del_bp.clicked.connect(self._del_bp)
        bpl.addWidget(self.btn_resume)
        bpl.addWidget(self.btn_del_bp)
        bpl.addStretch()
        gbl.addLayout(bpl)
        lay.addWidget(gb)

        gp = QGroupBox('迁移进度')
        gpl = QVBoxLayout(gp)
        r1 = QHBoxLayout()
        r1.addWidget(QLabel('总体进度:'))
        self.pb_overall = QProgressBar()
        self.pb_overall.setFormat('%v / %m 个表')
        r1.addWidget(self.pb_overall)
        gpl.addLayout(r1)
        r2 = QHBoxLayout()
        r2.addWidget(QLabel('当前表:'))
        self.lbl_cur_tbl = QLabel('-')
        self.lbl_cur_tbl.setStyleSheet('font-weight:bold;')
        r2.addWidget(self.lbl_cur_tbl)
        r2.addStretch()
        gpl.addLayout(r2)
        r3 = QHBoxLayout()
        r3.addWidget(QLabel('当前表进度:'))
        self.pb_cur = QProgressBar()
        self.pb_cur.setFormat('%v / %m 行 (%p%)')
        r3.addWidget(self.pb_cur)
        gpl.addLayout(r3)
        lay.addWidget(gp)

        cl = QHBoxLayout()
        self.btn_start = QPushButton('▶ 开始迁移')
        self.btn_start.setFixedHeight(42)
        self.btn_start.setMinimumWidth(160)
        self.btn_start.setStyleSheet('font-size:14px;font-weight:bold;')
        self.btn_start.clicked.connect(lambda: self._start(False))
        cl.addWidget(self.btn_start)
        self.btn_stop = QPushButton('■ 停止迁移')
        self.btn_stop.setFixedHeight(42)
        self.btn_stop.setMinimumWidth(160)
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self._stop)
        cl.addWidget(self.btn_stop)
        cl.addStretch()
        lay.addLayout(cl)
        lay.addStretch()
        self.tabs.addTab(w, '④ 数据迁移')
        
    def _tab_about(self):
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.addStretch()
        
        # 程序标题
        title = QLabel("Access → SQL Server 数据迁移工具")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 22px; font-weight: bold; color: #2c3e50; margin-bottom: 15px;")
        lay.addWidget(title)
        
        # 功能简介
        intro = QLabel(
            "本工具旨在提供稳定、高效、可控的 Access 数据库向 SQL Server 迁移方案。\n\n"
            "核心功能特性：\n"
            "•  智能字段类型映射与自定义精细配置\n"
            "•  IDENTITY 自增字段创建与导入特殊处理\n"
            "•  基于本地数据库的大数据量断点续传机制\n"
            "•  实时双进度条监控与详细分级日志记录\n"
            "•  批量高速导入与异常脏数据容错跳过"
        )
        intro.setAlignment(Qt.AlignCenter)
        intro.setWordWrap(True)
        intro.setStyleSheet("font-size: 13px; color: #555; line-height: 1.6; margin-bottom: 30px;")
        lay.addWidget(intro)
        
        # 作者及版权信息区域
        info_label = QLabel(
            "<b>程序作者：</b>xmge<br><br>"
            "<b>AI 协助开发：</b>GLM-5-Turbo<br><br>"
            "<b>支持网站：</b><a href='https://www.xmge.site' style='color: #2980b9; text-decoration: none;'>https://www.xmge.site</a>"
        )
        info_label.setAlignment(Qt.AlignCenter)
        # 关键设置：允许点击链接直接调用系统默认浏览器打开
        info_label.setOpenExternalLinks(True) 
        info_label.setStyleSheet(
            "font-size: 14px; color: #333; "
            "background-color: #f0f2f5; padding: 20px 40px; "
            "border-radius: 8px; border: 1px solid #dcdfe6;"
        )
        lay.addWidget(info_label, alignment=Qt.AlignCenter)
        
        lay.addStretch()
        
        # 添加到标签页，排在第5位
        self.tabs.addTab(w, '⑤ 关于')        

    def _browse_mdb(self):
        # 修复了文件过滤器格式：必须写成 "显示名称
        p, _ = QFileDialog.getOpenFileName(self, '选择 Access 数据库文件', '', 'Access 数据库;*.*;所有文件')
        if p:
            self.ed_mdb.setText(p)

    def _load_drivers(self):
        try:
            drivers = pyodbc.drivers()
            
            # --- 处理 Access 驱动 ---
            acc = [d for d in drivers if 'access' in d.lower() or 'mdb' in d.lower()]
            if not acc:
                acc = drivers
            self.cb_access_drv.clear()
            self.cb_access_drv.addItems(acc)
            for d in acc:
                if 'ACE' in d or 'accdb' in d.lower():
                    self.cb_access_drv.setCurrentText(d)
                    break

            # --- 处理 SQL Server 驱动 ---
            self.cb_sql_drv.clear()
            sql_drvs = [d for d in drivers if 'sql server' in d.lower()]
            
            if sql_drvs:
                self.cb_sql_drv.addItems(sql_drvs)
                # 默认选中最新的 OBC 驱动
                for d in reversed(sql_drvs):
                    if 'ODBC Driver' in d:
                        self.cb_sql_drv.setCurrentText(d)
                        break
            else:
                # 如果枚举不到任何驱动，提供常见的预设选项让用户手动选或填
                self._log('WARNING', '未自动检测到 SQL Server 驱动，已加载常见驱动预设，请根据实际情况选择或手动输入。')
                default_sql_drivers = [
                    "ODBC Driver 18 for SQL Server",
                    "ODBC Driver 17 for SQL Server",
                    "ODBC Driver 13 for SQL Server",
                    "SQL Server",
                    "SQL Server Native Client 11.0"
                ]
                self.cb_sql_drv.addItems(default_sql_drivers)
                
            # 如果系统完全没有任何驱动，给出强烈警告
            if not drivers:
                self._log('ERROR', '系统级 ODBC 驱动枚举失败（返回为空）！')
                self._log('ERROR', '原因通常是 Python 位数与操作系统 ODBC 管理器位数不一致。')

        except Exception as e:
            self._log('ERROR', f'加载驱动失败: {e}')
            
    def _connect_access(self):
        drv = self.cb_access_drv.currentText().strip()
        path = self.ed_mdb.text().strip()
        
        # 增加详细的校验日志，解决"点了没反应"的盲区问题
        if not drv:
            self._log('ERROR', '连接失败：未选择 Access ODBC 驱动！')
            QMessageBox.warning(self, '连接错误', 
                '未选择 Access ODBC 驱动！\n如果下拉框为空，请安装 Microsoft Access Database Engine。')
            return
            
        if not path:
            self._log('ERROR', '连接失败：未输入数据库文件路径！')
            QMessageBox.warning(self, '连接错误', '请先选择或输入有效的 .mdb / .accdb 文件路径！')
            return
            
        if not os.path.exists(path):
            self._log('ERROR', f'连接失败：文件不存在 -> {path}')
            QMessageBox.warning(self, '连接错误', f'文件不存在：\n{path}')
            return
            
        try:
            self._log('INFO', f'正在连接 Access: [{drv}] -> {path}')
            if self.access_helper:
                self.access_helper.close()
            self.access_helper = AccessHelper(drv, path).connect()
            tables = self.access_helper.get_tables()
            self.lst_tables.clear()
            self.lst_check.clear()
            self.table_mappings.clear()
            for t in tables:
                self.lst_tables.addItem(t)
                it = QListWidgetItem(t)
                it.setFlags(it.flags() | Qt.ItemIsUserCheckable)
                it.setCheckState(Qt.Unchecked)
                self.lst_check.addItem(it)
            self.task_id = os.path.basename(path)
            self._log('INFO', f'✅ 连接成功，检测到 {len(tables)} 个表')
            self.statusBar().showMessage(f'已连接: {os.path.basename(path)}')
            self._refresh_bp()
        except Exception as e:
            err_msg = str(e)
            self._log('ERROR', f'连接异常: {err_msg}')
            QMessageBox.critical(self, '连接失败', 
                f'{err_msg}\n\n常见原因：\n1. Python 位数与 Access 驱动位数不一致\n2. 文件被其他程序独占打开\n3. 驱动版本不支持该文件格式')

    def _toggle_sql_auth(self, checked):
        self.ed_user.setEnabled(checked)
        self.ed_pwd.setEnabled(checked)

    def _test_sql(self):
        drv = self.cb_sql_drv.currentText()
        srv = self.ed_server.text().strip()
        db = self.ed_db.text().strip()
        if not srv or not db:
            return QMessageBox.warning(self, '警告', '请填写服务器和数据库名')
        win = self.rb_win.isChecked()
        user = self.ed_user.text().strip() if not win else None
        pwd = self.ed_pwd.text() if not win else None
        try:
            if self.sql_helper:
                self.sql_helper.close()
            SQLServerHelper(drv, srv, db, user, pwd, win).connect().close()
            self.lbl_sql_status.setText('✅ 连接成功！')
            self.lbl_sql_status.setStyleSheet('color:green;font-weight:bold;')
            self._log('INFO', f'SQL Server 连接成功: {srv}/{db}')
        except Exception as e:
            self.lbl_sql_status.setText(f'❌ {e}')
            self.lbl_sql_status.setStyleSheet('color:red;')
            self._log('ERROR', f'SQL Server 连接失败: {e}')

    def _sel_all(self, checked):
        for i in range(self.lst_check.count()):
            self.lst_check.item(i).setCheckState(Qt.Checked if checked else Qt.Unchecked)

    def _selected_tables(self):
        return [self.lst_check.item(i).text()
                for i in range(self.lst_check.count())
                if self.lst_check.item(i).checkState() == Qt.Checked]

    def _default_mapping(self):
        if not self.access_helper:
            return QMessageBox.warning(self, '警告', '请先连接Access数据库')
        tables = self._selected_tables()
        if not tables:
            return QMessageBox.warning(self, '警告', '请至少选择一个表')
        cnt = 0
        for t in tables:
            cols = self.access_helper.get_columns(t)
            pks = self.access_helper.get_primary_keys(t)
            mapping = []
            for c in cols:
                mapping.append({
                    'name': c['name'],
                    'sql_type': ACCESS_TO_SQL_DEFAULTS.get(c['type'].upper(), 'NVARCHAR(255)'),
                    'is_identity': c.get('is_auto', False),
                    'nullable': c.get('nullable', True),
                })
            self.table_mappings[t] = {'columns': mapping, 'sql_table': t, 'primary_keys': pks}
            cnt += 1
        self.lbl_map_status.setText(f'✅ 已使用默认映射配置 {cnt} 个表（可点击"自定义字段映射"修改）')
        self.lbl_map_status.setStyleSheet('color:green;padding:5px;')
        self._log('INFO', f'默认映射已配置 {cnt} 个表')
        self._refresh_bp()

    def _custom_mapping(self):
        if not self.access_helper:
            return QMessageBox.warning(self, '警告', '请先连接Access数据库')
        tables = self._selected_tables()
        if not tables:
            return QMessageBox.warning(self, '警告', '请至少选择一个表')
        cnt = 0
        for t in tables:
            cols = self.access_helper.get_columns(t)
            pks = self.access_helper.get_primary_keys(t)
            existing = self.table_mappings.get(t, {}).get('columns')
            # --- 修改点：在这里增加传入表名参数 t ---
            dlg = FieldMappingDialog(t, cols, existing, self)
            if dlg.exec_() == QDialog.Accepted:
                self.table_mappings[t] = {'columns': dlg.result, 'sql_table': t, 'primary_keys': pks}
                cnt += 1
            else:
                break
        if cnt:
            self.lbl_map_status.setText(f'✅ 已自定义配置 {cnt} 个表的字段映射')
            self.lbl_map_status.setStyleSheet('color:green;padding:5px;')
            self._log('INFO', f'自定义映射已配置 {cnt} 个表')
            self._refresh_bp() 

    def _refresh_bp(self):
        if not self.task_id:
            self.lbl_bp.setText('当前无断点记录')
            self.btn_resume.setEnabled(False)
            self.btn_del_bp.setEnabled(False)
            return
        bps = self.bp_mgr.get(self.task_id)
        if bps:
            lines = ['检测到断点记录:']
            for bp in bps:
                st = {'completed': '已完成', 'in_progress': '进行中', 'paused': '已暂停', 'error': '出错'}.get(
                    bp['status'], bp['status'])
                lines.append(f'  • {bp["table"]}: {bp["imported"]}/{bp["total"]} 行 [{st}]')
            self.lbl_bp.setText('\n'.join(lines))
            self.btn_resume.setEnabled(self.bp_mgr.has_resumable(self.task_id))
            self.btn_del_bp.setEnabled(True)
        else:
            self.lbl_bp.setText('当前无断点记录')
            self.btn_resume.setEnabled(False)
            self.btn_del_bp.setEnabled(False)

    def _del_bp(self):
        if not self.task_id:
            return
        if QMessageBox.question(self, '确认', '确定删除断点？删除后将从头开始导入。',
                               QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            self.bp_mgr.delete(self.task_id)
            self._refresh_bp()
            self._log('INFO', '断点已删除')

    def _start(self, resume=False):
        if not self.access_helper:
            return QMessageBox.warning(self, '警告', '请先连接Access数据库')
        drv = self.cb_sql_drv.currentText()
        srv = self.ed_server.text().strip()
        db = self.ed_db.text().strip()
        if not drv or not srv or not db:
            return QMessageBox.warning(self, '警告', '请先配置并测试SQL Server连接')

        tables = self._selected_tables()
        configs = []
        for t in tables:
            if t not in self.table_mappings:
                self._log('ERROR', f'表 {t} 未配置映射，跳过')
                continue
            m = self.table_mappings[t]
            configs.append({
                'access_table': t, 'sql_table': m['sql_table'],
                'columns': m['columns'], 'primary_keys': m['primary_keys'],
            })
        if not configs:
            return QMessageBox.warning(self, '警告', '没有可迁移的表')

        win = self.rb_win.isChecked()
        access_params = {'driver': self.cb_access_drv.currentText(),
                         'file_path': self.ed_mdb.text().strip()}
        sql_params = {'driver': drv, 'server': srv, 'database': db,
                      'username': self.ed_user.text().strip() if not win else None,
                      'password': self.ed_pwd.text() if not win else None,
                      'windows_auth': win}
        if_map = {'跳过已存在的表': 'skip', '清空后重新导入': 'clear', '追加数据': 'append'}
        options = {
            'if_exists': if_map.get(self.cb_if_exist.currentText(), 'skip'),
            'keep_identity': self.chk_keep_id.isChecked(),
            'reseed_identity': self.chk_reseed.isChecked(),
            'batch_size': self.spn_batch.value(),
            'resume': resume,
        }

        self.worker = MigrationWorker(access_params, sql_params, configs,
                                      options, self.bp_mgr, self.task_id)
        self.worker.table_start.connect(self._on_tbl_start)
        self.worker.table_done.connect(self._on_tbl_done)
        self.worker.progress.connect(self._on_progress)
        self.worker.overall.connect(self._on_overall)
        self.worker.log.connect(self._log)
        self.worker.finished_signal.connect(self._on_done)

        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_resume.setEnabled(False)
        self.tabs.setCurrentIndex(3)
        self._log('INFO', f'开始迁移 {len(configs)} 个表' + ('（断点续传）' if resume else ''))
        self.worker.start()

    def _stop(self):
        if self.worker:
            self._log('WARNING', '正在停止...')
            self.worker.stop()

    def _on_tbl_start(self, t):
        self.lbl_cur_tbl.setText(t)
        self.pb_cur.setValue(0)

    def _on_tbl_done(self, t, ok, msg):
        icon = '✅' if ok else '❌'
        self._log('INFO' if ok else 'ERROR', f'{icon} {t}: {msg}')

    def _on_progress(self, t, cur, total):
        self.pb_cur.setMaximum(total if total else 1)
        self.pb_cur.setValue(cur)
        pct = (cur * 100 // total) if total else 0
        self.pb_cur.setFormat(f'{cur} / {total} 行 ({pct}%)')

    def _on_overall(self, done, total):
        self.pb_overall.setMaximum(total)
        self.pb_overall.setValue(done)

    def _on_done(self, ok, msg):
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self._refresh_bp()
        if ok:
            self._log('INFO', f'🎉 {msg}')
            QMessageBox.information(self, '完成', msg)
        else:
            self._log('ERROR', f'迁移失败: {msg}')

    def _log(self, level, msg):
        ts = datetime.now().strftime('%H:%M:%S')
        colors = {'INFO': '#006600', 'WARNING': '#CC6600', 'ERROR': '#CC0000'}
        c = colors.get(level, '#333')
        self.log_edit.append(
            f'<span style="color:#888">[{ts}]</span> '
            f'<span style="color:{c};font-weight:bold">[{level}]</span> {msg}')
        self.log_edit.verticalScrollBar().setValue(self.log_edit.verticalScrollBar().maximum())

    def _apply_style(self):
        self.setStyleSheet('''
            QMainWindow { background:#f5f5f5; }
            QGroupBox { font-weight:bold; border:1px solid #ccc; border-radius:4px;
                        margin-top:8px; padding-top:16px; }
            QGroupBox::title { subcontrol-origin:margin; left:10px; padding:0 5px; }
            QPushButton { background:#4a90d9; color:white; border:none; border-radius:4px;
                          padding:6px 16px; font-size:13px; }
            QPushButton:hover { background:#357abd; }
            QPushButton:pressed { background:#2a6196; }
            QPushButton:disabled { background:#b0b0b0; }
            QLineEdit, QComboBox, QSpinBox { padding:4px 8px; border:1px solid #ccc;
                                             border-radius:3px; background:white; }
            QTabWidget::pane { border:1px solid #ccc; border-radius:4px; }
            QTabBar::tab { padding:8px 20px; font-size:13px; }
            QTabBar::tab:selected { background:white; border-bottom:2px solid #4a90d9; }
            QTextEdit { border:1px solid #ccc; border-radius:3px; background:#1e1e1e; color:#d4d4d4; }
            QProgressBar { border:1px solid #ccc; border-radius:4px; text-align:center; height:22px; }
            QProgressBar::chunk { background:#4a90d9; border-radius:3px; }
            QListWidget { border:1px solid #ccc; border-radius:3px; background:white; }
            QTableWidget { border:1px solid #ccc; border-radius:3px; background:white;
                           gridline-color:#ddd; }
        ''')

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            r = QMessageBox.question(self, '确认退出',
                                     '迁移正在进行，退出后断点会保存，下次可继续。是否退出？',
                                     QMessageBox.Yes | QMessageBox.No)
            if r == QMessageBox.No:
                event.ignore()
                return
            self.worker.stop()
            self.worker.wait(5000)
        if self.access_helper:
            self.access_helper.close()
        if self.sql_helper:
            self.sql_helper.close()
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setFont(QFont('Microsoft YaHei', 10))
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()