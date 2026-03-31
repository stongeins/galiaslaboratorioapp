import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import date, datetime
import random
import hashlib
import os
import re
import shutil
import base64
from io import BytesIO

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'arcillas_vzla.db')
REPO_DIR = os.path.join(BASE_DIR, 'repositorio')

# =====================================================
# 1. BASE DE DATOS AMPLIADA
# =====================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Tabla principal de muestras (con más campos de ubicación)
    c.execute('''CREATE TABLE IF NOT EXISTS muestras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        codigo_lab TEXT UNIQUE,
        yacimiento TEXT,
        estado TEXT,
        municipio TEXT,
        latitud REAL,
        longitud REAL,
        fecha DATE,
        observaciones TEXT,
        creado_por TEXT DEFAULT 'sistema'
    )''')

    # Química ampliada (todos los óxidos + elementos menores)
    c.execute('''CREATE TABLE IF NOT EXISTS quimica (
        muestra_id INTEGER PRIMARY KEY,
        fe2o3 REAL, al2o3 REAL, sio2 REAL,
        tio2 REAL, cao REAL, mgo REAL,
        k2o REAL, na2o REAL, ppc REAL,
        so3 REAL, p2o5 REAL, mno REAL,
        h2o REAL, carbono REAL, azufre REAL,
        FOREIGN KEY(muestra_id) REFERENCES muestras(id) ON DELETE CASCADE
    )''')

    # Física ampliada (cocción + mecánica + plasticidad + granulometría)
    c.execute('''CREATE TABLE IF NOT EXISTS fisica (
        muestra_id INTEGER PRIMARY KEY,
        absorcion REAL, contraccion REAL,
        l_color REAL, a_color REAL, b_color REAL,
        resistencia_flexion REAL,
        densidad REAL,
        temperatura_coccion REAL,
        superficie_especifica REAL,
        mor_verde REAL, mor_seco REAL,
        mor_cocido_kgf REAL, mor_cocido_mpa REAL,
        pfefferkorn REAL,
        limite_liquido REAL, limite_plastico REAL,
        indice_plasticidad REAL,
        residuo_45um REAL, menor_2um REAL, d50 REAL,
        contraccion_secado REAL, contraccion_total REAL,
        porosidad_abierta REAL,
        FOREIGN KEY(muestra_id) REFERENCES muestras(id) ON DELETE CASCADE
    )''')

    # Tabla de usuarios para acceso restringido
    c.execute('''CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        rol TEXT DEFAULT 'cliente',
        nombre_completo TEXT,
        estado TEXT DEFAULT 'pendiente',
        permisos TEXT DEFAULT ''
    )''')

    # Tabla de blends/composites
    c.execute('''CREATE TABLE IF NOT EXISTS blends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        descripcion TEXT,
        creado_por TEXT,
        fecha DATE,
        objetivo_uso TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS blend_componentes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        blend_id INTEGER,
        muestra_id INTEGER,
        porcentaje REAL,
        FOREIGN KEY(blend_id) REFERENCES blends(id) ON DELETE CASCADE,
        FOREIGN KEY(muestra_id) REFERENCES muestras(id)
    )''')

    # Tabla de repositorio de archivos
    c.execute('''CREATE TABLE IF NOT EXISTS repositorio (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre_archivo TEXT NOT NULL,
        nombre_original TEXT,
        categoria TEXT NOT NULL,
        subcategoria TEXT,
        muestra_id INTEGER,
        descripcion TEXT,
        tipo_archivo TEXT,
        tamano_bytes INTEGER,
        ruta_relativa TEXT NOT NULL,
        fecha_subida DATETIME DEFAULT CURRENT_TIMESTAMP,
        subido_por TEXT DEFAULT 'sistema',
        tags TEXT,
        FOREIGN KEY(muestra_id) REFERENCES muestras(id) ON DELETE SET NULL
    )''')

    # Tabla de especificaciones por producto cerámico (ISO 13006 / EN 14411)
    c.execute('''CREATE TABLE IF NOT EXISTS especificaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        producto TEXT NOT NULL,
        parametro TEXT NOT NULL,
        min_valor REAL,
        max_valor REAL,
        unidad TEXT,
        norma TEXT DEFAULT 'ISO 13006'
    )''')

    # Pre-cargar especificaciones si la tabla está vacía
    c.execute("SELECT COUNT(*) FROM especificaciones")
    if c.fetchone()[0] == 0:
        specs = [
            # BIa - Porcelanato Técnico
            ('BIa - Porcelanato', 'absorcion',     None,  0.5,  '%',     'ISO 13006'),
            ('BIa - Porcelanato', 'mor_cocido_mpa', 35,   None, 'MPa',   'ISO 13006'),
            ('BIa - Porcelanato', 'fe2o3',         None,  0.8,  '%',     'Comercial'),
            ('BIa - Porcelanato', 'contraccion',    6.0,  8.0,  '%',     'Referencia'),
            ('BIa - Porcelanato', 'al2o3',          18,   None, '%',     'Referencia'),
            # BIb - Gres Porcelánico
            ('BIb - Gres',        'absorcion',      0.5,  3.0,  '%',     'ISO 13006'),
            ('BIb - Gres',        'mor_cocido_mpa', 30,   None, 'MPa',   'ISO 13006'),
            ('BIb - Gres',        'contraccion',    5.0,  8.0,  '%',     'Referencia'),
            # BIIa - Semi-gres
            ('BIIa - Semi-gres',  'absorcion',      3.0,  6.0,  '%',     'ISO 13006'),
            ('BIIa - Semi-gres',  'mor_cocido_mpa', 22,   None, 'MPa',   'ISO 13006'),
            ('BIIa - Semi-gres',  'fe2o3',         None,  3.0,  '%',     'Referencia'),
            # BIIb - Loza vitrificada
            ('BIIb - Loza',       'absorcion',      6.0,  10.0, '%',     'ISO 13006'),
            ('BIIb - Loza',       'mor_cocido_mpa', 18,   None, 'MPa',   'ISO 13006'),
            # BIII - Revestimiento / Monoporosa
            ('BIII - Revestimiento', 'absorcion',  10.0,  None, '%',     'ISO 13006'),
            ('BIII - Revestimiento', 'mor_cocido_mpa', 15, None, 'MPa',  'ISO 13006'),
            # Ladrillería
            ('Ladrilleria',       'absorcion',      8.0,  25.0, '%',     'NTE INEN / NTC'),
            ('Ladrilleria',       'mor_cocido_mpa',  5,   None, 'MPa',   'NTE INEN / NTC'),
            ('Ladrilleria',       'fe2o3',         None,  8.0,  '%',     'Referencia'),
        ]
        c.executemany(
            "INSERT INTO especificaciones (producto, parametro, min_valor, max_valor, unidad, norma) "
            "VALUES (?, ?, ?, ?, ?, ?)", specs
        )

    # Tabla de certificados emitidos
    c.execute('''CREATE TABLE IF NOT EXISTS certificados_emitidos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        muestra_id INTEGER,
        producto TEXT,
        fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
        resultado TEXT,
        emitido_por TEXT,
        numero_certificado TEXT UNIQUE,
        FOREIGN KEY(muestra_id) REFERENCES muestras(id)
    )''')

    # Tabla de solicitudes de validación de ingeniero
    c.execute('''CREATE TABLE IF NOT EXISTS validaciones_ingeniero (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        solicitante_id INTEGER,
        solicitante_nombre TEXT,
        muestras_ids TEXT,
        modulo_origen TEXT,
        descripcion_duda TEXT,
        contexto_uso TEXT,
        tarifa REAL DEFAULT 0,
        metodo_pago TEXT DEFAULT 'pendiente',
        referencia_pago TEXT,
        estado TEXT DEFAULT 'nueva',
        prioridad TEXT DEFAULT 'normal',
        fecha_solicitud DATETIME DEFAULT CURRENT_TIMESTAMP,
        fecha_respuesta DATETIME,
        ingeniero_asignado TEXT,
        dictamen TEXT,
        recomendacion_tecnica TEXT,
        archivo_firmado TEXT,
        FOREIGN KEY(solicitante_id) REFERENCES usuarios(id)
    )''')

    # Tabla de datos extra (DRX, DTA, TGA, SEM, FTIR, otros)
    c.execute('''CREATE TABLE IF NOT EXISTS datos_extra (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        muestra_id INTEGER,
        tipo TEXT NOT NULL,
        parametro TEXT,
        valor TEXT,
        unidad TEXT,
        observaciones TEXT,
        fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
        ingresado_por TEXT DEFAULT 'admin',
        FOREIGN KEY(muestra_id) REFERENCES muestras(id) ON DELETE CASCADE
    )''')

    # Tabla de codigos de acceso para validacion de ingeniero
    c.execute('''CREATE TABLE IF NOT EXISTS codigos_validacion (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        usuario_id INTEGER,
        usado INTEGER DEFAULT 0,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        fecha_uso DATETIME,
        creado_por TEXT DEFAULT 'admin',
        FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
    )''')

    # Tabla de clasificaciones editables de arcillas
    c.execute('''CREATE TABLE IF NOT EXISTS clasificaciones_uso (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo TEXT NOT NULL,
        nombre TEXT NOT NULL,
        campo TEXT NOT NULL,
        min_valor REAL,
        max_valor REAL,
        orden INTEGER DEFAULT 0
    )''')

    # Pre-cargar clasificaciones por defecto si vacía
    c.execute("SELECT COUNT(*) FROM clasificaciones_uso")
    if c.fetchone()[0] == 0:
        clasificaciones_default = [
            ('calidad', 'Premium (Clara)', 'fe2o3', None, 1.0, 1),
            ('calidad', 'Estandar (Beige/Rosada)', 'fe2o3', 1.0, 1.8, 2),
            ('calidad', 'Industrial (Roja)', 'fe2o3', 1.8, None, 3),
            ('uso', 'Porcelanato Tecnico', 'absorcion', None, 0.5, 1),
            ('uso', 'Gres Esmaltado / Vitrificado', 'absorcion', 0.5, 3.0, 2),
            ('uso', 'Piso Stoneware / Semi-Gres', 'absorcion', 3.0, 6.0, 3),
            ('uso', 'Revestimiento Pared / Monoporosa', 'absorcion', 6.0, 10.0, 4),
            ('uso', 'Ladrilleria / Tejas', 'absorcion', 10.0, None, 5),
        ]
        for tipo, nombre, campo, min_v, max_v, orden in clasificaciones_default:
            c.execute(
                "INSERT INTO clasificaciones_uso (tipo, nombre, campo, min_valor, max_valor, orden) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (tipo, nombre, campo, min_v, max_v, orden))

    # Migrar tablas existentes (agregar columnas nuevas si no existen)
    _migrate_columns(c)

    # Pre-cargar muestras objetivo si la tabla está vacía
    c.execute("SELECT COUNT(*) FROM muestras")
    if c.fetchone()[0] == 0:
        _precargar_muestras_objetivo(c)

    # Crear usuario admin por defecto si no existe
    c.execute("SELECT COUNT(*) FROM usuarios")
    if c.fetchone()[0] == 0:
        admin_hash = hashlib.sha256("admin2026".encode()).hexdigest()
        c.execute("""INSERT INTO usuarios
                     (username, password_hash, rol, nombre_completo, estado, permisos)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  ("admin", admin_hash, "admin", "Administrador", "activo",
                   "Control de Calidad,Curvas de Gresificación,Fórmula Seger / UMF,Predicción de Color,Optimizador de Mezclas,Cargar desde Excel,Agregar Muestra Manual,Blender & Composite"))
    else:
        # Asegurar que el admin existente tenga estado='activo' y permisos completos
        c.execute("UPDATE usuarios SET estado='activo' WHERE username='admin' AND (estado IS NULL OR estado='')")
        c.execute("UPDATE usuarios SET permisos=? WHERE username='admin' AND (permisos IS NULL OR permisos='')",
                  (",".join(["Control de Calidad", "Curvas de Gresificación",
                             "Fórmula Seger / UMF", "Predicción de Color"]),))

    conn.commit()
    conn.close()


def _precargar_muestras_objetivo(cursor):
    """Pre-carga las 3 muestras objetivo desde el Excel o desde constantes hardcoded."""
    excel_path = os.path.join(BASE_DIR, 'base datos muestras objetivo.xlsx')
    muestras_obj = []
    if os.path.exists(excel_path):
        try:
            df_obj = pd.read_excel(excel_path)
            # Mapeo fuzzy de columnas del Excel a campos internos
            col_map_obj = {}
            target_prefixes = {
                'muestra': 'nombre', 'codigo': 'codigo_lab', 'yacimiento': 'yacimiento',
                'plasticidad pfeff': 'pfefferkorn', 'sio2': 'sio2', 'al2o3': 'al2o3',
                'fe2o3': 'fe2o3', 'tio2': 'tio2', 'cao': 'cao', 'mgo': 'mgo',
                'k2o': 'k2o', 'na2o': 'na2o', 'ppc': 'ppc', 'aa': 'absorcion',
                'contraccion': 'contraccion', 'l*': 'l_color', 'a*': 'a_color',
                'b*': 'b_color', 'c ': 'carbono', 's ': 'azufre',
                'ss': 'superficie_especifica', 'mor seco': 'mor_seco',
            }
            for col in df_obj.columns:
                cl = str(col).strip().lower()
                for prefix, field in target_prefixes.items():
                    if cl.startswith(prefix):
                        col_map_obj[col] = field
                        break
            df_obj = df_obj.rename(columns=col_map_obj)
            for _, row in df_obj.iterrows():
                nombre = row.get('nombre')
                if not nombre or pd.isna(nombre):
                    continue
                muestras_obj.append({
                    'nombre': str(nombre).strip(),
                    'codigo_lab': str(row.get('codigo_lab', '')).strip() if pd.notna(row.get('codigo_lab')) else '',
                    'yacimiento': str(row.get('yacimiento', '')).strip() if pd.notna(row.get('yacimiento')) else '',
                    'sio2': float(row['sio2']) if pd.notna(row.get('sio2')) else None,
                    'al2o3': float(row['al2o3']) if pd.notna(row.get('al2o3')) else None,
                    'fe2o3': float(row['fe2o3']) if pd.notna(row.get('fe2o3')) else None,
                    'tio2': float(row['tio2']) if pd.notna(row.get('tio2')) else None,
                    'cao': float(row['cao']) if pd.notna(row.get('cao')) else None,
                    'mgo': float(row['mgo']) if pd.notna(row.get('mgo')) else None,
                    'k2o': float(row['k2o']) if pd.notna(row.get('k2o')) else None,
                    'na2o': float(row['na2o']) if pd.notna(row.get('na2o')) else None,
                    'ppc': float(row['ppc']) if pd.notna(row.get('ppc')) else None,
                    'absorcion': float(row['absorcion']) if pd.notna(row.get('absorcion')) else None,
                    'contraccion': float(row['contraccion']) if pd.notna(row.get('contraccion')) else None,
                    'l_color': float(row['l_color']) if pd.notna(row.get('l_color')) else None,
                    'a_color': float(row['a_color']) if pd.notna(row.get('a_color')) else None,
                    'b_color': float(row['b_color']) if pd.notna(row.get('b_color')) else None,
                    'superficie_especifica': float(row['superficie_especifica']) if pd.notna(row.get('superficie_especifica')) else None,
                    'mor_seco': float(row['mor_seco']) if pd.notna(row.get('mor_seco')) else None,
                    'pfefferkorn': float(row['pfefferkorn']) if pd.notna(row.get('pfefferkorn')) else None,
                })
        except Exception:
            muestras_obj = []

    # Fallback: usar constantes hardcoded si no se pudo leer el Excel
    if not muestras_obj:
        muestras_obj = [
            {'nombre': 'OBJETIVO 1', 'codigo_lab': 'UCRANIA NUEVA', 'yacimiento': 'UCRANIA',
             'sio2': 60.00, 'al2o3': 26.50, 'fe2o3': 0.90, 'tio2': 1.40,
             'cao': 0.30, 'mgo': 0.50, 'k2o': 1.90, 'na2o': 0.15, 'ppc': 7.50,
             'absorcion': 0.50, 'contraccion': 12.50,
             'l_color': 80.75, 'a_color': 3.05, 'b_color': 16.18,
             'superficie_especifica': 140, 'mor_seco': 45, 'pfefferkorn': 36.2},
            {'nombre': 'OBJETIVO 2', 'codigo_lab': 'UCRANIA ANT.', 'yacimiento': 'UCRANIA',
             'sio2': 60.10, 'al2o3': 26.02, 'fe2o3': 1.00, 'tio2': 1.50,
             'cao': 0.30, 'mgo': 0.52, 'k2o': 2.30, 'na2o': 0.44, 'ppc': 7.60,
             'absorcion': 0.23, 'contraccion': 6.88,
             'l_color': 69.30, 'a_color': 6.40, 'b_color': 21.17,
             'superficie_especifica': 140, 'mor_seco': None, 'pfefferkorn': 26.0},
            {'nombre': 'OBJETIVO 3', 'codigo_lab': 'INDIA AG-30', 'yacimiento': 'INDIA',
             'sio2': 52.00, 'al2o3': 30.00, 'fe2o3': 1.20, 'tio2': 1.10,
             'cao': 1.60, 'mgo': 0.40, 'k2o': 1.20, 'na2o': 0.30, 'ppc': 12.20,
             'absorcion': 1.80, 'contraccion': 9.00,
             'l_color': 78.00, 'a_color': 0.50, 'b_color': 13.00,
             'superficie_especifica': 105, 'mor_seco': None, 'pfefferkorn': 32.5},
            {'nombre': 'OBJETIVO 4', 'codigo_lab': 'UCRA TECHNIC', 'yacimiento': 'UCRA VESCO',
             'sio2': 68.00, 'al2o3': 22.00, 'fe2o3': 1.10, 'tio2': 1.50,
             'cao': 0.30, 'mgo': 0.35, 'k2o': 1.90, 'na2o': 0.15, 'ppc': 6.30,
             'absorcion': 1.25, 'contraccion': 11.11,
             'l_color': 78.38, 'a_color': 4.11, 'b_color': 17.90,
             'superficie_especifica': 80, 'mor_seco': 38, 'pfefferkorn': 22.0},
        ]

    for m in muestras_obj:
        nombre = m.get('nombre', '')
        codigo = m.get('codigo_lab', '') or nombre
        yacimiento = m.get('yacimiento', 'Objetivo')
        cursor.execute("""INSERT INTO muestras
            (nombre, codigo_lab, yacimiento, estado, municipio, fecha, observaciones, creado_por)
            VALUES (?, ?, ?, '', '', ?, 'Muestra Objetivo precargada', 'sistema')""",
            (nombre, codigo, yacimiento, date.today().isoformat()))
        mid = cursor.lastrowid
        cursor.execute("""INSERT INTO quimica
            (muestra_id, fe2o3, al2o3, sio2, tio2, cao, mgo, k2o, na2o, ppc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (mid, m.get('fe2o3'), m.get('al2o3'), m.get('sio2'), m.get('tio2'),
             m.get('cao'), m.get('mgo'), m.get('k2o'), m.get('na2o'), m.get('ppc')))
        cursor.execute("""INSERT INTO fisica
            (muestra_id, absorcion, contraccion, l_color, a_color, b_color,
             superficie_especifica, mor_seco, pfefferkorn)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (mid, m.get('absorcion'), m.get('contraccion'),
             m.get('l_color'), m.get('a_color'), m.get('b_color'),
             m.get('superficie_especifica'), m.get('mor_seco'), m.get('pfefferkorn')))


def _migrate_columns(cursor):
    """Agrega columnas nuevas a tablas existentes sin perder datos."""
    migrations = [
        ("muestras", "estado", "TEXT"),
        ("muestras", "municipio", "TEXT"),
        ("muestras", "latitud", "REAL"),
        ("muestras", "longitud", "REAL"),
        ("muestras", "observaciones", "TEXT"),
        ("muestras", "creado_por", "TEXT DEFAULT 'sistema'"),
        ("quimica", "tio2", "REAL"),
        ("quimica", "cao", "REAL"),
        ("quimica", "mgo", "REAL"),
        ("quimica", "k2o", "REAL"),
        ("quimica", "na2o", "REAL"),
        ("quimica", "ppc", "REAL"),
        ("quimica", "so3", "REAL"),
        ("quimica", "p2o5", "REAL"),
        ("quimica", "mno", "REAL"),
        ("quimica", "h2o", "REAL"),
        ("quimica", "carbono", "REAL"),
        ("quimica", "azufre", "REAL"),
        ("fisica", "resistencia_flexion", "REAL"),
        ("fisica", "densidad", "REAL"),
        ("fisica", "temperatura_coccion", "REAL"),
        ("fisica", "superficie_especifica", "REAL"),
        ("fisica", "mor_verde", "REAL"),
        ("fisica", "mor_seco", "REAL"),
        ("fisica", "mor_cocido_kgf", "REAL"),
        ("fisica", "mor_cocido_mpa", "REAL"),
        ("fisica", "pfefferkorn", "REAL"),
        ("fisica", "limite_liquido", "REAL"),
        ("fisica", "limite_plastico", "REAL"),
        ("fisica", "indice_plasticidad", "REAL"),
        ("fisica", "residuo_45um", "REAL"),
        ("fisica", "menor_2um", "REAL"),
        ("fisica", "d50", "REAL"),
        ("fisica", "contraccion_secado", "REAL"),
        ("fisica", "contraccion_total", "REAL"),
        ("fisica", "porosidad_abierta", "REAL"),
        # RBAC columns
        ("usuarios", "estado", "TEXT DEFAULT 'activo'"),
        ("usuarios", "permisos", "TEXT DEFAULT ''"),
    ]
    for table, col, col_type in migrations:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        except Exception:
            pass  # La columna ya existe


def get_conn():
    return sqlite3.connect(DB_PATH)


# =====================================================
# 2. REPOSITORIO INTELIGENTE - AUTO-CLASIFICADOR
# =====================================================
CATEGORIAS_REPO = {
    'frx': {
        'titulo': 'Certificados FRX / Análisis Químico',
        'icon': '🔬',
        'keywords': ['frx', 'fluorescencia', 'xrf', 'rayos x', 'quimic', 'oxido', 'certificado'],
        'extensions': [],
    },
    'campo': {
        'titulo': 'Fotos de Campo / Mina',
        'icon': '⛏️',
        'keywords': ['campo', 'mina', 'cantera', 'excavac', 'operacion', 'terreno', 'calicata', 'trinchera', 'pozo'],
        'extensions': [],
    },
    'drone': {
        'titulo': 'Vista Aérea / Drone',
        'icon': '🛩️',
        'keywords': ['drone', 'aere', 'aérea', 'satelit', 'google earth', 'vista', 'area de trabajo', 'sobrevuelo'],
        'extensions': [],
    },
    'laboratorio': {
        'titulo': 'Laboratorio / Probetas',
        'icon': '🧪',
        'keywords': ['lab', 'probeta', 'ensayo', 'coccion', 'cocida', 'muestra', 'color', 'absorcion'],
        'extensions': [],
    },
    'informes': {
        'titulo': 'Informes y Documentos Técnicos',
        'icon': '📄',
        'keywords': ['informe', 'report', 'analisis', 'estudio', 'evaluacion', 'recurso'],
        'extensions': ['.docx', '.doc', '.pdf', '.pptx'],
    },
    'datos': {
        'titulo': 'Datos / Hojas de Cálculo',
        'icon': '📊',
        'keywords': ['data', 'dato', 'tabla', 'resultado', 'completo'],
        'extensions': ['.xlsx', '.xls', '.csv', '.tsv'],
    },
    'mapas': {
        'titulo': 'Mapas y Planos',
        'icon': '🗺️',
        'keywords': ['mapa', 'plano', 'topograf', 'perfil', 'poligonal', 'coordenada', 'dwg', 'cad'],
        'extensions': ['.dwg', '.dxf', '.kml', '.kmz', '.shp'],
    },
    'procesos': {
        'titulo': 'Diagramas de Procesos / Planta',
        'icon': '🏭',
        'keywords': ['planta', 'proceso', 'diagrama', 'beneficio', 'molienda', 'isometric'],
        'extensions': [],
    },
    'logos': {
        'titulo': 'Identidad / Logos',
        'icon': '🏷️',
        'keywords': ['logo', 'marca', 'identidad', 'brand'],
        'extensions': ['.svg', '.ai', '.eps'],
    },
    'otros': {
        'titulo': 'Otros Archivos',
        'icon': '📁',
        'keywords': [],
        'extensions': [],
    },
}


def clasificar_archivo(nombre_archivo):
    """Auto-clasifica un archivo por nombre y extensión."""
    nombre_lower = nombre_archivo.lower()
    ext = os.path.splitext(nombre_lower)[1]

    # Primero: revisar extensiones específicas
    for cat_key, cat_info in CATEGORIAS_REPO.items():
        if cat_key == 'otros':
            continue
        if ext in cat_info['extensions']:
            return cat_key

    # Segundo: revisar keywords en el nombre
    scores = {}
    for cat_key, cat_info in CATEGORIAS_REPO.items():
        if cat_key == 'otros':
            continue
        score = sum(1 for kw in cat_info['keywords'] if kw in nombre_lower)
        if score > 0:
            scores[cat_key] = score

    if scores:
        return max(scores, key=scores.get)

    # FRX pattern: nombre empieza con FRX
    if nombre_lower.startswith('frx'):
        return 'frx'

    # Imágenes sin clasificar -> campo por defecto
    img_exts = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tiff'}
    if ext in img_exts:
        return 'campo'

    return 'otros'


def detectar_muestra_en_nombre(nombre_archivo, muestras_df):
    """Intenta vincular automáticamente un archivo a una muestra existente."""
    nombre_lower = nombre_archivo.lower().replace('_', ' ').replace('-', ' ')

    best_match = None
    best_score = 0

    for _, row in muestras_df.iterrows():
        nombre_muestra = str(row['nombre']).lower()
        codigo = str(row.get('codigo_lab', '')).lower()

        # Check código exacto
        if codigo and codigo in nombre_lower:
            return int(row['id'])

        # Check partes del nombre de muestra
        partes = nombre_muestra.split()
        score = sum(1 for p in partes if len(p) > 2 and p in nombre_lower)
        if score > best_score and score >= 2:
            best_score = score
            best_match = int(row['id'])

    return best_match


def guardar_archivo_repo(uploaded_file, categoria, muestra_id=None, descripcion='',
                         tags='', usuario='sistema'):
    """Guarda un archivo en el repositorio y registra en la BD."""
    # Crear directorio de categoría
    cat_dir = os.path.join(REPO_DIR, categoria)
    os.makedirs(cat_dir, exist_ok=True)

    # Nombre seguro (evitar duplicados)
    nombre_original = uploaded_file.name
    nombre_base, ext = os.path.splitext(nombre_original)
    nombre_seguro = re.sub(r'[^\w\-. ]', '_', nombre_base)
    nombre_final = f"{nombre_seguro}{ext}"

    # Si existe, agregar timestamp
    ruta_completa = os.path.join(cat_dir, nombre_final)
    if os.path.exists(ruta_completa):
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_final = f"{nombre_seguro}_{ts}{ext}"
        ruta_completa = os.path.join(cat_dir, nombre_final)

    # Guardar archivo
    with open(ruta_completa, 'wb') as f:
        f.write(uploaded_file.getbuffer())

    tamano = os.path.getsize(ruta_completa)
    ruta_rel = f"{categoria}/{nombre_final}"
    tipo = ext.lstrip('.').upper() or 'UNKNOWN'

    # Registrar en BD
    conn = get_conn()
    c = conn.cursor()
    c.execute("""INSERT INTO repositorio
        (nombre_archivo, nombre_original, categoria, muestra_id, descripcion,
         tipo_archivo, tamano_bytes, ruta_relativa, subido_por, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (nombre_final, nombre_original, categoria, muestra_id, descripcion,
         tipo, tamano, ruta_rel, usuario, tags))
    conn.commit()
    file_id = c.lastrowid
    conn.close()

    return file_id, ruta_rel


def obtener_archivos_repo(categoria=None, muestra_id=None):
    """Obtiene archivos del repositorio con filtros opcionales."""
    conn = get_conn()
    query = """SELECT r.*, m.nombre as muestra_nombre
               FROM repositorio r
               LEFT JOIN muestras m ON r.muestra_id = m.id
               WHERE 1=1"""
    params = []
    if categoria:
        query += " AND r.categoria = ?"
        params.append(categoria)
    if muestra_id:
        query += " AND r.muestra_id = ?"
        params.append(muestra_id)
    query += " ORDER BY r.fecha_subida DESC"
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


# =====================================================
# 3. CLASIFICACIÓN DE ARCILLAS
# =====================================================
def clasificar_arcilla(fe2o3, al2o3, absorcion):
    """Clasifica arcilla usando reglas editables de la BD (con fallback hardcoded)."""
    fe = float(fe2o3) if fe2o3 is not None else 0.0
    ab = float(absorcion) if absorcion is not None else 0.0

    calidad = "Sin clasificar"
    uso = "Sin clasificar"

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Clasificacion de calidad (por fe2o3)
        c.execute("SELECT nombre, min_valor, max_valor FROM clasificaciones_uso "
                  "WHERE tipo='calidad' ORDER BY orden")
        for nombre, min_v, max_v in c.fetchall():
            cumple = True
            if min_v is not None and fe < min_v:
                cumple = False
            if max_v is not None and fe >= max_v:
                cumple = False
            if cumple:
                calidad = nombre
                break

        # Clasificacion de uso (por absorcion)
        c.execute("SELECT nombre, min_valor, max_valor FROM clasificaciones_uso "
                  "WHERE tipo='uso' ORDER BY orden")
        for nombre, min_v, max_v in c.fetchall():
            cumple = True
            if min_v is not None and ab < min_v:
                cumple = False
            if max_v is not None and ab >= max_v:
                cumple = False
            if cumple:
                uso = nombre
                break

        conn.close()
    except Exception:
        # Fallback a clasificacion hardcoded
        if fe < 1.0:
            calidad = "Premium (Clara)"
        elif fe < 1.8:
            calidad = "Estandar (Beige/Rosada)"
        else:
            calidad = "Industrial (Roja)"

        if ab < 0.5:
            uso = "Porcelanato Tecnico"
        elif ab < 3.0:
            uso = "Gres Esmaltado / Vitrificado"
        elif ab < 6.0:
            uso = "Piso Stoneware / Semi-Gres"
        elif ab < 10.0:
            uso = "Revestimiento Pared / Monoporosa"
        else:
            uso = "Ladrilleria / Tejas"

    return calidad, uso


# =====================================================
# 3. ESTIMACIÓN DE PROPIEDADES EN MEZCLAS
# =====================================================
def estimar_propiedades_blend(componentes, df_muestras):
    """
    Calcula propiedades de una mezcla con modelos lineales y no-lineales
    basados en literatura cerámica (Reed, Norton, Kingery, Singer & Singer).
    Retorna dict con valores estimados y si son lineales o con corrección.
    """
    COLS_QUIMICA = ['sio2', 'al2o3', 'fe2o3', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc',
                    'so3', 'p2o5', 'mno', 'h2o', 'carbono', 'azufre']
    COLS_FISICA = ['absorcion', 'contraccion', 'l_color', 'a_color', 'b_color', 'densidad',
                   'superficie_especifica', 'mor_verde', 'mor_seco', 'mor_cocido_kgf', 'mor_cocido_mpa',
                   'pfefferkorn', 'limite_liquido', 'limite_plastico', 'indice_plasticidad',
                   'residuo_45um', 'menor_2um', 'd50', 'contraccion_secado', 'contraccion_total',
                   'porosidad_abierta', 'temperatura_coccion']

    resultado = {}
    metodo = {}  # 'lineal' o 'estimado'

    # Recoger fracciones y valores por propiedad
    fracciones = []
    valores_por_prop = {col: [] for col in COLS_QUIMICA + COLS_FISICA}

    for comp in componentes:
        row = df_muestras[df_muestras['nombre'] == comp['nombre']].iloc[0]
        frac = comp['pct'] / 100.0
        fracciones.append(frac)
        for col in COLS_QUIMICA + COLS_FISICA:
            val = row.get(col)
            valores_por_prop[col].append(float(val) if pd.notna(val) else None)

    n = len(fracciones)

    # --- QUÍMICA: Estrictamente lineal (ley de mezclas) ---
    for col in COLS_QUIMICA:
        vals = valores_por_prop[col]
        if any(v is not None for v in vals):
            total = sum(f * (v or 0) for f, v in zip(fracciones, vals))
            resultado[col] = total
            metodo[col] = 'lineal'

    # --- SUPERFICIE ESPECÍFICA: Lineal con factor de empaquetamiento 0.95 ---
    vals_ss = valores_por_prop['superficie_especifica']
    if any(v is not None for v in vals_ss):
        lineal = sum(f * (v or 0) for f, v in zip(fracciones, vals_ss))
        resultado['superficie_especifica'] = lineal * 0.95
        metodo['superficie_especifica'] = 'estimado'

    # --- MOR VERDE: No lineal, penalización -12% por interacción ---
    vals_mg = valores_por_prop['mor_verde']
    if any(v is not None for v in vals_mg):
        lineal = sum(f * (v or 0) for f, v in zip(fracciones, vals_mg))
        correction = 0.0
        for i in range(n):
            for j in range(i + 1, n):
                vi, vj = vals_mg[i] or 0, vals_mg[j] or 0
                correction += fracciones[i] * fracciones[j] * abs(vi - vj) * (-0.12)
        resultado['mor_verde'] = max(0, lineal + correction)
        metodo['mor_verde'] = 'estimado'

    # --- MOR SECO: Similar a verde, penalización -10% ---
    vals_ms = valores_por_prop['mor_seco']
    if any(v is not None for v in vals_ms):
        lineal = sum(f * (v or 0) for f, v in zip(fracciones, vals_ms))
        correction = 0.0
        for i in range(n):
            for j in range(i + 1, n):
                vi, vj = vals_ms[i] or 0, vals_ms[j] or 0
                correction += fracciones[i] * fracciones[j] * abs(vi - vj) * (-0.10)
        resultado['mor_seco'] = max(0, lineal + correction)
        metodo['mor_seco'] = 'estimado'

    # --- MOR COCIDO: No lineal, posible sinergia +10% por vitrificación ---
    for col_mor in ['mor_cocido_kgf', 'mor_cocido_mpa']:
        vals_mc = valores_por_prop[col_mor]
        if any(v is not None for v in vals_mc):
            lineal = sum(f * (v or 0) for f, v in zip(fracciones, vals_mc))
            correction = 0.0
            for i in range(n):
                for j in range(i + 1, n):
                    vi, vj = vals_mc[i] or 0, vals_mc[j] or 0
                    correction += fracciones[i] * fracciones[j] * abs(vi - vj) * 0.10
            resultado[col_mor] = max(0, lineal + correction)
            metodo[col_mor] = 'estimado'

    # --- PFEFFERKORN: No lineal, concavo negativo K=0.10 ---
    vals_pf = valores_por_prop['pfefferkorn']
    if any(v is not None for v in vals_pf):
        lineal = sum(f * (v or 0) for f, v in zip(fracciones, vals_pf))
        penalty = 0.0
        for i in range(n):
            for j in range(i + 1, n):
                vi, vj = vals_pf[i] or 0, vals_pf[j] or 0
                penalty += fracciones[i] * fracciones[j] * abs(vi - vj)
        resultado['pfefferkorn'] = max(0, lineal - 0.10 * penalty)
        metodo['pfefferkorn'] = 'estimado'

    # --- ATTERBERG (LL, LP, IP): Aproximadamente lineal ---
    for col_at in ['limite_liquido', 'limite_plastico', 'indice_plasticidad']:
        vals_at = valores_por_prop[col_at]
        if any(v is not None for v in vals_at):
            resultado[col_at] = sum(f * (v or 0) for f, v in zip(fracciones, vals_at))
            metodo[col_at] = 'lineal'

    # --- ABSORCIÓN: No lineal, sinergia de vitrificación -15% ---
    vals_aa = valores_por_prop['absorcion']
    if any(v is not None for v in vals_aa):
        lineal = sum(f * (v or 0) for f, v in zip(fracciones, vals_aa))
        correction = 0.0
        for i in range(n):
            for j in range(i + 1, n):
                vi, vj = vals_aa[i] or 0, vals_aa[j] or 0
                correction += fracciones[i] * fracciones[j] * abs(vi - vj)
        resultado['absorcion'] = max(0, lineal - 0.15 * correction)
        metodo['absorcion'] = 'estimado'

    # --- CONTRACCIÓN SECADO: Aprox lineal con -3% ---
    vals_cs = valores_por_prop['contraccion_secado']
    if any(v is not None for v in vals_cs):
        lineal = sum(f * (v or 0) for f, v in zip(fracciones, vals_cs))
        resultado['contraccion_secado'] = lineal * 0.97
        metodo['contraccion_secado'] = 'estimado'

    # --- CONTRACCIÓN COCCIÓN: No lineal, +8% sinergia ---
    vals_cc = valores_por_prop['contraccion']
    if any(v is not None for v in vals_cc):
        lineal = sum(f * (v or 0) for f, v in zip(fracciones, vals_cc))
        correction = 0.0
        for i in range(n):
            for j in range(i + 1, n):
                vi, vj = vals_cc[i] or 0, vals_cc[j] or 0
                correction += fracciones[i] * fracciones[j] * abs(vi - vj)
        resultado['contraccion'] = lineal + 0.08 * correction
        metodo['contraccion'] = 'estimado'

    # --- CONTRACCIÓN TOTAL: Suma secado + cocción ---
    if 'contraccion_secado' in resultado and 'contraccion' in resultado:
        resultado['contraccion_total'] = resultado['contraccion_secado'] + resultado['contraccion']
        metodo['contraccion_total'] = 'estimado'
    else:
        vals_ct = valores_por_prop['contraccion_total']
        if any(v is not None for v in vals_ct):
            resultado['contraccion_total'] = sum(f * (v or 0) for f, v in zip(fracciones, vals_ct))
            metodo['contraccion_total'] = 'lineal'

    # --- COLOR L*a*b*: Lineal crudo, corrección Kubelka-Munk para cocido ---
    vals_L = valores_por_prop['l_color']
    vals_a = valores_por_prop['a_color']
    vals_b = valores_por_prop['b_color']
    if any(v is not None for v in vals_L):
        L_blend = sum(f * (v or 50) for f, v in zip(fracciones, vals_L))
        a_blend = sum(f * (v or 0) for f, v in zip(fracciones, vals_a))
        b_blend = sum(f * (v or 0) for f, v in zip(fracciones, vals_b))
        # Corrección Kubelka-Munk: arcillas oscuras dominan
        L_range = max((v or 50) for v in vals_L) - min((v or 50) for v in vals_L)
        if L_range > 10:
            L_blend *= 0.93  # 7% más oscuro
            a_blend *= 1.08  # 8% más rojo
            metodo['l_color'] = 'estimado'
            metodo['a_color'] = 'estimado'
            metodo['b_color'] = 'estimado'
        else:
            metodo['l_color'] = 'lineal'
            metodo['a_color'] = 'lineal'
            metodo['b_color'] = 'lineal'
        resultado['l_color'] = L_blend
        resultado['a_color'] = a_blend
        resultado['b_color'] = b_blend

    # --- DENSIDAD, POROSIDAD, GRANULOMETRÍA: Lineal ---
    for col_lin in ['densidad', 'porosidad_abierta', 'residuo_45um', 'menor_2um', 'd50', 'temperatura_coccion']:
        vals_lin = valores_por_prop[col_lin]
        if any(v is not None for v in vals_lin):
            resultado[col_lin] = sum(f * (v or 0) for f, v in zip(fracciones, vals_lin))
            metodo[col_lin] = 'lineal'

    return resultado, metodo


def generar_ficha_pdf_blend(nombre_blend, objetivo, componentes, resultado, metodo, df_muestras):
    """Genera una ficha técnica PDF del blend/composite."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm, cm
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.5*cm, bottomMargin=1.5*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = getSampleStyleSheet()

    # Estilos personalizados
    title_style = ParagraphStyle('TitleCustom', parent=styles['Title'], fontSize=18,
                                  textColor=colors.HexColor('#1a237e'), spaceAfter=6)
    subtitle_style = ParagraphStyle('SubtitleCustom', parent=styles['Heading2'], fontSize=13,
                                     textColor=colors.HexColor('#283593'), spaceBefore=12, spaceAfter=6)
    normal_style = ParagraphStyle('NormalCustom', parent=styles['Normal'], fontSize=9, leading=12)
    small_style = ParagraphStyle('SmallCustom', parent=styles['Normal'], fontSize=7, leading=9,
                                  textColor=colors.HexColor('#666666'))
    center_style = ParagraphStyle('CenterCustom', parent=styles['Normal'], fontSize=10,
                                   alignment=TA_CENTER, textColor=colors.HexColor('#333333'))

    elements = []

    # --- ENCABEZADO ---
    logo_path = os.path.join(BASE_DIR, 'assets', 'geocivmet_logo.png')
    if os.path.exists(logo_path):
        try:
            logo = Image(logo_path, width=4*cm, height=4*cm)
            logo.hAlign = 'CENTER'
            elements.append(logo)
        except Exception:
            pass

    elements.append(Paragraph("GEOCIVMET Consultores Técnicos", title_style))
    elements.append(Paragraph("Geología • Ingeniería Civil • Minería • Metalurgia • Tecnología", center_style))
    elements.append(Spacer(1, 8*mm))

    # Línea separadora
    line_data = [['']]
    line_table = Table(line_data, colWidths=[doc.width])
    line_table.setStyle(TableStyle([('LINEBELOW', (0, 0), (-1, -1), 2, colors.HexColor('#1a237e'))]))
    elements.append(line_table)
    elements.append(Spacer(1, 5*mm))

    # --- INFORMACIÓN DEL BLEND ---
    elements.append(Paragraph("FICHA TÉCNICA DE MEZCLA / COMPOSITE", subtitle_style))

    info_data = [
        ['Nombre:', nombre_blend, 'Fecha:', date.today().strftime('%d/%m/%Y')],
        ['Objetivo:', objetivo, 'N° Componentes:', str(len(componentes))],
    ]
    info_table = Table(info_data, colWidths=[3*cm, 6*cm, 3.5*cm, 4*cm])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 5*mm))

    # --- COMPOSICIÓN ---
    elements.append(Paragraph("1. Composición de la Mezcla", subtitle_style))
    comp_header = ['Componente', 'Yacimiento', '%', 'SiO₂', 'Al₂O₃', 'Fe₂O₃']
    comp_rows = [comp_header]
    for comp in componentes:
        row = df_muestras[df_muestras['nombre'] == comp['nombre']].iloc[0]
        comp_rows.append([
            comp['nombre'],
            str(row.get('yacimiento', '-') or '-'),
            f"{comp['pct']:.1f}%",
            f"{row.get('sio2', 0) or 0:.2f}",
            f"{row.get('al2o3', 0) or 0:.2f}",
            f"{row.get('fe2o3', 0) or 0:.3f}",
        ])
    comp_table = Table(comp_rows, colWidths=[4*cm, 3.5*cm, 1.5*cm, 2.2*cm, 2.2*cm, 2.2*cm])
    comp_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a237e')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('ALIGN', (2, 0), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')]),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
    ]))
    elements.append(comp_table)
    elements.append(Spacer(1, 5*mm))

    # --- ANÁLISIS QUÍMICO ---
    LABELS_PDF = {
        'sio2': 'SiO₂ (%)', 'al2o3': 'Al₂O₃ (%)', 'fe2o3': 'Fe₂O₃ (%)',
        'tio2': 'TiO₂ (%)', 'cao': 'CaO (%)', 'mgo': 'MgO (%)',
        'k2o': 'K₂O (%)', 'na2o': 'Na₂O (%)', 'ppc': 'PPC (%)',
        'so3': 'SO₃ (%)', 'h2o': 'H₂O (%)', 'carbono': 'C (%)', 'azufre': 'S (%)',
        'absorcion': 'Absorción Agua (%)', 'contraccion': 'Contracción Cocción (%)',
        'contraccion_secado': 'Contracción Secado (%)', 'contraccion_total': 'Contracción Total (%)',
        'superficie_especifica': 'Sup. Específica (m²/g)',
        'l_color': 'L*', 'a_color': 'a*', 'b_color': 'b*',
        'mor_verde': 'MOR Verde (kgf/cm²)', 'mor_seco': 'MOR Seco (kgf/cm²)',
        'mor_cocido_kgf': 'MOR Cocido (kgf/cm²)', 'mor_cocido_mpa': 'MOR Cocido (MPa)',
        'pfefferkorn': 'Pfefferkorn (%)', 'limite_liquido': 'Límite Líquido (%)',
        'limite_plastico': 'Límite Plástico (%)', 'indice_plasticidad': 'Índice Plasticidad (%)',
        'residuo_45um': 'Residuo 45μm (%)', 'menor_2um': '< 2μm (%)', 'd50': 'D50 (μm)',
        'densidad': 'Densidad (g/cm³)', 'porosidad_abierta': 'Porosidad Abierta (%)',
    }

    # Química
    quim_keys = ['sio2', 'al2o3', 'fe2o3', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc', 'so3', 'h2o', 'carbono', 'azufre']
    quim_vals = [(k, resultado.get(k)) for k in quim_keys if k in resultado]
    if quim_vals:
        elements.append(Paragraph("2. Análisis Químico Estimado (FRX)", subtitle_style))
        q_header = ['Parámetro', 'Valor', 'Método']
        q_rows = [q_header]
        for k, v in quim_vals:
            met = "Promedio ponderado" if metodo.get(k) == 'lineal' else "Estimación*"
            q_rows.append([LABELS_PDF.get(k, k), f"{v:.3f}", met])
        q_table = Table(q_rows, colWidths=[5.5*cm, 3*cm, 5*cm])
        q_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2e7d32')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#e8f5e9')]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements.append(q_table)
        elements.append(Spacer(1, 5*mm))

    # Propiedades físicas y mecánicas
    fis_keys = ['absorcion', 'contraccion', 'contraccion_secado', 'contraccion_total',
                'superficie_especifica', 'densidad', 'porosidad_abierta',
                'mor_verde', 'mor_seco', 'mor_cocido_kgf', 'mor_cocido_mpa',
                'pfefferkorn', 'limite_liquido', 'limite_plastico', 'indice_plasticidad',
                'residuo_45um', 'menor_2um', 'd50', 'l_color', 'a_color', 'b_color']
    fis_vals = [(k, resultado.get(k)) for k in fis_keys if k in resultado]
    if fis_vals:
        elements.append(Paragraph("3. Propiedades Físicas y Mecánicas", subtitle_style))
        f_header = ['Parámetro', 'Valor', 'Método de Estimación']
        f_rows = [f_header]

        metodo_desc = {
            'lineal': 'Promedio ponderado',
            'estimado': 'Estimación con corrección*',
        }
        for k, v in fis_vals:
            met_text = metodo_desc.get(metodo.get(k, 'lineal'), 'Promedio ponderado')
            f_rows.append([LABELS_PDF.get(k, k), f"{v:.2f}", met_text])

        f_table = Table(f_rows, colWidths=[5.5*cm, 3*cm, 5*cm])
        f_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e65100')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff3e0')]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
        ]))
        elements.append(f_table)
        elements.append(Spacer(1, 5*mm))

    # Clasificación
    fe = resultado.get('fe2o3', 0)
    al = resultado.get('al2o3', 0)
    ab = resultado.get('absorcion', 0)
    calidad, uso = clasificar_arcilla(fe, al, ab)
    elements.append(Paragraph("4. Clasificación", subtitle_style))
    clas_data = [
        ['Clasificación:', calidad],
        ['Uso Estimado:', uso],
    ]
    clas_table = Table(clas_data, colWidths=[4*cm, 10*cm])
    clas_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    elements.append(clas_table)
    elements.append(Spacer(1, 8*mm))

    # --- NOTAS ---
    elements.append(Paragraph("Notas Metodológicas", subtitle_style))
    notas = [
        "• Los valores químicos (óxidos, PPC, LOI) se calculan por promedio ponderado lineal (ley de mezclas).",
        "• Absorción de agua: modelo no-lineal con sinergia de vitrificación (-15%). Ref: Kingery et al.",
        "• MOR Verde/Seco: modelo cuadrático con penalización -10 a -12%. Ref: Reed, Principles of Ceramics Processing.",
        "• MOR Cocido: modelo cuadrático con sinergia de vitrificación +10%. Ref: Norton, Fine Ceramics.",
        "• Pfefferkorn: modelo concavo-negativo K=0.10. Ref: Bourry; Singer & Singer, Industrial Ceramics.",
        "• Superficie Específica: lineal con factor de empaquetamiento 0.95. Ref: Reed.",
        "• Color L*a*b*: corrección Kubelka-Munk cuando rango L* > 10 (L*×0.93, a*×1.08).",
        "• Contracción secado: lineal ×0.97. Contracción cocción: cuadrática +8%.",
        "• (*) Los valores marcados como 'Estimación con corrección' son aproximaciones teóricas.",
        "• Se recomienda validar con ensayos de laboratorio para formulaciones definitivas.",
    ]
    for nota in notas:
        elements.append(Paragraph(nota, small_style))

    elements.append(Spacer(1, 10*mm))
    elements.append(Paragraph(
        f"Documento generado por GEOCIVMET Consultores Técnicos — {date.today().strftime('%d/%m/%Y')}",
        ParagraphStyle('Footer', parent=styles['Normal'], fontSize=7, alignment=TA_CENTER,
                       textColor=colors.HexColor('#999999'))
    ))

    doc.build(elements)
    buf.seek(0)
    return buf


# =====================================================
# 4. GUARDADO INTELIGENTE
# =====================================================
def guardar_muestra(data, usuario="sistema"):
    """Guarda una muestra completa. data es un dict con todos los campos."""
    conn = get_conn()
    c = conn.cursor()
    try:
        # Generar código único: si es vacío, "ND", "DRX", genérico → auto-generar
        codigo_raw = str(data.get('codigo_lab') or '').strip()
        codigos_genericos = ['nd', 'n/d', 'n/a', 'na', 'drx', 'ppi redistribuido', 'sin codigo', 'pending', '']
        if codigo_raw.lower() in codigos_genericos:
            # Auto-generar código basado en nombre de muestra
            nombre_base = str(data.get('nombre', 'M')).strip()[:8].upper().replace(' ', '')
            codigo = f"{nombre_base}-{random.randint(1000, 9999)}"
        else:
            codigo = codigo_raw

        # Si el código ya existe, agregar sufijo
        c.execute("SELECT COUNT(*) FROM muestras WHERE codigo_lab = ?", (codigo,))
        if c.fetchone()[0] > 0:
            codigo = f"{codigo}-{random.randint(100, 999)}"

        fecha = data.get('fecha') or date.today().isoformat()
        nombre = str(data.get('nombre', '')).strip()
        if not nombre:
            return False, "Nombre vacío"

        # Chequeo de duplicados por nombre
        c.execute("SELECT id FROM muestras WHERE nombre = ?", (nombre,))
        if c.fetchone():
            return False, f"Duplicado: '{nombre}'"

        c.execute("""INSERT INTO muestras
            (nombre, codigo_lab, yacimiento, estado, municipio, latitud, longitud, fecha, observaciones, creado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (nombre, codigo,
             data.get('yacimiento', 'General'),
             data.get('estado', ''),
             data.get('municipio', ''),
             data.get('latitud'),
             data.get('longitud'),
             fecha,
             data.get('observaciones', ''),
             usuario))
        mid = c.lastrowid

        c.execute("""INSERT INTO quimica
            (muestra_id, fe2o3, al2o3, sio2, tio2, cao, mgo, k2o, na2o, ppc,
             so3, p2o5, mno, h2o, carbono, azufre)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (mid,
             _float(data.get('fe2o3')),
             _float(data.get('al2o3')),
             _float(data.get('sio2')),
             _float(data.get('tio2')),
             _float(data.get('cao')),
             _float(data.get('mgo')),
             _float(data.get('k2o')),
             _float(data.get('na2o')),
             _float(data.get('ppc')),
             _float(data.get('so3')),
             _float(data.get('p2o5')),
             _float(data.get('mno')),
             _float(data.get('h2o')),
             _float(data.get('carbono')),
             _float(data.get('azufre'))))

        c.execute("""INSERT INTO fisica
            (muestra_id, absorcion, contraccion, l_color, a_color, b_color,
             resistencia_flexion, densidad, temperatura_coccion,
             superficie_especifica, mor_verde, mor_seco, mor_cocido_kgf, mor_cocido_mpa,
             pfefferkorn, limite_liquido, limite_plastico, indice_plasticidad,
             residuo_45um, menor_2um, d50,
             contraccion_secado, contraccion_total, porosidad_abierta)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (mid,
             _float(data.get('absorcion')),
             _float(data.get('contraccion')),
             _float(data.get('l_color')),
             _float(data.get('a_color')),
             _float(data.get('b_color')),
             _float(data.get('resistencia_flexion')),
             _float(data.get('densidad')),
             _float(data.get('temperatura_coccion')),
             _float(data.get('superficie_especifica')),
             _float(data.get('mor_verde')),
             _float(data.get('mor_seco')),
             _float(data.get('mor_cocido_kgf')),
             _float(data.get('mor_cocido_mpa')),
             _float(data.get('pfefferkorn')),
             _float(data.get('limite_liquido')),
             _float(data.get('limite_plastico')),
             _float(data.get('indice_plasticidad')),
             _float(data.get('residuo_45um')),
             _float(data.get('menor_2um')),
             _float(data.get('d50')),
             _float(data.get('contraccion_secado')),
             _float(data.get('contraccion_total')),
             _float(data.get('porosidad_abierta'))))

        conn.commit()
        return True, f"OK - ID {mid}"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()


def _float(val):
    """Convierte a float de forma segura."""
    if val is None or val == '' or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def eliminar_muestra(muestra_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM fisica WHERE muestra_id = ?", (muestra_id,))
    c.execute("DELETE FROM quimica WHERE muestra_id = ?", (muestra_id,))
    c.execute("DELETE FROM muestras WHERE id = ?", (muestra_id,))
    conn.commit()
    conn.close()


def obtener_datos_completos():
    conn = get_conn()
    df = pd.read_sql("""
        SELECT m.id, m.nombre, m.codigo_lab, m.yacimiento, m.estado, m.municipio,
               m.fecha, m.observaciones,
               q.fe2o3, q.al2o3, q.sio2, q.tio2, q.cao, q.mgo, q.k2o, q.na2o, q.ppc,
               q.so3, q.p2o5, q.mno, q.h2o, q.carbono, q.azufre,
               f.absorcion, f.contraccion, f.l_color, f.a_color, f.b_color,
               f.resistencia_flexion, f.densidad, f.temperatura_coccion,
               f.superficie_especifica, f.mor_verde, f.mor_seco,
               f.mor_cocido_kgf, f.mor_cocido_mpa,
               f.pfefferkorn, f.limite_liquido, f.limite_plastico, f.indice_plasticidad,
               f.residuo_45um, f.menor_2um, f.d50,
               f.contraccion_secado, f.contraccion_total, f.porosidad_abierta
        FROM muestras m
        LEFT JOIN quimica q ON m.id = q.muestra_id
        LEFT JOIN fisica f ON m.id = f.muestra_id
        ORDER BY m.nombre
    """, conn)
    conn.close()
    return df


# =====================================================
# 4. AUTENTICACIÓN
# =====================================================
# Modulos protegidos por permisos RBAC
MODULOS_PROTEGIDOS = [
    "Control de Calidad",
    "Curvas de Gresificación",
    "Fórmula Seger / UMF",
    "Predicción de Color",
    "Optimizador de Mezclas",
    "Cargar desde Excel",
    "Agregar Muestra Manual",
    "Blender & Composite",
]


def verificar_usuario(username, password):
    """Verifica credenciales. Retorna (id, rol, nombre, estado, permisos) o None."""
    conn = get_conn()
    c = conn.cursor()
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    c.execute("""SELECT id, rol, nombre_completo, estado, permisos
                 FROM usuarios WHERE username = ? AND password_hash = ?""",
              (username, pw_hash))
    result = c.fetchone()
    conn.close()
    return result


def crear_usuario(username, password, rol, nombre_completo, estado='pendiente', permisos=''):
    """Crea un usuario. Los nuevos clientes quedan en estado 'pendiente'."""
    conn = get_conn()
    c = conn.cursor()
    pw_hash = hashlib.sha256(password.encode()).hexdigest()
    try:
        c.execute("""INSERT INTO usuarios
                     (username, password_hash, rol, nombre_completo, estado, permisos)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (username, pw_hash, rol, nombre_completo, estado, permisos))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def usuario_tiene_permiso(user_info, modulo):
    """Verifica si el usuario tiene permiso para acceder a un modulo."""
    if user_info is None:
        return False
    if user_info.get('rol') == 'admin':
        return True
    permisos = user_info.get('permisos', '')
    if not permisos:
        return False
    lista = [p.strip() for p in permisos.split(',') if p.strip()]
    return modulo in lista


def login_section():
    """Widget de login/registro en sidebar. Retorna (logged_in, user_info)."""
    if 'user_logged' not in st.session_state:
        st.session_state.user_logged = False
        st.session_state.user_info = None

    if st.session_state.user_logged:
        info = st.session_state.user_info
        rol_badge = "🔑 Admin" if info['rol'] == 'admin' else "👤 Cliente"
        st.sidebar.success(f"{info['nombre']}  ({rol_badge})")
        if st.sidebar.button("Cerrar Sesion"):
            st.session_state.user_logged = False
            st.session_state.user_info = None
            st.rerun()
        return True, info

    tab_login, tab_registro = st.sidebar.tabs(["Iniciar Sesion", "Registrarse"])

    with tab_login:
        user = st.text_input("Usuario", key="login_user")
        pw = st.text_input("Contrasena", type="password", key="login_pw")
        if st.button("Entrar", key="btn_login"):
            if not user or not pw:
                st.error("Ingrese usuario y contrasena.")
            else:
                result = verificar_usuario(user, pw)
                if result:
                    estado = result[3] or 'activo'
                    if estado == 'pendiente':
                        st.warning("⏳ Tu cuenta esta pendiente de aprobacion. "
                                   "Contacta al administrador.")
                    elif estado == 'inactivo':
                        st.error("🚫 Tu cuenta ha sido desactivada.")
                    else:
                        st.session_state.user_logged = True
                        st.session_state.user_info = {
                            'id': result[0], 'rol': result[1],
                            'nombre': result[2],
                            'estado': estado,
                            'permisos': result[4] or '',
                        }
                        st.rerun()
                else:
                    st.error("Credenciales incorrectas")

    with tab_registro:
        st.caption("Los registros nuevos requieren aprobacion del administrador.")
        reg_nombre = st.text_input("Nombre completo", key="reg_nombre")
        reg_user = st.text_input("Usuario deseado", key="reg_user")
        reg_pw = st.text_input("Contrasena", type="password", key="reg_pw")
        reg_pw2 = st.text_input("Confirmar contrasena", type="password", key="reg_pw2")
        if st.button("Solicitar Registro", key="btn_registro"):
            if not reg_nombre.strip() or not reg_user.strip() or not reg_pw:
                st.error("Todos los campos son obligatorios.")
            elif reg_pw != reg_pw2:
                st.error("Las contrasenas no coinciden.")
            elif len(reg_pw) < 4:
                st.error("La contrasena debe tener al menos 4 caracteres.")
            else:
                ok = crear_usuario(reg_user.strip(), reg_pw, 'cliente',
                                   reg_nombre.strip(), 'pendiente', '')
                if ok:
                    st.success("✅ Registro enviado. Tu cuenta sera revisada por el administrador.")
                else:
                    st.error("El nombre de usuario ya esta registrado.")

    return False, None


# =====================================================
# 5. INTERFAZ PRINCIPAL
# =====================================================
# =====================================================
# ESPECIFICACIONES Y CONTROL DE CALIDAD
# =====================================================
def obtener_especificaciones():
    """Retorna DataFrame con todas las especificaciones."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM especificaciones", conn)
    conn.close()
    return df


def obtener_productos():
    """Retorna lista de productos disponibles."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT producto FROM especificaciones ORDER BY producto")
    productos = [r[0] for r in c.fetchall()]
    conn.close()
    return productos


def evaluar_muestra_vs_specs(row, specs_producto):
    """
    Evalúa una muestra contra las specs de un producto.
    Retorna: (semaforo, n_cumple, n_falla, detalles)
      semaforo: 'verde', 'amarillo', 'rojo'
      detalles: lista de dicts con cada parámetro evaluado
    """
    detalles = []
    n_cumple = 0
    n_falla = 0

    for _, spec in specs_producto.iterrows():
        param = spec['parametro']
        val = row.get(param)
        min_v = spec['min_valor']
        max_v = spec['max_valor']

        if val is None or (isinstance(val, float) and pd.isna(val)):
            detalles.append({
                'parametro': param, 'valor': None, 'min': min_v, 'max': max_v,
                'cumple': None, 'estado': 'sin_dato'
            })
            continue

        val = float(val)
        cumple = True
        if min_v is not None and val < min_v:
            cumple = False
        if max_v is not None and val > max_v:
            cumple = False

        if cumple:
            n_cumple += 1
        else:
            n_falla += 1

        detalles.append({
            'parametro': param, 'valor': val, 'min': min_v, 'max': max_v,
            'cumple': cumple, 'estado': 'cumple' if cumple else 'falla',
            'unidad': spec.get('unidad', ''),
        })

    total_evaluados = n_cumple + n_falla
    if n_falla == 0:
        semaforo = 'verde'
    elif n_falla <= 2:
        semaforo = 'amarillo'
    else:
        semaforo = 'rojo'

    return semaforo, n_cumple, n_falla, detalles


PARAM_LABELS_SPEC = {
    'absorcion': 'Absorcion (%)', 'mor_cocido_mpa': 'MOR Cocido (MPa)',
    'fe2o3': 'Fe₂O₃ (%)', 'al2o3': 'Al₂O₃ (%)', 'contraccion': 'Contraccion (%)',
    'sio2': 'SiO₂ (%)', 'mor_cocido_kgf': 'MOR Cocido (kgf/cm²)',
    'superficie_especifica': 'Sup. Especifica', 'porosidad_abierta': 'Porosidad (%)',
}

SEMAFORO_ICONS = {'verde': '🟢', 'amarillo': '🟡', 'rojo': '🔴'}
SEMAFORO_COLORS = {'verde': '#4CAF50', 'amarillo': '#FFC107', 'rojo': '#F44336'}

# Pesos por parámetro para industria cerámica
PESOS_PARAMETRO = {
    'fe2o3': 25,
    'absorcion': 25,
    'al2o3': 15,
    'mor_cocido_mpa': 10,
    'l_color': 10,
    'contraccion': 8,
    'sio2': 7,
    'pfefferkorn': 8,
    'superficie_especifica': 7,
    'ppc': 5,
}
PESO_OTROS = 5

# ═══════════════════════════════════════════════════════
# ARCILLAS OBJETIVO DE REFERENCIA
# ═══════════════════════════════════════════════════════
ARCILLA_OBJ_UCRANIA_NUEVA = {
    'nombre': 'OBJ 1 — Ucrania Nueva',
    'sio2': 60.00, 'al2o3': 26.50, 'fe2o3': 0.90, 'tio2': 1.40,
    'cao': 0.30, 'mgo': 0.50, 'k2o': 1.90, 'na2o': 0.15, 'ppc': 7.50,
    'absorcion': 0.50, 'contraccion': 12.50,
    'l_color': 80.75, 'a_color': 3.05, 'b_color': 16.18,
    'superficie_especifica': 140, 'mor_seco': 45,
    'pfefferkorn': 36.2,
}
ARCILLA_OBJ_UCRANIA_ANT = {
    'nombre': 'OBJ 2 — Ucrania Ant.',
    'sio2': 60.10, 'al2o3': 26.02, 'fe2o3': 1.00, 'tio2': 1.50,
    'cao': 0.30, 'mgo': 0.52, 'k2o': 2.30, 'na2o': 0.44, 'ppc': 7.60,
    'absorcion': 0.23, 'contraccion': 6.88,
    'l_color': 69.30, 'a_color': 6.40, 'b_color': 21.17,
    'superficie_especifica': 140,
    'pfefferkorn': 26.0,
}
ARCILLA_OBJ_INDIA = {
    'nombre': 'OBJ 3 — India AG-30',
    'sio2': 52.00, 'al2o3': 30.00, 'fe2o3': 1.20, 'tio2': 1.10,
    'cao': 1.60, 'mgo': 0.40, 'k2o': 1.20, 'na2o': 0.30, 'ppc': 12.20,
    'absorcion': 1.80, 'contraccion': 9.00,
    'l_color': 78.00, 'a_color': 0.50, 'b_color': 13.00,
    'superficie_especifica': 105,
    'pfefferkorn': 32.5,
}
ARCILLA_OBJ_UCRA_TECHNIC = {
    'nombre': 'OBJ 4 — Ucra Technic',
    'sio2': 68.00, 'al2o3': 22.00, 'fe2o3': 1.10, 'tio2': 1.50,
    'cao': 0.30, 'mgo': 0.35, 'k2o': 1.90, 'na2o': 0.15, 'ppc': 6.30,
    'absorcion': 1.25, 'contraccion': 11.11,
    'l_color': 78.38, 'a_color': 4.11, 'b_color': 17.90,
    'superficie_especifica': 80, 'mor_seco': 38,
    'pfefferkorn': 22.0,
}
ARCILLAS_OBJETIVO = {
    'OBJ 1 — Ucrania Nueva': ARCILLA_OBJ_UCRANIA_NUEVA,
    'OBJ 2 — Ucrania Ant.': ARCILLA_OBJ_UCRANIA_ANT,
    'OBJ 3 — India AG-30': ARCILLA_OBJ_INDIA,
    'OBJ 4 — Ucra Technic': ARCILLA_OBJ_UCRA_TECHNIC,
}

# Tolerancias porcentuales para comparación contra arcilla objetivo
TOLERANCIAS_COMPARACION = {
    'fe2o3': 0.20,      # ±20% del valor objetivo
    'al2o3': 0.15,
    'sio2': 0.10,
    'absorcion': 0.25,
    'contraccion': 0.20,
    'l_color': 0.08,
    'a_color': 0.50,
    'b_color': 0.30,
    'mor_cocido_mpa': 0.20,
    'superficie_especifica': 0.20,
    'pfefferkorn': 0.25,
    'ppc': 0.25,
    'mor_seco': 0.25,
}


def calcular_scoring(muestra_row, specs_producto):
    """
    Calcula un score de aptitud (0-100) de una muestra contra las specs de un producto.
    Para cada parametro de la spec:
      - Dentro de spec: 100 pts
      - Fuera por <20%: 70 pts
      - Fuera por 20-50%: 40 pts
      - Fuera por >50%: 0 pts
    El score final es el promedio ponderado según PESOS_PARAMETRO.
    """
    scores_parciales = []
    pesos = []

    for _, spec in specs_producto.iterrows():
        param = spec['parametro']
        val = muestra_row.get(param)
        min_v = spec['min_valor']
        max_v = spec['max_valor']

        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue

        val = float(val)
        peso = PESOS_PARAMETRO.get(param, PESO_OTROS)

        # Determinar si cumple y cuanto se desvía
        dentro = True
        max_desvio_pct = 0.0

        if min_v is not None and val < min_v:
            dentro = False
            ref = abs(min_v) if min_v != 0 else 1.0
            max_desvio_pct = max(max_desvio_pct, abs(min_v - val) / ref * 100)
        if max_v is not None and val > max_v:
            dentro = False
            ref = abs(max_v) if max_v != 0 else 1.0
            max_desvio_pct = max(max_desvio_pct, abs(val - max_v) / ref * 100)

        if dentro:
            score_param = 100
        elif max_desvio_pct < 20:
            score_param = 70
        elif max_desvio_pct <= 50:
            score_param = 40
        else:
            score_param = 0

        scores_parciales.append(score_param)
        pesos.append(peso)

    if not scores_parciales:
        return None, []

    total_peso = sum(pesos)
    score_final = sum(s * p for s, p in zip(scores_parciales, pesos)) / total_peso if total_peso > 0 else 0
    return round(score_final, 1), list(zip(
        [spec['parametro'] for _, spec in specs_producto.iterrows() if muestra_row.get(spec['parametro']) is not None
         and not (isinstance(muestra_row.get(spec['parametro']), float) and pd.isna(muestra_row.get(spec['parametro'])))],
        scores_parciales, pesos
    ))


def calcular_scoring_vs_objetivo(muestra_row, objetivo):
    """
    Calcula un score de similitud (0-100) de una muestra contra una arcilla objetivo.
    Para cada parametro del objetivo:
      - Dentro de tolerancia: 100 pts
      - Fuera por <25% adicional: 70 pts
      - Fuera por 25-60% adicional: 40 pts
      - Fuera por >60% adicional: 0 pts
    El score final es el promedio ponderado segun PESOS_PARAMETRO.
    """
    scores_parciales = []
    pesos = []
    detalles = []

    PARAMS_COMPARAR = ['fe2o3', 'al2o3', 'sio2', 'absorcion', 'contraccion',
                        'l_color', 'a_color', 'b_color', 'mor_cocido_mpa',
                        'superficie_especifica', 'tio2', 'cao', 'mgo', 'k2o', 'na2o',
                        'pfefferkorn', 'ppc', 'mor_seco']

    for param in PARAMS_COMPARAR:
        val_obj = objetivo.get(param)
        val_muestra = muestra_row.get(param)

        if val_obj is None or val_muestra is None:
            continue
        if isinstance(val_muestra, float) and pd.isna(val_muestra):
            continue
        if isinstance(val_obj, float) and pd.isna(val_obj):
            continue

        val_muestra = float(val_muestra)
        val_obj = float(val_obj)
        peso = PESOS_PARAMETRO.get(param, PESO_OTROS)
        tolerancia = TOLERANCIAS_COMPARACION.get(param, 0.20)

        # Calcular desviación porcentual respecto al objetivo
        ref = abs(val_obj) if val_obj != 0 else 1.0
        desviacion_pct = abs(val_muestra - val_obj) / ref

        if desviacion_pct <= tolerancia:
            score_param = 100
        elif desviacion_pct <= tolerancia + 0.25:
            score_param = 70
        elif desviacion_pct <= tolerancia + 0.60:
            score_param = 40
        else:
            score_param = 0

        scores_parciales.append(score_param)
        pesos.append(peso)
        detalles.append((param, val_muestra, val_obj, score_param, desviacion_pct * 100))

    if not scores_parciales:
        return None, []

    total_peso = sum(pesos)
    score_final = sum(s * p for s, p in zip(scores_parciales, pesos)) / total_peso if total_peso > 0 else 0
    return round(score_final, 1), detalles


def _barra_progreso_html(score):
    """Genera HTML de barra de progreso visual para un score 0-100."""
    if score is None:
        return '<span style="color:#888">Sin datos</span>'
    if score >= 80:
        color = '#4CAF50'
    elif score >= 60:
        color = '#FFC107'
    elif score >= 40:
        color = '#FF9800'
    else:
        color = '#F44336'
    return (
        f'<div style="background:#e0e0e0;border-radius:8px;height:18px;width:100%;position:relative">'
        f'<div style="background:{color};border-radius:8px;height:18px;width:{min(score,100):.0f}%"></div>'
        f'<span style="position:absolute;top:0;left:50%;transform:translateX(-50%);font-size:11px;'
        f'font-weight:700;line-height:18px;color:#222">{score:.0f}</span></div>'
    )


def page_ranking_aptitud():
    """Ranking de similitud: muestras comparadas contra arcillas objetivo de referencia."""
    st.title("🏆 Ranking de Similitud vs Arcillas Objetivo")
    st.markdown('<div class="section-badge">📊 Comparacion ponderada contra arcillas de referencia internacional</div>',
                unsafe_allow_html=True)
    st.caption("Score de similitud (0-100) de cada muestra contra las arcillas objetivo "
               "OBJ 1 Ucrania Nueva, OBJ 2 Ucrania Ant., OBJ 3 India AG-30 y OBJ 4 Ucra Technic.")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    # Selector de arcilla objetivo
    tab_todas, tab_ukr_nueva, tab_ukr_ant, tab_india, tab_technic = st.tabs([
        "📊 Comparacion Cuadruple",
        "🇺🇦 vs OBJ 1 Ucrania Nueva",
        "🇺🇦 vs OBJ 2 Ucrania Ant.",
        "🇮🇳 vs OBJ 3 India AG-30",
        "🇺🇦 vs OBJ 4 Ucra Technic"])

    def _calcular_rankings(df, objetivo):
        """Calcula rankings de todas las muestras contra un objetivo."""
        resultados = []
        for _, row in df.iterrows():
            score, detalles = calcular_scoring_vs_objetivo(row, objetivo)
            resultados.append({
                'id': row['id'],
                'Muestra': row['nombre'],
                'Yacimiento': row.get('yacimiento', ''),
                'Fe2O3': row.get('fe2o3'),
                'Al2O3': row.get('al2o3'),
                'SiO2': row.get('sio2'),
                'Absorcion': row.get('absorcion'),
                'Contraccion': row.get('contraccion'),
                'L*': row.get('l_color'),
                'Score': score,
                'detalles': detalles,
            })
        df_rank = pd.DataFrame(resultados).dropna(subset=['Score'])
        if not df_rank.empty:
            df_rank = df_rank.sort_values('Score', ascending=False).reset_index(drop=True)
            df_rank.index = df_rank.index + 1
            df_rank.index.name = '#'
        return df_rank

    def _mostrar_ficha_objetivo(obj):
        """Muestra tarjeta con las propiedades de la arcilla objetivo."""
        quim_items = [(k.upper().replace('2O3','₂O₃').replace('O2','O₂').replace('2O','₂O'),
                       f"{v:.2f}%") for k, v in obj.items()
                      if k in ('sio2','al2o3','fe2o3','tio2','cao','mgo','k2o','na2o') and v is not None]
        fis_items = []
        if obj.get('absorcion') is not None: fis_items.append(('Absorcion', f"{obj['absorcion']:.1f}%"))
        if obj.get('contraccion') is not None: fis_items.append(('Contraccion', f"{obj['contraccion']:.1f}%"))
        if obj.get('l_color') is not None: fis_items.append(('L*', f"{obj['l_color']:.1f}"))
        if obj.get('a_color') is not None: fis_items.append(('a*', f"{obj['a_color']:.1f}"))
        if obj.get('b_color') is not None: fis_items.append(('b*', f"{obj['b_color']:.1f}"))
        if obj.get('superficie_especifica') is not None: fis_items.append(('Sup. Espec.', f"{obj['superficie_especifica']}"))
        if obj.get('mor_cocido_mpa') is not None: fis_items.append(('MOR', f"{obj['mor_cocido_mpa']} MPa"))

        quim_html = ''.join(f'<span style="background:#e0e7ff;color:#3730a3;padding:2px 8px;'
                            f'border-radius:6px;font-size:11px;margin:2px">'
                            f'{k}: {v}</span>' for k, v in quim_items)
        fis_html = ''.join(f'<span style="background:#fef3c7;color:#92400e;padding:2px 8px;'
                           f'border-radius:6px;font-size:11px;margin:2px">'
                           f'{k}: {v}</span>' for k, v in fis_items)

        st.markdown(f"""
        <div style="background:linear-gradient(135deg,#f0f9ff,#e0f2fe);border:1px solid #93c5fd;
             border-radius:12px;padding:14px;margin-bottom:14px">
            <div style="font-weight:800;color:#1e40af;font-size:14px;margin-bottom:8px">
                🎯 {obj['nombre']}</div>
            <div style="margin-bottom:6px"><b style="font-size:11px;color:#475569">Quimica:</b><br>
                {quim_html}</div>
            <div><b style="font-size:11px;color:#475569">Fisicas:</b><br>
                {fis_html}</div>
        </div>""", unsafe_allow_html=True)

    def _render_ranking_completo(df_rank, objetivo, obj_name, key_suffix):
        """Renderiza ranking completo con KPIs, Top5, tabla, scatter, export."""
        if df_rank.empty:
            st.warning("Ninguna muestra tiene datos suficientes para evaluar.")
            return

        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Muestras Evaluadas", len(df_rank))
        k2.metric("Score Promedio", f"{df_rank['Score'].mean():.1f}")
        k3.metric("Mejor Score", f"{df_rank['Score'].max():.1f}")
        n_sim = len(df_rank[df_rank['Score'] >= 70])
        k4.metric("Similares (>=70)", n_sim)

        st.markdown("---")

        # Top 5
        st.subheader(f"Top 5 Mas Similares a {obj_name}")
        top5 = df_rank.head(5)
        if top5['Score'].iloc[0] >= 90:
            st.balloons()
            st.success(f"🎉 ¡{top5['Muestra'].iloc[0]} tiene una similitud excepcional de "
                       f"{top5['Score'].iloc[0]:.0f} con {obj_name}!")

        cols_top = st.columns(min(5, len(top5)))
        for i, (_, r) in enumerate(top5.iterrows()):
            with cols_top[i]:
                medal = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣'][i]
                bg = '#e8f5e9' if r['Score'] >= 80 else ('#fff8e1' if r['Score'] >= 60 else '#fbe9e7')
                st.markdown(f"""
                <div style="background:{bg};border-radius:12px;padding:14px;text-align:center;
                            border:1px solid rgba(0,0,0,0.08);min-height:160px">
                    <div style="font-size:28px">{medal}</div>
                    <div style="font-weight:700;font-size:13px;margin:4px 0">{r['Muestra']}</div>
                    <div style="font-size:11px;color:#666">{r['Yacimiento'] or '—'}</div>
                    <div style="font-size:26px;font-weight:800;color:{'#4CAF50' if r['Score']>=80 else '#FF9800'};
                         margin-top:6px">{r['Score']:.0f}</div>
                    <div style="font-size:9px;color:#888">vs {obj_name}</div>
                </div>""", unsafe_allow_html=True)

        st.markdown("---")

        # Tabla completa
        st.subheader("Ranking Completo")
        html_rows = ""
        for pos, (_, r) in enumerate(df_rank.iterrows(), 1):
            barra = _barra_progreso_html(r['Score'])
            fe = f"{r['Fe2O3']:.3f}" if pd.notna(r.get('Fe2O3')) else '—'
            al = f"{r['Al2O3']:.2f}" if pd.notna(r.get('Al2O3')) else '—'
            si = f"{r['SiO2']:.2f}" if pd.notna(r.get('SiO2')) else '—'
            aa = f"{r['Absorcion']:.2f}" if pd.notna(r.get('Absorcion')) else '—'
            lc = f"{r['L*']:.1f}" if pd.notna(r.get('L*')) else '—'
            html_rows += f"""<tr>
                <td style="text-align:center;font-weight:700">{pos}</td>
                <td>{r['Muestra']}</td>
                <td>{r['Yacimiento'] or '—'}</td>
                <td style="text-align:center">{fe}</td>
                <td style="text-align:center">{al}</td>
                <td style="text-align:center">{si}</td>
                <td style="text-align:center">{aa}</td>
                <td style="text-align:center">{lc}</td>
                <td style="min-width:120px">{barra}</td>
            </tr>"""

        st.markdown(f"""
        <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;font-size:12px">
        <thead><tr style="background:#1b3a4b;color:white">
            <th style="padding:8px;border-radius:6px 0 0 0">#</th>
            <th style="padding:8px">Muestra</th>
            <th style="padding:8px">Yacimiento</th>
            <th style="padding:8px">Fe₂O₃</th>
            <th style="padding:8px">Al₂O₃</th>
            <th style="padding:8px">SiO₂</th>
            <th style="padding:8px">Absorcion</th>
            <th style="padding:8px">L*</th>
            <th style="padding:8px;border-radius:0 6px 0 0">Score</th>
        </tr></thead>
        <tbody>{html_rows}</tbody>
        </table></div>
        """, unsafe_allow_html=True)

        st.markdown("")

        # Detalles expandibles por muestra (top 10)
        with st.expander("🔍 Ver detalles de comparacion (Top 10)", expanded=False):
            for _, r in df_rank.head(10).iterrows():
                if not r['detalles']:
                    continue
                st.markdown(f"**{r['Muestra']}** — Score: **{r['Score']:.1f}**")
                det_data = []
                for param, val_m, val_o, sc, desv in r['detalles']:
                    label = PARAM_LABELS_SPEC.get(param, param)
                    det_data.append({
                        'Parametro': label,
                        'Muestra': f"{val_m:.2f}",
                        'Objetivo': f"{val_o:.2f}",
                        'Desv. %': f"{desv:.1f}%",
                        'Score': sc
                    })
                if det_data:
                    st.dataframe(pd.DataFrame(det_data), use_container_width=True, hide_index=True)
                st.markdown("---")

        # Scatter: Score vs Fe2O3
        st.subheader(f"Score vs Fe₂O₃ — {obj_name}")
        df_scatter = df_rank.dropna(subset=['Fe2O3'])
        if not df_scatter.empty:
            fig_sc = px.scatter(
                df_scatter, x='Fe2O3', y='Score', color='Yacimiento',
                hover_name='Muestra', size_max=12,
                labels={'Fe2O3': 'Fe₂O₃ (%)', 'Score': 'Score Similitud'},
                title=f"Similitud vs Fe₂O₃ — {obj_name}",
            )
            fig_sc.add_hline(y=70, line_dash='dash', line_color='green',
                             annotation_text='Umbral similar (70)')
            # Linea vertical del objetivo
            fe_obj = objetivo.get('fe2o3')
            if fe_obj is not None:
                fig_sc.add_vline(x=fe_obj, line_dash='dot', line_color='blue',
                                 annotation_text=f'OBJ Fe₂O₃={fe_obj}')
            fig_sc.update_layout(height=500)
            st.plotly_chart(fig_sc, use_container_width=True)

        # Radar comparativo (Top 3 vs Objetivo)
        st.subheader(f"Radar: Top 3 vs {obj_name}")
        radar_params = ['fe2o3', 'al2o3', 'sio2', 'absorcion', 'contraccion', 'l_color']
        radar_labels = ['Fe₂O₃', 'Al₂O₃', 'SiO₂', 'Absorcion', 'Contraccion', 'L*']

        fig_radar = go.Figure()
        # Objetivo
        obj_vals = [objetivo.get(p, 0) for p in radar_params]
        fig_radar.add_trace(go.Scatterpolar(
            r=obj_vals + [obj_vals[0]], theta=radar_labels + [radar_labels[0]],
            fill='toself', name=obj_name, opacity=0.3,
            line=dict(color='#1a237e', width=3)
        ))
        # Top 3 muestras
        colors_radar = ['#ef4444', '#f59e0b', '#10b981']
        for i, (_, r) in enumerate(df_rank.head(3).iterrows()):
            row_orig = df[df['nombre'] == r['Muestra']].iloc[0] if not df[df['nombre'] == r['Muestra']].empty else None
            if row_orig is not None:
                m_vals = [float(row_orig.get(p, 0) or 0) for p in radar_params]
                fig_radar.add_trace(go.Scatterpolar(
                    r=m_vals + [m_vals[0]], theta=radar_labels + [radar_labels[0]],
                    fill='toself', name=r['Muestra'], opacity=0.2,
                    line=dict(color=colors_radar[i], width=2)
                ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True)),
            showlegend=True, height=500,
            title=f"Perfil Comparativo vs {obj_name}"
        )
        st.plotly_chart(fig_radar, use_container_width=True)

        # Export Excel
        st.subheader("Exportar Ranking")
        df_export = df_rank[['Muestra', 'Yacimiento', 'Fe2O3', 'Al2O3', 'SiO2',
                             'Absorcion', 'L*', 'Score']].copy()
        df_export.index.name = 'Posicion'
        buf = BytesIO()
        df_export.to_excel(buf, index=True, sheet_name=f'Ranking_{key_suffix}')
        buf.seek(0)
        st.download_button(
            label=f"📥 Descargar Ranking vs {obj_name} (.xlsx)",
            data=buf,
            file_name=f"ranking_vs_{key_suffix}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_rank_{key_suffix}"
        )

    # ══════════ TAB 1: COMPARACION CUADRUPLE ══════════
    with tab_todas:
        st.subheader("Comparacion Simultanea vs 4 Arcillas Objetivo")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            _mostrar_ficha_objetivo(ARCILLA_OBJ_UCRANIA_NUEVA)
        with c2:
            _mostrar_ficha_objetivo(ARCILLA_OBJ_UCRANIA_ANT)
        with c3:
            _mostrar_ficha_objetivo(ARCILLA_OBJ_INDIA)
        with c4:
            _mostrar_ficha_objetivo(ARCILLA_OBJ_UCRA_TECHNIC)

        df_rank_ukr1 = _calcular_rankings(df, ARCILLA_OBJ_UCRANIA_NUEVA)
        df_rank_ukr2 = _calcular_rankings(df, ARCILLA_OBJ_UCRANIA_ANT)
        df_rank_ind = _calcular_rankings(df, ARCILLA_OBJ_INDIA)
        df_rank_tech = _calcular_rankings(df, ARCILLA_OBJ_UCRA_TECHNIC)

        if df_rank_ukr1.empty and df_rank_ukr2.empty and df_rank_ind.empty and df_rank_tech.empty:
            st.warning("No hay datos suficientes para ranking.")
            return

        st.subheader("Ranking Cuadruple de Similitud")

        dual_data = []
        for _, row in df.iterrows():
            sc_1, _ = calcular_scoring_vs_objetivo(row, ARCILLA_OBJ_UCRANIA_NUEVA)
            sc_2, _ = calcular_scoring_vs_objetivo(row, ARCILLA_OBJ_UCRANIA_ANT)
            sc_3, _ = calcular_scoring_vs_objetivo(row, ARCILLA_OBJ_INDIA)
            sc_4, _ = calcular_scoring_vs_objetivo(row, ARCILLA_OBJ_UCRA_TECHNIC)
            scores = {'Ucrania Nueva': sc_1, 'Ucrania Ant.': sc_2, 'India AG-30': sc_3, 'Ucra Technic': sc_4}
            valid_scores = {k: v for k, v in scores.items() if v is not None}
            if valid_scores:
                mejor_ref = max(valid_scores, key=valid_scores.get)
                mejor_score = valid_scores[mejor_ref]
                dual_data.append({
                    'Muestra': row['nombre'],
                    'Yacimiento': row.get('yacimiento', ''),
                    'Score Ukr. Nueva': sc_1,
                    'Score Ukr. Ant.': sc_2,
                    'Score India': sc_3,
                    'Score Technic': sc_4,
                    'Mejor Score': mejor_score,
                    'Mas Similar a': mejor_ref,
                })

        if dual_data:
            df_dual = pd.DataFrame(dual_data).sort_values('Mejor Score', ascending=False).reset_index(drop=True)
            df_dual.index = df_dual.index + 1
            df_dual.index.name = '#'

            # KPIs
            dk1, dk2, dk3, dk4, dk5 = st.columns(5)
            dk1.metric("Ukr. Nueva",
                       df_dual[df_dual['Mas Similar a'] == 'Ucrania Nueva'].shape[0])
            dk2.metric("Ukr. Ant.",
                       df_dual[df_dual['Mas Similar a'] == 'Ucrania Ant.'].shape[0])
            dk3.metric("India",
                       df_dual[df_dual['Mas Similar a'] == 'India AG-30'].shape[0])
            dk4.metric("Technic",
                       df_dual[df_dual['Mas Similar a'] == 'Ucra Technic'].shape[0])
            dk5.metric("Score Promedio",
                       f"{df_dual['Mejor Score'].mean():.1f}")

            html_rows_d = ""
            for pos, (_, r) in enumerate(df_dual.iterrows(), 1):
                s1 = r['Score Ukr. Nueva']
                s2 = r['Score Ukr. Ant.']
                s3 = r['Score India']
                s4 = r['Score Technic']
                barra_1 = _barra_progreso_html(s1)
                barra_2 = _barra_progreso_html(s2)
                barra_3 = _barra_progreso_html(s3)
                barra_4 = _barra_progreso_html(s4)
                ref_map = {'Ucrania Nueva': '🇺🇦1', 'Ucrania Ant.': '🇺🇦2', 'India AG-30': '🇮🇳', 'Ucra Technic': '🇺🇦T'}
                ref_badge = ref_map.get(r['Mas Similar a'], '?')
                html_rows_d += f"""<tr>
                    <td style="text-align:center;font-weight:700">{pos}</td>
                    <td>{r['Muestra']}</td>
                    <td>{r['Yacimiento'] or '\u2014'}</td>
                    <td style="min-width:80px">{barra_1}</td>
                    <td style="min-width:80px">{barra_2}</td>
                    <td style="min-width:80px">{barra_3}</td>
                    <td style="min-width:80px">{barra_4}</td>
                    <td style="text-align:center;font-size:16px">{ref_badge}</td>
                </tr>"""

            st.markdown(f"""
            <div style="overflow-x:auto">
            <table style="width:100%;border-collapse:collapse;font-size:12px">
            <thead><tr style="background:#1b3a4b;color:white">
                <th style="padding:8px;border-radius:6px 0 0 0">#</th>
                <th style="padding:8px">Muestra</th>
                <th style="padding:8px">Yacimiento</th>
                <th style="padding:8px">Ukr. Nueva</th>
                <th style="padding:8px">Ukr. Ant.</th>
                <th style="padding:8px">India</th>
                <th style="padding:8px">Technic</th>
                <th style="padding:8px;border-radius:0 6px 0 0">Mas Similar</th>
            </tr></thead>
            <tbody>{html_rows_d}</tbody>
            </table></div>
            """, unsafe_allow_html=True)

            buf_d = BytesIO()
            df_dual.to_excel(buf_d, index=True, sheet_name='Ranking_Cuadruple')
            buf_d.seek(0)
            st.download_button(
                "📥 Descargar Ranking Cuadruple (.xlsx)", data=buf_d,
                file_name="ranking_cuadruple_objetivos.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_rank_cuadruple"
            )

    # ══════════ TAB 2: VS OBJ 1 UCRANIA NUEVA ══════════
    with tab_ukr_nueva:
        _mostrar_ficha_objetivo(ARCILLA_OBJ_UCRANIA_NUEVA)
        df_rank_ukr = _calcular_rankings(df, ARCILLA_OBJ_UCRANIA_NUEVA)
        _render_ranking_completo(df_rank_ukr, ARCILLA_OBJ_UCRANIA_NUEVA, 'OBJ 1 Ucrania Nueva', 'ukr_nueva')

    # ══════════ TAB 3: VS OBJ 2 UCRANIA ANT. ══════════
    with tab_ukr_ant:
        _mostrar_ficha_objetivo(ARCILLA_OBJ_UCRANIA_ANT)
        df_rank_ukr2 = _calcular_rankings(df, ARCILLA_OBJ_UCRANIA_ANT)
        _render_ranking_completo(df_rank_ukr2, ARCILLA_OBJ_UCRANIA_ANT, 'OBJ 2 Ucrania Ant.', 'ukr_ant')

    # ══════════ TAB 4: VS OBJ 3 INDIA ══════════
    with tab_india:
        _mostrar_ficha_objetivo(ARCILLA_OBJ_INDIA)
        df_rank_ind = _calcular_rankings(df, ARCILLA_OBJ_INDIA)
        _render_ranking_completo(df_rank_ind, ARCILLA_OBJ_INDIA, 'OBJ 3 India AG-30', 'india')

    # ══════════ TAB 5: VS OBJ 4 UCRA TECHNIC ══════════
    with tab_technic:
        _mostrar_ficha_objetivo(ARCILLA_OBJ_UCRA_TECHNIC)
        df_rank_tech = _calcular_rankings(df, ARCILLA_OBJ_UCRA_TECHNIC)
        _render_ranking_completo(df_rank_tech, ARCILLA_OBJ_UCRA_TECHNIC, 'OBJ 4 Ucra Technic', 'technic')


def _widget_ranking_dashboard(df, productos=None, specs_all=None):
    """Widget resumen de ranking para integrar en Dashboard General."""
    st.markdown("---")
    st.subheader("Ranking Rapido vs Arcillas Objetivo")

    scores_triple = []
    for _, row in df.iterrows():
        sc_1, _ = calcular_scoring_vs_objetivo(row, ARCILLA_OBJ_UCRANIA_NUEVA)
        sc_2, _ = calcular_scoring_vs_objetivo(row, ARCILLA_OBJ_UCRANIA_ANT)
        sc_3, _ = calcular_scoring_vs_objetivo(row, ARCILLA_OBJ_INDIA)
        sc_4, _ = calcular_scoring_vs_objetivo(row, ARCILLA_OBJ_UCRA_TECHNIC)
        scores = {'🇺🇦1': sc_1, '🇺🇦2': sc_2, '🇮🇳': sc_3, '🇺🇦T': sc_4}
        valid = {k: v for k, v in scores.items() if v is not None}
        if valid:
            best_ref = max(valid, key=valid.get)
            best_score = valid[best_ref]
            scores_triple.append({'Muestra': row['nombre'], 'Score': best_score, 'Ref': best_ref,
                                  'Yacimiento': row.get('yacimiento', '')})

    if not scores_triple:
        st.info("Sin datos suficientes para ranking.")
        return

    df_sc = pd.DataFrame(scores_triple).sort_values('Score', ascending=False)
    top3 = df_sc.head(3)

    cols = st.columns(3)
    medals = ['🥇', '🥈', '🥉']
    for i, (_, r) in enumerate(top3.iterrows()):
        with cols[i]:
            color = '#4CAF50' if r['Score'] >= 80 else ('#FFC107' if r['Score'] >= 60 else '#F44336')
            st.markdown(f"""
            <div style="text-align:center;padding:8px;background:rgba(0,0,0,0.03);border-radius:10px;
                        border-left:4px solid {color}">
                <span style="font-size:20px">{medals[i]}</span>
                <div style="font-weight:700;font-size:13px">{r['Muestra']}</div>
                <div style="font-size:22px;font-weight:800;color:{color}">{r['Score']:.0f} {r['Ref']}</div>
            </div>""", unsafe_allow_html=True)

    avg_score = df_sc['Score'].mean()
    n_sim = len(df_sc[df_sc['Score'] >= 70])
    st.caption(f"Score promedio: **{avg_score:.1f}** | Similares (>=70): **{n_sim}/{len(df_sc)}**")


def page_control_calidad():
    st.title("🔍 Control de Calidad")
    st.markdown('<div class="section-badge">🏭 ISO 13006 / EN 14411</div>', unsafe_allow_html=True)
    st.caption("Evaluacion de muestras contra especificaciones de productos ceramicos "
               "segun ISO 13006 / EN 14411.")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    productos = obtener_productos()
    if not productos:
        st.error("No hay especificaciones cargadas. Reinicie la app para crear las tablas.")
        return

    specs_df = obtener_especificaciones()

    # ── Selección de producto ──
    col_prod, col_info = st.columns([2, 1])
    with col_prod:
        producto_sel = st.selectbox("Producto target:", productos)
    with col_info:
        specs_prod = specs_df[specs_df['producto'] == producto_sel]
        st.metric("Parametros a evaluar", len(specs_prod))

    # Mostrar specs del producto seleccionado
    with st.expander("Ver especificaciones del producto", expanded=False):
        spec_display = []
        for _, s in specs_prod.iterrows():
            rango = ""
            if s['min_valor'] is not None and s['max_valor'] is not None:
                rango = f"{s['min_valor']:.1f} – {s['max_valor']:.1f}"
            elif s['min_valor'] is not None:
                rango = f"≥ {s['min_valor']:.1f}"
            elif s['max_valor'] is not None:
                rango = f"≤ {s['max_valor']:.1f}"
            spec_display.append({
                'Parametro': PARAM_LABELS_SPEC.get(s['parametro'], s['parametro']),
                'Rango': rango,
                'Unidad': s['unidad'] or '',
                'Norma': s['norma'] or '',
            })
        st.dataframe(pd.DataFrame(spec_display), use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Evaluar todas las muestras ──
    resultados = []
    for _, row in df.iterrows():
        semaforo, n_ok, n_fail, detalles = evaluar_muestra_vs_specs(row, specs_prod)
        entry = {
            'Muestra': row['nombre'],
            'Yacimiento': row.get('yacimiento', '—') or '—',
            'Aptitud': f"{SEMAFORO_ICONS[semaforo]} {semaforo.capitalize()}",
            '_semaforo': semaforo,
            'Cumple': n_ok,
            'Falla': n_fail,
        }
        # Agregar valor de cada parámetro con indicador
        for d in detalles:
            p_label = PARAM_LABELS_SPEC.get(d['parametro'], d['parametro'])
            if d['valor'] is not None:
                icon = '✅' if d['cumple'] else '❌'
                entry[p_label] = f"{icon} {d['valor']:.2f}"
            else:
                entry[p_label] = '—'
        resultados.append(entry)

    df_res = pd.DataFrame(resultados)

    # ── KPIs ──
    n_verde = len(df_res[df_res['_semaforo'] == 'verde'])
    n_amarillo = len(df_res[df_res['_semaforo'] == 'amarillo'])
    n_rojo = len(df_res[df_res['_semaforo'] == 'rojo'])
    total = len(df_res)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Muestras", total)
    k2.metric("Aptas", f"{n_verde} ({n_verde/total*100:.0f}%)" if total > 0 else "0")
    k3.metric("Parciales", f"{n_amarillo}")
    k4.metric("No Aptas", f"{n_rojo}")

    # Barra de progreso visual
    if total > 0:
        pct_verde = n_verde / total
        pct_amarillo = n_amarillo / total
        pct_rojo = n_rojo / total
        st.markdown(f"""
        <div style="display:flex;height:28px;border-radius:8px;overflow:hidden;margin:8px 0 16px 0;
                    box-shadow:0 1px 3px rgba(0,0,0,0.1)">
            <div style="width:{pct_verde*100}%;background:#4CAF50;display:flex;align-items:center;
                        justify-content:center;color:#fff;font-size:11px;font-weight:600">
                {f'{n_verde} aptas' if pct_verde > 0.08 else ''}</div>
            <div style="width:{pct_amarillo*100}%;background:#FFC107;display:flex;align-items:center;
                        justify-content:center;color:#333;font-size:11px;font-weight:600">
                {f'{n_amarillo}' if pct_amarillo > 0.05 else ''}</div>
            <div style="width:{pct_rojo*100}%;background:#F44336;display:flex;align-items:center;
                        justify-content:center;color:#fff;font-size:11px;font-weight:600">
                {f'{n_rojo}' if pct_rojo > 0.05 else ''}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown(f"**{n_verde} de {total} muestras aptas para {producto_sel}**")

    # ── Filtro por semáforo ──
    filtro = st.radio("Filtrar:", ["Todas", "🟢 Aptas", "🟡 Parciales", "🔴 No aptas"],
                      horizontal=True)
    if filtro == "🟢 Aptas":
        df_res = df_res[df_res['_semaforo'] == 'verde']
    elif filtro == "🟡 Parciales":
        df_res = df_res[df_res['_semaforo'] == 'amarillo']
    elif filtro == "🔴 No aptas":
        df_res = df_res[df_res['_semaforo'] == 'rojo']

    # ── Tabla de resultados ──
    display_cols = [c for c in df_res.columns if c != '_semaforo']
    st.dataframe(df_res[display_cols], use_container_width=True, hide_index=True)

    # ── Gráfico de distribución por yacimiento ──
    if total > 0:
        st.markdown("---")
        st.subheader("Distribucion por Yacimiento")

        df_full = pd.DataFrame(resultados)
        df_yac = df_full.groupby(['Yacimiento', '_semaforo']).size().reset_index(name='count')

        fig_yac = go.Figure()
        for sem, color in [('verde', '#4CAF50'), ('amarillo', '#FFC107'), ('rojo', '#F44336')]:
            d = df_yac[df_yac['_semaforo'] == sem]
            if not d.empty:
                fig_yac.add_trace(go.Bar(
                    name=sem.capitalize(), x=d['Yacimiento'], y=d['count'],
                    marker_color=color,
                ))
        fig_yac.update_layout(
            barmode='stack', height=400,
            yaxis_title='Muestras',
            plot_bgcolor='#ffffff',
            xaxis=dict(gridcolor='#f0f0f0'),
            yaxis=dict(gridcolor='#f0f0f0'),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
        )
        st.plotly_chart(fig_yac, use_container_width=True)


# =====================================================
# GENERAR CERTIFICADO DE ANÁLISIS (PDF)
# =====================================================
def _generar_numero_certificado(muestra_id):
    """Genera número secuencial: CERT-YYYY-NNNNN"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM certificados_emitidos")
    seq = c.fetchone()[0] + 1
    conn.close()
    return f"CERT-{date.today().year}-{seq:05d}"


def _generar_certificado_pdf(row, producto, specs_prod, user_info):
    """Genera un PDF de Certificado de Análisis profesional."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm, cm
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                     Paragraph, Spacer, Image, KeepTogether)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.graphics.shapes import Drawing, Rect

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.2*cm, bottomMargin=2*cm,
                            leftMargin=1.8*cm, rightMargin=1.8*cm)
    styles = getSampleStyleSheet()
    W = doc.width

    # Estilos
    s_title = ParagraphStyle('CertTitle', parent=styles['Title'], fontSize=15,
                              textColor=colors.HexColor('#0d1b2a'), spaceAfter=2,
                              fontName='Helvetica-Bold')
    s_subtitle = ParagraphStyle('CertSub', parent=styles['Heading2'], fontSize=11,
                                 textColor=colors.HexColor('#1a237e'), spaceBefore=10, spaceAfter=4,
                                 fontName='Helvetica-Bold')
    s_normal = ParagraphStyle('CertNorm', parent=styles['Normal'], fontSize=9, leading=12)
    s_small = ParagraphStyle('CertSmall', parent=styles['Normal'], fontSize=7, leading=9,
                              textColor=colors.HexColor('#666666'))
    s_center = ParagraphStyle('CertCenter', parent=styles['Normal'], fontSize=9,
                               alignment=TA_CENTER)
    s_right = ParagraphStyle('CertRight', parent=styles['Normal'], fontSize=8,
                              alignment=TA_RIGHT, textColor=colors.HexColor('#555'))

    elements = []

    # Número de certificado
    num_cert = _generar_numero_certificado(row.get('id', 0))

    # ── HEADER ──
    logo_path = os.path.join(BASE_DIR, 'logo_geocivmet.png')
    header_data = []
    if os.path.exists(logo_path):
        try:
            logo = Image(logo_path, width=3*cm, height=3*cm)
        except Exception:
            logo = Paragraph("GEOCIVMET", s_title)
    else:
        logo = Paragraph("GEOCIVMET", s_title)

    header_data = [[
        logo,
        Paragraph("<b>GEOCIVMET CONSULTORES TECNICOS</b><br/>"
                   "<font size='8'>Geologia - Ingenieria Civil - Mineria - Metalurgia</font><br/>"
                   "<font size='7' color='#777'>Laboratorio de Caracterizacion de Materias Primas</font>",
                   s_normal),
        Paragraph(f"<b>N°:</b> {num_cert}<br/>"
                   f"<b>Fecha:</b> {date.today().strftime('%d/%m/%Y')}<br/>"
                   f"<b>Pagina:</b> 1 de 1", s_right),
    ]]
    ht = Table(header_data, colWidths=[3.5*cm, W - 3.5*cm - 4.5*cm, 4.5*cm])
    ht.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LINEBELOW', (0, 0), (-1, -1), 2, colors.HexColor('#1a237e')),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    elements.append(ht)
    elements.append(Spacer(1, 6*mm))

    # ── TÍTULO ──
    elements.append(Paragraph("CERTIFICADO DE ANALISIS DE MATERIA PRIMA", s_title))
    elements.append(Spacer(1, 3*mm))

    # ── DATOS DE LA MUESTRA ──
    elements.append(Paragraph("1. Identificacion de la Muestra", s_subtitle))
    id_data = [
        ['Nombre:', str(row.get('nombre', '—')), 'Codigo Lab:', str(row.get('codigo_lab', '—') or '—')],
        ['Yacimiento:', str(row.get('yacimiento', '—') or '—'), 'Fecha Muestra:', str(row.get('fecha', '—') or '—')],
        ['Estado:', str(row.get('estado', '—') or '—'), 'Municipio:', str(row.get('municipio', '—') or '—')],
        ['Producto Evaluado:', producto, 'Temperatura:', f"{row.get('temperatura_coccion', '—') or '—'}°C"],
    ]
    id_table = Table(id_data, colWidths=[3*cm, 5.5*cm, 3*cm, 5*cm])
    id_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8f9ff')),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#ddd')),
    ]))
    elements.append(id_table)
    elements.append(Spacer(1, 5*mm))

    # ── EVALUAR CONTRA SPECS ──
    semaforo, n_ok, n_fail, detalles = evaluar_muestra_vs_specs(row, specs_prod)

    # Separar química y física
    QUIM_PARAMS = {'sio2', 'al2o3', 'fe2o3', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc'}
    det_quim = [d for d in detalles if d['parametro'] in QUIM_PARAMS]
    det_fis = [d for d in detalles if d['parametro'] not in QUIM_PARAMS]

    # Agregar parámetros químicos no especificados pero con datos
    ALL_QUIM = ['sio2', 'al2o3', 'fe2o3', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc']
    spec_params = {d['parametro'] for d in detalles}
    for qp in ALL_QUIM:
        if qp not in spec_params:
            val = row.get(qp)
            if pd.notna(val):
                det_quim.append({
                    'parametro': qp, 'valor': float(val),
                    'min': None, 'max': None, 'cumple': None, 'estado': 'info',
                    'unidad': '%',
                })

    # Agregar propiedades físicas no especificadas
    ALL_FIS = ['absorcion', 'contraccion', 'mor_cocido_mpa', 'mor_verde', 'mor_seco',
               'densidad', 'superficie_especifica', 'pfefferkorn', 'indice_plasticidad',
               'residuo_45um', 'porosidad_abierta']
    FIS_UNITS = {'absorcion': '%', 'contraccion': '%', 'mor_cocido_mpa': 'MPa',
                 'mor_verde': 'kgf/cm²', 'mor_seco': 'kgf/cm²', 'densidad': 'g/cm³',
                 'superficie_especifica': 'm²/g', 'pfefferkorn': '%',
                 'indice_plasticidad': '%', 'residuo_45um': '%', 'porosidad_abierta': '%'}
    for fp in ALL_FIS:
        if fp not in spec_params:
            val = row.get(fp)
            if pd.notna(val):
                det_fis.append({
                    'parametro': fp, 'valor': float(val),
                    'min': None, 'max': None, 'cumple': None, 'estado': 'info',
                    'unidad': FIS_UNITS.get(fp, ''),
                })

    def _make_result_table(titulo, numero, det_list):
        """Crea tabla de resultados con semáforo."""
        elements.append(Paragraph(f"{numero}. {titulo}", s_subtitle))

        header = ['Parametro', 'Resultado', 'Especificacion', 'Unidad', 'Cumple']
        rows = [header]

        for d in det_list:
            p_label = PARAM_LABELS_SPEC.get(d['parametro'], d['parametro'])
            val_str = f"{d['valor']:.3f}" if d['valor'] is not None else '—'

            spec_str = ''
            if d['min'] is not None and d['max'] is not None:
                spec_str = f"{d['min']:.1f} – {d['max']:.1f}"
            elif d['min'] is not None:
                spec_str = f"≥ {d['min']:.1f}"
            elif d['max'] is not None:
                spec_str = f"≤ {d['max']:.1f}"
            else:
                spec_str = '—'

            if d['estado'] == 'info':
                cumple_str = '—'
            elif d['cumple'] is True:
                cumple_str = 'CUMPLE'
            elif d['cumple'] is False:
                cumple_str = 'NO CUMPLE'
            else:
                cumple_str = 'S/D'

            rows.append([p_label, val_str, spec_str, d.get('unidad', ''), cumple_str])

        t = Table(rows, colWidths=[4.5*cm, 2.5*cm, 3*cm, 2*cm, 4.5*cm])

        # Estilo base
        style_cmds = [
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a237e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#ccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f7ff')]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
        ]

        # Colorear celdas de cumplimiento
        for i, d in enumerate(det_list, start=1):
            if d['cumple'] is True:
                style_cmds.append(('TEXTCOLOR', (4, i), (4, i), colors.HexColor('#2E7D32')))
                style_cmds.append(('FONTNAME', (4, i), (4, i), 'Helvetica-Bold'))
            elif d['cumple'] is False:
                style_cmds.append(('TEXTCOLOR', (4, i), (4, i), colors.HexColor('#C62828')))
                style_cmds.append(('FONTNAME', (4, i), (4, i), 'Helvetica-Bold'))
                style_cmds.append(('BACKGROUND', (4, i), (4, i), colors.HexColor('#ffebee')))

        t.setStyle(TableStyle(style_cmds))
        elements.append(t)
        elements.append(Spacer(1, 4*mm))

    # ── TABLA QUÍMICA ──
    if det_quim:
        _make_result_table("Analisis Quimico (FRX)", "2", det_quim)

    # ── TABLA FÍSICA ──
    if det_fis:
        _make_result_table("Propiedades Fisicas y Mecanicas", "3", det_fis)

    # ── COLORIMETRÍA ──
    _L = row.get('l_color')
    _a = row.get('a_color')
    _b = row.get('b_color')
    L_val = float(_L) if pd.notna(_L) else None
    a_val = float(_a) if pd.notna(_a) else None
    b_val = float(_b) if pd.notna(_b) else None

    if L_val is not None:
        elements.append(Paragraph("4. Colorimetria (CIE L*a*b*)", s_subtitle))
        r_c = max(0, min(255, int((L_val / 100 + a_val / 200) * 255))) if a_val else 200
        g_c = max(0, min(255, int((L_val / 100 - (a_val or 0) / 400 - (b_val or 0) / 400) * 255)))
        b_c = max(0, min(255, int((L_val / 100 - (b_val or 0) / 200) * 255)))

        color_data = [
            ['L* (Luminosidad)', f'{L_val:.2f}', ''],
            ['a* (Rojo-Verde)', f'{a_val:.2f}' if a_val is not None else '—', ''],
            ['b* (Amarillo-Azul)', f'{b_val:.2f}' if b_val is not None else '—', ''],
        ]
        ct = Table(color_data, colWidths=[5*cm, 3*cm, 8.5*cm])
        ct.setStyle(TableStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('GRID', (0, 0), (1, -1), 0.3, colors.HexColor('#ddd')),
            ('BACKGROUND', (2, 0), (2, -1), colors.Color(r_c/255, g_c/255, b_c/255)),
            ('SPAN', (2, 0), (2, -1)),
        ]))
        elements.append(ct)
        elements.append(Spacer(1, 4*mm))
        next_sec = "5"
    else:
        next_sec = "4"

    # ── CLASIFICACIÓN ──
    fe = float(row.get('fe2o3', 0) or 0)
    al = float(row.get('al2o3', 0) or 0)
    ab = float(row.get('absorcion', 0) or 0)
    calidad, uso = clasificar_arcilla(fe, al, ab)

    elements.append(Paragraph(f"{next_sec}. Clasificacion y Uso Recomendado", s_subtitle))
    clas_data = [
        ['Clasificacion por Calidad:', calidad],
        ['Uso Recomendado:', uso],
        ['Relacion SiO2/Al2O3:', f"{float(row.get('sio2', 0) or 0) / max(float(row.get('al2o3', 1) or 1), 0.01):.2f}"],
    ]
    clas_t = Table(clas_data, colWidths=[5.5*cm, 11*cm])
    clas_t.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f0f4ff')),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#ccc')),
    ]))
    elements.append(clas_t)
    elements.append(Spacer(1, 6*mm))

    # ── DICTAMEN ──
    next_sec2 = str(int(next_sec) + 1)
    elements.append(Paragraph(f"{next_sec2}. Dictamen", s_subtitle))

    if semaforo == 'verde':
        dictamen = "APROBADO"
        dict_color = colors.HexColor('#2E7D32')
        dict_bg = colors.HexColor('#e8f5e9')
        dict_detail = f"La muestra cumple con TODAS las especificaciones del producto {producto}."
    elif semaforo == 'amarillo':
        dictamen = "APROBADO CON OBSERVACIONES"
        dict_color = colors.HexColor('#F57F17')
        dict_bg = colors.HexColor('#fff8e1')
        dict_detail = (f"La muestra cumple parcialmente. {n_fail} parametro(s) fuera de especificacion. "
                       f"Se recomienda revision antes de uso.")
    else:
        dictamen = "RECHAZADO"
        dict_color = colors.HexColor('#C62828')
        dict_bg = colors.HexColor('#ffebee')
        dict_detail = (f"La muestra NO cumple con las especificaciones del producto {producto}. "
                       f"{n_fail} parametro(s) fuera de rango.")

    # Parámetros fuera de spec
    fuera = [d for d in detalles if d['cumple'] is False]
    if fuera:
        dict_detail += " Parametros fuera: " + ", ".join(
            PARAM_LABELS_SPEC.get(d['parametro'], d['parametro']) for d in fuera
        ) + "."

    dict_data = [
        [Paragraph(f"<font size='14'><b>{dictamen}</b></font>",
                    ParagraphStyle('dict', alignment=TA_CENTER, textColor=dict_color))],
        [Paragraph(dict_detail, ParagraphStyle('dictd', fontSize=8, alignment=TA_CENTER,
                                                textColor=colors.HexColor('#333')))],
    ]
    dict_t = Table(dict_data, colWidths=[W])
    dict_t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), dict_bg),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (0, 0), 12),
        ('BOTTOMPADDING', (0, -1), (0, -1), 10),
        ('BOX', (0, 0), (-1, -1), 1.5, dict_color),
        ('ROUNDEDCORNERS', [6, 6, 6, 6]),
    ]))
    elements.append(dict_t)
    elements.append(Spacer(1, 8*mm))

    # ── FIRMAS ──
    firma_data = [[
        Paragraph("_________________________<br/><font size='7'>Analista de Laboratorio</font>", s_center),
        Paragraph("_________________________<br/><font size='7'>Jefe de Laboratorio</font>", s_center),
        Paragraph("_________________________<br/><font size='7'>Control de Calidad</font>", s_center),
    ]]
    firma_t = Table(firma_data, colWidths=[W/3]*3)
    firma_t.setStyle(TableStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                                  ('TOPPADDING', (0, 0), (-1, -1), 20)]))
    elements.append(firma_t)
    elements.append(Spacer(1, 6*mm))

    # ── FOOTER ──
    elements.append(Paragraph(
        f"Este certificado fue generado automaticamente por GEOCIVMET Lab System v3.0 — "
        f"{date.today().strftime('%d/%m/%Y')} — {num_cert}<br/>"
        f"Emitido por: {user_info.get('nombre_completo', 'Sistema')}<br/>"
        f"Los resultados aplican unicamente a la muestra analizada.",
        ParagraphStyle('footer', fontSize=7, alignment=TA_CENTER,
                       textColor=colors.HexColor('#999'), spaceBefore=6)
    ))

    doc.build(elements)
    buf.seek(0)
    return buf, num_cert, dictamen


def page_certificado_analisis(user_info):
    st.title("📜 Generar Certificado de Analisis")
    st.caption("Genera certificados PDF profesionales evaluando muestras contra "
               "especificaciones de producto ISO 13006 / EN 14411.")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras.")
        return

    productos = obtener_productos()
    if not productos:
        st.error("No hay especificaciones cargadas.")
        return

    specs_df = obtener_especificaciones()

    # ── Selección ──
    col_m, col_p = st.columns(2)
    with col_m:
        nombres = sorted(df['nombre'].dropna().unique())
        muestra_sel = st.selectbox("Muestra:", nombres)
    with col_p:
        producto_sel = st.selectbox("Producto / Especificacion:", productos)

    row = df[df['nombre'] == muestra_sel].iloc[0]
    specs_prod = specs_df[specs_df['producto'] == producto_sel]

    # ── Vista previa ──
    st.markdown("---")
    semaforo, n_ok, n_fail, detalles = evaluar_muestra_vs_specs(row, specs_prod)

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Muestra", muestra_sel)
    k2.metric("Producto", producto_sel)
    k3.metric("Parametros OK", f"{n_ok} / {n_ok + n_fail}")
    icon = SEMAFORO_ICONS[semaforo]
    k4.metric("Dictamen", f"{icon} {'Aprobado' if semaforo in ('verde', 'amarillo') else 'Rechazado'}")

    # Tabla preview
    preview_rows = []
    for d in detalles:
        p_label = PARAM_LABELS_SPEC.get(d['parametro'], d['parametro'])
        val_str = f"{d['valor']:.3f}" if d['valor'] is not None else '—'
        spec_str = ''
        if d['min'] is not None and d['max'] is not None:
            spec_str = f"{d['min']:.1f} – {d['max']:.1f}"
        elif d['min'] is not None:
            spec_str = f"≥ {d['min']:.1f}"
        elif d['max'] is not None:
            spec_str = f"≤ {d['max']:.1f}"
        cumple_icon = '✅' if d['cumple'] else ('❌' if d['cumple'] is False else '—')
        preview_rows.append({
            'Parametro': p_label, 'Valor': val_str,
            'Especificacion': spec_str, 'Cumple': cumple_icon,
        })
    st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)

    # ── Generar PDF ──
    st.markdown("---")
    col_gen, col_hist = st.columns([1, 1])

    with col_gen:
        if st.button("Generar Certificado PDF", type="primary", use_container_width=True):
            try:
                pdf_buf, num_cert, dictamen = _generar_certificado_pdf(
                    row, producto_sel, specs_prod, user_info
                )
                st.session_state['cert_pdf'] = pdf_buf.getvalue()
                st.session_state['cert_num'] = num_cert
                st.session_state['cert_dict'] = dictamen
                st.session_state['cert_muestra'] = muestra_sel
                st.session_state['cert_producto'] = producto_sel

                # Guardar en BD
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute(
                    "INSERT INTO certificados_emitidos "
                    "(muestra_id, producto, resultado, emitido_por, numero_certificado) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (int(row['id']), producto_sel, dictamen,
                     user_info.get('nombre_completo', 'admin'), num_cert)
                )
                conn.commit()
                conn.close()
                st.success(f"Certificado {num_cert} generado — **{dictamen}**")
            except Exception as e:
                st.error(f"Error generando certificado: {e}")

    with col_hist:
        if 'cert_pdf' in st.session_state:
            st.download_button(
                "Descargar PDF",
                data=st.session_state['cert_pdf'],
                file_name=f"{st.session_state.get('cert_num', 'CERT')}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )

    # ── Historial de certificados ──
    st.markdown("---")
    st.subheader("Historial de Certificados Emitidos")
    conn = sqlite3.connect(DB_PATH)
    try:
        df_hist = pd.read_sql(
            "SELECT c.numero_certificado, m.nombre AS muestra, c.producto, "
            "c.resultado, c.fecha, c.emitido_por "
            "FROM certificados_emitidos c "
            "LEFT JOIN muestras m ON c.muestra_id = m.id "
            "ORDER BY c.id DESC LIMIT 20", conn
        )
        conn.close()
        if not df_hist.empty:
            st.dataframe(df_hist, use_container_width=True, hide_index=True)
        else:
            st.info("No se han emitido certificados aun.")
    except Exception:
        conn.close()
        st.info("No se han emitido certificados aun.")


# =====================================================
# CARTAS DE CONTROL SPC (Statistical Process Control)
# =====================================================
def _westgard_rules(values, mean, sigma):
    """
    Aplica reglas Westgard simplificadas.
    Retorna lista de dicts: {indice, regla, severidad}.
    """
    n = len(values)
    violations = []
    if sigma == 0 or n < 4:
        return violations

    for i in range(n):
        z = (values[i] - mean) / sigma

        # 1_3s: 1 punto fuera de ±3s
        if abs(z) > 3:
            violations.append({'idx': i, 'regla': '1-3s', 'sev': 'alarma',
                               'desc': f'Punto fuera de ±3σ (z={z:.2f})'})

        # 2_2s: 2 consecutivos fuera de ±2s en mismo lado
        if i >= 1:
            z_prev = (values[i-1] - mean) / sigma
            if abs(z) > 2 and abs(z_prev) > 2:
                if (z > 0 and z_prev > 0) or (z < 0 and z_prev < 0):
                    violations.append({'idx': i, 'regla': '2-2s', 'sev': 'advertencia',
                                       'desc': '2 consecutivos fuera de ±2σ mismo lado'})

        # R_4s: rango entre 2 consecutivos > 4s
        if i >= 1:
            rango = abs(values[i] - values[i-1])
            if rango > 4 * sigma:
                violations.append({'idx': i, 'regla': 'R-4s', 'sev': 'advertencia',
                                   'desc': f'Rango entre consecutivos > 4σ ({rango:.3f})'})

        # 4_1s: 4 consecutivos del mismo lado de la media
        if i >= 3:
            last4 = [values[i-3] - mean, values[i-2] - mean, values[i-1] - mean, values[i] - mean]
            if all(v > 0 for v in last4) or all(v < 0 for v in last4):
                violations.append({'idx': i, 'regla': '4-1s', 'sev': 'advertencia',
                                   'desc': '4 consecutivos del mismo lado de la media'})

    # Dedup por índice (mantener la más severa)
    seen = {}
    for v in violations:
        idx = v['idx']
        if idx not in seen or v['sev'] == 'alarma':
            seen[idx] = v
    return list(seen.values())


def page_spc():
    st.title("📉 Cartas de Control SPC")
    st.caption("Control Estadistico de Procesos — Graficos Shewhart con reglas Westgard "
               "para monitoreo de calidad de materias primas ceramicas.")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    # Parámetros monitoreables
    PARAMS_SPC = {
        'fe2o3': 'Fe₂O₃ (%)', 'al2o3': 'Al₂O₃ (%)', 'sio2': 'SiO₂ (%)',
        'tio2': 'TiO₂ (%)', 'cao': 'CaO (%)', 'mgo': 'MgO (%)',
        'k2o': 'K₂O (%)', 'na2o': 'Na₂O (%)', 'ppc': 'PPC (%)',
        'absorcion': 'Absorcion (%)', 'contraccion': 'Contraccion (%)',
        'l_color': 'L* (Color)', 'a_color': 'a* (Color)', 'b_color': 'b* (Color)',
        'mor_cocido_mpa': 'MOR Cocido (MPa)', 'mor_verde': 'MOR Verde (kgf/cm²)',
        'superficie_especifica': 'Sup. Especifica (m²/g)',
        'indice_plasticidad': 'Ind. Plasticidad (%)',
        'residuo_45um': 'Residuo 45μm (%)', 'densidad': 'Densidad (g/cm³)',
    }

    # ── Controles ──
    col_param, col_yac, col_prod = st.columns(3)
    with col_param:
        param_key = st.selectbox("Parametro a monitorear:",
                                 list(PARAMS_SPC.keys()),
                                 format_func=lambda k: PARAMS_SPC[k])
    with col_yac:
        yacimientos = ['Todos'] + sorted(df['yacimiento'].dropna().unique().tolist())
        yac_sel = st.selectbox("Yacimiento:", yacimientos)
    with col_prod:
        productos = ['— Sin spec —'] + obtener_productos()
        prod_sel = st.selectbox("Spec producto (Cp/Cpk):", productos)

    # Filtrar datos
    df_work = df.copy()
    if yac_sel != 'Todos':
        df_work = df_work[df_work['yacimiento'] == yac_sel]

    # Extraer valores válidos ordenados por fecha/id
    df_work = df_work.dropna(subset=[param_key]).sort_values('id')
    values = df_work[param_key].astype(float).values
    nombres = df_work['nombre'].values
    ids = df_work['id'].values

    if len(values) < 3:
        st.warning(f"Se necesitan al menos 3 muestras con datos de {PARAMS_SPC[param_key]}. "
                   f"Solo hay {len(values)}.")
        return

    # ── Estadísticas ──
    mean = np.mean(values)
    sigma = np.std(values, ddof=1)
    ucl_3 = mean + 3 * sigma
    lcl_3 = mean - 3 * sigma
    ucl_2 = mean + 2 * sigma
    lcl_2 = mean - 2 * sigma
    n = len(values)
    x_axis = list(range(1, n + 1))

    # Regresión lineal para tendencia
    x_reg = np.arange(n)
    if n >= 3:
        slope, intercept = np.polyfit(x_reg, values, 1)
        trend_line = slope * x_reg + intercept
        if abs(slope) < sigma * 0.01:
            tendencia = 'Estable'
            trend_icon = '➡️'
            trend_color = '#4CAF50'
        elif slope > 0:
            tendencia = 'Creciente'
            trend_icon = '📈'
            trend_color = '#FF9800'
        else:
            tendencia = 'Decreciente'
            trend_icon = '📉'
            trend_color = '#2196F3'
    else:
        slope, intercept = 0, mean
        trend_line = np.full(n, mean)
        tendencia = 'Insuficientes datos'
        trend_icon = '—'
        trend_color = '#999'

    # Westgard
    violations = _westgard_rules(values.tolist(), mean, sigma)
    violation_indices = {v['idx'] for v in violations}
    alarmas = [v for v in violations if v['sev'] == 'alarma']
    advertencias = [v for v in violations if v['sev'] == 'advertencia']

    # Cp / Cpk si hay specs
    cp_val = None
    cpk_val = None
    spec_min = None
    spec_max = None
    if prod_sel != '— Sin spec —':
        specs_df = obtener_especificaciones()
        sp = specs_df[(specs_df['producto'] == prod_sel) & (specs_df['parametro'] == param_key)]
        if not sp.empty:
            spec_min = sp.iloc[0]['min_valor']
            spec_max = sp.iloc[0]['max_valor']
            if spec_min is not None and spec_max is not None and sigma > 0:
                cp_val = (spec_max - spec_min) / (6 * sigma)
                cpk_val = min((spec_max - mean) / (3 * sigma),
                              (mean - spec_min) / (3 * sigma))
            elif spec_max is not None and sigma > 0:
                cpk_val = (spec_max - mean) / (3 * sigma)
            elif spec_min is not None and sigma > 0:
                cpk_val = (mean - spec_min) / (3 * sigma)

    # ── LAYOUT: Gráfico + Panel ──
    col_chart, col_panel = st.columns([3, 1])

    with col_panel:
        st.markdown("#### Estadisticas")
        st.metric("Media (X̄)", f"{mean:.4f}")
        st.metric("Desv. Est. (σ)", f"{sigma:.4f}")
        st.metric("n (muestras)", n)
        st.markdown("---")
        st.metric("UCL (+3σ)", f"{ucl_3:.4f}")
        st.metric("LCL (−3σ)", f"{lcl_3:.4f}")
        st.markdown("---")

        if cp_val is not None:
            cp_color = '#4CAF50' if cp_val >= 1.33 else ('#FFC107' if cp_val >= 1.0 else '#F44336')
            st.markdown(f"**Cp:** <span style='color:{cp_color};font-weight:700'>{cp_val:.2f}</span>",
                        unsafe_allow_html=True)
        if cpk_val is not None:
            cpk_color = '#4CAF50' if cpk_val >= 1.33 else ('#FFC107' if cpk_val >= 1.0 else '#F44336')
            st.markdown(f"**Cpk:** <span style='color:{cpk_color};font-weight:700'>{cpk_val:.2f}</span>",
                        unsafe_allow_html=True)
        if cp_val is not None or cpk_val is not None:
            st.markdown("---")

        st.markdown(f"**Tendencia:** {trend_icon} {tendencia}")
        st.markdown(f"<small>Pendiente: {slope:.6f}/muestra</small>", unsafe_allow_html=True)
        st.markdown("---")

        st.markdown("#### Violaciones Westgard")
        st.markdown(f"🔴 **Alarmas (1-3s):** {len(alarmas)}")
        st.markdown(f"🟡 **Advertencias:** {len(advertencias)}")
        if violations:
            with st.expander("Detalle"):
                for v in violations:
                    icon = '🔴' if v['sev'] == 'alarma' else '🟡'
                    st.markdown(f"{icon} **#{v['idx']+1}** {nombres[v['idx']]}: "
                                f"{v['regla']} — {v['desc']}")

    with col_chart:
        fig = go.Figure()

        # Banda ±3σ (relleno)
        fig.add_trace(go.Scatter(
            x=x_axis + x_axis[::-1],
            y=[ucl_3]*n + [lcl_3]*n,
            fill='toself', fillcolor='rgba(244,67,54,0.05)',
            line=dict(width=0), showlegend=False, hoverinfo='skip',
        ))
        # Banda ±2σ
        fig.add_trace(go.Scatter(
            x=x_axis + x_axis[::-1],
            y=[ucl_2]*n + [lcl_2]*n,
            fill='toself', fillcolor='rgba(255,193,7,0.06)',
            line=dict(width=0), showlegend=False, hoverinfo='skip',
        ))

        # Líneas de control
        for yval, label, color, dash in [
            (ucl_3, 'UCL +3σ', '#E53935', 'dash'),
            (lcl_3, 'LCL −3σ', '#E53935', 'dash'),
            (ucl_2, '+2σ', '#FFA726', 'dot'),
            (lcl_2, '−2σ', '#FFA726', 'dot'),
            (mean,  'X̄', '#1565C0', 'solid'),
        ]:
            fig.add_hline(y=yval, line_dash=dash, line_color=color, line_width=1.5,
                          annotation_text=f"{label} ({yval:.3f})",
                          annotation_position='right',
                          annotation_font_size=9,
                          annotation_font_color=color)

        # Líneas de spec si existen
        if spec_min is not None:
            fig.add_hline(y=spec_min, line_dash='longdash', line_color='#7B1FA2', line_width=2,
                          annotation_text=f'LSL ({spec_min:.2f})',
                          annotation_position='left',
                          annotation_font_color='#7B1FA2')
        if spec_max is not None:
            fig.add_hline(y=spec_max, line_dash='longdash', line_color='#7B1FA2', line_width=2,
                          annotation_text=f'USL ({spec_max:.2f})',
                          annotation_position='left',
                          annotation_font_color='#7B1FA2')

        # Puntos normales
        normal_idx = [i for i in range(n) if i not in violation_indices]
        fig.add_trace(go.Scatter(
            x=[x_axis[i] for i in normal_idx],
            y=[values[i] for i in normal_idx],
            mode='markers+lines',
            name='En control',
            line=dict(color='#1565C0', width=1.5),
            marker=dict(size=8, color='#1565C0', line=dict(width=1, color='#fff')),
            hovertemplate=['<b>%s</b><br>#%d<br>Valor: %.4f<extra></extra>' %
                           (nombres[i], i+1, values[i]) for i in normal_idx],
        ))

        # Puntos con violación
        if violation_indices:
            v_list = sorted(violation_indices)
            v_desc = {}
            for v in violations:
                v_desc[v['idx']] = v
            fig.add_trace(go.Scatter(
                x=[x_axis[i] for i in v_list],
                y=[values[i] for i in v_list],
                mode='markers',
                name='Fuera de control',
                marker=dict(size=13, color='#F44336', symbol='x',
                            line=dict(width=2, color='#B71C1C')),
                hovertemplate=['<b>%s</b><br>#%d — %s<br>Valor: %.4f<br>%s<extra></extra>' %
                               (nombres[i], i+1, v_desc[i]['regla'], values[i], v_desc[i]['desc'])
                               for i in v_list],
            ))

        # Línea de tendencia
        fig.add_trace(go.Scatter(
            x=x_axis, y=trend_line,
            mode='lines', name=f'Tendencia ({tendencia})',
            line=dict(color=trend_color, width=1.5, dash='dashdot'),
            hoverinfo='skip',
        ))

        # Conectar todos los puntos con línea gris suave
        fig.add_trace(go.Scatter(
            x=x_axis, y=values,
            mode='lines', showlegend=False,
            line=dict(color='rgba(21,101,192,0.3)', width=1),
            hoverinfo='skip',
        ))

        fig.update_layout(
            title=dict(text=f'Carta de Control — {PARAMS_SPC[param_key]}',
                       font=dict(size=16)),
            xaxis_title='Muestra (orden)',
            yaxis_title=PARAMS_SPC[param_key],
            height=520,
            plot_bgcolor='#ffffff',
            xaxis=dict(gridcolor='#f0f0f0', dtick=max(1, n//20)),
            yaxis=dict(gridcolor='#f0f0f0'),
            legend=dict(orientation='h', yanchor='bottom', y=1.02,
                        xanchor='center', x=0.5, font_size=10),
            margin=dict(r=120),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Histograma + Distribución ──
    st.markdown("---")
    col_hist, col_run = st.columns(2)

    with col_hist:
        st.subheader("Distribucion de Valores")
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Histogram(
            x=values, nbinsx=min(30, max(8, n//3)),
            marker_color='#1565C0', opacity=0.7,
            name='Frecuencia',
        ))
        # Curva normal superpuesta
        x_norm = np.linspace(mean - 4*sigma, mean + 4*sigma, 200)
        y_norm = (1/(sigma * np.sqrt(2 * np.pi))) * np.exp(-0.5 * ((x_norm - mean)/sigma)**2)
        bin_width = (values.max() - values.min()) / min(30, max(8, n//3))
        y_norm_scaled = y_norm * n * bin_width
        fig_hist.add_trace(go.Scatter(
            x=x_norm, y=y_norm_scaled,
            mode='lines', name='Normal teórica',
            line=dict(color='#E53935', width=2),
        ))
        for lv, lc, ln in [(ucl_3, '#E53935', 'UCL'), (lcl_3, '#E53935', 'LCL'),
                            (mean, '#1565C0', 'Media')]:
            fig_hist.add_vline(x=lv, line_dash='dash', line_color=lc, line_width=1)
        fig_hist.update_layout(
            height=350, plot_bgcolor='#ffffff',
            xaxis_title=PARAMS_SPC[param_key], yaxis_title='Frecuencia',
            showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    with col_run:
        st.subheader("Run Chart (Rangos Moviles)")
        if n >= 2:
            mr = [abs(values[i] - values[i-1]) for i in range(1, n)]
            mr_mean = np.mean(mr)
            mr_ucl = mr_mean * 3.267  # D4 para n=2

            fig_mr = go.Figure()
            fig_mr.add_trace(go.Scatter(
                x=list(range(2, n+1)), y=mr,
                mode='lines+markers', name='MR',
                line=dict(color='#00897B', width=1.5),
                marker=dict(size=6),
            ))
            fig_mr.add_hline(y=mr_mean, line_color='#1565C0', line_width=1.5,
                             annotation_text=f'MR̄={mr_mean:.4f}',
                             annotation_position='right')
            fig_mr.add_hline(y=mr_ucl, line_dash='dash', line_color='#E53935', line_width=1.5,
                             annotation_text=f'UCL={mr_ucl:.4f}',
                             annotation_position='right')
            fig_mr.update_layout(
                height=350, plot_bgcolor='#ffffff',
                xaxis_title='Muestra', yaxis_title='Rango Movil',
                showlegend=False,
            )
            st.plotly_chart(fig_mr, use_container_width=True)
        else:
            st.info("Se necesitan al menos 2 muestras para el Run Chart.")

    # ── Tabla de datos ──
    with st.expander("Ver datos utilizados"):
        data_table = pd.DataFrame({
            '#': range(1, n+1),
            'Muestra': nombres,
            PARAMS_SPC[param_key]: [f"{v:.4f}" for v in values],
            'z-score': [f"{(v - mean)/sigma:.2f}" if sigma > 0 else '0' for v in values],
            'Estado': ['🔴 Fuera' if i in violation_indices else '🟢 OK' for i in range(n)],
        })
        st.dataframe(data_table, use_container_width=True, hide_index=True)

    # ── Resumen Westgard ──
    st.markdown("---")
    st.subheader("Resumen de Reglas Westgard")

    reglas_ref = [
        ('1-3s', 'Un punto fuera de ±3σ', 'Alarma — posible error o cambio de proceso',
         len([v for v in violations if v['regla'] == '1-3s'])),
        ('2-2s', 'Dos consecutivos fuera de ±2σ mismo lado', 'Advertencia — sesgo sistematico',
         len([v for v in violations if v['regla'] == '2-2s'])),
        ('R-4s', 'Rango entre consecutivos > 4σ', 'Advertencia — imprecision repentina',
         len([v for v in violations if v['regla'] == 'R-4s'])),
        ('4-1s', 'Cuatro consecutivos del mismo lado de X̄', 'Advertencia — tendencia/sesgo',
         len([v for v in violations if v['regla'] == '4-1s'])),
    ]
    reglas_data = []
    for regla, desc, accion, count in reglas_ref:
        icon = '🟢' if count == 0 else ('🔴' if 'Alarma' in accion else '🟡')
        reglas_data.append({
            'Regla': regla, 'Descripcion': desc,
            'Accion': accion, 'Violaciones': f"{icon} {count}",
        })
    st.dataframe(pd.DataFrame(reglas_data), use_container_width=True, hide_index=True)


# =====================================================
# CURVAS DE GRESIFICACIÓN
# =====================================================
def page_curvas_gresificacion():
    st.title("🔥 Curvas de Gresificacion")
    st.caption("Analisis de comportamiento ceramico: Absorcion de Agua y Contraccion en funcion "
               "de la temperatura de coccion. Herramienta clave para determinar la ventana de coccion optima.")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    nombres = sorted(df['nombre'].dropna().unique())
    sel = st.multiselect("Selecciona muestras:", nombres,
                         default=nombres[:6] if len(nombres) >= 6 else nombres)
    if not sel:
        st.info("Selecciona al menos una muestra.")
        return

    df_sel = df[df['nombre'].isin(sel)].copy()

    # Verificar si hay datos multi-temperatura (varias filas por muestra con distinta T)
    has_multi_temp = False
    for nombre in sel:
        df_m = df_sel[df_sel['nombre'] == nombre]
        temps = df_m['temperatura_coccion'].dropna().unique()
        if len(temps) > 1:
            has_multi_temp = True
            break

    # ── CONSTANTES DE ZONAS ──
    ZONAS = [
        {'nombre': 'Porcelanato optimo', 'aa_min': 0,  'aa_max': 0.5,
         'color': 'rgba(76,175,80,0.12)',  'border': '#4CAF50', 'text_color': '#2E7D32'},
        {'nombre': 'Gres vitrificado',   'aa_min': 0.5, 'aa_max': 3.0,
         'color': 'rgba(255,235,59,0.10)', 'border': '#FBC02D', 'text_color': '#F57F17'},
        {'nombre': 'Stoneware',          'aa_min': 3.0, 'aa_max': 6.0,
         'color': 'rgba(255,152,0,0.10)',  'border': '#FF9800', 'text_color': '#E65100'},
        {'nombre': 'Semi-gres',          'aa_min': 6.0, 'aa_max': 10.0,
         'color': 'rgba(244,67,54,0.08)',  'border': '#EF5350', 'text_color': '#C62828'},
        {'nombre': 'Poroso',             'aa_min': 10.0, 'aa_max': 25.0,
         'color': 'rgba(158,158,158,0.08)', 'border': '#9E9E9E', 'text_color': '#616161'},
    ]

    COLORES_MUESTRAS = [
        '#1565C0', '#C62828', '#2E7D32', '#E65100', '#6A1B9A',
        '#00838F', '#4E342E', '#283593', '#AD1457', '#00695C',
        '#EF6C00', '#1B5E20', '#4527A0', '#BF360C', '#006064',
    ]

    tab1, tab2 = st.tabs(["Curvas de Gresificacion", "Mapa AA vs Contraccion"])

    # ────────── TAB 1: CURVAS ──────────
    with tab1:
        if has_multi_temp:
            st.subheader("Curvas de Gresificacion (Multi-temperatura)")
            st.caption("Absorcion de Agua (AA%) y Contraccion Lineal (CL%) vs Temperatura de Coccion.")

            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                subplot_titles=("Absorcion de Agua (%)", "Contraccion Lineal (%)"),
            )

            for idx, nombre in enumerate(sel):
                df_m = df_sel[df_sel['nombre'] == nombre].copy()
                df_m = df_m.dropna(subset=['temperatura_coccion'])
                if df_m.empty:
                    continue
                df_m = df_m.sort_values('temperatura_coccion')
                color = COLORES_MUESTRAS[idx % len(COLORES_MUESTRAS)]

                # AA vs T
                if df_m['absorcion'].notna().any():
                    fig.add_trace(go.Scatter(
                        x=df_m['temperatura_coccion'], y=df_m['absorcion'],
                        mode='lines+markers', name=f'{nombre} (AA)',
                        line=dict(color=color, width=2.5),
                        marker=dict(size=8, line=dict(width=1, color='#fff')),
                        legendgroup=nombre, showlegend=True,
                        hovertemplate=f'<b>{nombre}</b><br>T=%{{x:.0f}}°C<br>AA=%{{y:.2f}}%<extra></extra>',
                    ), row=1, col=1)

                # Contracción vs T
                if df_m['contraccion'].notna().any():
                    fig.add_trace(go.Scatter(
                        x=df_m['temperatura_coccion'], y=df_m['contraccion'],
                        mode='lines+markers', name=f'{nombre} (CL)',
                        line=dict(color=color, width=2.5, dash='dot'),
                        marker=dict(size=8, symbol='diamond', line=dict(width=1, color='#fff')),
                        legendgroup=nombre, showlegend=False,
                        hovertemplate=f'<b>{nombre}</b><br>T=%{{x:.0f}}°C<br>CL=%{{y:.2f}}%<extra></extra>',
                    ), row=2, col=1)

            # Zonas horizontales en gráfico de AA
            for z in ZONAS:
                if z['aa_max'] <= 15:
                    fig.add_hrect(y0=z['aa_min'], y1=z['aa_max'],
                                  fillcolor=z['color'], line_width=0,
                                  annotation_text=z['nombre'],
                                  annotation_position='right',
                                  annotation_font_size=9,
                                  annotation_font_color=z['text_color'],
                                  row=1, col=1)

            fig.update_layout(
                height=700, plot_bgcolor='#ffffff',
                xaxis2=dict(title='Temperatura (°C)', gridcolor='#eee'),
                yaxis=dict(title='AA (%)', gridcolor='#eee', zeroline=True, zerolinecolor='#ccc'),
                yaxis2=dict(title='CL (%)', gridcolor='#eee', zeroline=True, zerolinecolor='#ccc'),
                legend=dict(orientation='h', yanchor='bottom', y=1.04, xanchor='center', x=0.5,
                            font_size=10),
                margin=dict(t=60),
            )
            st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("No se detectan datos multi-temperatura. Usa la pestana **Mapa AA vs Contraccion** "
                    "para analisis con datos a una sola temperatura.")

        # ── Tabla de resumen por muestra ──
        st.subheader("Resumen de Parametros de Coccion")
        resumen_rows = []
        for nombre in sel:
            df_m = df_sel[df_sel['nombre'] == nombre].copy()
            aa_vals = df_m['absorcion'].dropna()
            cl_vals = df_m['contraccion'].dropna()
            t_vals  = df_m['temperatura_coccion'].dropna()

            if aa_vals.empty:
                continue

            aa_min = aa_vals.min()
            cl_max = cl_vals.max() if not cl_vals.empty else None
            t_opt  = None
            ventana = None

            if not t_vals.empty and len(t_vals) > 1:
                # T óptima = temperatura donde AA es mínima
                idx_min = aa_vals.idxmin()
                t_opt = df_m.loc[idx_min, 'temperatura_coccion']
                t_opt = float(t_opt) if pd.notna(t_opt) else None

                # Ventana de cocción: rango de T donde AA < AA_min * 1.5 (o +0.5% abs)
                umbral = max(aa_min * 1.5, aa_min + 0.5)
                t_dentro = t_vals[aa_vals <= umbral]
                if len(t_dentro) >= 2:
                    ventana = f"{t_dentro.min():.0f} – {t_dentro.max():.0f}°C (±{(t_dentro.max()-t_dentro.min())/2:.0f}°C)"
                elif t_opt:
                    ventana = f"~{t_opt:.0f}°C (dato unico)"
            elif not t_vals.empty:
                t_opt = float(t_vals.iloc[0])
                ventana = f"~{t_opt:.0f}°C (dato unico)"

            # Clasificación por AA
            if aa_min < 0.5:
                clasif = 'Porcelanato'
            elif aa_min < 3:
                clasif = 'Gres vitrificado'
            elif aa_min < 6:
                clasif = 'Stoneware'
            elif aa_min < 10:
                clasif = 'Semi-gres'
            else:
                clasif = 'Poroso'

            resumen_rows.append({
                'Muestra': nombre,
                'AA Minima (%)': f"{aa_min:.2f}",
                'CL Maxima (%)': f"{cl_max:.2f}" if cl_max is not None else '—',
                'T Optima (°C)': f"{t_opt:.0f}" if t_opt else '—',
                'Ventana Coccion': ventana or '—',
                'Clasificacion': clasif,
            })

        if resumen_rows:
            st.dataframe(pd.DataFrame(resumen_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No hay datos de absorcion disponibles para las muestras seleccionadas.")

    # ────────── TAB 2: MAPA AA vs CONTRACCIÓN ──────────
    with tab2:
        st.subheader("Mapa Absorcion vs Contraccion")
        st.caption("Cada punto es una muestra. Las zonas coloreadas indican clasificacion ceramica.")

        # Filtrar muestras con datos
        df_plot = df_sel.dropna(subset=['absorcion']).copy()
        if 'contraccion' in df_plot.columns:
            df_plot['contraccion'] = df_plot['contraccion'].fillna(0)
        else:
            df_plot['contraccion'] = 0

        if df_plot.empty:
            st.warning("No hay datos de absorcion para graficar.")
            return

        fig2 = go.Figure()

        # Zonas de referencia
        max_cl = max(df_plot['contraccion'].max() * 1.3, 12) if df_plot['contraccion'].max() > 0 else 12

        for z in ZONAS:
            fig2.add_shape(
                type='rect',
                x0=z['aa_min'], x1=z['aa_max'],
                y0=0, y1=max_cl,
                fillcolor=z['color'],
                line=dict(color=z['border'], width=1, dash='dot'),
                layer='below',
            )
            fig2.add_annotation(
                x=(z['aa_min'] + min(z['aa_max'], 20)) / 2,
                y=max_cl * 0.95,
                text=f"<b>{z['nombre']}</b>",
                showarrow=False,
                font=dict(size=9, color=z['text_color']),
                opacity=0.85,
            )

        # Zona especial porcelanato óptimo (rectángulo verde fuerte)
        fig2.add_shape(
            type='rect', x0=0, x1=0.5, y0=6, y1=8,
            fillcolor='rgba(76,175,80,0.25)',
            line=dict(color='#2E7D32', width=2),
            layer='below',
        )
        fig2.add_annotation(
            x=0.25, y=7, text='<b>Porcelanato<br>optimo</b>',
            showarrow=False, font=dict(size=8, color='#1B5E20'),
        )

        # Puntos de muestras
        for idx, nombre in enumerate(sel):
            df_m = df_plot[df_plot['nombre'] == nombre]
            if df_m.empty:
                continue
            color = COLORES_MUESTRAS[idx % len(COLORES_MUESTRAS)]

            fig2.add_trace(go.Scatter(
                x=df_m['absorcion'], y=df_m['contraccion'],
                mode='markers+text',
                name=nombre,
                text=[nombre] * len(df_m),
                textposition='top center',
                textfont=dict(size=9, color=color),
                marker=dict(
                    size=14,
                    color=color,
                    line=dict(width=2, color='#ffffff'),
                    opacity=0.9,
                ),
                hovertemplate=(
                    f'<b>{nombre}</b><br>'
                    'AA: %{x:.2f}%<br>'
                    'CL: %{y:.2f}%<extra></extra>'
                ),
            ))

        # Línea de referencia AA=0.5% (umbral porcelanato)
        fig2.add_vline(x=0.5, line_dash='dash', line_color='#2E7D32', line_width=1.5,
                       annotation_text='AA=0.5%', annotation_position='top',
                       annotation_font_size=9, annotation_font_color='#2E7D32')

        # Línea de referencia AA=3% (umbral gres)
        fig2.add_vline(x=3.0, line_dash='dash', line_color='#F57F17', line_width=1,
                       annotation_text='AA=3%', annotation_position='top',
                       annotation_font_size=9, annotation_font_color='#F57F17')

        fig2.update_layout(
            xaxis_title='Absorcion de Agua (%)',
            yaxis_title='Contraccion Lineal (%)',
            height=600,
            plot_bgcolor='#ffffff',
            xaxis=dict(gridcolor='#f0f0f0', zeroline=False, range=[-0.5, max(df_plot['absorcion'].max()*1.2, 15)]),
            yaxis=dict(gridcolor='#f0f0f0', zeroline=False),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5, font_size=10),
            margin=dict(t=50),
        )
        st.plotly_chart(fig2, use_container_width=True)

        # KPIs rápidos del grupo seleccionado
        st.markdown("---")
        aa_all = df_plot['absorcion'].dropna()
        cl_all = df_plot['contraccion'].dropna()
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("AA Promedio", f"{aa_all.mean():.2f}%")
        k2.metric("AA Rango", f"{aa_all.min():.1f} – {aa_all.max():.1f}%")
        k3.metric("CL Promedio", f"{cl_all.mean():.2f}%" if not cl_all.empty else "—")

        # Contar cuántas caen en cada zona
        n_porc = len(aa_all[aa_all < 0.5])
        n_gres = len(aa_all[(aa_all >= 0.5) & (aa_all < 3)])
        n_stone = len(aa_all[(aa_all >= 3) & (aa_all < 6)])
        n_semi = len(aa_all[(aa_all >= 6) & (aa_all < 10)])
        n_poro = len(aa_all[aa_all >= 10])
        k4.metric("Distribucion",
                   f"{n_porc}P {n_gres}G {n_stone}S {n_semi}SG {n_poro}Po")

        # Pie chart de distribución
        dist_labels = ['Porcelanato', 'Gres', 'Stoneware', 'Semi-gres', 'Poroso']
        dist_vals = [n_porc, n_gres, n_stone, n_semi, n_poro]
        dist_colors = ['#4CAF50', '#FBC02D', '#FF9800', '#EF5350', '#9E9E9E']
        if sum(dist_vals) > 0:
            c_pie, c_table = st.columns([1, 1])
            with c_pie:
                fig_pie = go.Figure(data=[go.Pie(
                    labels=dist_labels, values=dist_vals,
                    marker=dict(colors=dist_colors, line=dict(color='#fff', width=2)),
                    textinfo='label+value', textfont_size=11,
                    hole=0.4,
                )])
                fig_pie.update_layout(
                    height=350, showlegend=False, margin=dict(t=20, b=20),
                    annotations=[dict(text=f'{sum(dist_vals)}', x=0.5, y=0.5,
                                      font_size=20, showarrow=False)],
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            with c_table:
                st.markdown("**Distribucion por zona:**")
                for label, val, color in zip(dist_labels, dist_vals, dist_colors):
                    if val > 0:
                        pct = val / sum(dist_vals) * 100
                        st.markdown(
                            f'<div style="display:flex;align-items:center;gap:8px;margin:4px 0">'
                            f'<div style="width:14px;height:14px;background:{color};border-radius:3px"></div>'
                            f'<span style="font-size:13px"><b>{label}:</b> {val} ({pct:.0f}%)</span></div>',
                            unsafe_allow_html=True
                        )


# =====================================================
# FÓRMULA SEGER / UMF (Unity Molecular Formula)
# =====================================================
def page_seger_umf():
    st.title("⚗️ Formula Seger / UMF")
    st.caption("Conversion de analisis quimico (FRX) a Formula Molecular Unitaria — "
               "metodo estandar para evaluacion ceramica de materias primas y pastas.")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    # Pesos moleculares
    PM = {
        'sio2':   60.08,
        'al2o3': 101.96,
        'fe2o3': 159.69,
        'tio2':   79.87,
        'cao':    56.08,
        'mgo':    40.30,
        'k2o':    94.20,
        'na2o':   61.98,
    }

    # Grupos Seger
    FUNDENTES   = ['cao', 'mgo', 'k2o', 'na2o']   # RO / R₂O
    ESTABILIZ   = ['al2o3', 'fe2o3', 'tio2']       # R₂O₃
    FORMADORES  = ['sio2']                           # RO₂

    LABELS_SEGER = {
        'sio2': 'SiO₂', 'al2o3': 'Al₂O₃', 'fe2o3': 'Fe₂O₃', 'tio2': 'TiO₂',
        'cao': 'CaO', 'mgo': 'MgO', 'k2o': 'K₂O', 'na2o': 'Na₂O',
    }

    # ── Selección de muestras ──
    nombres = sorted(df['nombre'].dropna().unique())
    sel = st.multiselect("Selecciona muestras para calcular UMF:",
                         nombres, default=nombres[:5] if len(nombres) >= 5 else nombres)

    if not sel:
        st.info("Selecciona al menos una muestra.")
        return

    df_sel = df[df['nombre'].isin(sel)].copy()

    # ── Cálculo UMF por muestra ──
    umf_rows = []
    for _, row in df_sel.iterrows():
        nombre = row['nombre']

        # 1. Porcentaje a moles
        moles = {}
        for ox, pm in PM.items():
            val = row.get(ox)
            moles[ox] = float(val) / pm if pd.notna(val) and float(val) > 0 else 0.0

        # 2. Total de fundentes para normalización
        total_fund = sum(moles[ox] for ox in FUNDENTES)
        if total_fund <= 0:
            continue  # no se puede normalizar

        # 3. Normalizar (Unity = fundentes suman 1.0)
        factor = 1.0 / total_fund
        umf = {ox: moles[ox] * factor for ox in PM}

        # 4. Ratios
        al2o3_umf = umf.get('al2o3', 0)
        sio2_umf  = umf.get('sio2', 0)
        ratio_si_al = sio2_umf / al2o3_umf if al2o3_umf > 0 else 0
        alcalis   = umf.get('k2o', 0) + umf.get('na2o', 0)
        alcalinot = umf.get('cao', 0) + umf.get('mgo', 0)
        ratio_alc = alcalis / alcalinot if alcalinot > 0 else 0

        umf_entry = {'Muestra': nombre}
        for ox in FUNDENTES:
            umf_entry[LABELS_SEGER[ox]] = umf[ox]
        umf_entry['Σ RO/R₂O'] = 1.0
        for ox in ESTABILIZ:
            umf_entry[LABELS_SEGER[ox]] = umf[ox]
        umf_entry['Σ R₂O₃'] = sum(umf[ox] for ox in ESTABILIZ)
        for ox in FORMADORES:
            umf_entry[LABELS_SEGER[ox]] = umf[ox]
        umf_entry['SiO₂/Al₂O₃'] = ratio_si_al
        umf_entry['Álcalis/Alcalinot.'] = ratio_alc

        umf_rows.append(umf_entry)

    if not umf_rows:
        st.error("Ninguna muestra tiene datos quimicos suficientes para calcular la UMF.")
        return

    df_umf = pd.DataFrame(umf_rows)

    # ── Tabs ──
    tab1, tab2, tab3 = st.tabs(["Tabla UMF", "Diagrama Seger", "Ratios y Clasificacion"])

    # ────────── TAB 1: TABLA UMF ──────────
    with tab1:
        st.subheader("Formula Molecular Unitaria (UMF)")
        st.caption("Normalizada a Σ Fundentes (RO + R₂O) = 1.000")

        # Formato de la tabla con 3 bloques de color
        fund_cols  = [LABELS_SEGER[ox] for ox in FUNDENTES] + ['Σ RO/R₂O']
        estab_cols = [LABELS_SEGER[ox] for ox in ESTABILIZ] + ['Σ R₂O₃']
        form_cols  = [LABELS_SEGER[ox] for ox in FORMADORES]
        ratio_cols = ['SiO₂/Al₂O₃', 'Álcalis/Alcalinot.']

        # Mostrar con colores por grupo
        st.markdown("**Fundentes (RO / R₂O)**")
        st.dataframe(
            df_umf[['Muestra'] + fund_cols].style.format(
                {c: '{:.4f}' for c in fund_cols}, na_rep='—'
            ).background_gradient(subset=fund_cols, cmap='Blues', vmin=0),
            use_container_width=True, hide_index=True
        )

        st.markdown("**Estabilizadores (R₂O₃)**")
        st.dataframe(
            df_umf[['Muestra'] + estab_cols].style.format(
                {c: '{:.4f}' for c in estab_cols}, na_rep='—'
            ).background_gradient(subset=estab_cols, cmap='Greens', vmin=0),
            use_container_width=True, hide_index=True
        )

        st.markdown("**Formadores (RO₂) y Ratios**")
        st.dataframe(
            df_umf[['Muestra'] + form_cols + ratio_cols].style.format(
                {c: '{:.4f}' for c in form_cols}, na_rep='—'
            ).format({c: '{:.2f}' for c in ratio_cols}, na_rep='—'
            ).background_gradient(subset=form_cols, cmap='Oranges', vmin=0),
            use_container_width=True, hide_index=True
        )

        # Tabla completa compacta
        with st.expander("Ver tabla completa en una sola vista"):
            all_cols = ['Muestra'] + fund_cols + estab_cols + form_cols + ratio_cols
            fmt = {c: '{:.4f}' for c in fund_cols + estab_cols + form_cols}
            fmt.update({c: '{:.2f}' for c in ratio_cols})
            st.dataframe(df_umf[all_cols].style.format(fmt, na_rep='—'),
                         use_container_width=True, hide_index=True)

    # ────────── TAB 2: DIAGRAMA SEGER ──────────
    with tab2:
        st.subheader("Diagrama Al₂O₃ vs SiO₂ (UMF)")
        st.caption("Zonas de referencia tipicas para productos ceramicos cocidos.")

        fig = go.Figure()

        # Zonas de referencia (rectángulos)
        zonas = [
            {'nombre': 'Porcelanato',     'al2o3': (0.35, 0.55), 'sio2': (3.5, 5.5),
             'color': 'rgba(33,150,243,0.10)', 'border': '#2196F3'},
            {'nombre': 'Gres',            'al2o3': (0.25, 0.50), 'sio2': (2.8, 4.5),
             'color': 'rgba(76,175,80,0.10)',  'border': '#4CAF50'},
            {'nombre': 'Loza / Earthenware', 'al2o3': (0.15, 0.35), 'sio2': (2.0, 4.0),
             'color': 'rgba(255,152,0,0.10)',  'border': '#FF9800'},
            {'nombre': 'Refractario',     'al2o3': (0.50, 1.20), 'sio2': (2.0, 6.0),
             'color': 'rgba(244,67,54,0.08)',  'border': '#f44336'},
        ]

        for z in zonas:
            x0, x1 = z['al2o3']
            y0, y1 = z['sio2']
            fig.add_shape(type='rect', x0=x0, y0=y0, x1=x1, y1=y1,
                          fillcolor=z['color'], line=dict(color=z['border'], width=1.5, dash='dot'),
                          layer='below')
            fig.add_annotation(x=(x0 + x1) / 2, y=y1,
                               text=f"<b>{z['nombre']}</b>", showarrow=False,
                               font=dict(size=10, color=z['border']),
                               yshift=10, opacity=0.8)

        # Puntos de las muestras
        fig.add_trace(go.Scatter(
            x=df_umf['Al₂O₃'],
            y=df_umf['SiO₂'],
            mode='markers+text',
            text=df_umf['Muestra'],
            textposition='top center',
            textfont=dict(size=9),
            marker=dict(
                size=12,
                color=df_umf['SiO₂/Al₂O₃'],
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title='SiO₂/Al₂O₃'),
                line=dict(width=1.5, color='#333'),
            ),
            hovertemplate=(
                '<b>%{text}</b><br>'
                'Al₂O₃ UMF: %{x:.4f}<br>'
                'SiO₂ UMF: %{y:.4f}<br>'
                'SiO₂/Al₂O₃: %{marker.color:.2f}<extra></extra>'
            ),
            name='Muestras'
        ))

        fig.update_layout(
            xaxis_title='Al₂O₃ (UMF)',
            yaxis_title='SiO₂ (UMF)',
            height=550,
            plot_bgcolor='#fafafa',
            xaxis=dict(gridcolor='#eee', zeroline=False),
            yaxis=dict(gridcolor='#eee', zeroline=False),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Gráfico de barras apiladas con composición UMF
        st.subheader("Composicion UMF por Muestra")
        fig_bar = go.Figure()
        colores_ox = {
            'CaO': '#2196F3', 'MgO': '#03A9F4', 'K₂O': '#00BCD4', 'Na₂O': '#009688',
            'Al₂O₃': '#4CAF50', 'Fe₂O₃': '#8BC34A', 'TiO₂': '#CDDC39',
            'SiO₂': '#FF9800',
        }
        all_oxides = [LABELS_SEGER[ox] for ox in FUNDENTES + ESTABILIZ + FORMADORES]
        for ox_label in all_oxides:
            if ox_label in df_umf.columns:
                fig_bar.add_trace(go.Bar(
                    name=ox_label,
                    x=df_umf['Muestra'],
                    y=df_umf[ox_label],
                    marker_color=colores_ox.get(ox_label, '#999'),
                ))
        fig_bar.update_layout(
            barmode='stack',
            yaxis_title='Moles (UMF)',
            height=450,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    # ────────── TAB 3: RATIOS Y CLASIFICACIÓN ──────────
    with tab3:
        st.subheader("Ratios Clave y Clasificacion")

        # Métricas promedio
        avg_si_al = df_umf['SiO₂/Al₂O₃'].mean()
        avg_alc   = df_umf['Álcalis/Alcalinot.'].mean()
        avg_al2o3 = df_umf['Al₂O₃'].mean()
        avg_sio2  = df_umf['SiO₂'].mean()

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("SiO₂/Al₂O₃ Prom.", f"{avg_si_al:.2f}")
        m2.metric("Álcalis/Alcalinot.", f"{avg_alc:.2f}")
        m3.metric("Al₂O₃ UMF Prom.", f"{avg_al2o3:.4f}")
        m4.metric("SiO₂ UMF Prom.", f"{avg_sio2:.4f}")

        # Clasificación por ratio SiO₂/Al₂O₃
        st.markdown("---")
        st.markdown("**Clasificacion por ratio SiO₂/Al₂O₃ molar:**")

        ref_data = [
            {'Rango SiO₂/Al₂O₃': '< 5', 'Tipo': 'Caolinítica / Refractaria',
             'Característica': 'Alta alúmina, alta refractariedad, baja fundencia'},
            {'Rango SiO₂/Al₂O₃': '5 – 8', 'Tipo': 'Arcilla plástica / Ball Clay',
             'Característica': 'Buena plasticidad, uso en gres y porcelanato'},
            {'Rango SiO₂/Al₂O₃': '8 – 12', 'Tipo': 'Arcilla común / Earthenware',
             'Característica': 'Alta sílice libre, uso en ladrillería y tejas'},
            {'Rango SiO₂/Al₂O₃': '> 12', 'Tipo': 'Arcilla silícea / Arenosa',
             'Característica': 'Exceso de cuarzo, baja plasticidad'},
        ]
        st.dataframe(pd.DataFrame(ref_data), use_container_width=True, hide_index=True)

        # Tabla de clasificación individual
        st.markdown("**Resultado por muestra:**")
        clas_rows = []
        for _, r in df_umf.iterrows():
            ratio = r['SiO₂/Al₂O₃']
            if ratio < 5:
                tipo = 'Caolinítica / Refractaria'
            elif ratio < 8:
                tipo = 'Arcilla plástica / Ball Clay'
            elif ratio < 12:
                tipo = 'Arcilla común / Earthenware'
            else:
                tipo = 'Arcilla silícea / Arenosa'
            clas_rows.append({
                'Muestra': r['Muestra'],
                'SiO₂/Al₂O₃': f"{ratio:.2f}",
                'Álcalis/Alcalinot.': f"{r['Álcalis/Alcalinot.']:.2f}",
                'Clasificacion': tipo,
            })
        st.dataframe(pd.DataFrame(clas_rows), use_container_width=True, hide_index=True)

        # Scatter de ratios
        st.markdown("---")
        st.subheader("Mapa de Ratios")
        fig_rat = px.scatter(
            df_umf, x='SiO₂/Al₂O₃', y='Álcalis/Alcalinot.',
            text='Muestra', size_max=14,
            labels={'SiO₂/Al₂O₃': 'SiO₂ / Al₂O₃ (molar)',
                    'Álcalis/Alcalinot.': '(K₂O+Na₂O) / (CaO+MgO)'},
        )
        fig_rat.update_traces(
            marker=dict(size=12, line=dict(width=1, color='#333')),
            textposition='top center', textfont_size=9,
        )
        # Líneas de referencia
        fig_rat.add_vline(x=5, line_dash='dash', line_color='#2196F3',
                          annotation_text='Caolinítica', annotation_position='top left')
        fig_rat.add_vline(x=8, line_dash='dash', line_color='#4CAF50',
                          annotation_text='Ball Clay', annotation_position='top left')
        fig_rat.add_vline(x=12, line_dash='dash', line_color='#FF9800',
                          annotation_text='Común', annotation_position='top left')
        fig_rat.update_layout(height=500, plot_bgcolor='#fafafa')
        st.plotly_chart(fig_rat, use_container_width=True)


# =====================================================
# PREDICCION DE COLOR COCIDO (ML)
# =====================================================
_PRED_FEATURES = ['fe2o3', 'al2o3', 'sio2', 'tio2', 'cao', 'k2o', 'na2o']
_PRED_TARGETS = ['l_color', 'a_color', 'b_color']
_PRED_LABELS = {
    'fe2o3': 'Fe₂O₃ (%)', 'al2o3': 'Al₂O₃ (%)', 'sio2': 'SiO₂ (%)',
    'tio2': 'TiO₂ (%)', 'cao': 'CaO (%)',
    'k2o': 'K₂O (%)', 'na2o': 'Na₂O (%)',
}

# Intentar importar sklearn; si no esta, usar fallback numpy
try:
    from sklearn.linear_model import LinearRegression
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import r2_score, mean_absolute_error
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False


def _entrenar_modelos_color(df_train):
    """Entrena modelos de regresion para L*, a*, b*.
    Retorna dict con info de cada modelo + el dataframe limpio usado."""
    X = df_train[_PRED_FEATURES].values
    resultados = {}
    for target in _PRED_TARGETS:
        y = df_train[target].values
        if _HAS_SKLEARN:
            # Linear Regression
            lr = LinearRegression()
            lr.fit(X, y)
            y_pred_lr = lr.predict(X)
            r2_lr = r2_score(y, y_pred_lr)
            mae_lr = mean_absolute_error(y, y_pred_lr)
            # Random Forest
            rf = RandomForestRegressor(n_estimators=80, max_depth=8,
                                       random_state=42, n_jobs=-1)
            rf.fit(X, y)
            y_pred_rf = rf.predict(X)
            r2_rf = r2_score(y, y_pred_rf)
            mae_rf = mean_absolute_error(y, y_pred_rf)
            # Elegir el mejor
            if r2_rf >= r2_lr:
                best_model, best_name = rf, 'RandomForest'
                best_pred, best_r2, best_mae = y_pred_rf, r2_rf, mae_rf
            else:
                best_model, best_name = lr, 'LinearRegression'
                best_pred, best_r2, best_mae = y_pred_lr, r2_lr, mae_lr
            resultados[target] = {
                'model': best_model, 'name': best_name,
                'r2': best_r2, 'mae': best_mae,
                'y_true': y, 'y_pred': best_pred,
                'lr_r2': r2_lr, 'rf_r2': r2_rf,
                'lr_mae': mae_lr, 'rf_mae': mae_rf,
            }
        else:
            # Fallback: numpy polyfit para cada feature (multivariado simple)
            # Usamos pseudoinversa para regresion lineal manual
            X_b = np.column_stack([X, np.ones(len(X))])
            coefs, _, _, _ = np.linalg.lstsq(X_b, y, rcond=None)
            y_pred = X_b @ coefs
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
            mae = np.mean(np.abs(y - y_pred))
            resultados[target] = {
                'model': coefs, 'name': 'NumPy OLS (fallback)',
                'r2': r2, 'mae': mae,
                'y_true': y, 'y_pred': y_pred,
                'lr_r2': r2, 'rf_r2': None, 'lr_mae': mae, 'rf_mae': None,
            }
    return resultados


def _predecir_color(modelos, x_input):
    """Predice L*, a*, b* a partir de un vector de features."""
    preds = {}
    x_arr = np.array(x_input).reshape(1, -1)
    for target in _PRED_TARGETS:
        info = modelos[target]
        if _HAS_SKLEARN:
            preds[target] = float(info['model'].predict(x_arr)[0])
        else:
            x_b = np.append(x_arr.flatten(), 1.0)
            preds[target] = float(x_b @ info['model'])
    return preds


def _lab_to_rgb_safe(L, a, b):
    """Conversion simplificada L*a*b* -> RGB para visualizacion."""
    L = max(0, min(100, L))
    a = max(-128, min(127, a))
    b = max(-128, min(127, b))
    # L*a*b* -> XYZ (D65 iluminante)
    fy = (L + 16) / 116
    fx = a / 500 + fy
    fz = fy - b / 200
    eps = 0.008856
    kappa = 903.3
    xr = fx**3 if fx**3 > eps else (116 * fx - 16) / kappa
    yr = ((L + 16) / 116)**3 if L > kappa * eps else L / kappa
    zr = fz**3 if fz**3 > eps else (116 * fz - 16) / kappa
    # D65 reference white
    X = xr * 0.95047
    Y = yr * 1.00000
    Z = zr * 1.08883
    # XYZ -> linear RGB (sRGB)
    r_lin =  3.2404542 * X - 1.5371385 * Y - 0.4985314 * Z
    g_lin = -0.9692660 * X + 1.8760108 * Y + 0.0415560 * Z
    b_lin =  0.0556434 * X - 0.2040259 * Y + 1.0572252 * Z
    # Gamma correction
    def gamma(c):
        c = max(0, min(1, c))
        return 12.92 * c if c <= 0.0031308 else 1.055 * c**(1/2.4) - 0.055
    R = int(round(gamma(r_lin) * 255))
    G = int(round(gamma(g_lin) * 255))
    B = int(round(gamma(b_lin) * 255))
    return max(0, min(255, R)), max(0, min(255, G)), max(0, min(255, B))


def page_prediccion_color():
    """Modulo de Prediccion de Color Cocido usando ML."""
    st.title("🎨 Prediccion de Color Cocido")
    st.markdown('<div class="section-badge">🤖 Machine Learning aplicado a ceramica</div>',
                unsafe_allow_html=True)
    st.caption("Predice el color L*a*b* de coccion a partir de la composicion quimica "
               "y temperatura, usando modelos entrenados con los datos de la base.")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos para entrenar el modelo.")
        return

    # Filtrar muestras con datos completos
    required_cols = _PRED_FEATURES + _PRED_TARGETS
    df_clean = df.dropna(subset=required_cols).copy()

    if len(df_clean) < 5:
        st.error(f"Se necesitan al menos 5 muestras con datos completos de quimica, "
                 f"temperatura y color. Actualmente hay **{len(df_clean)}**.")
        st.info("Columnas necesarias: " + ", ".join(_PRED_LABELS.values()) + ", L*, a*, b*")
        return

    # ── Entrenar modelos (cachear en session_state) ──
    cache_key = f"pred_color_models_{len(df_clean)}"
    if cache_key not in st.session_state:
        with st.spinner("Entrenando modelos de prediccion de color..."):
            st.session_state[cache_key] = _entrenar_modelos_color(df_clean)
    modelos = st.session_state[cache_key]

    # ── Tabs principales ──
    tab_pred, tab_valid, tab_info = st.tabs([
        "🎯 Prediccion", "📊 Validacion del Modelo", "ℹ️ Info del Modelo"
    ])

    # ══════════ TAB 1: PREDICCION ══════════
    with tab_pred:
        st.subheader("Ingrese composicion quimica y temperatura")

        # Rangos de la BD para los sliders
        rangos = {}
        for feat in _PRED_FEATURES:
            col_data = df_clean[feat]
            rangos[feat] = {
                'min': float(col_data.min()),
                'max': float(col_data.max()),
                'mean': float(col_data.mean()),
            }

        # Sliders
        col_s1, col_s2, col_s3 = st.columns(3)
        inputs = {}
        slider_cols = [col_s1, col_s2, col_s3, col_s1, col_s2, col_s3]
        for i, feat in enumerate(_PRED_FEATURES):
            r = rangos[feat]
            step = 0.01 if feat != 'temperatura_coccion' else 5.0
            lo = r['min'] if feat != 'temperatura_coccion' else max(900.0, r['min'])
            hi = r['max'] if feat != 'temperatura_coccion' else min(1300.0, r['max'])
            # Expand range slightly for exploration
            margin = (hi - lo) * 0.15
            lo_slider = max(0.0, lo - margin) if feat != 'temperatura_coccion' else lo
            hi_slider = hi + margin if feat != 'temperatura_coccion' else hi
            with slider_cols[i]:
                inputs[feat] = st.slider(
                    _PRED_LABELS[feat],
                    min_value=round(lo_slider, 2),
                    max_value=round(hi_slider, 2),
                    value=round(r['mean'], 2),
                    step=step,
                    key=f"pred_slider_{feat}"
                )

        # Advertencia fuera de rango
        fuera_rango = []
        for feat in _PRED_FEATURES:
            r = rangos[feat]
            if inputs[feat] < r['min'] or inputs[feat] > r['max']:
                fuera_rango.append(_PRED_LABELS[feat])
        if fuera_rango:
            st.warning(f"⚠️ Valores fuera del rango de entrenamiento: **{', '.join(fuera_rango)}**. "
                       "La prediccion puede ser menos confiable.")

        st.markdown("---")

        # Predecir
        x_input = [inputs[f] for f in _PRED_FEATURES]
        preds = _predecir_color(modelos, x_input)

        L_pred = preds['l_color']
        a_pred = preds['a_color']
        b_pred = preds['b_color']

        # Mostrar resultado
        st.subheader("Resultado de Prediccion")
        rc, gc, bc = _lab_to_rgb_safe(L_pred, a_pred, b_pred)

        col_color, col_vals = st.columns([1, 2])
        with col_color:
            st.markdown(f"""
            <div style="text-align:center;padding:16px">
                <div style="width:160px;height:120px;background:rgb({rc},{gc},{bc});
                     border:3px solid #333;border-radius:16px;margin:0 auto;
                     box-shadow:0 4px 15px rgba(0,0,0,0.2)"></div>
                <div style="margin-top:10px;font-size:12px;color:#666">
                    RGB({rc}, {gc}, {bc})</div>
                <div style="font-weight:700;margin-top:4px;font-size:14px">
                    Color Predicho</div>
            </div>""", unsafe_allow_html=True)

        with col_vals:
            m1, m2, m3 = st.columns(3)
            m1.metric("L* (Luminosidad)", f"{L_pred:.1f}")
            m2.metric("a* (Rojo-Verde)", f"{a_pred:.1f}")
            m3.metric("b* (Amarillo-Azul)", f"{b_pred:.1f}")

            # Interpretacion
            brillo = "Claro" if L_pred > 70 else ("Medio" if L_pred > 45 else "Oscuro")
            tono_a = "Rojizo" if a_pred > 2 else ("Verdoso" if a_pred < -2 else "Neutro")
            tono_b = "Amarillento" if b_pred > 5 else ("Azulado" if b_pred < -2 else "Neutro")
            st.markdown(f"""
            <div class="metric-card metric-card-blue" style="margin-top:8px">
                <div class="mc-label">Interpretacion del Color</div>
                <div style="font-size:13px;color:#334155">
                    <b>{brillo}</b> · <b>{tono_a}</b> · <b>{tono_b}</b>
                </div>
                <div class="mc-sub">
                    {"Tipico de arcillas con bajo Fe₂O₃" if inputs['fe2o3'] < 1.5
                     else "Arcilla ferruginosa - tonos rojizos esperables" if inputs['fe2o3'] > 3
                     else "Contenido moderado de Fe₂O₃"}
                </div>
            </div>""", unsafe_allow_html=True)

        # ── Muestra mas similar (distancia euclidiana en espacio de oxidos) ──
        st.markdown("---")
        st.subheader("Muestra mas Similar en Base de Datos")
        x_arr = np.array(x_input)
        X_train = df_clean[_PRED_FEATURES].values
        dists = np.sqrt(np.sum(((X_train - x_arr) / (X_train.std(axis=0) + 1e-9)) ** 2, axis=1))
        idx_min = int(np.argmin(dists))
        similar = df_clean.iloc[idx_min]

        col_sim1, col_sim2 = st.columns(2)
        with col_sim1:
            st.markdown(f"""
            <div class="metric-card metric-card-amber">
                <div class="mc-label">Muestra mas cercana</div>
                <div class="mc-value" style="font-size:20px">{similar['nombre']}</div>
                <div class="mc-sub">Yacimiento: {similar.get('yacimiento', '—')} ·
                    Distancia normalizada: {dists[idx_min]:.2f}</div>
            </div>""", unsafe_allow_html=True)

        with col_sim2:
            L_sim = float(similar['l_color'])
            a_sim = float(similar['a_color'])
            b_sim = float(similar['b_color'])
            rs, gs, bs = _lab_to_rgb_safe(L_sim, a_sim, b_sim)
            st.markdown(f"""
            <div style="display:flex;gap:16px;align-items:center;justify-content:center;padding:10px">
                <div style="text-align:center">
                    <div style="width:80px;height:60px;background:rgb({rc},{gc},{bc});
                         border:2px solid #555;border-radius:10px"></div>
                    <div style="font-size:11px;margin-top:4px">Predicho</div>
                    <div style="font-size:10px;color:#888">L={L_pred:.1f}</div>
                </div>
                <div style="font-size:20px;color:#999">→</div>
                <div style="text-align:center">
                    <div style="width:80px;height:60px;background:rgb({rs},{gs},{bs});
                         border:2px solid #555;border-radius:10px"></div>
                    <div style="font-size:11px;margin-top:4px">{similar['nombre']}</div>
                    <div style="font-size:10px;color:#888">L={L_sim:.1f}</div>
                </div>
            </div>""", unsafe_allow_html=True)

        # Tabla comparativa
        comp_data = []
        for feat in _PRED_FEATURES:
            comp_data.append({
                'Parametro': _PRED_LABELS[feat],
                'Input': f"{inputs[feat]:.2f}",
                'Muestra Similar': f"{similar[feat]:.2f}" if pd.notna(similar[feat]) else '—',
            })
        for t, label in [('l_color', 'L*'), ('a_color', 'a*'), ('b_color', 'b*')]:
            comp_data.append({
                'Parametro': label,
                'Input': f"{preds[t]:.1f} (pred)",
                'Muestra Similar': f"{similar[t]:.1f}" if pd.notna(similar[t]) else '—',
            })
        st.dataframe(pd.DataFrame(comp_data), use_container_width=True, hide_index=True)

    # ══════════ TAB 2: VALIDACION ══════════
    with tab_valid:
        st.subheader("Predicho vs Real (datos de entrenamiento)")
        target_labels = {'l_color': 'L* (Luminosidad)', 'a_color': 'a* (Rojo-Verde)',
                         'b_color': 'b* (Amarillo-Azul)'}

        for target in _PRED_TARGETS:
            info = modelos[target]
            y_true = info['y_true']
            y_pred = info['y_pred']

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=y_true, y=y_pred, mode='markers',
                marker=dict(size=7, color='#1a237e', opacity=0.6),
                name='Muestras',
                hovertemplate='Real: %{x:.1f}<br>Predicho: %{y:.1f}<extra></extra>'
            ))
            # Linea ideal
            rng = [min(y_true.min(), y_pred.min()), max(y_true.max(), y_pred.max())]
            fig.add_trace(go.Scatter(
                x=rng, y=rng, mode='lines',
                line=dict(color='#F44336', dash='dash', width=2),
                name='Ideal (y=x)', showlegend=True
            ))
            fig.update_layout(
                title=f"{target_labels[target]} — R²={info['r2']:.3f}, MAE={info['mae']:.2f}",
                xaxis_title="Valor Real",
                yaxis_title="Valor Predicho",
                height=400,
                plot_bgcolor='#fafafa',
            )
            st.plotly_chart(fig, use_container_width=True)

        # Resumen de errores
        st.subheader("Resumen de Precision")
        resumen_data = []
        for target in _PRED_TARGETS:
            info = modelos[target]
            row = {
                'Variable': target_labels[target],
                'Modelo Elegido': info['name'],
                'R²': f"{info['r2']:.4f}",
                'MAE': f"{info['mae']:.2f}",
                'LinearReg R²': f"{info['lr_r2']:.4f}",
            }
            if info['rf_r2'] is not None:
                row['RandomForest R²'] = f"{info['rf_r2']:.4f}"
            else:
                row['RandomForest R²'] = 'N/A'
            resumen_data.append(row)
        st.dataframe(pd.DataFrame(resumen_data), use_container_width=True, hide_index=True)

        # Barplot de R² comparativo
        fig_r2 = go.Figure()
        names = [target_labels[t] for t in _PRED_TARGETS]
        lr_r2s = [modelos[t]['lr_r2'] for t in _PRED_TARGETS]
        rf_r2s = [modelos[t]['rf_r2'] if modelos[t]['rf_r2'] is not None else 0
                  for t in _PRED_TARGETS]
        fig_r2.add_trace(go.Bar(name='Linear Regression', x=names, y=lr_r2s,
                                marker_color='#5c6bc0'))
        if _HAS_SKLEARN:
            fig_r2.add_trace(go.Bar(name='Random Forest', x=names, y=rf_r2s,
                                    marker_color='#26a69a'))
        fig_r2.update_layout(
            title="Comparacion de R² por Modelo",
            yaxis_title="R²", barmode='group', height=380,
            yaxis=dict(range=[0, 1.05]),
            plot_bgcolor='#fafafa',
        )
        st.plotly_chart(fig_r2, use_container_width=True)

    # ══════════ TAB 3: INFO ══════════
    with tab_info:
        st.subheader("Informacion del Modelo")

        backend = "scikit-learn (LinearRegression + RandomForestRegressor)" if _HAS_SKLEARN \
                  else "NumPy OLS (fallback — instale scikit-learn para mejores resultados)"
        st.markdown(f"""
        <div class="metric-card metric-card-blue">
            <div class="mc-label">Backend de ML</div>
            <div style="font-size:14px;font-weight:600;color:#1a237e">{backend}</div>
        </div>""", unsafe_allow_html=True)

        st.markdown("")
        k1, k2, k3 = st.columns(3)
        k1.metric("Muestras de entrenamiento", len(df_clean))
        k2.metric("Features de entrada", len(_PRED_FEATURES))
        k3.metric("Variables de salida", len(_PRED_TARGETS))

        st.markdown("---")
        st.markdown("**Variables de entrada:**")
        for feat in _PRED_FEATURES:
            r = rangos[feat] if 'rangos' in dir() else {
                'min': df_clean[feat].min(), 'max': df_clean[feat].max(),
                'mean': df_clean[feat].mean()
            }
            st.markdown(f"- **{_PRED_LABELS[feat]}**: rango [{r['min']:.2f} — {r['max']:.2f}], "
                        f"media {r['mean']:.2f}")

        st.markdown("")
        st.markdown("**Fundamento cientifico:**")
        st.markdown("""
        - **Fe₂O₃** es el principal cromoforo: >3% produce tonos rojizos oscuros (L* bajo, a* alto)
        - **TiO₂** actua como opacificante y modifica la tonalidad
        - **CaO** en atmosfera oxidante genera tonos claros (blanquea)
        - **K₂O / Na₂O** forman fases vitreas que afectan la reflectancia
        - **Temperatura** controla la vitrificacion y las transformaciones de fases de hierro
          (hematita → magnetita/hercynita a T>1100°C)
        """)

        if not _HAS_SKLEARN:
            st.warning("⚠️ scikit-learn no esta instalado. Usando regresion lineal basica con NumPy. "
                       "Instale scikit-learn para modelos RandomForest con mejor precision:\n\n"
                       "`pip install scikit-learn`")


# =====================================================
# OPTIMIZADOR DE MEZCLAS (problema inverso con scipy)
# =====================================================
from scipy.optimize import minimize as scipy_minimize

# Propiedades que el optimizador puede targetear
_OPT_PROPS = {
    'absorcion':      {'label': 'Absorcion (%)',      'tipo': 'target', 'default': 3.0},
    'fe2o3':          {'label': 'Fe₂O₃ (%)',          'tipo': 'max',    'default': 1.0},
    'al2o3':          {'label': 'Al₂O₃ (%)',          'tipo': 'min',    'default': 18.0},
    'l_color':        {'label': 'L* (Luminosidad)',    'tipo': 'min',    'default': 65.0},
    'sio2':           {'label': 'SiO₂ (%)',           'tipo': 'target', 'default': 65.0},
    'mor_cocido_mpa': {'label': 'MOR Cocido (MPa)',   'tipo': 'min',    'default': 30.0},
    'contraccion':    {'label': 'Contraccion (%)',     'tipo': 'target', 'default': 6.5},
}

# Pesos para la funcion objetivo del optimizador
_OPT_PESOS = {
    'absorcion': 30, 'fe2o3': 25, 'al2o3': 15, 'l_color': 10,
    'sio2': 5, 'mor_cocido_mpa': 10, 'contraccion': 5,
}


def _objetivo_mezcla(x, nombres_mp, df_muestras, targets, pesos_target):
    """
    Funcion objetivo para scipy: minimizar distancia ponderada al target.
    x: array de fracciones (0-1) que suman 1.
    """
    componentes = []
    for i, nombre in enumerate(nombres_mp):
        if x[i] > 0.001:
            componentes.append({'nombre': nombre, 'pct': x[i] * 100.0})
    if not componentes:
        return 1e6

    resultado, _ = estimar_propiedades_blend(componentes, df_muestras)

    costo = 0.0
    for prop, tinfo in targets.items():
        val_est = resultado.get(prop)
        if val_est is None:
            continue
        target_val = tinfo['valor']
        tipo = tinfo['tipo']
        peso = pesos_target.get(prop, 5)
        ref = abs(target_val) if target_val != 0 else 1.0

        if tipo == 'target':
            desvio = abs(val_est - target_val) / ref
        elif tipo == 'max':
            desvio = max(0, val_est - target_val) / ref
        elif tipo == 'min':
            desvio = max(0, target_val - val_est) / ref
        else:
            desvio = abs(val_est - target_val) / ref

        costo += peso * desvio ** 2

    return costo


def _optimizar_mezcla(nombres_mp, df_muestras, targets, pesos_target,
                      bounds_por_mp, max_componentes=None):
    """
    Ejecuta la optimizacion SLSQP.
    bounds_por_mp: dict nombre -> (min_frac, max_frac)  en 0-1.
    Retorna: (fracciones_opt, costo, exito)
    """
    n = len(nombres_mp)
    bounds = []
    for nombre in nombres_mp:
        lo, hi = bounds_por_mp.get(nombre, (0, 1))
        bounds.append((lo, hi))

    # Punto inicial: distribucion uniforme
    x0 = np.full(n, 1.0 / n)
    # Ajustar x0 a bounds
    for i in range(n):
        x0[i] = max(bounds[i][0], min(bounds[i][1], x0[i]))
    # Normalizar a sum=1
    s = x0.sum()
    if s > 0:
        x0 = x0 / s

    # Restriccion: sum(x) = 1
    constraints = [{'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}]

    # Si max_componentes, agregar penalizacion en la funcion objetivo
    # (soft constraint via L0-like penalty)
    def objetivo_con_penalty(x):
        base = _objetivo_mezcla(x, nombres_mp, df_muestras, targets, pesos_target)
        if max_componentes is not None:
            n_activos = np.sum(x > 0.02)
            if n_activos > max_componentes:
                base += 50 * (n_activos - max_componentes)
        return base

    result = scipy_minimize(
        objetivo_con_penalty, x0,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints,
        options={'maxiter': 500, 'ftol': 1e-9}
    )

    return result.x, result.fun, result.success


def _generar_pdf_receta(nombre_receta, componentes_opt, props_est, metodo_est,
                        targets, df_muestras):
    """Genera PDF de la receta optimizada."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm, cm
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.5*cm, bottomMargin=1.5*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = getSampleStyleSheet()
    title_s = ParagraphStyle('T', parent=styles['Title'], fontSize=18,
                             textColor=colors.HexColor('#1a237e'), spaceAfter=4)
    sub_s = ParagraphStyle('S', parent=styles['Heading2'], fontSize=13,
                           textColor=colors.HexColor('#283593'), spaceBefore=12, spaceAfter=6)
    norm_s = ParagraphStyle('N', parent=styles['Normal'], fontSize=9, leading=12)

    story = []
    story.append(Paragraph("GEOCIVMET — Receta de Mezcla Optimizada", title_s))
    story.append(Paragraph(f"Receta: {nombre_receta}", sub_s))
    story.append(Paragraph(f"Fecha: {date.today().isoformat()}", norm_s))
    story.append(Spacer(1, 8*mm))

    # Tabla de componentes
    story.append(Paragraph("Componentes de la Mezcla", sub_s))
    data_comp = [['Materia Prima', 'Porcentaje (%)']]
    for comp in componentes_opt:
        data_comp.append([comp['nombre'], f"{comp['pct']:.1f}"])
    t_comp = Table(data_comp, colWidths=[10*cm, 4*cm])
    t_comp.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a237e')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
        ('ALIGN', (1, 0), (1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    story.append(t_comp)
    story.append(Spacer(1, 8*mm))

    # Tabla de propiedades vs targets
    story.append(Paragraph("Propiedades Estimadas vs Targets", sub_s))
    data_props = [['Propiedad', 'Target', 'Tipo', 'Estimado', 'Cumple']]
    for prop, tinfo in targets.items():
        val_est = props_est.get(prop)
        est_str = f"{val_est:.2f}" if val_est is not None else "—"
        target_str = f"{tinfo['valor']:.2f}"
        tipo = tinfo['tipo'].upper()
        if val_est is not None:
            if tinfo['tipo'] == 'target':
                cumple = abs(val_est - tinfo['valor']) / max(abs(tinfo['valor']), 1) < 0.15
            elif tinfo['tipo'] == 'max':
                cumple = val_est <= tinfo['valor'] * 1.05
            else:
                cumple = val_est >= tinfo['valor'] * 0.95
            cumple_str = "SI" if cumple else "NO"
        else:
            cumple_str = "—"
        lbl = _OPT_PROPS.get(prop, {}).get('label', prop)
        data_props.append([lbl, target_str, tipo, est_str, cumple_str])

    t_props = Table(data_props, colWidths=[5*cm, 3*cm, 2*cm, 3*cm, 2*cm])
    style_rows = [
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a237e')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]
    # Color cumple/no cumple
    for i in range(1, len(data_props)):
        if data_props[i][4] == 'SI':
            style_rows.append(('TEXTCOLOR', (4, i), (4, i), colors.HexColor('#16a34a')))
        elif data_props[i][4] == 'NO':
            style_rows.append(('TEXTCOLOR', (4, i), (4, i), colors.HexColor('#dc2626')))
    t_props.setStyle(TableStyle(style_rows))
    story.append(t_props)
    story.append(Spacer(1, 10*mm))

    # Footer
    story.append(Paragraph("Generado por GEOCIVMET Lab System v4.0 — Optimizador de Mezclas",
                           ParagraphStyle('footer', parent=norm_s, fontSize=8,
                                          textColor=colors.HexColor('#94a3b8'))))

    doc.build(story)
    buf.seek(0)
    return buf


def page_optimizador_mezclas(user_info):
    """Modulo Optimizador de Mezclas: problema inverso con scipy SLSQP."""
    st.title("🧩 Optimizador de Mezclas")
    st.markdown('<div class="section-badge">🎯 Problema inverso — encontrar la mezcla optima</div>',
                unsafe_allow_html=True)
    st.caption("Defina propiedades target y el sistema calculara la mezcla optima "
               "de materias primas disponibles usando optimizacion numerica (SLSQP).")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    muestras_disponibles = df['nombre'].tolist()
    if len(muestras_disponibles) < 2:
        st.warning("Se necesitan al menos 2 muestras para optimizar una mezcla.")
        return

    # ══════════ PASO 1: SELECCIONAR MATERIAS PRIMAS ══════════
    st.subheader("1. Materias Primas Disponibles")
    st.caption("Seleccione las materias primas que desea considerar en la mezcla.")

    # Multiselect con todas las muestras
    mp_seleccionadas = st.multiselect(
        "Materias primas:",
        muestras_disponibles,
        default=muestras_disponibles[:min(6, len(muestras_disponibles))],
        key="opt_mp_sel"
    )

    if len(mp_seleccionadas) < 2:
        st.info("Seleccione al menos 2 materias primas.")
        return

    # Restricciones por componente
    with st.expander("⚙️ Restricciones por componente (min/max %)", expanded=False):
        st.caption("Defina el rango permitido de cada materia prima en la mezcla (0-100%).")
        bounds_mp = {}
        n_cols = min(3, len(mp_seleccionadas))
        cols_bounds = st.columns(n_cols)
        for i, nombre in enumerate(mp_seleccionadas):
            with cols_bounds[i % n_cols]:
                c1, c2 = st.columns(2)
                with c1:
                    lo = st.number_input(f"Min % {nombre[:15]}", 0.0, 100.0, 0.0,
                                         step=5.0, key=f"opt_lo_{i}")
                with c2:
                    hi = st.number_input(f"Max % {nombre[:15]}", 0.0, 100.0, 100.0,
                                         step=5.0, key=f"opt_hi_{i}")
                bounds_mp[nombre] = (lo / 100.0, hi / 100.0)

    max_comp = st.slider("Maximo de componentes en la mezcla:", 2, len(mp_seleccionadas),
                         min(len(mp_seleccionadas), 5), key="opt_max_comp")

    st.markdown("---")

    # ══════════ PASO 2: DEFINIR TARGETS ══════════
    st.subheader("2. Propiedades Target")
    st.caption("Active las propiedades que desea optimizar y defina su valor objetivo.")

    targets_activos = {}
    n_tgt_cols = 3
    tgt_cols = st.columns(n_tgt_cols)
    for i, (prop, info) in enumerate(_OPT_PROPS.items()):
        with tgt_cols[i % n_tgt_cols]:
            activo = st.checkbox(info['label'], value=(prop in ['absorcion', 'fe2o3', 'al2o3']),
                                 key=f"opt_chk_{prop}")
            if activo:
                col_v, col_t = st.columns([2, 1])
                with col_v:
                    val = st.number_input(f"Valor {info['label'][:12]}",
                                          value=info['default'],
                                          step=0.5, key=f"opt_val_{prop}")
                with col_t:
                    tipo_opciones = ['target', 'max', 'min']
                    tipo_default = tipo_opciones.index(info['tipo'])
                    tipo = st.selectbox("Tipo", tipo_opciones, index=tipo_default,
                                        key=f"opt_tipo_{prop}")
                targets_activos[prop] = {'valor': val, 'tipo': tipo}

    if not targets_activos:
        st.info("Active al menos una propiedad target.")
        return

    st.markdown("---")

    # ══════════ PASO 3: OPTIMIZAR ══════════
    st.subheader("3. Ejecutar Optimizacion")

    if st.button("🚀 Optimizar Mezcla", type="primary", use_container_width=True):
        with st.spinner("Optimizando... (SLSQP iterando)"):
            pesos = {}
            for prop in targets_activos:
                pesos[prop] = _OPT_PESOS.get(prop, 5)

            fracs, costo, exito = _optimizar_mezcla(
                mp_seleccionadas, df, targets_activos, pesos,
                bounds_mp, max_componentes=max_comp
            )

        # Guardar resultados en session_state
        st.session_state['opt_result'] = {
            'fracs': fracs, 'costo': costo, 'exito': exito,
            'mp_names': mp_seleccionadas, 'targets': targets_activos,
        }

    # ══════════ PASO 4: MOSTRAR RESULTADOS ══════════
    if 'opt_result' not in st.session_state:
        return

    res = st.session_state['opt_result']
    fracs = res['fracs']
    costo = res['costo']
    exito = res['exito']
    mp_names = res['mp_names']
    targets = res['targets']

    st.markdown("---")
    st.subheader("4. Resultado de la Optimizacion")

    if exito:
        st.success(f"Optimizacion convergida exitosamente (costo: {costo:.4f})")
    else:
        st.warning(f"Optimizacion finalizada con advertencias (costo: {costo:.4f}). "
                   "El resultado puede ser suboptimo.")

    # Filtrar componentes con > 0.5%
    componentes_opt = []
    for i, nombre in enumerate(mp_names):
        pct = fracs[i] * 100.0
        if pct >= 0.5:
            componentes_opt.append({'nombre': nombre, 'pct': round(pct, 1)})

    # Normalizar al 100% tras filtrar
    total = sum(c['pct'] for c in componentes_opt)
    if total > 0 and abs(total - 100.0) > 0.1:
        for c in componentes_opt:
            c['pct'] = round(c['pct'] / total * 100.0, 1)

    # Ordenar de mayor a menor
    componentes_opt.sort(key=lambda c: c['pct'], reverse=True)

    # KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("Componentes activos", len(componentes_opt))
    k2.metric("Costo (distancia)", f"{costo:.4f}")
    k3.metric("Convergencia", "Exitosa" if exito else "Parcial")

    # ── Tabla de receta ──
    st.subheader("Receta Optimizada")

    # Barras visuales
    html_receta = ""
    colores_barra = ['#1a237e', '#283593', '#3949ab', '#5c6bc0', '#7986cb',
                     '#9fa8da', '#c5cae9', '#e8eaf6']
    for idx, comp in enumerate(componentes_opt):
        color = colores_barra[idx % len(colores_barra)]
        html_receta += f"""
        <div style="display:flex;align-items:center;margin:6px 0;gap:10px">
            <div style="width:160px;font-size:13px;font-weight:600;color:#334155;
                 text-align:right;flex-shrink:0">{comp['nombre']}</div>
            <div style="flex:1;background:#e2e8f0;border-radius:8px;height:28px;
                 position:relative;overflow:hidden">
                <div style="background:{color};height:100%;width:{comp['pct']:.0f}%;
                     border-radius:8px;transition:width 0.5s ease"></div>
                <span style="position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);
                      font-size:12px;font-weight:700;color:#1e293b">{comp['pct']:.1f}%</span>
            </div>
        </div>"""
    st.markdown(f'<div style="padding:10px 0">{html_receta}</div>', unsafe_allow_html=True)

    # ── Propiedades estimadas vs targets ──
    st.subheader("Propiedades Estimadas vs Target")
    props_est, metodo_est = estimar_propiedades_blend(componentes_opt, df)

    comp_rows = []
    for prop, tinfo in targets.items():
        val_est = props_est.get(prop)
        target_val = tinfo['valor']
        tipo = tinfo['tipo']
        label = _OPT_PROPS.get(prop, {}).get('label', prop)

        if val_est is not None:
            if tipo == 'target':
                desvio_pct = abs(val_est - target_val) / max(abs(target_val), 0.01) * 100
                cumple = desvio_pct < 15
            elif tipo == 'max':
                desvio_pct = max(0, (val_est - target_val) / max(abs(target_val), 0.01) * 100)
                cumple = val_est <= target_val * 1.05
            else:
                desvio_pct = max(0, (target_val - val_est) / max(abs(target_val), 0.01) * 100)
                cumple = val_est >= target_val * 0.95
        else:
            desvio_pct = None
            cumple = None

        comp_rows.append({
            'prop': prop, 'label': label, 'target': target_val,
            'tipo': tipo, 'estimado': val_est, 'desvio_pct': desvio_pct, 'cumple': cumple,
        })

    # Barras comparativas
    html_comp = ""
    for row in comp_rows:
        est = row['estimado']
        tgt = row['target']
        if est is None:
            bar_html = '<span style="color:#94a3b8">Sin datos</span>'
        else:
            color_bar = '#16a34a' if row['cumple'] else '#dc2626'
            # Escalar la barra: max entre target y estimado
            scale_max = max(abs(tgt), abs(est)) * 1.3 if max(abs(tgt), abs(est)) > 0 else 1
            w_tgt = abs(tgt) / scale_max * 100
            w_est = abs(est) / scale_max * 100
            desvio_str = f"{row['desvio_pct']:.1f}%" if row['desvio_pct'] is not None else ""
            icono = "✅" if row['cumple'] else "❌"
            bar_html = f"""
            <div style="position:relative;height:36px">
                <div style="position:absolute;top:0;left:0;height:16px;width:{w_tgt:.0f}%;
                     background:#c5cae9;border-radius:4px" title="Target: {tgt:.2f}"></div>
                <div style="position:absolute;top:18px;left:0;height:16px;width:{w_est:.0f}%;
                     background:{color_bar};border-radius:4px;opacity:0.85"
                     title="Estimado: {est:.2f}"></div>
                <span style="position:absolute;right:0;top:0;font-size:10px;color:#64748b">
                    Target: {tgt:.2f}</span>
                <span style="position:absolute;right:0;top:18px;font-size:10px;color:{color_bar};
                      font-weight:700">
                    {est:.2f} {icono} {desvio_str}</span>
            </div>"""

        html_comp += f"""
        <div style="display:flex;align-items:flex-start;margin:10px 0;gap:12px">
            <div style="width:140px;font-size:12px;font-weight:600;color:#334155;
                 flex-shrink:0;padding-top:2px">{row['label']}
                <br><span style="font-size:10px;color:#94a3b8;font-weight:400">
                    ({row['tipo'].upper()})</span></div>
            <div style="flex:1">{bar_html}</div>
        </div>"""

    st.markdown(f'<div style="padding:8px 0">{html_comp}</div>', unsafe_allow_html=True)

    # ── Pie chart de composicion ──
    fig_pie = px.pie(
        names=[c['nombre'] for c in componentes_opt],
        values=[c['pct'] for c in componentes_opt],
        title="Composicion de la Mezcla Optimizada",
        color_discrete_sequence=px.colors.sequential.Purples_r,
    )
    fig_pie.update_layout(height=400)
    st.plotly_chart(fig_pie, use_container_width=True)

    st.markdown("---")

    # ══════════ ACCIONES: GUARDAR + PDF ══════════
    st.subheader("5. Guardar / Exportar")

    col_name, _ = st.columns([2, 1])
    with col_name:
        nombre_receta = st.text_input("Nombre de la mezcla:", value="Mezcla Optimizada",
                                      key="opt_nombre_receta")

    col_save, col_pdf = st.columns(2)

    with col_save:
        if st.button("💾 Guardar como Blend", type="primary", use_container_width=True):
            if not nombre_receta.strip():
                st.warning("Ingrese un nombre para la mezcla.")
            else:
                conn = get_conn()
                c = conn.cursor()
                desc = ", ".join([f"{comp['nombre']} ({comp['pct']:.1f}%)"
                                  for comp in componentes_opt])
                tgt_desc = ", ".join([f"{_OPT_PROPS.get(p,{}).get('label',p)}={t['valor']}"
                                      for p, t in targets.items()])
                c.execute("""INSERT INTO blends (nombre, descripcion, creado_por, fecha, objetivo_uso)
                             VALUES (?, ?, ?, ?, ?)""",
                          (nombre_receta.strip(),
                           f"Optimizado: {desc}",
                           user_info.get('nombre', 'sistema'),
                           date.today().isoformat(),
                           f"Target: {tgt_desc}"))
                blend_id = c.lastrowid
                for comp in componentes_opt:
                    match = df[df['nombre'] == comp['nombre']]
                    if not match.empty:
                        mid = int(match.iloc[0]['id'])
                        c.execute("INSERT INTO blend_componentes (blend_id, muestra_id, porcentaje) "
                                  "VALUES (?, ?, ?)", (blend_id, mid, comp['pct']))
                conn.commit()
                conn.close()
                st.success(f"Mezcla **{nombre_receta}** guardada como Blend #{blend_id}")
                st.balloons()

    with col_pdf:
        try:
            pdf_buf = _generar_pdf_receta(
                nombre_receta or "Mezcla Optimizada",
                componentes_opt, props_est, metodo_est, targets, df
            )
            st.download_button(
                "📥 Descargar Receta (PDF)",
                data=pdf_buf,
                file_name=f"receta_{nombre_receta.replace(' ', '_')}.pdf",
                mime="application/pdf",
                use_container_width=True,
            )
        except Exception as e:
            st.error(f"Error generando PDF: {e}")


# =====================================================
# VALIDACION DE INGENIERO (servicio profesional)
# =====================================================
TARIFAS_VALIDACION = {
    'basica': {
        'nombre': 'Revision Basica',
        'descripcion': 'Revision de resultados quimicos y fisicos de hasta 3 muestras. '
                       'Incluye dictamen de aptitud general.',
        'precio': 50.0, 'moneda': 'USD', 'tiempo': '48 horas',
    },
    'completa': {
        'nombre': 'Validacion Completa',
        'descripcion': 'Analisis detallado de hasta 10 muestras con recomendacion de uso, '
                       'formulacion de mezcla sugerida y reporte firmado.',
        'precio': 120.0, 'moneda': 'USD', 'tiempo': '5 dias habiles',
    },
    'premium': {
        'nombre': 'Consultoria Premium',
        'descripcion': 'Consultoria completa incluyendo: validacion de resultados, '
                       'recomendacion de producto ceramico, formulacion optimizada, '
                       'reunion virtual de 30 min con ingeniero ceramista.',
        'precio': 250.0, 'moneda': 'USD', 'tiempo': '7 dias habiles',
    },
}


def _generar_pdf_recomendacion(solicitud, muestras_info):
    """Genera PDF de recomendacion tecnica firmada por GEOCIVMET."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm, cm
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=2*cm, bottomMargin=2*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    styles = getSampleStyleSheet()

    title_s = ParagraphStyle('GCM_Title', parent=styles['Title'], fontSize=20,
                             textColor=colors.HexColor('#1a237e'), spaceAfter=4,
                             fontName='Helvetica-Bold')
    sub_s = ParagraphStyle('GCM_Sub', parent=styles['Heading2'], fontSize=13,
                           textColor=colors.HexColor('#283593'), spaceBefore=14, spaceAfter=6,
                           fontName='Helvetica-Bold')
    body_s = ParagraphStyle('GCM_Body', parent=styles['Normal'], fontSize=10, leading=14,
                            alignment=TA_JUSTIFY)
    small_s = ParagraphStyle('GCM_Small', parent=styles['Normal'], fontSize=8,
                             textColor=colors.HexColor('#64748b'), leading=10)
    center_s = ParagraphStyle('GCM_Center', parent=styles['Normal'], fontSize=10,
                              alignment=TA_CENTER)

    story = []

    # Header
    story.append(Paragraph("GEOCIVMET — Consultores Tecnicos", title_s))
    story.append(Paragraph("Geologia · Ingenieria Civil · Mineria · Metalurgia · Tecnologia",
                           small_s))
    story.append(Spacer(1, 4*mm))

    # Linea decorativa
    line_data = [['', '']]
    line_t = Table(line_data, colWidths=[doc.width, 0])
    line_t.setStyle(TableStyle([
        ('LINEBELOW', (0, 0), (0, 0), 3, colors.HexColor('#1a237e')),
    ]))
    story.append(line_t)
    story.append(Spacer(1, 6*mm))

    # Tipo de documento
    story.append(Paragraph("RECOMENDACION TECNICA PROFESIONAL", sub_s))
    num_ref = f"VAL-{solicitud['id']:05d}-{datetime.now().strftime('%Y%m%d')}"
    story.append(Paragraph(f"<b>Referencia:</b> {num_ref}", body_s))
    story.append(Paragraph(f"<b>Fecha:</b> {datetime.now().strftime('%d/%m/%Y')}", body_s))
    story.append(Paragraph(f"<b>Solicitante:</b> {solicitud['solicitante_nombre']}", body_s))
    story.append(Paragraph(f"<b>Tipo de servicio:</b> {solicitud.get('tarifa_nombre', 'Validacion')}", body_s))
    story.append(Spacer(1, 6*mm))

    # Contexto
    story.append(Paragraph("1. CONTEXTO DE LA SOLICITUD", sub_s))
    story.append(Paragraph(solicitud.get('descripcion_duda', '—'), body_s))
    if solicitud.get('contexto_uso'):
        story.append(Spacer(1, 2*mm))
        story.append(Paragraph(f"<b>Uso previsto:</b> {solicitud['contexto_uso']}", body_s))
    story.append(Spacer(1, 4*mm))

    # Muestras evaluadas
    if muestras_info:
        story.append(Paragraph("2. MUESTRAS EVALUADAS", sub_s))
        m_data = [['Muestra', 'Yacimiento', 'Fe₂O₃ (%)', 'Al₂O₃ (%)', 'AA (%)', 'L*']]
        for m in muestras_info:
            m_data.append([
                str(m.get('nombre', '—')),
                str(m.get('yacimiento', '—')),
                f"{m['fe2o3']:.3f}" if pd.notna(m.get('fe2o3')) else '—',
                f"{m['al2o3']:.2f}" if pd.notna(m.get('al2o3')) else '—',
                f"{m['absorcion']:.2f}" if pd.notna(m.get('absorcion')) else '—',
                f"{m['l_color']:.1f}" if pd.notna(m.get('l_color')) else '—',
            ])
        t_m = Table(m_data, colWidths=[4*cm, 3*cm, 2*cm, 2*cm, 2*cm, 1.5*cm])
        t_m.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a237e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
            ('ALIGN', (2, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(t_m)
        story.append(Spacer(1, 6*mm))

    # Dictamen
    story.append(Paragraph("3. DICTAMEN TECNICO", sub_s))
    dictamen = solicitud.get('dictamen', 'Pendiente de evaluacion.')
    story.append(Paragraph(dictamen, body_s))
    story.append(Spacer(1, 6*mm))

    # Recomendación
    story.append(Paragraph("4. RECOMENDACION PROFESIONAL", sub_s))
    recomendacion = solicitud.get('recomendacion_tecnica', 'Pendiente.')
    story.append(Paragraph(recomendacion, body_s))
    story.append(Spacer(1, 10*mm))

    # Disclaimer
    story.append(Paragraph(
        "<i>Esta recomendacion esta basada en los datos suministrados y los ensayos registrados "
        "en el sistema GEOCIVMET Lab System. Los resultados son orientativos y deben ser "
        "validados con ensayos de produccion a escala piloto antes de su implementacion industrial.</i>",
        small_s))
    story.append(Spacer(1, 12*mm))

    # Firmas
    ing = solicitud.get('ingeniero_asignado', 'Ing. GEOCIVMET')
    firma_data = [
        ['', ''],
        ['_' * 35, '_' * 35],
        [Paragraph(f"<b>{ing}</b><br/>Ingeniero Ceramista<br/>GEOCIVMET Consultores Tecnicos", small_s),
         Paragraph("<b>Director Tecnico</b><br/>GEOCIVMET Consultores Tecnicos", small_s)],
    ]
    t_firma = Table(firma_data, colWidths=[doc.width / 2 - 5*mm] * 2)
    t_firma.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
    ]))
    story.append(t_firma)
    story.append(Spacer(1, 8*mm))

    # Footer
    story.append(Paragraph(
        f"Documento generado por GEOCIVMET Lab System v4.0 | Ref: {num_ref} | "
        f"Este documento tiene validez como recomendacion tecnica profesional.",
        ParagraphStyle('footer', parent=small_s, fontSize=7,
                       textColor=colors.HexColor('#94a3b8'))
    ))

    doc.build(story)
    buf.seek(0)
    return buf, num_ref


def page_validacion_ingeniero(user_info):
    """Pagina para solicitar validacion tecnica profesional de GEOCIVMET."""
    st.title("👨‍🔬 Validacion de Ingeniero")
    st.markdown('<div class="section-badge">🏗️ Servicio profesional de GEOCIVMET Consultores Tecnicos</div>',
                unsafe_allow_html=True)

    st.markdown("""
    <div class="metric-card metric-card-blue" style="margin-bottom:20px">
        <div class="mc-label">Servicio Profesional</div>
        <div style="font-size:13px;color:#334155;line-height:1.6">
            ¿Tiene dudas sobre la interpretacion de sus resultados? Nuestro equipo de
            <b>ingenieros ceramistas</b> de GEOCIVMET revisara sus datos y emitira una
            <b>recomendacion tecnica firmada</b> con dictamen de aptitud, sugerencias de
            formulacion y uso recomendado para su materia prima.<br><br>
            <b>Paso 1:</b> Solicite el servicio por email o mensaje.<br>
            <b>Paso 2:</b> Recibira un <b>codigo de acceso</b> para ver planes y tarifas.<br>
            <b>Paso 3:</b> Seleccione su plan y envie la solicitud.
        </div>
    </div>""", unsafe_allow_html=True)

    df = obtener_datos_completos()

    tab_solicitar, tab_acceso, tab_historial = st.tabs([
        "📧 Solicitar Servicio", "🔑 Tengo Codigo de Acceso", "📋 Mis Solicitudes"])

    # ══════════ TAB 1: SOLICITAR SERVICIO (sin ver planes) ══════════
    with tab_solicitar:
        st.subheader("Solicitar Informacion del Servicio")
        st.markdown("""
        <div style="background:linear-gradient(135deg,#eff6ff,#e0f2fe);border:1px solid #93c5fd;
             border-radius:14px;padding:24px;margin:10px 0">
            <div style="font-weight:700;color:#1e40af;font-size:15px;margin-bottom:12px">
                📩 ¿Interesado en validacion profesional?</div>
            <div style="font-size:13px;color:#1e3a5f;line-height:1.8">
                Envie un mensaje o email solicitando el servicio de validacion de ingeniero.
                Nuestro equipo le respondera con un <b>codigo de acceso exclusivo</b>
                para que pueda ver los planes, tarifas y realizar su solicitud formal.<br><br>
                <b>Contacto:</b>
            </div>
        </div>""", unsafe_allow_html=True)

        user_name = user_info.get('nombre', '') if user_info else ''
        contact_html = (
            f'<div style="text-align:center;margin:16px 0">'
            f'<a href="mailto:geocivmetven@gmail.com?subject=Solicitud%20de%20Validacion%20de%20Ingeniero'
            f'&body=Estimado%20equipo%20GEOCIVMET%2C%0A%0ASolicito%20informacion%20sobre%20el%20servicio'
            f'%20de%20validacion%20de%20ingeniero.%0A%0ANombre%3A%20{user_name}'
            f'%0A%0AGracias."'
            f' style="background:#1a237e;color:white;padding:12px 28px;border-radius:8px;'
            f'text-decoration:none;font-weight:700;font-size:14px;display:inline-block">'
            f'📧 geocivmetven@gmail.com</a></div>'
            f'<div style="font-size:11px;color:#64748b;text-align:center;margin-top:8px">'
            f'Tiempo de respuesta: 24-48 horas habiles</div>'
        )
        st.markdown(contact_html, unsafe_allow_html=True)

        st.markdown("---")
        st.subheader("Solicitud Rapida")
        sol_desc = st.text_area(
            "Describa brevemente su necesidad:",
            placeholder="Ej: Necesito validacion para 3 arcillas de Tachira para uso en porcelanato...",
            height=100, key="val_solicitud_rapida"
        )
        if st.button("📩 Enviar Solicitud de Informacion", type="primary", use_container_width=True):
            if not sol_desc.strip():
                st.error("Describa brevemente su necesidad.")
            else:
                # Notificacion al admin
                if 'notificaciones_admin' not in st.session_state:
                    st.session_state['notificaciones_admin'] = []
                st.session_state['notificaciones_admin'].append({
                    'tipo': 'solicitud_info_validacion',
                    'usuario': user_info['nombre'],
                    'fecha': datetime.now().strftime('%Y-%m-%d %H:%M'),
                    'mensaje': f"Solicitud de informacion de validacion de {user_info['nombre']}: "
                               f"{sol_desc.strip()[:200]}"
                })
                # Guardar en BD
                conn = get_conn()
                c = conn.cursor()
                c.execute("""INSERT INTO validaciones_ingeniero
                    (solicitante_id, solicitante_nombre, muestras_ids, modulo_origen,
                     descripcion_duda, contexto_uso, tarifa, estado, prioridad)
                    VALUES (?, ?, '', 'General', ?, '', 0, 'info_solicitada', 'normal')""",
                    (user_info['id'], user_info['nombre'], sol_desc.strip()))
                conn.commit()
                conn.close()
                st.success("✅ Solicitud enviada. Recibira un codigo de acceso por email "
                           "para ver planes y tarifas. Contacto: geocivmetven@gmail.com")

    # ══════════ TAB 2: ACCESO CON CODIGO ══════════
    with tab_acceso:
        st.subheader("🔑 Ingrese su Codigo de Acceso")
        st.caption("Si ya recibio un codigo por email, ingreselo aqui para ver los planes y tarifas.")

        codigo_input = st.text_input("Codigo de acceso:", key="val_codigo_acceso",
                                      placeholder="Ej: GCM-VAL-A1B2C3")

        # Verificar codigo o si el admin ya lo tiene en session
        codigo_valido = False
        if 'validacion_codigo_autorizado' in st.session_state and st.session_state['validacion_codigo_autorizado']:
            codigo_valido = True

        if codigo_input.strip() and not codigo_valido:
            conn = get_conn()
            c = conn.cursor()
            c.execute("SELECT id, usado FROM codigos_validacion WHERE codigo = ?",
                      (codigo_input.strip().upper(),))
            row = c.fetchone()
            if row and row[1] == 0:
                # Marcar como usado
                c.execute("UPDATE codigos_validacion SET usado=1, fecha_uso=?, usuario_id=? WHERE id=?",
                          (datetime.now().isoformat(), user_info['id'], row[0]))
                conn.commit()
                st.session_state['validacion_codigo_autorizado'] = True
                codigo_valido = True
                st.success("✅ Codigo valido. Ya puede ver los planes y solicitar el servicio.")
                st.rerun()
            elif row and row[1] == 1:
                st.warning("⚠️ Este codigo ya fue utilizado.")
            else:
                st.error("❌ Codigo invalido. Verifique e intente de nuevo.")
            conn.close()

        # Admin siempre tiene acceso
        if user_info and user_info.get('rol') == 'admin':
            codigo_valido = True

        if not codigo_valido:
            st.info("Ingrese un codigo valido para acceder a los planes de servicio.")
            return

        # ── MOSTRAR PLANES (solo con codigo valido) ──
        st.markdown("---")
        st.subheader("Seleccione un Plan de Servicio")

        plan_cols = st.columns(3)
        for i, (plan_key, plan_info) in enumerate(TARIFAS_VALIDACION.items()):
            with plan_cols[i]:
                is_premium = plan_key == 'premium'
                border_color = '#1a237e' if not is_premium else '#d97706'
                badge = ''
                if is_premium:
                    badge = '<span style="background:#d97706;color:white;font-size:9px;padding:2px 8px;border-radius:10px;font-weight:700">RECOMENDADO</span>'
                card_html = (
                    f'<div style="background:white;border-radius:14px;padding:20px;text-align:center;'
                    f'border:2px solid {border_color};min-height:260px;'
                    f'box-shadow:0 2px 10px rgba(0,0,0,0.06)">'
                    f'{badge}'
                    f'<div style="font-size:16px;font-weight:800;color:#1a237e;margin:10px 0 4px">'
                    f'{plan_info["nombre"]}</div>'
                    f'<div style="font-size:32px;font-weight:900;color:#0d1b2a">'
                    f'${plan_info["precio"]:.0f}'
                    f'<span style="font-size:13px;font-weight:400;color:#64748b"> USD</span></div>'
                    f'<div style="font-size:11px;color:#64748b;margin:6px 0 12px">'
                    f'Tiempo: {plan_info["tiempo"]}</div>'
                    f'<div style="font-size:12px;color:#334155;text-align:left;line-height:1.5">'
                    f'{plan_info["descripcion"]}</div>'
                    f'</div>'
                )
                st.markdown(card_html, unsafe_allow_html=True)

        st.markdown("")
        plan_seleccionado = st.radio(
            "Seleccione plan:",
            list(TARIFAS_VALIDACION.keys()),
            format_func=lambda k: f"{TARIFAS_VALIDACION[k]['nombre']} — ${TARIFAS_VALIDACION[k]['precio']:.0f} USD",
            horizontal=True, key="val_plan_sel"
        )
        tarifa = TARIFAS_VALIDACION[plan_seleccionado]

        st.markdown("---")
        st.subheader("Detalle de la Solicitud")

        # Seleccion de muestras
        if not df.empty:
            muestras_sel = st.multiselect(
                "Muestras a evaluar:",
                df['nombre'].tolist(),
                default=df['nombre'].tolist()[:3] if len(df) >= 3 else df['nombre'].tolist(),
                key="val_muestras"
            )
        else:
            muestras_sel = []
            st.info("No hay muestras en la base de datos.")

        modulo_origen = st.selectbox("Modulo de origen de la duda:", [
            "General", "Control de Calidad", "Ranking de Aptitud", "Prediccion de Color",
            "Curvas de Gresificacion", "Formula Seger / UMF", "Optimizador de Mezclas",
            "Otro"
        ], key="val_modulo")

        descripcion = st.text_area(
            "Describa su duda o lo que necesita validar:",
            placeholder="Ej: Tengo 3 arcillas del estado Tachira y necesito saber cual es mas "
                        "apta para porcelanato. Los resultados de Fe2O3 me parecen altos y "
                        "quisiera una opinion experta sobre si son viables...",
            height=120, key="val_desc"
        )

        contexto_uso = st.text_input(
            "Uso previsto del material:",
            placeholder="Ej: Pasta para porcelanato, esmalte, ladrillo refractario...",
            key="val_contexto"
        )

        prioridad = st.radio("Prioridad:", ["normal", "urgente (+30%)"],
                             horizontal=True, key="val_prioridad")
        precio_final = tarifa['precio']
        if 'urgente' in prioridad:
            precio_final *= 1.30

        st.markdown("---")

        # Resumen y pago
        st.subheader("Resumen de la Solicitud")
        r1, r2, r3 = st.columns(3)
        r1.metric("Plan", tarifa['nombre'])
        r2.metric("Muestras", len(muestras_sel))
        r3.metric("Total", f"${precio_final:.2f} USD")

        st.markdown("")
        st.markdown("""
        <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:12px;padding:16px;
             margin-bottom:16px">
            <div style="font-weight:700;color:#1e40af;font-size:13px;margin-bottom:6px">
                💳 Metodos de Pago Aceptados</div>
            <div style="font-size:12px;color:#1e3a5f;line-height:1.7">
                • <b>Transferencia bancaria</b> — Datos enviados tras confirmar solicitud<br>
                • <b>Pago movil</b> — Venezuela (Banesco, Mercantil, Provincial)<br>
                • <b>PayPal / Zelle</b> — geocivmet@gmail.com<br>
                • <b>Binance Pay / USDT</b> — ID disponible tras confirmar
            </div>
        </div>""", unsafe_allow_html=True)

        referencia_pago = st.text_input(
            "Referencia de pago (si ya realizo el pago):",
            placeholder="Ej: Transferencia #00123456, Pago Movil ref. 789...",
            key="val_ref_pago"
        )

        # Boton de envio
        if st.button("📩 Enviar Solicitud de Validacion", type="primary", use_container_width=True):
            if not descripcion.strip():
                st.error("Describa su duda o necesidad de validacion.")
            elif not muestras_sel:
                st.error("Seleccione al menos una muestra.")
            else:
                # Obtener IDs de las muestras
                muestras_ids_str = ','.join(
                    str(int(df[df['nombre'] == n].iloc[0]['id']))
                    for n in muestras_sel if not df[df['nombre'] == n].empty
                )
                conn = get_conn()
                c = conn.cursor()
                c.execute("""INSERT INTO validaciones_ingeniero
                    (solicitante_id, solicitante_nombre, muestras_ids, modulo_origen,
                     descripcion_duda, contexto_uso, tarifa, metodo_pago, referencia_pago,
                     estado, prioridad)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (user_info['id'], user_info['nombre'], muestras_ids_str,
                     modulo_origen, descripcion.strip(), contexto_uso.strip(),
                     precio_final,
                     'con_referencia' if referencia_pago.strip() else 'pendiente',
                     referencia_pago.strip() or '',
                     'nueva',
                     'urgente' if 'urgente' in prioridad else 'normal'))
                conn.commit()
                conn.close()

                # Notificacion automática al admin
                if 'notificaciones_admin' not in st.session_state:
                    st.session_state['notificaciones_admin'] = []
                st.session_state['notificaciones_admin'].append({
                    'tipo': 'validacion_ingeniero',
                    'usuario': user_info['nombre'],
                    'plan': tarifa['nombre'],
                    'monto': f"${precio_final:.2f}",
                    'muestras': len(muestras_sel),
                    'fecha': datetime.now().strftime('%Y-%m-%d %H:%M'),
                    'mensaje': f"Nueva solicitud de validacion de {user_info['nombre']} "
                               f"({tarifa['nombre']} - ${precio_final:.2f} USD) "
                               f"con {len(muestras_sel)} muestra(s)."
                })

                st.success("✅ Solicitud enviada exitosamente. El equipo de GEOCIVMET "
                           "revisara su solicitud y le contactara pronto.")
                st.balloons()

        # ── Seccion de Contacto Directo ──
        st.markdown("---")
        st.subheader("📧 Contactar Directamente")
        st.markdown("""
        <div style="background:linear-gradient(135deg,#eff6ff,#e0f2fe);border:1px solid #93c5fd;
             border-radius:14px;padding:20px;margin:10px 0">
            <div style="font-weight:700;color:#1e40af;font-size:15px;margin-bottom:8px">
                ✉️ Contacto para Solicitud de Validacion</div>
            <div style="font-size:13px;color:#1e3a5f;line-height:1.7">
                Si prefiere contactarnos directamente o tiene preguntas antes de solicitar
                el servicio, escribanos a:<br><br>
                <div style="text-align:center;margin:10px 0">
                    <a href="mailto:geocivmetven@gmail.com?subject=Solicitud%20de%20Validacion%20de%20Ingeniero&body=Estimado%20equipo%20GEOCIVMET%2C%0A%0ASolicito%20validacion%20tecnica%20para%20mis%20muestras.%0A%0ANombre%3A%20""" + str(user_info['nombre']) + """%0APlan%20de%20interes%3A%20%0ADescripcion%3A%20%0A%0AGracias."
                       style="background:#1a237e;color:white;padding:10px 24px;border-radius:8px;
                              text-decoration:none;font-weight:700;font-size:14px;
                              display:inline-block">
                        📧 geocivmetven@gmail.com
                    </a>
                </div>
                <div style="font-size:11px;color:#64748b;text-align:center;margin-top:8px">
                    Tiempo de respuesta: 24-48 horas habiles
                </div>
            </div>
        </div>""", unsafe_allow_html=True)

    # ══════════ TAB 2: HISTORIAL ══════════
    with tab_historial:
        st.subheader("Mis Solicitudes de Validacion")
        conn = get_conn()
        df_val = pd.read_sql(
            """SELECT id, fecha_solicitud, modulo_origen, estado, prioridad, tarifa,
                      dictamen, recomendacion_tecnica, ingeniero_asignado, fecha_respuesta,
                      descripcion_duda, contexto_uso, muestras_ids
               FROM validaciones_ingeniero
               WHERE solicitante_id = ?
               ORDER BY id DESC""",
            conn, params=(user_info['id'],))
        conn.close()

        if df_val.empty:
            st.info("No tiene solicitudes de validacion anteriores.")
        else:
            for _, vrow in df_val.iterrows():
                estado = vrow['estado']
                estado_color = {
                    'nueva': '#3b82f6', 'en_revision': '#d97706',
                    'pago_pendiente': '#ef4444', 'completada': '#16a34a',
                    'rechazada': '#6b7280',
                }.get(estado, '#64748b')
                estado_icon = {
                    'nueva': '🔵', 'en_revision': '🟡',
                    'pago_pendiente': '🔴', 'completada': '🟢',
                    'rechazada': '⚫',
                }.get(estado, '⚪')

                with st.expander(
                    f"{estado_icon} VAL-{vrow['id']:05d} | {vrow['modulo_origen']} | "
                    f"${vrow['tarifa']:.0f} | {estado.upper().replace('_',' ')}",
                    expanded=(estado == 'completada')
                ):
                    st.caption(f"Fecha: {vrow['fecha_solicitud']} | Prioridad: {vrow['prioridad']}")
                    st.markdown(f"**Duda:** {vrow['descripcion_duda']}")
                    if vrow['contexto_uso']:
                        st.markdown(f"**Uso previsto:** {vrow['contexto_uso']}")

                    if estado == 'completada' and vrow['dictamen']:
                        st.markdown("---")
                        st.markdown(f"**Ingeniero:** {vrow['ingeniero_asignado'] or '—'}")
                        st.markdown(f"**Dictamen:** {vrow['dictamen']}")
                        st.markdown(f"**Recomendacion:** {vrow['recomendacion_tecnica']}")
                        st.caption(f"Respondido: {vrow['fecha_respuesta']}")

                        # Boton para descargar PDF
                        sol_dict = vrow.to_dict()
                        sol_dict['tarifa_nombre'] = 'Validacion GEOCIVMET'
                        # Obtener info de muestras
                        m_info = []
                        if vrow['muestras_ids'] and not df.empty:
                            for mid_str in str(vrow['muestras_ids']).split(','):
                                try:
                                    mid = int(mid_str.strip())
                                    mrow = df[df['id'] == mid]
                                    if not mrow.empty:
                                        m_info.append(mrow.iloc[0].to_dict())
                                except (ValueError, IndexError):
                                    pass
                        try:
                            pdf_buf, num_ref = _generar_pdf_recomendacion(sol_dict, m_info)
                            st.download_button(
                                f"📥 Descargar Recomendacion Firmada ({num_ref})",
                                data=pdf_buf,
                                file_name=f"Recomendacion_{num_ref}.pdf",
                                mime="application/pdf",
                                use_container_width=True
                            )
                        except Exception as e:
                            st.error(f"Error generando PDF: {e}")

                    elif estado == 'pago_pendiente':
                        st.warning("Su pago esta pendiente de verificacion. "
                                   "Envie la referencia de pago a geocivmet@gmail.com")

                    elif estado == 'en_revision':
                        st.info("Su solicitud esta siendo revisada por nuestro equipo.")


def page_admin_validaciones():
    """Panel admin para gestionar solicitudes de validacion."""

    # ── Generador de codigos de acceso ──
    st.subheader("🔑 Generar Codigo de Acceso")
    gc1, gc2 = st.columns([2, 1])
    with gc1:
        st.caption("Genere un codigo para que un cliente pueda ver los planes y tarifas de validacion.")
    with gc2:
        if st.button("🔑 Generar Codigo", type="primary", key="btn_gen_code"):
            import string
            code = f"GCM-VAL-{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"
            conn = get_conn()
            c = conn.cursor()
            c.execute("INSERT INTO codigos_validacion (codigo, creado_por) VALUES (?, 'admin')", (code,))
            conn.commit()
            conn.close()
            st.success(f"Codigo generado: **{code}**")
            st.code(code, language=None)

    # Mostrar codigos existentes
    conn = get_conn()
    df_codes = pd.read_sql("SELECT * FROM codigos_validacion ORDER BY id DESC LIMIT 20", conn)
    conn.close()
    if not df_codes.empty:
        with st.expander("Codigos generados recientes", expanded=False):
            for _, cr in df_codes.iterrows():
                estado_code = "✅ Usado" if cr['usado'] else "🟢 Disponible"
                st.text(f"{cr['codigo']} | {estado_code} | Creado: {cr['fecha_creacion']}")
    st.markdown("---")

    st.subheader("📋 Solicitudes de Validacion de Ingeniero")

    conn = get_conn()
    df_val = pd.read_sql(
        """SELECT v.*, u.username
           FROM validaciones_ingeniero v
           LEFT JOIN usuarios u ON v.solicitante_id = u.id
           ORDER BY
               CASE v.estado
                   WHEN 'nueva' THEN 1 WHEN 'en_revision' THEN 2
                   WHEN 'pago_pendiente' THEN 3 ELSE 4
               END,
               v.prioridad DESC, v.id DESC""", conn)
    conn.close()

    if df_val.empty:
        st.success("No hay solicitudes de validacion.")
        return

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total", len(df_val))
    k2.metric("Nuevas", len(df_val[df_val['estado'] == 'nueva']))
    k3.metric("En Revision", len(df_val[df_val['estado'] == 'en_revision']))
    ingresos = df_val[df_val['estado'] == 'completada']['tarifa'].sum()
    k4.metric("Ingresos", f"${ingresos:.0f}")

    st.markdown("---")

    df_muestras = obtener_datos_completos()

    for _, vrow in df_val.iterrows():
        vid = int(vrow['id'])
        estado = vrow['estado']
        badge_color = {
            'nueva': '#3b82f6', 'en_revision': '#d97706',
            'pago_pendiente': '#ef4444', 'completada': '#16a34a',
        }.get(estado, '#6b7280')

        with st.expander(
            f"VAL-{vid:05d} | {vrow['solicitante_nombre']} | "
            f"${vrow['tarifa']:.0f} | {estado.upper().replace('_',' ')} | "
            f"{vrow['prioridad']}",
            expanded=(estado in ('nueva', 'en_revision'))
        ):
            c1, c2 = st.columns([3, 1])
            with c1:
                st.markdown(f"**Usuario:** {vrow.get('username', '—')} ({vrow['solicitante_nombre']})")
                st.markdown(f"**Modulo:** {vrow['modulo_origen']} | **Fecha:** {vrow['fecha_solicitud']}")
                st.markdown(f"**Duda:** {vrow['descripcion_duda']}")
                if vrow['contexto_uso']:
                    st.markdown(f"**Uso previsto:** {vrow['contexto_uso']}")
                if vrow['referencia_pago']:
                    st.markdown(f"**Ref. pago:** `{vrow['referencia_pago']}`")

                # Mostrar muestras involucradas
                if vrow['muestras_ids'] and not df_muestras.empty:
                    mids = [int(x.strip()) for x in str(vrow['muestras_ids']).split(',') if x.strip()]
                    df_m_sel = df_muestras[df_muestras['id'].isin(mids)]
                    if not df_m_sel.empty:
                        st.dataframe(
                            df_m_sel[['nombre', 'yacimiento', 'fe2o3', 'al2o3', 'absorcion', 'l_color']],
                            use_container_width=True, hide_index=True)

            with c2:
                st.markdown(f"""
                <div style="text-align:center;padding:10px;background:{badge_color}15;
                     border-radius:10px;border:1px solid {badge_color}40">
                    <div style="font-size:24px;font-weight:900;color:{badge_color}">
                        ${vrow['tarifa']:.0f}</div>
                    <div style="font-size:10px;color:{badge_color};text-transform:uppercase;
                         font-weight:700">{estado.replace('_',' ')}</div>
                </div>""", unsafe_allow_html=True)

            # Controles admin
            if estado != 'completada':
                st.markdown("---")
                ac1, ac2 = st.columns(2)
                with ac1:
                    nuevo_estado = st.selectbox(
                        "Cambiar estado:", ['nueva', 'pago_pendiente', 'en_revision', 'completada'],
                        index=['nueva', 'pago_pendiente', 'en_revision', 'completada'].index(estado)
                              if estado in ['nueva', 'pago_pendiente', 'en_revision', 'completada'] else 0,
                        key=f"vest_{vid}")
                with ac2:
                    ingeniero = st.text_input("Ingeniero asignado:",
                                              value=vrow['ingeniero_asignado'] or '',
                                              key=f"ving_{vid}")

                dictamen_txt = st.text_area("Dictamen tecnico:",
                                            value=vrow['dictamen'] or '',
                                            height=80, key=f"vdict_{vid}")
                recomendacion_txt = st.text_area("Recomendacion profesional:",
                                                 value=vrow['recomendacion_tecnica'] or '',
                                                 height=100, key=f"vrec_{vid}")

                if st.button("💾 Guardar Respuesta", key=f"vsave_{vid}", type="primary"):
                    conn = get_conn()
                    fecha_resp = datetime.now().isoformat() if nuevo_estado == 'completada' else None
                    conn.execute("""UPDATE validaciones_ingeniero
                                    SET estado=?, ingeniero_asignado=?, dictamen=?,
                                        recomendacion_tecnica=?, fecha_respuesta=?
                                    WHERE id=?""",
                                 (nuevo_estado, ingeniero.strip(), dictamen_txt.strip(),
                                  recomendacion_txt.strip(), fecha_resp, vid))
                    conn.commit()
                    conn.close()
                    st.success(f"VAL-{vid:05d} actualizada.")
                    st.rerun()


def render_portada():
    """Portada principal de GEOCIVMET Consultores Técnicos con logo real."""
    st.markdown("")  # spacer

    # Logo centrado con columnas simétricas
    logo_path = os.path.join(BASE_DIR, 'logo_geocivmet.png')
    if os.path.exists(logo_path):
        col_left, col_center, col_right = st.columns([1, 1, 1])
        with col_center:
            st.image(logo_path, width=280, use_container_width=False)

    # Subtítulo y tagline
    st.markdown("""
    <div class="portada-hero">
        <h3>CONSULTORES TECNICOS</h3>
        <p class="portada-tagline">
            Geologia &bull; Ingenieria Civil &bull; Mineria &bull; Metalurgia &bull; Tecnologia
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="gcm-section-divider"></div>', unsafe_allow_html=True)

    # Botones interactivos que cambian la sección al hacer clic
    s1, s2, s3, s4, s5 = st.columns(5)
    services = [
        (s1, "🔬", "Analisis\nQuimico", "Dashboard General"),
        (s2, "⚙️", "Control de\nCalidad", "Control de Calidad"),
        (s3, "📈", "Analiticas\nAvanzadas", "Analíticas Detalladas"),
        (s4, "🏆", "Ranking\nAptitud", "Ranking de Aptitud"),
        (s5, "📄", "Fichas\nTecnicas", "Ficha Técnica"),
    ]
    for col, icon, title, target in services:
        with col:
            if st.button(f"{icon}\n{title}", key=f"btn_portada_{target}",
                         use_container_width=True, help=f"Ir a {target}"):
                st.session_state['nav_target'] = target
                st.rerun()

    st.markdown('<div class="gcm-section-divider"></div>', unsafe_allow_html=True)

    st.markdown("""<p style='text-align:center;font-size:11px;color:#94a3b8;letter-spacing:0.5px'>
        Sistema de Gestion de Materias Primas Ceramicas<br>
        <span style="font-size:9px;text-transform:uppercase;letter-spacing:1.5px">Created, Engineered &amp; Developed by</span><br>
        <b style="font-size:13px;color:#1a237e">GEOCIVMET</b><br>
        <span style="font-size:10px">v4.0 Lab System | &copy; 2026 Todos los derechos reservados.</span><br>
        <span style="font-size:10px;color:#64748b">Seleccione una opcion en el menu lateral o haga clic en un modulo</span>
    </p>""", unsafe_allow_html=True)

    # KPIs rápidos si hay datos
    df = obtener_datos_completos()
    if not df.empty:
        st.divider()
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Muestras Registradas", len(df))
        k2.metric("Yacimientos", df['yacimiento'].nunique())
        fe_mean = df['fe2o3'].dropna().mean()
        aa_mean = df['absorcion'].dropna().mean()
        k3.metric("Fe₂O₃ Promedio", f"{fe_mean:.3f}%" if pd.notna(fe_mean) else "—")
        k4.metric("Absorción Promedio", f"{aa_mean:.2f}%" if pd.notna(aa_mean) else "—")
    else:
        st.info("Base de datos vacía. Comience cargando muestras desde **Cargar desde Excel** "
                "o **Agregar Muestra Manual** en el menú lateral.")


def main():
    st.set_page_config(
        page_title="GEOCIVMET - Sistema de Materias Primas",
        layout="wide",
        page_icon="⚒️",
        initial_sidebar_state="expanded"
    )
    init_db()

    # ── TEMA VISUAL PROFESIONAL v4.0 ─────────────────────────────────
    st.markdown("""
    <style>
    /* ══════════ GOOGLE FONT ══════════ */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

    /* ══════════ CSS VARIABLES ══════════ */
    :root {
        --gcm-navy:     #0d1b2a;
        --gcm-indigo:   #1a237e;
        --gcm-indigo2:  #283593;
        --gcm-slate:    #1b2838;
        --gcm-teal:     #1b3a4b;
        --gcm-bg:       #f8f9fa;
        --gcm-surface:  #ffffff;
        --gcm-border:   #e2e8f0;
        --gcm-text:     #1e293b;
        --gcm-muted:    #64748b;
        --gcm-green:    #16a34a;
        --gcm-red:      #dc2626;
        --gcm-amber:    #d97706;
        --gcm-radius:   14px;
        --gcm-shadow-sm: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.03);
        --gcm-shadow-md: 0 4px 14px rgba(0,0,0,0.07), 0 2px 6px rgba(0,0,0,0.04);
        --gcm-shadow-lg: 0 10px 30px rgba(0,0,0,0.10), 0 4px 10px rgba(0,0,0,0.05);
        --gcm-transition: 0.25s cubic-bezier(.4,0,.2,1);
    }

    /* ══════════ GLOBAL ══════════ */
    .stApp {
        background: var(--gcm-bg) !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
        color: var(--gcm-text);
    }
    .block-container {
        padding-top: 2rem !important;
        max-width: 1280px;
    }

    /* ══════════ SIDEBAR ══════════ */
    section[data-testid="stSidebar"] {
        background: linear-gradient(175deg, #0a1628 0%, var(--gcm-navy) 25%,
                    var(--gcm-slate) 60%, var(--gcm-teal) 100%) !important;
        border-right: 1px solid rgba(255,255,255,0.04) !important;
    }
    section[data-testid="stSidebar"]::before {
        content: '';
        position: absolute;
        inset: 0;
        background: radial-gradient(ellipse at 30% 20%, rgba(26,35,126,0.15) 0%, transparent 60%);
        pointer-events: none;
    }
    section[data-testid="stSidebar"] * {
        color: #c8d3de !important;
    }
    section[data-testid="stSidebar"] .stRadio label {
        color: #a0b0c0 !important;
        font-size: 12.5px !important;
        font-weight: 500 !important;
        padding: 7px 14px !important;
        border-radius: 9px !important;
        margin: 1px 0 !important;
        transition: all var(--gcm-transition) !important;
        border: 1px solid transparent !important;
    }
    section[data-testid="stSidebar"] .stRadio label:hover {
        background: rgba(255,255,255,0.07) !important;
        color: #ffffff !important;
        border-color: rgba(255,255,255,0.06) !important;
    }
    section[data-testid="stSidebar"] .stRadio label[data-checked="true"],
    section[data-testid="stSidebar"] input[type="radio"]:checked + label {
        background: linear-gradient(135deg, rgba(26,35,126,0.45), rgba(26,35,126,0.25)) !important;
        color: #ffffff !important;
        border-color: rgba(26,35,126,0.5) !important;
        font-weight: 600 !important;
    }
    section[data-testid="stSidebar"] hr {
        border-color: rgba(255,255,255,0.06) !important;
        margin: 12px 0 !important;
    }
    section[data-testid="stSidebar"] .stImage {
        border-radius: var(--gcm-radius);
        overflow: hidden;
    }
    /* Sidebar login — contraste visible */
    section[data-testid="stSidebar"] .stTextInput input {
        background: rgba(255,255,255,0.92) !important;
        border: 1.5px solid rgba(26,35,126,0.35) !important;
        color: #1a1a2e !important;
        border-radius: 8px !important;
        padding: 10px 12px !important;
        font-size: 14px !important;
    }
    section[data-testid="stSidebar"] .stTextInput input::placeholder {
        color: rgba(26,26,46,0.45) !important;
    }
    section[data-testid="stSidebar"] .stTextInput input:focus {
        border-color: rgba(26,35,126,0.7) !important;
        box-shadow: 0 0 0 2px rgba(26,35,126,0.25) !important;
        background: #ffffff !important;
    }
    section[data-testid="stSidebar"] .stTextInput label {
        color: #ffffff !important;
        font-weight: 500 !important;
        font-size: 13px !important;
    }

    /* ══════════ METRIC CARDS ══════════ */
    div[data-testid="stMetric"] {
        background: var(--gcm-surface);
        border-radius: var(--gcm-radius);
        padding: 20px 22px 18px;
        box-shadow: var(--gcm-shadow-sm);
        border-left: 4px solid var(--gcm-indigo);
        border-top: 1px solid rgba(0,0,0,0.03);
        transition: all var(--gcm-transition);
        animation: metricFadeIn 0.55s ease both;
        position: relative;
        overflow: hidden;
    }
    div[data-testid="stMetric"]::after {
        content: '';
        position: absolute;
        top: 0; right: 0;
        width: 80px; height: 80px;
        background: radial-gradient(circle at top right, rgba(26,35,126,0.04) 0%, transparent 70%);
        pointer-events: none;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-3px);
        box-shadow: var(--gcm-shadow-md);
        border-left-color: var(--gcm-indigo2);
    }
    div[data-testid="stMetric"] label {
        color: var(--gcm-muted) !important;
        font-size: 10.5px !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.8px !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--gcm-navy) !important;
        font-weight: 800 !important;
        font-size: 26px !important;
        line-height: 1.2 !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        font-size: 11px !important;
        font-weight: 600 !important;
    }
    /* Stagger animation delay */
    @keyframes metricFadeIn {
        from { opacity: 0; transform: translateY(12px) scale(0.97); }
        to   { opacity: 1; transform: translateY(0) scale(1); }
    }
    div[data-testid="stMetric"]:nth-child(1) { animation-delay: 0s; }
    div[data-testid="stMetric"]:nth-child(2) { animation-delay: 0.06s; }
    div[data-testid="stMetric"]:nth-child(3) { animation-delay: 0.12s; }
    div[data-testid="stMetric"]:nth-child(4) { animation-delay: 0.18s; }
    div[data-testid="stMetric"]:nth-child(5) { animation-delay: 0.24s; }

    /* ══════════ HEADINGS ══════════ */
    h1 {
        color: var(--gcm-navy) !important;
        font-weight: 800 !important;
        letter-spacing: -0.5px !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 2rem !important;
        padding-bottom: 4px !important;
        border-bottom: 3px solid var(--gcm-indigo) !important;
        display: inline-block !important;
        margin-bottom: 12px !important;
    }
    h2 {
        color: var(--gcm-indigo) !important;
        font-weight: 700 !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 1.35rem !important;
        margin-top: 1.2rem !important;
    }
    h3 {
        color: #334155 !important;
        font-weight: 600 !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 1.1rem !important;
    }

    /* ══════════ PRIMARY BUTTONS ══════════ */
    .stButton > button[kind="primary"],
    .stButton > button[data-testid="stBaseButton-primary"] {
        background: linear-gradient(135deg, var(--gcm-indigo) 0%, var(--gcm-indigo2) 100%) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        font-size: 13px !important;
        padding: 10px 28px !important;
        letter-spacing: 0.3px !important;
        box-shadow: 0 2px 10px rgba(26,35,126,0.25) !important;
        transition: all var(--gcm-transition) !important;
    }
    .stButton > button[kind="primary"]:hover,
    .stButton > button[data-testid="stBaseButton-primary"]:hover {
        background: linear-gradient(135deg, var(--gcm-indigo2) 0%, #3949ab 100%) !important;
        box-shadow: 0 6px 20px rgba(26,35,126,0.3) !important;
        transform: translateY(-2px) !important;
    }
    .stButton > button[kind="primary"]:active {
        transform: translateY(0) !important;
    }

    /* ══════════ SECONDARY BUTTONS ══════════ */
    .stButton > button {
        border-radius: 10px !important;
        font-weight: 500 !important;
        font-size: 12.5px !important;
        border: 1.5px solid var(--gcm-border) !important;
        transition: all var(--gcm-transition) !important;
        background: var(--gcm-surface) !important;
    }
    .stButton > button:hover {
        border-color: var(--gcm-indigo) !important;
        color: var(--gcm-indigo) !important;
        background: rgba(26,35,126,0.03) !important;
        box-shadow: 0 2px 8px rgba(26,35,126,0.08) !important;
    }

    /* ══════════ DOWNLOAD BUTTONS ══════════ */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        font-size: 12.5px !important;
        box-shadow: 0 2px 10px rgba(46,125,50,0.2) !important;
        transition: all var(--gcm-transition) !important;
    }
    .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #2e7d32 0%, #388e3c 100%) !important;
        box-shadow: 0 6px 20px rgba(46,125,50,0.3) !important;
        transform: translateY(-2px) !important;
    }

    /* ══════════ PILL TABS ══════════ */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: linear-gradient(135deg, #e8eaf6, #ede7f6);
        border-radius: 14px;
        padding: 5px;
        box-shadow: inset 0 1px 3px rgba(0,0,0,0.06);
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px !important;
        font-weight: 600 !important;
        font-size: 12.5px !important;
        padding: 8px 22px !important;
        color: #5c6b7a !important;
        background: transparent !important;
        transition: all var(--gcm-transition) !important;
        border: 1px solid transparent !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: rgba(255,255,255,0.65) !important;
        color: var(--gcm-indigo) !important;
    }
    .stTabs [aria-selected="true"] {
        background: var(--gcm-surface) !important;
        color: var(--gcm-indigo) !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08) !important;
        border-color: rgba(26,35,126,0.08) !important;
    }
    .stTabs [data-baseweb="tab-highlight"],
    .stTabs [data-baseweb="tab-border"] {
        display: none !important;
    }

    /* ══════════ DATAFRAMES ══════════ */
    .stDataFrame {
        border-radius: var(--gcm-radius) !important;
        overflow: hidden;
        box-shadow: var(--gcm-shadow-sm) !important;
        border: 1px solid rgba(0,0,0,0.04) !important;
    }

    /* ══════════ EXPANDERS ══════════ */
    .streamlit-expanderHeader,
    [data-testid="stExpander"] summary {
        font-weight: 600 !important;
        color: var(--gcm-indigo) !important;
        background: linear-gradient(135deg, #f8f9ff 0%, #f1f3ff 100%) !important;
        border-radius: 10px !important;
        border: 1px solid rgba(26,35,126,0.06) !important;
        transition: all var(--gcm-transition) !important;
    }
    .streamlit-expanderHeader:hover,
    [data-testid="stExpander"] summary:hover {
        background: linear-gradient(135deg, #eff1ff 0%, #e8eaff 100%) !important;
    }

    /* ══════════ INPUTS ══════════ */
    .stTextInput input, .stNumberInput input, .stSelectbox > div > div,
    .stMultiSelect > div > div {
        border-radius: 9px !important;
        border: 1.5px solid var(--gcm-border) !important;
        font-size: 13px !important;
        transition: all var(--gcm-transition) !important;
        background: var(--gcm-surface) !important;
    }
    .stTextInput input:focus, .stNumberInput input:focus {
        border-color: var(--gcm-indigo) !important;
        box-shadow: 0 0 0 3px rgba(26,35,126,0.08) !important;
    }

    /* ══════════ PLOTLY CHARTS ══════════ */
    .stPlotlyChart {
        background: var(--gcm-surface);
        border-radius: var(--gcm-radius);
        padding: 10px;
        box-shadow: var(--gcm-shadow-sm);
        border: 1px solid rgba(0,0,0,0.03);
        transition: box-shadow var(--gcm-transition);
    }
    .stPlotlyChart:hover {
        box-shadow: var(--gcm-shadow-md);
    }

    /* ══════════ ALERTS ══════════ */
    .stAlert {
        border-radius: 12px !important;
        font-size: 13px !important;
    }

    /* ══════════ MULTISELECT TAGS ══════════ */
    [data-baseweb="tag"] {
        background: linear-gradient(135deg, #e8eaf6, #ede7f6) !important;
        color: var(--gcm-indigo) !important;
        border-radius: 7px !important;
        font-weight: 500 !important;
    }

    /* ══════════ SECTION DIVIDER ══════════ */
    .gcm-section-divider {
        background: linear-gradient(90deg, var(--gcm-indigo) 0%, transparent 100%);
        height: 2px;
        border: none;
        margin: 2rem 0 1rem;
        opacity: 0.3;
    }

    /* ══════════ METRIC CARD WRAPPERS ══════════ */
    .metric-card {
        background: var(--gcm-surface);
        border-radius: var(--gcm-radius);
        padding: 22px 24px;
        box-shadow: var(--gcm-shadow-sm);
        border: 1px solid rgba(0,0,0,0.04);
        transition: all var(--gcm-transition);
        animation: metricFadeIn 0.5s ease both;
    }
    .metric-card:hover {
        box-shadow: var(--gcm-shadow-md);
        transform: translateY(-2px);
    }
    .metric-card-green { border-left: 4px solid var(--gcm-green); }
    .metric-card-red   { border-left: 4px solid var(--gcm-red); }
    .metric-card-amber  { border-left: 4px solid var(--gcm-amber); }
    .metric-card-blue  { border-left: 4px solid var(--gcm-indigo); }
    .metric-card .mc-label {
        font-size: 10.5px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: var(--gcm-muted);
        margin-bottom: 6px;
    }
    .metric-card .mc-value {
        font-size: 28px;
        font-weight: 800;
        color: var(--gcm-navy);
        line-height: 1.1;
    }
    .metric-card .mc-sub {
        font-size: 11px;
        color: var(--gcm-muted);
        margin-top: 4px;
    }

    /* ══════════ SECTION TITLE BADGES ══════════ */
    .section-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: linear-gradient(135deg, #f0f1ff 0%, #e8eaf6 100%);
        border: 1px solid rgba(26,35,126,0.08);
        border-radius: 10px;
        padding: 6px 16px;
        margin-bottom: 12px;
        font-size: 12px;
        font-weight: 600;
        color: var(--gcm-indigo);
        letter-spacing: 0.3px;
    }

    /* ══════════ GLOBAL FADE-IN ══════════ */
    @keyframes fadeSlideIn {
        from { opacity: 0; transform: translateY(10px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    @keyframes fadeInScale {
        from { opacity: 0; transform: scale(0.96); }
        to   { opacity: 1; transform: scale(1); }
    }
    .stPlotlyChart { animation: fadeInScale 0.4s ease both; }

    /* ══════════ DIVIDERS ══════════ */
    hr {
        border-color: var(--gcm-border) !important;
        margin: 1.5rem 0 !important;
    }

    /* ══════════ SCROLLBAR ══════════ */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: #c1c8d4; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }

    /* ══════════ STATUS PILLS ══════════ */
    .status-pill {
        display: inline-block;
        padding: 3px 12px;
        border-radius: 20px;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.3px;
    }
    .status-pill-green  { background: #dcfce7; color: #166534; }
    .status-pill-red    { background: #fee2e2; color: #991b1b; }
    .status-pill-amber  { background: #fef3c7; color: #92400e; }

    /* ══════════ PORTADA HERO ══════════ */
    .portada-hero {
        text-align: center;
        padding: 30px 0 10px;
        animation: fadeSlideIn 0.6s ease both;
    }
    .portada-hero h3 {
        color: #415a77 !important;
        letter-spacing: 5px;
        font-weight: 300 !important;
        text-transform: uppercase;
    }
    .portada-tagline {
        color: #778da9;
        font-size: 12.5px;
        letter-spacing: 1.5px;
        text-transform: uppercase;
    }

    /* ══════════ SIDEBAR FOOTER ══════════ */
    .sidebar-footer {
        text-align: center;
        padding: 12px 0;
    }
    .sidebar-footer .sf-powered {
        font-size: 8.5px;
        color: rgba(255,255,255,0.3);
        letter-spacing: 2px;
        text-transform: uppercase;
    }
    .sidebar-footer .sf-brand {
        font-size: 14px;
        font-weight: 800;
        color: rgba(255,255,255,0.85);
        letter-spacing: 3px;
        margin: 3px 0;
    }
    .sidebar-footer .sf-version {
        font-size: 9px;
        color: rgba(255,255,255,0.28);
        letter-spacing: 0.5px;
    }
    </style>
    """, unsafe_allow_html=True)

    # Sidebar branding con logo real
    logo_path = os.path.join(BASE_DIR, 'logo_geocivmet.png')
    if os.path.exists(logo_path):
        st.sidebar.image(logo_path, use_container_width=True)
    else:
        st.sidebar.markdown("""
        <div style="text-align:center;padding:10px 0 5px 0">
            <div style="font-size:24px;font-weight:900;letter-spacing:3px;color:#0d1b2a">GEOCIVMET</div>
            <div style="font-size:10px;color:#778da9;letter-spacing:2px;text-transform:uppercase">
                Consultores T&eacute;cnicos</div>
        </div>
        """, unsafe_allow_html=True)

    logged_in, user_info = login_section()

    st.sidebar.markdown("---")

    # Menú dinámico según autenticación y permisos RBAC
    menu_items = [
        "Inicio",
        "Dashboard General",
        "Análisis Comparativo",
        "Analíticas Detalladas",
        "Consulta de Muestras",
        "Ranking de Aptitud",
        "Ficha Técnica",
        "Galería de Imágenes",
    ]

    is_admin = logged_in and user_info.get('rol') == 'admin'

    # Modulos protegidos por permisos individuales
    for modulo in MODULOS_PROTEGIDOS:
        if is_admin or (logged_in and usuario_tiene_permiso(user_info, modulo)):
            menu_items.append(modulo)

    # Repositorio, Certificados y SPC: admin o logged_in
    if is_admin:
        menu_items.append("Repositorio Inteligente")
        menu_items.append("Certificado de Analisis")
    if logged_in:
        menu_items.append("Cartas de Control SPC")
        menu_items.append("Validacion de Ingeniero")
    if is_admin:
        menu_items.append("Administración")
        menu_items.append("⚙️ Panel de Administrador")

    # Soporte para navegación desde botones de portada
    nav_default = 0
    if 'nav_target' in st.session_state:
        target = st.session_state.pop('nav_target')
        if target in menu_items:
            nav_default = menu_items.index(target)

    menu = st.sidebar.radio("Navegación:", menu_items, index=nav_default)

    # Footer sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    <div class="sidebar-footer">
        <div class="sf-powered">Created, Engineered &amp; Developed by</div>
        <div class="sf-brand">GEOCIVMET</div>
        <div class="sf-version">v4.0 Lab System | &copy; 2026 Todos los derechos reservados.</div>
    </div>
    """, unsafe_allow_html=True)

    # ----- ROUTING -----
    if menu == "Inicio":
        render_portada()

    elif menu == "Dashboard General":
        page_dashboard()

    elif menu == "Análisis Comparativo":
        page_analisis_comparativo()

    elif menu == "Analíticas Detalladas":
        page_analiticas_detalladas()

    elif menu == "Repositorio Inteligente":
        page_repositorio(user_info if logged_in else None)

    elif menu == "Galería de Imágenes":
        page_galeria(user_info if logged_in else None)

    elif menu == "Agregar Muestra Manual":
        if is_admin or (logged_in and usuario_tiene_permiso(user_info, "Agregar Muestra Manual")):
            page_agregar_manual()
        else:
            st.error("No tiene permisos para agregar muestras.")

    elif menu == "Cargar desde Excel":
        if is_admin or (logged_in and usuario_tiene_permiso(user_info, "Cargar desde Excel")):
            page_cargar_excel()
        else:
            st.error("No tiene permisos para cargar desde Excel.")

    elif menu == "Consulta de Muestras":
        page_consulta()

    elif menu == "Control de Calidad":
        page_control_calidad()

    elif menu == "Ranking de Aptitud":
        page_ranking_aptitud()

    elif menu == "Ficha Técnica":
        page_ficha_tecnica()

    elif menu == "Curvas de Gresificación":
        page_curvas_gresificacion()

    elif menu == "Fórmula Seger / UMF":
        page_seger_umf()

    elif menu == "Predicción de Color":
        page_prediccion_color()

    elif menu == "Certificado de Analisis":
        if logged_in and user_info.get('rol') == 'admin':
            page_certificado_analisis(user_info)
        else:
            st.error("Acceso restringido a administradores.")

    elif menu == "Cartas de Control SPC":
        if logged_in:
            page_spc()
        else:
            st.warning("Debes iniciar sesion para acceder a Cartas de Control SPC.")

    elif menu == "Optimizador de Mezclas":
        if is_admin or (logged_in and usuario_tiene_permiso(user_info, "Optimizador de Mezclas")):
            page_optimizador_mezclas(user_info)
        else:
            st.warning("No tiene permisos para acceder al Optimizador de Mezclas.")

    elif menu == "Blender & Composite":
        if is_admin or (logged_in and usuario_tiene_permiso(user_info, "Blender & Composite")):
            page_blender_composite(user_info)
        else:
            st.warning("No tiene permisos para acceder a Blender & Composite.")

    elif menu == "Administración":
        if logged_in and user_info['rol'] == 'admin':
            page_admin()
        else:
            st.error("Acceso restringido a administradores.")

    elif menu == "Validacion de Ingeniero":
        if logged_in:
            page_validacion_ingeniero(user_info)
        else:
            st.warning("Inicie sesion para solicitar validacion de ingeniero.")

    elif menu == "⚙️ Panel de Administrador":
        if logged_in and user_info.get('rol') == 'admin':
            page_panel_admin()
        else:
            st.error("Acceso restringido a administradores.")


# =====================================================
# PANEL DE ADMINISTRADOR (RBAC)
# =====================================================

def page_panel_admin():
    """Panel de Administrador: gestion de usuarios, estados y permisos RBAC."""
    st.title("⚙️ Panel de Administrador")
    st.markdown('<div class="section-badge">🔐 Control de acceso basado en roles (RBAC)</div>',
                unsafe_allow_html=True)

    tab_usuarios, tab_pendientes, tab_validaciones = st.tabs(
        ["👥 Gestion de Usuarios", "⏳ Solicitudes Pendientes", "📋 Validaciones de Ingeniero"])

    # ══════════ TAB 1: GESTION DE USUARIOS ══════════
    with tab_usuarios:
        conn = get_conn()
        df_users = pd.read_sql(
            "SELECT id, username, nombre_completo, rol, estado, permisos FROM usuarios ORDER BY id",
            conn)
        conn.close()

        if df_users.empty:
            st.info("No hay usuarios registrados.")
            return

        st.subheader("Usuarios del Sistema")
        st.caption("Modifique el estado, rol y permisos de cada usuario. "
                   "Los cambios se guardan al presionar el boton de cada fila.")

        # Encabezado de tabla
        hdr_cols = st.columns([1.5, 2, 1, 1, 3.5, 1])
        headers = ["Usuario", "Nombre", "Rol", "Estado", "Permisos de Modulos", "Accion"]
        for col, h in zip(hdr_cols, headers):
            col.markdown(f"**{h}**")
        st.markdown("---")

        for _, u_row in df_users.iterrows():
            uid = int(u_row['id'])
            is_self_admin = (u_row['username'] == 'admin')
            permisos_actuales = [p.strip() for p in (u_row['permisos'] or '').split(',') if p.strip()]

            c1, c2, c3, c4, c5, c6 = st.columns([1.5, 2, 1, 1, 3.5, 1])

            with c1:
                st.text(u_row['username'])
            with c2:
                st.text(u_row['nombre_completo'] or '—')
            with c3:
                if is_self_admin:
                    st.text("admin")
                    nuevo_rol = 'admin'
                else:
                    nuevo_rol = st.selectbox(
                        "Rol", ['cliente', 'admin'],
                        index=0 if u_row['rol'] != 'admin' else 1,
                        key=f"rol_{uid}", label_visibility="collapsed")
            with c4:
                if is_self_admin:
                    st.text("activo")
                    nuevo_estado = 'activo'
                else:
                    estados_opc = ['pendiente', 'activo', 'inactivo']
                    idx_est = estados_opc.index(u_row['estado']) if u_row['estado'] in estados_opc else 0
                    nuevo_estado = st.selectbox(
                        "Estado", estados_opc, index=idx_est,
                        key=f"est_{uid}", label_visibility="collapsed")
            with c5:
                if nuevo_rol == 'admin':
                    st.caption("Admin tiene todos los permisos")
                    nuevos_permisos = ','.join(MODULOS_PROTEGIDOS)
                else:
                    perms_check = []
                    perm_cols = st.columns(len(MODULOS_PROTEGIDOS))
                    for j, modulo in enumerate(MODULOS_PROTEGIDOS):
                        with perm_cols[j]:
                            checked = st.checkbox(
                                modulo.split('/')[0].strip()[:14],
                                value=(modulo in permisos_actuales),
                                key=f"perm_{uid}_{j}",
                                help=modulo)
                            if checked:
                                perms_check.append(modulo)
                    nuevos_permisos = ','.join(perms_check)
            with c6:
                if is_self_admin:
                    st.caption("—")
                else:
                    if st.button("💾", key=f"save_{uid}", help="Guardar cambios"):
                        conn = get_conn()
                        conn.execute(
                            "UPDATE usuarios SET rol=?, estado=?, permisos=? WHERE id=?",
                            (nuevo_rol, nuevo_estado, nuevos_permisos, uid))
                        conn.commit()
                        conn.close()
                        st.success(f"Usuario '{u_row['username']}' actualizado.")
                        st.rerun()

        # Resumen
        st.markdown("---")
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Total Usuarios", len(df_users))
        r2.metric("Activos", len(df_users[df_users['estado'] == 'activo']))
        r3.metric("Pendientes", len(df_users[df_users['estado'] == 'pendiente']))
        r4.metric("Inactivos", len(df_users[df_users['estado'] == 'inactivo']))

    # ══════════ TAB 2: SOLICITUDES PENDIENTES ══════════
    with tab_pendientes:
        conn = get_conn()
        df_pend = pd.read_sql(
            "SELECT id, username, nombre_completo, rol FROM usuarios WHERE estado='pendiente' ORDER BY id",
            conn)
        conn.close()

        if df_pend.empty:
            st.success("No hay solicitudes pendientes.")
            return

        st.subheader(f"Solicitudes Pendientes ({len(df_pend)})")
        st.caption("Apruebe o rechace los registros de nuevos usuarios.")

        for _, p_row in df_pend.iterrows():
            pid = int(p_row['id'])
            with st.container():
                pc1, pc2, pc3, pc4 = st.columns([2, 2.5, 3, 2])
                with pc1:
                    st.markdown(f"**{p_row['username']}**")
                with pc2:
                    st.text(p_row['nombre_completo'] or '—')
                with pc3:
                    # Seleccion rapida de permisos al aprobar
                    perms_aprobacion = []
                    pa_cols = st.columns(len(MODULOS_PROTEGIDOS))
                    for j, modulo in enumerate(MODULOS_PROTEGIDOS):
                        with pa_cols[j]:
                            if st.checkbox(modulo.split('/')[0].strip()[:14],
                                           key=f"pa_perm_{pid}_{j}", help=modulo):
                                perms_aprobacion.append(modulo)
                with pc4:
                    bc1, bc2 = st.columns(2)
                    with bc1:
                        if st.button("✅", key=f"apr_{pid}", help="Aprobar"):
                            permisos_str = ','.join(perms_aprobacion)
                            conn = get_conn()
                            conn.execute(
                                "UPDATE usuarios SET estado='activo', permisos=? WHERE id=?",
                                (permisos_str, pid))
                            conn.commit()
                            conn.close()
                            st.success(f"'{p_row['username']}' aprobado.")
                            st.rerun()
                    with bc2:
                        if st.button("❌", key=f"rej_{pid}", help="Rechazar"):
                            conn = get_conn()
                            conn.execute(
                                "UPDATE usuarios SET estado='inactivo' WHERE id=?",
                                (pid,))
                            conn.commit()
                            conn.close()
                            st.warning(f"'{p_row['username']}' rechazado.")
                            st.rerun()
                st.markdown("---")

    # ══════════ TAB 3: VALIDACIONES DE INGENIERO ══════════
    with tab_validaciones:
        page_admin_validaciones()


# =====================================================
# PÁGINAS
# =====================================================

def page_dashboard():
    st.title("📊 Dashboard Ejecutivo")
    st.markdown('<div class="section-badge">🏭 Vision general del inventario de materias primas</div>',
                unsafe_allow_html=True)

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos. Carga datos desde Excel o agrega manualmente.")
        return

    df['Calidad'], df['Uso'] = zip(*df.apply(
        lambda r: clasificar_arcilla(r['fe2o3'], r['al2o3'], r['absorcion']), axis=1))

    GCM_COLORS = ['#1a237e', '#283593', '#3949ab', '#5c6bc0', '#7986cb', '#9fa8da']

    # -- SIDEBAR FILTERS --
    with st.sidebar.expander("🔍 Filtros del Dashboard", expanded=False):
        yacimientos = sorted(df['yacimiento'].dropna().unique())
        sel_yac = st.multiselect("Yacimiento:", yacimientos, key="dash_yac")
        estados = sorted(df['estado'].dropna().unique()) if 'estado' in df.columns else []
        sel_est = st.multiselect("Estado:", estados, key="dash_est") if estados else []
        # Date range
        if 'fecha' in df.columns and df['fecha'].notna().any():
            try:
                df['_fecha_dt'] = pd.to_datetime(df['fecha'], errors='coerce')
                fecha_min = df['_fecha_dt'].dropna().min()
                fecha_max = df['_fecha_dt'].dropna().max()
                if pd.notna(fecha_min) and pd.notna(fecha_max):
                    rango = st.date_input("Rango de fechas:", [fecha_min.date(), fecha_max.date()], key="dash_fecha")
                    if len(rango) == 2:
                        df = df[(df['_fecha_dt'] >= pd.Timestamp(rango[0])) & (df['_fecha_dt'] <= pd.Timestamp(rango[1]))]
            except Exception:
                pass

    if sel_yac:
        df = df[df['yacimiento'].isin(sel_yac)]
    if sel_est:
        df = df[df['estado'].isin(sel_est)]

    # ========== FILA 1: 6 KPIs ==========
    fe_avg = df['fe2o3'].dropna().mean()
    aa_avg = df['absorcion'].dropna().mean()
    # % aptas para porcelanato (AA < 0.5% y Fe2O3 < 0.8%)
    n_aptas_porc = len(df[(df['absorcion'].fillna(99) < 0.5) & (df['fe2o3'].fillna(99) < 0.8)])
    pct_aptas = (n_aptas_porc / len(df) * 100) if len(df) > 0 else 0
    # Ultimo ingreso
    ultimo = "—"
    if 'fecha' in df.columns and df['fecha'].notna().any():
        try:
            ultimo = str(df['fecha'].dropna().iloc[-1])[:10]
        except Exception:
            pass

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Muestras", len(df))
    k2.metric("Yacimientos Activos", df['yacimiento'].nunique())
    k3.metric("Fe₂O₃ Promedio", f"{fe_avg:.3f}%" if pd.notna(fe_avg) else "—")
    k4.metric("AA Promedio", f"{aa_avg:.2f}%" if pd.notna(aa_avg) else "—")
    k5.metric("Aptas Porcelanato", f"{pct_aptas:.0f}%")
    k6.metric("Ultimo Ingreso", ultimo)

    st.markdown("")

    # ========== FILA 2: Gauge + Pie ==========
    col_gauge, col_pie = st.columns(2)

    with col_gauge:
        # Indice de Calidad General (avg score de ranking si hay specs)
        try:
            productos = obtener_productos()
            specs_all = obtener_especificaciones()
            if productos and not specs_all.empty:
                # Use first product (porcelanato) as reference
                specs_ref = specs_all[specs_all['producto'] == productos[0]]
                if not specs_ref.empty:
                    scores_list = []
                    for _, row in df.iterrows():
                        sc, _ = calcular_scoring(row, specs_ref)
                        if sc is not None:
                            scores_list.append(sc)
                    avg_score = np.mean(scores_list) if scores_list else 0
                else:
                    avg_score = 0
            else:
                avg_score = 0
        except Exception:
            avg_score = 0

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=avg_score,
            title={'text': "Indice de Calidad General", 'font': {'size': 16, 'family': 'Inter'}},
            number={'suffix': '/100', 'font': {'size': 32}},
            gauge={
                'axis': {'range': [0, 100], 'tickwidth': 2},
                'bar': {'color': '#1a237e'},
                'steps': [
                    {'range': [0, 40], 'color': '#fee2e2'},
                    {'range': [40, 70], 'color': '#fef3c7'},
                    {'range': [70, 100], 'color': '#dcfce7'},
                ],
                'threshold': {
                    'line': {'color': '#dc2626', 'width': 3},
                    'thickness': 0.8, 'value': 70
                }
            }
        ))
        fig_gauge.update_layout(height=320, margin=dict(t=60, b=20, l=30, r=30),
                                template='plotly_white')
        st.plotly_chart(fig_gauge, use_container_width=True)

    with col_pie:
        cal_counts = df['Calidad'].value_counts().reset_index()
        cal_counts.columns = ['Calidad', 'Cantidad']
        fig_pie = px.pie(cal_counts, names='Calidad', values='Cantidad',
                         title="Distribucion por Clasificacion",
                         color_discrete_sequence=GCM_COLORS,
                         hole=0.4)
        fig_pie.update_layout(height=320, template='plotly_white',
                              margin=dict(t=60, b=20))
        fig_pie.update_traces(textinfo='percent+label', textfont_size=12)
        st.plotly_chart(fig_pie, use_container_width=True)

    # ========== FILA 3: Radar por yacimiento ==========
    if df['yacimiento'].nunique() > 1:
        st.subheader("Perfil Normalizado por Yacimiento")
        radar_props = ['fe2o3', 'al2o3', 'sio2', 'absorcion', 'contraccion', 'l_color']
        radar_labels = ['Fe₂O₃', 'Al₂O₃', 'SiO₂', 'Absorcion', 'Contraccion', 'L*']
        radar_maxes = [8, 45, 85, 20, 12, 100]

        fig_radar = go.Figure()
        yacs = df['yacimiento'].dropna().unique()
        for i, yac in enumerate(yacs[:8]):  # max 8 yacimientos
            df_yac = df[df['yacimiento'] == yac]
            vals = []
            for prop, mx in zip(radar_props, radar_maxes):
                mean_v = df_yac[prop].dropna().mean()
                vals.append(min((mean_v / mx * 100) if pd.notna(mean_v) and mx > 0 else 0, 100))
            vals.append(vals[0])
            cats = radar_labels + [radar_labels[0]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals, theta=cats, fill='toself', name=yac,
                line=dict(color=GCM_COLORS[i % len(GCM_COLORS)]),
                hovertemplate='%{theta}: %{r:.1f}%<extra>' + yac + '</extra>'
            ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            height=480, template='plotly_white', showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=-0.2, xanchor='center', x=0.5)
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    # ========== FILA 4: Heatmap de completitud ==========
    st.subheader("Mapa de Completitud de Datos")
    completitud_cols = ['sio2', 'al2o3', 'fe2o3', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc',
                        'absorcion', 'contraccion', 'l_color', 'a_color', 'b_color',
                        'densidad', 'temperatura_coccion', 'mor_cocido_mpa']
    comp_labels = ['SiO₂', 'Al₂O₃', 'Fe₂O₃', 'TiO₂', 'CaO', 'MgO', 'K₂O', 'Na₂O', 'PPC',
                   'AA', 'Contr.', 'L*', 'a*', 'b*', 'Dens.', 'T°C', 'MOR']
    # Limit to max 40 samples for readability
    df_heat = df.head(40)
    matrix = []
    for _, row in df_heat.iterrows():
        fila = []
        for col in completitud_cols:
            val = row.get(col)
            fila.append(1 if val is not None and not (isinstance(val, float) and pd.isna(val)) else 0)
        matrix.append(fila)

    fig_heat = go.Figure(data=go.Heatmap(
        z=matrix,
        x=comp_labels,
        y=df_heat['nombre'].tolist(),
        colorscale=[[0, '#fee2e2'], [1, '#dcfce7']],
        showscale=False,
        hovertemplate='%{y}<br>%{x}: %{z}<extra></extra>',
        text=[['✓' if v == 1 else '✗' for v in row] for row in matrix],
        texttemplate='%{text}',
        textfont=dict(size=9),
    ))
    fig_heat.update_layout(
        height=max(300, len(df_heat) * 22 + 100),
        template='plotly_white',
        yaxis=dict(autorange='reversed'),
        margin=dict(l=10, r=10, t=30, b=30),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # Completitud summary
    total_cells = len(df) * len(completitud_cols)
    filled = sum(1 for _, row in df.iterrows() for col in completitud_cols
                 if row.get(col) is not None and not (isinstance(row.get(col), float) and pd.isna(row.get(col))))
    pct_complete = (filled / total_cells * 100) if total_cells > 0 else 0
    st.caption(f"Completitud global: **{pct_complete:.1f}%** ({filled}/{total_cells} celdas con dato)")

    # ========== FILA 5: Timeline ==========
    if '_fecha_dt' in df.columns or 'fecha' in df.columns:
        try:
            if '_fecha_dt' not in df.columns:
                df['_fecha_dt'] = pd.to_datetime(df['fecha'], errors='coerce')
            df_timeline = df.dropna(subset=['_fecha_dt']).copy()
            if not df_timeline.empty and len(df_timeline) > 1:
                st.subheader("Timeline de Ingresos")
                fig_time = px.scatter(
                    df_timeline, x='_fecha_dt', y='fe2o3',
                    color='yacimiento', hover_name='nombre',
                    size='al2o3', size_max=14,
                    labels={'_fecha_dt': 'Fecha', 'fe2o3': 'Fe₂O₃ (%)'},
                    title="Muestras por Fecha de Ingreso",
                    color_discrete_sequence=GCM_COLORS,
                )
                fig_time.update_layout(height=380, template='plotly_white',
                                       xaxis_title="Fecha de Ingreso",
                                       yaxis_title="Fe₂O₃ (%)")
                st.plotly_chart(fig_time, use_container_width=True)
        except Exception:
            pass

    # -- Widget Ranking de Aptitud integrado --
    try:
        productos = obtener_productos()
        specs_all = obtener_especificaciones()
        _widget_ranking_dashboard(df, productos, specs_all)
    except Exception:
        pass


# =====================================================
# ANÁLISIS COMPARATIVO AVANZADO
# =====================================================
def page_analisis_comparativo():
    st.title("🔬 Analisis Comparativo Detallado")
    df = obtener_datos_completos()

    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    df['Calidad'], df['Uso'] = zip(*df.apply(
        lambda r: clasificar_arcilla(r['fe2o3'], r['al2o3'], r['absorcion']), axis=1))
    df['ratio_sio2_al2o3'] = df['sio2'] / df['al2o3'].replace(0, 1)

    # --- Selector de muestras a comparar (objetivos siempre preseleccionados) ---
    st.subheader("Seleccionar Muestras para Comparar")
    todas = df['nombre'].tolist()
    # Preseleccionar siempre las muestras objetivo
    nombres_objetivo = [n for n in todas if n.upper().startswith('OBJETIVO')]
    default_sel = nombres_objetivo if nombres_objetivo else (todas[:5] if len(todas) >= 5 else todas)
    seleccionadas = st.multiselect("Muestras:", todas, default=default_sel)

    if not seleccionadas:
        st.info("Selecciona al menos una muestra.")
        return

    dfs = df[df['nombre'].isin(seleccionadas)]

    # ========== 1. RADAR COMPARATIVO MULTI-MUESTRA ==========
    st.subheader("1. Perfil Radar Comparativo")
    categories = ['Fe₂O₃', 'Al₂O₃', 'SiO₂', 'Absorción', 'Contracción', 'L* Color']
    max_vals = [5, 40, 80, 15, 10, 100]

    fig_radar = go.Figure()
    for _, row in dfs.iterrows():
        vals = [row['fe2o3'] or 0, row['al2o3'] or 0, row['sio2'] or 0,
                row['absorcion'] or 0, row['contraccion'] or 0, row['l_color'] or 0]
        norm = [min(v / m * 100, 100) for v, m in zip(vals, max_vals)]
        norm.append(norm[0])
        cats = categories + [categories[0]]
        fig_radar.add_trace(go.Scatterpolar(
            r=norm, theta=cats, fill='toself', name=row['nombre'],
            hovertemplate='%{theta}: %{r:.1f}%<extra>' + row['nombre'] + '</extra>'
        ))
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        title="Perfil Normalizado de Propiedades",
        height=500, showlegend=True
    )
    st.plotly_chart(fig_radar, use_container_width=True)

    # ========== 2. HEATMAP DE COMPOSICIÓN QUÍMICA ==========
    st.subheader("2. Mapa de Calor - Composición Química")
    oxidos = ['fe2o3', 'al2o3', 'sio2', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc']
    oxidos_label = ['Fe₂O₃', 'Al₂O₃', 'SiO₂', 'TiO₂', 'CaO', 'MgO', 'K₂O', 'Na₂O', 'PPC']
    oxidos_presentes = [ox for ox in oxidos if dfs[ox].notna().any()]
    labels_presentes = [oxidos_label[oxidos.index(ox)] for ox in oxidos_presentes]

    heat_data = dfs.set_index('nombre')[oxidos_presentes].fillna(0)
    fig_heat = go.Figure(data=go.Heatmap(
        z=heat_data.values,
        x=labels_presentes,
        y=heat_data.index.tolist(),
        colorscale='YlOrRd',
        text=heat_data.values.round(3),
        texttemplate='%{text}',
        textfont=dict(size=10),
        hovertemplate='%{y}<br>%{x}: %{z:.3f}%<extra></extra>'
    ))
    fig_heat.update_layout(title="Concentración de Óxidos por Muestra (%)",
                           height=max(350, len(seleccionadas) * 35 + 150),
                           yaxis=dict(autorange='reversed'))
    st.plotly_chart(fig_heat, use_container_width=True)

    # ========== 3. DIAGRAMA TERNARIO SiO2-Al2O3-Fe2O3 ==========
    st.subheader("3. Diagrama Ternario SiO₂ - Al₂O₃ - Fe₂O₃")
    df_tern = dfs.dropna(subset=['sio2', 'al2o3', 'fe2o3']).copy()
    if df_tern.empty:
        st.warning("No hay datos completos de SiO₂, Al₂O₃ y Fe₂O₃ para el diagrama ternario.")
        fig_tern = go.Figure()
    else:
        size_tern = None
        if 'absorcion' in df_tern.columns and df_tern['absorcion'].notna().any():
            df_tern['absorcion_size'] = df_tern['absorcion'].fillna(df_tern['absorcion'].median())
            size_tern = 'absorcion_size'
        fig_tern = px.scatter_ternary(
            df_tern, a='sio2', b='al2o3', c='fe2o3',
            color='Calidad', hover_name='nombre',
            size=size_tern,
            title="Clasificación Ternaria de Materias Primas",
            labels={'sio2': 'SiO₂', 'al2o3': 'Al₂O₃', 'fe2o3': 'Fe₂O₃'}
        )
    fig_tern.update_layout(height=550)
    st.plotly_chart(fig_tern, use_container_width=True)

    # ========== 4. BARRAS AGRUPADAS POR YACIMIENTO ==========
    st.subheader("4. Comparación por Yacimiento")

    tab_yac1, tab_yac2 = st.tabs(["Química", "Física"])

    with tab_yac1:
        df_yac = dfs.groupby('yacimiento')[oxidos_presentes].mean().reset_index()
        fig_bar = px.bar(df_yac, x='yacimiento', y=oxidos_presentes, barmode='group',
                         title="Promedio de Óxidos por Yacimiento",
                         labels={"value": "% en masa", "variable": "Óxido", "yacimiento": "Yacimiento"})
        fig_bar.update_layout(height=450)
        st.plotly_chart(fig_bar, use_container_width=True)

    with tab_yac2:
        fisicas = ['absorcion', 'contraccion']
        fisicas_label = {'absorcion': 'Absorción (%)', 'contraccion': 'Contracción (%)'}
        df_yac_f = dfs.groupby('yacimiento')[fisicas].mean().reset_index()
        fig_bar_f = px.bar(df_yac_f, x='yacimiento', y=fisicas, barmode='group',
                           title="Propiedades Físicas Promedio por Yacimiento",
                           labels={"value": "%", "variable": "Propiedad", "yacimiento": "Yacimiento"})
        fig_bar_f.update_layout(height=450)
        st.plotly_chart(fig_bar_f, use_container_width=True)

    # ========== 5. SCATTER MATRIX ==========
    st.subheader("5. Matriz de Correlación")
    vars_corr = ['fe2o3', 'al2o3', 'sio2', 'absorcion', 'contraccion', 'l_color']
    vars_present = [v for v in vars_corr if dfs[v].notna().any()]
    if len(vars_present) >= 3:
        fig_matrix = px.scatter_matrix(
            dfs, dimensions=vars_present, color='Calidad',
            hover_name='nombre',
            title="Matriz de Dispersión - Relaciones entre Variables",
            labels={v: v.replace('_', ' ').title() for v in vars_present}
        )
        fig_matrix.update_layout(height=700)
        fig_matrix.update_traces(diagonal_visible=False, marker=dict(size=5))
        st.plotly_chart(fig_matrix, use_container_width=True)

    # ========== 6. BOX PLOTS ==========
    st.subheader("6. Distribución por Yacimiento")
    prop_box = st.selectbox("Variable:", ['fe2o3', 'al2o3', 'sio2', 'absorcion', 'contraccion', 'l_color'],
                            format_func=lambda x: {'fe2o3': 'Fe₂O₃', 'al2o3': 'Al₂O₃', 'sio2': 'SiO₂',
                                                    'absorcion': 'Absorción', 'contraccion': 'Contracción',
                                                    'l_color': 'L* Color'}.get(x, x))
    fig_box = px.box(dfs, x='yacimiento', y=prop_box, color='yacimiento',
                     points='all', hover_name='nombre',
                     title=f"Distribución de {prop_box} por Yacimiento")
    fig_box.update_layout(height=450, showlegend=False)
    st.plotly_chart(fig_box, use_container_width=True)

    # ========== 7. GRÁFICO DE BARRAS HORIZONTAL - RANKING ==========
    st.subheader("7. Ranking de Muestras")
    prop_rank = st.selectbox("Ordenar por:", ['fe2o3', 'al2o3', 'absorcion', 'l_color', 'contraccion'],
                             format_func=lambda x: {'fe2o3': 'Fe₂O₃ (menor = más blanca)',
                                                      'al2o3': 'Al₂O₃ (mayor = más refractaria)',
                                                      'absorcion': 'Absorción (menor = más vitrificada)',
                                                      'l_color': 'L* Luminosidad (mayor = más clara)',
                                                      'contraccion': 'Contracción'}.get(x, x),
                             key="rank_prop")
    dfs_sorted = dfs.sort_values(prop_rank, ascending=True)
    fig_rank = px.bar(dfs_sorted, y='nombre', x=prop_rank, color='Calidad',
                      orientation='h', hover_data=['yacimiento', 'Uso'],
                      title=f"Ranking por {prop_rank}")
    fig_rank.update_layout(height=max(400, len(seleccionadas) * 30 + 100), yaxis=dict(autorange='reversed'))
    st.plotly_chart(fig_rank, use_container_width=True)

    # ========== 8. MAPA DE COLOR L*a*b* ==========
    st.subheader("8. Mapa de Color de Muestras Cocidas")
    st.markdown("Cada barra muestra el color aproximado de la muestra basado en valores L\\*a\\*b\\*")

    color_html = '<div style="display:flex;flex-wrap:wrap;gap:8px;margin:10px 0">'
    for _, row in dfs.iterrows():
        L = row.get('l_color')
        a = row.get('a_color')
        b = row.get('b_color')
        L = float(L) if pd.notna(L) else 50.0
        a = float(a) if pd.notna(a) else 0.0
        b = float(b) if pd.notna(b) else 0.0
        r_c = max(0, min(255, int((L/100 + a/200) * 255)))
        g_c = max(0, min(255, int((L/100 - a/400 - b/400) * 255)))
        b_c_rgb = max(0, min(255, int((L/100 - b/200) * 255)))
        color_html += f'''<div style="text-align:center;min-width:90px">
            <div style="width:80px;height:50px;background:rgb({r_c},{g_c},{b_c_rgb});
            border:2px solid #333;border-radius:6px;margin:0 auto"></div>
            <div style="font-size:9px;margin-top:3px;max-width:90px;overflow:hidden;
            text-overflow:ellipsis;white-space:nowrap" title="{row['nombre']}">{row['nombre'][:15]}</div>
            <div style="font-size:8px;color:#777">L={L:.0f} a={a:.1f} b={b:.1f}</div>
        </div>'''
    color_html += '</div>'
    st.markdown(color_html, unsafe_allow_html=True)

    # ========== 9. RELACIÓN SiO2/Al2O3 ==========
    st.subheader("9. Relación SiO₂/Al₂O₃ (Índice de Refractariedad)")
    st.caption("Valores bajos (~1.2) indican alta refractariedad (caolinita pura). Valores altos (>3) indican arcillas con mucho cuarzo libre.")
    fig_ratio = px.bar(dfs.sort_values('ratio_sio2_al2o3'), x='nombre', y='ratio_sio2_al2o3',
                       color='Calidad', hover_data=['yacimiento'],
                       title="Relación SiO₂/Al₂O₃ por Muestra")
    fig_ratio.add_hline(y=1.18, line_dash="dash", line_color="green",
                        annotation_text="Caolinita pura (1.18)")
    fig_ratio.add_hline(y=2.0, line_dash="dash", line_color="orange",
                        annotation_text="Límite fundentes")
    fig_ratio.update_layout(height=450, xaxis_tickangle=-45)
    st.plotly_chart(fig_ratio, use_container_width=True)


# =====================================================
# GALERÍA DE IMÁGENES
# =====================================================
def page_galeria(user_info=None):
    st.title("🖼️ Galeria de Imagenes")

    IMG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'imagenes')
    is_admin = user_info and user_info.get('rol') == 'admin'

    # Dividir en dos secciones principales
    tab_lab, tab_campo = st.tabs(["🔬 Laboratorio", "🏔️ Yacimientos / Campo"])

    # Categorías organizadas por sección
    cats_laboratorio = {
        'laboratorio': {
            'titulo': 'Análisis de Laboratorio (FRX)',
            'descripcion': 'Certificados de Fluorescencia de Rayos X y análisis químicos.',
            'cols': 2,
        },
        'muestras': {
            'titulo': 'Muestras y Probetas',
            'descripcion': 'Muestras de arcilla cruda, cocida y probetas de ensayo.',
            'cols': 3,
        },
        'procesos': {
            'titulo': 'Procesos y Fichas Técnicas',
            'descripcion': 'Diagramas de planta, fichas técnicas de proveedores y referencias.',
            'cols': 3,
        },
    }

    cats_campo = {
        'drone': {
            'titulo': 'Vista Aérea / Drone',
            'descripcion': 'Imágenes aéreas de zonas de mineralización y diseño de mina.',
            'cols': 2,
        },
        'mina': {
            'titulo': 'Operaciones de Mina',
            'descripcion': 'Excavación, extracción y operaciones en cantera de arcillas.',
            'cols': 3,
        },
        'logos': {
            'titulo': 'Identidad Corporativa',
            'descripcion': 'Logos e identidad de las empresas.',
            'cols': 4,
        },
    }

    def _render_galeria_section(categorias_dict):
        has_images = False
        for cat_key, cat_info in categorias_dict.items():
            cat_path = os.path.join(IMG_DIR, cat_key)
            if not os.path.exists(cat_path):
                continue
            images = sorted([f for f in os.listdir(cat_path)
                            if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp', '.bmp'))])
            if not images:
                continue
            has_images = True

            st.subheader(cat_info['titulo'])
            st.caption(cat_info['descripcion'])
            cols_per_row = cat_info.get('cols', 3)
            cols = st.columns(cols_per_row)
            for idx, img_file in enumerate(images):
                img_path = os.path.join(cat_path, img_file)
                with cols[idx % cols_per_row]:
                    st.image(img_path, caption=img_file, use_container_width=True)
            st.divider()

        if not has_images:
            st.info("No hay imágenes en esta sección. Un administrador puede subirlas.")

    with tab_lab:
        _render_galeria_section(cats_laboratorio)

    with tab_campo:
        _render_galeria_section(cats_campo)

    # Subir imágenes - SOLO ADMIN
    st.markdown("---")
    if is_admin:
        st.subheader("Agregar Nuevas Imágenes (Admin)")
        all_cats = {**cats_laboratorio, **cats_campo}
        col_sec, col_cat = st.columns(2)
        seccion = col_sec.selectbox("Sección:", ["Laboratorio", "Yacimientos / Campo"])
        cats_sel = cats_laboratorio if seccion == "Laboratorio" else cats_campo
        cat_upload = col_cat.selectbox("Categoría:", list(cats_sel.keys()),
                                        format_func=lambda x: cats_sel[x]['titulo'])
        uploaded = st.file_uploader("Subir imagen", type=['jpg', 'jpeg', 'png', 'webp'],
                                    key="upload_galeria", accept_multiple_files=True)
        if uploaded:
            cat_path = os.path.join(IMG_DIR, cat_upload)
            os.makedirs(cat_path, exist_ok=True)
            for up_file in uploaded:
                save_path = os.path.join(cat_path, up_file.name)
                with open(save_path, 'wb') as f:
                    f.write(up_file.getbuffer())
            st.success(f"{len(uploaded)} imagen(es) guardada(s) en {cat_upload}/")
            st.rerun()
    else:
        st.caption("Solo los administradores pueden subir imágenes. Inicie sesión como admin.")


def page_agregar_manual():
    st.title("✏️ Agregar Muestra Manual")
    st.info("Completa los campos disponibles. Solo el Nombre es obligatorio. Los demás campos son opcionales.")

    with st.form("form_nueva_muestra", clear_on_submit=True):
        st.subheader("Identificación")
        col1, col2, col3 = st.columns(3)
        nombre = col1.text_input("Nombre de Muestra *")
        codigo = col2.text_input("Código Lab (auto si vacío)")
        yacimiento = col3.text_input("Yacimiento")

        col4, col5, col6 = st.columns(3)
        estado = col4.text_input("Estado")
        municipio = col5.text_input("Municipio")
        fecha = col6.date_input("Fecha", value=date.today())

        col_lat, col_lon = st.columns(2)
        latitud = col_lat.number_input("Latitud", value=0.0, format="%.6f", min_value=-90.0, max_value=90.0)
        longitud = col_lon.number_input("Longitud", value=0.0, format="%.6f", min_value=-180.0, max_value=180.0)

        observaciones = st.text_area("Observaciones")

        st.subheader("Análisis Químico (%)")
        qc1, qc2, qc3, qc4, qc5 = st.columns(5)
        fe2o3 = qc1.number_input("Fe₂O₃ *", min_value=0.0, max_value=100.0, format="%.3f")
        al2o3 = qc2.number_input("Al₂O₃ *", min_value=0.0, max_value=100.0, format="%.3f")
        sio2 = qc3.number_input("SiO₂", min_value=0.0, max_value=100.0, format="%.3f")
        tio2 = qc4.number_input("TiO₂", min_value=0.0, max_value=100.0, format="%.3f")
        cao = qc5.number_input("CaO", min_value=0.0, max_value=100.0, format="%.3f")

        qc6, qc7, qc8, qc9 = st.columns(4)
        mgo = qc6.number_input("MgO", min_value=0.0, max_value=100.0, format="%.3f")
        k2o = qc7.number_input("K₂O", min_value=0.0, max_value=100.0, format="%.3f")
        na2o = qc8.number_input("Na₂O", min_value=0.0, max_value=100.0, format="%.3f")
        ppc = qc9.number_input("PPC (LOI)", min_value=0.0, max_value=100.0, format="%.3f")

        st.subheader("Propiedades Físicas")
        fc1, fc2, fc3, fc4 = st.columns(4)
        absorcion = fc1.number_input("Absorción (%) *", min_value=0.0, max_value=100.0, format="%.2f")
        contraccion = fc2.number_input("Contracción (%)", min_value=0.0, max_value=100.0, format="%.2f")
        resistencia = fc3.number_input("Resistencia Flexión (N/mm²)", min_value=0.0, format="%.2f")
        densidad = fc4.number_input("Densidad (g/cm³)", min_value=0.0, format="%.3f")

        fc5, fc6, fc7, fc8 = st.columns(4)
        l_color = fc5.number_input("L* (Color)", min_value=0.0, max_value=100.0, format="%.2f")
        a_color = fc6.number_input("a* (Color)", min_value=-128.0, max_value=128.0, format="%.2f")
        b_color = fc7.number_input("b* (Color)", min_value=-128.0, max_value=128.0, format="%.2f")
        temp_coccion = fc8.number_input("Temp. Cocción (°C)", min_value=0.0, max_value=1500.0, format="%.0f")

        submitted = st.form_submit_button("Guardar Muestra", type="primary")

        if submitted:
            if not nombre.strip():
                st.error("El nombre de la muestra es obligatorio.")
            else:
                data = {
                    'nombre': nombre, 'codigo_lab': codigo or None,
                    'yacimiento': yacimiento or 'General',
                    'estado': estado, 'municipio': municipio,
                    'latitud': latitud if latitud != 0 else None,
                    'longitud': longitud if longitud != 0 else None,
                    'fecha': fecha.isoformat(),
                    'observaciones': observaciones,
                    'fe2o3': fe2o3, 'al2o3': al2o3, 'sio2': sio2,
                    'tio2': tio2, 'cao': cao, 'mgo': mgo,
                    'k2o': k2o, 'na2o': na2o, 'ppc': ppc,
                    'absorcion': absorcion, 'contraccion': contraccion,
                    'l_color': l_color, 'a_color': a_color, 'b_color': b_color,
                    'resistencia_flexion': resistencia,
                    'densidad': densidad if densidad > 0 else None,
                    'temperatura_coccion': temp_coccion if temp_coccion > 0 else None,
                }
                ok, msg = guardar_muestra(data)
                if ok:
                    calidad, uso = clasificar_arcilla(fe2o3, al2o3, absorcion)
                    st.success(f"Muestra guardada. Clasificación: {calidad} | Uso: {uso}")
                else:
                    st.error(f"Error: {msg}")


def page_cargar_excel():
    st.title("📥 Carga Inteligente desde Excel")
    uploaded_file = st.file_uploader("Sube tu Excel aquí", type=["xlsx", "xls", "csv"])

    if not uploaded_file:
        return

    try:
        if uploaded_file.name.endswith('.csv'):
            df_raw = pd.read_csv(uploaded_file)
        else:
            df_raw = pd.read_excel(uploaded_file)

        st.write(f"**{len(df_raw)} filas leídas.** Vista previa:")
        st.dataframe(df_raw.head(5), use_container_width=True)

        # Traductor de columnas ampliado
        mapeo = {
            'Muestra': 'nombre', 'Nombre': 'nombre', 'Sample': 'nombre',
            # Subíndices Unicode
            'Fe₂O₃ (%)': 'fe2o3', 'Al₂O₃ (%)': 'al2o3', 'SiO₂ (%)': 'sio2',
            'TiO₂ (%)': 'tio2', 'CaO (%)': 'cao', 'MgO (%)': 'mgo',
            'K₂O (%)': 'k2o', 'Na₂O (%)': 'na2o',
            # Normales
            'Fe2O3 (%)': 'fe2o3', 'Fe2O3': 'fe2o3',
            'Al2O3 (%)': 'al2o3', 'Al2O3': 'al2o3',
            'SiO2 (%)': 'sio2', 'SiO2': 'sio2',
            'TiO2 (%)': 'tio2', 'TiO2': 'tio2',
            'CaO (%)': 'cao', 'CaO': 'cao',
            'MgO (%)': 'mgo', 'MgO': 'mgo',
            'K2O (%)': 'k2o', 'K2O': 'k2o',
            'Na2O (%)': 'na2o', 'Na2O': 'na2o',
            'PPC (%)': 'ppc', 'PPC': 'ppc', 'LOI': 'ppc', 'LOI (%)': 'ppc',
            'PPC/PF (%)': 'ppc', 'PF (%)': 'ppc',
            'SO3 (%)': 'so3', 'SO3': 'so3',
            'P2O5 (%)': 'p2o5', 'P2O5': 'p2o5',
            'MnO (%)': 'mno', 'MnO': 'mno',
            'H2O (%)': 'h2o', 'H2O': 'h2o',
            'C (%)': 'carbono', 'Carbono': 'carbono',
            'S (%)': 'azufre', 'Azufre': 'azufre',
            # Físicas
            'AA (%)': 'absorcion', 'Water Absorption': 'absorcion', 'Absorcion': 'absorcion',
            'Shrinkage (%)': 'contraccion', 'Contraccion': 'contraccion',
            'Contraccion Coccion (%)': 'contraccion', 'Contraccion (R)': 'contraccion',
            'L': 'l_color', 'L*': 'l_color',
            'a': 'a_color', 'a*': 'a_color',
            'b': 'b_color', 'b*': 'b_color',
            'Origen/Ficha': 'yacimiento', 'Yacimiento': 'yacimiento',
            'Estado': 'estado', 'Municipio': 'municipio',
            'Codigo': 'codigo_lab', 'Código': 'codigo_lab',
            'Resistencia': 'resistencia_flexion', 'MOR': 'resistencia_flexion',
            'Densidad': 'densidad', 'Temperatura': 'temperatura_coccion',
            'Temperatura Coccion (C)': 'temperatura_coccion',
            # Nuevos campos
            'Superficie Especifica (m2/g)': 'superficie_especifica', 'SS': 'superficie_especifica',
            'Plasticidad Pfeff. (%)': 'pfefferkorn', 'Plasticidad': 'pfefferkorn',
            'MOR Verde (kgf/cm2)': 'mor_verde',
            'MOR Seco (kgf/cm2)': 'mor_seco',
            'MOR Cocido (kgf/cm2)': 'mor_cocido_kgf',
            'MOR Cocido (MPa)': 'mor_cocido_mpa',
            'Pfefferkorn (%)': 'pfefferkorn', 'Pfefferkorn': 'pfefferkorn',
            'Limite Liquido LL (%)': 'limite_liquido', 'LL': 'limite_liquido',
            'Limite Plastico LP (%)': 'limite_plastico', 'LP': 'limite_plastico',
            'Indice Plasticidad IP (%)': 'indice_plasticidad', 'IP': 'indice_plasticidad',
            'Residuo 45um (%)': 'residuo_45um',
            'Menor 2um (%)': 'menor_2um', '% < 2um': 'menor_2um',
            'D50 (um)': 'd50', 'D50': 'd50',
            'Densidad Aparente (g/cm3)': 'densidad',
            'Contraccion Secado (%)': 'contraccion_secado',
            'Contraccion Total (%)': 'contraccion_total',
            'Porosidad Abierta (%)': 'porosidad_abierta',
        }

        # Mapeo fuzzy para columnas con caracteres especiales (ej: SS (m²/g), MOR Seco)
        fuzzy_map = {
            'SS': 'superficie_especifica',
            'MOR Seco': 'mor_seco',
            'MOR Verde': 'mor_verde',
            'MOR Cocido': 'mor_cocido_kgf',
            'Plasticidad': 'pfefferkorn',
            'PPC': 'ppc',
            'Contraccion': 'contraccion',
        }
        for col_orig in df_raw.columns:
            if col_orig not in mapeo:
                col_clean = col_orig.strip()
                for prefix, target in fuzzy_map.items():
                    if col_clean.startswith(prefix) and target not in mapeo.values():
                        mapeo[col_orig] = target
                        break

        df_listo = df_raw.rename(columns=mapeo)

        # Solo nombre es realmente obligatorio
        required = ['nombre']
        missing = [col for col in required if col not in df_listo.columns]

        if missing:
            st.error(f"Falta la columna obligatoria: **Muestra** (nombre de la muestra)")
            st.info("Columnas disponibles en tu archivo: " + ", ".join(df_raw.columns.tolist()))
            return

        # Resumen de columnas detectadas
        cols_quimica = [c for c in ['sio2', 'al2o3', 'fe2o3', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc', 'so3', 'p2o5', 'mno', 'h2o', 'carbono', 'azufre'] if c in df_listo.columns]
        cols_fisica = [c for c in ['absorcion', 'contraccion', 'l_color', 'a_color', 'b_color', 'mor_verde', 'mor_seco', 'mor_cocido_kgf', 'mor_cocido_mpa', 'pfefferkorn', 'limite_liquido', 'limite_plastico', 'indice_plasticidad', 'superficie_especifica', 'residuo_45um', 'menor_2um', 'd50', 'densidad', 'temperatura_coccion', 'contraccion_secado', 'contraccion_total', 'porosidad_abierta'] if c in df_listo.columns]

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Muestras", len(df_listo))
        with col2:
            st.metric("Datos Químicos", len(cols_quimica))
        with col3:
            st.metric("Datos Físicos", len(cols_fisica))

        # Detectar valores sospechosos (probables errores de punto decimal)
        oxidos_percent = ['sio2', 'al2o3', 'fe2o3', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc']
        warnings_data = []
        for ox in oxidos_percent:
            if ox in df_listo.columns:
                for idx, val in df_listo[ox].items():
                    if pd.notna(val) and isinstance(val, (int, float)) and val > 100:
                        warnings_data.append(f"Fila {idx+2}: **{ox.upper()}** = {val} (¿debería ser {val/1000:.3f}%?)")

        if warnings_data:
            with st.expander(f"⚠️ {len(warnings_data)} valores sospechosos detectados (posible error decimal)", expanded=True):
                for w in warnings_data[:20]:
                    st.warning(w)
                auto_fix = st.checkbox("Corregir automáticamente (dividir entre 1000 valores >100 en óxidos)", value=True)
        else:
            auto_fix = False

        # Opción de modo
        st.markdown("---")
        modo = st.radio("Modo de carga:", [
            "➕ Agregar (mantener datos existentes)",
            "🔄 Reemplazar todo (borrar base y cargar de nuevo)"
        ], help="'Agregar' conserva las muestras que ya existen. 'Reemplazar' borra todo y carga solo este archivo.")

        if st.button("🚀 Procesar Datos", type="primary", use_container_width=True):
            # Corregir valores sospechosos si el usuario aceptó
            if auto_fix:
                for ox in oxidos_percent:
                    if ox in df_listo.columns:
                        df_listo[ox] = df_listo[ox].apply(
                            lambda v: v / 1000 if pd.notna(v) and isinstance(v, (int, float)) and v > 100 else v
                        )

            if modo.startswith("🔄"):
                conn = get_conn()
                c = conn.cursor()
                c.execute("DELETE FROM blend_componentes")
                c.execute("DELETE FROM blends")
                c.execute("DELETE FROM repositorio")
                c.execute("DELETE FROM fisica")
                c.execute("DELETE FROM quimica")
                c.execute("DELETE FROM muestras")
                conn.commit()
                conn.close()

            count_ok, count_dup, errores = 0, 0, []
            bar = st.progress(0)
            status_text = st.empty()
            total = len(df_listo)

            for i, row in df_listo.iterrows():
                data = {}
                for col in df_listo.columns:
                    val = row[col]
                    data[col] = val if not (isinstance(val, float) and pd.isna(val)) else None

                ok, msg = guardar_muestra(data)
                if ok:
                    count_ok += 1
                elif "Duplicado" in msg:
                    count_dup += 1
                else:
                    errores.append(f"Fila {i+2}: {msg}")
                bar.progress((i + 1) / total)
                status_text.text(f"Procesando {i+1}/{total}...")

            status_text.empty()
            bar.empty()

            # Resumen visual
            col1, col2, col3 = st.columns(3)
            with col1:
                st.success(f"✅ {count_ok} importadas")
            with col2:
                if count_dup > 0:
                    st.warning(f"⏭️ {count_dup} duplicadas (omitidas)")
            with col3:
                if errores:
                    st.error(f"❌ {len(errores)} errores")

            if errores:
                with st.expander("Errores"):
                    for e in errores:
                        st.text(e)

    except Exception as e:
        st.error(f"Error leyendo archivo: {e}")


def page_consulta():
    st.title("🔎 Consulta de Muestras")
    df = obtener_datos_completos()

    if df.empty:
        st.warning("Sin datos.")
        return

    df['Calidad'], df['Uso'] = zip(*df.apply(
        lambda r: clasificar_arcilla(r['fe2o3'], r['al2o3'], r['absorcion']), axis=1))

    # Aptitud contra producto seleccionado
    productos = obtener_productos()
    col_bus, col_prod = st.columns([2, 1])
    with col_bus:
        buscar = st.text_input("Buscar por nombre o yacimiento:")
    with col_prod:
        prod_eval = st.selectbox("Evaluar aptitud para:", ["— Sin evaluar —"] + productos,
                                 key="consulta_prod_eval")

    if buscar:
        mask = (df['nombre'].str.contains(buscar, case=False, na=False) |
                df['yacimiento'].str.contains(buscar, case=False, na=False))
        df = df[mask]

    # Agregar columna de aptitud si se seleccionó un producto
    if prod_eval != "— Sin evaluar —":
        specs_df = obtener_especificaciones()
        specs_prod = specs_df[specs_df['producto'] == prod_eval]
        aptitudes = []
        for _, row in df.iterrows():
            sem, n_ok, n_fail, _ = evaluar_muestra_vs_specs(row, specs_prod)
            aptitudes.append(f"{SEMAFORO_ICONS[sem]} {sem.capitalize()}")
        df = df.copy()
        df['Aptitud'] = aptitudes

    st.dataframe(df, use_container_width=True, hide_index=True)

    # Ficha individual
    if not df.empty:
        sel = st.selectbox("Ver ficha técnica de:", df['nombre'].tolist())
        row = df[df['nombre'] == sel].iloc[0]

        st.subheader(f"Ficha: {sel}")
        c1, c2, c3 = st.columns(3)
        c1.markdown(f"""
**Identificación**
- Código: `{row.get('codigo_lab', 'N/A')}`
- Yacimiento: {row.get('yacimiento', 'N/A')}
- Estado: {row.get('estado', 'N/A')}
- Fecha: {row.get('fecha', 'N/A')}
- Calidad: **{row['Calidad']}**
- Uso recomendado: **{row['Uso']}**
""")
        c2.markdown(f"""
**Análisis Químico (%)**
- Fe₂O₃: {row['fe2o3']:.3f}
- Al₂O₃: {row['al2o3']:.3f}
- SiO₂: {row['sio2']:.3f}
- TiO₂: {_display(row.get('tio2'))}
- CaO: {_display(row.get('cao'))}
- MgO: {_display(row.get('mgo'))}
- K₂O: {_display(row.get('k2o'))}
- Na₂O: {_display(row.get('na2o'))}
- PPC: {_display(row.get('ppc'))}
""")
        c3.markdown(f"""
**Propiedades Físicas**
- Absorción: {row['absorcion']:.2f}%
- Contracción: {row['contraccion']:.2f}%
- Color L*a*b*: ({_display(row['l_color'])}, {_display(row['a_color'])}, {_display(row['b_color'])})
- Resistencia: {_display(row.get('resistencia_flexion'))}
- Densidad: {_display(row.get('densidad'))}
- Temp. Cocción: {_display(row.get('temperatura_coccion'))}
""")


def _display(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    if isinstance(val, float):
        return f"{val:.3f}"
    return str(val)


# =====================================================
# FICHA TÉCNICA PROFESIONAL
# =====================================================
def _generar_html_ficha(row, lab_nombre="Laboratorio de Ensayos Cerámicos",
                        lab_direccion="Venezuela", num_informe=None, report_config=None):
    """Genera HTML profesional estilo certificado de análisis de laboratorio."""
    if report_config is None:
        report_config = {
            'show_quimica': True,
            'show_fisica': True,
            'show_color': True,
            'show_clasificacion': True,
            'show_uso_recomendado': False,
            'show_extra_data': True,
        }
    calidad, uso = clasificar_arcilla(row['fe2o3'], row['al2o3'], row['absorcion'])
    num_informe = num_informe or f"INF-{row.get('codigo_lab', 'N/A')}-{datetime.now().strftime('%Y%m%d')}"
    fecha_emision = datetime.now().strftime("%d/%m/%Y")

    # Color swatch aproximado L*a*b* -> RGB (protección NaN)
    _L = row.get('l_color')
    _a = row.get('a_color')
    _b = row.get('b_color')
    L = float(_L) if pd.notna(_L) else 50.0
    a = float(_a) if pd.notna(_a) else 0.0
    b = float(_b) if pd.notna(_b) else 0.0
    r_c = max(0, min(255, int((L/100 + a/200) * 255)))
    g_c = max(0, min(255, int((L/100 - a/400 - b/400) * 255)))
    b_c = max(0, min(255, int((L/100 - b/200) * 255)))

    def v(val, fmt=".3f"):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "—"
        try:
            return f"{float(val):{fmt}}"
        except (ValueError, TypeError):
            return str(val)

    # Calcular total de óxidos
    oxidos_vals = [row.get(ox) for ox in ['sio2','al2o3','fe2o3','tio2','cao','mgo','k2o','na2o','ppc']]
    total_oxidos = sum(float(x) for x in oxidos_vals if x is not None and not (isinstance(x, float) and pd.isna(x)))

    # Build HTML in parts
    html_head = f'''<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Ficha Técnica - {row["nombre"]}</title>
<style>
  @page {{ size: A4; margin: 15mm; }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'Segoe UI', Arial, Helvetica, sans-serif;
    color: #1a1a1a; font-size: 11pt; line-height: 1.4;
    background: #fff;
  }}
  .page {{
    max-width: 210mm; margin: 0 auto; padding: 20mm 18mm;
    border: 1px solid #ccc; position: relative;
  }}
  /* Watermark */
  .page::before {{
    content: "ORIGINAL"; position: absolute; top: 50%; left: 50%;
    transform: translate(-50%, -50%) rotate(-35deg);
    font-size: 90pt; color: rgba(0,0,0,0.03); font-weight: bold;
    pointer-events: none; z-index: 0;
  }}
  .header {{ display: flex; justify-content: space-between; align-items: flex-start;
    border-bottom: 3px solid #1a3a5c; padding-bottom: 12px; margin-bottom: 15px; position: relative; z-index: 1; }}
  .header-left {{ flex: 1; }}
  .header-left h1 {{ font-size: 16pt; color: #1a3a5c; margin-bottom: 2px; }}
  .header-left p {{ font-size: 8.5pt; color: #555; }}
  .header-right {{ text-align: right; }}
  .header-right .doc-type {{ font-size: 13pt; font-weight: bold; color: #1a3a5c;
    border: 2px solid #1a3a5c; padding: 4px 12px; display: inline-block; margin-bottom: 6px; }}
  .header-right p {{ font-size: 8.5pt; color: #555; }}
  .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 0;
    border: 1px solid #bbb; margin-bottom: 18px; position: relative; z-index: 1; }}
  .info-cell {{ padding: 6px 10px; border-bottom: 1px solid #ddd; }}
  .info-cell:nth-child(odd) {{ border-right: 1px solid #ddd; }}
  .info-cell .label {{ font-size: 8pt; text-transform: uppercase; color: #777; letter-spacing: 0.5px; }}
  .info-cell .value {{ font-size: 10pt; font-weight: 600; color: #1a1a1a; }}
  .section {{ margin-bottom: 16px; position: relative; z-index: 1; }}
  .section h2 {{ font-size: 11pt; color: #fff; background: #1a3a5c; padding: 5px 10px;
    margin-bottom: 0; letter-spacing: 0.5px; }}
  table {{ width: 100%; border-collapse: collapse; }}
  table th {{ background: #e8edf2; color: #1a3a5c; font-size: 8.5pt; text-transform: uppercase;
    padding: 5px 8px; text-align: left; border: 1px solid #bbb; letter-spacing: 0.3px; }}
  table td {{ padding: 5px 8px; font-size: 10pt; border: 1px solid #ddd; }}
  table tr:nth-child(even) td {{ background: #f7f9fb; }}
  table .val {{ text-align: right; font-variant-numeric: tabular-nums; }}
  table .total-row td {{ font-weight: bold; background: #e8edf2 !important; border-top: 2px solid #1a3a5c; }}
  .clasif-box {{ display: flex; gap: 15px; margin-top: 12px; position: relative; z-index: 1; }}
  .clasif-card {{ flex: 1; border: 1px solid #bbb; border-radius: 4px; padding: 10px 14px; text-align: center; }}
  .clasif-card .tag {{ font-size: 8pt; text-transform: uppercase; color: #777; }}
  .clasif-card .val {{ font-size: 12pt; font-weight: bold; color: #1a3a5c; }}
  .color-block {{ display: flex; align-items: center; gap: 12px; margin-top: 8px; position: relative; z-index: 1; }}
  .color-swatch {{ width: 50px; height: 50px; border: 2px solid #333; border-radius: 6px; }}
  .color-data {{ font-size: 9.5pt; }}
  .footer {{ margin-top: 25px; border-top: 2px solid #1a3a5c; padding-top: 10px;
    display: flex; justify-content: space-between; font-size: 8pt; color: #777; position: relative; z-index: 1; }}
  .footer .disclaimer {{ max-width: 65%; line-height: 1.3; }}
  .footer .signatures {{ text-align: right; }}
  .sig-line {{ border-top: 1px solid #333; width: 150px; margin: 25px 0 3px auto; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px;
    font-size: 8pt; font-weight: 600; color: #fff; }}
  .badge-premium {{ background: #0d6efd; }}
  .badge-standard {{ background: #fd7e14; }}
  .badge-industrial {{ background: #dc3545; }}
  @media print {{
    body {{ margin: 0; }} .page {{ border: none; padding: 10mm; }}
  }}
</style>
</head>
<body>
<div class="page">

  <!-- HEADER -->
  <div class="header">
    <div class="header-left">
      <h1>{lab_nombre}</h1>
      <p>{lab_direccion}</p>
      <p style="margin-top:2px; font-size:7.5pt;">Ensayos Químicos y Físicos de Materias Primas Cerámicas</p>
    </div>
    <div class="header-right">
      <div class="doc-type">CERTIFICADO DE ANÁLISIS</div>
      <p><b>N.º Informe:</b> {num_informe}</p>
      <p><b>Fecha emisión:</b> {fecha_emision}</p>
      <p><b>Página:</b> 1 de 1</p>
    </div>
  </div>

  <!-- IDENTIFICACIÓN DE MUESTRA -->
  <div class="info-grid">
    <div class="info-cell"><div class="label">Muestra / Sample</div><div class="value">{row["nombre"]}</div></div>
    <div class="info-cell"><div class="label">Código Laboratorio</div><div class="value">{row.get("codigo_lab","N/A")}</div></div>
    <div class="info-cell"><div class="label">Yacimiento / Procedencia</div><div class="value">{row.get("yacimiento","N/A")}</div></div>
    <div class="info-cell"><div class="label">Fecha de Muestreo</div><div class="value">{row.get("fecha","N/A")}</div></div>
    <div class="info-cell"><div class="label">Estado / Municipio</div><div class="value">{row.get("estado","") or "—"} / {row.get("municipio","") or "—"}</div></div>
    <div class="info-cell"><div class="label">Observaciones</div><div class="value">{row.get("observaciones","") or "—"}</div></div>
  </div>
'''

    # ANÁLISIS QUÍMICO (conditional)
    html_quimica = ''
    if report_config.get('show_quimica', True):
        html_quimica = f'''
  <!-- ANÁLISIS QUÍMICO -->
  <div class="section">
    <h2>ANÁLISIS QUÍMICO — Fluorescencia de Rayos X (FRX)</h2>
    <table>
      <thead>
        <tr>
          <th style="width:35%">Óxido / Oxide</th>
          <th style="width:25%" class="val">Resultado (%)</th>
          <th style="width:20%">Método</th>
          <th style="width:20%">Norma</th>
        </tr>
      </thead>
      <tbody>
        <tr><td>SiO₂ (Óxido de Silicio)</td><td class="val">{v(row.get("sio2"))}</td><td>FRX</td><td>ASTM C573</td></tr>
        <tr><td>Al₂O₃ (Óxido de Aluminio)</td><td class="val">{v(row.get("al2o3"))}</td><td>FRX</td><td>ASTM C573</td></tr>
        <tr><td>Fe₂O₃ (Óxido de Hierro)</td><td class="val">{v(row.get("fe2o3"))}</td><td>FRX</td><td>ASTM C573</td></tr>
        <tr><td>TiO₂ (Óxido de Titanio)</td><td class="val">{v(row.get("tio2"))}</td><td>FRX</td><td>ASTM C573</td></tr>
        <tr><td>CaO (Óxido de Calcio)</td><td class="val">{v(row.get("cao"))}</td><td>FRX</td><td>ASTM C573</td></tr>
        <tr><td>MgO (Óxido de Magnesio)</td><td class="val">{v(row.get("mgo"))}</td><td>FRX</td><td>ASTM C573</td></tr>
        <tr><td>K₂O (Óxido de Potasio)</td><td class="val">{v(row.get("k2o"))}</td><td>FRX</td><td>ASTM C573</td></tr>
        <tr><td>Na₂O (Óxido de Sodio)</td><td class="val">{v(row.get("na2o"))}</td><td>FRX</td><td>ASTM C573</td></tr>
        <tr><td>PPC / LOI (Pérdida por Calcinación)</td><td class="val">{v(row.get("ppc"))}</td><td>Gravimétrico</td><td>ASTM C573</td></tr>
        <tr class="total-row"><td>TOTAL</td><td class="val">{v(total_oxidos, ".2f") if total_oxidos > 0 else "—"}</td><td colspan="2"></td></tr>
      </tbody>
    </table>
  </div>
'''

    # PROPIEDADES FÍSICAS (conditional)
    html_fisica = ''
    if report_config.get('show_fisica', True):
        html_fisica = f'''
  <!-- PROPIEDADES FÍSICAS -->
  <div class="section">
    <h2>PROPIEDADES FÍSICAS Y CERÁMICAS</h2>
    <table>
      <thead>
        <tr>
          <th style="width:40%">Propiedad / Property</th>
          <th style="width:20%" class="val">Resultado</th>
          <th style="width:15%">Unidad</th>
          <th style="width:25%">Norma</th>
        </tr>
      </thead>
      <tbody>
        <tr><td>Absorción de Agua (AA)</td><td class="val">{v(row.get("absorcion"), ".2f")}</td><td>%</td><td>ASTM C373</td></tr>
        <tr><td>Contracción Lineal</td><td class="val">{v(row.get("contraccion"), ".2f")}</td><td>%</td><td>ASTM C326</td></tr>
        <tr><td>Resistencia a la Flexión (MOR)</td><td class="val">{v(row.get("resistencia_flexion"), ".1f")}</td><td>N/mm²</td><td>ASTM C674</td></tr>
        <tr><td>Densidad Aparente</td><td class="val">{v(row.get("densidad"))}</td><td>g/cm³</td><td>ASTM C373</td></tr>
        <tr><td>Temperatura de Cocción</td><td class="val">{v(row.get("temperatura_coccion"), ".0f")}</td><td>°C</td><td>—</td></tr>
      </tbody>
    </table>
  </div>
'''

    # COLORIMETRÍA (conditional)
    html_color = ''
    if report_config.get('show_color', True):
        html_color = f'''
  <!-- COLORIMETRÍA -->
  <div class="section">
    <h2>COLORIMETRÍA — CIE L*a*b*</h2>
    <div class="color-block">
      <div class="color-swatch" style="background:rgb({r_c},{g_c},{b_c})"></div>
      <div class="color-data">
        <table style="width:auto">
          <tr><th>L* (Luminosidad)</th><td class="val" style="min-width:80px">{v(row.get("l_color"), ".2f")}</td></tr>
          <tr><th>a* (Rojo-Verde)</th><td class="val">{v(row.get("a_color"), ".2f")}</td></tr>
          <tr><th>b* (Amarillo-Azul)</th><td class="val">{v(row.get("b_color"), ".2f")}</td></tr>
        </table>
        <p style="font-size:8pt; color:#777; margin-top:4px;">Espacio de color CIE 1976 L*a*b* | Iluminante D65</p>
      </div>
    </div>
  </div>
'''

    # DATOS COMPLEMENTARIOS (conditional)
    html_extra = ''
    if report_config.get('show_extra_data', True):
        try:
            muestra_id = row.get('id')
            if muestra_id is not None:
                conn = get_conn()
                df_extra = pd.read_sql(
                    "SELECT tipo, parametro, valor, unidad FROM datos_extra WHERE muestra_id = ? ORDER BY tipo, parametro",
                    conn, params=(int(muestra_id),))
                conn.close()
                if not df_extra.empty:
                    extra_rows = ''
                    for _, erow in df_extra.iterrows():
                        extra_rows += f'        <tr><td>{erow["tipo"]}</td><td>{erow["parametro"]}</td><td class="val">{erow["valor"]}</td><td>{erow["unidad"]}</td></tr>\n'
                    html_extra = f'''
  <!-- DATOS COMPLEMENTARIOS -->
  <div class="section">
    <h2>DATOS COMPLEMENTARIOS</h2>
    <table>
      <thead>
        <tr>
          <th style="width:20%">Ensayo</th>
          <th style="width:35%">Parámetro</th>
          <th style="width:25%" class="val">Valor</th>
          <th style="width:20%">Unidad</th>
        </tr>
      </thead>
      <tbody>
{extra_rows}      </tbody>
    </table>
  </div>
'''
        except Exception:
            pass

    # CLASIFICACIÓN (always show calidad and ratio, never show uso_recomendado)
    badge_class = "badge-premium" if "Premium" in calidad else "badge-standard" if "Estándar" in calidad else "badge-industrial"
    ratio_val = v(float(row.get("sio2") or 0) / float(row.get("al2o3") or 1), ".2f")
    html_clasif = f'''
  <!-- CLASIFICACIÓN -->
  <div class="clasif-box">
    <div class="clasif-card">
      <div class="tag">Clasificación de Calidad</div>
      <div class="val">
        <span class="badge {badge_class}">{calidad}</span>
      </div>
    </div>
    <div class="clasif-card">
      <div class="tag">Relación SiO₂/Al₂O₃</div>
      <div class="val">{ratio_val}</div>
    </div>
  </div>
'''

    html_footer = f'''
  <!-- FOOTER -->
  <div class="footer">
    <div class="disclaimer">
      <p><b>Nota:</b> Los resultados corresponden exclusivamente a la muestra recibida y ensayada.
      Este certificado no podrá ser reproducido parcialmente sin autorización escrita del laboratorio.</p>
      <p style="margin-top:4px;">Los ensayos fueron realizados conforme a normas ASTM vigentes.</p>
    </div>
    <div class="signatures">
      <div class="sig-line"></div>
      <p><b>Analista Responsable</b></p>
      <p style="margin-top:15px;">
      <div class="sig-line"></div>
      <p><b>Director Técnico</b></p>
    </div>
  </div>

</div>
</body>
</html>'''

    html = html_head + html_quimica + html_fisica + html_color + html_extra + html_clasif + html_footer
    return html


def page_ficha_tecnica():
    st.title("📄 Generador de Ficha Tecnica")
    st.markdown("Genera certificados de análisis profesionales en formato HTML/PDF, "
                "con el estilo estándar de laboratorios cerámicos (SGS, Bureau Veritas, etc.).")

    df = obtener_datos_completos()
    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    df['Calidad'], df['Uso'] = zip(*df.apply(
        lambda r: clasificar_arcilla(r['fe2o3'], r['al2o3'], r['absorcion']), axis=1))

    # Configuración del laboratorio
    with st.expander("Personalizar Encabezado del Laboratorio", expanded=False):
        lab_nombre = st.text_input("Nombre del Laboratorio",
                                   value="Laboratorio de Ensayos Cerámicos")
        lab_dir = st.text_input("Dirección / Ubicación", value="Venezuela")

    # Report config for admin
    is_admin_ficha = (st.session_state.get('user_logged') and
                      st.session_state.get('user_info', {}).get('rol') == 'admin')
    report_config = {
        'show_quimica': True,
        'show_fisica': True,
        'show_color': True,
        'show_clasificacion': True,
        'show_uso_recomendado': False,
        'show_extra_data': True,
    }
    if is_admin_ficha:
        with st.expander("Configurar contenido del reporte (Admin)", expanded=False):
            report_config['show_quimica'] = st.checkbox("Mostrar Analisis Quimico", value=True, key="rc_quimica")
            report_config['show_fisica'] = st.checkbox("Mostrar Propiedades Fisicas", value=True, key="rc_fisica")
            report_config['show_color'] = st.checkbox("Mostrar Colorimetria", value=True, key="rc_color")
            report_config['show_extra_data'] = st.checkbox("Mostrar Datos Complementarios (DRX, SEM...)", value=True, key="rc_extra")

    st.divider()

    # Selección de muestras
    modo = st.radio("Generar ficha para:", ["Una muestra", "Varias muestras (lote)"], horizontal=True)

    if modo == "Una muestra":
        sel = st.selectbox("Seleccionar muestra:", df['nombre'].tolist())
        row = df[df['nombre'] == sel].iloc[0]

        # Preview en la app
        st.subheader(f"Vista previa: {sel}")
        c1, c2, c3 = st.columns(3)
        c1.metric("Calidad", row['Calidad'])
        c2.metric("Uso", row['Uso'])
        c3.metric("Fe₂O₃", f"{row['fe2o3']:.3f}%")

        # Botón de generación
        if st.button("Generar Ficha Técnica", type="primary", use_container_width=True):
            html = _generar_html_ficha(row, lab_nombre, lab_dir, report_config=report_config)

            # Mostrar preview inline
            st.components.v1.html(html, height=900, scrolling=True)

            # Botón de descarga
            st.download_button(
                "Descargar Ficha HTML",
                html.encode('utf-8'),
                file_name=f"Ficha_{sel.replace(' ', '_')}.html",
                mime="text/html",
                type="primary",
                use_container_width=True
            )
            st.caption("Abre el archivo HTML en tu navegador y usa Ctrl+P para imprimir o guardar como PDF.")

        # Admin-only: show uso recomendado
        if is_admin_ficha:
            st.markdown("---")
            st.subheader("Uso Recomendado (Solo visible para admin)")
            calidad_admin, uso_admin = clasificar_arcilla(row['fe2o3'], row['al2o3'], row['absorcion'])
            ca1, ca2 = st.columns(2)
            ca1.info(f"**Clasificacion:** {calidad_admin}")
            ca2.info(f"**Uso Recomendado:** {uso_admin}")

    else:
        # Selección múltiple
        opciones = st.multiselect("Seleccionar muestras:", df['nombre'].tolist(),
                                  default=df['nombre'].tolist()[:3])

        if opciones and st.button("Generar Fichas del Lote", type="primary", use_container_width=True):
            fichas_html = []
            for nombre in opciones:
                row = df[df['nombre'] == nombre].iloc[0]
                fichas_html.append(_generar_html_ficha(row, lab_nombre, lab_dir, report_config=report_config))

            # HTML combinado con saltos de página
            combined = '''<!DOCTYPE html><html><head><meta charset="UTF-8">
            <title>Lote de Fichas Técnicas</title>
            <style>@media print { .page-break { page-break-before: always; } }</style>
            </head><body>'''
            for i, ficha in enumerate(fichas_html):
                # Extraer solo el contenido del body
                body_start = ficha.find('<div class="page">')
                body_end = ficha.rfind('</div>') + len('</div>')
                content = ficha[body_start:body_end]
                # Reutilizar el CSS del primero
                if i == 0:
                    style_start = ficha.find('<style>')
                    style_end = ficha.find('</style>') + len('</style>')
                    combined = combined.replace('</head>', ficha[style_start:style_end] + '</head>')
                if i > 0:
                    combined += '<div class="page-break"></div>'
                combined += content
            combined += '</body></html>'

            st.success(f"{len(opciones)} fichas generadas.")
            st.download_button(
                f"Descargar Lote ({len(opciones)} fichas) HTML",
                combined.encode('utf-8'),
                file_name=f"Lote_Fichas_{datetime.now().strftime('%Y%m%d')}.html",
                mime="text/html",
                type="primary",
                use_container_width=True
            )

            # Preview de la primera
            st.subheader(f"Vista previa: {opciones[0]}")
            st.components.v1.html(fichas_html[0], height=900, scrolling=True)


# =====================================================
# BLENDER & COMPOSITE (PROTEGIDO)
# =====================================================
def _get_all_muestras_for_blend():
    """Obtiene TODAS las muestras con sus datos, usando LEFT JOIN para incluir muestras parciales."""
    conn = get_conn()
    df = pd.read_sql("""
        SELECT m.id, m.nombre, m.yacimiento,
               q.sio2, q.al2o3, q.fe2o3, q.tio2, q.cao, q.mgo, q.k2o, q.na2o, q.ppc,
               q.so3, q.p2o5, q.mno, q.h2o, q.carbono, q.azufre,
               f.absorcion, f.contraccion, f.l_color, f.a_color, f.b_color,
               f.densidad, f.temperatura_coccion, f.superficie_especifica,
               f.mor_verde, f.mor_seco, f.mor_cocido_kgf, f.mor_cocido_mpa,
               f.pfefferkorn, f.limite_liquido, f.limite_plastico, f.indice_plasticidad,
               f.residuo_45um, f.menor_2um, f.d50,
               f.contraccion_secado, f.contraccion_total, f.porosidad_abierta
        FROM muestras m
        LEFT JOIN quimica q ON m.id = q.muestra_id
        LEFT JOIN fisica f ON m.id = f.muestra_id
        ORDER BY m.nombre
    """, conn)
    conn.close()
    return df


def page_blender_composite(user_info):
    st.title("🧪 Mezclas & Composites")
    st.caption(f"Sesión: {user_info['nombre']} ({user_info['rol']})")

    tab1, tab2, tab3 = st.tabs([
        "🔬 Diseñador de Mezclas",
        "📊 Comparar vs Muestras",
        "📋 Historial de Blends"
    ])

    df_muestras = _get_all_muestras_for_blend()
    if df_muestras.empty:
        st.warning("No hay muestras en la base de datos. Carga datos primero.")
        return

    # Columnas numéricas para promediar en blend
    COLS_QUIMICA = ['sio2', 'al2o3', 'fe2o3', 'tio2', 'cao', 'mgo', 'k2o', 'na2o', 'ppc', 'so3', 'p2o5', 'mno', 'h2o', 'carbono', 'azufre']
    COLS_FISICA = ['absorcion', 'contraccion', 'l_color', 'a_color', 'b_color', 'densidad',
                   'superficie_especifica', 'mor_verde', 'mor_seco', 'mor_cocido_kgf', 'mor_cocido_mpa',
                   'pfefferkorn', 'limite_liquido', 'limite_plastico', 'indice_plasticidad',
                   'residuo_45um', 'menor_2um', 'd50', 'contraccion_secado', 'contraccion_total', 'porosidad_abierta']
    COLS_ALL = COLS_QUIMICA + COLS_FISICA

    LABELS = {
        'sio2': 'SiO₂', 'al2o3': 'Al₂O₃', 'fe2o3': 'Fe₂O₃', 'tio2': 'TiO₂',
        'cao': 'CaO', 'mgo': 'MgO', 'k2o': 'K₂O', 'na2o': 'Na₂O', 'ppc': 'PPC',
        'so3': 'SO₃', 'p2o5': 'P₂O₅', 'mno': 'MnO', 'h2o': 'H₂O',
        'carbono': 'C', 'azufre': 'S',
        'absorcion': 'Absorción %', 'contraccion': 'Contracción Cocción %',
        'l_color': 'L*', 'a_color': 'a*', 'b_color': 'b*',
        'densidad': 'Densidad', 'superficie_especifica': 'Sup. Específica m²/g',
        'mor_verde': 'MOR Verde kgf/cm²', 'mor_seco': 'MOR Seco kgf/cm²',
        'mor_cocido_kgf': 'MOR Cocido kgf/cm²', 'mor_cocido_mpa': 'MOR Cocido MPa',
        'pfefferkorn': 'Pfefferkorn %', 'limite_liquido': 'LL %',
        'limite_plastico': 'LP %', 'indice_plasticidad': 'IP %',
        'residuo_45um': 'Residuo 45μm %', 'menor_2um': '< 2μm %', 'd50': 'D50 μm',
        'contraccion_secado': 'Contr. Secado %', 'contraccion_total': 'Contr. Total %',
        'porosidad_abierta': 'Porosidad Abierta %',
    }

    # ==========================================
    # TAB 1: DISEÑADOR DE MEZCLAS
    # ==========================================
    with tab1:
        st.markdown("### Crear nueva mezcla")

        col_info1, col_info2 = st.columns(2)
        with col_info1:
            nombre_blend = st.text_input("Nombre de la mezcla:", placeholder="Ej: Pasta Porcelanato V1")
        with col_info2:
            objetivo = st.selectbox("Objetivo de uso:", [
                "Porcelanato Técnico", "Gres Esmaltado", "Stoneware",
                "Revestimiento", "Ladrillería", "Sanitarios", "Refractario", "Otro"
            ])

        st.markdown("---")
        st.markdown("**Componentes de la mezcla:**")

        if 'blend_components' not in st.session_state:
            st.session_state.blend_components = [{'muestra': None, 'pct': 0.0}]

        col_add, col_clear = st.columns([1, 1])
        with col_add:
            if st.button("➕ Agregar Componente", use_container_width=True):
                st.session_state.blend_components.append({'muestra': None, 'pct': 0.0})
                st.rerun()
        with col_clear:
            if st.button("🗑️ Limpiar Todo", use_container_width=True):
                st.session_state.blend_components = [{'muestra': None, 'pct': 0.0}]
                st.rerun()

        componentes_validos = []
        total_pct = 0.0
        nombres_lista = df_muestras['nombre'].tolist()

        for idx in range(len(st.session_state.blend_components)):
            cols = st.columns([4, 1.5, 0.5])
            muestra = cols[0].selectbox(
                f"Componente {idx+1}",
                nombres_lista,
                key=f"comp_muestra_{idx}",
                label_visibility="collapsed" if idx > 0 else "visible"
            )
            pct = cols[1].number_input(
                "%", min_value=0.0, max_value=100.0, value=0.0,
                key=f"comp_pct_{idx}", format="%.1f"
            )
            if idx > 0:
                if cols[2].button("✕", key=f"comp_del_{idx}"):
                    st.session_state.blend_components.pop(idx)
                    st.rerun()

            if pct > 0:
                componentes_validos.append({'nombre': muestra, 'pct': pct})
                total_pct += pct

        # Barra visual de porcentaje
        if total_pct > 0:
            color_bar = "#28a745" if abs(total_pct - 100) < 0.1 else "#dc3545" if total_pct > 100 else "#ffc107"
            st.markdown(f"""
            <div style="background:#e9ecef;border-radius:10px;height:30px;margin:10px 0;overflow:hidden">
                <div style="background:{color_bar};height:100%;width:{min(total_pct, 100)}%;
                     display:flex;align-items:center;justify-content:center;color:white;font-weight:bold;
                     border-radius:10px;transition:width 0.3s">
                    {total_pct:.1f}%
                </div>
            </div>
            """, unsafe_allow_html=True)

        # ---- CÁLCULO DE PROPIEDADES CON MODELOS DE ESTIMACIÓN ----
        if componentes_validos and abs(total_pct - 100) < 0.1:
            st.markdown("---")
            st.markdown("### 📊 Propiedades Calculadas de la Mezcla")

            # Usar función de estimación con modelos científicos
            props, metodo_est = estimar_propiedades_blend(componentes_validos, df_muestras)

            # --- Leyenda de métodos ---
            st.caption("🔵 = Promedio ponderado lineal | 🟠 = Estimación con corrección científica*")

            # --- Química ---
            quim_datos = {k: v for k, v in props.items() if k in COLS_QUIMICA}
            if quim_datos:
                st.markdown("#### 🧪 Composición Química")
                n_cols = min(len(quim_datos), 5)
                for i in range(0, len(quim_datos), n_cols):
                    items = list(quim_datos.items())[i:i+n_cols]
                    metric_cols = st.columns(n_cols)
                    for j, (k, v) in enumerate(items):
                        label = LABELS.get(k, k)
                        metric_cols[j].metric(label, f"{v:.3f}%")

                # Gráfico de barras química
                fig_q = go.Figure()
                fig_q.add_trace(go.Bar(
                    x=[LABELS.get(k, k) for k in quim_datos.keys()],
                    y=list(quim_datos.values()),
                    marker_color=['#2196F3', '#4CAF50', '#f44336', '#FF9800', '#9C27B0',
                                  '#00BCD4', '#795548', '#607D8B', '#E91E63', '#3F51B5',
                                  '#CDDC39', '#FF5722', '#009688', '#673AB7', '#FFC107'][:len(quim_datos)],
                    text=[f"{v:.2f}%" for v in quim_datos.values()],
                    textposition='outside'
                ))
                fig_q.update_layout(title="Composición Química del Blend", yaxis_title="%", height=400)
                st.plotly_chart(fig_q, use_container_width=True)

            # --- Física y Mecánica ---
            fis_datos = {k: v for k, v in props.items() if k in COLS_FISICA}
            if fis_datos:
                st.markdown("#### ⚙️ Propiedades Físicas y Mecánicas")

                # Separar en categorías para mejor visualización
                cat_mecanica = {k: v for k, v in fis_datos.items() if 'mor' in k}
                cat_plasticidad = {k: v for k, v in fis_datos.items() if k in ['pfefferkorn', 'limite_liquido', 'limite_plastico', 'indice_plasticidad']}
                cat_coccion = {k: v for k, v in fis_datos.items() if k in ['absorcion', 'contraccion', 'contraccion_secado', 'contraccion_total', 'porosidad_abierta']}
                cat_otros = {k: v for k, v in fis_datos.items() if k not in cat_mecanica and k not in cat_plasticidad and k not in cat_coccion}

                for cat_name, cat_data in [("Resistencia Mecánica", cat_mecanica),
                                            ("Plasticidad", cat_plasticidad),
                                            ("Propiedades de Cocción", cat_coccion),
                                            ("Otras Propiedades", cat_otros)]:
                    if cat_data:
                        st.markdown(f"**{cat_name}:**")
                        n_cols = min(len(cat_data), 4)
                        items = list(cat_data.items())
                        for i in range(0, len(items), n_cols):
                            chunk = items[i:i+n_cols]
                            metric_cols = st.columns(n_cols)
                            for j, (k, v) in enumerate(chunk):
                                label = LABELS.get(k, k)
                                est_mark = " *" if metodo_est.get(k) == 'estimado' else ""
                                icon = "🟠" if metodo_est.get(k) == 'estimado' else "🔵"
                                metric_cols[j].metric(f"{icon} {label}", f"{v:.2f}{est_mark}")

            # Clasificación
            fe = props.get('fe2o3', 0)
            al = props.get('al2o3', 0)
            ab = props.get('absorcion', 0)
            calidad, uso = clasificar_arcilla(fe, al, ab)
            st.info(f"**Clasificación estimada:** {calidad} → Uso: {uso}")

            # Gráfico radar
            radar_keys = [k for k in ['sio2', 'al2o3', 'fe2o3', 'absorcion', 'contraccion',
                          'pfefferkorn', 'mor_cocido_mpa', 'superficie_especifica',
                          'indice_plasticidad', 'porosidad_abierta'] if k in props][:8]
            if len(radar_keys) >= 3:
                radar_labels = [LABELS.get(k, k) for k in radar_keys]
                radar_vals = [props[k] for k in radar_keys]
                max_ref = {'sio2': 80, 'al2o3': 45, 'fe2o3': 10, 'absorcion': 20,
                           'contraccion': 15, 'pfefferkorn': 40, 'mor_cocido_mpa': 60,
                           'superficie_especifica': 50, 'indice_plasticidad': 40, 'porosidad_abierta': 30}
                norm = [min(v / max_ref.get(k, max(v, 1)) * 100, 100) for k, v in zip(radar_keys, radar_vals)]
                norm.append(norm[0])
                radar_labels.append(radar_labels[0])

                fig_r = go.Figure(data=go.Scatterpolar(
                    r=norm, theta=radar_labels, fill='toself',
                    name=nombre_blend or "Mezcla",
                    fillcolor='rgba(33, 150, 243, 0.3)',
                    line=dict(color='#2196F3', width=2)
                ))
                fig_r.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                    title="Perfil Radar de la Mezcla",
                    height=450
                )
                st.plotly_chart(fig_r, use_container_width=True)

            # Color preview
            if all(k in props for k in ['l_color', 'a_color', 'b_color']):
                L_val = props['l_color']
                a_val = props['a_color']
                b_val = props['b_color']
                r_c = max(0, min(255, int((L_val / 100 + a_val / 200) * 255)))
                g_c = max(0, min(255, int((L_val / 100 - a_val / 400 - b_val / 400) * 255)))
                b_c = max(0, min(255, int((L_val / 100 - b_val / 200) * 255)))
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:15px;margin:10px 0">
                    <div style="width:80px;height:50px;background:rgb({r_c},{g_c},{b_c});
                         border:2px solid #333;border-radius:8px"></div>
                    <span>Color estimado: L*={L_val:.1f}, a*={a_val:.1f}, b*={b_val:.1f}
                    {'(corr. Kubelka-Munk)' if metodo_est.get('l_color') == 'estimado' else ''}</span>
                </div>
                """, unsafe_allow_html=True)

            # Tabla resumen de componentes
            st.markdown("#### 📋 Detalle de Componentes")
            comp_data = []
            for comp in componentes_validos:
                row = df_muestras[df_muestras['nombre'] == comp['nombre']].iloc[0]
                entry = {'Muestra': comp['nombre'], '%': comp['pct']}
                for col in COLS_QUIMICA[:6]:
                    val = row.get(col)
                    entry[LABELS.get(col, col)] = f"{val:.3f}" if pd.notna(val) else "-"
                comp_data.append(entry)
            # Fila del blend
            blend_row = {'Muestra': f"MEZCLA → {nombre_blend or 'SIN NOMBRE'}", '%': 100.0}
            for col in COLS_QUIMICA[:6]:
                blend_row[LABELS.get(col, col)] = f"{props.get(col, 0):.3f}" if col in props else "-"
            comp_data.append(blend_row)
            st.dataframe(pd.DataFrame(comp_data), use_container_width=True, hide_index=True)

            # --- ACCIONES: Guardar + PDF ---
            st.markdown("---")
            col_save, col_pdf = st.columns(2)

            with col_save:
                if nombre_blend:
                    if st.button("💾 Guardar Mezcla", type="primary", use_container_width=True):
                        conn = get_conn()
                        c = conn.cursor()
                        desc_parts = [f"{comp['nombre']} ({comp['pct']}%)" for comp in componentes_validos]
                        c.execute("""INSERT INTO blends (nombre, descripcion, creado_por, fecha, objetivo_uso)
                                     VALUES (?, ?, ?, ?, ?)""",
                                  (nombre_blend,
                                   "Componentes: " + ", ".join(desc_parts),
                                   user_info['nombre'],
                                   date.today().isoformat(),
                                   objetivo))
                        blend_id = c.lastrowid
                        for comp in componentes_validos:
                            mid = df_muestras[df_muestras['nombre'] == comp['nombre']].iloc[0]['id']
                            c.execute("INSERT INTO blend_componentes (blend_id, muestra_id, porcentaje) VALUES (?, ?, ?)",
                                      (blend_id, int(mid), comp['pct']))
                        conn.commit()
                        conn.close()
                        st.success(f"Mezcla guardada")
                        st.balloons()
                else:
                    st.warning("Ingresa un nombre para guardar.")

            with col_pdf:
                try:
                    pdf_buf = generar_ficha_pdf_blend(
                        nombre_blend or "Mezcla Sin Nombre",
                        objetivo, componentes_validos, props, metodo_est, df_muestras
                    )
                    st.download_button(
                        "📄 Descargar Ficha Técnica PDF",
                        data=pdf_buf.getvalue(),
                        file_name=f"Ficha_Tecnica_{(nombre_blend or 'Mezcla').replace(' ', '_')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                except ImportError:
                    st.error("Instala reportlab: `pip install reportlab`")
                except Exception as e:
                    st.error(f"Error generando PDF: {e}")

    # ==========================================
    # TAB 2: COMPARAR BLEND VS MUESTRAS
    # ==========================================
    with tab2:
        st.markdown("### Comparar Mezcla vs Muestras Individuales")

        if not componentes_validos or abs(total_pct - 100) > 0.1:
            st.info("Primero diseña una mezcla válida (100%) en la pestaña 'Diseñador de Mezclas'.")
        else:
            # Seleccionar muestras para comparar
            comparar_con = st.multiselect(
                "Selecciona muestras para comparar:",
                nombres_lista,
                default=nombres_lista[:3] if len(nombres_lista) >= 3 else nombres_lista
            )

            if comparar_con:
                # Gráfico comparativo de química
                quim_keys = [k for k in COLS_QUIMICA if k in props]
                if quim_keys:
                    fig_comp = go.Figure()

                    # Blend como barras
                    fig_comp.add_trace(go.Bar(
                        name=nombre_blend or "MEZCLA",
                        x=[LABELS.get(k, k) for k in quim_keys],
                        y=[props.get(k, 0) for k in quim_keys],
                        marker_color='#2196F3',
                        opacity=0.9
                    ))

                    # Cada muestra comparada
                    colores = ['#f44336', '#4CAF50', '#FF9800', '#9C27B0', '#00BCD4', '#795548']
                    for i, nombre_m in enumerate(comparar_con):
                        row_m = df_muestras[df_muestras['nombre'] == nombre_m].iloc[0]
                        vals = []
                        for k in quim_keys:
                            v = row_m.get(k)
                            vals.append(float(v) if pd.notna(v) else 0)
                        fig_comp.add_trace(go.Bar(
                            name=nombre_m,
                            x=[LABELS.get(k, k) for k in quim_keys],
                            y=vals,
                            marker_color=colores[i % len(colores)],
                            opacity=0.7
                        ))

                    fig_comp.update_layout(
                        barmode='group',
                        title="Comparación Química: Mezcla vs Muestras",
                        yaxis_title="%",
                        height=500
                    )
                    st.plotly_chart(fig_comp, use_container_width=True)

                # Radar comparativo
                radar_keys2 = [k for k in ['sio2', 'al2o3', 'fe2o3', 'absorcion', 'pfefferkorn',
                               'mor_cocido_mpa', 'indice_plasticidad'] if k in props][:6]
                if len(radar_keys2) >= 3:
                    fig_radar = go.Figure()
                    rl = [LABELS.get(k, k) for k in radar_keys2] + [LABELS.get(radar_keys2[0], radar_keys2[0])]

                    # Blend
                    bv = [props.get(k, 0) for k in radar_keys2]
                    max_r = {k: max([props.get(k, 0)] + [float(df_muestras[df_muestras['nombre'] == n].iloc[0].get(k, 0) or 0) for n in comparar_con], default=1) for k in radar_keys2}
                    bn = [v / max(max_r[k], 0.001) * 100 for k, v in zip(radar_keys2, bv)] + [bv[0] / max(max_r[radar_keys2[0]], 0.001) * 100]
                    fig_radar.add_trace(go.Scatterpolar(r=bn, theta=rl, fill='toself', name=nombre_blend or "MEZCLA",
                                                        fillcolor='rgba(33,150,243,0.2)', line=dict(color='#2196F3', width=3)))

                    for i, nm in enumerate(comparar_con[:4]):
                        row_m = df_muestras[df_muestras['nombre'] == nm].iloc[0]
                        mv = [float(row_m.get(k, 0) or 0) for k in radar_keys2]
                        mn = [v / max(max_r[k], 0.001) * 100 for k, v in zip(radar_keys2, mv)] + [mv[0] / max(max_r[radar_keys2[0]], 0.001) * 100]
                        fig_radar.add_trace(go.Scatterpolar(r=mn, theta=rl, name=nm,
                                                             line=dict(color=colores[i % len(colores)])))

                    fig_radar.update_layout(
                        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                        title="Radar Comparativo",
                        height=500
                    )
                    st.plotly_chart(fig_radar, use_container_width=True)

    # ==========================================
    # TAB 3: HISTORIAL DE BLENDS
    # ==========================================
    with tab3:
        conn = get_conn()
        df_blends = pd.read_sql("SELECT * FROM blends ORDER BY fecha DESC", conn)
        conn.close()

        if df_blends.empty:
            st.info("No hay mezclas guardadas aún. Crea una en la pestaña 'Diseñador de Mezclas'.")
        else:
            st.markdown(f"### {len(df_blends)} Mezclas Guardadas")

            for _, blend in df_blends.iterrows():
                with st.expander(f"🧪 {blend['nombre']} — {blend['objetivo_uso']} ({blend['fecha']})"):
                    st.write(f"**Creado por:** {blend['creado_por']}")
                    st.write(f"**Descripción:** {blend['descripcion']}")

                    # Cargar componentes
                    conn = get_conn()
                    df_comp = pd.read_sql(f"""
                        SELECT m.nombre, bc.porcentaje
                        FROM blend_componentes bc
                        JOIN muestras m ON bc.muestra_id = m.id
                        WHERE bc.blend_id = {blend['id']}
                    """, conn)
                    conn.close()

                    if not df_comp.empty:
                        # Gráfico pie de composición
                        fig_pie = px.pie(df_comp, values='porcentaje', names='nombre',
                                         title=f"Composición: {blend['nombre']}")
                        fig_pie.update_layout(height=350)
                        st.plotly_chart(fig_pie, use_container_width=True)

                    # Botón eliminar
                    if st.button(f"🗑️ Eliminar", key=f"del_blend_{blend['id']}"):
                        conn = get_conn()
                        c = conn.cursor()
                        c.execute("DELETE FROM blend_componentes WHERE blend_id = ?", (blend['id'],))
                        c.execute("DELETE FROM blends WHERE id = ?", (blend['id'],))
                        conn.commit()
                        conn.close()
                        st.success("Mezcla eliminada.")
                        st.rerun()


# =====================================================
# ANALÍTICAS DETALLADAS
# =====================================================
def page_analiticas_detalladas():
    st.title("📈 Analiticas Detalladas de Arcillas")
    df = obtener_datos_completos()

    if df.empty:
        st.warning("No hay muestras en la base de datos.")
        return

    df['Calidad'], df['Uso'] = zip(*df.apply(
        lambda r: clasificar_arcilla(r['fe2o3'], r['al2o3'], r['absorcion']), axis=1))
    df['ratio_sio2_al2o3'] = df['sio2'] / df['al2o3'].replace(0, 1)

    # === FILTROS ===
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        yacs = sorted(df['yacimiento'].dropna().unique())
        sel_yac = st.multiselect("Filtrar Yacimiento:", yacs, key="ad_yac")
    with col_f2:
        cals = sorted(df['Calidad'].unique())
        sel_cal = st.multiselect("Filtrar Calidad:", cals, key="ad_cal")
    if sel_yac:
        df = df[df['yacimiento'].isin(sel_yac)]
    if sel_cal:
        df = df[df['Calidad'].isin(sel_cal)]

    if df.empty:
        st.info("No hay datos con los filtros seleccionados.")
        return

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Granulometría Estimada", "Mineralogía Indicativa",
        "Mapa de Idoneidad", "Análisis Estadístico", "Comparador Visual"
    ])

    # ========== TAB 1: CURVA GRANULOMÉTRICA ESTIMADA ==========
    with tab1:
        st.subheader("Curva Granulométrica Estimada (basada en composición)")
        st.caption("Estimación teórica basada en la relación SiO₂/Al₂O₃ y contenido de fundentes. "
                   "No reemplaza un análisis granulométrico real.")

        fig_gran = go.Figure()
        tamanos = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0]
        tamanos_label = ['0.001', '0.002', '0.005', '0.01', '0.02', '0.05', '0.1', '0.25', '0.5', '1.0', '2.0']

        for _, row in df.iterrows():
            ratio = float(row.get('ratio_sio2_al2o3') or 2.5)
            al = float(row.get('al2o3') or 20)
            # Más Al2O3 y menor ratio = más partículas finas (arcillosas)
            finura = min(max((40 - al) / 30 + (ratio - 1.18) / 3, 0.1), 0.95)
            # Generar curva sigmoide
            pct_pasante = [max(2, min(100, 100 / (1 + np.exp(-8 * (np.log10(t) - np.log10(0.05 + finura * 0.3)))))) for t in tamanos]
            fig_gran.add_trace(go.Scatter(
                x=tamanos_label, y=pct_pasante, mode='lines+markers',
                name=row['nombre'], hovertemplate='%{x} mm: %{y:.1f}%<extra>' + str(row['nombre']) + '</extra>'
            ))

        fig_gran.update_layout(
            title="Curvas Granulométricas Estimadas",
            xaxis_title="Tamaño de Partícula (mm)", yaxis_title="% Pasante Acumulado",
            xaxis_type="log", height=500, yaxis=dict(range=[0, 105]),
            legend=dict(orientation="h", yanchor="bottom", y=-0.3)
        )
        # Zonas de clasificación
        fig_gran.add_vrect(x0=0.001, x1=0.002, fillcolor="brown", opacity=0.1,
                           annotation_text="Arcilla", annotation_position="top left")
        fig_gran.add_vrect(x0=0.002, x1=0.05, fillcolor="orange", opacity=0.08,
                           annotation_text="Limo", annotation_position="top left")
        fig_gran.add_vrect(x0=0.05, x1=2.0, fillcolor="yellow", opacity=0.06,
                           annotation_text="Arena", annotation_position="top left")
        st.plotly_chart(fig_gran, use_container_width=True)

    # ========== TAB 2: MINERALOGÍA INDICATIVA ==========
    with tab2:
        st.subheader("Composición Mineralógica Indicativa")
        st.caption("Estimación basada en estequiometría normativa (Método CIPW adaptado a arcillas). "
                   "Para mineralogía real, se requiere DRX.")

        sel_min = st.selectbox("Muestra:", df['nombre'].tolist(), key="min_sel")
        row_m = df[df['nombre'] == sel_min].iloc[0]

        sio2_v = float(row_m['sio2'] or 0)
        al2o3_v = float(row_m['al2o3'] or 0)
        fe2o3_v = float(row_m['fe2o3'] or 0)
        k2o_v = float(row_m.get('k2o') or 0) if not pd.isna(row_m.get('k2o')) else 0
        na2o_v = float(row_m.get('na2o') or 0) if not pd.isna(row_m.get('na2o')) else 0
        cao_v = float(row_m.get('cao') or 0) if not pd.isna(row_m.get('cao')) else 0
        tio2_v = float(row_m.get('tio2') or 0) if not pd.isna(row_m.get('tio2')) else 0

        # Caolinita: Al2O3·2SiO2·2H2O -> ratio 1:1.18 de Al2O3:SiO2
        caolinita = min(al2o3_v * 2.53, sio2_v * 0.85) * 0.8
        sio2_restante = max(0, sio2_v - caolinita * 0.465)
        cuarzo = sio2_restante * 0.9
        feldespato_k = k2o_v * 5.9 if k2o_v > 0 else 0
        feldespato_na = na2o_v * 8.5 if na2o_v > 0 else 0
        hematita = fe2o3_v * 1.0
        rutilo = tio2_v * 1.0
        calcita = cao_v * 1.78 if cao_v > 0 else 0
        otros = max(0, 100 - caolinita - cuarzo - feldespato_k - feldespato_na - hematita - rutilo - calcita)

        minerales = {
            'Caolinita (Al₂Si₂O₅(OH)₄)': caolinita,
            'Cuarzo (SiO₂)': cuarzo,
            'Feldespato K (KAlSi₃O₈)': feldespato_k,
            'Feldespato Na (NaAlSi₃O₈)': feldespato_na,
            'Hematita (Fe₂O₃)': hematita,
            'Rutilo (TiO₂)': rutilo,
            'Calcita (CaCO₃)': calcita,
            'Otros / Amorfos': otros,
        }
        minerales = {k: v for k, v in minerales.items() if v > 0.1}

        col_pie, col_bar = st.columns(2)
        with col_pie:
            fig_min = px.pie(names=list(minerales.keys()), values=list(minerales.values()),
                             title=f"Mineralogía Estimada: {sel_min}",
                             color_discrete_sequence=px.colors.qualitative.Set3)
            fig_min.update_traces(textinfo='label+percent', textposition='outside')
            fig_min.update_layout(height=450, showlegend=False)
            st.plotly_chart(fig_min, use_container_width=True)

        with col_bar:
            fig_min_bar = px.bar(x=list(minerales.values()), y=list(minerales.keys()),
                                 orientation='h', title="Proporción Mineral (%)",
                                 labels={'x': '%', 'y': 'Mineral'},
                                 color=list(minerales.keys()),
                                 color_discrete_sequence=px.colors.qualitative.Set3)
            fig_min_bar.update_layout(height=450, showlegend=False)
            st.plotly_chart(fig_min_bar, use_container_width=True)

        # Tabla interpretativa
        st.markdown("#### Interpretación")
        if caolinita > 50:
            st.success("Alta caolinita - Arcilla de alta calidad para cerámica fina")
        elif caolinita > 30:
            st.info("Caolinita moderada - Apta para cerámica general")
        else:
            st.warning("Baja caolinita - Material con alto contenido de cuarzo libre")

        if hematita > 2:
            st.warning(f"Fe₂O₃ = {fe2o3_v:.3f}% - Coloración rojiza en cocción")
        elif hematita < 0.5:
            st.success(f"Fe₂O₃ = {fe2o3_v:.3f}% - Material apto para productos blancos")

    # ========== TAB 3: MAPA DE IDONEIDAD ==========
    with tab3:
        st.subheader("Mapa de Idoneidad por Producto")
        st.caption("Evalúa qué tan apta es cada muestra para diferentes aplicaciones cerámicas.")

        productos = {
            'Porcelanato Técnico': {'fe2o3_max': 0.8, 'absorcion_max': 0.5, 'al2o3_min': 20, 'contraccion_target': 8},
            'Gres Esmaltado': {'fe2o3_max': 1.5, 'absorcion_max': 3, 'al2o3_min': 18, 'contraccion_target': 6},
            'Stoneware / Semi-Gres': {'fe2o3_max': 2.5, 'absorcion_max': 6, 'al2o3_min': 15, 'contraccion_target': 5},
            'Revestimiento Pared': {'fe2o3_max': 3, 'absorcion_max': 10, 'al2o3_min': 12, 'contraccion_target': 3},
            'Sanitarios': {'fe2o3_max': 0.6, 'absorcion_max': 0.5, 'al2o3_min': 22, 'contraccion_target': 7},
            'Ladrillería': {'fe2o3_max': 10, 'absorcion_max': 20, 'al2o3_min': 8, 'contraccion_target': 4},
        }

        # Calcular score de idoneidad
        scores_data = []
        for _, row in df.iterrows():
            for prod_name, specs in productos.items():
                fe = float(row['fe2o3'] or 0)
                ab = float(row['absorcion'] or 0)
                al = float(row['al2o3'] or 0)

                score_fe = max(0, 100 - (fe / specs['fe2o3_max']) * 100) if fe <= specs['fe2o3_max'] * 1.5 else 0
                score_ab = max(0, 100 - (ab / specs['absorcion_max']) * 100) if ab <= specs['absorcion_max'] * 1.5 else 0
                score_al = min(100, (al / specs['al2o3_min']) * 100)
                score_total = (score_fe * 0.35 + score_ab * 0.35 + score_al * 0.3)

                scores_data.append({
                    'Muestra': row['nombre'],
                    'Producto': prod_name,
                    'Score': round(score_total, 1),
                    'Yacimiento': row['yacimiento'],
                })

        df_scores = pd.DataFrame(scores_data)

        # Heatmap
        pivot = df_scores.pivot(index='Muestra', columns='Producto', values='Score')
        fig_idon = go.Figure(data=go.Heatmap(
            z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
            colorscale=[[0, '#d32f2f'], [0.5, '#ffc107'], [0.75, '#66bb6a'], [1, '#1b5e20']],
            text=pivot.values.round(0), texttemplate='%{text}%',
            textfont=dict(size=9), zmin=0, zmax=100,
            hovertemplate='%{y}<br>%{x}: %{z:.0f}%<extra></extra>',
            colorbar=dict(title="Idoneidad %")
        ))
        fig_idon.update_layout(
            title="Idoneidad de cada Muestra por Producto (%)",
            height=max(450, len(df) * 28 + 150),
            yaxis=dict(autorange='reversed'),
            xaxis_tickangle=-30
        )
        st.plotly_chart(fig_idon, use_container_width=True)

        # Top recomendaciones
        st.subheader("Top Recomendaciones por Producto")
        for prod in productos:
            subset = df_scores[df_scores['Producto'] == prod].nlargest(3, 'Score')
            top_text = " | ".join([f"**{r['Muestra']}** ({r['Score']}%)" for _, r in subset.iterrows()])
            st.markdown(f"**{prod}:** {top_text}")

    # ========== TAB 4: ANÁLISIS ESTADÍSTICO ==========
    with tab4:
        st.subheader("Análisis Estadístico Completo")

        props_stats = ['fe2o3', 'al2o3', 'sio2', 'absorcion', 'contraccion', 'l_color']
        props_labels = {'fe2o3': 'Fe₂O₃', 'al2o3': 'Al₂O₃', 'sio2': 'SiO₂',
                        'absorcion': 'Absorción', 'contraccion': 'Contracción', 'l_color': 'L* Color'}

        # Tabla de estadísticos
        stats_rows = []
        for p in props_stats:
            vals = df[p].dropna()
            if len(vals) > 0:
                stats_rows.append({
                    'Propiedad': props_labels.get(p, p),
                    'N': len(vals),
                    'Media': f"{vals.mean():.3f}",
                    'Mediana': f"{vals.median():.3f}",
                    'Desv. Std': f"{vals.std():.3f}",
                    'Min': f"{vals.min():.3f}",
                    'Max': f"{vals.max():.3f}",
                    'CV (%)': f"{(vals.std() / vals.mean() * 100):.1f}" if vals.mean() != 0 else "—",
                })
        st.dataframe(pd.DataFrame(stats_rows), use_container_width=True, hide_index=True)

        # Histogramas
        st.subheader("Distribución de Variables")
        prop_hist = st.selectbox("Variable:", props_stats,
                                  format_func=lambda x: props_labels.get(x, x), key="hist_var")
        fig_hist = px.histogram(df, x=prop_hist, nbins=15, color='Calidad',
                                 title=f"Distribución de {props_labels.get(prop_hist, prop_hist)}",
                                 marginal='box', opacity=0.7)
        fig_hist.update_layout(height=400)
        st.plotly_chart(fig_hist, use_container_width=True)

        # Correlación
        st.subheader("Matriz de Correlación")
        corr_vars = [p for p in props_stats if df[p].notna().sum() > 2]
        if len(corr_vars) >= 3:
            corr_matrix = df[corr_vars].corr()
            fig_corr = go.Figure(data=go.Heatmap(
                z=corr_matrix.values,
                x=[props_labels.get(c, c) for c in corr_matrix.columns],
                y=[props_labels.get(c, c) for c in corr_matrix.index],
                colorscale='RdBu_r', zmin=-1, zmax=1,
                text=corr_matrix.values.round(2), texttemplate='%{text}',
                textfont=dict(size=11)
            ))
            fig_corr.update_layout(title="Matriz de Correlación de Pearson", height=450)
            st.plotly_chart(fig_corr, use_container_width=True)

    # ========== TAB 5: COMPARADOR VISUAL ==========
    with tab5:
        st.subheader("Comparador Visual de Muestras")
        st.caption("Selecciona 2 muestras para comparar lado a lado")

        col_a, col_b = st.columns(2)
        with col_a:
            m1 = st.selectbox("Muestra A:", df['nombre'].tolist(), key="cmp_a")
        with col_b:
            m2 = st.selectbox("Muestra B:", df['nombre'].tolist(),
                              index=min(1, len(df)-1), key="cmp_b")

        if m1 and m2:
            r1 = df[df['nombre'] == m1].iloc[0]
            r2 = df[df['nombre'] == m2].iloc[0]

            # Radar comparativo
            cats = ['Fe₂O₃', 'Al₂O₃', 'SiO₂', 'Absorción', 'Contracción', 'L*']
            maxv = [5, 40, 80, 15, 10, 100]
            fields = ['fe2o3', 'al2o3', 'sio2', 'absorcion', 'contraccion', 'l_color']

            fig_cmp = go.Figure()
            for row_data, name, color in [(r1, m1, 'blue'), (r2, m2, 'red')]:
                vals = [float(row_data[f] or 0) for f in fields]
                norm = [min(v / m * 100, 100) for v, m in zip(vals, maxv)]
                norm.append(norm[0])
                fig_cmp.add_trace(go.Scatterpolar(
                    r=norm, theta=cats + [cats[0]], fill='toself', name=name,
                    line=dict(color=color)
                ))
            fig_cmp.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                                   title="Comparación Radar", height=450)
            st.plotly_chart(fig_cmp, use_container_width=True)

            # Tabla comparativa
            st.subheader("Tabla Comparativa")
            comp_data = []
            all_fields = [('Fe₂O₃ (%)', 'fe2o3', '.3f'), ('Al₂O₃ (%)', 'al2o3', '.3f'),
                          ('SiO₂ (%)', 'sio2', '.3f'), ('Absorción (%)', 'absorcion', '.2f'),
                          ('Contracción (%)', 'contraccion', '.2f'), ('L*', 'l_color', '.2f'),
                          ('a*', 'a_color', '.2f'), ('b*', 'b_color', '.2f'),
                          ('Calidad', 'Calidad', 's'), ('Uso', 'Uso', 's')]
            for label, field, fmt in all_fields:
                v1 = r1.get(field, None)
                v2 = r2.get(field, None)
                if fmt == 's':
                    s1, s2 = str(v1 or '—'), str(v2 or '—')
                else:
                    s1 = f"{float(v1):{fmt}}" if v1 is not None and not (isinstance(v1, float) and pd.isna(v1)) else '—'
                    s2 = f"{float(v2):{fmt}}" if v2 is not None and not (isinstance(v2, float) and pd.isna(v2)) else '—'
                comp_data.append({'Propiedad': label, m1: s1, m2: s2})
            st.dataframe(pd.DataFrame(comp_data), use_container_width=True, hide_index=True)

            # Color swatches
            st.subheader("Color de Cocción")
            cc1, cc2 = st.columns(2)
            for col, row_data, name in [(cc1, r1, m1), (cc2, r2, m2)]:
                _L = row_data.get('l_color')
                _a = row_data.get('a_color')
                _b = row_data.get('b_color')
                L = float(_L) if pd.notna(_L) else 50.0
                a = float(_a) if pd.notna(_a) else 0.0
                b = float(_b) if pd.notna(_b) else 0.0
                rc = max(0, min(255, int((L/100 + a/200) * 255)))
                gc = max(0, min(255, int((L/100 - a/400 - b/400) * 255)))
                bc = max(0, min(255, int((L/100 - b/200) * 255)))
                with col:
                    st.markdown(f"""<div style="text-align:center">
                        <div style="width:120px;height:80px;background:rgb({rc},{gc},{bc});
                        border:3px solid #333;border-radius:10px;margin:0 auto"></div>
                        <b>{name}</b><br><small>L={L:.0f} a={a:.1f} b={b:.1f}</small>
                    </div>""", unsafe_allow_html=True)


# =====================================================
# REPOSITORIO INTELIGENTE
# =====================================================
def page_repositorio(user_info):
    st.title("🗄️ Repositorio Inteligente de Documentos")
    st.markdown("Sube cualquier archivo y el sistema lo **auto-clasifica** y vincula a la muestra correspondiente.")

    # Crear directorio base
    os.makedirs(REPO_DIR, exist_ok=True)

    tab_upload, tab_browse, tab_muestra = st.tabs([
        "Subir Archivos", "Explorar Repositorio", "Archivos por Muestra"
    ])

    # ========== TAB UPLOAD ==========
    with tab_upload:
        st.subheader("Carga Inteligente de Archivos")

        uploaded_files = st.file_uploader(
            "Arrastra archivos aquí (fotos, FRX, Word, Excel, PDF, etc.)",
            type=['jpg', 'jpeg', 'png', 'webp', 'bmp', 'tiff',
                  'pdf', 'docx', 'doc', 'xlsx', 'xls', 'csv',
                  'pptx', 'dwg', 'dxf', 'kml', 'mp4', 'zip'],
            accept_multiple_files=True,
            key="repo_upload"
        )

        if uploaded_files:
            # Obtener muestras para auto-vinculación
            df_muestras = obtener_datos_completos()

            st.markdown("### Vista Previa de Clasificación")
            st.caption("Revisa la clasificación automática y ajusta si es necesario antes de confirmar.")

            archivos_info = []
            for i, f in enumerate(uploaded_files):
                cat_auto = clasificar_archivo(f.name)
                muestra_auto = detectar_muestra_en_nombre(f.name, df_muestras) if not df_muestras.empty else None

                with st.expander(f"📎 {f.name} ({f.size / 1024:.0f} KB)", expanded=(i < 3)):
                    c1, c2, c3 = st.columns([2, 2, 1])

                    with c1:
                        cat_keys = list(CATEGORIAS_REPO.keys())
                        cat_display = [f"{CATEGORIAS_REPO[k]['icon']} {CATEGORIAS_REPO[k]['titulo']}" for k in cat_keys]
                        auto_idx = cat_keys.index(cat_auto) if cat_auto in cat_keys else len(cat_keys) - 1
                        cat_sel = st.selectbox(
                            "Categoría:", cat_keys,
                            index=auto_idx,
                            format_func=lambda x: f"{CATEGORIAS_REPO[x]['icon']} {CATEGORIAS_REPO[x]['titulo']}",
                            key=f"cat_{i}"
                        )

                    with c2:
                        opciones_muestra = ['Sin vincular'] + df_muestras['nombre'].tolist() if not df_muestras.empty else ['Sin vincular']
                        default_idx = 0
                        if muestra_auto and not df_muestras.empty:
                            match_name = df_muestras[df_muestras['id'] == muestra_auto]
                            if not match_name.empty:
                                try:
                                    default_idx = opciones_muestra.index(match_name.iloc[0]['nombre'])
                                except ValueError:
                                    default_idx = 0
                        muestra_sel = st.selectbox("Vincular a muestra:", opciones_muestra,
                                                    index=default_idx, key=f"muestra_{i}")

                    with c3:
                        # Preview para imágenes
                        ext = os.path.splitext(f.name)[1].lower()
                        if ext in ['.jpg', '.jpeg', '.png', '.webp', '.bmp']:
                            st.image(f, width=100)

                    desc = st.text_input("Descripción (opcional):", key=f"desc_{i}",
                                          placeholder="Ej: Certificado FRX de muestra AP-T1-C6")
                    tags = st.text_input("Tags (separados por coma):", key=f"tags_{i}",
                                          placeholder="Ej: frx, ucv, 2026")

                    muestra_id = None
                    if muestra_sel != 'Sin vincular' and not df_muestras.empty:
                        match_df = df_muestras[df_muestras['nombre'] == muestra_sel]
                        if not match_df.empty:
                            muestra_id = int(match_df.iloc[0]['id'])

                    archivos_info.append({
                        'file': f, 'categoria': cat_sel, 'muestra_id': muestra_id,
                        'descripcion': desc, 'tags': tags
                    })

            st.divider()
            if st.button("Subir Todos los Archivos", type="primary", use_container_width=True):
                bar = st.progress(0)
                ok_count = 0
                usuario = user_info['nombre'] if user_info else 'anónimo'
                for i, info in enumerate(archivos_info):
                    try:
                        fid, ruta = guardar_archivo_repo(
                            info['file'], info['categoria'], info['muestra_id'],
                            info['descripcion'], info['tags'], usuario
                        )
                        ok_count += 1
                    except Exception as e:
                        st.error(f"Error con {info['file'].name}: {e}")
                    bar.progress((i + 1) / len(archivos_info))
                st.success(f"{ok_count}/{len(archivos_info)} archivos subidos correctamente al repositorio.")
                st.rerun()

    # ========== TAB BROWSE ==========
    with tab_browse:
        st.subheader("Explorar Repositorio")

        # Stats
        conn = get_conn()
        total_files = pd.read_sql("SELECT COUNT(*) as n FROM repositorio", conn).iloc[0]['n']
        cat_counts = pd.read_sql("SELECT categoria, COUNT(*) as n FROM repositorio GROUP BY categoria", conn)
        conn.close()

        if total_files == 0:
            st.info("El repositorio está vacío. Sube archivos en la pestaña 'Subir Archivos'.")
            return

        st.metric("Total de Archivos", int(total_files))

        # Filtro por categoría
        cat_filter = st.selectbox("Filtrar por categoría:", ['Todas'] + list(CATEGORIAS_REPO.keys()),
                                   format_func=lambda x: f"Todas ({total_files})" if x == 'Todas'
                                   else f"{CATEGORIAS_REPO[x]['icon']} {CATEGORIAS_REPO[x]['titulo']}",
                                   key="browse_cat")

        cat_param = None if cat_filter == 'Todas' else cat_filter
        df_repo = obtener_archivos_repo(categoria=cat_param)

        if df_repo.empty:
            st.info("No hay archivos en esta categoría.")
            return

        # Grid de archivos
        img_exts = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tiff'}

        for _, archivo in df_repo.iterrows():
            ruta = os.path.join(REPO_DIR, archivo['ruta_relativa'])
            ext = os.path.splitext(archivo['nombre_archivo'])[1].lower()
            icon = CATEGORIAS_REPO.get(archivo['categoria'], {}).get('icon', '📁')

            with st.container():
                cols = st.columns([0.5, 3, 2, 1])
                with cols[0]:
                    if ext in img_exts and os.path.exists(ruta):
                        st.image(ruta, width=60)
                    else:
                        st.markdown(f"<div style='font-size:36px;text-align:center'>{icon}</div>",
                                    unsafe_allow_html=True)
                with cols[1]:
                    st.markdown(f"**{archivo['nombre_original'] or archivo['nombre_archivo']}**")
                    muestra_txt = archivo.get('muestra_nombre') or 'Sin vincular'
                    st.caption(f"{CATEGORIAS_REPO.get(archivo['categoria'], {}).get('titulo', archivo['categoria'])} "
                               f"| Muestra: {muestra_txt}")
                    if archivo.get('descripcion'):
                        st.caption(archivo['descripcion'])
                with cols[2]:
                    tamano_kb = (archivo.get('tamano_bytes') or 0) / 1024
                    st.caption(f"{archivo['tipo_archivo']} | {tamano_kb:.0f} KB")
                    st.caption(f"Subido: {archivo.get('fecha_subida', 'N/A')}")
                with cols[3]:
                    if os.path.exists(ruta):
                        with open(ruta, 'rb') as fp:
                            st.download_button("⬇", fp.read(),
                                               file_name=archivo['nombre_original'] or archivo['nombre_archivo'],
                                               key=f"dl_{archivo['id']}")
                st.divider()

    # ========== TAB POR MUESTRA ==========
    with tab_muestra:
        st.subheader("Archivos Vinculados por Muestra")
        df_muestras = obtener_datos_completos()

        if df_muestras.empty:
            st.info("No hay muestras en la base de datos.")
            return

        sel_m = st.selectbox("Seleccionar Muestra:", df_muestras['nombre'].tolist(), key="repo_muestra")
        mid = int(df_muestras[df_muestras['nombre'] == sel_m].iloc[0]['id'])

        df_archivos = obtener_archivos_repo(muestra_id=mid)

        if df_archivos.empty:
            st.info(f"No hay archivos vinculados a '{sel_m}'. Sube archivos y vincúlalos a esta muestra.")
        else:
            st.success(f"{len(df_archivos)} archivo(s) vinculados a '{sel_m}'")

            # Agrupar por categoría
            for cat in df_archivos['categoria'].unique():
                cat_info = CATEGORIAS_REPO.get(cat, {'titulo': cat, 'icon': '📁'})
                st.markdown(f"#### {cat_info['icon']} {cat_info['titulo']}")

                archivos_cat = df_archivos[df_archivos['categoria'] == cat]
                img_exts = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tiff'}

                # Si son imágenes, mostrar en grid
                imgs = []
                for _, a in archivos_cat.iterrows():
                    ruta = os.path.join(REPO_DIR, a['ruta_relativa'])
                    ext = os.path.splitext(a['nombre_archivo'])[1].lower()
                    if ext in img_exts and os.path.exists(ruta):
                        imgs.append((ruta, a.get('descripcion') or a['nombre_original'] or a['nombre_archivo']))
                    else:
                        st.markdown(f"- 📎 **{a['nombre_original'] or a['nombre_archivo']}** "
                                    f"({a['tipo_archivo']}, {(a.get('tamano_bytes') or 0)/1024:.0f} KB)")

                if imgs:
                    cols = st.columns(min(3, len(imgs)))
                    for idx, (img_path, caption) in enumerate(imgs):
                        with cols[idx % len(cols)]:
                            st.image(img_path, caption=caption, use_container_width=True)


def page_admin():
    st.title("⚙️ Administracion")

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Usuarios", "Gestión de Muestras", "Datos Extra (DRX, SEM...)", "Respaldo",
        "Empaquetar para Cliente", "📊 Clasificaciones"
    ])

    with tab1:
        st.subheader("Crear Nuevo Usuario")
        with st.form("form_nuevo_usuario"):
            nu_user = st.text_input("Username")
            nu_pass = st.text_input("Contrasena", type="password")
            nu_nombre = st.text_input("Nombre Completo")
            nu_rol = st.selectbox("Rol", ["cliente", "admin"])
            nu_estado = st.selectbox("Estado", ["activo", "pendiente", "inactivo"])
            if st.form_submit_button("Crear Usuario"):
                if nu_user and nu_pass:
                    permisos_ini = ','.join(MODULOS_PROTEGIDOS) if nu_rol == 'admin' else ''
                    ok = crear_usuario(nu_user, nu_pass, nu_rol, nu_nombre, nu_estado, permisos_ini)
                    if ok:
                        st.success(f"Usuario '{nu_user}' creado con estado '{nu_estado}'.")
                    else:
                        st.error("El username ya existe.")

        st.subheader("Usuarios Existentes")
        st.caption("Para gestion detallada de permisos, use **⚙️ Panel de Administrador**.")
        conn = get_conn()
        df_users = pd.read_sql(
            "SELECT id, username, rol, nombre_completo, estado, permisos FROM usuarios", conn)
        conn.close()
        st.dataframe(df_users, use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Eliminar Muestra")
        df = obtener_datos_completos()
        if not df.empty:
            sel_del = st.selectbox("Seleccionar muestra a eliminar:", df['nombre'].tolist())
            mid = df[df['nombre'] == sel_del].iloc[0]['id']
            if st.button("Eliminar Muestra", type="primary"):
                eliminar_muestra(int(mid))
                st.success(f"Muestra '{sel_del}' eliminada.")
                st.rerun()

    with tab3:
        st.subheader("📡 Datos Complementarios por Muestra")
        st.caption("Ingrese datos de DRX, DTA/TGA, SEM, FTIR u otros ensayos especiales.")

        df_all = obtener_datos_completos()
        if df_all.empty:
            st.warning("No hay muestras.")
        else:
            sel_muestra_extra = st.selectbox("Muestra:", df_all['nombre'].tolist(), key="admin_extra_muestra")
            muestra_id = int(df_all[df_all['nombre'] == sel_muestra_extra].iloc[0]['id'])

            # Form to add new extra data
            with st.form("form_dato_extra"):
                tipo_dato = st.selectbox("Tipo de ensayo:",
                    ["DRX", "DTA", "TGA", "SEM", "FTIR", "Dilatometria", "Otro"])
                param = st.text_input("Parametro (ej: Cuarzo, Caolinita, Illita...)")
                valor_extra = st.text_input("Valor (ej: 32.5, Presente, Traza...)")
                unidad_extra = st.text_input("Unidad (ej: %, °C, um...)", value="%")
                obs_extra = st.text_area("Observaciones", height=68)
                if st.form_submit_button("Agregar Dato", type="primary"):
                    if param.strip():
                        conn = get_conn()
                        c = conn.cursor()
                        c.execute("""INSERT INTO datos_extra
                                     (muestra_id, tipo, parametro, valor, unidad, observaciones, ingresado_por)
                                     VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                  (muestra_id, tipo_dato, param.strip(), valor_extra.strip(),
                                   unidad_extra.strip(), obs_extra.strip(), 'admin'))
                        conn.commit()
                        conn.close()
                        st.success(f"Dato '{param}' agregado a {sel_muestra_extra}.")
                        st.rerun()
                    else:
                        st.warning("Ingrese el parametro.")

            # Show existing extra data for this sample
            st.markdown("---")
            st.subheader(f"Datos registrados: {sel_muestra_extra}")
            conn = get_conn()
            df_extra = pd.read_sql(
                "SELECT id, tipo, parametro, valor, unidad, observaciones, fecha FROM datos_extra WHERE muestra_id = ? ORDER BY tipo, parametro",
                conn, params=(muestra_id,))
            conn.close()
            if df_extra.empty:
                st.info("No hay datos complementarios para esta muestra.")
            else:
                st.dataframe(df_extra.drop(columns=['id']), use_container_width=True, hide_index=True)
                # Delete option
                del_id = st.selectbox("Eliminar registro:", df_extra['id'].tolist(),
                                       format_func=lambda x: f"{df_extra[df_extra['id']==x].iloc[0]['tipo']} - {df_extra[df_extra['id']==x].iloc[0]['parametro']}",
                                       key="del_extra")
                if st.button("🗑️ Eliminar registro seleccionado"):
                    conn = get_conn()
                    conn.execute("DELETE FROM datos_extra WHERE id = ?", (del_id,))
                    conn.commit()
                    conn.close()
                    st.success("Registro eliminado.")
                    st.rerun()

    with tab4:
        st.subheader("Exportar Base de Datos")
        df = obtener_datos_completos()
        if not df.empty:
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button("Descargar CSV completo", csv,
                               file_name="arcillas_vzla_backup.csv", mime="text/csv")

            buf = BytesIO()
            df.to_excel(buf, index=False, sheet_name="Muestras")
            st.download_button("Descargar Excel completo", buf.getvalue(),
                               file_name="arcillas_vzla_backup.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ========== TAB 5: EMPAQUETAR PARA CLIENTE ==========
    with tab5:
        st.subheader("Empaquetar App para Cliente")
        st.markdown("""
        Genera una copia completa de la app lista para entregar a un cliente.
        Incluye la **base de datos cargada**, **imágenes**, **repositorio** y la app configurada.
        """)

        st.divider()

        # Estado actual
        df = obtener_datos_completos()
        conn = get_conn()
        n_repo = pd.read_sql("SELECT COUNT(*) as n FROM repositorio", conn).iloc[0]['n']
        n_blends = pd.read_sql("SELECT COUNT(*) as n FROM blends", conn).iloc[0]['n']
        n_users = pd.read_sql("SELECT COUNT(*) as n FROM usuarios", conn).iloc[0]['n']
        conn.close()

        st.markdown("#### Estado Actual de la App")
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Muestras", len(df))
        s2.metric("Archivos Repo", int(n_repo))
        s3.metric("Blends", int(n_blends))
        s4.metric("Usuarios", int(n_users))

        # Contar imágenes en galería
        img_dir = os.path.join(BASE_DIR, 'imagenes')
        n_imgs = 0
        if os.path.exists(img_dir):
            for root, dirs, files in os.walk(img_dir):
                n_imgs += len([f for f in files if f.lower().endswith(
                    ('.jpg', '.jpeg', '.png', '.webp', '.bmp'))])
        st.caption(f"Imágenes en galería: {n_imgs}")

        st.divider()

        # Configuración del paquete
        st.markdown("#### Configurar Paquete del Cliente")
        with st.form("form_empaquetar"):
            nombre_cliente = st.text_input("Nombre del Cliente / Empresa *",
                                            placeholder="Ej: Cerámicas del Norte, S.A.")
            carpeta_destino = st.text_input("Carpeta de destino",
                                             value="D:/Arturo/Desktop",
                                             help="Se creará una subcarpeta con el nombre del cliente")

            st.markdown("**Incluir en el paquete:**")
            inc_c1, inc_c2 = st.columns(2)
            inc_db = inc_c1.checkbox("Base de datos (muestras, química, física)", value=True)
            inc_repo = inc_c1.checkbox("Repositorio de archivos", value=True)
            inc_imgs = inc_c2.checkbox("Galería de imágenes", value=True)
            inc_logo = inc_c2.checkbox("Logo GEOCIVMET", value=True)

            submitted = st.form_submit_button("Generar Paquete para Cliente", type="primary",
                                               use_container_width=True)

            if submitted:
                if not nombre_cliente.strip():
                    st.error("Ingresa el nombre del cliente.")
                else:
                    nombre_safe = re.sub(r'[^\w\- ]', '', nombre_cliente).strip().replace(' ', '_')
                    dest = os.path.join(carpeta_destino, f"GEOCIVMET_App_{nombre_safe}")

                    if os.path.exists(dest):
                        st.warning(f"La carpeta '{dest}' ya existe. Se sobreescribirá.")

                    try:
                        os.makedirs(dest, exist_ok=True)

                        # 1. Copiar app principal
                        shutil.copy2(os.path.join(BASE_DIR, 'app_arcillas.py'),
                                     os.path.join(dest, 'app_arcillas.py'))

                        # 2. Copiar base de datos
                        if inc_db:
                            shutil.copy2(DB_PATH, os.path.join(dest, 'arcillas_vzla.db'))

                        # 3. Copiar repositorio
                        repo_src = os.path.join(BASE_DIR, 'repositorio')
                        if inc_repo and os.path.exists(repo_src):
                            shutil.copytree(repo_src, os.path.join(dest, 'repositorio'),
                                            dirs_exist_ok=True)

                        # 4. Copiar galería de imágenes
                        if inc_imgs and os.path.exists(img_dir):
                            shutil.copytree(img_dir, os.path.join(dest, 'imagenes'),
                                            dirs_exist_ok=True)
                        else:
                            # Crear estructura vacía de imagenes
                            for cat in ['drone', 'laboratorio', 'logos', 'mina', 'muestras', 'procesos']:
                                os.makedirs(os.path.join(dest, 'imagenes', cat), exist_ok=True)

                        # 5. Copiar logo
                        logo_src = os.path.join(BASE_DIR, 'logo_geocivmet.png')
                        if inc_logo and os.path.exists(logo_src):
                            shutil.copy2(logo_src, os.path.join(dest, 'logo_geocivmet.png'))

                        # 6. Crear archivo de instrucciones
                        instrucciones = f"""# GEOCIVMET - Sistema de Materias Primas
# Paquete preparado para: {nombre_cliente}
# Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')}
#
# INSTRUCCIONES DE USO:
# 1. Instalar Python 3.10+ (https://python.org)
# 2. Instalar dependencias:
#    pip install streamlit pandas plotly openpyxl numpy
# 3. Ejecutar la aplicación:
#    streamlit run app_arcillas.py
# 4. Abrir en navegador: http://localhost:8501
#
# CREDENCIALES POR DEFECTO:
#   Usuario: admin
#   Contraseña: admin2026
#
# Created, Engineered & Developed by GEOCIVMET Consultores Técnicos
# v4.0 Lab System | © 2026 Todos los derechos reservados.
"""
                        with open(os.path.join(dest, 'INSTRUCCIONES.txt'), 'w', encoding='utf-8') as f:
                            f.write(instrucciones)

                        # 7. Crear requirements.txt
                        reqs = "streamlit>=1.30.0\npandas>=2.0.0\nplotly>=5.18.0\nopenpyxl>=3.1.0\nnumpy>=1.24.0\n"
                        with open(os.path.join(dest, 'requirements.txt'), 'w') as f:
                            f.write(reqs)

                        # Contar archivos copiados
                        total_files = sum(len(files) for _, _, files in os.walk(dest))

                        st.success(f"""
                        Paquete generado exitosamente!

                        **Cliente:** {nombre_cliente}
                        **Ubicación:** `{dest}`
                        **Archivos totales:** {total_files}
                        **Muestras incluidas:** {len(df)}
                        """)

                        st.balloons()

                    except Exception as e:
                        st.error(f"Error generando paquete: {e}")

        st.divider()

        # Herramienta de limpieza rápida
        st.markdown("#### Limpiar App para Nuevo Cliente")
        st.caption("Borra TODOS los datos (muestras, archivos, imágenes) para preparar la app para otro cliente.")

        if st.button("Limpiar Todo (Resetear)", type="secondary"):
            st.session_state['confirm_clean'] = True

        if st.session_state.get('confirm_clean'):
            st.warning("Esto borrará TODA la data. No se puede deshacer.")
            c_yes, c_no = st.columns(2)
            if c_yes.button("SI, LIMPIAR TODO", type="primary"):
                conn = get_conn()
                c = conn.cursor()
                for t in ['blend_componentes', 'blends', 'repositorio', 'fisica', 'quimica', 'muestras']:
                    c.execute(f"DELETE FROM {t}")
                    try:
                        c.execute(f"DELETE FROM sqlite_sequence WHERE name='{t}'")
                    except Exception:
                        pass
                conn.commit()
                conn.close()
                # Limpiar archivos
                for d in ['repositorio', 'imagenes']:
                    dp = os.path.join(BASE_DIR, d)
                    if os.path.exists(dp):
                        for root, dirs, files in os.walk(dp):
                            for f in files:
                                os.remove(os.path.join(root, f))
                st.session_state['confirm_clean'] = False
                st.success("App limpiada. Lista para cargar datos de un nuevo cliente.")
                st.rerun()
            if c_no.button("Cancelar"):
                st.session_state['confirm_clean'] = False
                st.rerun()

    # ========== TAB 6: CLASIFICACIONES EDITABLES ==========
    with tab6:
        st.subheader("📊 Clasificaciones Editables de Arcillas")
        st.caption("Edite las reglas de clasificacion por calidad (Fe₂O₃) y uso ceramico (Absorcion).")

        conn = get_conn()
        df_clasif = pd.read_sql(
            "SELECT id, tipo, nombre, campo, min_valor, max_valor, orden "
            "FROM clasificaciones_uso ORDER BY tipo, orden", conn)
        conn.close()

        if df_clasif.empty:
            st.info("No hay clasificaciones registradas. Reinicie la app para cargar las predeterminadas.")
        else:
            # Mostrar clasificaciones de CALIDAD
            st.markdown("#### 🎨 Clasificacion por Calidad (Fe₂O₃)")
            df_cal = df_clasif[df_clasif['tipo'] == 'calidad'].copy()
            if not df_cal.empty:
                for _, row in df_cal.iterrows():
                    col_n, col_min, col_max, col_del = st.columns([3, 2, 2, 1])
                    with col_n:
                        st.text(f"{row['nombre']}")
                    with col_min:
                        st.text(f"Min: {row['min_valor'] if row['min_valor'] is not None else '—'}")
                    with col_max:
                        st.text(f"Max: {row['max_valor'] if row['max_valor'] is not None else '—'}")
                    with col_del:
                        if st.button("🗑️", key=f"del_clasif_{row['id']}"):
                            conn = get_conn()
                            conn.execute("DELETE FROM clasificaciones_uso WHERE id = ?", (row['id'],))
                            conn.commit()
                            conn.close()
                            st.rerun()

            st.markdown("---")

            # Mostrar clasificaciones de USO
            st.markdown("#### 🏭 Clasificacion por Uso Ceramico (Absorcion)")
            df_uso = df_clasif[df_clasif['tipo'] == 'uso'].copy()
            if not df_uso.empty:
                for _, row in df_uso.iterrows():
                    col_n, col_min, col_max, col_del = st.columns([3, 2, 2, 1])
                    with col_n:
                        st.text(f"{row['nombre']}")
                    with col_min:
                        st.text(f"Min: {row['min_valor'] if row['min_valor'] is not None else '—'}")
                    with col_max:
                        st.text(f"Max: {row['max_valor'] if row['max_valor'] is not None else '—'}")
                    with col_del:
                        if st.button("🗑️", key=f"del_clasif_{row['id']}"):
                            conn = get_conn()
                            conn.execute("DELETE FROM clasificaciones_uso WHERE id = ?", (row['id'],))
                            conn.commit()
                            conn.close()
                            st.rerun()

        st.markdown("---")

        # Formulario para agregar nueva clasificacion
        st.markdown("#### Agregar Nueva Regla de Clasificacion")
        with st.form("form_nueva_clasificacion"):
            nc_tipo = st.selectbox("Tipo", ["calidad", "uso"])
            nc_nombre = st.text_input("Nombre (ej: Premium, Porcelanato Tecnico)")
            nc_campo = st.selectbox("Campo de referencia", ["fe2o3", "absorcion"])
            nc_c1, nc_c2, nc_c3 = st.columns(3)
            nc_min = nc_c1.number_input("Min valor", value=0.0, format="%.2f",
                                         help="Dejar en 0 si no aplica limite inferior")
            nc_max = nc_c2.number_input("Max valor", value=0.0, format="%.2f",
                                         help="Dejar en 0 si no aplica limite superior")
            nc_orden = nc_c3.number_input("Orden", value=1, min_value=1, max_value=20)
            nc_use_min = st.checkbox("Aplicar limite minimo", value=True)
            nc_use_max = st.checkbox("Aplicar limite maximo", value=True)

            if st.form_submit_button("Agregar Clasificacion", type="primary"):
                if nc_nombre.strip():
                    conn = get_conn()
                    conn.execute(
                        "INSERT INTO clasificaciones_uso (tipo, nombre, campo, min_valor, max_valor, orden) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (nc_tipo, nc_nombre.strip(), nc_campo,
                         nc_min if nc_use_min else None,
                         nc_max if nc_use_max else None,
                         nc_orden))
                    conn.commit()
                    conn.close()
                    st.success(f"Clasificacion '{nc_nombre}' agregada.")
                    st.rerun()
                else:
                    st.warning("Ingrese un nombre.")

        # Boton para restaurar clasificaciones predeterminadas
        st.markdown("---")
        if st.button("Restaurar Clasificaciones Predeterminadas", key="restore_clasif"):
            conn = get_conn()
            conn.execute("DELETE FROM clasificaciones_uso")
            clasificaciones_default = [
                ('calidad', 'Premium (Clara)', 'fe2o3', None, 1.0, 1),
                ('calidad', 'Estandar (Beige/Rosada)', 'fe2o3', 1.0, 1.8, 2),
                ('calidad', 'Industrial (Roja)', 'fe2o3', 1.8, None, 3),
                ('uso', 'Porcelanato Tecnico', 'absorcion', None, 0.5, 1),
                ('uso', 'Gres Esmaltado / Vitrificado', 'absorcion', 0.5, 3.0, 2),
                ('uso', 'Piso Stoneware / Semi-Gres', 'absorcion', 3.0, 6.0, 3),
                ('uso', 'Revestimiento Pared / Monoporosa', 'absorcion', 6.0, 10.0, 4),
                ('uso', 'Ladrilleria / Tejas', 'absorcion', 10.0, None, 5),
            ]
            for tipo, nombre, campo, min_v, max_v, orden in clasificaciones_default:
                conn.execute(
                    "INSERT INTO clasificaciones_uso (tipo, nombre, campo, min_valor, max_valor, orden) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (tipo, nombre, campo, min_v, max_v, orden))
            conn.commit()
            conn.close()
            st.success("Clasificaciones restauradas a valores predeterminados.")
            st.rerun()


if __name__ == '__main__':
    main()
