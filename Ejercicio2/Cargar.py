import pandas as pd
import pyodbc


# Conexion

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=Netflix_1FN;"
    "UID=sa;"
    "PWD=Pentaho*;"
)
cursor = conn.cursor()


# CSV

df = pd.read_csv("netflix_titles.csv")


# Limpieza

df['director'] = df['director'].fillna('Unknown')
df['cast'] = df['cast'].fillna('')
df['country'] = df['country'].fillna('Unknown')
df['listed_in'] = df['listed_in'].fillna('Unknown')

df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')


# Funciones


def get_or_insert(table, name):
    name = name.strip()

    if not name:
        return None

    try:
        cursor.execute(f"""
            INSERT INTO {table} (name)
            OUTPUT INSERTED.id
            VALUES (?)
        """, (name,))
        return cursor.fetchone()[0]

    except pyodbc.IntegrityError:
        cursor.execute(f"SELECT id FROM {table} WHERE name = ?", (name,))
        row = cursor.fetchone()
        return row[0] if row else None

def clean_text(text):
    if pd.isna(text):
        return None
    return text.strip()


def split_values(value):
    return [clean_text(v) for v in str(value).split(",") if clean_text(v)]

def safe_insert_relation(query, params):
    try:
        cursor.execute(query, params)
    except pyodbc.IntegrityError:
        pass


# Insertar datos


for index, row in df.iterrows():
    try:
        
        
       
        cursor.execute("""
            INSERT INTO shows (show_id, type, title, date_added, release_year, rating, duration, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['show_id'],
            row['type'],
            row['title'],
            row['date_added'],
            int(row['release_year']) if not pd.isna(row['release_year']) else None,
            row['rating'],
            row['duration'],
            row['description']
        ))

        
        # Directores
        
        for director in split_values(row['director']):
            director_id = get_or_insert("directors", director)
            safe_insert_relation("""
                INSERT INTO show_directors (show_id, director_id)
                VALUES (?, ?)
            """, (row['show_id'], director_id))

        
        # Actores
        
        for actor in split_values(row['cast']):
            actor_id = get_or_insert("actors", actor)
            safe_insert_relation("""
                INSERT INTO show_cast (show_id, actor_id)
                VALUES (?, ?)
            """, (row['show_id'], actor_id))

        
        # Paises
        
        for country in split_values(row['country']):
            country_id = get_or_insert("countries", country)
            safe_insert_relation("""
                INSERT INTO show_countries (show_id, country_id)
                VALUES (?, ?)
            """, (row['show_id'], country_id))

        
        # Generos
        
        for genre in split_values(row['listed_in']):
            genre_id = get_or_insert("genres", genre)
            safe_insert_relation("""
                INSERT INTO show_genres (show_id, genre_id)
                VALUES (?, ?)
            """, (row['show_id'], genre_id))

        if index % 500 == 0:
           conn.commit()

    except Exception as e:
        print(f"Error en fila {index}: {e}")
        conn.rollback()


# Cerre

conn.commit()
cursor.close()
conn.close()


print("Carga completada correctamente 🚀")