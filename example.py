import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import matplotlib

matplotlib.use("TkAgg")  # o "Qt5Agg" si tenés Qt instalado
conn = psycopg2.connect(
    "postgresql://postgres:Asdqwerty_09@db.ikdkhversotaizbhvwyh.supabase.co:5432/postgres"
)

sql = """
    SELECT * FROM "LIQUIDEZTEST" ORDER BY "date_id" DESC
"""

with conn.cursor() as cur:
    cur.execute(sql)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

df = pd.DataFrame(rows, columns=colnames)

# Ordenar por fecha y resetear índice
df_sorted = df.sort_values("date_id").reset_index(drop=True)

# X = índice simple, Y = price_bid
x = df_sorted.index
y = df_sorted["price_bid"]

# Puntos donde boolean es False
mask = ~df_sorted["boolean"].astype(bool)

plt.figure(figsize=(10, 4))

# Línea azul
plt.plot(x, y, label="Precio Bid", color="blue", zorder=1)

# Puntos rojos
plt.scatter(x[mask], y[mask], color="red", marker="o", s=20,
            label="Evento (boolean=False)", zorder=3)

# Líneas horizontales a la derecha
for xi, yi in zip(x[mask], y[mask]):
    plt.hlines(yi, xi, xi + (len(x)-xi) +20, colors="red", linewidth=1.5,linestyles='dashed', zorder=2)

sum_ticks = df['count_tick'].sum()
# Estética
plt.xlabel("Registro")
plt.ylabel("Price Bid")
plt.title(f"Liquidez limpia 12:00-12:30 - 14/07/2025 - cantidad de ticks: {sum_ticks} - sample: 1seg ")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.ion()  # Modo interactivo
plt.show()
input("Enter para cerrar")