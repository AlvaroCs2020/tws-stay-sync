import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
# plt.ion()  # Modo interactivo
def format_int_to_string(num):
    def fmt(n):
        return f"{n:.1f}".rstrip("0").rstrip(".")  # Ej: -109.0 -> -109, -109.50 -> -109.5

    abs_num = abs(num)
    sign = "-" if num < 0 else ""

    if abs_num >= 1_000_000_000:
        return f"{sign}{fmt(abs_num / 1_000_000_000)} B"
    elif abs_num >= 1_000_000:
        return f"{sign}{fmt(abs_num / 1_000_000)} M"
    elif abs_num >= 1_000:
        return f"{sign}{fmt(abs_num / 1_000)} K"
    else:
        return f"{num}"
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
count_tick = df["count_tick"].sum()
# Ordenar por fecha y resetear índice
df_sorted = df.sort_values("date_id").reset_index(drop=True)
max_time = df_sorted["date_id"].max()
min_time = df_sorted["date_id"].min()
# X = índice simple, Y = price_bid
x = df_sorted.index
x_date = df_sorted["date_id"]
y = df_sorted["price_bid"]

# Puntos donde boolean es False
mask = ~df_sorted["boolean"].astype(bool)

plt.figure(figsize=(15, 6))

# Línea azul
plt.plot(x, y, label="Precio Bid", color="blue", zorder=1)

# Puntos rojos
plt.scatter(x[mask], y[mask], color="red", marker="o", s=5,
            label="Liquidez sin saldar", zorder=3)

# Líneas horizontales a la derecha
for xi, yi in zip(x[mask], y[mask]):

    # Obtener el valor de sum_ask para esa fila

    diff = df_sorted.loc[xi, "difference"]

    diff_str = format_int_to_string(diff)

    if diff > 0:
        color = "red"
    else:
        color = "green"
    # Mostrar texto al final de la línea horizontal (pequeño offset para que no se superponga)
    plt.hlines(yi, xi, len(x) + 50, colors=color, linewidth=1.5, zorder=2)
    #no mostramos el texto por que se va t0do al choto
    #plt.text(len(x) + 52, yi, f"{diff_str}", color=color, fontsize=10, va="center")

# Estética
plt.xlabel("Tiempo [s]")
plt.ylabel("Price Bid")
plt.title(f"Curva Price Bid - sample: MAX 1seg - {min_time} to {max_time} - ticks: {count_tick} - registros: {len(df_sorted)} ")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()
input("presiona enter")